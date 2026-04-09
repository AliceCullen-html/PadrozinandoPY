from io import BytesIO
import re
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ABA_ALVO = "Projeção - Ton. Movimentação"

MESES = [
    "janeiro", "fevereiro", "março", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
]

def normalizar_colunas(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df

def ct_valido(valor):
    if pd.isna(valor):
        return False
    texto = str(valor).strip().upper()
    return bool(re.match(r"^CT-\d+[A-Z]?$", texto))

@app.get("/")
def home():
    return {"ok": True, "mensagem": "API online"}


@app.post("/transformar")
async def transformar(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Arquivo não enviado.")

        nome = file.filename.lower()

        if nome.endswith(".xls"):
            engine = "xlrd"
        elif nome.endswith(".xlsx") or nome.endswith(".xlsm"):
            engine = "openpyxl"
        else:
            raise HTTPException(status_code=400, detail="Formato inválido.")

        conteudo = await file.read()
        entrada = BytesIO(conteudo)

        xls = pd.ExcelFile(entrada, engine=engine)

        if ABA_ALVO not in xls.sheet_names:
            raise HTTPException(status_code=400, detail="Aba não encontrada.")

        df = pd.read_excel(xls, sheet_name=ABA_ALVO, header=1, engine=engine)
        df = normalizar_colunas(df)

        df = df[df["CT"].apply(ct_valido)].copy()

        df["Cliente"] = df["Cliente"].astype(str).str.strip()
        df["Produto"] = df["Produto"].astype(str).str.strip()

        meses_existentes = [m for m in MESES if m in df.columns]

        df_final = df.melt(
            id_vars=["CT", "Cliente", "Produto"],
            value_vars=meses_existentes,
            var_name="MÊS",
            value_name="Movimentação(TON)"
        )

        df_final["Movimentação(TON)"] = pd.to_numeric(
            df_final["Movimentação(TON)"], errors="coerce"
        )

        df_final = df_final[df_final["Movimentação(TON)"].notna()]
        df_final = df_final[df_final["Movimentação(TON)"] > 0]

        df_final["ANO"] = 2026

        df_final = df_final[[
            "ANO", "MÊS", "Cliente", "Produto", "Movimentação(TON)"
        ]]

        saida = BytesIO()
        df_final.to_excel(saida, index=False)
        saida.seek(0)

        return StreamingResponse(
            saida,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=base.xlsx"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/transformar_terminal")
async def transformar_terminal(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Arquivo não enviado.")

        nome = file.filename.lower()

        if nome.endswith(".xls"):
            engine = "xlrd"
        elif nome.endswith(".xlsx") or nome.endswith(".xlsm"):
            engine = "openpyxl"
        else:
            raise HTTPException(
                status_code=400,
                detail="Formato inválido. Envie .xls, .xlsx ou .xlsm."
            )

        conteudo = await file.read()
        entrada = BytesIO(conteudo)

        # lê bruto
        df_raw = pd.read_excel(entrada, header=None, engine=engine)

        # corta antes de tudo que vem abaixo de "Total mensal"
        mask_total_mensal = df_raw.astype(str).apply(
            lambda row: row.str.strip().str.lower().eq("total mensal").any(),
            axis=1
        )

        idx_total = df_raw.index[mask_total_mensal]
        if len(idx_total) > 0:
            fim = idx_total.tolist()[0]
            df_raw = df_raw.loc[:fim - 1].copy()

        # acha linha do cabeçalho real
        header_idx = None
        for i in df_raw.index:
            linha = [str(x).strip().lower() for x in df_raw.loc[i].tolist()]
            if "terminal" in linha and "produto" in linha and "operação" in linha:
                header_idx = i
                break
            if "terminal" in linha and "produto" in linha and "operacao" in linha:
                header_idx = i
                break

        if header_idx is None:
            raise HTTPException(
                status_code=400,
                detail="Não foi possível localizar o cabeçalho da tabela principal."
            )

        header = [str(x).strip() for x in df_raw.loc[header_idx].tolist()]
        df = df_raw.loc[header_idx + 1:].copy()
        df.columns = header

        # remove colunas totalmente vazias
        df = df.dropna(axis=1, how="all").copy()

        # normaliza nomes
        df.columns = [str(c).strip() for c in df.columns]

        # localiza nomes reais das colunas
        col_terminal = next((c for c in df.columns if str(c).strip().upper() == "TERMINAL"), None)
        col_produto = next((c for c in df.columns if str(c).strip().upper() == "PRODUTO"), None)
        col_operacao = next(
            (c for c in df.columns if str(c).strip().upper() in ["OPERAÇÃO", "OPERACAO"]),
            None
        )

        faltando = [x for x in ["TERMINAL", "PRODUTO", "OPERAÇÃO"] if {
            "TERMINAL": col_terminal,
            "PRODUTO": col_produto,
            "OPERAÇÃO": col_operacao
        }[x] is None]

        if faltando:
            raise HTTPException(
                status_code=400,
                detail=f"Colunas obrigatórias ausentes: {faltando}"
            )

        # mantém só até a tabela principal
        df[col_terminal] = df[col_terminal].ffill()

        df[col_terminal] = df[col_terminal].astype(str).str.strip()
        df[col_produto] = df[col_produto].astype(str).str.strip()
        df[col_operacao] = df[col_operacao].astype(str).str.strip()

        # remove linhas inúteis
        df = df[
            (df[col_produto] != "") &
            (df[col_produto].str.lower() != "nan") &
            (~df[col_produto].str.lower().eq("total")) &
            (~df[col_terminal].str.lower().str.contains("total mensal", na=False)) &
            (~df[col_terminal].str.lower().eq("nan"))
        ].copy()

        # colunas de mês válidas: jan/21, fev/21...
        regex_mes = re.compile(r"^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)/\d{2}$", re.IGNORECASE)
        colunas_meses = [c for c in df.columns if regex_mes.match(str(c).strip())]

        if not colunas_meses:
            raise HTTPException(
                status_code=400,
                detail=f"Nenhuma coluna de mês/ano encontrada. Colunas lidas: {list(df.columns)}"
            )

        df_long = df.melt(
            id_vars=[col_terminal, col_produto, col_operacao],
            value_vars=colunas_meses,
            var_name="MES_ANO",
            value_name="Mov (TON)"
        )

        # converte valores
        df_long["Mov (TON)"] = (
            df_long["Mov (TON)"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df_long["Mov (TON)"] = pd.to_numeric(df_long["Mov (TON)"], errors="coerce")
        df_long = df_long[df_long["Mov (TON)"].notna()].copy()

        mapa_meses = {
            "jan": ("Janeiro", 1),
            "fev": ("Fevereiro", 2),
            "mar": ("Março", 3),
            "abr": ("Abril", 4),
            "mai": ("Maio", 5),
            "jun": ("Junho", 6),
            "jul": ("Julho", 7),
            "ago": ("Agosto", 8),
            "set": ("Setembro", 9),
            "out": ("Outubro", 10),
            "nov": ("Novembro", 11),
            "dez": ("Dezembro", 12),
        }

        df_long["MES_ANO"] = df_long["MES_ANO"].astype(str).str.strip().str.lower()
        partes = df_long["MES_ANO"].str.split("/", n=1, expand=True)

        if partes.shape[1] < 2:
            raise HTTPException(
                status_code=400,
                detail="Não foi possível separar mês/ano das colunas."
            )

        df_long["MES_SIGLA"] = partes[0].str[:3]
        df_long["ANO"] = pd.to_numeric("20" + partes[1], errors="coerce")
        df_long["MÊS"] = df_long["MES_SIGLA"].map(lambda x: mapa_meses.get(x, ("", None))[0])
        df_long["MES_NUM"] = df_long["MES_SIGLA"].map(lambda x: mapa_meses.get(x, ("", None))[1])

        df_long = df_long[df_long["ANO"].notna()].copy()
        df_long = df_long[df_long["MES_NUM"].notna()].copy()

        df_long["ANO"] = df_long["ANO"].astype(int)

        df_long["Data"] = pd.to_datetime(
            dict(year=df_long["ANO"], month=df_long["MES_NUM"].astype(int), day=1)
        )

        df_final = df_long.rename(columns={
            col_terminal: "TERMINAL",
            col_produto: "PRODUTO",
            col_operacao: "Operação"
        })

        df_final["GRUPO DE PRODUTOS"] = None

        df_final = df_final[
            ["TERMINAL", "ANO", "MÊS", "PRODUTO", "GRUPO DE PRODUTOS", "Mov (TON)", "Data", "Operação"]
        ].copy()

        saida = BytesIO()
        with pd.ExcelWriter(saida, engine="openpyxl") as writer:
            df_final.to_excel(writer, index=False, sheet_name="Base Padronizada")

        saida.seek(0)

        return StreamingResponse(
            saida,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=base_terminal_padronizada.xlsx",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar base de terminais: {repr(e)}"
        )
