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

def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def ct_valido(valor) -> bool:
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

        nome_arquivo = file.filename.lower()

        if nome_arquivo.endswith(".xls"):
            engine = "xlrd"
        elif nome_arquivo.endswith(".xlsx"):
            engine = "openpyxl"
        else:
            raise HTTPException(
                status_code=400,
                detail="Formato inválido. Envie um arquivo .xls ou .xlsx."
            )

        conteudo = await file.read()
        entrada = BytesIO(conteudo)

        try:
            xls = pd.ExcelFile(entrada, engine=engine)
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Não foi possível ler o Excel: {str(e)}"
            )

        if ABA_ALVO not in xls.sheet_names:
            raise HTTPException(
                status_code=400,
                detail=f"A aba '{ABA_ALVO}' não foi encontrada no arquivo."
            )

        df = pd.read_excel(
            xls,
            sheet_name=ABA_ALVO,
            header=1,
            engine=engine
        )

        df = normalizar_colunas(df)

        colunas_obrigatorias = ["CT", "Cliente", "Produto"]
        faltando = [c for c in colunas_obrigatorias if c not in df.columns]
        if faltando:
            raise HTTPException(
                status_code=400,
                detail=f"Colunas obrigatórias ausentes: {faltando}"
            )

        meses_existentes = [m for m in MESES if m in df.columns]
        if not meses_existentes:
            raise HTTPException(
                status_code=400,
                detail="Nenhuma coluna de mês encontrada."
            )

        # Mantém só linhas reais da tabela
        df["CT"] = df["CT"].astype(str).str.strip()
        df = df[df["CT"].apply(ct_valido)].copy()

        # Limpa campos texto
        df["Cliente"] = df["Cliente"].astype(str).str.strip()
        df["Produto"] = df["Produto"].astype(str).str.strip()

        # Remove linhas obviamente inválidas antes do melt
        df = df[
            (df["Cliente"] != "") &
            (df["Produto"] != "") &
            (df["Cliente"].str.lower() != "nan") &
            (df["Produto"].str.lower() != "nan") &
            (~df["Cliente"].str.contains(r"adicione\s*0\s*linha", case=False, na=False)) &
            (~df["Produto"].str.contains(r"^total$", case=False, na=False))
        ].copy()

        df_final = df.melt(
            id_vars=["CT", "Cliente", "Produto"],
            value_vars=meses_existentes,
            var_name="MÊS",
            value_name="Movimentação(TON)"
        )

        # Remove vazios
        df_final = df_final[df_final["Movimentação(TON)"].notna()].copy()

        # Converte valor para número
        df_final["Movimentação(TON)"] = pd.to_numeric(
            df_final["Movimentação(TON)"],
            errors="coerce"
        )

        df_final = df_final[df_final["Movimentação(TON)"].notna()].copy()

        # Remove linhas com valor <= 0 se quiser só movimento real
        df_final = df_final[df_final["Movimentação(TON)"] > 0].copy()

        # Ano fixo por enquanto
        df_final["ANO"] = 2026

        # Saída final sem CT
        df_final = df_final[[
            "ANO",
            "MÊS",
            "Cliente",
            "Produto",
            "Movimentação(TON)"
        ]]

        saida = BytesIO()
        with pd.ExcelWriter(saida, engine="openpyxl") as writer:
            df_final.to_excel(writer, index=False, sheet_name="Base Padronizada")

        saida.seek(0)

        return StreamingResponse(
            saida,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=base_padronizada.xlsx",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar arquivo: {str(e)}"
        )

@app.post("/transformar_terminal")
async def transformar_terminal(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Arquivo não enviado.")

        nome_arquivo = file.filename.lower()

        if nome_arquivo.endswith(".xls"):
            engine = "xlrd"
        elif nome_arquivo.endswith(".xlsx") or nome_arquivo.endswith(".xlsm"):
            engine = "openpyxl"
        else:
            raise HTTPException(
                status_code=400,
                detail="Formato inválido. Envie .xls, .xlsx ou .xlsm."
            )

        conteudo = await file.read()
        entrada = BytesIO(conteudo)

        # lê sem assumir cabeçalho perfeito
        df = pd.read_excel(entrada, header=None, engine=engine)

        # corta tudo abaixo de "Total mensal"
        idx_total_mensal = df[
            df.iloc[:, 0].astype(str).str.strip().str.lower().eq("total mensal")
        ].index

        if len(idx_total_mensal) > 0:
            df = df.loc[:idx_total_mensal[0] - 1].copy()

        # a linha de cabeçalho real costuma ser a que contém TERMINAL / PRODUTO / OPERAÇÃO
        header_idx = None
        for i in df.index:
            linha = df.loc[i].astype(str).str.strip().str.lower().tolist()
            if "terminal" in linha and "produto" in linha and "operação" in linha:
                header_idx = i
                break

        if header_idx is None:
            raise HTTPException(
                status_code=400,
                detail="Não foi possível localizar o cabeçalho da tabela principal."
            )

        header = df.loc[header_idx].tolist()
        df = df.loc[header_idx + 1:].copy()
        df.columns = [str(c).strip() for c in header]

        # remove colunas totalmente vazias
        df = df.dropna(axis=1, how="all").copy()

        # normaliza nomes
        df.columns = [str(c).strip() for c in df.columns]

        col_terminal = "TERMINAL"
        col_produto = "PRODUTO"
        col_operacao = "OPERAÇÃO"

        faltando = [c for c in [col_terminal, col_produto, col_operacao] if c not in df.columns]
        if faltando:
            raise HTTPException(
                status_code=400,
                detail=f"Colunas obrigatórias ausentes: {faltando}"
            )

        # forward fill do terminal
        df[col_terminal] = df[col_terminal].ffill()

        # remove linhas inúteis
        df[col_produto] = df[col_produto].astype(str).str.strip()
        df[col_terminal] = df[col_terminal].astype(str).str.strip()
        df[col_operacao] = df[col_operacao].astype(str).str.strip()

        df = df[
            (df[col_produto] != "") &
            (df[col_produto].str.lower() != "nan") &
            (~df[col_produto].str.lower().eq("total")) &
            (~df[col_terminal].str.lower().str.contains("total mensal", na=False)) &
            (~df[col_terminal].str.lower().eq("nan"))
        ].copy()

        # colunas de mês
        colunas_meses = []
        for c in df.columns:
            c_txt = str(c).strip().lower()
            if "/" in c_txt and len(c_txt) <= 7:
                colunas_meses.append(c)

        if not colunas_meses:
            raise HTTPException(
                status_code=400,
                detail="Nenhuma coluna de mês/ano encontrada."
            )

        df_long = df.melt(
            id_vars=[col_terminal, col_produto, col_operacao],
            value_vars=colunas_meses,
            var_name="MES_ANO",
            value_name="Mov (TON)"
        )

        df_long["Mov (TON)"] = pd.to_numeric(df_long["Mov (TON)"], errors="coerce")
        df_long = df_long[df_long["Mov (TON)"].notna()].copy()

        # separa mês e ano
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
        partes = df_long["MES_ANO"].str.split("/", expand=True)

        df_long["MES_SIGLA"] = partes[0].str[:3]
        df_long["ANO"] = ("20" + partes[1].astype(str)).astype(int)

        df_long["MÊS"] = df_long["MES_SIGLA"].map(lambda x: mapa_meses.get(x, ("", None))[0])
        df_long["MES_NUM"] = df_long["MES_SIGLA"].map(lambda x: mapa_meses.get(x, ("", None))[1])

        df_long = df_long[df_long["MES_NUM"].notna()].copy()

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
            detail=f"Erro ao processar base de terminais: {str(e)}"
        )
