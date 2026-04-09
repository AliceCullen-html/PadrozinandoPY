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

MAPA_MESES = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro"
}

SIGLA_PARA_MES = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12
}


def normalizar_colunas(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df


def ct_valido(valor):
    if pd.isna(valor):
        return False
    texto = str(valor).strip().upper()
    return bool(re.match(r"^CT-\d+[A-Z]?$", texto))


def detectar_engine(nome_arquivo: str) -> str:
    nome = nome_arquivo.lower()

    if nome.endswith(".xls"):
        return "xlrd"
    elif nome.endswith(".xlsx") or nome.endswith(".xlsm"):
        return "openpyxl"
    else:
        raise HTTPException(
            status_code=400,
            detail="Formato inválido. Envie .xls, .xlsx ou .xlsm."
        )


def extrair_mes_ano(valor):
    if isinstance(valor, pd.Timestamp):
        return valor.year, valor.month

    texto = str(valor).strip()

    try:
        dt = pd.to_datetime(texto, errors="raise")
        if pd.notna(dt):
            return dt.year, dt.month
    except Exception:
        pass

    m = re.match(r"^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)/(\d{2})$", texto.lower())
    if m:
        sigla = m.group(1)
        ano = int("20" + m.group(2))
        mes = SIGLA_PARA_MES[sigla]
        return ano, mes

    return None, None


@app.get("/")
def home():
    return {"ok": True, "mensagem": "API online"}


# ========================
# ROTA 1 - BASE COMERCIAL
# ========================
@app.post("/transformar")
async def transformar(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Arquivo não enviado.")

        engine = detectar_engine(file.filename)

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

        df["CT"] = df["CT"].astype(str).str.strip()
        df = df[df["CT"].apply(ct_valido)].copy()

        df["Cliente"] = df["Cliente"].astype(str).str.strip()
        df["Produto"] = df["Produto"].astype(str).str.strip()

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

        df_final["Movimentação(TON)"] = pd.to_numeric(
            df_final["Movimentação(TON)"],
            errors="coerce"
        )

        df_final = df_final[df_final["Movimentação(TON)"].notna()].copy()
        df_final = df_final[df_final["Movimentação(TON)"] > 0].copy()

        df_final["ANO"] = 2026

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
            detail=f"Erro ao processar arquivo: {repr(e)}"
        )


# ========================
# ROTA 2 - TERMINAIS
# ========================
@app.post("/transformar_terminal")
async def transformar_terminal(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Arquivo não enviado.")

        engine = detectar_engine(file.filename)

        conteudo = await file.read()
        entrada = BytesIO(conteudo)

        df_raw = pd.read_excel(entrada, header=None, engine=engine)

        # corta tudo abaixo de "Total mensal"
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
            if "terminal" in linha and "produto" in linha and ("operação" in linha or "operacao" in linha):
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

        df = df.dropna(axis=1, how="all").copy()
        df.columns = [str(c).strip() for c in df.columns]

        col_terminal = next((c for c in df.columns if str(c).strip().upper() == "TERMINAL"), None)
        col_produto = next((c for c in df.columns if str(c).strip().upper() == "PRODUTO"), None)
        col_operacao = next(
            (c for c in df.columns if str(c).strip().upper() in ["OPERAÇÃO", "OPERACAO"]),
            None
        )

        faltando = []
        if col_terminal is None:
            faltando.append("TERMINAL")
        if col_produto is None:
            faltando.append("PRODUTO")
        if col_operacao is None:
            faltando.append("OPERAÇÃO")

        if faltando:
            raise HTTPException(
                status_code=400,
                detail=f"Colunas obrigatórias ausentes: {faltando}"
            )

        df[col_terminal] = df[col_terminal].ffill()

        df[col_terminal] = df[col_terminal].astype(str).str.strip()
        df[col_produto] = df[col_produto].astype(str).str.strip()
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
            ano, mes = extrair_mes_ano(c)
            if ano is not None and mes is not None:
                colunas_meses.append(c)

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

        df_long["Mov (TON)"] = (
            df_long["Mov (TON)"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )

        df_long["Mov (TON)"] = pd.to_numeric(df_long["Mov (TON)"], errors="coerce")
        df_long = df_long[df_long["Mov (TON)"].notna()].copy()

        df_long[["ANO", "MES_NUM"]] = df_long["MES_ANO"].apply(
            lambda x: pd.Series(extrair_mes_ano(x))
        )

        df_long = df_long[df_long["ANO"].notna()].copy()
        df_long = df_long[df_long["MES_NUM"].notna()].copy()

        df_long["ANO"] = df_long["ANO"].astype(int)
        df_long["MES_NUM"] = df_long["MES_NUM"].astype(int)
        df_long["MÊS"] = df_long["MES_NUM"].map(MAPA_MESES)

        df_long["Data"] = pd.to_datetime(
            dict(year=df_long["ANO"], month=df_long["MES_NUM"], day=1)
        )

        df_final = df_long.rename(columns={
            col_terminal: "TERMINAL",
            col_produto: "PRODUTO",
            col_operacao: "Operação"
        })

        df_final["GRUPO DE PRODUTOS"] = None

        df_final = df_final[[
            "TERMINAL",
            "ANO",
            "MÊS",
            "PRODUTO",
            "GRUPO DE PRODUTOS",
            "Mov (TON)",
            "Data",
            "Operação"
        ]].copy()

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
