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
ABA_FATURAMENTO = "Faturamento"

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
    12: "Dezembro",
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
    "dez": 12,
}


def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def ct_valido(valor) -> bool:
    if pd.isna(valor):
        return False
    texto = str(valor).strip().upper()
    return bool(re.match(r"^CT-\d+[A-Z]?$", texto))


def detectar_engine(nome_arquivo: str) -> str:
    nome = nome_arquivo.lower()

    if nome.endswith(".xls"):
        return "xlrd"
    if nome.endswith(".xlsx") or nome.endswith(".xlsm"):
        return "openpyxl"

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


def converter_mov(valor):
    if pd.isna(valor):
        return None

    if isinstance(valor, (int, float)) and not isinstance(valor, bool):
        return float(valor)

    texto = str(valor).strip()

    if texto == "" or texto.lower() == "nan":
        return None

    if re.match(r"^\d{4}-\d{2}-\d{2}", texto):
        return None

    texto = texto.replace(".", "").replace(",", ".")

    try:
        return float(texto)
    except Exception:
        return None


def converter_moeda_brasileira(valor):
    if pd.isna(valor):
        return None

    if isinstance(valor, (int, float)) and not isinstance(valor, bool):
        return float(valor)

    texto = str(valor).strip()

    if texto == "" or texto.lower() == "nan":
        return None

    texto = texto.replace("R$", "").replace("$", "").strip()
    texto = texto.replace(".", "").replace(",", ".")

    texto = re.sub(r"[^\d\.\-]", "", texto)

    if texto in ("", "-", ".", "-."):
        return None

    try:
        return float(texto)
    except Exception:
        return None


def limpar_texto(valor):
    if pd.isna(valor):
        return ""
    return str(valor).strip()


def linha_tem_total(row) -> bool:
    for v in row:
        txt = str(v).strip().lower()
        if txt.startswith("total") or txt == "total":
            return True
    return False


def encontrar_header_faturamento(df_raw: pd.DataFrame):
    """
    Procura a linha de cabeçalho real onde existam ao menos:
    cliente / produto / total / meses
    """
    for i in df_raw.index:
        linha = [str(x).strip().lower() for x in df_raw.loc[i].tolist()]

        tem_cliente = any("cliente" == c for c in linha)
        tem_produto = any("produto" == c for c in linha)
        tem_total = any("total" in c for c in linha)
        qtd_meses = sum(1 for c in linha if c in MESES)

        if tem_cliente and tem_produto and tem_total and qtd_meses >= 6:
            return i

    return None


def encontrar_coluna(df: pd.DataFrame, nomes_possiveis):
    for c in df.columns:
        if str(c).strip().lower() in [n.lower() for n in nomes_possiveis]:
            return c
    return None


@app.get("/")
def home():
    return {"ok": True, "mensagem": "API online"}


# ========================
# ROTA 1 - BASE COMERCIAL
# GERA MOVIMENTAÇÃO ORÇADA
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

        meses_existentes = [m for m in MESES if m in [str(c).strip().lower() for c in df.columns]]
        mapa_cols = {str(c).strip().lower(): c for c in df.columns}
        meses_cols = [mapa_cols[m] for m in meses_existentes if m in mapa_cols]

        if not meses_cols:
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
            value_vars=meses_cols,
            var_name="MÊS",
            value_name="Movimentação(TON)"
        )

        df_final["Movimentação(TON)"] = pd.to_numeric(
            df_final["Movimentação(TON)"],
            errors="coerce"
        )

        df_final = df_final[df_final["Movimentação(TON)"].notna()].copy()
        df_final = df_final[df_final["Movimentação(TON)"] > 0].copy()

        df_final["MÊS"] = df_final["MÊS"].astype(str).str.strip().str.capitalize()
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
# GERA MOV REALIZADO
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

        mask_total_mensal = df_raw.astype(str).apply(
            lambda row: row.str.strip().str.lower().eq("total mensal").any(),
            axis=1
        )
        idx_total = df_raw.index[mask_total_mensal]
        if len(idx_total) > 0:
            fim = idx_total.tolist()[0]
            df_raw = df_raw.loc[:fim - 1].copy()

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
        cols = list(df.columns)

        def primeira_coluna_igual(alvos):
            for i, c in enumerate(cols):
                if str(c).strip().upper() in alvos:
                    return i, c
            return None, None

        idx_terminal, col_terminal = primeira_coluna_igual({"TERMINAL"})
        idx_produto, col_produto = primeira_coluna_igual({"PRODUTO"})
        idx_operacao, col_operacao = primeira_coluna_igual({"OPERAÇÃO", "OPERACAO"})

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

        inicio = min(idx_terminal, idx_produto, idx_operacao)
        fim = idx_operacao + 12

        if fim >= len(cols):
            raise HTTPException(
                status_code=400,
                detail=f"Não encontrei 12 colunas mensais após OPERAÇÃO. Colunas lidas: {cols}"
            )

        cols_bloco = cols[inicio:fim + 1]
        df = df.loc[:, cols_bloco].copy()

        cols = list(df.columns)
        col_terminal = next((c for c in cols if str(c).strip().upper() == "TERMINAL"), None)
        col_produto = next((c for c in cols if str(c).strip().upper() == "PRODUTO"), None)
        col_operacao = next((c for c in cols if str(c).strip().upper() in ["OPERAÇÃO", "OPERACAO"]), None)

        df[col_terminal] = df[col_terminal].ffill()

        df[col_terminal] = df[col_terminal].astype(str).str.strip()
        df[col_produto] = df[col_produto].astype(str).str.strip()
        df[col_operacao] = df[col_operacao].astype(str).str.strip()

        idx_operacao = cols.index(col_operacao)
        colunas_meses = cols[idx_operacao + 1: idx_operacao + 13]

        if len(colunas_meses) != 12:
            raise HTTPException(
                status_code=400,
                detail=f"Esperadas 12 colunas mensais, encontradas {len(colunas_meses)}."
            )

        meses_ok = []
        for c in colunas_meses:
            ano, mes = extrair_mes_ano(c)
            if ano is not None and mes is not None:
                meses_ok.append(c)

        if len(meses_ok) != 12:
            raise HTTPException(
                status_code=400,
                detail=f"As 12 colunas após OPERAÇÃO não são meses válidos. Colunas: {colunas_meses}"
            )

        operacoes_validas = ["imp", "exp", "cab", "exp/imp"]

        df = df[
            (df[col_produto] != "") &
            (df[col_produto].str.lower() != "nan") &
            (~df[col_produto].str.lower().eq("total")) &
            (~df[col_terminal].str.lower().eq("total")) &
            (~df[col_terminal].str.lower().str.startswith("total", na=False)) &
            (~df[col_terminal].str.lower().str.contains("total mensal", na=False)) &
            (~df[col_terminal].str.lower().eq("nan")) &
            (df[col_operacao].str.lower().isin(operacoes_validas))
        ].copy()

        temp_meses = df.loc[:, meses_ok].copy()
        for col in meses_ok:
            temp_meses[col] = temp_meses[col].map(converter_mov)

        mask_tem_valor = temp_meses.notna().any(axis=1)
        df = df.loc[mask_tem_valor].copy()

        df_long = df.melt(
            id_vars=[col_terminal, col_produto, col_operacao],
            value_vars=meses_ok,
            var_name="MES_ANO",
            value_name="Mov (TON)"
        )

        df_long = df_long[
            df_long[col_produto].astype(str).str.strip().ne("")
        ].copy()

        df_long = df_long[
            ~df_long[col_produto].astype(str).str.strip().str.lower().isin(["nan", "total"])
        ].copy()

        df_long = df_long[
            ~df_long[col_terminal].astype(str).str.strip().str.lower().str.startswith("total", na=False)
        ].copy()

        df_long["Mov (TON)"] = df_long["Mov (TON)"].map(converter_mov)
        df_long = df_long[df_long["Mov (TON)"].notna()].copy()
        df_long = df_long[df_long["Mov (TON)"] > 0].copy()
        df_long = df_long[df_long["Mov (TON)"] < 1000000].copy()

        ano_mes = df_long["MES_ANO"].map(extrair_mes_ano)
        df_long["ANO"] = [x[0] for x in ano_mes]
        df_long["MES_NUM"] = [x[1] for x in ano_mes]

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


# ========================
# ROTA 3 - FATURAMENTO ORÇADO
# GERA FATURAMENTO ORÇADO
# ========================
@app.post("/transformar_faturamento")
async def transformar_faturamento(file: UploadFile = File(...)):
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

        if ABA_FATURAMENTO not in xls.sheet_names:
            raise HTTPException(
                status_code=400,
                detail=f"A aba '{ABA_FATURAMENTO}' não foi encontrada no arquivo."
            )

        df_raw = pd.read_excel(
            xls,
            sheet_name=ABA_FATURAMENTO,
            header=None,
            engine=engine
        )

        header_idx = encontrar_header_faturamento(df_raw)
        if header_idx is None:
            raise HTTPException(
                status_code=400,
                detail="Não foi possível localizar o cabeçalho da aba Faturamento."
            )

        header = [str(x).strip() for x in df_raw.loc[header_idx].tolist()]
        df = df_raw.loc[header_idx + 1:].copy()
        df.columns = header

        df = df.dropna(axis=1, how="all").copy()
        df = normalizar_colunas(df)

        col_ct = encontrar_coluna(df, ["CT"])
        col_cliente = encontrar_coluna(df, ["Cliente"])
        col_produto = encontrar_coluna(df, ["Produto"])
        col_total = next((c for c in df.columns if "total" in str(c).strip().lower()), None)

        faltando = []
        if col_ct is None:
            faltando.append("CT")
        if col_cliente is None:
            faltando.append("Cliente")
        if col_produto is None:
            faltando.append("Produto")

        if faltando:
            raise HTTPException(
                status_code=400,
                detail=f"Colunas obrigatórias ausentes na aba Faturamento: {faltando}"
            )

        cols_lower = {str(c).strip().lower(): c for c in df.columns}
        meses_cols = [cols_lower[m] for m in MESES if m in cols_lower]

        if len(meses_cols) < 12:
            raise HTTPException(
                status_code=400,
                detail=f"Não encontrei as 12 colunas de meses na aba Faturamento. Encontradas: {meses_cols}"
            )

        cols_utilizadas = [col_ct, col_cliente, col_produto]
        if col_total is not None:
            cols_utilizadas.append(col_total)
        cols_utilizadas += meses_cols

        df = df.loc[:, cols_utilizadas].copy()

        df = df[~df.apply(linha_tem_total, axis=1)].copy()

        df[col_ct] = df[col_ct].astype(str).str.strip()
        df[col_cliente] = df[col_cliente].astype(str).str.strip()
        df[col_produto] = df[col_produto].astype(str).str.strip()

        df = df[df[col_ct].apply(ct_valido)].copy()

        df = df[
            (df[col_cliente] != "") &
            (df[col_produto] != "") &
            (df[col_cliente].str.lower() != "nan") &
            (df[col_produto].str.lower() != "nan") &
            (~df[col_cliente].str.contains(r"adicione\s*0\s*linha", case=False, na=False)) &
            (~df[col_produto].str.contains(r"^total$", case=False, na=False))
        ].copy()

        df_long = df.melt(
            id_vars=[col_ct, col_cliente, col_produto],
            value_vars=meses_cols,
            var_name="MÊS",
            value_name="Faturamento (R$)"
        )

        df_long["Faturamento (R$)"] = df_long["Faturamento (R$)"].map(converter_moeda_brasileira)

        df_long = df_long[df_long["Faturamento (R$)"].notna()].copy()
        df_long = df_long[df_long["Faturamento (R$)"] != 0].copy()

        df_long["MÊS"] = df_long["MÊS"].astype(str).str.strip().str.capitalize()
        df_long["ANO"] = 2026

        df_final = df_long.rename(columns={
            col_cliente: "Cliente",
            col_produto: "Produto"
        })

        df_final = df_final[[
            "ANO",
            "MÊS",
            "Cliente",
            "Produto",
            "Faturamento (R$)"
        ]].copy()

        saida = BytesIO()
        with pd.ExcelWriter(saida, engine="openpyxl") as writer:
            df_final.to_excel(writer, index=False, sheet_name="Base Padronizada")

        saida.seek(0)

        return StreamingResponse(
            saida,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=base_faturamento_padronizada.xlsx",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao processar base de faturamento: {repr(e)}"
        )
