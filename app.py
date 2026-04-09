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
            raise HTTPException(status_code=400, detail="Formato inválido.")

        conteudo = await file.read()
        entrada = BytesIO(conteudo)

        df = pd.read_excel(entrada, header=None, engine=engine)

        # corta após "Total mensal"
        idx_total = df[
            df.astype(str).apply(
                lambda row: row.str.strip().str.lower().eq("total mensal").any(),
                axis=1
            )
        ].index

        if len(idx_total) > 0:
            df = df.loc[:idx_total[0] - 1]

        # acha cabeçalho
        header_idx = None
        for i in df.index:
            linha = df.loc[i].astype(str).str.lower().tolist()
            if "terminal" in linha and "produto" in linha:
                header_idx = i
                break

        if header_idx is None:
            raise HTTPException(status_code=400, detail="Cabeçalho não encontrado")

        header = df.loc[header_idx]
        df = df.loc[header_idx + 1:]
        df.columns = header

        df.columns = [str(c).strip() for c in df.columns]

        df["TERMINAL"] = df["TERMINAL"].ffill()

        df = df[
            (~df["PRODUTO"].astype(str).str.lower().eq("total")) &
            (df["PRODUTO"].notna())
        ]

        # meses
        cols_meses = [c for c in df.columns if "/" in str(c)]

        df_long = df.melt(
            id_vars=["TERMINAL", "PRODUTO", "OPERAÇÃO"],
            value_vars=cols_meses,
            var_name="MES_ANO",
            value_name="Mov"
        )

        df_long["Mov"] = pd.to_numeric(df_long["Mov"], errors="coerce")
        df_long = df_long[df_long["Mov"].notna()]

        partes = df_long["MES_ANO"].str.split("/", expand=True)

        mapa = {
            "jan": "Janeiro", "fev": "Fevereiro", "mar": "Março",
            "abr": "Abril", "mai": "Maio", "jun": "Junho",
            "jul": "Julho", "ago": "Agosto", "set": "Setembro",
            "out": "Outubro", "nov": "Novembro", "dez": "Dezembro"
        }

        df_long["MÊS"] = partes[0].str[:3].map(mapa)
        df_long["ANO"] = ("20" + partes[1]).astype(int)

        df_long["Data"] = pd.to_datetime(
            dict(year=df_long["ANO"], month=1, day=1)
        )

        df_final = df_long.rename(columns={
            "TERMINAL": "TERMINAL",
            "PRODUTO": "PRODUTO",
            "OPERAÇÃO": "Operação",
            "Mov": "Mov (TON)"
        })

        df_final["GRUPO DE PRODUTOS"] = None

        df_final = df_final[
            ["TERMINAL", "ANO", "MÊS", "PRODUTO",
             "GRUPO DE PRODUTOS", "Mov (TON)", "Data", "Operação"]
        ]

        saida = BytesIO()
        df_final.to_excel(saida, index=False)
        saida.seek(0)

        return StreamingResponse(
            saida,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=terminal.xlsx"}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
