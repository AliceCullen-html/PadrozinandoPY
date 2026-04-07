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
