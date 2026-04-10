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

        # acha cabeçalho real
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

        # usa o cabeçalho bruto inteiro, sem tentar reduzir tamanho aqui
        header = [str(x).strip() for x in df_raw.loc[header_idx].tolist()]
        df = df_raw.loc[header_idx + 1:].copy()
        df.columns = header

        # remove colunas totalmente vazias
        df = df.dropna(axis=1, how="all").copy()

        # normaliza nomes
        df.columns = [str(c).strip() for c in df.columns]

        # pega primeira ocorrência das colunas principais
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

        # pega exatamente o bloco da tabela:
        # TERMINAL | PRODUTO | OPERAÇÃO | 12 meses
        inicio = min(idx_terminal, idx_produto, idx_operacao)
        fim = idx_operacao + 12

        if fim >= len(cols):
            raise HTTPException(
                status_code=400,
                detail=f"Não encontrei 12 colunas mensais após OPERAÇÃO. Colunas lidas: {cols}"
            )

        cols_bloco = cols[inicio:fim + 1]
        df = df.loc[:, cols_bloco].copy()

        # redefine nomes após corte
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

        # valida que as 12 colunas são realmente mês/ano
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

        # remove linhas inválidas
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

        # mantém só linhas com pelo menos 1 valor mensal real
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
