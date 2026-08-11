# -*- coding: utf-8 -*-
"""
Created on Tue Aug 26 10:01:40 2025

@author: glima
"""
import urllib3
import requests
import os
import pandas as pd
import numpy as np
from clean_text import clean_text


def converter_para_hl(df_conversao, qtd_produzida, unidade_medida, cod_produto=None):
    """
    Converte uma quantidade para hectolitros (hL) baseado nas regras do CSV.
    
    Parâmetros:
    - qtd_produzida: quantidade a ser convertida
    - unidade_medida: unidade de medida original
    - cod_produto: código do produto (opcional, para regras específicas)
    
    Retorna:
    - Quantidade convertida em hL ou np.nan se não encontrar conversão
    """
    # Primeiro tenta encontrar por código específico do produto
    if cod_produto is not None:
        # Verifica se o código começa com algum valor específico no CSV
        mascara_cod = df_conversao['cod_produto'].astype(str).str.startswith(str(cod_produto))
        mascara_unidade = df_conversao['unidade'] == unidade_medida
        resultado_especifico = df_conversao[mascara_cod & mascara_unidade]
        
        if not resultado_especifico.empty:
            return qtd_produzida * resultado_especifico.iloc[0]['hl']
    
    # Se não encontrou específico, procura nas regras gerais
    mascara_geral = (df_conversao['cod_produto'] == 'geral') & (df_conversao['unidade'] == unidade_medida)
    resultado_geral = df_conversao[mascara_geral]
    
    if not resultado_geral.empty:
        return qtd_produzida * resultado_geral.iloc[0]['hl']
    
    # Se não encontrou em nenhum lugar, retorna NaN
    return pd.NA




#%% Função de tratamento de outliers

def tratamento_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica uma série de filtros e tratamentos em um DataFrame de produção.
    
    Esta versão calcula uma mediana refinada (excluindo outliers) para ser usada
    especificamente no preenchimento de dados faltantes, tornando a imputação
    mais robusta.

    Args:
        df (pd.DataFrame): O DataFrame de entrada.

    Returns:
        pd.DataFrame: Um novo DataFrame com os outliers tratados, as séries 
                      temporais preenchidas e a coluna de observação.
    """
    
    print("Iniciando o tratamento de outliers e preenchimento de dados...")
    
    # --- Validação e Pré-processamento ---

    # Lista de colunas essenciais para o funcionamento da função.
    colunas_necessarias = [
        'mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto', 'num_ano',
        'SITUACAO CADASTRAL', 'Produção (Ton ou hL)'
    ]
    # Valida se todas as colunas necessárias existem no DataFrame de entrada.
    for col in colunas_necessarias:
        if col not in df.columns:
            raise ValueError(f"A coluna obrigatória '{col}' não foi encontrada no DataFrame.")

    # Define as colunas que identificam uma série temporal única (unidade produtiva + produto).
    group_cols = ['mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto']
    
    # Define as colunas para identificar registros duplicados (mesma unidade, produto e ano).
    agg_cols = group_cols + ['num_ano']
    # Verifica se existem registros duplicados.
    if df.duplicated(subset=agg_cols).any():
        print("Detectados registros duplicados. Agregando valores...")
        # Cria um dicionário de agregação: soma a produção e pega o primeiro valor para as outras colunas.
        agg_dict = {'Produção (Ton ou hL)': 'sum'}
        other_cols = [col for col in df.columns if col not in agg_cols and col != 'Produção (Ton ou hL)']
        for col in other_cols:
            agg_dict[col] = 'first'
        # Agrupa os dados e aplica as regras de agregação para consolidar as duplicatas.
        df = df.groupby(agg_cols, as_index=False).agg(agg_dict)
        print("Agregação de duplicatas concluída.")

    # --- 1º Filtro: Verificar histórico de reporte ---
    print("Aplicando o 1º Filtro: Verificação do histórico de reporte.")

    # Função auxiliar para verificar se um grupo tem histórico de reporte suficiente.
    def _verificar_historico(anos_serie):
        anos_unicos = sorted(anos_serie.unique())
        # Critério 1: Pelo menos 5 anos distintos de reporte.
        if len(anos_unicos) >= 5: return True
        # Critério 2: Pelo menos 3 anos consecutivos de reporte.
        if len(anos_unicos) >= 3:
            for i in range(len(anos_unicos) - 2):
                if anos_unicos[i+2] - anos_unicos[i] == 2: return True
        return False

    # Aplica a função de verificação a cada grupo e cria uma máscara booleana.
    mascara_historico = df.groupby(group_cols)['num_ano'].transform(_verificar_historico)
    # Filtra o DataFrame, mantendo apenas os grupos com histórico suficiente.
    df_filtrado_1 = df[mascara_historico].copy()
    
    print(f"O DataFrame foi reduzido para {len(df_filtrado_1)} linhas após o 1º filtro.")
    
    # Se nenhum dado passar pelo filtro, retorna um DataFrame vazio.
    if df_filtrado_1.empty:
        print("Nenhum dado passou pelo primeiro filtro. Retornando DataFrame vazio.")
        return pd.DataFrame()
        
    # Inicializa a coluna de rastreamento do tratamento, marcando todos os dados como 'Original' por padrão.
    df_filtrado_1['status_v04'] = 'Original'

    # --- 2º Filtro: Substituir outliers pela mediana ---
    print("Aplicando o 2º Filtro: Tratamento de outliers.")
    
    # Calcula a mediana da produção para cada grupo. 'transform' alinha o resultado de volta ao DataFrame original.
    medianas_grupo = df_filtrado_1.groupby(group_cols)['Produção (Ton ou hL)'].transform('median')
    # Cria uma máscara booleana para identificar outliers (produção >= 3x a mediana do grupo).
    mascara_outliers = (
        (df_filtrado_1['Produção (Ton ou hL)'] >= 3 * medianas_grupo) |
        (df_filtrado_1['Produção (Ton ou hL)'] <= medianas_grupo / 3)
        )

    # Usa a máscara para atualizar a coluna de rastreamento, marcando os outliers.
    df_filtrado_1.loc[mascara_outliers, 'status_v04'] = 'Outlier substituído'
    
    # Cria uma coluna temporária para armazenar os valores de produção tratados.
    df_filtrado_1['Producao_Tratada'] = df_filtrado_1['Produção (Ton ou hL)']
    # Substitui os valores dos outliers pela mediana do grupo correspondente na coluna tratada.
    df_filtrado_1.loc[mascara_outliers, 'Producao_Tratada'] = medianas_grupo[mascara_outliers]
    
    num_outliers = mascara_outliers.sum()
    print(f"{num_outliers} outliers foram identificados e substituídos.")

    # --- 3º Filtro: Preencher anos faltantes ---
    print("Aplicando o 3º Filtro: Preenchimento de anos faltantes.")

    # Função auxiliar que será aplicada a cada grupo para preencher suas lacunas.
    def _preencher_anos_faltantes(grupo):
        # Define 'num_ano' como índice para permitir a reindexação baseada no tempo.
        grupo = grupo.set_index('num_ano')
        
        # Coleta informações do grupo para definir o range de anos.
        situacao = grupo['SITUACAO CADASTRAL'].iloc[0]
        ano_min = grupo.index.min()
        ano_max_reportado = grupo.index.max()
        ANO_FINAL_ATIVAS = 2024
        
        ''' Estou com dúvida se ele está aplicando o ultimo ano op cada cnpj'''
        # Determina o último ano da série temporal: 2024 para empresas ativas, ou o último ano reportado para as demais.
        if isinstance(situacao, str) and situacao.upper() == 'ATIVA':
            ano_final = ANO_FINAL_ATIVAS
        else:
            ano_final = ano_max_reportado
            
        # Cria um novo índice contendo todos os anos, do início ao fim da série.
        novo_indice = pd.Index(range(ano_min, ano_final + 1), name='num_ano')
        # Reindexa o grupo. Isso cria novas linhas com valores NaN para os anos que estavam faltando.
        grupo_completo = grupo.reindex(novo_indice)
        
        # As novas linhas criadas terão NaN na coluna 'status_v04'. Preenchemos com a marcação correta.
        grupo_completo['status_v04'] = grupo_completo['status_v04'].fillna('Dado preenchido')
        
        # --- LÓGICA DA MEDIANA REFINADA ---
        # 1. Filtra o grupo original para pegar apenas os dados que não foram classificados como outliers.
        grupo_sem_outliers = grupo[grupo['status_v04'] == 'Original']
        
        # 2. Calcula a mediana "refinada" com base nesses dados 'limpos'.
        #    Se um grupo for composto apenas de outliers (caso raro), usa a mediana do grupo todo como fallback.
        if not grupo_sem_outliers.empty:
            mediana_para_preenchimento = grupo_sem_outliers['Producao_Tratada'].median()
        else:
            mediana_para_preenchimento = grupo['Producao_Tratada'].median() # Fallback

        # 3. Usa essa mediana refinada para preencher os valores de produção das novas linhas (que eram NaN).
        grupo_completo['Producao_Tratada'] = grupo_completo['Producao_Tratada'].fillna(mediana_para_preenchimento)
        # --- FIM DA LÓGICA ---
        
        # Atualiza a coluna de produção final com os valores tratados e preenchidos.
        grupo_completo['Produção (Ton ou hL)'] = grupo_completo['Producao_Tratada']
        
        # Preenche os valores das outras colunas para as novas linhas (ex: CNPJ, situação) usando os valores existentes.
        # Retorna o índice 'num_ano' para ser uma coluna novamente.
        return grupo_completo.ffill().bfill().reset_index()

    # Aplica a função de preenchimento a cada grupo do DataFrame.
    df_final = df_filtrado_1.groupby(group_cols, group_keys=False).apply(_preencher_anos_faltantes)
    
    # Remove a coluna temporária de produção tratada, pois já foi copiada para a coluna final.
    df_final.drop(columns=['Producao_Tratada'], inplace=True)
    # Garante que a coluna de ano seja do tipo inteiro.
    df_final['num_ano'] = df_final['num_ano'].astype(int)
    
    print(f"Processo finalizado. O DataFrame final contém {len(df_final)} linhas.")
    
    # Reseta o índice do DataFrame final para ser uma sequência limpa (0, 1, 2, ...).
    return df_final.reset_index(drop=True)

#%% novo trat outliers

def tratamento_outliers_V2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza um tratamento de outliers e preenchimento de dados de forma robusta.
    Agora:
    - Etapa 4 e 4b preenchem os dados fixos (status, etc.) de forma segura,
      preservando o histórico e evitando sobrescrever dados existentes.
    """

    print("Iniciando o tratamento de dados (Ordem: Corrigir > Preencher)...")

    # --- 1. Pré-processamento e Validação ---
    colunas_necessarias = [
        'mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto',
        'num_ano', 'Produção (Ton ou hL)', 'SITUACAO CADASTRAL'
    ]
    for col in colunas_necessarias:
        if col not in df.columns:
            raise ValueError(f"A coluna '{col}' não foi encontrada.")
    group_cols = ['mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto']
    agg_cols = group_cols + ['num_ano']
    if df.duplicated(subset=agg_cols).any():
        print("Consolidando registros duplicados (usando 'mean')...")
        df = df.groupby(agg_cols, as_index=False).agg({
            'Produção (Ton ou hL)': 'mean',
            **{c: 'first' for c in df.columns if c not in agg_cols and c != 'Produção (Ton ou hL)'}
        })

    # --- 2. Filtro de Histórico de Reporte ---
    print("Etapa 2: Filtrando séries com histórico insuficiente...")
    def _verificar_historico_suficiente(anos_serie: pd.Series) -> bool:
        anos_unicos = sorted(anos_serie.unique())
        if len(anos_unicos) >= 5: return True
        if len(anos_unicos) >= 3:
            for i in range(len(anos_unicos) - 2):
                if (anos_unicos[i+1] - anos_unicos[i] == 1) and (anos_unicos[i+2] - anos_unicos[i+1] == 1):
                    return True
        return False
    df_filtrado = df.groupby(group_cols).filter(lambda x: _verificar_historico_suficiente(x['num_ano'])).copy()
    if df_filtrado.empty:
        print("Nenhum dado passou pelo filtro de histórico."); return pd.DataFrame()
    print(f"{len(df)} -> {len(df_filtrado)} linhas após filtro de histórico.")
    df_filtrado['status_v04'] = 'Original'

    # --- 3. Correção Automática (Outliers Extremos - IQR 3.0) ---
    print("Etapa 3: Corrigindo outliers extremos nos dados existentes...")
    df_para_corrigir = df_filtrado.copy()
    df_para_corrigir['vizinho_anterior'] = df_para_corrigir.groupby(group_cols)['Produção (Ton ou hL)'].transform(lambda x: x.replace(0, np.nan).shift(1))
    df_para_corrigir['vizinho_posterior'] = df_para_corrigir.groupby(group_cols)['Produção (Ton ou hL)'].transform(lambda x: x.replace(0, np.nan).shift(-1))
    fator_iqr_extremo = 3.0
    Q1 = df_para_corrigir.groupby(group_cols)['Produção (Ton ou hL)'].transform(lambda x: x[x > 0].quantile(0.25))
    Q3 = df_para_corrigir.groupby(group_cols)['Produção (Ton ou hL)'].transform(lambda x: x[x > 0].quantile(0.75))
    IQR = Q3 - Q1
    lim_sup = Q3 + (fator_iqr_extremo * IQR)
    lim_inf = Q1 - (fator_iqr_extremo * IQR)
    mascara_normal = ((df_para_corrigir['Produção (Ton ou hL)'] > lim_sup) | (df_para_corrigir['Produção (Ton ou hL)'] < lim_inf)) & (IQR > 0)
    mascara_iqr_zero = (df_para_corrigir['Produção (Ton ou hL)'] != Q1) & (IQR == 0)
    mascara_extremos = (mascara_normal | mascara_iqr_zero) & (df_para_corrigir['status_v04'] == 'Original')
    if mascara_extremos.sum() > 0:
        print(f"Encontrados {mascara_extremos.sum()} outliers extremos. Corrigindo com mediana de vizinhos...")
        valores_substitutos = df_para_corrigir[['vizinho_anterior', 'vizinho_posterior']].median(axis=1)
        df_para_corrigir.loc[mascara_extremos, 'Produção (Ton ou hL)'] = valores_substitutos[mascara_extremos]
        df_para_corrigir.loc[mascara_extremos, 'status_v04'] = f'Outlier Extremo Corrigido (IQR {fator_iqr_extremo}x)'
    else:
        print("Nenhum outlier extremo encontrado para correção automática.")
    df_apos_correcao = df_para_corrigir.drop(columns=['vizinho_anterior', 'vizinho_posterior'])

    # --- 4. Preenchimento Local (min–max da série) ---
    print("Etapa 4: Preenchendo dados faltantes no intervalo local...")
    def _preencher_serie(grupo):
        grupo = grupo.set_index('num_ano').sort_index()
        mediana_grupo_temp = grupo['Produção (Ton ou hL)'].median()
        ano_min, ano_max = grupo.index.min(), grupo.index.max()
        grupo_completo = grupo.reindex(range(ano_min, ano_max + 1))
        linhas_preenchidas = grupo_completo['Produção (Ton ou hL)'].isna()
        grupo_completo.loc[linhas_preenchidas, 'status_v04'] = 'Dado preenchido (local)'
        grupo_completo['Produção (Ton ou hL)'] = grupo_completo['Produção (Ton ou hL)'].fillna(mediana_grupo_temp)
        
        # *** LÓGICA DE PREENCHIMENTO CORRIGIDA para preservar o histórico ***
        # Pega as colunas que não devem mudar ano a ano
        cols_fixas = [col for col in grupo.columns if col not in ['Produção (Ton ou hL)', 'status_v04']]
        # Preenche os NaNs das novas linhas usando o último valor válido (ffill) e o próximo (bfill)
        grupo_completo[cols_fixas] = grupo_completo[cols_fixas].ffill().bfill()

        return grupo_completo.reset_index()

    df_preenchido = df_apos_correcao.groupby(group_cols, group_keys=False).apply(_preencher_serie)

    # --- 4b. Ajuste por Situação Cadastral ---
    ano_min_geral = df_preenchido['num_ano'].min()
    ano_max_geral = df_preenchido['num_ano'].max()
    total_anos_possiveis = ano_max_geral - ano_min_geral + 1

    def _aplicar_logica_cadastral(grupo, total_anos_possiveis):
        status_cadastral = grupo['SITUACAO CADASTRAL'].iloc[0]
        if status_cadastral == 'Cadastramento indevido':
            grupo['Produção (Ton ou hL)'] = 0
            grupo['status_v04'] = 'Zerado (Cad. Indevido)'
            return grupo
        elif status_cadastral == 'Ativa':
            num_pontos_validos = grupo[grupo['Produção (Ton ou hL)'].notna()].shape[0]
            e_densa = (num_pontos_validos / total_anos_possiveis) >= 0.75
            if not e_densa: return grupo

            grupo = grupo.set_index('num_ano').sort_index()
            grupo_completo = grupo.reindex(range(ano_min_geral, ano_max_geral + 1))
            mascara_preencher = grupo_completo['Produção (Ton ou hL)'].isna()
            grupo_completo['Produção (Ton ou hL)'] = grupo_completo['Produção (Ton ou hL)'].interpolate(method='linear', limit_direction='both')
            mediana_serie = grupo_completo['Produção (Ton ou hL)'].median()
            grupo_completo['Produção (Ton ou hL)'].fillna(mediana_serie, inplace=True)
            grupo_completo.loc[mascara_preencher, 'status_v04'] = 'Preenchido (Série Ativa - Global)'
            
            # *** MESMA LÓGICA DE PREENCHIMENTO CORRIGIDA APLICADA AQUI ***
            cols_fixas = [col for col in grupo.columns if col not in ['Produção (Ton ou hL)', 'status_v04']]
            grupo_completo[cols_fixas] = grupo_completo[cols_fixas].ffill().bfill()

            return grupo_completo.reset_index()
        else:
            return grupo

    df_final = df_preenchido.groupby(group_cols, group_keys=False).apply(lambda g: _aplicar_logica_cadastral(g, total_anos_possiveis))

    # --- 5. Sinalização de Outliers Moderados ---
    print("Etapa 5: Sinalizando outliers moderados para revisão...")
    Q1_mod = df_final.groupby(group_cols)['Produção (Ton ou hL)'].transform(lambda x: x[x > 0].quantile(0.25))
    Q3_mod = df_final.groupby(group_cols)['Produção (Ton ou hL)'].transform(lambda x: x[x > 0].quantile(0.75))
    IQR_mod = Q3_mod - Q1_mod
    lim_sup_mod = Q3_mod + (1.5 * IQR_mod)
    lim_inf_mod = Q1_mod - (1.5 * IQR_mod)
    mascara_normal_mod = ((df_final['Produção (Ton ou hL)'] > lim_sup_mod) | (df_final['Produção (Ton ou hL)'] < lim_inf_mod)) & (IQR_mod > 0)
    mascara_iqr_zero_mod = (df_final['Produção (Ton ou hL)'] != Q1_mod) & (IQR_mod == 0)
    mascara_moderados = (mascara_normal_mod | mascara_iqr_zero_mod) & (df_final['status_v04'].isin(['Original', 'Dado preenchido (local)', 'Preenchido (Série Ativa - Global)']))
    df_final['outlier_iq1,5_verif'] = mascara_moderados
    df_final['limite_sup_revisao'] = lim_sup_mod
    df_final['limite_inf_revisao'] = lim_inf_mod
    num_sinalizados = df_final['outlier_iq1,5_verif'].sum()
    if num_sinalizados > 0: print(f"Sinalizados {num_sinalizados} outliers moderados para sua análise.")
    else: print("Nenhum outlier moderado encontrado para sinalização.")

    # --- 6. Finalização ---
    print(f"Processo finalizado. O DataFrame final contém {len(df_final)} linhas.")
    df_final['num_ano'] = df_final['num_ano'].astype(int)
    if 'cod_produto' in df_final.columns: df_final['cod_produto'] = df_final['cod_produto'].astype(str)
    
    return df_final.reset_index(drop=True)


def verif_outliers_manual(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica correções em um DataFrame de produção com base em uma coluna de verificação manual.

    A função opera em três etapas principais:
    1. Exclui séries temporais inteiras marcadas como "Suspeito".
    2. Mantém os dados marcados como "Dado coerente".
    3. Corrige os dados marcados como "Dado incoerente" com base na contagem
       de ocorrências dentro de cada série temporal (CNPJ, município, produto).

    Args:
        df (pd.DataFrame): O DataFrame contendo os dados de produção e a coluna
                           de verificação manual. Deve conter as colunas:
                           'mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto',
                           'num_ano', 'Produção (Ton ou hL)', e 'status_v06'.

    Returns:
        pd.DataFrame: Um novo DataFrame com as correções aplicadas, contendo
                      as colunas adicionais 'Produção (Ton ou hL)_Revisado' e 'status_v07'.
    """
    print("Iniciando a aplicação de correções manuais (função verif_outliers_manual)...")

    colunas_necessarias = [
        'mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto', 'num_ano',
        'Produção (Ton ou hL)', 'status_v06'
    ]
    for col in colunas_necessarias:
        if col not in df.columns:
            raise ValueError(f"Erro: A coluna obrigatória '{col}' não foi encontrada no DataFrame.")

    df_processado = df.copy()
    group_cols = ['mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto']

    # --- REGRA 2: Excluir séries inteiras marcadas como "Suspeito" ---
    mascara_suspeitos = df_processado['status_v06'].str.startswith('Suspeito', na=False)
    if mascara_suspeitos.any():
        print("Identificando séries marcadas como 'Suspeito' para exclusão...")
        grupos_a_excluir = df_processado[mascara_suspeitos][group_cols].drop_duplicates()
        n_grupos_excluidos = len(grupos_a_excluir)
        
        # *** ALTERAÇÃO AQUI: Dando um nome único à coluna indicadora ***
        indicator_col_name = 'origem_merge' 
        df_merged = df_processado.merge(grupos_a_excluir, on=group_cols, how='left', indicator=indicator_col_name)
        
        n_linhas_antes = len(df_processado)
        # *** ALTERAÇÃO AQUI: Usando o novo nome da coluna para filtrar e remover ***
        df_processado = df_merged[df_merged[indicator_col_name] == 'left_only'].drop(columns=[indicator_col_name])
        n_linhas_depois = len(df_processado)

        print(f"-> {n_grupos_excluidos} séries foram completamente removidas ({n_linhas_antes - n_linhas_depois} linhas).")
    else:
        print("-> Nenhuma série marcada como 'Suspeito' foi encontrada.")

    # --- Preparação das novas colunas no DataFrame filtrado ---
    df_processado['Produção (Ton ou hL)_Revisado'] = df_processado['Produção (Ton ou hL)']
    df_processado['status_v07'] = 'Dado original'

    # --- REGRA 3: Corrigir dados marcados como "Dado incoerente" ---
    print("Processando correções para dados marcados como 'Dado incoerente'...")

    def _aplicar_correcoes_grupo(grupo):
        grupo = grupo.sort_values(by='num_ano')
        mascara_incoerente = grupo['status_v06'].str.startswith('Dado incoerente', na=False)
        n_incoerentes = mascara_incoerente.sum()

        if n_incoerentes == 0:
            return grupo

        elif n_incoerentes == 1:
            idx_incoerente = grupo[mascara_incoerente].index[0]
            vizinho_anterior = grupo['Produção (Ton ou hL)_Revisado'].shift(1)
            vizinho_posterior = grupo['Produção (Ton ou hL)_Revisado'].shift(-1)
            val_anterior = vizinho_anterior.loc[idx_incoerente]
            val_posterior = vizinho_posterior.loc[idx_incoerente]
            valor_substituto = np.nanmean([val_anterior, val_posterior])
            grupo.loc[idx_incoerente, 'Produção (Ton ou hL)_Revisado'] = valor_substituto
            grupo.loc[idx_incoerente, 'status_v07'] = 'Corrigido (média dos vizinhos)'

        else: 
            mascara_coerente = ~mascara_incoerente
            mediana_coerente = grupo.loc[mascara_coerente, 'Produção (Ton ou hL)'].median()
            if pd.isna(mediana_coerente):
                mediana_coerente = 0
            grupo.loc[mascara_incoerente, 'Produção (Ton ou hL)_Revisado'] = mediana_coerente
            grupo.loc[mascara_incoerente, 'status_v07'] = 'Corrigido (mediana da série)'
            
        return grupo

    df_final = df_processado.groupby(group_cols, group_keys=False).apply(_aplicar_correcoes_grupo)
    
    n_corrigidos_media = (df_final['status_v07'] == 'Corrigido (média dos vizinhos)').sum()
    n_corrigidos_mediana = (df_final['status_v07'] == 'Corrigido (mediana da série)').sum()
    print(f"-> {n_corrigidos_media} registros corrigidos com a média dos vizinhos.")
    print(f"-> {n_corrigidos_mediana} registros corrigidos com a mediana da série.")
    
    print("Processo de correção manual finalizado.")
    return df_final.reset_index(drop=True)

def sinalizar_variacoes_producao(
    df: pd.DataFrame,
    fator_mediana: float = 2.0,
    fator_aumento_anual: float = 2,
    fator_reducao_anual: float = 0.5
) -> pd.DataFrame:
    """
    Cria duas colunas de sinalização (flags) para identificar variações atípicas de produção.

    Esta função implementa duas lógicas de verificação:
    1. Desvio da Mediana: Compara a produção de um ano com a mediana de toda a
       série temporal do grupo, sinalizando valores X vezes maiores ou menores.
    2. Variação Anual: Compara a produção de um ano com a do ano anterior,
       sinalizando saltos ou quedas bruscas.

    Args:
        df (pd.DataFrame): DataFrame de entrada. Deve conter as colunas:
                           'mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto',
                           'num_ano', e 'Produção (Ton ou hL)'.
        fator_mediana (float, optional): Multiplicador para a verificação da mediana.
                                         Sinaliza se producao > mediana * fator ou
                                         producao < mediana / fator. Padrão 5.0.
        fator_aumento_anual (float, optional): Fator para detectar saltos anuais.
                                               Sinaliza se producao_atual / producao_anterior > fator.
                                               Padrão 10.0 (aumento de 10x).
        fator_reducao_anual (float, optional): Fator para detectar quedas anuais.
                                               Sinaliza se producao_atual / producao_anterior < fator.
                                               Padrão 0.1 (queda de 90%).

    Returns:
        pd.DataFrame: O DataFrame original com duas novas colunas booleanas:
                      'flag_desvio_mediana' e 'flag_variacao_anual'.
    """
    print("Iniciando a sinalização automática de variações de produção...")
    df_sinalizado = df.copy()

    # --- Definição das colunas chave ---
    group_cols = ['mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto']
    value_col = 'Produção (Ton ou hL)_Revisado'
    year_col = 'num_ano'

    # --- 1. Verificação do Desvio da Mediana da Série ---
    print(f"-> Verificando desvios da mediana com fator {fator_mediana}x...")
    
    # Calcula a mediana para cada grupo e a expande para todas as linhas do grupo
    medianas_grupo = df_sinalizado.groupby(group_cols)[value_col].transform('median')
    
    # Define os limites superior e inferior com base no fator
    limite_superior = medianas_grupo * fator_mediana
    limite_inferior = medianas_grupo / fator_mediana
    
    # A verificação só é aplicada onde a mediana é positiva para evitar divisões por zero ou resultados estranhos
    mascara_mediana = (
        (df_sinalizado[value_col] > limite_superior) |
        (df_sinalizado[value_col] < limite_inferior)
    ) & (medianas_grupo > 0)
    
    df_sinalizado['flag_desvio_mediana'] = mascara_mediana

    # --- 2. Verificação da Variação Anual ---
    print(f"-> Verificando variações anuais maiores que {fator_aumento_anual}x ou menores que {fator_reducao_anual}x...")
    
    # É essencial ordenar por ano dentro de cada grupo para a lógica de 'ano anterior'
    df_sinalizado = df_sinalizado.sort_values(by=group_cols + [year_col])
    
    # Pega o valor da produção do ano anterior para cada registro dentro do seu grupo
    producao_anterior = df_sinalizado.groupby(group_cols)[value_col].shift(1)
    
    # Evita divisão por zero substituindo 0 por NaN (que será ignorado nos cálculos)
    producao_anterior_safe = producao_anterior.replace(0, np.nan)
    
    # Calcula a razão entre o ano atual e o anterior
    razao_anual = df_sinalizado[value_col] / producao_anterior_safe
    
    # O primeiro ano de cada série terá razão NaN, que resulta em False (correto)
    mascara_anual = (razao_anual > fator_aumento_anual) | (razao_anual < fator_reducao_anual)
    
    df_sinalizado['flag_variacao_anual'] = mascara_anual

    # --- Finalização ---
    num_flags_mediana = df_sinalizado['flag_desvio_mediana'].sum()
    num_flags_anual = df_sinalizado['flag_variacao_anual'].sum()
    print(f"Finalizado. {num_flags_mediana} registros sinalizados por desvio da mediana.")
    print(f"           {num_flags_anual} registros sinalizados por variação anual.")
    
    return df_sinalizado

def sinalizar_variacoes_producao_v2(
    df: pd.DataFrame,
    janela_movel: int = 5,
    fator_mediana: float = 3.0, 
    fator_aumento_anual: float = 2.0,
    fator_reducao_anual: float = 0.5
) -> pd.DataFrame:
    """
    Sinaliza variações com MEDIANA MÓVEL SEM LAG para aceitar novos patamares. (V3)
    """
    print("Iniciando a sinalização V3 (sem lag)...")
    df_sinalizado = df.copy()

    group_cols = ['mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto']
    value_col = 'Produção (Ton ou hL)'
    year_col = 'num_ano'

    df_sinalizado = df_sinalizado.sort_values(by=group_cols + [year_col])

    # --- 1. Verificação com Mediana Móvel SEM LAG ---
    # A ÚNICA MUDANÇA ESTÁ AQUI: REMOÇÃO DO .shift(1)
    medianas_moveis = df_sinalizado.groupby(group_cols)[value_col].transform(
        lambda x: x.rolling(window=janela_movel, min_periods=1, center=True).median()
    )
    # Usar center=True cria uma referência ainda mais justa com o passado e futuro.
    
    limite_superior = medianas_moveis * fator_mediana
    limite_inferior = medianas_moveis / fator_mediana
    
    # A mascara compara o valor com a sua própria janela móvel
    mascara_mediana = (
        (df_sinalizado[value_col] > limite_superior) |
        (df_sinalizado[value_col] < limite_inferior)
    ) & (medianas_moveis > 0)
    
    df_sinalizado['flag_desvio_mediana'] = mascara_mediana

    # --- 2. Verificação da Variação Anual (ótima para picos) ---
    producao_anterior = df_sinalizado.groupby(group_cols)[value_col].shift(1)
    producao_anterior_safe = producao_anterior.replace(0, np.nan)
    razao_anual = df_sinalizado[value_col] / producao_anterior_safe
    
    mascara_anual = (razao_anual > fator_aumento_anual) | (razao_anual < fator_reducao_anual)
    df_sinalizado['flag_variacao_anual'] = mascara_anual

    print("Sinalização V3 finalizada.")
    return df_sinalizado

def verif_outliers_manual_v02(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica uma segunda camada de correções automáticas em dados de produção.

    Esta função é projetada para rodar após uma etapa de sinalização (flags).
    Ela corrige os pontos sinalizados usando dois métodos sequenciais:
    1. A média dos valores vizinhos (ano anterior e posterior).
    2. A mediana dos valores estáveis da própria série temporal.

    Se nenhum dos métodos conseguir produzir um valor de correção válido,
    o valor original é mantido.

    Args:
        df (pd.DataFrame): DataFrame que já contém as colunas de 'flag' e uma
                           coluna de produção já revisada (ex: '_Revisado').

    Returns:
        pd.DataFrame: DataFrame com as correções aplicadas em novas colunas
                      ('_Revisado_V2' e 'status_v08_auto').
    """
    print("Iniciando a segunda camada de correção automática...")

    df_processado = df.copy()
    group_cols = ['mv.num_cpf_cnpj', 'mv.nom_municipio', 'cod_produto']

    # --- Preparação das colunas de resultado ---
    df_processado['Produção (Ton ou hL)_Revisado_V2'] = df_processado['Produção (Ton ou hL)_Revisado']
    df_processado['status_v08_auto'] = df_processado['status_v07']

    # --- Lógica de Correção Automática ---
    print("-> Processando correções com base nas flags...")
    def _aplicar_correcoes_grupo(grupo):
        grupo = grupo.sort_values(by='num_ano')
        
        mascara_correcao = (grupo['flag_desvio_mediana'] == True) | (grupo['flag_variacao_anual'] == True)
        n_a_corrigir = mascara_correcao.sum()

        if n_a_corrigir == 0:
            return grupo
        
        for idx_corrigir in grupo[mascara_correcao].index:
            valor_substituto = np.nan
            status_correcao = ""

            # --- Método 1: Tenta corrigir com a média dos vizinhos ---
            vizinho_anterior = grupo['Produção (Ton ou hL)_Revisado_V2'].shift(1).loc[idx_corrigir]
            vizinho_posterior = grupo['Produção (Ton ou hL)_Revisado_V2'].shift(-1).loc[idx_corrigir]
            
            if not np.isnan(vizinho_anterior) or not np.isnan(vizinho_posterior):
                valor_substituto = np.nanmean([vizinho_anterior, vizinho_posterior])
                status_correcao = "Corrigido Auto (média vizinhos)"
            
            # --- Método 2: Se o anterior falhou, tenta com a mediana da série ---
            if np.isnan(valor_substituto):
                mascara_estavel = ~mascara_correcao
                if mascara_estavel.sum() > 0:
                    mediana_estavel = grupo.loc[mascara_estavel, 'Produção (Ton ou hL)_Revisado_V2'].median()
                    if not pd.isna(mediana_estavel):
                        valor_substituto = mediana_estavel
                        status_correcao = "Corrigido Auto (mediana série)"

            # Aplica a correção APENAS se um valor substituto válido foi encontrado
            if not np.isnan(valor_substituto):
                grupo.loc[idx_corrigir, 'Produção (Ton ou hL)_Revisado_V2'] = valor_substituto
                grupo.loc[idx_corrigir, 'status_v08_auto'] = status_correcao
            
        return grupo

    df_final = df_processado.groupby(group_cols, group_keys=False).apply(_aplicar_correcoes_grupo)
    
    # Garante que nenhum valor nulo surja de uma correção falha
    df_final = df_final.fillna({'Produção (Ton ou hL)_Revisado_V2': 0})
    
    print("Processo de correção finalizado.")
    return df_final.reset_index(drop=True)

import pandas as pd
import numpy as np

import pandas as pd
import numpy as np
import pandas as pd
import numpy as np # Adicionado import que estava faltando

def tratamento_outliers_v3(
    df: pd.DataFrame,
    janela_movel: int = 5,
    fator_mediana: float = 3.0,
    fator_aumento_anual: float = 2.0,
    fator_reducao_anual: float = 0.5
) -> pd.DataFrame:
    """
    Realiza um tratamento completo e rastreável de séries temporais de produção.

    O processo é dividido em etapas sequenciais, onde cada etapa principal gera uma
    nova coluna de produção (`prodtonhl_vX`) e uma coluna de status (`status_v0X`),
    permitindo uma auditoria clara das transformações aplicadas a cada registro.

    Etapas:
    1.  **Preparação (v1):**
        - `prodtonhl_v1`: Coluna de produção original, após consolidação de duplicatas.
    2.  **Filtragem (v2):**
        - `prodtonhl_v2`: Resultado da aplicação de filtros. Séries com histórico
          insuficiente ou inteiramente zeradas são marcadas e seus valores zerados
          nesta coluna, sendo excluídas das etapas seguintes.
        - `status_v06`: Descreve o resultado da Etapa 1 (ex: 'Apto para Análise',
          'Histórico Insuficiente', 'Série Zerada').
    3.  **Correção de Outliers (v3):**
        - `prodtonhl_v3`: Resultado da correção de outliers identificados em `v2`.
          Os outliers são substituídos por valores interpolados/extrapolados.
        - `status_v07`: Descreve a correção aplicada (ex: 'Valor Mantido',
          'Corrigido (Interpolação)').
    4.  **Preenchimento de Lacunas (v4):**
        - `prodtonhl_v4`: Versão final da série temporal, com anos faltantes
          preenchidos com base no status cadastral e na densidade de dados.
        - `status_v08`: Descreve o preenchimento (ex: 'Valor Mantido',
          'Preenchido (Global - Interpolação)').

    Args:
        df: DataFrame contendo os dados de produção. Deve incluir colunas de
            agrupamento, ano, valor de produção e situação cadastral.
        janela_movel: Tamanho da janela para cálculo da mediana móvel (detecção de outliers).
        fator_mediana: Fator multiplicativo para definir os limites da mediana móvel.
        fator_aumento_anual: Fator máximo de aumento anual permitido.
        fator_reducao_anual: Fator máximo de redução anual permitido.

    Returns:
        Um DataFrame com as colunas originais e as novas colunas de produção e status,
        refletindo cada etapa do tratamento.
    """
    print("Iniciando o tratamento de dados v4 (Etapas em Colunas)...")

    # --- 0. PREPARAÇÃO E VALIDAÇÃO ---
    df_processado = df.copy()
    group_cols = ['CNPJ', 'MUNICIPIO', 'cod_produto']
    year_col = 'num_ano'
    
    colunas_essenciais = group_cols + [year_col, 'prodtonhl_v1', 'SITUACAO CADASTRAL']
    for col in colunas_essenciais:
        if col not in df_processado.columns:
            raise ValueError(f"A coluna necessária '{col}' não foi encontrada no DataFrame.")

    agg_cols = group_cols + [year_col]
    if df_processado.duplicated(subset=agg_cols).any():
        print("-> Consolidando registros duplicados (usando 'mean')...")
        agg_dict = {'prodtonhl_v1': 'mean'}
        other_cols = {c: 'first' for c in df.columns if c not in agg_cols and c != 'prodtonhl_v1'}
        agg_dict.update(other_cols)
        df_processado = df_processado.groupby(agg_cols, as_index=False).agg(agg_dict)

    # --- [ADIÇÃO] Armazena a contagem total de linhas após a consolidação ---
    total_inicial_consolidado = len(df_processado)
    print(f"-> {total_inicial_consolidado} registros únicos (consolidados) para processar.")
    # --- [FIM ADIÇÃO] ---

    df_processado['prodtonhl_v2'] = df_processado['prodtonhl_v1']
    df_processado['status_v06'] = 'Apto para Análise'
    df_processado['status_v07'] = ''
    df_processado['status_v08'] = ''

    # --- 1. FILTRAGEM (Cria prodtonhl_v2 e status_v06) ---
    print("Etapa 1: Aplicando filtros (cria prodtonhl_v2)...")

    def _verificar_historico_suficiente(grupo: pd.DataFrame) -> bool:
        anos = sorted(grupo[year_col].unique())
        if len(anos) >= 5: return True
        if len(anos) >= 3:
            for i in range(len(anos) - 2):
                if anos[i+1] - anos[i] == 1 and anos[i+2] - anos[i+1] == 1:
                    return True
        return False

    ids_suficientes = df_processado.groupby(group_cols).filter(_verificar_historico_suficiente)
    mascara_insuficiente = ~df_processado.index.isin(ids_suficientes.index)
    
    df_processado.loc[mascara_insuficiente, 'status_v06'] = 'Histórico Insuficiente'
    df_processado.loc[mascara_insuficiente, 'prodtonhl_v2'] = 0
    
    df_filtrado = df_processado[~mascara_insuficiente].copy()
    
    if not df_filtrado.empty:
        somas_grupo = df_filtrado.groupby(group_cols)['prodtonhl_v1'].transform('sum')
        mascara_zerada = somas_grupo == 0
        indices_zerados = df_filtrado[mascara_zerada].index
        
        df_processado.loc[indices_zerados, 'status_v06'] = 'Série Zerada'
        df_processado.loc[indices_zerados, 'prodtonhl_v2'] = 0
        
        df_filtrado = df_filtrado[~mascara_zerada]

    # --- [RESUMO ETAPA 1] ---
    try:
        total_s1 = len(df_processado) # É igual a total_inicial_consolidado
        aptos_s1 = (df_processado['status_v06'] == 'Apto para Análise').sum()
        filtrados_s1 = total_s1 - aptos_s1
        perc_aptos = (aptos_s1 / total_s1) * 100 if total_s1 > 0 else 0
        
        print("\n--- Resumo Etapa 1 (Filtragem) ---")
        print(f"Total de linhas processadas: {total_s1}")
        print(f"Linhas Aptas p/ Análise:     {aptos_s1} ({perc_aptos:.2f}%)")
        print(f"Linhas Filtradas:            {filtrados_s1}")
        print(f"  - Histórico Insuficiente:  {(df_processado['status_v06'] == 'Histórico Insuficiente').sum()}")
        print(f"  - Série Zerada:            {(df_processado['status_v06'] == 'Série Zerada').sum()}")
        print("-----------------------------------\n")
    except Exception as e:
        print(f"*** Erro ao gerar resumo S1: {e} ***")
    # --- [FIM RESUMO ETAPA 1] ---
    
    # --- ETAPA 2: CORREÇÃO DE OUTLIERS (Cria prodtonhl_v3 e status_v07) ---
    print("Etapa 2: Corrigindo outliers (cria prodtonhl_v3)...")

    df_processado['status_v07'] = 'Não Aplicável'
    df_processado['prodtonhl_v3'] = df_processado['prodtonhl_v2']
    df_processado['flag_outlier'] = False

    if not df_filtrado.empty:
        df_filtrado = df_filtrado.sort_values(by=agg_cols)

        # Detecção iterativa: cada passe exclui zeros e outliers já detectados da mediana,
        # resolvendo contaminação de vizinho e medianas distorcidas por erros não detectados.
        max_iter = 10
        flag_outlier = pd.Series(False, index=df_filtrado.index)

        for iter_n in range(max_iter):
            # Mascara zeros e outliers anteriores para não distorcerem os limites
            serie_calc = df_filtrado['prodtonhl_v2'].where(
                (df_filtrado['prodtonhl_v2'] > 0) & (~flag_outlier), np.nan
            )
            df_filtrado['_calc'] = serie_calc

            medianas_moveis = df_filtrado.groupby(group_cols)['_calc'].transform(
                lambda x: x.rolling(window=janela_movel, min_periods=1, center=True).median()
            )
            lim_sup = medianas_moveis * fator_mediana
            lim_inf  = medianas_moveis / fator_mediana

            prod_ant = df_filtrado.groupby(group_cols)['_calc'].transform(lambda x: x.shift(1))
            razao    = serie_calc / prod_ant

            m_mediana = (
                ((df_filtrado['prodtonhl_v2'] > lim_sup) | (df_filtrado['prodtonhl_v2'] < lim_inf))
                & (medianas_moveis > 0)
                & (df_filtrado['prodtonhl_v2'] > 0)  # zeros já são erros confirmados, não outliers
            )
            m_anual = (razao > fator_aumento_anual) | (razao < fator_reducao_anual)

            new_flags = (m_mediana | m_anual) & (df_filtrado['status_v06'] == 'Apto para Análise')

            if new_flags.equals(flag_outlier):
                print(f"  -> Detecção estabilizou em {iter_n + 1} passada(s).")
                break

            flag_outlier = new_flags

        df_filtrado.drop(columns=['_calc'], inplace=True, errors='ignore')
        df_filtrado['flag_outlier'] = flag_outlier
        print(f"-> {df_filtrado['flag_outlier'].sum()} outliers sinalizados para correção.")

        df_filtrado['status_v07'] = 'Valor Mantido'

        def _corrigir_outliers(grupo):
            pontos_validos = grupo[~grupo['flag_outlier'] & (grupo['prodtonhl_v2'] > 0)]
            mediana_serie = pontos_validos['prodtonhl_v2'].median()

            for idx in grupo[grupo['flag_outlier']].index:
                ano_outlier = grupo.loc[idx, year_col]
                v = grupo.loc[idx, 'prodtonhl_v2']

                # Zeros confirmados: viram NaN para Stage 3 interpolar
                if v == 0:
                    grupo.loc[idx, 'prodtonhl_v3'] = np.nan
                    grupo.loc[idx, 'status_v07'] = 'Corrigido (Zero confirmado → NaN)'
                    continue

                # Testa fator ÷1000 e ×1000: se recoloca na faixa da série, é erro de unidade
                if pd.notna(mediana_serie) and mediana_serie > 0:
                    if mediana_serie / fator_mediana <= v / 1000 <= mediana_serie * fator_mediana:
                        grupo.loc[idx, 'prodtonhl_v3'] = v / 1000
                        grupo.loc[idx, 'status_v07'] = 'Corrigido (Erro Unidade ÷1000)'
                        continue
                    if mediana_serie / fator_mediana <= v * 1000 <= mediana_serie * fator_mediana:
                        grupo.loc[idx, 'prodtonhl_v3'] = v * 1000
                        grupo.loc[idx, 'status_v07'] = 'Corrigido (Erro Unidade ×1000)'
                        continue

                # Nenhum fator de escala encaixa → erro de digitação → interpola/extrapola/mediana
                validos_ant = pontos_validos[pontos_validos[year_col] < ano_outlier]
                validos_pos = pontos_validos[pontos_validos[year_col] > ano_outlier]
                valor_sub, metodo = np.nan, ""

                if not validos_ant.empty and not validos_pos.empty:
                    p_ant, p_pos = validos_ant.iloc[-1], validos_pos.iloc[0]
                    dy, dx = p_pos['prodtonhl_v2'] - p_ant['prodtonhl_v2'], p_pos[year_col] - p_ant[year_col]
                    if dx > 0:
                        valor_sub = p_ant['prodtonhl_v2'] + dy * ((ano_outlier - p_ant[year_col]) / dx)
                        metodo = 'Corrigido (Erro Digitação → Interpolação)'
                elif len(validos_ant) >= 2:
                    p1, p2 = validos_ant.iloc[-1], validos_ant.iloc[-2]
                    dy, dx = p1['prodtonhl_v2'] - p2['prodtonhl_v2'], p1[year_col] - p2[year_col]
                    if dx > 0:
                        valor_sub = p1['prodtonhl_v2'] + (dy/dx) * (ano_outlier - p1[year_col])
                        metodo = 'Corrigido (Erro Digitação → Extrapolação Fwd)'
                elif len(validos_pos) >= 2:
                    p1, p2 = validos_pos.iloc[0], validos_pos.iloc[1]
                    dy, dx = p2['prodtonhl_v2'] - p1['prodtonhl_v2'], p2[year_col] - p1[year_col]
                    if dx > 0:
                        valor_sub = p1['prodtonhl_v2'] + (dy/dx) * (ano_outlier - p1[year_col])
                        metodo = 'Corrigido (Erro Digitação → Extrapolação Bwd)'
                else:
                    if pd.notna(mediana_serie):
                        valor_sub, metodo = mediana_serie, 'Corrigido (Erro Digitação → Mediana Fallback)'

                if pd.notna(valor_sub):
                    grupo.loc[idx, 'prodtonhl_v3'] = max(0, valor_sub)
                    grupo.loc[idx, 'status_v07'] = metodo
            return grupo

        df_corrigido = df_filtrado.groupby(group_cols, group_keys=False).apply(_corrigir_outliers, include_groups=False)
        df_processado.update(df_corrigido)

    # --- [RESUMO ETAPA 2] ---
    try:
        # Base de análise são os aptos da S1
        base_s2 = (df_processado['status_v06'] == 'Apto para Análise').sum() 
        if base_s2 > 0:
            mantidos_s2 = (df_processado['status_v07'] == 'Valor Mantido').sum()
            # Conta qualquer status que comece com 'Corrigido'
            corrigidos_s2 = (df_processado['status_v07'].str.startswith('Corrigido', na=False)).sum()
            perc_corrigidos = (corrigidos_s2 / base_s2) * 100
            
            print("\n--- Resumo Etapa 2 (Outliers) ---")
            print(f"Total de linhas analisadas:     {base_s2} (Aptos da Etapa 1)")
            print(f"Linhas com Valor Mantido:       {mantidos_s2}")
            print(f"Linhas Corrigidas (Outliers): {corrigidos_s2} ({perc_corrigidos:.2f}%)")
            print("--------------------------------------\n")
        else:
             print("\n--- Resumo Etapa 2 (Outliers) ---")
             print("Nenhuma linha apta para análise de outliers.")
             print("--------------------------------------\n")
    except Exception as e:
        print(f"*** Erro ao gerar resumo S2: {e} ***")
    # --- [FIM RESUMO ETAPA 2] ---
    
    # --- ETAPA 3: PREENCHIMENTO DE LACUNAS (Cria prodtonhl_v4 e status_v08) ---
    print("Etapa 3: Preenchendo lacunas (cria prodtonhl_v4)...")
    
    df_processado['status_v08'] = 'Não Aplicável'
    df_processado['prodtonhl_v4'] = df_processado['prodtonhl_v3']
    
    ano_min_geral, ano_max_geral = df_processado[year_col].min(), df_processado[year_col].max()
    
    def _preencher_grupo(grupo, ano_min_g, ano_max_g):
        # --- SETUP INICIAL ---
        status_cadastral = str(grupo['SITUACAO CADASTRAL'].iloc[0])
        num_pontos = grupo.shape[0]
        densidade = num_pontos / (ano_max_g - ano_min_g + 1)
        
        grupo['status_v08'] = 'Valor Mantido'
    
        # --- LÓGICA DE DECISÃO DE PREENCHIMENTO ---
        if status_cadastral.upper() == 'ATIVA' and densidade >= 0.75:
            intervalo = range(ano_min_g, ano_max_g + 1)
            status_preenchimento = 'Preenchido (Global - Interpolação)'
    
        elif status_cadastral.upper().startswith('ENCERRAD') or (status_cadastral.upper() == 'ATIVA' and densidade < 0.75):
            intervalo = range(grupo['num_ano'].min(), grupo['num_ano'].max() + 1)
            status_preenchimento = 'Preenchido (Local - Interpolação)'
        
        else:
            grupo['prodtonhl_v4'] = 0
            grupo['status_v08'] = 'Zerado (Status Inválido/Outro)'
            return grupo
    
        # --- EXECUÇÃO DO PREENCHIMENTO ---
        grupo_reindex = grupo.set_index('num_ano').reindex(intervalo)
        mask_preenchidas = grupo_reindex['prodtonhl_v3'].isna()
        
        grupo_reindex['prodtonhl_v4'] = grupo_reindex['prodtonhl_v3'].interpolate(method='index', limit_direction='both')
        grupo_reindex.loc[mask_preenchidas, 'status_v08'] = status_preenchimento
        
        # --- [Correção] Adicionado 'flag_outlier' para não ser perdido
        static_cols = [c for c in grupo_reindex.columns if c not in ['prodtonhl_v1','prodtonhl_v2','prodtonhl_v3', 'prodtonhl_v4', 'status_v06', 'status_v07', 'status_v08', 'flag_outlier']]
        grupo_reindex[static_cols] = grupo_reindex[static_cols].ffill().bfill().infer_objects(copy=False)
        
        return grupo_reindex.reset_index()

    df_aptos_preenchimento = df_processado[df_processado['status_v06'] == 'Apto para Análise']
    if not df_aptos_preenchimento.empty:
        lista_grupos_preenchidos = []
        for _, grupo in df_aptos_preenchimento.groupby(group_cols):
            grupo_preenchido = _preencher_grupo(grupo, ano_min_geral, ano_max_geral)
            lista_grupos_preenchidos.append(grupo_preenchido)
        
        if lista_grupos_preenchidos:
            df_final_preenchido = pd.concat(lista_grupos_preenchidos, ignore_index=True)
            df_nao_processados = df_processado[df_processado['status_v06'] != 'Apto para Análise'].copy()
            df_processado = pd.concat([df_final_preenchido, df_nao_processados], ignore_index=True)

    # Linhas criadas por preenchimento não têm flag_outlier → False por padrão
    if 'flag_outlier' in df_processado.columns:
        df_processado['flag_outlier'] = df_processado['flag_outlier'].fillna(False)

    # --- [RESUMO ETAPA 3] ---
    try:
        total_s3_saida = len(df_processado)
        mantidos_s3 = (df_processado['status_v08'] == 'Valor Mantido').sum()
        preenchidos_s3 = (df_processado['status_v08'].str.contains('Preenchido', na=False)).sum()
        zerados_s3 = (df_processado['status_v08'] == 'Zerado (Status Inválido/Outro)').sum()
        nao_aplicavel_s3 = (df_processado['status_v08'] == 'Não Aplicável').sum()
        
        print("\n--- Resumo Etapa 3 (Preenchimento) ---")
        print(f"Total de linhas na saída:    {total_s3_saida}")
        print(f"Linhas Originais Mantidas:   {mantidos_s3}")
        print(f"Linhas Novas (Preenchidas):  {preenchidos_s3}")
        print(f"Linhas Zeradas (Status):     {zerados_s3}")
        print(f"Linhas Não Aplicáveis:       {nao_aplicavel_s3} (Filtradas na Etapa 1)")
        print("--------------------------------------\n")
    except Exception as e:
        print(f"*** Erro ao gerar resumo S3: {e} ***")
    # --- [FIM RESUMO ETAPA 3] ---

    # --- FINALIZAÇÃO ---
    
    # --- [RESUMO GERAL - CORRIGIDO] ---
    try:
        total_final = len(df_processado)
        
        # 1. Encontra os dados ORIGINAIS que foram MANTIDOS
        mascara_mantidos = (
            (df_processado['status_v06'] == 'Apto para Análise') &
            (df_processado['status_v07'] == 'Valor Mantido') &
            (df_processado['status_v08'] == 'Valor Mantido')
        )
        qtd_mantidos = mascara_mantidos.sum()
        
        # 2. Pega a contagem de dados ADICIONADOS (da S3)
        qtd_preenchidos = (df_processado['status_v08'].str.contains('Preenchido', na=False)).sum()
        
        # 3. Calcula alterados com base no total INICIAL
        qtd_alterados_originais = total_inicial_consolidado - qtd_mantidos
        
        if total_inicial_consolidado > 0:
            porc_alteracao_original = (qtd_alterados_originais / total_inicial_consolidado) * 100
        else:
            porc_alteracao_original = 0.0
            
        print("\n--- Resumo Geral do Tratamento ---")
        print(f"Qtd Total de dados Iniciais (Consolidados): {total_inicial_consolidado}")
        print(f"  - Qtd Dados Mantidos (Originais):           {qtd_mantidos}")
        print(f"  - Qtd Dados Alterados/Filtrados (Originais):{qtd_alterados_originais}")
        print(f"  - Porcentagem de Alteração (sobre Iniciais):{porc_alteracao_original:.2f} %")
        print(f"Qtd Dados Adicionados (Preenchimento):      {qtd_preenchidos}")
        print(f"Qtd Total de dados (Saída Final):           {total_final}")
        print("-------------------------------------------\n")

    except Exception as e:
        print(f"*** Erro ao gerar resumo GERAL: {e} ***")
    # --- [FIM RESUMO GERAL] ---

    # flag_outlier incluída na saída para auditoria (True = ponto detectado como outlier no Stage 2)
    colunas_finais = [c for c in df.columns if c not in ['prodtonhl_v1', 'flag_outlier']] + \
                     ['prodtonhl_v1', 'prodtonhl_v2', 'prodtonhl_v3', 'prodtonhl_v4',
                      'status_v06', 'status_v07', 'status_v08', 'flag_outlier']
    
    print("Tratamento de dados v4 concluído com sucesso!")
    # Garante que as colunas retornadas estejam na ordem correta
    colunas_existentes = [col for col in colunas_finais if col in df_processado.columns]
    
    return df_processado[colunas_existentes].sort_values(by=agg_cols).reset_index(drop=True)


def import_treat_export_food_code(repo_path):
    raw_dir = os.path.join(repo_path, 'inputs', 'MaterialBaixado')
    processed_dir = os.path.join(repo_path, 'outputs')

    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    xlsx_path = os.path.join(raw_dir, 'CódigosProdutosIBGE.xlsx')

    cod_produto = pd.read_excel(xlsx_path, header=2, dtype={'PRODLIST': str})
    cod_produto = cod_produto[~cod_produto['PRODLIST'].isin(['PRODLIST'])]
    cod_produto = cod_produto[~cod_produto['PRODLIST'].astype(str).str.startswith('CNAE')]
    cod_produto = cod_produto.dropna(subset=['PRODLIST'])
    cod_produto.reset_index(drop=True, inplace=True)

    return cod_produto


def conecta_ibama_ef(df_ibama, df_ef, df_conector):
    df_conector[['PRODLIST', 'NFR', 'Table']] = df_conector[['PRODLIST', 'NFR', 'Table']].astype(str)
    df_ibama['cod_produto'] = df_ibama['cod_produto'].astype(str)

    df_merged = df_ibama.merge(
        df_conector[['PRODLIST', 'NFR', 'Table']],
        left_on='cod_produto',
        right_on='PRODLIST',
        how='left'
    )

    df_final = df_merged.merge(
        df_ef,
        left_on=['NFR', 'Table'],
        right_on=['NFR', 'Table'],
        how='left'
    )

    return df_final