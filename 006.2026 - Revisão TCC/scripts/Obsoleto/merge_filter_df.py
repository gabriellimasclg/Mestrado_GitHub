# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 10:00:21 2025

@author: Gabriel
"""
import pandas as pd
import numpy as np
import warnings


def merge_cnpj_prod(cnpj,prod):
    '''
    
    Processa dados de atividade por CNPJ e Município para obter coordenada,
    código de atividade e código de categoria; Remove os que são CPF   
    
    Parâmetros: 
        - cnpj, prod = Base de dados de CNPJ e de Produção do ibama, e baixado pelas funções:
            -download_ibama_ctf_data
            -ibama_production_data
            
     Retorna:
         pd.DataFrame: DataFrame com os dados concatenados
         
    '''
    
    # Uma mesma indústria com o mesmo cnpj e lat lon produz produtos diferentes
    # Ou seja, multiplos codigo de categoria e codigo de atividade
    # Por isso, fiz um groupby onde os códigos de categoria e atividade viram uma lista para um mesmo CNPJ
    # acabei não utilizando estes códigos, mas mantive por segurança
    cnpj = cnpj.groupby(['CNPJ', 'MUNICIPIO', 'LATITUDE', 'LONGITUDE','ESTADO','SITUACAO CADASTRAL']).agg({
        'CODIGO DA CATEGORIA': list,
        'CODIGO DA ATIVIDADE': list,
        'ANO_INICIO': list,
        'ANO_FIM': list
    }).reset_index()
    
    
    # Fazer o merge com o df_ibama (sem duplicar linhas)
    df_ibama_completo = pd.merge(
        left=cnpj, #tabela unida a esqueda
        right=prod, #tabela unida a direita
        left_on=['CNPJ', 'MUNICIPIO'], #chaves da tabela a esqueda
        right_on=['mv.num_cpf_cnpj', 'mv.nom_municipio'], #chaves da tabela a direita
        how='right', # todas as linhas da tabela da direita (prod) serão mantidas.
        indicator=True #informa a condição da mesclagem (right_only - não estava em CNPJ)
    )
    
       
    return df_ibama_completo


def conecta_ibama_ef(df_ibama, df_ef, df_conector):
    # Garantir que os códigos estejam no mesmo tipo
    df_conector[['PRODLIST', 'NFR', 'Table']] = df_conector[['PRODLIST', 'NFR', 'Table']].astype(str)
    df_ibama['cod_produto'] = df_ibama['cod_produto'].astype(str)
    
    # Mesclar df_ibama com o conector via código do produto
    df_merged = df_ibama.merge(
        df_conector[['PRODLIST', 'NFR', 'Table']],
        left_on='cod_produto',
        right_on='PRODLIST',
        how='left'
    )

    # Realizar o merge com a base ef (EEA)
    df_final = df_merged.merge(
        df_ef,
        left_on=['NFR', 'Table'],
        right_on=['NFR', 'Table'],
        how='left'
    )

    # Remover coluna auxiliar
    df_final = df_final.drop(columns=['PRODLIST'])

    return df_final

import pandas as pd

def conecta_ibama_ef_debug(df_ibama, df_ef, df_conector):
    print("--- INICIANDO FUNÇÃO DE CONEXÃO ---")

    # --- ETAPA 1: Preparação dos Dados ---
    print("\n[ETAPA 1] Preparando os DataFrames...")
    try:
        df_conector[['PRODLIST', 'NFR', 'Table']] = df_conector[['PRODLIST', 'NFR', 'Table']].astype(str).apply(lambda x: x.str.strip())
        df_ibama['cod_produto'] = df_ibama['cod_produto'].astype(str).str.strip()
        # Garante que as colunas de chave em df_ef também sejam string e sem espaços
        df_ef[['NFR', 'Table']] = df_ef[['NFR', 'Table']].astype(str).apply(lambda x: x.str.strip())
        print("Tipos de dados e espaços em branco ajustados.")
    except Exception as e:
        print(f"ERRO na preparação dos dados: {e}")
        return None

    # --- ETAPA 2: Diagnóstico do Primeiro Merge ---
    print("\n[ETAPA 2] Diagnóstico do MERGE 1 (IBAMA <-> CONECTOR)...")
    matches = df_ibama['cod_produto'].isin(df_conector['PRODLIST']).sum()
    print(f"Total de linhas em df_ibama: {len(df_ibama)}")
    print(f"Total de linhas em df_conector: {len(df_conector)}")
    print(f"Encontradas {matches} correspondências entre 'cod_produto' e 'PRODLIST'.")

    if matches == 0:
        print("ALERTA: Nenhuma correspondência encontrada. O DataFrame final não terá dados de 'NFR' e 'Table'.")
        print("Primeiras 5 chaves únicas de df_ibama['cod_produto']:", df_ibama['cod_produto'].unique()[:5])
        print("Primeiras 5 chaves únicas de df_conector['PRODLIST']:", df_conector['PRODLIST'].unique()[:5])

    # --- ETAPA 3: Execução do Primeiro Merge ---
    df_merged = df_ibama.merge(
        df_conector[['PRODLIST', 'NFR', 'Table']],
        left_on='cod_produto',
        right_on='PRODLIST',
        how='left'
    )
    
    # --- ETAPA 4: Diagnóstico do Segundo Merge ---
    print("\n[ETAPA 4] Diagnóstico do MERGE 2 (Resultado anterior <-> Fator de Emissão)...")
    # Vamos verificar as chaves que NÃO são nulas após o primeiro merge
    df_merged_com_chaves = df_merged.dropna(subset=['NFR', 'Table'])
    print(f"{len(df_merged_com_chaves)} linhas possuem chaves ('NFR', 'Table') válidas após o primeiro merge.")

    if len(df_merged_com_chaves) > 0:
        # Criamos um "multi-índice" para facilitar a verificação de correspondência
        chaves_merged = set(zip(df_merged_com_chaves['NFR'], df_merged_com_chaves['Table']))
        chaves_ef = set(zip(df_ef['NFR'], df_ef['Table']))
        
        matches_segundo_merge = len(chaves_merged.intersection(chaves_ef))
        print(f"Encontradas {matches_segundo_merge} combinações de ('NFR', 'Table') correspondentes com a base de Fator de Emissão.")
        
        if matches_segundo_merge == 0:
             print("ALERTA: Nenhuma correspondência de ('NFR', 'Table') encontrada.")
             print("Exemplos de chaves do merge anterior:", list(chaves_merged)[:5])
             print("Exemplos de chaves da base de Fator de Emissão:", list(chaves_ef)[:5])

    # --- ETAPA 5: Execução do Segundo Merge ---
    df_final = df_merged.merge(
        df_ef,
        on=['NFR', 'Table'], # 'on' pode ser usado quando os nomes das colunas são iguais
        how='left'
    )

    # Remover coluna auxiliar
    df_final = df_final.drop(columns=['PRODLIST'])

    print("\n--- FUNÇÃO DE CONEXÃO FINALIZADA ---")
    
    # Verificação final
    colunas_do_ef = [col for col in df_ef.columns if col not in ['NFR', 'Table']]
    print(f"\nValores não nulos na coluna '{colunas_do_ef[0]}' (de df_ef) após o merge final: {df_final[colunas_do_ef[0]].notna().sum()}")

    return df_final


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


def agrupar_e_somar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa um DataFrame por um conjunto de colunas-chave e agrega os valores.

    As colunas numéricas (não presentes nas chaves de agrupamento) são somadas.
    As colunas não numéricas são preenchidas com o primeiro valor do grupo.

    Args:
        df (pd.DataFrame): O DataFrame de entrada para ser processado.

    Returns:
        pd.DataFrame: Um novo DataFrame com os dados agrupados e agregados.
    """
    # 1. Define as colunas que serão a "chave" para o agrupamento
    colunas_agrupamento = [
        'mv.num_cpf_cnpj', 
        'mv.nom_pessoa',
        'mv.nom_municipio',
        'num_ano',
        'cod_produto',
        'unidade_medida',
        'sig_unidmed'
    ]

    print(f"Número de linhas antes do agrupamento: {len(df)}")

    # 2. Cria dinamicamente as regras de agregação
    # O objetivo é dizer ao pandas o que fazer com cada coluna que NÃO está no agrupamento.
    regras_agregacao = {}
    colunas_para_agregar = [col for col in df.columns if col not in colunas_agrupamento]

    for coluna in colunas_para_agregar:
        # Verifica se a coluna contém dados numéricos (int, float, etc.)
        if pd.api.types.is_numeric_dtype(df[coluna]):
            regras_agregacao[coluna] = 'sum'  # Se for numérica, some
        else:
            regras_agregacao[coluna] = 'first' # Se não for, pegue o primeiro valor

    # 3. Executa o agrupamento e a agregação
    # .groupby() cria os grupos
    # .agg() aplica as regras que definimos
    # .reset_index() transforma as colunas de agrupamento de volta em colunas normais
    df_agrupado = df.groupby(colunas_agrupamento).agg(regras_agregacao).reset_index()

    print(f"Número de linhas após o agrupamento: {len(df_agrupado)}")
    
    return df_agrupado


