# -*- coding: utf-8 -*-
"""
Este módulo contém a função para tratamento de outliers e preenchimento de dados
conforme o fluxograma de análise.

Criado em Wed Jul 30 10:00:00 2025
Atualizado em Wed Jul 30 11:11:00 2025

@author: glima
"""

import pandas as pd

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
    df_filtrado_1['obs_tratamento'] = 'Original'

    # --- 2º Filtro: Substituir outliers pela mediana ---
    print("Aplicando o 2º Filtro: Tratamento de outliers.")
    
    # Calcula a mediana da produção para cada grupo. 'transform' alinha o resultado de volta ao DataFrame original.
    medianas_grupo = df_filtrado_1.groupby(group_cols)['Produção (Ton ou hL)'].transform('median')
    # Cria uma máscara booleana para identificar outliers (produção >= 3x a mediana do grupo).
    mascara_outliers = df_filtrado_1['Produção (Ton ou hL)'] >= (3 * medianas_grupo)
    
    # Usa a máscara para atualizar a coluna de rastreamento, marcando os outliers.
    df_filtrado_1.loc[mascara_outliers, 'obs_tratamento'] = 'Outlier substituído'
    
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
        
        # As novas linhas criadas terão NaN na coluna 'obs_tratamento'. Preenchemos com a marcação correta.
        grupo_completo['obs_tratamento'] = grupo_completo['obs_tratamento'].fillna('Dado preenchido')
        
        # --- LÓGICA DA MEDIANA REFINADA ---
        # 1. Filtra o grupo original para pegar apenas os dados que não foram classificados como outliers.
        grupo_sem_outliers = grupo[grupo['obs_tratamento'] == 'Original']
        
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