# -*- coding: utf-8 -*-
"""
Created on Mon Feb  9 09:00:41 2026

Base de dados extraída de https://sidra.ibge.gov.br/tabela/8885
Realizado o cálculo de um valor de desagregação temporal mensal, que será
posteriormente utilizado para desagregação de valores anuais

@GitHub: https://github.com/gabriellimasclg
"""

import pandas as pd
import numpy as np

# 1. Carregar os dados
# O delimitador automático identifica se é vírgula ou ponto-e-vírgula
df_raw = pd.read_csv(r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\003.2026 - DesagregacaoSIDRA8885\inputs\Tabela 8885; Produção Física Industrial, por grupos e classes industriais selecionados; 2022 = 100.csv', sep=None, engine='python')

# 2. Reorganizar o DataFrame (de colunas para linhas)
id_col = df_raw.columns[0] # "Grupos e classes industriais"
df_long = df_raw.melt(id_vars=[id_col], var_name='Mes_Ano', value_name='Producao')

# 3. Limpar os valores numéricos
# Substitui vírgula por ponto e converte para float
df_long['Producao'] = pd.to_numeric(df_long['Producao'].astype(str).str.replace(',', '.'), errors='coerce')

# 4. Converter a coluna de texto para Data real
month_map = {
    'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04',
    'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
    'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12'
}

def parse_data(texto):
    partes = texto.strip().split(' ')
    if len(partes) == 2:
        mes, ano = partes[0].lower(), partes[1]
        return pd.to_datetime(f"{ano}-{month_map[mes]}-01")
    return pd.NaT

df_long['Data'] = df_long['Mes_Ano'].apply(parse_data)
df_long['Ano'] = df_long['Data'].dt.year

# Remover linhas sem dados (ex: cabeçalhos extras ou anos incompletos se houver)
df_long = df_long.dropna(subset=['Producao', 'Ano'])

# 5. A TRANSFORMAÇÃO: Normalizar para que a SOMA DO ANO = 1
# Agrupamos por Categoria e Ano, e dividimos cada valor pela soma do grupo
df_long['Percentual_Anual'] = df_long.groupby([id_col, 'Ano'])['Producao'].transform(lambda x: x / x.sum())

# 6. Salvar o resultado
df_long.to_csv(r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\DesagregacaoSIDRA8885\outputs\producao_normalizada_2017_2024.csv', index=False)
