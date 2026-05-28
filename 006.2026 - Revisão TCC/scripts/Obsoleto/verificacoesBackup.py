# -*- coding: utf-8 -*-
"""
Created on Wed Jul 30 09:40:15 2025

@author: glima
"""


#%% Verifiquei se existe a possibilidade de um mesmo cnpj estar ativo p algumas
# atividades e encerrado para outras. Isso não ocorreu amém

df_ibama_filtro = df_ibama[df_ibama['SITUACAO CADASTRAL'] != 'Ativa']
df_ibama_filtro['CNPJ'].value_counts()

# Converte a coluna para string para evitar erros com 'nan'
# O uso de \b16\b garante que você encontre o número "16" exato, e não "116" ou "160"
mascara_texto = df_ibama_filtro['CODIGO DA CATEGORIA'].astype(str).str.contains(r'\b16\b', na=False)

df_resultado_texto = df_ibama_filtro[mascara_texto]

print(f"Foram encontradas {len(df_resultado_texto)} linhas com o código de categoria 16 (método de texto).")
print(df_resultado_texto)


df_ibama_filtro_2 = df_ibama[df_ibama['SITUACAO CADASTRAL'] == 'Ativa']

df_ibama_filtro_2 = df_ibama_filtro_2[df_ibama_filtro_2['CNPJ'] == '21042390000132']

# 1. Crie um conjunto com os CNPJs de cada DataFrame
set_inativos_cat16 = set(df_resultado_texto['CNPJ'])
set_ativos = set(df_ibama_filtro_2['CNPJ'])

# 2. Encontre a interseção (o que há em comum) entre os dois conjuntos
cnpjs_em_comum = set_inativos_cat16.intersection(set_ativos)

# 3. Verifique o resultado
if cnpjs_em_comum:
    print("Os seguintes CNPJs estão em ambas as listas:")
    print(list(cnpjs_em_comum))
else:
    print("Nenhum CNPJ em comum encontrado.")
    