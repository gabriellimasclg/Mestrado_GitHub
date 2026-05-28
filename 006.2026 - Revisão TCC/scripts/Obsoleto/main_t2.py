# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 07:47:09 2025

@author: Gabriel
"""
#%%=========================== Bibliotecas ==================================
import os
import numpy as np
import pandas as pd
from download_database import download_ibama_ctf_data
from import_database import ibama_production_data, import_products_code
from merge_filter_df import merge_cnpj_prod, conecta_ibama_ef, converter_para_hl

#%%===============================TCC========================================

# Caminho da pasta do projeto
repo_path = os.path.dirname(os.getcwd())

# Faz o downloado da base de dados com CNPJ + Coordenadas
df_ibama_cnpj = download_ibama_ctf_data(repo_path)

# Importa DF com Dados de produção com CNPJ + Código de Produto + Produção
df_ibama_prod = ibama_production_data(repo_path) 

'''
VERIFICAÇÃO SE TODOS OS MUNICÍPIOS DE PROD ESTÃO EM NO DF_IBAMA_CNPJ 
# Municípios únicos em cada base
municipios_prod = set(df_ibama_prod['mv.nom_municipio'].unique())
municipios_cnpj = set(df_ibama_cnpj['MUNICIPIO'].unique())

# Presentes em ambas
presentes_ambas = municipios_prod & municipios_cnpj

# Presentes apenas na primeira
apenas_prod = municipios_prod - municipios_cnpj

# Presentes apenas na segunda
apenas_cnpj = municipios_cnpj - municipios_prod
'''

df_dd = df_ibama_prod['mv.nom_municipio'].value_counts()

# DF com Produção + Código de produto + Coordenadas
df_ibama = merge_cnpj_prod(df_ibama_cnpj,df_ibama_prod) #mesclar p obter coordenadas e código de atividade

#Base de dados com todos os Códigos de Produto
cod_produto= import_products_code(repo_path)

# Vou exportar para classificar MANUALMENTE Código de Produto (IBAMA) vs NFR+Table (EEA)
cod_produto.to_excel(os.path.join(repo_path, 'outputs', 'cod_produto_tratado.xlsx'))

# RENOMEEI 'cod_produto_tratado.xlsx' para 'cod_produto_nfr_table.xlsx' para
# não correr o risco de perder o material por sobreposição ao rodar o código

#Importar DF no qual eu conecto MANUALMENTE o código do produto com NFR e TABLE
cod_produto_nfr_table = pd.read_excel(os.path.join(repo_path, 'inputs', 'cod_produto_nfr_table.xlsx'),
                                      dtype={'PRODLIST': str})

# DF com Fatores de emissão + NFR + Table
eea_ef = pd.read_csv(os.path.join(repo_path, 'inputs', 'ef_eea_tier2.csv'))
eea_ef['Table'] = eea_ef['Table'].str.replace('Table_', '', regex=False)

# DF com Fator de emissão + NFR + Table + Código do Produto + Produção + Coordenadas
df = conecta_ibama_ef(df_ibama,eea_ef,cod_produto_nfr_table)

'''
A conversão de unidades de medida precisará ser automatizda.
tipo, se for unidade X --> dicionario X de fator de conversão
'''

#%%===========================TRABALHO DO LEO==================================

# A partir daqui, vou filtrar apenas as bebidas, que eu classifiquei, para o 
#trabalho da disciplina ENS5132

#Filtrando as bebidas no df conector, que vou usar no trabalho do Leo
df_bebidas = df[
    df['cod_produto'].astype(str).str.startswith(('1113','1112','1111'))
]

# Trabalhando com fatores de conversão de unidades
df_bebidas['unidade_medida'].value_counts()

#Aqui eu estudo as unidades pois estou montando um csv de fator de conversão
df_cerveja = df_bebidas[df_bebidas['cod_produto'].astype(str).str.startswith('1113')]
df_cerveja['unidade_medida'].value_counts()

df_destilado = df_bebidas[df_bebidas['cod_produto'].astype(str).str.startswith('1111')]
df_destilado['unidade_medida'].value_counts()

# Carrega o CSV de conversão de unidades
df_conversao = pd.read_excel(os.path.join(repo_path, 'inputs', 'conversao_unidades.xlsx'))


# Aplica a função de conversão de unidades
df_bebidas['volume_hl'] = df_bebidas.apply(
    lambda row: converter_para_hl(
        df_conversao,
        row['qtd_produzida'],
        row['unidade_medida'],
        row.get('cod_produto')  # Usa .get() para caso a coluna não exista
    ),
    axis=1
)

# Faz o cálculo de NMVOC (kg)
df_bebidas['Value'] = pd.to_numeric(df_bebidas['Value'], errors='coerce')
df_bebidas['volume_hl'] = pd.to_numeric(df_bebidas['volume_hl'], errors='coerce')

mask = df_bebidas['volume_hl'].notna()
df_bebidas.loc[mask, 'NMVOC (kg)'] = df_bebidas.loc[mask, 'Value'] * df_bebidas.loc[mask, 'volume_hl']

# Calcula a porcentagem de valores nan (pra tentar assumir mais unidades)
total_valores = len(df_bebidas['NMVOC (kg)'])
total_nan = df_bebidas['NMVOC (kg)'].isna().sum()

print(f"Total de valores: {total_valores}")
print(f"Valores NaN: {total_nan}")
print(f"Porcentagem de NaN: {(total_nan/total_valores)*100:.2f}%")

#base para bruno analisar
df_bebidas_enviar = df_bebidas.drop(columns=['CNPJ','mv.num_cpf_cnpj','mv.nom_pessoa'])
df_bebidas_enviar.to_excel(os.path.join(repo_path, 'outputs', 'df_bebidas_para_analise.xlsx'))
#%% TRABALHO 02 DE PYTHON
'''
IDEIAS DE ANÁLISE E VISUALIZAÇÃO – INVENTÁRIO DE EMISSÕES DE BEBIDAS (CERVEJA, VINHO, DESTILADOS)

ANÁLISES PRÉVIAS:
- Estudo de tendência nas emissões por bebida (cerveja vs vinho vs destilado) e total agregado.
- Aplicação do teste de Mann-Kendall para identificar tendências de crescimento ou declínio no consumo/emissões por bebida.

APENAS GRÁFICOS:
- Análise do raster de uso do solo por estado, com foco nas áreas ocupadas pelas indústrias de bebidas.

MAPAS E GRÁFICOS (ESCALA BRASIL E SANTA CATARINA):
- Mapa de localização das indústrias (cores diferentes para cada bebida).
- Gráfico de barras empilhadas com a quantidade de indústrias por estado, separadas por tipo de bebida.
- Destaque para as 3 cidades com maior produção de cada bebida.
- Cálculo e comparação entre:
    • Quantidade total de indústrias por cidade
    • Densidade de indústrias (indústrias/m²) por cidade
    • Verificar se as cidades com maior produção são também as com maior número/densidade de indústrias.
- RASTERIZAÇÃO DAS EMISSÕES:
    • Geração de mapas com emissões para cada tipo de bebida e total agregado.
- Mapa do país com os resultados do teste de Mann-Kendall:
    • Identificar regiões com tendência de aumento ou queda nas emissões.
- Mapa de análise temporal espacial:
    • Evolução das emissões ao longo do tempo de forma geográfica (animação ou mapas por ano).
- Clusterização das indústrias:
    • Identificação de polos produtivos por tipo de bebida através de agrupamento espacial.

'''

## Estudo de tendência de emissões por bebida e total agregado

# Classificar o tipo de bebida
def classificar_bebida(codigo):
    if codigo.startswith('1111'):
        return 'Destilado'
    elif codigo.startswith('1112'):
        return 'Vinho'
    elif codigo.startswith('1113'):
        return 'Cerveja'
    else:
        return np.nan

df_bebidas['bebida'] = df_bebidas['cod_produto'].map(classificar_bebida)
df_bebidas['tipo_geral'] = 'bebida'

bebidas = np.unique(df_bebidas.bebida)
tipo_geral = np.unique(df_bebidas.tipo_geral)

df_bebidas.head()

import pandas as pd
import numpy as np
import pymannkendall as mk

def analisar_tendencia_nmvc(df, group_cols):
    resultados = []

    for grupo_valores, grupo_df in df.groupby(group_cols):
        if isinstance(grupo_valores, str):
            grupo_valores = (grupo_valores,)

        grupo_dict = dict(zip(group_cols, grupo_valores))
        print(f"\nAnalisando: {grupo_dict}")

        serie_anual = grupo_df.groupby('num_ano')["NMVOC (kg)"].sum().sort_index()

        try:
            resultado = mk.original_test(serie_anual)

            resultados.append({
                **grupo_dict,
                'tendência': resultado.trend,
                'p-valor': resultado.p,
                'z': resultado.z,
                'Tau': resultado.Tau,
                'slope': resultado.slope,
                'intercept': resultado.intercept
            })
            print(f"  Tendência: {resultado.trend}, p-valor: {resultado.p:.3f}")
        except Exception as e:
            print(f"  Erro na análise: {e}")
            resultados.append({
                **grupo_dict,
                'tendência': np.nan,
                'p-valor': np.nan,
                'z': np.nan,
                'Tau': np.nan,
                'slope': np.nan,
                'intercept': np.nan
            })

    return pd.DataFrame(resultados)

# Análise por estado e bebida
df_resultado_estado_bebida = analisar_tendencia_nmvc(df_bebidas, ['Estado', 'bebida'])
df_resultado_estado_bebida.to_csv('tendencia_por_estado_bebida.csv', index=False)

# Análise geral por bebida (todos os estados juntos)
df_resultado_bebida = analisar_tendencia_nmvc(df_bebidas, ['bebida'])
df_resultado_bebida.to_csv('tendencia_geral_bebida.csv', index=False)

# Análise por estado
df_resultado_estado_geral = analisar_tendencia_nmvc(df_bebidas, ['Estado'])
df_resultado_estado_geral.to_csv('tendencia_por_estado_geral.csv', index=False)

#%% Criação de mapas de tendência

## Agora vou criar os mapas! Encontrei resultados em
# df por bebida
#df com todas as bebidas juntas

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from clean_text import clean_text

# Carregar os estados do Brasil via GeoJSON
url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
estados = gpd.read_file(url)

# Padronizar nome dos estados
estados['Estado'] = estados['name'].apply(clean_text)

# Mesclar com os dados geográficos (assumindo que df_resultado_estado_geral já existe)
df_tend = estados.merge(df_resultado_estado_geral, on='Estado', how='left')

# Mapear cores conforme a tendência
cores = {
    'increasing': '#3b6fb6',  # azul
    'decreasing': '#c92525',  # vermelho
    'no trend': '#a3a3a3',    # cinza
    None: '#ffffff',          # branco (sem emissão)
    float('nan'): '#ffffff'   # branco também
}
df_tend['cor'] = df_tend['tendência'].map(cores)

# Criar o mapa
fig, ax = plt.subplots(figsize=(10, 10))
df_tend.plot(ax=ax, color=df_tend['cor'], edgecolor='black')

# Adicionar as siglas dos estados no centro de cada polígono
for idx, row in df_tend.iterrows():
    # Pegar o centroide (ponto central) do estado
    centroid = row['geometry'].centroid
    # Adicionar a sigla no centro
    ax.annotate(
        text=row['sigla'],  # Assumindo que a coluna se chama 'sigla'
        xy=(centroid.x, centroid.y),
        ha='center',  # alinhamento horizontal centralizado
        va='center',  # alinhamento vertical centralizado
        fontsize=14,
        color='black'  # cor do texto
    )

# Adicionar título e legenda
plt.title('Tendência de Emissão de NMVOC Relativo a Bebidas Alcoólicas por Estado', fontsize=14)

# Legenda manual
legend_patches = [
    mpatches.Patch(color='#3b6fb6', label='Increasing'),
    mpatches.Patch(color='#c92525', label='Decreasing'),
    mpatches.Patch(color='#a3a3a3', label='No Trend'),
    mpatches.Patch(color='#ffffff', label='Sem Emissão')
]
plt.legend(handles=legend_patches, loc='lower left')

plt.axis('off')
plt.tight_layout()
plt.savefig(os.path.join(repo_path,'figures','tendencia_bebidas_geral'))
plt.show()

# agr vou criar um mapa para cada bebida

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os

# Carregar os estados do Brasil via GeoJSON
url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
estados = gpd.read_file(url)

# Padronizar nome dos estados (assumindo que 'clean_text' está definido)
estados['Estado'] = estados['name'].apply(clean_text)

# Lista de bebidas (substitua pelas suas categorias)
bebidas = ['Cerveja', 'Vinho', 'Destilado']  # Exemplo

# Criar figura com 3 subplots (1 linha, 3 colunas)
fig, axes = plt.subplots(1, 3, figsize=(20, 8))  # Ajuste o tamanho conforme necessário

# Mapear cores
cores = {
    'increasing': '#3b6fb6',
    'decreasing': '#c92525',
    'no trend': '#a3a3a3',
    None: '#ffffff',
    float('nan'): '#ffffff'
}

# Loop para cada bebida e seu respectivo subplot
for i, bebida in enumerate(bebidas):
    ax = axes[i]  # Seleciona o subplot atual
    
    # Filtrar dados para a bebida específica
    df_bebida_temp = df_resultado_estado_bebida[df_resultado_estado_bebida['bebida'] == bebida]
    df_tend_temp = estados.merge(df_bebida_temp, on='Estado', how='left')
    df_tend_temp['cor'] = df_tend_temp['tendência'].map(cores)
    
    # Plotar o mapa
    df_tend_temp.plot(ax=ax, color=df_tend_temp['cor'], edgecolor='black')
    
    # Adicionar siglas dos estados
    for idx, row in df_tend_temp.iterrows():
        centroid = row['geometry'].centroid
        ax.annotate(
            text=row['sigla'],
            xy=(centroid.x, centroid.y),
            ha='center',
            va='center',
            fontsize=10,  # Reduzi um pouco para caber melhor
            color='black'
        )
    
    # Título do subplot
    ax.set_title(f'Tendência de NMVOC: {bebida}', fontsize=16)
    ax.axis('off')

# Adicionar legenda única para toda a figura
legend_patches = [
    mpatches.Patch(color='#3b6fb6', label='Aumentando'),
    mpatches.Patch(color='#c92525', label='Diminuindo'),
    mpatches.Patch(color='#a3a3a3', label='Sem Tendência'),
    mpatches.Patch(color='#ffffff', label='Sem Emissão')
]
fig.legend(handles=legend_patches, loc='lower center', ncol=4, bbox_to_anchor=(0.5, 0.01),fontsize=12)

# Ajustar layout e salvar/visualizar
plt.tight_layout()
plt.savefig(os.path.join(repo_path, 'figures', 'tendencia_bebidas_comparacao.png'), bbox_inches='tight', dpi=300)
plt.show()

#%% Analisar localização das indústrias no br

###AQUI TENHO Q AGRUPAR AS INDUSTRIAS PRIMEIRO!!!!

# Corrigir latitude e longitude ANTES do agrupamento
df_bebidas['latitudeFloat'] = df_bebidas['Latitude'].astype(str).str.replace(',', '.', regex=False).astype(float)
df_bebidas['longitudeFloat'] = df_bebidas['Longitude'].astype(str).str.replace(',', '.', regex=False).astype(float)

# Agrupar por indústria (CNPJ)
colunas_ind_agrupada = ['CNPJ', 'Municipio', 'Estado', 'latitudeFloat', 'longitudeFloat', 'bebida', 'volume_hl', 'NMVOC (kg)']
df_bebidas_ind_agrupada = df_bebidas[colunas_ind_agrupada].groupby(
    ['CNPJ', 'Municipio', 'Estado', 'bebida', 'latitudeFloat', 'longitudeFloat']
).sum(numeric_only=True).reset_index()

# Adicionar contagem
df_bebidas_ind_agrupada['Contagem'] = 1

indContageEstado = df_bebidas_ind_agrupada.groupby(['bebida']).agg(
    Contagem=('Contagem', 'sum'),  # ou 'count' se preferir
    volume_total=('volume_hl', 'sum')
).reset_index()

indContageMuni = df_bebidas_ind_agrupada.groupby(['Municipio', 'Estado','bebida']).agg(
    Contagem=('Contagem', 'sum'),  # ou 'count' se preferir
    volume_total=('volume_hl', 'sum')
).reset_index()

# Converter para GeoDataFrame
df_bebidas_gpd = gpd.GeoDataFrame(
    df_bebidas_ind_agrupada,
    geometry=gpd.points_from_xy(df_bebidas_ind_agrupada.longitudeFloat, df_bebidas_ind_agrupada.latitudeFloat),
    crs="EPSG:4326"
)

# Define cores para cada bebida
cores = {
    'Vinho': 'purple',
    'Cerveja': 'gold',
    'Destilado': 'blue'
}

# Cria subplots
fig, axes = plt.subplots(1, 3, figsize=(20, 8)) 

for i, (bebida, cor) in enumerate(cores.items()):
    ax = axes[i]
    
    estados.plot(ax=ax, color='white', edgecolor='black')

    df_bebidas_gpd[df_bebidas_gpd['bebida'] == bebida].plot(
        ax=ax, color=cor, label=bebida, markersize=30, edgecolor='black'
    )

    contTemp = indContageEstado.loc[indContageEstado['bebida'] == bebida, 'Contagem']
    
    ax.set_title(f'Distribuição das indústrias de {bebida} no Brasil\n{contTemp.item()} indústrias', fontsize=16)
    ax.axis('off')

plt.tight_layout()
plt.savefig(os.path.join(repo_path, 'figures', 'localizacao_prod_bebidas.png'), bbox_inches='tight', dpi=300)
plt.show()


#%% barras empilhadas por estado de qtde

import matplotlib.pyplot as plt
import pandas as pd

# 1. Certifique-se de que 'volume_hl' é numérico
df_bebidas['volume_hl'] = pd.to_numeric(df_bebidas['volume_hl'], errors='coerce').fillna(0)

# 2. Agregação por estado e bebida
bebidas_por_estado = df_bebidas.groupby(['Estado', 'bebida'])['volume_hl'].sum().unstack().fillna(0)


#bebidas_por_estado = bebidas_por_estado.drop('PERNAMBUCO')


# 3. Criação do gráfico
plt.figure(figsize=(15, 8))

cores = {
    'Vinho': '#8B0000',
    'Cerveja': '#FFD700',
    'Destilado': '#4682B4'
}

bottom = pd.Series([0] * len(bebidas_por_estado), index=bebidas_por_estado.index)

for bebida in ['Vinho', 'Cerveja', 'Destilado']:
    plt.bar(
        bebidas_por_estado.index,
        bebidas_por_estado[bebida],
        bottom=bottom,
        label=bebida,
        color=cores[bebida]
    )
    bottom += bebidas_por_estado[bebida]


# Personalização
plt.title('Produção de Bebidas por Estado (hl) entre 2017 e 2023', fontsize=16)
plt.yscale('log')
plt.ylabel('Volume (hl)', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)

plt.legend(loc='lower center', ncol=4, bbox_to_anchor=(0.5, -0.3),fontsize=12)

plt.tight_layout()
plt.savefig(os.path.join(repo_path, 'figures', 'qtd-por-estado.png'), bbox_inches='tight', dpi=300)
plt.show()

#%% tentativa de clusterização --> tem um outlier fudido q n to conseguindo tirar
from shapely.geometry import Point
import geopandas as gpd
import matplotlib.pyplot as plt

###AQUI TENHO Q AGRUPAR AS INDUSTRIAS PRIMEIRO!!!!

# 1. Converter para métrico
df_proj = df_bebidas_gpd.to_crs(epsg=3857)

# 2. Criar buffers de 50 km
df_proj['buffer'] = df_proj.geometry.buffer(50000)

# 3. Criar GeoDataFrame com buffers (mantém todos separados!)
buffers = gpd.GeoDataFrame(
    df_proj[['bebida', 'volume_hl']], geometry=df_proj['buffer'], crs=df_proj.crs
)

# 4. Voltar para WGS84
buffers = buffers.to_crs(epsg=4326)

# 5. Cores
cores = {'Vinho': 'purple', 'Cerveja': 'gold', 'Destilado': 'blue'}

# 6. Subplots
fig, axes = plt.subplots(1, 3, figsize=(20, 8))

for ax, (bebida, cor) in zip(axes, cores.items()):
    # Base: mapa do Brasil
    estados.plot(ax=ax, color='white', edgecolor='black')

    # Buffers da bebida atual
    df_bebida = buffers[buffers['bebida'] == bebida]

    # Plotar buffers com opacidade baixa (efeito de sobreposição)
    df_bebida.plot(
        ax=ax,
        color=cor,
        alpha=0.05,  # quanto menor, mais suave; sobreposição escurece
        linewidth=0,
    )
    
    ax.set_title(f'{bebida}', fontsize=14)
    ax.axis('off')

# Título geral
fig.suptitle('Densidade de Produção por Bebida (Buffer 50 km com sobreposição)', fontsize=18)
plt.tight_layout()
plt.savefig(os.path.join(repo_path, 'figures', 'clusterização.png'), bbox_inches='tight', dpi=300)
plt.subplots_adjust(top=0.88)
plt.show()
#%% DESCOBRIR AS CIDADES MAIS PRODUTORAS

# Ler malha municipal
malhaMuni = gpd.read_file(os.path.join(repo_path, 'inputs', 'BR_Municipios_2024', 'BR_Municipios_2024.shp'))
malhaMuni['NM_MUN'] = malhaMuni['NM_MUN'].apply(clean_text)
malhaMuni['NM_UF'] = malhaMuni['NM_UF'].apply(clean_text)

# Criar GeoDataFrame com indústrias
colunasProd = ['CNPJ', 'Municipio', 'Estado', 'latitudeFloat', 'longitudeFloat', 'bebida', 'volume_hl', 'NMVOC (kg)']
prod = df_bebidas[colunasProd].groupby(
    ['CNPJ', 'Municipio', 'Estado', 'bebida', 'latitudeFloat', 'longitudeFloat']
).sum(numeric_only=True).reset_index()

prod['Contagem'] = 1

prod['geometry'] = prod.apply(lambda row: Point(row['longitudeFloat'], row['latitudeFloat']), axis=1)
gpd_prod = gpd.GeoDataFrame(prod, geometry='geometry', crs='EPSG:4326')

# Agregação por qtd de Industrias
colunasIndMunicipal = ['Municipio', 'Estado', 'bebida', 'volume_hl', 'NMVOC (kg)']
indMunicipal = prod.groupby(['Municipio', 'Estado', 'bebida', 'latitudeFloat', 'longitudeFloat']).sum(numeric_only=True).reset_index()

# Agregação por município
colunasProdMunicipal = ['Municipio', 'Estado', 'bebida', 'volume_hl', 'NMVOC (kg)']
prodMunicipal = df_bebidas[colunasProdMunicipal].groupby(['Municipio', 'Estado', 'bebida']).sum(numeric_only=True).reset_index()

# Merge malha + produção por município
malhaComProd = malhaMuni.merge(
    prodMunicipal,
    left_on=['NM_MUN', 'NM_UF'],
    right_on=['Municipio', 'Estado'],
    how='inner'
)

# Top 3 por bebida
top_3_por_bebida = {
    bebida: prodMunicipal[prodMunicipal.bebida == bebida].nlargest(3, 'volume_hl')
    for bebida in ['Vinho', 'Cerveja', 'Destilado']
}

# Plotar mapas 1x3 para cada bebida
for bebida, df_top in top_3_por_bebida.items():
    fig, axes = plt.subplots(1, 3, figsize=(21, 7))
    
    for i, row in df_top.reset_index(drop=True).iterrows():
        ax = axes[i]
        municipio = row['Municipio']
        estado = row['Estado']
        volume_total = row['volume_hl']

        # Polígono do município
        base = malhaComProd[
            (malhaComProd['Municipio'] == municipio) &
            (malhaComProd['Estado'] == estado) &
            (malhaComProd['bebida'] == bebida)
        ]
        
        # Indústrias na cidade e bebida
        gpd_cidade = gpd_prod[
            (gpd_prod['Municipio'] == municipio) &
            (gpd_prod['Estado'] == estado) &
            (gpd_prod['bebida'] == bebida)
        ]

        # Plotagem
        base.plot(ax=ax, color='whitesmoke', edgecolor='black')
        if not gpd_cidade.empty:
            gpd_cidade.plot(ax=ax, color='red', markersize=30, edgecolor='black')

        # Ajustar visualização (com base no polígono)
        bounds = base.total_bounds
        ax.set_xlim(bounds[0] - 0.1, bounds[2] + 0.1)
        ax.set_ylim(bounds[1] - 0.1, bounds[3] + 0.1)

        # Título e anotação
        ax.set_title(f'{municipio}/ {estado}', fontsize=18)
        ax.axis('off')

        bounds = base.total_bounds  # [minx, miny, maxx, maxy]
        x_center = (bounds[0] + bounds[2]) / 2
        y_top = bounds[3]
        
        ax.text(
            x_center, y_top + 0.02,  # pequeno deslocamento para cima
            f'{volume_total:,.0f} hl',
            fontsize=11,
            ha='center', va='bottom',
            color='black',
            bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='black', lw=1)
        )

    
    #fig.suptitle(f'Top 3 cidades produtoras de {bebida}', fontsize=18)
    plt.tight_layout()
    plt.subplots_adjust(top=0.85)
    plt.savefig(os.path.join(repo_path, 'figures', f'topProducao {bebida}.png'), bbox_inches='tight', dpi=300)
    plt.show()

#%% Plotar maior densidade de industrias


# Merge malha + cont por município
malhaComCont = malhaMuni.merge(
    indContageMuni,
    left_on=['NM_MUN', 'NM_UF'],
    right_on=['Municipio', 'Estado'],
    how='inner'
)

#industrias a cada 100km² 
malhaComCont['Densidade'] = (malhaComCont['Contagem']/malhaComCont['AREA_KM2'])*100

# Top 3 DENSIDADE por bebida
top_3_cont_por_bebida = {
    bebida: malhaComCont[malhaComCont.bebida == bebida].nlargest(3, 'Densidade')
    for bebida in ['Vinho', 'Cerveja', 'Destilado']
}

for bebida, df_top in top_3_cont_por_bebida.items():
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    for i, row in df_top.reset_index(drop=True).iterrows():
        ax = axes[i]
        municipio = row['Municipio']
        estado = row['Estado']
        densidade = row['Densidade']
        total_industrias = row['Contagem']

        base = malhaComCont[
            (malhaComCont['Municipio'] == municipio) &
            (malhaComCont['Estado'] == estado) &
            (malhaComCont['bebida'] == bebida)
        ]
        
        gpd_cidade = gpd_prod[
            (gpd_prod['Municipio'] == municipio) &
            (gpd_prod['Estado'] == estado) &
            (gpd_prod['bebida'] == bebida)
        ]

        base.plot(ax=ax, color='whitesmoke', edgecolor='black')
        if not gpd_cidade.empty:
            gpd_cidade.plot(ax=ax, color='red', markersize=30, edgecolor='black')

        bounds = base.total_bounds
        ax.set_xlim(bounds[0] - 0.1, bounds[2] + 0.1)
        ax.set_ylim(bounds[1] - 0.1, bounds[3] + 0.1)

        ax.set_title(f'{municipio}\n{estado}', fontsize=18)
        ax.axis('off')

        centro_x = (bounds[0] + bounds[2]) / 2
        topo_y = bounds[3]

        # Anotar densidade
        ax.text(
            centro_x, topo_y + 0.02,
            f'{densidade:.2f} ind./100 km²\n{total_industrias} indústrias',
            fontsize=11, ha='center', va='bottom',
            bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='black', lw=1)
        )
    #fig.suptitle(f'Top 3 cidades com maior densidade de indústrias de {bebida}', fontsize=18)
    plt.tight_layout()
    plt.subplots_adjust(top=0.85)
    plt.savefig(os.path.join(repo_path, 'figures', f'topDensidade {bebida}.png'), bbox_inches='tight', dpi=300)
    plt.show()

#%% Top cidades com maior número BRUTO de indústrias

# Top 3 cidades com mais indústrias por bebida
top_3_contBruto_por_bebida = {
    bebida: malhaComCont[malhaComCont.bebida == bebida].nlargest(3, 'Contagem')
    for bebida in ['Vinho', 'Cerveja', 'Destilado']
}

for bebida, df_top in top_3_contBruto_por_bebida.items():
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    for i, row in df_top.reset_index(drop=True).iterrows():
        ax = axes[i]
        municipio = row['Municipio']
        estado = row['Estado']
        total_industrias = row['Contagem']

        base = malhaComCont[
            (malhaComCont['Municipio'] == municipio) &
            (malhaComCont['Estado'] == estado) &
            (malhaComCont['bebida'] == bebida)
        ]
        
        gpd_cidade = gpd_prod[
            (gpd_prod['Municipio'] == municipio) &
            (gpd_prod['Estado'] == estado) &
            (gpd_prod['bebida'] == bebida)
        ]

        base.plot(ax=ax, color='whitesmoke', edgecolor='black')
        if not gpd_cidade.empty:
            gpd_cidade.plot(ax=ax, color='red', markersize=30, edgecolor='black')

        bounds = base.total_bounds
        ax.set_xlim(bounds[0] - 0.1, bounds[2] + 0.1)
        ax.set_ylim(bounds[1] - 0.1, bounds[3] + 0.1)

        ax.set_title(f'{municipio}\n{estado}', fontsize=18)
        ax.axis('off')

        centro_x = (bounds[0] + bounds[2]) / 2
        topo_y = bounds[3]

        # Apenas total de indústrias
        ax.text(
            centro_x, topo_y + 0.02,
            f'{total_industrias} indústrias',
            fontsize=11, ha='center', va='bottom',
            bbox=dict(boxstyle='round,pad=0.3', fc='white', ec='black', lw=1)
        )

    #fig.suptitle(f'Top 3 cidades com maior número de indústrias de {bebida}', fontsize=18)
    plt.tight_layout()
    plt.subplots_adjust(top=0.85)
    plt.savefig(os.path.join(repo_path, 'figures', f'topContagem {bebida}.png'), bbox_inches='tight', dpi=300)
    plt.show()


#%% raster de densidade
from scipy.stats import gaussian_kde
import rasterio
from rasterio.transform import from_origin
import numpy as np
import matplotlib.pyplot as plt
import os
import matplotlib.colors as colors

# Filtrar apenas registros válidos com coordenadas e emissões
df_validos = df_bebidas.dropna(subset=['latitudeFloat', 'longitudeFloat', 'NMVOC (kg)', 'bebida'])

# Definir extensão fixa para todo o Brasil
xmin, xmax = -74, -34  # longitude
ymin, ymax = -34, 6    # latitude
res = 0.05  # resolução do pixel em graus

for bebida in ['Vinho', 'Cerveja', 'Destilado']:
    df_bebida = df_validos[df_validos['bebida'] == bebida]

    # Coordenadas e pesos
    x = df_bebida['longitudeFloat'].values
    y = df_bebida['latitudeFloat'].values
    z = df_bebida['NMVOC (kg)'].values

    # KDE ponderada
    kde = gaussian_kde([x, y], weights=z, bw_method=0.05)

    # Grid fixo
    yi, xi = np.mgrid[ymin:ymax:res, xmin:xmax:res]
    coords = np.vstack([xi.ravel(), yi.ravel()])

    # Densidade
    zi = kde(coords).reshape(yi.shape)

    # Máscara para valores baixos
    masked_zi = np.ma.masked_where(zi < 1e-6, zi)

    # Garantir vmin > 0 para LogNorm
    vmin = max(masked_zi.min(), 1e-6)
    vmax = masked_zi.max()

    # --- PLOT 1: escala log individual ---
    fig, ax = plt.subplots(figsize=(10, 8))
    estados.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.6)
    mesh = ax.pcolormesh(
        xi, yi, masked_zi,
        cmap='inferno',
        norm=colors.LogNorm(vmin=vmin, vmax=vmax),
        alpha=0.8,
        shading='auto'
    )
    plt.title(f'Densidade de Emissão de NMVOC (kg) - {bebida} (escala log)')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.colorbar(mesh, label='Densidade estimada (escala log)')
    plt.tight_layout()
    plt.savefig(os.path.join(repo_path, 'figures', f'raster-geral-log-{bebida}.png'), bbox_inches='tight', dpi=300)
    plt.show()

    # --- PLOT 2: densidade normalizada (0 a 1) para comparar padrão espacial ---
    zi_norm = zi / vmax  # normalizar pelo máximo
    masked_zi_norm = np.ma.masked_where(zi_norm < 1e-3, zi_norm)  # máscara ajustada para visualização

    fig, ax = plt.subplots(figsize=(10, 8))
    estados.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.6)
    mesh = ax.pcolormesh(
        xi, yi, masked_zi_norm,
        cmap='inferno',
        vmin=0,
        vmax=1,
        alpha=0.8,
        shading='auto'
    )
    plt.title(f'Densidade Normalizada de Emissão de NMVOC - {bebida} (0 a 1)')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.colorbar(mesh, label='Densidade normalizada (0 a 1)')
    plt.tight_layout()
    plt.savefig(os.path.join(repo_path, 'figures', f'raster-geral-normalizado-{bebida}.png'), bbox_inches='tight', dpi=300)
    plt.show()

    # Salvar raster GeoTIFF do KDE original (não normalizado)
    transform = from_origin(xmin, ymax, res, res)
    out_path = os.path.join(repo_path, 'outputs', f'densidade_{bebida.lower()}.tif')
    with rasterio.open(
        out_path,
        'w',
        driver='GTiff',
        height=zi.shape[0],
        width=zi.shape[1],
        count=1,
        dtype=zi.dtype,
        crs='EPSG:4326',
        transform=transform,
    ) as dst:
        dst.write(zi, 1)

#%%
import matplotlib.animation as animation
import matplotlib.colors as colors
import numpy as np

xmin, xmax = -74, -34  # longitude
ymin, ymax = -34, 6    # latitude
res = 0.05
anos = range(2017, 2024)
bebidas = ['Vinho', 'Cerveja', 'Destilado']

# Grid fixo
yi, xi = np.mgrid[ymin:ymax:res, xmin:xmax:res]
coords = np.vstack([xi.ravel(), yi.ravel()])

for bebida in bebidas:
    # Pré-calcular densidades para achar vmin e vmax globais só da bebida
    densidades = []
    for ano in anos:
        df_filtrado = df_bebidas[
            (df_bebidas['num_ano'] == ano) &
            (df_bebidas['bebida'] == bebida)
        ].dropna(subset=['latitudeFloat', 'longitudeFloat', 'NMVOC (kg)'])

        if df_filtrado.empty:
            continue

        x = df_filtrado['longitudeFloat'].values
        y = df_filtrado['latitudeFloat'].values
        z = df_filtrado['NMVOC (kg)'].values

        kde = gaussian_kde([x, y], weights=z, bw_method=0.05)
        zi = kde(coords).reshape(yi.shape)
        densidades.append(zi)

    if densidades:
        all_dens = np.stack(densidades)
        vmin_global = max(np.min(all_dens[all_dens > 0]), 1e-6)
        vmax_global = np.max(all_dens)
    else:
        vmin_global = 1e-6
        vmax_global = 1

    fig, ax = plt.subplots(figsize=(10, 8))

    def update(ano):
        ax.clear()
        df_filtrado = df_bebidas[
            (df_bebidas['num_ano'] == ano) &
            (df_bebidas['bebida'] == bebida)
        ].dropna(subset=['latitudeFloat', 'longitudeFloat', 'NMVOC (kg)'])

        if df_filtrado.empty:
            estados.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.6)
            ax.set_title(f'{bebida} - {ano} (sem dados)')
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            return

        x = df_filtrado['longitudeFloat'].values
        y = df_filtrado['latitudeFloat'].values
        z = df_filtrado['NMVOC (kg)'].values

        kde = gaussian_kde([x, y], weights=z, bw_method=0.05)
        zi = kde(coords).reshape(yi.shape)
        masked_zi = np.ma.masked_where(zi < vmin_global, zi)

        estados.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.6)
        mesh = ax.pcolormesh(
            xi, yi, masked_zi,
            cmap='inferno',
            norm=colors.LogNorm(vmin=vmin_global, vmax=vmax_global),
            alpha=0.8,
            shading='auto'
        )
        ax.set_title(f'Densidade NMVOC (kg) - {bebida} - {ano} (escala log)')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')

    # Plot inicial para barra de cor fixa
    empty = np.ma.masked_array(np.zeros_like(yi), mask=True)
    estados.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.6)
    mesh = ax.pcolormesh(
        xi, yi, empty,
        cmap='inferno',
        norm=colors.LogNorm(vmin=vmin_global, vmax=vmax_global),
        alpha=0.8,
        shading='auto'
    )
    cbar = fig.colorbar(mesh, ax=ax, label='Densidade estimada (escala log)')

    ani = animation.FuncAnimation(fig, update, frames=anos, repeat=False)

    out_gif = os.path.join(repo_path, 'outputs', f'animacao_emissoes_{bebida.lower()}.gif')
    ani.save(out_gif, writer='imagemagick', fps=0.5)  # fps=0.5 = 2 segundos por frame

    plt.close()



#%%

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import rasterio as rio

# Caminho para o arquivo do MapBiomas
mapBiomasPath = r"C:\Users\glima\OneDrive\Documentos\TCC\projeto\inputs\mapbiomas_10m_collection2_integration_v1-classification_2023.tif"

# Abrindo o raster
src = rio.open(mapBiomasPath)

# Supondo que prod tenha colunas de coordenadas (ex: 'longitudeFloat' e 'latitudeFloat')
prod = gpd.GeoDataFrame(
    prod,
    geometry=gpd.points_from_xy(prod['longitudeFloat'], prod['latitudeFloat']),
    crs="EPSG:4326"  # CRS original dos pontos, se for latitude/longitude
)


usoSolo = pd.read_csv(os.path.join(repo_path, 'inputs', 'usodosolo.csv'), encoding='utf-8', sep='\t')

prod_usosolo = prod.merge(usoSolo,left_on='mapbiomas',right_on='Class_ID')

import matplotlib.pyplot as plt

# Agrupar por uso do solo e bebida
grupo = prod_usosolo.groupby(['Descricao', 'bebida']).size().unstack(fill_value=0)

# Cores personalizadas para as bebidas
cores_bebida = {
    'Vinho': '#800000',     # vinho
    'Cerveja': '#FFD700',   # cerveja
    'Destilado': '#4682B4'  # destilado
}

# Plotar gráfico de barras empilhadas
grupo.plot(
    kind='bar',
    stacked=True,
    figsize=(10, 7),
    color=cores_bebida
)

plt.xticks(rotation=75)
plt.ylabel('Número de Indústrias')
plt.title('Uso do Solo por Tipo de Bebida')
plt.legend(title='Bebida')
plt.tight_layout()
plt.savefig(os.path.join(repo_path, 'figures', f'Quantidade de Indústrias por Uso do Solo.png'), bbox_inches='tight', dpi=300)
plt.show()


#================================




# Contagem total por bebida e uso do solo
contagem = prod_usosolo.groupby(['bebida', 'Descricao']).size().reset_index(name='count')

# Total por bebida para calcular porcentagem
total_bebida = contagem.groupby('bebida')['count'].transform('sum')
contagem['pct'] = contagem['count'] / total_bebida * 100

# Pegar cor única por uso do solo (MapBiomas)
cores_solo = prod_usosolo.drop_duplicates('Descricao').set_index('Descricao')['Color'].to_dict()

# Pivotar para plot
pivot = contagem.pivot(index='bebida', columns='Descricao', values='pct').fillna(0)

# Plot
fig, ax = plt.subplots(figsize=(12, 6))  # aumenta a largura
pivot.plot(
    kind='barh',
    stacked=True,
    color=[cores_solo[col] for col in pivot.columns],
    ax=ax
)

# Título e rótulos
ax.set_xlabel('Porcentagem de Estações')
ax.set_title('Distribuição Percentual do Uso do Solo por Bebida')

# Legenda abaixo
legend = ax.legend(
    title='Uso do Solo',
    bbox_to_anchor=(0.5, -0.2),  # centraliza abaixo
    loc='upper center',
    ncol=4,                      # 4 colunas de legenda (ajuste conforme necessário)
    frameon=False
)

plt.tight_layout(rect=[0, 0.1, 1, 1])  # espaço extra para legenda abaixo

# Salvar imagem
plt.savefig(os.path.join(repo_path, 'figures', f'Uso do Solo por Bebida.png'), bbox_inches='tight', dpi=300)
plt.show()


#================================
# Agrupar por Estado e uso do solo
grupo_estado = prod_usosolo.groupby(['Estado', 'Descricao']).size().reset_index(name='count')

# Total por Estado para calcular percentual
total_estado = grupo_estado.groupby('Estado')['count'].transform('sum')
grupo_estado['pct'] = grupo_estado['count'] / total_estado * 100

# Pivotar para gráfico
pivot_estado = grupo_estado.pivot(index='Estado', columns='Descricao', values='pct').fillna(0)

# Plot
fig, ax = plt.subplots(figsize=(12, 11))
pivot_estado.plot(
    kind='bar',
    stacked=True,
    color=[cores_solo[col] for col in pivot_estado.columns],
    ax=ax
)

# Rótulos e título
ax.set_ylabel('Porcentagem de Indústrias')
ax.set_title('Distribuição Percentual do Uso do Solo por Estado')
ax.legend(
    title='Uso do Solo',
    bbox_to_anchor=(0.5, -0.5),  # legenda centralizada abaixo
    loc='lower center',
    ncol=4,
    frameon=False
)

plt.tight_layout(rect=[0, 0.15, 1, 1])  # espaço extra para legenda
plt.savefig(os.path.join(repo_path, 'figures', 'Uso do Solo por Estado.png'), bbox_inches='tight', dpi=300)
plt.show()


#%% TRABALHO 3

#usar df_bebidas_dpf
























