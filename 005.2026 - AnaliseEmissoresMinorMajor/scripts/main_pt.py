# -*- coding: utf-8 -*-
"""
Created on Mon Mar 30 13:59:11 2026

@author: glima
"""

#%% Bicliotecas
import os
import pandas as pd
import numpy as np
import geopandas as gpd

from functions_pt import (
    plot_mapa_emissoes_por_poluente,
    plot_mapas_impacto,
    plot_barrash_impacto_poluentes,
    plot_mosaico_pixels_poluentes,
    plot_tabela_top3_setores_estado,
    plot_heatmap_setor_poluente
    )


import matplotlib.pyplot as plt

plt.rcParams["font.family"] = "DejaVu Sans"

#%% Ajuste da base de dados

#define caminho dos arquivos
repopath = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\005.2026 - AnaliseEmissoresMinorMajor'
inputs = os.path.join(repopath,'inputs')
outputs = os.path.join(repopath,'outputs')
figures = os.path.join(repopath,'figures','pt')

#importa inventário, definindo tipo de algumas colunas
inv = pd.read_csv(os.path.join(inputs,'emission_total_light_v2.csv'),
                  dtype={'SNAP':'str',
                         'Technology':'str',
                         'Abatement':'str',
                         'Fuel':'str',
                         'Region':'str'})

#lista com todos os poluentes do material
pol = ['TSP', 'PM10', 'SOx', 'NMVOC', 'CO','NOx', 'PM25', 'BC', 'HCB',
             'Benzobfluoranthene', 'Indenopyrene','Benzokfluoranthene',
             'Benzopyrene', 'PCB', 'PCDDF', 'Cd', 'Pb', 'Cr','Hg', 'As', 'Zn',
             'Se', 'Ni', 'Cu', 'NH3']

# índice do último ano de cada empresa
idx_ultimo = inv.groupby(['CPF_CNPJ', 'SIGLA_UF'])['ANO'].idxmax()
inv_ultimo = inv.loc[idx_ultimo].copy()

# classifica com base no último ano de cada empresa
inv_ultimo['impact'] = np.where(
    (inv_ultimo[pol] > 100).any(axis=1), 'major',
    np.where((inv_ultimo[pol] < 5).all(axis=1), 'minor', 'medium')
)

# traz para o df completo
inv = inv.drop(columns='impact', errors='ignore')
inv = inv[inv['ANO']!=2024]
inv = inv.merge(
    inv_ultimo[['CPF_CNPJ', 'SIGLA_UF', 'impact']],
    on=['CPF_CNPJ', 'SIGLA_UF'],
    how='left'
)

inv = inv.rename(columns={
    'PM10': 'MP10',
    'PM25': 'MP2.5',
    'TSP':'PTS'
})

# transformar df em gdf, para facilitar análises espaciais
inv_gdf = gpd.GeoDataFrame(
    inv.copy(),
    geometry=gpd.points_from_xy(inv['Longitude'], inv['Latitude']),
    crs='EPSG:4326'
)

# dicionário que classifica cada estado em região do país
mapa_regiao = {
    'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte',
    'TO': 'Norte',     'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste',
    'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
    'DF': 'Centro-oeste', 'GO': 'Centro-oeste', 'MT': 'Centro-oeste', 'MS': 'Centro-oeste',
    'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
}

#cria coluna com a região do país de cada item
inv_gdf['NM_REGIAO'] = inv_gdf['SIGLA_UF'].map(mapa_regiao)

#determina os poluentes de interesse a serem analisados
pol_interest = ['MP10', 'MP2.5','SOx', 'NOx', 'CO', 'PTS','Pb']
color_pol_interest = ['black','blue','orange','green','grey','red','brown']

#classifica cada indústria para uma cor padronizada

years = sorted(pd.to_numeric(inv['ANO'].dropna().astype(int).unique()))

#importa shape do br
br_estado = gpd.read_file(r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\input_base\BR_UF_2025.shp')
br_regiao = br_estado[['geometry','SIGLA_RG','NM_REGIAO']].dissolve(by='NM_REGIAO', as_index=False)

#%% Análises

#Mapa do brasil com pontos por impacto por região
plot_mapas_impacto(
    inv_gdf=inv_gdf,
    br_estado=br_estado,
    br_regiao=br_regiao,
    figures=figures,
)

# Gráfico de barras horizontal com emissão por poluente e por ctegoria de emissor
plot_barrash_impacto_poluentes(
    inv=inv,
    pol_interest=pol_interest,
    color_pol_interest=color_pol_interest,
    figures=figures,
)

#mosaico com emissões pixeladas
plot_mosaico_pixels_poluentes(
    inv_gdf=inv_gdf,
    br_estado=br_estado,
    br_regiao=br_regiao,
    pol_interest=pol_interest,
    pol_destaque='MP10',          # opcional, já é default
    figures=figures,
)

#Auxiliar: Comparação de emissões de poluente por estado, util para descrever
plot_mapa_emissoes_por_poluente(
    inv_gdf=inv_gdf,
    br_estado=br_estado,
    br_regiao=br_regiao,
    pol_interest=pol_interest,
    figures=figures,
)

# Seção 3.4.1 — Table Y
plot_tabela_top3_setores_estado(
    inv=inv,
    figures=figures,
    pol_interest=pol_interest,
)

# Seção 3.4.2 — Figure Z
plot_heatmap_setor_poluente(
    inv=inv,
    figures=figures,
    pol_interest=pol_interest,
)

