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

from functions import (
    sector_color,
    plot_emissoes_por_poluente,
    plot_mapa_emissoes_por_poluente,
    plot_barras_impacto_por_poluente
)
import matplotlib.pyplot as plt

plt.rcParams["font.family"] = "DejaVu Sans"

#%% Ajuste da base de dados

#define caminho dos arquivos
repopath = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\005.2026 - AnaliseEmissoresMinorMajor'
inputs = os.path.join(repopath,'inputs')
outputs = os.path.join(repopath,'outputs')
figures = os.path.join(repopath,'figures')

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

#clasasificação em major, minor e medium
inv['impact'] = np.where((inv[pol] > 100).any(axis=1),'major',
                         np.where((inv[pol] < 5).all(axis=1), 'minor',
                                  'medium'))

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
pol_interest = ['PM10', 'PM25','SOx', 'NOx', 'CO', 'TSP','Pb']
color_pol_interest = ['black','blue','orange','green','grey','red','brown']

#classifica cada indústria para uma cor padronizada
color_by_sector = sector_color(inv_gdf, col_setor='SETOR') 

years = sorted(pd.to_numeric(inv['ANO'].dropna().astype(int).unique()))

#importa shape do br
br_estado = gpd.read_file(r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\input_base\BR_UF_2025.shp')
br_regiao = br_estado[['geometry','SIGLA_RG','NM_REGIAO']].dissolve(by='NM_REGIAO', as_index=False)

#%% Análises

#Comparação de emissões anuais por setor e poluente
plot_emissoes_por_poluente(
    inv=inv,
    pol_interest=pol_interest,
    figures=figures,
)

#Comparação de emissões de poluente por estado
plot_mapa_emissoes_por_poluente(
    inv_gdf=inv_gdf,
    br_estado=br_estado,
    br_regiao=br_regiao,
    pol_interest=pol_interest,
    figures=figures,
)

plot_barras_impacto_por_poluente(
    inv=inv,
    pol_interest=pol_interest,
    figures=figures,
)

#%% Distribuição espacial das emissões por poluente

'''
3. setor que mais emite por região (mapa do br)
4. emissões por estado e por poluente (+tendencia nas cores das barras)
5. emissões pixeladas por poluente (média anual) 12x12
6. gráfico de tendencias por poluente (12x12)
'''
















