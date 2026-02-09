# -*- coding: utf-8 -*-
"""
Created on Fri Feb  6 09:00:20 2026

@author: glima
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import glob
import os

#%% importando materiais

path = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\MaioresEmissoresParaDesagregação\inputs\emission_total_light_v2.csv'

inv = pd.read_csv(path)

#LER SNAP COMO OBJETO
# agrupar
inv_setor = (
    inv
    .groupby('SETOR', as_index=False)
    .sum(numeric_only=True)
)

inv_setor_clean = inv_setor.drop(['Latitude','Longitude','ANO','CD_MUN','TIER'], axis=1, errors='ignore')

inv_setor_clean.to_csv(r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\MaioresEmissoresParaDesagregação\outputs\emissao_agg_por_setor.csv',encoding='latin1')
