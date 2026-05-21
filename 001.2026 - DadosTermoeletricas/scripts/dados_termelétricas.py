# -*- coding: utf-8 -*-
"""
Foram elaborados fatores de desagregação temporal diário, semanal, mensal e
horário, a partir da normalização anual da geração elétrica, preservando a
massa anual e refletindo o perfil operacional das usinas termelétricas.

Fonte dos CSVs: https://dados.ons.org.br/dataset/geracao-usina-2
"""
#Importando os pacotes

import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

#%% organizando o dado em escala horária

# Caminho para os arquivos CSV
repopath = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\001.2026 - DadosTermoeletricas'
path = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\001.2026 - DadosTermoeletricas\inputs\dados'
files = glob.glob(os.path.join(path, "*.csv"))
fig_path = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\001.2026 - DadosTermoeletricas\figures'

# Lista para armazenar os DataFrames
df_list = []

for file in files:
    df = pd.read_csv(file, sep=";")
    df_list.append(df)

# Concatena todos os DataFrames em um só
all_df = pd.concat(df_list, ignore_index=True)

all_df['nom_tipousina'].unique()

all_termica = all_df [all_df['nom_tipousina'] == 'TÉRMICA']

# garantir datetime
all_termica['din_instante'] = pd.to_datetime(all_termica['din_instante'])

# criar coluna horária
all_termica['datetime'] = all_termica['din_instante'].dt.floor('h')

# agrupar
df_hora = (
    all_termica
    .groupby('datetime', as_index=False)
    .sum(numeric_only=True)
)

#%% Desagregação

#Colocando o NFR adequado
df_hora['nfr'] = '1.A.1.a'

#Extraindo horas e dias da semana do df
df_hora['hour'] = df_hora['datetime'].dt.hour
df_hora['dayofweek'] = df_hora['datetime'].dt.dayofweek
df_hora['month'] = df_hora['datetime'].dt.month

# Perfil horário
hourly_profile = (
    df_hora.groupby(['nfr', 'hour'])['val_geracao']
    .agg(mean='mean', min='min', max='max')
    .reset_index()
)

# Perfil de dias da semana
dayofweek_profile = (
    df_hora.groupby(['nfr', 'dayofweek'])['val_geracao']
    .agg(mean='mean', min='min', max='max')
    .reset_index()
)

# Perfil mensal
monthly_profile = (
    df_hora.groupby(['nfr', 'month'])['val_geracao']
    .agg(mean='mean', min='min', max='max')
    .reset_index()
)

# Normalizar todos de uma vez
for profile in [hourly_profile, dayofweek_profile, monthly_profile]:
    soma = profile.groupby('nfr')['mean'].transform('sum')
    profile['factor']     = profile['mean'] / soma
    profile['factor_min'] = profile['min']  / soma
    profile['factor_max'] = profile['max']  / soma
        
#%% Plotagem

# Configurações estéticas gerais
plt.rcParams.update({'font.size': 10, 'axes.grid': True, 'grid.alpha': 0.3})

fig, axes = plt.subplots(3, 1, figsize=(9, 12))
fig.subplots_adjust(hspace=0.4)

# 1. Perfil Horário
axes[0].plot(hourly_profile['hour'], hourly_profile['factor'], marker='o', color='tab:blue', linewidth=1.5)
axes[0].set_title(r'Perfil Horário - NFR: $1.A.1.a$')
axes[0].set_xlabel('Hora do Dia')
axes[0].set_ylabel('Fator de Escala')
axes[0].set_xticks(range(0, 24))

# 2. Perfil por Dia da Semana
dias = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom']
axes[1].plot(dayofweek_profile['dayofweek'], dayofweek_profile['factor'], color='tab:orange', alpha=0.8)
axes[1].set_title('Perfil por Dia da Semana')
axes[1].set_xticks(range(7))
axes[1].set_xticklabels(dias)
axes[1].set_ylabel('Fator de Escala')

# 3. Perfil Mensal
meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
axes[2].plot(monthly_profile['month'], monthly_profile['factor'], marker='s', color='tab:green', linestyle='--')
axes[2].set_title('Perfil Mensal')
axes[2].set_xticks(range(1, 13))
axes[2].set_xticklabels(meses)
axes[2].set_ylabel('Fator de Escala')

plt.tight_layout()
plt.show()

#%% Exportação

folder = os.path.join(repopath,'outputs','profile')
os.makedirs(folder, exist_ok=True)

hourly_profile[['hour','factor']].to_csv(os.path.join(folder, "1.A.1.a_hourly.csv"), index=False)
monthly_profile[['month','factor']].to_csv(os.path.join(folder, "1.A.1.a_monthly.csv"), index=False)
dayofweek_profile[['dayofweek','factor']].to_csv(os.path.join(folder, "1.A.1.a_weekly.csv"), index=False)

import numpy as np

figure_path = os.path.join(repopath,'figures', 'profiles')
os.makedirs(figure_path, exist_ok=True)

def export_graphic_profiles(df_profile, profile_name, col_name):
    folder = os.path.join(figure_path, profile_name)
    os.makedirs(folder, exist_ok=True)
    
    for nfr_code, group in df_profile.groupby('nfr'):
        filename = f"{nfr_code}_{profile_name}.png"
        
        fig, ax = plt.subplots(figsize=(10, 5))
        
        # 1. A mancha (fill_between) - IMPORTANTE: passar group[col_name]
        ax.fill_between(group[col_name], 
                        group['factor_min'], 
                        group['factor_max'], 
                        color='blue', alpha=0.2, label='Min-Max')
        
        # 2. A linha da média
        ax.plot(group[col_name], group['factor'], color='blue', lw=2, label=f'{profile_name.capitalize()} factor - NFR: {nfr_code}')
        
        ax.set_xlabel(col_name)
        ax.set_ylabel('factor')
        #ax.set_title(f"{profile_name.capitalize()} profile - NFR: {nfr_code}")
        ax.legend(loc='lower center',ncols=2,bbox_to_anchor=(0.5,-0.2),frameon=False)
        # Pega o menor e o maior valor da coluna de tempo (ex: hour) e gera o range
        ax.set_xticks(np.arange(group[col_name].min(), group[col_name].max() + 1, 1))
        plt.tight_layout()
        ax.grid(
            True,
            axis='both',       # 'x', 'y' ou 'both'
            which='major',     # 'major', 'minor' ou 'both'
            color='gray',
            linestyle='--',
            linewidth=0.5,
            alpha=0.7
        )
        plt.savefig(os.path.join(folder, filename))
        plt.close()
        
export_graphic_profiles(hourly_profile, 'hourly', 'hour')
export_graphic_profiles(dayofweek_profile, 'weekly', 'dayofweek')
export_graphic_profiles(monthly_profile, 'monthly', 'month')