"""
Created on Mon Feb  9 09:00:41 2026

Base de dados extraída de https://sidra.ibge.gov.br/tabela/8885
Realizado o cálculo de um valor de desagregação temporal mensal, que será
posteriormente utilizado para desagregação de valores anuais

@GitHub: https://github.com/gabriellimasclg
"""
#Importacao e caminho/base

import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np

repopath = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\003.2026 - DesagregacaoSIDRA8885'


#%% Organização dos dados do SIDRA IBGE

# Carregar os dados
df_sidra_raw = pd.read_csv(os.path.join(repopath,'inputs','Tabela 8885; Produção Física Industrial, por grupos e classes industriais selecionados; 2022 = 100.csv'), sep=None, engine='python')

# Reorganizar o DataFrame (de colunas para linhas)
id_col = df_sidra_raw.columns[0] # "Grupos e classes industriais"
df_sidra = df_sidra_raw.melt(id_vars=[id_col], var_name='Mes_Ano', value_name='Producao')

# Limpar os valores numéricos
# Substitui vírgula por ponto e converte para float
df_sidra['Producao'] = pd.to_numeric(df_sidra['Producao'].astype(str).str.replace(',', '.'), errors='coerce')

# Converter a coluna de texto para Data real
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

df_sidra['datetime'] = df_sidra['Mes_Ano'].apply(parse_data)
df_sidra['year'] = df_sidra['datetime'].dt.year
df_sidra['month'] = df_sidra['datetime'].dt.month

# Remover linhas sem dados (ex: cabeçalhos extras ou anos incompletos se houver)
df_sidra = df_sidra.dropna(subset=['Producao', 'year']) # Cuidar com isso dps para média sair certo

df_sidra = df_sidra.rename(columns={'Producao':'Producao_mensal'})

#%% Desegregação da produção mensal por hora

# Calcular o número de horas de cada mês (considera anos bissextos)
df_sidra['horas_no_mes'] = df_sidra['datetime'].dt.days_in_month * 24

# Dividir a produção pelo total de horas do mês
df_sidra['Producao_horaria'] = df_sidra['Producao_mensal'] / df_sidra['horas_no_mes']

# Expandir o DataFrame: repete cada linha 'n' vezes (onde n = horas_no_mes)
df_sidra_hourly = df_sidra.loc[df_sidra.index.repeat(df_sidra['horas_no_mes'])].copy()

# Ajustar a coluna Data para incrementar de 1 em 1 hora dentro de cada grupo
# O cumcount() gera a sequência 0, 1, 2... para cada repetição da linha original
df_sidra_hourly['datetime'] = df_sidra_hourly['datetime'] + pd.to_timedelta(df_sidra_hourly.groupby(level=0).cumcount(), unit='h')

#%% Associar nfr com prod_code

df_sidra_hourly['prod_code']=df_sidra_hourly['﻿Grupos e classes industriais'].str.split(' ').str[0]

# Base de dados feita manualmente que associa 
nfr_prodcode = pd.read_csv(os.path.join(repopath,'inputs','manual_nft_prodcode.csv'),
                           encoding='latin1',
                           index_col=None,
                           dtype={'prod_code': str})

# Realizando o merge (usando how='left' para manter todos os dados de df_sidra)
df_nfr_prodcode = df_sidra_hourly.merge(nfr_prodcode, on='prod_code', how='outer')

#%% Criação dos perfís mensais, horários e diários

#Extraindo horas e dias da semana do df
df_nfr_prodcode['hour'] = df_nfr_prodcode['datetime'].dt.hour
df_nfr_prodcode['dayofweek'] = df_nfr_prodcode['datetime'].dt.dayofweek

def create_profile(df, group_cols):
    
    # Calcula média, min e max da produção para cada grupo
    profile = df.groupby(group_cols)['Producao_horaria'].agg(['mean', 'min', 'max']).reset_index()
    
    # Calcula o fator baseado na soma das médias (para a linha principal)
    # E escala o min/max pelo mesmo denominador para manter a proporção
    group_sum = profile.groupby('nfr')['mean'].transform('sum')
    
    profile['factor'] = profile['mean'] / group_sum
    profile['factor_min'] = profile['min'] / group_sum
    profile['factor_max'] = profile['max'] / group_sum
    return profile

# Aplica para cada escala
hourly_profile = create_profile(df_nfr_prodcode, ['nfr', 'hour'])
dayofweek_profile = create_profile(df_nfr_prodcode, ['nfr', 'dayofweek'])
monthly_profile = create_profile(df_nfr_prodcode, ['nfr', 'month'])


#%% Exportação dos perfis

profiles_path = os.path.join(repopath,'outputs', 'profiles')
os.makedirs(profiles_path, exist_ok=True)

def export_nfr_profiles(df_profile, profile_name, col_name):
    # Cria a pasta para o tipo de perfil (hourly, weekly, monthly)
    folder = os.path.join(profiles_path, profile_name)
    os.makedirs(folder, exist_ok=True)
    
    for nfr_code, group in df_profile.groupby('nfr'):
        # Cria nome do arquivo
        filename = f"{nfr_code}_{profile_name}.csv"
        # Exporta apenas a coluna do tempo e o factor
        group[[col_name, 'factor']].to_csv(os.path.join(folder, filename), index=False)

# 4. Executar exportações individuais
export_nfr_profiles(hourly_profile, 'hourly', 'hour')
export_nfr_profiles(dayofweek_profile, 'weekly', 'dayofweek')
export_nfr_profiles(monthly_profile, 'monthly', 'month')

print(f"Exportação concluída em: {profiles_path}")

#%% Gráfico para cada perfil criado

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