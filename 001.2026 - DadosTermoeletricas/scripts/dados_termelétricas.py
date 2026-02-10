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
import matplotlib.dates as mdates
import glob
import os

#%% organizando o dado em escala horária

# Caminho para os arquivos CSV
path = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\001.2026 - DadosTermoeletricas\inputs\dados'
files = glob.glob(os.path.join(path, "*.csv"))

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
all_termica['hora'] = all_termica['din_instante'].dt.floor('h')

# agrupar
df_hora = (
    all_termica
    .groupby('hora', as_index=False)
    .sum(numeric_only=True)
)

#%% plotagem

fig, ax = plt.subplots(figsize=(14, 5))

ax.plot(
    df_hora['hora'],
    df_hora['val_geracao'],
    linewidth=0.6,
    alpha=0.9
)

# ===== EIXO X =====

# Ticks principais = ANO
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

# Ticks secundários = MÊS (com nome)
ax.xaxis.set_minor_locator(mdates.MonthLocator())
ax.xaxis.set_minor_formatter(mdates.DateFormatter('%b'))

# Ajuste visual dos rótulos
ax.tick_params(axis='x', which='major', labelsize=11, pad=15)
ax.tick_params(axis='x', which='minor', labelsize=8, rotation=0)

# limites exatos: janeiro do primeiro ano → dezembro do último ano
ano_ini = df_hora['hora'].dt.year.min()
ano_fim = df_hora['hora'].dt.year.max()

xmin = pd.Timestamp(f'{ano_ini}-01-01')
xmax = pd.Timestamp(f'{ano_fim}-12-31 23:59:59')

ax.set_xlim(xmin, xmax)

# ===== GRID =====
ax.grid(True, which='major', linewidth=0.8, alpha=0.7)
ax.grid(True, which='minor', linewidth=0.3, alpha=0.3)

# ===== LABELS =====
ax.set_xlabel('Ano / Mês')
ax.set_ylabel('Geração (MW ou MWh)')
ax.set_title('Geração horária – Usinas Termelétricas (2021–2023)')

plt.tight_layout()
plt.show()

#%% Fator de desagregação horária (por ano)

df_hora['ano'] = df_hora['hora'].dt.year

geracao_anual = (
    df_hora
    .groupby('ano')['val_geracao']
    .sum()
    .rename('geracao_total_ano')
)

df_hora = df_hora.merge(
    geracao_anual,
    on='ano',
    how='left'
)

df_hora['fator_horario'] = (
    df_hora['val_geracao'] / df_hora['geracao_total_ano']
)

#check - tem que dar 1
df_hora.groupby('ano')['fator_horario'].sum()

#%% fator de desagregação diário

# criar data (sem hora)
df_hora['data'] = df_hora['hora'].dt.date

df_diario = (
    df_hora
    .groupby(['ano', 'data'], as_index=False)['val_geracao']
    .sum()
)

# total anual
total_ano = (
    df_diario
    .groupby('ano')['val_geracao']
    .sum()
    .rename('total_ano')
)

df_diario = df_diario.merge(total_ano, on='ano')

df_diario['fator_diario'] = (
    df_diario['val_geracao'] / df_diario['total_ano']
)

df_diario.groupby('ano')['fator_diario'].sum()

#check - tem que dar 1
print(df_diario.groupby('ano')['fator_diario'].sum())

#%% Fator de desagregação semanal 

df_hora['semana'] = df_hora['hora'].dt.isocalendar().week

df_semanal = (
    df_hora
    .groupby(['ano', 'semana'], as_index=False)['val_geracao']
    .sum()
)

total_ano = (
    df_semanal
    .groupby('ano')['val_geracao']
    .sum()
    .rename('total_ano')
)

df_semanal = df_semanal.merge(total_ano, on='ano')

df_semanal['fator_semanal'] = (
    df_semanal['val_geracao'] / df_semanal['total_ano']
)

#check - tem que dar 1
print(df_semanal.groupby('ano')['fator_semanal'].sum())

#%% Fator de desagregação mensal

df_hora['mes'] = df_hora['hora'].dt.month

df_mensal = (
    df_hora
    .groupby(['ano', 'mes'], as_index=False)['val_geracao']
    .sum()
)

total_ano = (
    df_mensal
    .groupby('ano')['val_geracao']
    .sum()
    .rename('total_ano')
)

df_mensal = df_mensal.merge(total_ano, on='ano')

df_mensal['fator_mensal'] = (
    df_mensal['val_geracao'] / df_mensal['total_ano']
)

#check - tem que dar 1
print(df_mensal.groupby('ano')['fator_mensal'].sum())

#%% EXPORTAÇÃO COMPLETA DOS FATORES (todas as colunas)

import os

# pasta de saída
output_path = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\001.2026 - DadosTermoeletricas\outputs\fatoresTermo'
os.makedirs(output_path, exist_ok=True)

# =========================
# FATOR HORÁRIO
# =========================
df_hora.to_csv(
    os.path.join(output_path, 'fator_desagregacao_horario_completo.csv'),
    index=False
)

# =========================
# FATOR DIÁRIO
# =========================
df_diario.to_csv(
    os.path.join(output_path, 'fator_desagregacao_diario_completo.csv'),
    index=False
)

# =========================
# FATOR SEMANAL
# =========================
df_semanal.to_csv(
    os.path.join(output_path, 'fator_desagregacao_semanal_completo.csv'),
    index=False
)

# =========================
# FATOR MENSAL
# =========================
df_mensal.to_csv(
    os.path.join(output_path, 'fator_desagregacao_mensal_completo.csv'),
    index=False
)

#%% calcular media, p05, p95 e mediaRelativa dos fatores de desagregação horário

#
df_hora['horario'] = df_hora['hora'].dt.hour

# Agrupa calculando média, percentil 5 e percentil 95
df_hora_agrupado = df_hora.groupby('horario')['fator_horario'].agg(
    fator_horario='mean',
    p05=lambda x: x.quantile(0.05),
    p95=lambda x: x.quantile(0.95)
).reset_index()

# Calcula o relativo baseado na média
df_hora_agrupado['fator_horario_relativo'] = df_hora_agrupado['fator_horario'] / df_hora_agrupado['fator_horario'].sum()
df_hora_agrupado['fator_horario_p05_relativo'] = df_hora_agrupado['p05'] / df_hora_agrupado['p05'].sum()
df_hora_agrupado['fator_horario_p95_relativo'] = df_hora_agrupado['p95'] / df_hora_agrupado['p95'].sum()

# Configuração do gráfico
plt.plot(df_hora_agrupado['horario'], df_hora_agrupado['fator_horario_relativo'], 
         label='Média Relativa', color='blue', lw=2)

plt.xlabel('Hora do Dia')
plt.ylabel('Fator Relativo')
plt.title('Variação Horária Relativa')
plt.xticks(range(0, 24))
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend()
plt.tight_layout()







