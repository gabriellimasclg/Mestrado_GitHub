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

#%% Perfil Horário do fator relativo

df_hora['horario'] = df_hora['hora'].dt.hour

# Agrupa calculando média, percentil 5 e percentil 95
df_hora_agrupado = df_hora.groupby('horario')['fator_horario'].agg(
    fator_horario='mean',
    p05=lambda x: x.quantile(0.05),
    p95=lambda x: x.quantile(0.95)
).reset_index()

# Calcula o relativo baseado na média
df_hora_agrupado['fator_horario_relativo'] = df_hora_agrupado['fator_horario'] / df_hora_agrupado['fator_horario'].sum()

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
plt.savefig(os.path.join(fig_path,'Perfil Horário do fator relativo.png'),dpi=300)


#%% Perfil Horário Interanual do fator relativo

# Extrair ano
df_hora['ano'] = df_hora['hora'].dt.year

plt.figure(figsize=(12, 6))

# Plotar linhas finas para cada ano
for ano in sorted(df_hora['ano'].unique()):
    df_ano = df_hora[df_hora['ano'] == ano]
    # Agrupa por hora para aquele ano específico
    agrupado_ano = df_ano.groupby('horario')['fator_horario'].mean().reset_index()
    # Normaliza pela soma do próprio ano para ver o formato do perfil
    relativo_ano = agrupado_ano['fator_horario'] / agrupado_ano['fator_horario'].sum()
    
    plt.plot(agrupado_ano['horario'], relativo_ano, color='gray', lw=0.5, alpha=0.5)

# Plotar a média geral (que você já calculou) em destaque
plt.plot(df_hora_agrupado['horario'], df_hora_agrupado['fator_horario_relativo'], 
         label='Média Geral (2017-2024)', color='blue', lw=3)

plt.title('Variação Horária: Linhas Anuais vs Média Geral')
plt.xlabel('Hora do Dia')
plt.ylabel('Fator Relativo')
plt.xticks(range(0, 24))
plt.legend()
plt.grid(True, linestyle='--', alpha=0.3)
plt.show()
plt.savefig(os.path.join(fig_path,'Perfil Horário Interanual do fator relativo.png'),dpi=300)


#%% Perfil Semanal do fator relativo

import matplotlib.pyplot as plt

# 1. Extrair o dia da semana (0=Segunda, 6=Domingo)
df_hora['dia_semana_num'] = df_hora['hora'].dt.dayofweek
# Mapeamento para nomes em português
dias_nomes = {0: 'Seg', 1: 'Ter', 2: 'Qua', 3: 'Qui', 4: 'Sex', 5: 'Sab', 6: 'Dom'}

# 2. Agrupamento por dia da semana
df_semana_agrupado = df_hora.groupby('dia_semana_num')['fator_horario'].agg(
    fator_medio='mean',
    p05=lambda x: x.quantile(0.05),
    p95=lambda x: x.quantile(0.95)
).reset_index()

# 3. Normalização Consistente
# Dividimos pela soma das médias para que a linha azul (média) some 1
total_referencia = df_semana_agrupado['fator_medio'].sum()

df_semana_agrupado['rel_medio'] = df_semana_agrupado['fator_medio'] / total_referencia

# 4. Gráfico
plt.figure(figsize=(10, 5))

plt.plot(df_semana_agrupado['dia_semana_num'], df_semana_agrupado['rel_medio'], 
         label='Média Diária', color='blue', lw=3, marker='o')


# Ajustar os nomes no eixo X
plt.xticks(ticks=range(7), labels=[dias_nomes[i] for i in range(7)])

plt.title('Perfil Semanal de Variação Relativa 2017-2024')
plt.xlabel('Dia da Semana')
plt.ylabel('Fator Relativo')
plt.grid(True, axis='y', linestyle=':', alpha=0.7)
plt.legend()
plt.tight_layout()

plt.savefig(os.path.join(fig_path,'Perfil dos dias da semana do fator relativo.png'),dpi=300)


#%% Perfil Semanal Interanual do fator relativo

plt.figure(figsize=(10, 5))

for ano in sorted(df_hora['ano'].unique()):
    df_ano = df_hora[df_hora['ano'] == ano]
    agrupado_ano = df_ano.groupby('dia_semana_num')['fator_horario'].mean().reset_index()
    # Normaliza pela soma do ano
    total_ano = agrupado_ano['fator_horario'].sum()
    
    plt.plot(agrupado_ano['dia_semana_num'], agrupado_ano['fator_horario'] / total_ano, 
             color='gray', lw=0.5, alpha=0.5)

plt.plot(df_semana_agrupado['dia_semana_num'], df_semana_agrupado['rel_medio'], 
         label='Média Semanal Geral', color='blue', lw=3, marker='o')

plt.xticks(ticks=range(7), labels=[dias_nomes[i] for i in range(7)])
plt.title('Perfil Semanal: Variação entre Anos (2017-2024)')
plt.legend()
plt.show()

plt.savefig(os.path.join(fig_path,'Perfil dos dias da semana interanual do fator relativo.png'),dpi=300)

#%% Perfil mensal do fator relativo

# 1. Extrair o mês (1 a 12)
df_hora['mes'] = df_hora['hora'].dt.month

# 2. Agrupamento Mensal
df_mensal_agrupado = df_hora.groupby('mes')['fator_horario'].agg(
    fator_medio='mean',
    p05=lambda x: x.quantile(0.05),
    p95=lambda x: x.quantile(0.95)
).reset_index()

# 3. Normalização Técnica (Denominador Único)
# Isso garante que a hierarquia Vermelho > Azul > Verde seja respeitada
total_referencia = df_mensal_agrupado['fator_medio'].sum()

df_mensal_agrupado['rel_medio'] = df_mensal_agrupado['fator_medio'] / total_referencia

# 4. Gráfico
plt.figure(figsize=(12, 6))

# Linhas de tendência
plt.plot(df_mensal_agrupado['mes'], df_mensal_agrupado['rel_medio'], 
         label='Média Mensal', color='blue', lw=3, marker='s')

# Ajustes de Eixo e Legenda
plt.title('Sazonalidade Mensal Relativa 2017-2024', fontsize=14)
plt.xlabel('Mês do Ano')
plt.ylabel('Fator Relativo')
plt.xticks(range(1, 13)) # Garante que apareçam os 12 meses
plt.grid(True, linestyle=':', alpha=0.6)
plt.legend(loc='upper right')
plt.tight_layout()

plt.savefig(os.path.join(fig_path,'Perfil mensal do fator relativo.png'),dpi=300)

#%% Perfil mensal interanual do fator relativo

plt.figure(figsize=(12, 6))

for ano in sorted(df_hora['ano'].unique()):
    df_ano = df_hora[df_hora['ano'] == ano]
    agrupado_ano = df_ano.groupby('mes')['fator_horario'].mean().reset_index()
    total_ano = agrupado_ano['fator_horario'].sum()
    
    plt.plot(agrupado_ano['mes'], agrupado_ano['fator_horario'] / total_ano, 
             color='gray', lw=0.5, alpha=0.5)

plt.plot(df_mensal_agrupado['mes'], df_mensal_agrupado['rel_medio'], 
         label='Média Mensal Geral', color='blue', lw=3, marker='s')

plt.title('Sazonalidade Mensal: Variação entre Anos (2017-2024)')
plt.xticks(range(1, 13))
plt.legend()
plt.show()

plt.savefig(os.path.join(fig_path,'Perfil mensal interanual do fator relativo.png'),dpi=300)
