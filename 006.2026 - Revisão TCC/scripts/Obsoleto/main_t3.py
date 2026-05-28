# -*- coding: utf-8 -*-
"""
Created on Tue Jun 17 13:57:56 2025

@author: glima
"""

#%%=========================== Bibliotecas ==================================
import os
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import box
from download_database import download_ibama_ctf_data
from import_database import ibama_production_data, import_products_code
from merge_filter_df import merge_cnpj_prod, conecta_ibama_ef, converter_para_hl
from clean_text import clean_text

#%%===============================TCC========================================

# Caminho da pasta do projeto
repo_path = os.path.dirname(os.getcwd())

# Pega dados de CNPJ + coordenadas
df_ibama_cnpj = download_ibama_ctf_data(repo_path) #dados de cnpj
df_ibama_cnpj['Municipio'] = df_ibama_cnpj['Municipio'].str.replace(
    r"TRAJANO DE MORAIS", "TRAJANO DE MORAES", regex=True
    )

# DF com Dados de produção com CNPJ + Código de Produto + Produção
df_ibama_prod = ibama_production_data(repo_path) #dados de produção
df_ibama_prod['mv.nom_municipio'] = df_ibama_prod['mv.nom_municipio'].str.strip()
df_ibama_prod['mv.nom_municipio'] = df_ibama_prod['mv.nom_municipio'].str.replace(
    r"SANT'? ?ANA DO LIVRAMENTO", "SANTANA DO LIVRAMENTO", regex=True
    )
df_ibama_prod['mv.nom_municipio'] = df_ibama_prod['mv.nom_municipio'].str.replace(
    r"PRESIDENTE CASTELLO BRANCO", "PRESIDENTE CASTELO BRANCO", regex=True
    )

# Falar que tem algumas cidades que ainda não conectaram e ver o que fazer quanto
# a cpf e cnpj --> ideia é pegar coordenada do municipio geral

df_dd = df_ibama_prod['mv.nom_municipio'].value_counts()

# DF com Produção + Código de produto + Coordenadas
df_ibama = merge_cnpj_prod(df_ibama_cnpj,df_ibama_prod) #mesclar p obter coordenadas e código de atividade

#Base de dados com todos os Códigos de Produto
# ela foi exportada para classificar MANUALMENTE Código de Produto (IBAMA) vs NFR+Table (EEA)
cod_produto= import_products_code(repo_path)


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

#%%===========================TRABALHOS DO LEO==================================

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

# 1. Encontrar TODAS as colunas que contêm listas
colunas_com_listas = []
for col in df_bebidas.columns:
    # Checa o tipo do primeiro valor não-nulo da coluna
    if not df_bebidas[col].dropna().empty:
        if isinstance(df_bebidas[col].dropna().iloc[0], list):
            colunas_com_listas.append(col)

if colunas_com_listas:
    print(f"Colunas problemáticas encontradas: {colunas_com_listas}")

    # 2. Criar a lista de colunas para analisar, excluindo TODAS as que contêm listas
    colunas_para_analisar = [col for col in df_bebidas.columns if col not in colunas_com_listas]

    # 3. Executar o drop_duplicates com o subset correto
    df_bebidas_sem_duplicatas = df_bebidas.drop_duplicates(subset=colunas_para_analisar)
    
    print("\nDuplicatas removidas com sucesso!")
    print("DataFrame original tinha:", len(df_bebidas), "linhas.")
    print("DataFrame resultante tem:", len(df_bebidas_sem_duplicatas), "linhas.")

else:
    print("Nenhuma coluna com listas foi encontrada. O erro pode ter outra origem.")

# Define a condição diretamente no .loc e atualiza as duas colunas de uma vez
df_bebidas.loc[
    (df_bebidas['CNPJ'] == '11169030000223') & 
    (df_bebidas['unidade_medida'] == 'Mil Métros Cúbicos (KM3)'), 
    ['unidade_medida', 'ig_unidmed']
] = ['Metro Cúbico', 'M3']


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

#%% TRABALHO 03 DO LEO
import geopandas as gpd
import pandas as pd
import xarray as xr
import numpy as np

# Carregar os estados do Brasil via GeoJSON
url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
estados = gpd.read_file(url)
estados['Estado'] = estados['name'].apply(clean_text)

#classificar df_bebidas por bebida 
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

#criar grid

def CreateGrid(Tam_pixel,minx,maxx,miny,maxy):
    
    x_coords = np.arange(minx, maxx, Tam_pixel)
    y_coords = np.arange(miny, maxy, Tam_pixel)
    grid_cells = [box(x, y, x + Tam_pixel, y + Tam_pixel) for x in x_coords for y in y_coords]
    gridGerado = gpd.GeoDataFrame(geometry=grid_cells, crs='EPSG:4674')
    
    # Extraindo coordenadas do centroide
    gridGerado['lon'] = gridGerado.geometry.centroid.x
    gridGerado['lat'] = gridGerado.geometry.centroid.y
    
    # Determinando as coordenadas de cada célula 
    xx, yy = np.meshgrid(np.sort(np.unique(gridGerado.lon)), #Cria matriz 2d, em ordem crescente dos valores unidos de lonlat
                         np.sort(np.unique(gridGerado.lat)))
    
    return gridGerado, xx, yy

xmin, xmax = -74, -34  # longitude
ymin, ymax = -34, 6    # latitude
res = 1  # resolução do pixel em graus

grid_base, xx, yy = CreateGrid(res,xmin,xmax,ymin,ymax)
fig,ax = plt.subplots()
estados.boundary.plot(ax=ax)
grid_base.boundary.plot(ax=ax)


# Assumindo que 'df_bebidas' já está carregado e tratado como antes
# Removendo linhas onde a geolocalização não é válida
df_bebidas_geo = df_bebidas.dropna(subset=['Latitude', 'Longitude', 'NMVOC (kg)'])

# Criando o GeoDataFrame (forma correta)
gdf_emissoes = gpd.GeoDataFrame(
    df_bebidas_geo,
    geometry=gpd.points_from_xy(
        df_bebidas_geo.Longitude.str.replace(',', '.').astype(float),
        df_bebidas_geo.Latitude.str.replace(',', '.').astype(float)
    ),
    crs='EPSG:4674'  # Usando o mesmo CRS da grade!
)

# Faz a junção espacial. Para cada ponto de emissão, ele informa em qual célula da grade (pelo 'index_right') ele caiu.
pontos_na_grade = gpd.sjoin(gdf_emissoes, grid_base, how="inner", predicate="within")

# Agrupando por célula (grid), ano e bebida - agora com soma total
emissoes_agregadas_por_bebida = (
    pontos_na_grade
    .groupby(['index_right', 'num_ano', 'bebida'], as_index=False)['NMVOC (kg)']
    .sum()
    .rename(columns={'index_right': 'grid_id'})
)

emissoes_agregadas_por_bebida.rename(columns={'index_right': 'grid_id'}, inplace=True)

print("Emissões agregadas por célula e por ano:")
print(emissoes_agregadas_por_bebida.head())

# Pegando os anos únicos do seu dataset para a dimensão de tempo
anos = np.sort(emissoes_agregadas_por_bebida['num_ano'].unique())
tipos_bebida = emissoes_agregadas_por_bebida['bebida'].unique()
lat_coords = yy[:, 0]
lon_coords = xx[0, :]

# As coordenadas de lat e lon são as colunas da sua grade xx e yy
# No entanto, para o xarray, precisamos dos vetores 1D que formam a grade
lat_coords = yy[:, 0]
lon_coords = xx[0, :]

# 1. Crie um DataArray vazio com as dimensões (tempo, lat, lon) preenchido com NaN
#    Usamos as coordenadas que acabamos de definir.
emissoes_cubo_4d = xr.DataArray(
    np.nan,
    dims=('bebida', 'time', 'lat', 'lon'),
    coords={'bebida': tipos_bebida, 'time': anos, 'lat': lat_coords, 'lon': lon_coords}
)

# 2. Preencha o cubo com os dados de emissões agregadas
for _, row in emissoes_agregadas_por_bebida.iterrows():
    grid_id = row['grid_id']
    ano = row['num_ano']
    bebida = row['bebida']
    emissao = row['NMVOC (kg)']

    lat_da_celula = grid_base.loc[grid_id, 'lat']
    lon_da_celula = grid_base.loc[grid_id, 'lon']
    
    emissoes_cubo_4d.loc[dict(bebida=bebida, time=ano, lat=lat_da_celula, lon=lon_da_celula)] = emissao

ds_emissoes_completo = xr.Dataset({'nmvoc_emissions': emissoes_cubo_4d})
print(ds_emissoes_completo)

print("\nEstrutura do seu Data Cube (xarray.Dataset):")
print(ds_emissoes_completo)

#==============================

import matplotlib.pyplot as plt
import os
from matplotlib.colors import LogNorm # Precisamos importar a classe LogNorm

# --- PREPARAÇÃO ---
# 1. Definir o caminho da pasta para salvar as figuras
figures_path = os.path.join(repo_path, 'figures')
os.makedirs(figures_path, exist_ok=True) # Cria a pasta se ela não existir

# --- LOOP PARA PLOTAR E SALVAR ---
print("Iniciando a geração dos mapas de emissão...")

# 2. Iterar por cada tipo de bebida
for bebida_tipo in ds_emissoes_completo['bebida'].values:
    
    # --- PREPARAÇÃO POR BEBIDA ---
    # Fatiar todos os dados para a bebida atual
    data_bebida_slice = ds_emissoes_completo['nmvoc_emissions'].sel(bebida=bebida_tipo)
    
    # Pular esta bebida se não houver nenhuma emissão em nenhum ano
    if data_bebida_slice.isnull().all():
        print(f"\nSem dados de emissão para {bebida_tipo} em nenhum ano. Pulando bebida.")
        continue
        
    # Calcular o valor máximo para a escala de cores DESTA BEBIDA
    vmax_bebida = data_bebida_slice.max().item()
    
    # Para a escala log, o mínimo deve ser maior que zero.
    # Encontramos o menor valor de emissão positivo para usar como nosso vmin.
    vmin_bebida = data_bebida_slice.where(data_bebida_slice > 0).min().item()

    # Criar o normalizador de escala de cores
    # Se vmin_bebida for NaN (ou seja, não há emissões > 0), não podemos usar escala log.
    if pd.isna(vmin_bebida) or vmin_bebida >= vmax_bebida:
        print(f"\nNão foi possível usar escala log para '{bebida_tipo}'. Usando escala linear.")
        norm_obj = None # Usará escala linear padrão
        cbar_label = 'Emissão de NMVOC (kg) [Escala Linear]'
    else:
        print(f"\nPreparando mapas para '{bebida_tipo}' com escala log (de {vmin_bebida:.2f} a {vmax_bebida:.2f}).")
        norm_obj = LogNorm(vmin=vmin_bebida, vmax=vmax_bebida)
        cbar_label = 'Emissão de NMVOC (kg) [Escala Log]'

    # 3. Iterar por cada ano para a bebida atual
    for ano in data_bebida_slice['time'].values:
        
        # Seleciona a 'fatia' 2D do cubo de dados
        data_slice_ano = data_bebida_slice.sel(time=ano)
        
        if data_slice_ano.isnull().all():
            print(f"-> Sem dados para {ano}. Mapa não gerado.")
            continue
            
        # 4. Criação da figura
        fig, ax = plt.subplots(figsize=(10, 10), constrained_layout=True)
        
        # 5. Plotar as camadas
        estados.boundary.plot(ax=ax, linewidth=0.8, color='darkgray')
        
        data_slice_ano.plot(
            ax=ax,
            cmap='YlOrRd',
            add_colorbar=True,
            cbar_kwargs={'label': cbar_label},
            norm=norm_obj # <-- AQUI ESTÁ A MUDANÇA PRINCIPAL!
        )
        
        # 6. Customização e salvamento
        ax.set_title(f'Emissão de NMVOC - {bebida_tipo} ({ano})', fontsize=16, weight='bold')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        
        filename = f"emissao_log_{str(bebida_tipo).replace(' ', '_')}_{ano}.png"
        filepath = os.path.join(figures_path, filename)
        
        plt.savefig(filepath, dpi=200, bbox_inches='tight')
        plt.close(fig)
        
        print(f"-> Mapa salvo: {filename}")

print("\nProcesso finalizado!")

#===================
import pymannkendall as mk
from matplotlib.colors import ListedColormap
import matplotlib.patches as mpatches

# --- CÁLCULO DA TENDÊNCIA (VERSÃO ROBUSTA) ---

# 1. Função de teste aprimorada com validações
def mann_kendall_test_numeric(x):
    series = x[~np.isnan(x)]
    
    if len(series) < 4 or np.all(series == series[0]) or np.all(series == 0):
        return np.nan
    if np.count_nonzero(series) < 2:
        return np.nan

    try:
        trend, h, *_ = mk.original_test(series, alpha=0.05)
        if trend == 'increasing':
            return 1
        elif trend == 'decreasing':
            return -1
        else:
            return 0
    except:
        return np.nan


# 2. Aplicar a nova função segura a cada pixel
print("\nIniciando o teste de tendência (versão segura) para cada pixel...")
tendencia_por_pixel = xr.apply_ufunc(
    mann_kendall_test_numeric,
    ds_emissoes_completo['nmvoc_emissions'],
    input_core_dims=[['time']],
    output_core_dims=[[]],
    exclude_dims=set(('time',)),
    vectorize=True,
    dask="parallelized",
    output_dtypes=[float]
)

print("Cálculo de tendência finalizado.")


# --- VISUALIZAÇÃO DOS RESULTADOS (COM AJUSTE NA LEGENDA) ---

from matplotlib.colors import BoundaryNorm

# Mapeamento de cores
cmap_tendencia = ListedColormap(['#c92525', '#a3a3a3', '#3b6fb6', '#ffffff'])  # Queda, Neutra, Aumento, NaN
bounds = [-1.5, -0.5, 0.5, 1.5, 2.5]
norm = BoundaryNorm(bounds, cmap_tendencia.N)

print("Iniciando a geração dos mapas de tendência...")
for bebida_tipo in tendencia_por_pixel['bebida'].values:
    
    mapa_tendencia_bebida = tendencia_por_pixel.sel(bebida=bebida_tipo)

    fig, ax = plt.subplots(figsize=(10, 10), constrained_layout=True)

    mapa_tendencia_bebida.plot(
        ax=ax,
        cmap=cmap_tendencia,
        norm=norm,
        add_colorbar=False
    )

    estados.boundary.plot(ax=ax, linewidth=0.8, color='black')
    
    ax.set_title(f'Tendência de Emissão de NMVOC por Pixel: {bebida_tipo}', fontsize=16, weight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    
    legend_patches = [
        mpatches.Patch(color='#3b6fb6', label='Tendência de Aumento'),
        mpatches.Patch(color='#c92525', label='Tendência de Queda'),
        mpatches.Patch(color='#a3a3a3', label='Sem Tendência Significativa'),
        mpatches.Patch(color='#ffffff', label='Sem Emissão / Dados Insuficientes')
    ]
    ax.legend(handles=legend_patches, loc='lower left', fontsize=12, frameon=True)

    filename = f"tendencia_pixel_{str(bebida_tipo).replace(' ', '_')}.png"
    filepath = os.path.join(figures_path, filename)
    
    plt.savefig(filepath, dpi=200, bbox_inches='tight')
    plt.close(fig)

    print(f"-> Mapa de tendência salvo: {filename}")

print("\nProcesso finalizado!")

# --- CONTAGEM DE TENDÊNCIAS ---

import pandas as pd

# Seleciona a bebida, achata e transforma em Series
valores = tendencia_por_pixel.sel(bebida='Vinho').values.ravel()
contagem = pd.Series(valores).value_counts(dropna=False).sort_index()

# Exibe a contagem organizada
print("\nContagem de pixels por tipo de tendência (bebida = Vinho):")
print(contagem)


