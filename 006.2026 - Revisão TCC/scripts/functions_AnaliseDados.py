# -*- coding: utf-8 -*-
"""
Created on Thu Aug 14 09:18:30 2025

@author: glima
"""
import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import pymannkendall as mk
import xarray as xr
from shapely.geometry import box
import geopandas as gpd
from matplotlib.colors import LogNorm, Normalize, ListedColormap, BoundaryNorm
import matplotlib.gridspec as gridspec
from matplotlib.patches import Patch
from clean_text import clean_text
import matplotlib.cm as cm


#%% porcentagens base

def calcular_emissoes_agregadas(df_principal, coluna_estado, coluna_emissoes):
    """
    Calcula as emissões agregadas por UF e por Região a partir 
    do DataFrame principal do inventário.

    Argumentos:
    df_principal (pd.DataFrame): O DataFrame já carregado e limpo.
    coluna_estado (str): O nome da coluna com os nomes dos estados (ex: 'ESTADO').
    coluna_emissoes (str): O nome da coluna com os valores de emissão (ex: 'Emissão NMCOV (ton)').

    Retorna:
    tuple: (df_uf, df_regiao)
           Dois DataFrames com os cálculos de porcentagem.
    """
    
    print("Iniciando cálculos por UF e Região...")
    
    # Criar uma cópia para evitar 'SettingWithCopyWarning'
    df = df_principal.copy()

    # 1. Garantir que as colunas existem
    if coluna_estado not in df.columns:
        print(f"Erro na função: Coluna '{coluna_estado}' não encontrada.")
        return None, None
    if coluna_emissoes not in df.columns:
        print(f"Erro na função: Coluna '{coluna_emissoes}' não encontrada.")
        return None, None

    # 2. Garantir que a coluna de emissões é numérica
    df[coluna_emissoes] = pd.to_numeric(df[coluna_emissoes], errors='coerce')
    df = df.dropna(subset=[coluna_emissoes]) 

    # 3. Calcular Total Nacional
    total_emissions = df[coluna_emissoes].sum()
    print(f"Emissão Total Nacional ({coluna_emissoes}): {total_emissions:,.2f} t/ano")

    # 4. Cálculo por UF (Estado)
    # Padronizar a coluna de estado para maiúsculas (necessário para o map)
    df[coluna_estado] = df[coluna_estado].str.upper()
    
    # Agrupar por estado
    df_uf = df.groupby(coluna_estado)[coluna_emissoes].sum().reset_index()
    df_uf['Porcentagem (%)'] = (df_uf[coluna_emissoes] / total_emissions) * 100
    df_uf = df_uf.sort_values(by='Porcentagem (%)', ascending=False)

    print("\n--- (Função) Emissões Acumuladas por UF (Top 5) ---")
    print(df_uf.head().to_string(index=False, float_format='%.2f'))

    # 5. Cálculo por Região
    mapa_regioes_completo = {
        'ACRE': 'Norte', 'ALAGOAS': 'Nordeste', 'AMAPA': 'Norte', 'AMAZONAS': 'Norte',
        'BAHIA': 'Nordeste', 'CEARA': 'Nordeste', 'DISTRITO FEDERAL': 'Centro-Oeste',
        'ESPIRITO SANTO': 'Sudeste', 'GOIAS': 'Centro-Oeste', 'MARANHAO': 'Nordeste',
        'MATO GROSSO': 'Centro-Oeste', 'MATO GROSSO DO SUL': 'Centro-Oeste',
        'MINAS GERAIS': 'Sudeste', 'PARA': 'Norte', 'PARAIBA': 'Nordeste',
        'PARANA': 'Sul', 'PERNAMBUCO': 'Nordeste', 'PIAUI': 'Nordeste',
        'RIO DE JANEIRO': 'Sudeste', 'RIO GRANDE DO NORTE': 'Nordeste',
        'RIO GRANDE DO SUL': 'Sul', 'RONDONIA': 'Norte', 'RORAIMA': 'Norte',
        'SANTA CATARINA': 'Sul', 'SAO PAULO': 'Sudeste', 'SERGIPE': 'Nordeste',
        'TOCANTINS': 'Norte'
    }
    
    # Criar a coluna 'Região' no DataFrame local 'df'
    df['Região'] = df[coluna_estado].map(mapa_regioes_completo)
    
    # Calcular Emissões por Região
    df_regiao = df.groupby('Região')[coluna_emissoes].sum().reset_index()
    df_regiao['Porcentagem (%)'] = (df_regiao[coluna_emissoes] / total_emissions) * 100
    df_regiao = df_regiao.sort_values(by='Porcentagem (%)', ascending=False)

    print("\n--- (Função) Emissões Acumuladas por Região ---")
    print(df_regiao.to_string(index=False, float_format='%.2f'))

    print("\nFunção 'calcular_emissoes_agregadas_tcc' concluída.")
    
    # Retorna os dois dataframes
    return df_uf, df_regiao
#%% plot de emissão por ano - pode ser no brasil ou por estado

def plot_emissao(df, path, coluna=None):
    """
    Plota gráficos de emissões de NMCOV (ton) por ano com intervalo de confiança.
    
    Parâmetros:
    - df: DataFrame com as colunas:
        'num_ano', 'Emissão NMCOV (ton)', 
        'Emissão NMCOV CI_lower (ton)', 'Emissão NMCOV CI_upper (ton)'
    - coluna: str (opcional) -> coluna para separar gráficos (ex: 'estado')
    
    Exemplos:
    plot_emissao(df)              # Brasil inteiro
    plot_emissao(df, coluna='UF') # Gráfico por estado
    """
    
    def plot_um(df_plot, titulo_extra):
        """Plota um gráfico para um conjunto filtrado."""
        # Agrupa por ano
        df_agg = df_plot.groupby('num_ano', as_index=False).agg({
            'Emissão NMCOV (ton)': 'sum',
            'Emissão NMCOV CI_lower (ton)': 'sum',
            'Emissão NMCOV CI_upper (ton)': 'sum'
        })
        
        plt.figure(figsize=(10, 6))
        plt.plot(df_agg['num_ano'], df_agg['Emissão NMCOV (ton)'], 
                 marker='o', linestyle='-', label='Emissão de NMCOV (ton)')
        plt.fill_between(df_agg['num_ano'],
                         df_agg['Emissão NMCOV CI_lower (ton)'],
                         df_agg['Emissão NMCOV CI_upper (ton)'],
                         color='b', alpha=0.2, label='Margem de Erro (IC)')
        #plt.title(f'Emissão de NMCOV (ton) da Indústria Alimentícia - {titulo_extra}')
        plt.ylabel('Emissão de NMCOV (ton)')
        plt.yscale("log")
        for i, row in df_agg.iterrows():
            # Define o alinhamento horizontal (ha) dinamicamente
            if i == 0:  # Primeiro ponto
                ha = 'left'
                d = 2
            elif i == len(df_agg) - 1:  # Último ponto
                ha = 'right'
                d = -2
            else:  # Pontos do meio
                ha = 'center'
                d = 0
        
            plt.annotate(
                text=f"{row['Emissão NMCOV (ton)']:.0f}", # O texto a ser exibido
                xy=(row['num_ano'], row['Emissão NMCOV (ton)']), # O ponto (x,y) a ser anotado
                xytext=(d, 10), # O deslocamento do texto: (0 pontos na horizontal, 10 pontos para cima)
                textcoords='offset points', # Unidade do deslocamento (em pontos)
                color='white',
                fontsize=11,
                fontweight='bold',
                ha=ha, # Alinhamento horizontal dinâmico
                va='bottom', # Alinhamento vertical
                bbox=dict(facecolor='black', alpha=0.5, edgecolor='none', boxstyle='round,pad=0.3')
            )
        plt.xlim(df['num_ano'].min(), df['num_ano'].max())
        
        plt.grid(which='major', linestyle='-', linewidth='0.6', color='gray')
        plt.grid(which='minor', axis='y', linestyle=':', linewidth='0.4', color='gray')
        
        plt.legend(ncols=2, loc="upper center",bbox_to_anchor=(0.5, 1.08), frameon=False)
        plt.tight_layout()
        plt.savefig(os.path.join(path,f'Série Histórica de Emissão {titulo_extra}'))
        plt.show()
    
    if coluna is None:
        # Um único gráfico Brasil
        plot_um(df, "Brasil")
    else:
        # Um gráfico para cada valor único da coluna
        for valor in sorted(df[coluna].dropna().unique()):
            df_filtro = df[df[coluna] == valor]
            plot_um(df_filtro, f"{coluna}: {valor}")

#%% Emissões por estado - escala log

def plot_emissoes_estado(df_final, figpath, top_n=None):
    """
    Plota gráfico de barras com IC em escala log,
    colorindo pelas tendências já existentes no df_final.
    """
    # Ordenar
    df_plot = df_final.sort_values('Emissão NMCOV (ton)', ascending=False)
    if top_n:
        df_plot = df_plot.head(top_n)

    # Cores pela tendência
    mapa_cores = {
        'increasing': '#d7191c',
        'decreasing': '#0077b6',
        'no trend': 'gray'
    }
    
    mapa_legenda = {
        'Aumento Significativo': '#d7191c',
        'Diminuição Significativa': '#0077b6',
        'Sem Tendência Significativa': 'gray'
    }
    
    cores = df_plot['tendência'].map(mapa_cores).fillna('lightgray')

    # Valores e erros
    valores = df_plot['Emissão NMCOV (ton)']
    erro_inferior = valores - df_plot['Emissão NMCOV CI_lower (ton)']
    erro_superior = df_plot['Emissão NMCOV CI_upper (ton)'] - valores
    erros = np.array([erro_inferior, erro_superior])

    # Plot
    plt.figure(figsize=(12,6))
    plt.bar(df_plot['ESTADO'], valores,
            yerr=erros, capsize=4, color=cores, alpha=0.85)

    plt.yscale("log")
    
    plt.ylabel("Emissões de NMCOV (ton) [escala log]",fontsize=14)
    plt.title("Emissões Anuais Acumuladas de NMCOV da Indústria Alimentícia por UF\nPeríodo de 2017 à 2024",fontsize=14)
    plt.xticks(rotation=45, ha="right",fontsize=12)
    plt.tight_layout()
    plt.grid(True, axis="y", which="major", linestyle="--", color="gray", alpha=0.5)

    # Legenda
    legenda = [Patch(color=v, label=k) for k,v in mapa_legenda.items()]
    plt.legend(handles=legenda, title="Tendência")
    
    plt.savefig(os.path.join(figpath,'EmissõesAcumuladasEstado.png'))
    plt.show()
    
#%% emissões UF acumulado com barras empilhadas - escala normal
'''
NÃO FICA LEGAL POIS - linear são paulo fica mt maior q todos
log ele fica distorcido (2017, primeiro ano, vai parecer maior q os demais,
                         atrapalhando interpretação).
'''
def plot_emissoes_estado_ano(df_emissoes_uf_ano, figpath, top_n=None):
    """
    Plota um gráfico de barras empilhadas das emissões por estado ao longo dos anos.
    Cada barra representa um estado, e os segmentos da barra representam as emissões de cada ano.
    
    Args:
        df_emissoes_uf_ano (pd.DataFrame): DataFrame com MultiIndex ('ESTADO', 'num_ano')
                                         e a coluna 'Emissão NMCOV (ton)'.
        figpath (str): Caminho para salvar a figura.
        top_n (int, optional): Número de estados com maiores emissões para mostrar. 
                               Se None, mostra todos.
    """
    # --- 1. Reestruturação e Ordenação dos Dados ---
    
    # Pivota o DataFrame para que os anos virem colunas
    df_pivot = df_emissoes_uf_ano['Emissão NMCOV (ton)'].unstack('num_ano')
    
    # Calcula o total por estado para ordenar corretamente
    df_pivot['Total'] = df_pivot.sum(axis=1)
    df_plot = df_pivot.sort_values('Total', ascending=False).drop('Total', axis=1)
    
    # Aplica o filtro top_n, se especificado
    if top_n:
        df_plot = df_plot.head(top_n)
        
    # --- 2. Preparação para a Plotagem ---
    
    # ***** ESTA É A LINHA CORRIGIDA *****
    anos = df_plot.columns 
    estados = df_plot.index
    
    # Gera um mapa de cores para os anos
    cores = cm.Accent(np.linspace(0, 1, len(anos)))
    
    plt.figure(figsize=(15, 8))
    
    # Variável para controlar a base de cada segmento empilhado
    bottom = np.zeros(len(estados))
    
    # --- 3. Loop para Plotar as Barras Empilhadas ---
    
    for i, ano in enumerate(anos):
        valores = df_plot[ano].fillna(0) # Garante que não há NaNs
        plt.bar(
            estados, 
            valores, 
            bottom=bottom, 
            label=str(ano), 
            color=cores[i], 
            alpha=0.85
        )
        # Atualiza a base para a próxima camada
        bottom += valores

    # --- 4. Formatação do Gráfico ---
    
    #plt.yscale("linear")
    plt.ylabel("Emissões de NMCOV (ton)") # Removido [escala log] para clareza
    plt.title(f"Emissões Anuais de NMCOV da Indústria Alimentícia por UF (Top {top_n})" if top_n else "Emissões Anuais de NMCOV da Indústria Alimentícia por UF")
    plt.xticks(rotation=45, ha="right")
    
    plt.grid(True, axis="y", which="major", linestyle="--", color="gray", alpha=0.5)
    plt.ylim(bottom=0) 
    # Adiciona a legenda para os anos, posicionada fora do gráfico
    plt.legend(title="Ano", bbox_to_anchor=(1.02, 1), loc='upper left')
    
    plt.tight_layout() # Ajusta o layout para evitar sobreposição
    
    # Salva a figura
    # Certifique-se de que o diretório figpath existe
    if not os.path.exists(figpath):
        os.makedirs(figpath)
    plt.savefig(os.path.join(figpath, 'EmissoesAnuaisEstado_Empilhado.png'), bbox_inches='tight')
    plt.show()
    
#%% função de análise de tendência de emissões
            
def analisar_tendencia_nmvc(df, group_cols):
    resultados = []

    for grupo_valores, grupo_df in df.groupby(group_cols):
        if isinstance(grupo_valores, str):
            grupo_valores = (grupo_valores,)

        grupo_dict = dict(zip(group_cols, grupo_valores))
        print(f"\nAnalisando: {grupo_dict}")

        serie_anual = grupo_df.groupby('num_ano')["Emissão NMCOV (ton)"].sum().sort_index()

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
#%% Função de geração do cube-data

def criar_cubo_emissoes_geograficas(
    df_emissoes: pd.DataFrame,
    coluna_emissao: str,
    coluna_lat: str = 'LATITUDE',
    coluna_lon: str = 'LONGITUDE',
    coluna_ano: str = 'num_ano',
    coluna_estado: str = 'ESTADO',
    limites_grid: dict = {'xmin': -74, 'xmax': -34, 'ymin': -34, 'ymax': 6},
    resolucao: float = 0.5,
    crs: str = 'EPSG:4674',
    preencher_na_com: int = 0
) -> xr.Dataset:
    
    # --- 1. FUNÇÃO AUXILIAR PARA CRIAR A GRADE ---
    def _create_grid(res, xmin, xmax, ymin, ymax, crs_grid):
        x_coords = np.arange(xmin, xmax, res)
        y_coords = np.arange(ymin, ymax, res)
        grid_cells = [box(x, y, x + res, y + res) for x in x_coords for y in y_coords]
        grid = gpd.GeoDataFrame(geometry=grid_cells, crs=crs_grid)
        grid['lon'] = grid.geometry.centroid.x
        grid['lat'] = grid.geometry.centroid.y
        return grid

    # --- 2. PREPARAÇÃO E VALIDAÇÃO DOS DADOS ---
    print("Iniciando a criação do cubo de dados...")
    df_filtrado = df_emissoes.dropna(
        subset=[coluna_lat, coluna_lon, coluna_emissao]
    ).copy()

    gdf_emissoes = gpd.GeoDataFrame(
        df_filtrado,
        geometry=gpd.points_from_xy(
            df_filtrado[coluna_lon].astype(float),
            df_filtrado[coluna_lat].astype(float)
        ),
        crs=crs
    )

    # --- 3. CRIAÇÃO DA GRADE E JUNÇÃO ESPACIAL ---
    print("Criando grade de referência e realizando junção espacial...")
    grid_base = _create_grid(resolucao, **limites_grid, crs_grid=crs)

    # Checa se há pontos para processar
    if gdf_emissoes.empty:
        print("Atenção: O DataFrame de emissões está vazio após a filtragem. Retornando um cubo vazio.")
        # Retorna um cubo vazio, mas com a estrutura correta (ainda a ser implementado se necessário)
        # Por enquanto, vamos parar aqui para evitar erros. O ideal seria construir um cubo de zeros.
        return None # Ou uma estrutura de cubo vazia

    pontos_na_grade = gpd.sjoin(gdf_emissoes, grid_base, how="inner", predicate="within")

    # --- 4. AGREGAÇÃO DOS DADOS ---
    print("Agregando emissões por célula da grade, ano e estado...")
    emissoes_agregadas = (
        pontos_na_grade
        .groupby(['index_right', coluna_ano, coluna_estado], as_index=False)[coluna_emissao]
        .sum()
        .rename(columns={'index_right': 'grid_id'})
    )

    # --- 5. TRANSFORMAÇÃO ROBUSTA PARA O FORMATO XARRAY ---
    print("Estruturando os dados no formato xarray (método robusto)...")
    
    # Junta os dados agregados com as coordenadas da grade
    emissoes_com_coords = pd.merge(
        emissoes_agregadas,
        grid_base[['lat', 'lon']],
        left_on='grid_id',
        right_index=True
    )
    
    # Define o índice com as futuras dimensões do cubo
    emissoes_com_indice = emissoes_com_coords.set_index(
        [coluna_estado, coluna_ano, 'lat', 'lon']
    )[coluna_emissao]

    # Converte de Pandas Series para xarray DataArray (ainda pode ser esparso)
    data_array_esparso = xr.DataArray.from_series(emissoes_com_indice)

    # *** A MUDANÇA PRINCIPAL ESTÁ AQUI ***
    # Define TODAS as coordenadas que o cubo final DEVE ter
    todos_estados = df_emissoes[coluna_estado].unique()
    todos_anos = df_emissoes[coluna_ano].unique()
    todas_lats = grid_base['lat'].unique()
    todas_lons = grid_base['lon'].unique()

    # Usa .reindex() para garantir que o array tenha todas as coordenadas
    # Isso cria um cubo denso, preenchendo com NaN onde não há dados
    data_array_denso = data_array_esparso.reindex(
        {
            coluna_estado: todos_estados,
            coluna_ano: todos_anos,
            'lat': todas_lats,
            'lon': todas_lons
        }
    )

    # Renomeia as dimensões para o padrão desejado
    data_array_final = data_array_denso.rename({
        coluna_estado: 'estado',
        coluna_ano: 'time'
    })
    
    # Cria o Dataset final
    ds_emissoes = xr.Dataset({'emissions': data_array_final})
    
    # Preenche os valores ausentes (NaN) com o valor especificado (ex: 0)
    ds_emissoes = ds_emissoes.fillna(preencher_na_com)
    
    # A conversão explícita de tipo agora é uma garantia extra
    ds_emissoes = ds_emissoes.assign_coords(
        lon=ds_emissoes.coords['lon'].astype(float),
        lat=ds_emissoes.coords['lat'].astype(float)
    )

    return ds_emissoes

#%% Plotart mosaico emissões

def plotar_mosaico_emissoes(
    ds,
    data_var='emissions',
    titulo='Evolução Anual da Emissão Agregada no Brasil (kg)',
    cbar_label='Emissão Agregada (kg)',
    cmap='YlOrRd',
    scale='log',
    grid_cols=3,
    save_path=None
):
    """
    Cria e salva um mosaico de mapas de emissões anuais para o Brasil.

    Args:
        ds (xr.Dataset): O Dataset xarray contendo os dados de emissões com as
            dimensões ('estado', 'time', 'lat', 'lon').
        data_var (str, optional): Nome da variável de dados a ser plotada. 
            Padrão 'emissions'.
        titulo (str, optional): Título principal da figura.
        cbar_label (str, optional): Rótulo da barra de cores.
        cmap (str, optional): Mapa de cores (colormap) a ser usado. Padrão 'YlOrRd'.
        scale (str, optional): Escala da barra de cores ('log' ou 'linear'). Padrão 'log'.
        grid_cols (int, optional): Número de colunas no grid de subplots. Padrão 3.
        save_path (str, optional): Caminho completo (incluindo nome do arquivo e extensão,
            ex: 'figuras/meu_mapa.png') para salvar a figura. Se None, a figura
            apenas será exibida. Padrão None.

    Returns:
        matplotlib.figure.Figure: O objeto da figura criada.
        matplotlib.axes.Axes: O array de eixos (subplots) criados.
    """
    # --- 1. PREPARAÇÃO DOS DADOS ---
    print("Iniciando a criação do mosaico...")
    # Carregar os limites dos estados do Brasil (dentro da função para ser autossuficiente)
    try:
        url_estados = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
        estados = gpd.read_file(url_estados)
    except Exception as e:
        print(f"Não foi possível carregar o GeoJSON dos estados: {e}")
        return None, None
        
    # Extrai os limites geográficos do shapefile dos estados
    xmin, ymin, xmax, ymax = estados.total_bounds

    # Agrega os dados, somando as emissões de todos os estados para cada ponto e ano
    dados_agregados = ds[data_var].sum(dim='estado')
    anos = dados_agregados['time'].values

    # --- 2. CONFIGURAÇÃO DA ESCALA DE CORES ---
    vmax = dados_agregados.max().item()
    if scale == 'log':
        # Para escala log, o mínimo não pode ser zero. Encontramos o menor valor > 0.
        vmin = dados_agregados.where(dados_agregados > 0).min().item()
        if np.isnan(vmin) or vmin <= 0: # Caso não haja valores > 0
            vmin = 1e-9
        norm = LogNorm(vmin=vmin, vmax=vmax)
        cbar_label_final = f"{cbar_label} [Escala Log]"
    else: # Escala linear
        vmin = dados_agregados.min().item()
        norm = Normalize(vmin=vmin, vmax=vmax)
        cbar_label_final = f"{cbar_label} [Escala Linear]"

    # --- 3. CONFIGURAÇÃO DO GRID DE PLOTS ---
    num_plots = len(anos)
    grid_rows = int(np.ceil(num_plots / grid_cols))
    
    fig, axes = plt.subplots(
        nrows=grid_rows,
        ncols=grid_cols,
        figsize=(4 * grid_cols, 4.5 * grid_rows),
        constrained_layout=True
    )
    axes_flat = axes.flatten()

    # --- 4. LOOP PARA CRIAR CADA SUBPLOT ---
    print(f"Gerando {num_plots} mapas anuais...")
    for i, ano in enumerate(anos):
        ax = axes_flat[i]
        data_slice = dados_agregados.sel(time=ano)
        
        # Plotar limites dos estados
        estados.boundary.plot(ax=ax, linewidth=0.6, color='gray', zorder=2)
        
        # Plotar heatmap de emissões
        mappable = data_slice.plot(
            ax=ax,
            cmap=cmap,
            add_colorbar=False,
            norm=norm,
            zorder=1
        )
        
        # Customização do subplot
        ax.set_title(str(ano), fontsize=14)
        ax.set_aspect('equal')
        ax.set_xlim(xmin, xmax)
        ax.set_ylim(ymin, ymax)
        ax.set_xlabel('')
        ax.set_ylabel('')
        ax.tick_params(axis='both', which='both', bottom=False, top=False, left=False, right=False, labelbottom=False, labelleft=False)
        for spine in ax.spines.values():
            spine.set_visible(False)

    # Esconder eixos não utilizados
    for i in range(num_plots, len(axes_flat)):
        axes_flat[i].set_visible(False)

    # --- 5. FINALIZAÇÃO DA FIGURA ---
    fig.suptitle(titulo, fontsize=22, weight='bold', y=1.03)
    
    # Barra de cores compartilhada
    cbar = fig.colorbar(
        mappable,
        ax=axes.ravel().tolist(),
        orientation='horizontal',
        location='bottom',
        shrink=0.6,
        pad=0.05,
        aspect=40
    )
    cbar.ax.tick_params(labelsize=14)
    cbar.set_label(cbar_label_final, fontsize=14)

    # Salvar a figura, se um caminho foi fornecido
    if save_path:
        # Garante que o diretório de destino exista
        save_dir = os.path.dirname(save_path)
        if save_dir and not os.path.exists(save_dir):
            os.makedirs(save_dir)
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Figura salva em: {save_path}")
    
    plt.show()
    print("Processo concluído.")
    
    return fig, axes

#%% video
import imageio
import os
import glob
import matplotlib.pyplot as plt
import geopandas as gpd
import numpy as np
from matplotlib.colors import LogNorm, Normalize
import warnings

# (Certifique-se de que imageio-ffmpeg está instalado)

def criar_video_emissoes(
    ds,
    data_var='emissions',
    titulo='Evolução da Emissão de NMVOC da Indústria Alimentícia',
    cbar_label='Emissão de NMVOC (ton)',
    cmap='YlOrRd',
    scale='log',
    save_path='animacao_emissoes.mp4', # Default agora é .mp4
    duration_per_frame=0.5
):
    """
    Cria e salva um VÍDEO .mp4 animado das emissões anuais para o Brasil.

    Args:
        ds (xr.Dataset): Dataset xarray com dimensões ('estado', 'time', 'lat', 'lon').
        data_var (str, optional): Nome da variável de dados. Padrão 'emissions'.
        titulo (str, optional): Título principal (constante) da figura.
        cbar_label (str, optional): Rótulo da barra de cores.
        cmap (str, optional): Mapa de cores. Padrão 'YlOrRd'.
        scale (str, optional): Escala da barra de cores ('log' ou 'linear'). Padrão 'log'.
        save_path (str, optional): Caminho completo para salvar o .mp4 final.
        duration_per_frame (float, optional): Duração (em segundos) de cada frame (ano). Padrão 0.5.
    """
    
    # --- 1. PREPARAÇÃO DOS DADOS (Idêntico) ---
    print("Iniciando a criação do VÍDEO...")
    try:
        url_estados = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
        estados = gpd.read_file(url_estados)
    except Exception as e:
        print(f"Não foi possível carregar o GeoJSON dos estados: {e}")
        return

    xmin, ymin, xmax, ymax = estados.total_bounds
    dados_agregados = ds[data_var].sum(dim='estado')
    anos = dados_agregados['time'].values

    # --- 2. CONFIGURAÇÃO DA ESCALA DE CORES (Idêntico) ---
    vmax = dados_agregados.max().item()
    if scale == 'log':
        vmin = dados_agregados.where(dados_agregados > 0).min().item()
        if np.isnan(vmin) or vmin <= 0:
            vmin = 1e-9
        norm = LogNorm(vmin=vmin, vmax=vmax)
        cbar_label_final = f"{cbar_label} [Escala Log]"
    else:
        vmin = dados_agregados.min().item()
        norm = Normalize(vmin=vmin, vmax=vmax)
        cbar_label_final = f"{cbar_label} [Escala Linear]"

    # --- 3. GERAÇÃO DOS FRAMES (Idêntico, exceto nome da pasta temp) ---
    temp_dir = 'temp_video_frames' # Mudei o nome da pasta
    os.makedirs(temp_dir, exist_ok=True)
    frame_files = []
    
    print(f"Gerando {len(anos)} frames (imagens) temporários...")

    for i, ano in enumerate(anos):
        fig, ax = plt.subplots(figsize=(8, 8.5), constrained_layout=True)
        data_slice = dados_agregados.sel(time=ano)
        
        estados.boundary.plot(ax=ax, linewidth=0.6, color='gray', zorder=2)
        
        mappable = data_slice.plot(
            ax=ax, cmap=cmap, add_colorbar=False, norm=norm, zorder=1
        )
        
        cbar = fig.colorbar(
            mappable, ax=ax, orientation='vertical', label=cbar_label_final, 
            fraction=0.04, pad=0.04
        )
        cbar.ax.tick_params(labelsize=10)
        cbar.set_label(cbar_label_final, fontsize=12)

        ax.set_aspect('equal')
        ax.set_xlim(xmin, xmax)
        ax.set_ylim(ymin, ymax)
        ax.set_xlabel('')
        ax.set_ylabel('')
        ax.tick_params(axis='both', which='both', bottom=False, top=False, left=False, right=False, labelbottom=False, labelleft=False)
        for spine in ax.spines.values():
            spine.set_visible(False)

        ax.set_title(str(ano), fontsize=16, weight='bold')
        fig.suptitle(titulo, fontsize=18, weight='bold', y=1.03)

        frame_path = os.path.join(temp_dir, f'frame_{i:03d}.png')
        plt.savefig(frame_path, dpi=150, bbox_inches='tight')
        frame_files.append(frame_path)
        plt.close(fig)

    # --- 4. COMPILAÇÃO DO VÍDEO (AQUI ESTÁ A MUDANÇA) ---
    print(f"Compilando o Vídeo: {save_path}")
    
    images = []
    for filename in sorted(frame_files):
        images.append(imageio.v2.imread(filename))

    # Convertendo a duração para FPS
    # 0.5s por frame = 2 frames por segundo (1 / 0.5)
    fps = 1.0 / duration_per_frame 

    # Salva o VÍDEO (ex: .mp4) usando FPS
    # O imageio vai usar o ffmpeg automaticamente
    imageio.mimsave(save_path, images, fps=fps)

    # --- 5. LIMPEZA DOS ARQUIVOS TEMPORÁRIOS (Idêntico) ---
    print("Limpando arquivos temporários...")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for f in frame_files:
            os.remove(f)
        os.rmdir(temp_dir)
        
    print(f"Vídeo salvo com sucesso em: {save_path}")

#%% Função de análise de tendência por pixel

def analisar_tendencia_pixel(ds_estado, alpha=0.05, data_var='emissions'):
    """
    Aplica o teste de Mann-Kendall a cada pixel de um DataArray xarray.
    Garante a diferenciação entre 'sem tendência' e 'sem dados'.
    """
    def mk_test_wrapper(timeseries_1d):
        if np.all(np.isnan(timeseries_1d)) or np.all(timeseries_1d == 0):
            return "no data", np.nan
        try:
            resultado = mk.original_test(timeseries_1d)
            return resultado.trend, resultado.p
        except Exception:
            return "no data", np.nan

    tendencia_str, p_valor = xr.apply_ufunc(
        mk_test_wrapper,
        ds_estado[data_var],
        input_core_dims=[['time']],
        output_core_dims=[[], []],
        exclude_dims=set(('time',)),
        vectorize=True,
        dask="parallelized",
        output_dtypes=[object, float]
    )
    
    # Mapeia resultados de string para número (1, -1, 0, ou NaN para 'no data')
    tendencia_num = xr.full_like(tendencia_str, fill_value=np.nan, dtype=float)
    tendencia_num = tendencia_num.where(tendencia_str != 'increasing', 1)
    tendencia_num = tendencia_num.where(tendencia_str != 'decreasing', -1)
    tendencia_num = tendencia_num.where(tendencia_str != 'no trend', 0)
    
    # Aplica o filtro de significância (p < alpha).
    # Onde a tendência não é significativa, o valor vira 0.
    tendencia_com_filtro_p = tendencia_num.where(p_valor < alpha, 0)
    
    tendencia_significativa = tendencia_com_filtro_p.where(tendencia_str != 'no data')
    
    #filtro para onde n tem dado
    tendencia_final = tendencia_significativa.fillna(-999)

    return xr.Dataset({'tendencia': tendencia_final, 'p_valor': p_valor})




#%% Função que plota o mosaico por estado

def plotar_mosaico_estado(df, ds, tendencia_uf_df, estado_alvo, save_path=None):
    """
    Gera um mosaico completo para um único estado.
    Escala log, layout compacto.
    """
    # Geração da figura
    fig = plt.figure(figsize=(18, 9))
    
    # Ajustar a proporção de altura para diminuir o espaço do texto inferior
    gs = gridspec.GridSpec(2, 2, height_ratios=[8, 1], figure=fig)
    
    fig.suptitle(f"Emissões de NMVOC da Indústria Alimentícia - {estado_alvo}", fontsize=26, weight='bold', y=0.98)

    # GRÁFICO DE BARRAS
    ax1 = fig.add_subplot(gs[0, 0])
    df_estado = df[df['ESTADO'] == estado_alvo]

    colunas_emissao = 'Emissão NMCOV (ton)'
    coluna_ic_lower = 'Emissão NMCOV CI_lower (ton)'
    coluna_ic_upper = 'Emissão NMCOV CI_upper (ton)'
    has_ic = all(col in df_estado.columns for col in [coluna_ic_lower, coluna_ic_upper])

    if has_ic:
        df_agg = df_estado.groupby('num_ano', as_index=False).agg({
            colunas_emissao: 'sum',
            coluna_ic_lower: 'sum',
            coluna_ic_upper: 'sum'
        })
    else:
        df_agg = df_estado.groupby('num_ano', as_index=False).agg({
            colunas_emissao: 'sum'
        })
        print("Aviso: Colunas de Intervalo de Confiança não encontradas. O gráfico será gerado sem a margem de erro.")

    ax1.bar(df_agg['num_ano'], df_agg[colunas_emissao], color='skyblue', edgecolor='black', zorder=2)

    if has_ic:
        lower_error = df_agg[colunas_emissao] - df_agg[coluna_ic_lower]
        upper_error = df_agg[coluna_ic_upper] - df_agg[colunas_emissao]
        asymmetric_error = [lower_error, upper_error]
        ax1.errorbar(df_agg['num_ano'], df_agg[colunas_emissao], yerr=asymmetric_error,
                     fmt='none', capsize=5, color='black', elinewidth=1.5, zorder=3)

    anos = df_agg['num_ano']
    emissoes = df_agg[colunas_emissao]
    slope, intercept = np.polyfit(anos, emissoes, 1)
    trend_line = slope * anos + intercept
    ax1.plot(anos, trend_line, color='red', linestyle='--', linewidth=2, label='Linha de Tendência', zorder=4)

    ax1.set_title('Emissão Anual Total', fontsize=20)
    ax1.set_ylabel('Emissão (ton)', fontsize=18)
    ax1.set_xlabel('')
    ax1.grid(axis='y', linestyle='--', alpha=0.7, zorder=0)
    ax1.set_xticks(df_agg['num_ano'])
    ax1.tick_params(axis='x', rotation=45, labelsize=16)
    ax1.tick_params(axis='y', labelsize=16)
    ax1.legend(fontsize=12)
    
    # Colocar o eixo Y em escala logarítmica
    ax1.set_yscale('log')

    # MAPA DE TENDÊNCIA
    ax2 = fig.add_subplot(gs[0, 1])
    try:
        url_estados = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
        gdf_estados = gpd.read_file(url_estados)
        nome_limpo_alvo = clean_text(estado_alvo)
        gdf_estado_alvo = gdf_estados[gdf_estados['name'].apply(clean_text) == nome_limpo_alvo]
        if gdf_estado_alvo.empty: raise ValueError(f"Não foi possível encontrar o estado '{estado_alvo}' no GeoJSON.")
    except Exception as e:
        ax2.text(0.5, 0.5, f"Erro ao carregar shapefile:\n{e}", ha='center', va='center', wrap=True)
        return fig
    ds_estado = ds.sel(estado=estado_alvo)
    ds_tendencia_pixel = analisar_tendencia_pixel(ds_estado)
    colors = ['#ffffff', '#0077b6', '#a9a9a9', '#d7191c']
    bounds = [-1000, -998, -0.5, 0.5, 1.5]
    cmap = ListedColormap(colors)
    norm = BoundaryNorm(bounds, cmap.N)
    ds_tendencia_pixel['tendencia'].plot.imshow(ax=ax2, cmap=cmap, norm=norm, add_colorbar=False, alpha=0.8)
    gdf_estado_alvo.boundary.plot(ax=ax2, linewidth=1.5, color='black', zorder=10)
    xmin, ymin, xmax, ymax = gdf_estado_alvo.total_bounds
    ax2.set_xlim(xmin - 1, xmax + 1); ax2.set_ylim(ymin - 1, ymax + 1)
    ax2.set_title('Tendência de Emissão por Pixel (p < 0.05)', fontsize=20)
    ax2.set_xlabel('Longitude'); ax2.set_ylabel('Latitude'); ax2.set_aspect('equal')
    legend_elements = [
        Patch(facecolor='#d7191c', edgecolor='black', label='Aumento Significativo'),
        Patch(facecolor='#0077b6', edgecolor='black', label='Diminuição Significativa'),
        Patch(facecolor='#a9a9a9', edgecolor='black', label='Sem Tendência Significativa'),
        Patch(facecolor='#ffffff', edgecolor='black', label='Sem Emissão / Dados')
    ]
    ax2.legend(handles=legend_elements, loc='lower center', fontsize=14,ncols = 2)

    # Texto com tendência geral
    ax_text = fig.add_subplot(gs[1, :])
    ax_text.axis("off")
    try:
        linha = tendencia_uf_df[tendencia_uf_df['ESTADO'] == estado_alvo].iloc[0]
        traducoes = {'increasing': 'Aumento Significativo', 'decreasing': 'Diminuição Significativa', 'no trend': 'Sem Tendência'}
        tendencia_original = str(linha['tendência'])
        tendencia = traducoes.get(tendencia_original, tendencia_original.title())
        p_valor = linha['p-valor']
        frase = f"Tendência Geral para o Estado: {tendencia} (p-valor = {p_valor:.3f})"
    except (IndexError, KeyError):
        frase = "Tendência Geral para o Estado: Dados não encontrados."
    ax_text.text(0.5, 0.5, frase, ha='center', va='center', fontsize=18)

    # Ajuste das margens e salvar figura
    plt.tight_layout(rect=[0, 0.01, 1, 0.97])
    
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
    
    return fig

#%% plot_producao_empilhada

def plot_producao_empilhada(
    df,
    figpath,
    col_ano="num_ano",
    col_valor="prodtonhl_v4",
    col_categoria="tipo_industria_nfr",
    col_cor="food_color",
    titulo="Produção por Ano"
):
    """
    Plota gráfico de barras empilhadas da produção por ano.

    Parâmetros:
    -----------
    df : DataFrame
        Dados de entrada.
    col_ano : str
        Nome da coluna com os anos (eixo X).
    col_valor : str
        Nome da coluna com os valores de produção (eixo Y).
    col_categoria : str
        Nome da coluna que define as categorias para empilhamento.
    col_cor : str
        Nome da coluna que define as cores das categorias.
    titulo : str
        Título do gráfico.
    """

    # agrupa valores
    dados = df.groupby([col_ano, col_categoria, col_cor])[col_valor].sum().reset_index()

    # pivot para formato wide (categorias como colunas)
    tabela = dados.pivot_table(
        index=col_ano,
        columns=col_categoria,
        values=col_valor,
        aggfunc="sum",
        fill_value=0
    )

    # dicionário de cores (corrigido)
    cores = {
        row[col_categoria]: row[col_cor]
        for _, row in dados.drop_duplicates(col_categoria).iterrows()
    }

    # plota
    ax = tabela.plot(
        kind="bar",
        stacked=True,
        figsize=(13, 8),
        color=[cores[col] for col in tabela.columns]
    )

    #ax.set_title(titulo, fontsize=16, weight="bold")
    ax.set_ylabel('Produção de Alimentos (Ton ou hL)', fontsize=14)
    ax.set_xlabel('')
    plt.rc('xtick', labelsize=12) 
    plt.rc('ytick', labelsize=12) 
    plt.xticks(rotation=0)
    plt.grid(alpha = 0.3)
    plt.legend(bbox_to_anchor=(0.5, -0.2), loc="lower center", ncols = 3,frameon = False, fontsize=12)
    plt.tight_layout()
    plt.savefig(os.path.join(figpath),dpi=300, bbox_inches='tight')
    plt.show()
#%% comparação pia e eu

import pandas as pd
from scipy.stats import pearsonr
import os
import matplotlib.pyplot as plt
import numpy as np # Necessário para o código do scatter, boa prática manter

# -------------------------------------------------------------------------
# CÓDIGO DO GRÁFICO DE LINHAS (COM FONTES AJUSTADAS)
# -------------------------------------------------------------------------

def plot_mosaico_linhas_dfs(
    df1, df2, figpath,
    col_ano1="num_ano", col_valor1="Producao (Ton)", col_categoria1="Produto",
    col_ano2="ano", col_valor2="Valor_Prod", col_categoria2="Produto",
    titulo="Produção por Produto (Comparativo)",
    ncols=3, nrows=3, figsize=(15, 10),
    map_unidade=None
):
    """
    Plota um mosaico de gráficos de linhas comparando df1 e df2,
    com anotação de correlação, com fontes padronizadas (baseado no scatter).
    """
    os.makedirs(figpath, exist_ok=True)

    # Pivot df1
    tabela1 = df1.groupby([col_ano1, col_categoria1])[col_valor1].sum().reset_index()
    tabela1 = tabela1.pivot(index=col_ano1, columns=col_categoria1, values=col_valor1).fillna(0)

    # Pivot df2
    tabela2 = df2.groupby([col_ano2, col_categoria2])[col_valor2].sum().reset_index()
    tabela2 = tabela2.pivot(index=col_ano2, columns=col_categoria2, values=col_valor2).fillna(0)

    produtos = sorted(set(tabela1.columns).union(tabela2.columns))

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=figsize)
    axes = axes.flatten()

    for i, prod in enumerate(produtos):
        if i >= nrows * ncols:
            break
        ax = axes[i]

        # Plotar os dados
        if prod in tabela1.columns:
            ax.plot(tabela1.index, tabela1[prod], label="Inventário", marker='o')
        if prod in tabela2.columns:
            ax.plot(tabela2.index, tabela2[prod], label="PIA-IBGE", marker='s')

        # --- Título com unidade, se existir ---
        unidade = map_unidade.get(prod, '') if map_unidade else ''
        titulo_prod = f"{prod} ({unidade})" if unidade else prod
        ax.set_title(titulo_prod, fontsize=14, weight='bold')
        ax.grid(alpha=0.3)

        # --- Cálculo e anotação da correlação (mantido como estava) ---
        if prod in tabela1.columns and prod in tabela2.columns:
            temp_df = pd.concat([tabela1[prod], tabela2[prod]], axis=1, join='inner')
            temp_df.columns = ['df1', 'df2']
            if len(temp_df) >= 2:
                corr, p_value = pearsonr(temp_df['df1'], temp_df['df2'])
                
    # Remove eixos extras
    for j in range(len(produtos), nrows * ncols):
        fig.delaxes(axes[j])
        
    handles, labels = [], []
    for ax in fig.axes:
        for h, l in zip(*ax.get_legend_handles_labels()):
            if l not in labels:
                handles.append(h)
                labels.append(l)
    if handles:
        fig.legend(handles, labels, loc='upper center',
                   bbox_to_anchor=(0.5, 0.1),
                   ncol=2, frameon=False, fontsize=14)

    plt.tight_layout(rect=[0.05, 0.08, 1, 0.95], h_pad=0.8, w_pad=0.8)
   
    plt.savefig(
        os.path.join(figpath, 'mosaico_linhas_com_correlacao_fontes_ajustadas.png'),
        dpi=300, bbox_inches='tight'
    )


#%%
import os
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.stats import pearsonr

def plot_mosaico_scatter_dfs(
    df1, df2, figpath,
    col_ano1="num_ano", col_valor1="Producao (Ton)", col_categoria1="Produto",
    col_ano2="ano", col_valor2="Valor_Prod", col_categoria2="Produto",
    titulo="Correlação entre Inventário e PIA-IBGE por Produto",
    ncols=3, nrows=3, figsize=(15, 10),
    map_unidade=None
):
    """
    Plota um mosaico de gráficos de dispersão comparando df1 e df2,
    com linha de ajuste linear e correlação de Pearson.
    """
    os.makedirs(figpath, exist_ok=True)

    # Pivotar e agregar dados anuais
    tabela1 = df1.groupby([col_ano1, col_categoria1])[col_valor1].sum().reset_index()
    tabela1 = tabela1.pivot(index=col_ano1, columns=col_categoria1, values=col_valor1).fillna(0)

    tabela2 = df2.groupby([col_ano2, col_categoria2])[col_valor2].sum().reset_index()
    tabela2 = tabela2.pivot(index=col_ano2, columns=col_categoria2, values=col_valor2).fillna(0)

    produtos = sorted(set(tabela1.columns).union(tabela2.columns))

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=figsize)
    axes = axes.flatten()

    for i, prod in enumerate(produtos):
        if i >= nrows * ncols:
            break
        ax = axes[i]

        if prod in tabela1.columns and prod in tabela2.columns:
            # Alinha os dados pelo índice (ano)
            temp_df = pd.concat([tabela1[prod], tabela2[prod]], axis=1, join='inner')
            temp_df.columns = ['df1', 'df2']

            if len(temp_df) >= 2:
                # --- Dispersão ---
                ax.scatter(temp_df['df2'], temp_df['df1'],
                           s=60, alpha=0.8, edgecolors='k', label='Observações')

                # --- Ajuste linear ---
                m, b = np.polyfit(temp_df['df2'], temp_df['df1'], 1)
                x_vals = np.linspace(temp_df['df2'].min(), temp_df['df2'].max(), 100)
                ax.plot(x_vals, m * x_vals + b, color='red', linestyle='--', linewidth=1.8, label='Ajuste Linear')

                # --- Correlação ---
                corr, p_value = pearsonr(temp_df['df1'], temp_df['df2'])
                
                # --- AJUSTE: Mover correlação para subtítulo ---
                ax.text(
                    0.5, 0.95,  # Posição: X=centro (0.5), Y=topo (0.95)
                    f"Correlação de Pearson = {corr:.2f}\n(p-valor: {p_value:.2f})",
                    transform=ax.transAxes,
                    fontsize=13, # Fonte do subtítulo
                    va='top', ha='center', # Alinhamento central
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='wheat', alpha=0.6, edgecolor='none')
                )

        # --- AJUSTE: Aumentar fonte do título ---
        
        unidade = map_unidade.get(prod, '')
        titulo_prod = f"{prod} ({unidade})" if unidade else prod
        ax.set_title(titulo_prod, fontsize=14, weight='bold') 
        
        # --- AJUSTE: REMOVER rótulos individuais ---
        # ax.set_xlabel("PIA-IBGE", fontsize=14)
        # ax.set_ylabel("Inventário", fontsize=14)
        ax.grid(alpha=0.3)

    # Remove eixos vazios
    for j in range(len(produtos), nrows * ncols):
        fig.delaxes(axes[j])

    # --- Legenda global embaixo, sem borda, 2 colunas ---
    # ... (código do loop for) ...

    # --- Legenda global embaixo, sem borda, 2 colunas ---
    handles, labels = [], []
    for ax in fig.axes:
        for h, l in zip(*ax.get_legend_handles_labels()):
            if l not in labels:
                handles.append(h)
                labels.append(l)

    if handles:
        # --- AJUSTE: Mover a legenda para o TOPO da margem inferior ---
        fig.legend(handles, labels, loc='upper center',
                   # Alinha o topo da legenda ao fim dos gráficos (y=0.08)
                   bbox_to_anchor=(0.5, 0.1), 
                   ncol=2, frameon=False, fontsize=14)

    # Título e layout
    #fig.suptitle(titulo, fontsize=15, weight="bold", y=1.02)
    
    # --- AJUSTE: ADICIONAR RÓTULOS GLOBAIS COM POSIÇÃO FIXA ---
    # Posição X explícita (na margem esquerda)
    fig.supylabel('Eixo Y - Inventário', fontsize=15, x=0.08)
    # Posição Y explícita (na margem inferior, abaixo da legenda)
    fig.supxlabel('Eixo X - PIA-IBGE', fontsize=15, y=0.1)
    
    # --- AJUSTE: Reservar espaço para os rótulos e legenda ---
    # rect=[left, bottom, right, top]
    # left=0.05 -> 5% de espaço para o supylabel
    # bottom=0.08 -> 8% de espaço para a legenda E o supxlabel
    plt.tight_layout(
        rect=[0.05, 0.08, 1, 0.95], 
        h_pad=0.8,
        w_pad=0.8
    )
    
    # Salvar imagem
    plt.savefig(os.path.join(figpath, "mosaico_scatter_com_correlacao.png"),
                dpi=300, bbox_inches='tight')
    # plt.show()  # Descomente se quiser exibir
    # plt.show()  # Descomente se quiser exibir
    
#%%

import pandas as pd
import numpy as np

def calcular_tabela_bias(
    df1, df2,
    col_ano1="num_ano", col_valor1="prodtonhl_v4", col_categoria1="tipo_industria_nfr",
    col_ano2="ANO", col_valor2="PRODUÇÃO_NOVO", col_categoria2="tipo_industria_nfr"
):
    """
    Prepara os dados do Inventário (df1) e PIA (df2) e calcula o BIAS.
    Retorna um DataFrame longo com os dados de BIAS e a lista de produtos na ordem.
    """
    
    # 1. Pivotar dados (mesma lógica do seu código)
    tabela1 = df1.groupby([col_ano1, col_categoria1])[col_valor1].sum().reset_index()
    tabela1 = tabela1.pivot(index=col_ano1, columns=col_categoria1, values=col_valor1).fillna(0)
    
    tabela2 = df2.groupby([col_ano2, col_categoria2])[col_valor2].sum().reset_index()
    tabela2 = tabela2.pivot(index=col_ano2, columns=col_categoria2, values=col_valor2).fillna(0)

    # 2. Obter lista de produtos na ordem alfabética (mesma lógica do seu código)
    # Isso garante que a ordem será a mesma do mosaico de correlação
    produtos_ordenados = sorted(set(tabela1.columns).union(tabela2.columns))

    lista_dfs_bias = []
    
    # 3. Iterar e calcular o BIAS
    for prod in produtos_ordenados:
        if prod in tabela1.columns and prod in tabela2.columns:
            # Alinha os dados pelo índice (ano)
            temp_df = pd.concat([tabela1[prod], tabela2[prod]], axis=1, join='inner')
            temp_df.columns = ['Inventario', 'PIA_IBGE']
            
            # Remove anos onde ambos são zero (se houver)
            temp_df = temp_df.loc[(temp_df['Inventario'] != 0) | (temp_df['PIA_IBGE'] != 0)]
            
            if not temp_df.empty:
                # Calcula o BIAS
                temp_df['BIAS'] = temp_df['Inventario'] - temp_df['PIA_IBGE']
                
                # Adiciona coluna de Produto
                temp_df['Produto'] = prod
                
                # Reseta o índice (Ano) para uma coluna
                temp_df.reset_index(names='Ano', inplace=True)
                
                lista_dfs_bias.append(temp_df)

    # Concatena tudo em um DataFrame longo
    df_bias_final = pd.concat(lista_dfs_bias, ignore_index=True)
    
    return df_bias_final, produtos_ordenados
