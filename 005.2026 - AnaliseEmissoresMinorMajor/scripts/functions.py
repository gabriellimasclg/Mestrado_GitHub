# -*- coding: utf-8 -*-
"""
Functions para análise multipoluente do inventário
"""

import os
import hashlib
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pymannkendall as mk
import xarray as xr

from shapely.geometry import box
from matplotlib.patches import Patch
from matplotlib.colors import LogNorm, Normalize

import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patheffects as pe          # <- novo import
from matplotlib.colorbar import ColorbarBase



# =========================================================
# CORES POR SETOR
# =========================================================

def sector_color(df, col_setor='SETOR', s=0.65, v=0.85):
    """
    Gera um dicionário estável {setor: cor_hex}.
    A mesma categoria sempre recebe a mesma cor.
    """
    def gerar_cor(setor):
        texto = str(setor).strip().lower()
        h = hashlib.md5(texto.encode('utf-8')).hexdigest()
        hue = int(h[:8], 16) / 16**8
        rgb = mcolors.hsv_to_rgb((hue, s, v))
        return mcolors.to_hex(rgb)

    setores = sorted(df[col_setor].dropna().astype(str).unique())
    return {setor: gerar_cor(setor) for setor in setores}

def plot_emissoes_por_poluente(
    inv,
    pol_interest,
    figures,
    col_setor='SETOR',
    col_ano='ANO',
    n_top=7,
    palette=None,
    figsize=(10, 5),
    dpi=300,
    yscale='log',
    ncol_legenda=2,
):
    """
    Gera um gráfico de linhas por poluente mostrando os top N setores emissores
    ao longo dos anos, agrupando os demais em 'Demais setores'.

    Parâmetros
    ----------
    inv : pd.DataFrame
        Inventário com colunas de setor, ano e poluentes.
    pol_interest : list[str]
        Lista de poluentes a plotar.
    figures : str
        Caminho da pasta onde os PNGs serão salvos.
    col_setor : str
        Nome da coluna de setor no DataFrame.
    col_ano : str
        Nome da coluna de ano no DataFrame.
    n_top : int
        Número de setores principais (os demais viram 'Demais setores').
    palette : list[str] | None
        Lista de cores hex. Deve ter n_top + 1 elementos (último = Demais).
        Se None, usa paleta padrão.
    figsize : tuple
        Tamanho de cada figura.
    dpi : int
        Resolução do arquivo salvo.
    yscale : str
        Escala do eixo y ('log' ou 'linear').
    ncol_legenda : int
        Número de colunas da legenda.
    """

    if palette is None:
        palette = [
            '#e41a1c', '#377eb8', '#4daf4a', '#984ea3',
            '#ff7f00', '#a65628', '#f781bf', '#aaaaaa'
        ]

    DEMAIS = 'Demais setores'

    def _get_top_com_demais(df, pol, n):
        top = df.groupby(col_setor)[pol].sum().nlargest(n).index
        df = df.copy()
        df['SETOR_PLOT'] = df[col_setor].where(df[col_setor].isin(top), other=DEMAIS)
        return df.groupby([col_ano, 'SETOR_PLOT'])[pol].sum().reset_index()

    emis_year = inv.groupby([col_ano, col_setor])[pol_interest].sum().reset_index()

    for pol in pol_interest:
        df_pol = _get_top_com_demais(emis_year, pol, n_top)

        ordem = (
            df_pol[df_pol['SETOR_PLOT'] != DEMAIS]
            .groupby('SETOR_PLOT')[pol]
            .sum()
            .sort_values(ascending=False)
            .index
            .tolist()
        )
        setores_ordenados = ordem + [DEMAIS]
        color_map = {s: palette[j] for j, s in enumerate(setores_ordenados)}

        fig, ax = plt.subplots(figsize=figsize)

        for idx, setor in enumerate(setores_ordenados, start=1):
            grupo = df_pol[df_pol['SETOR_PLOT'] == setor]
            grupo = grupo[grupo[pol] > 0].sort_values(col_ano)

            ax.plot(
                grupo[col_ano], grupo[pol],
                label=f'{idx}. {setor}',
                color=color_map[setor],
                linewidth=1.5 if setor != DEMAIS else 1,
                linestyle='-' if setor != DEMAIS else '--',
                alpha=1.0 if setor != DEMAIS else 0.5,
                marker='o',
            )

            if not grupo.empty:
                x0 = grupo[col_ano].iloc[0]
                y0 = grupo[pol].iloc[0]
                ax.scatter(x0, y0, color=color_map[setor], s=120, zorder=5)
                ax.text(x0, y0, str(idx),
                        ha='center', va='center',
                        fontsize=6, fontweight='bold',
                        color='white', zorder=6)

        ax.set_title(f'Emissões de {pol}', fontweight='bold')
        ax.set_ylabel('Toneladas')
        ax.set_yscale(yscale)
        ax.grid(True, which='major', linestyle='-', linewidth=0.5, alpha=0.7)
        ax.grid(True, which='minor', linestyle='--', linewidth=0.3, alpha=0.4)
        ax.legend(
            loc='upper center',
            bbox_to_anchor=(0.5, -0.15),
            ncol=ncol_legenda,
            frameon=False,
        )

        plt.tight_layout()
        plt.savefig(os.path.join(figures, f'top{n_top}_{pol}.png'),
                    dpi=dpi, bbox_inches='tight')
        plt.show()
        plt.close()
        
        
def plot_mapa_emissoes_por_poluente(
    inv_gdf, br_estado, br_regiao, pol_interest, figures,
    col_uf='SIGLA_UF', col_regiao='NM_REGIAO',
    threshold_anotacao_uf=5.0, dpi=300,
):
    
    cmap = plt.cm.Reds

    for pol in pol_interest:

        emis_reg = (
            inv_gdf.groupby(col_regiao)[pol].sum()
            .reset_index().rename(columns={pol: 'emissao'})
        )
        total = emis_reg['emissao'].sum()
        emis_reg['pct'] = emis_reg['emissao'] / total * 100
        br_reg_plot = br_regiao.merge(emis_reg, on=col_regiao, how='left')

        emis_uf = (
            inv_gdf.groupby(col_uf)[pol].sum()
            .reset_index().rename(columns={pol: 'emissao'})
        )
        emis_uf['pct'] = emis_uf['emissao'] / total * 100
        br_uf_plot = br_estado.merge(emis_uf, on=col_uf, how='left')

        norm = mcolors.Normalize(vmin=0, vmax=100)

        fig, axes = plt.subplots(
            1, 2, figsize=(14, 6),
            gridspec_kw={'wspace': -0.4}      # <- mapas colados
        )
        fig.subplots_adjust(top=0.92, bottom=0.10, left=0.02, right=0.98)

        for ax in axes:
            ax.set_axis_off()

        # mapa regiões
        br_reg_plot.plot(
            column='pct', ax=axes[0], cmap=cmap, norm=norm,
            edgecolor='black', linewidth=0.6,
            missing_kwds={'color': 'lightgrey'}
        )
        for _, row in br_reg_plot.iterrows():
            if row.geometry is None or row.geometry.is_empty:
                continue
            pct = row.get('pct', np.nan)
            x, y = row.geometry.centroid.x, row.geometry.centroid.y
            label = f"{row[col_regiao]}\n{pct:.1f}%" if not np.isnan(pct) else row[col_regiao]
            axes[0].annotate(
                label, xy=(x, y), ha='center', va='center',
                fontsize=8, fontweight='bold', color='black',
                path_effects=[pe.withStroke(linewidth=2.5, foreground='white')]
            )

        # mapa estados
        br_uf_plot.plot(
            column='pct', ax=axes[1], cmap=cmap, norm=norm,
            edgecolor='black', linewidth=0.4,
            missing_kwds={'color': 'lightgrey'}
        )
        for _, row in br_uf_plot.iterrows():
            if row.geometry is None or row.geometry.is_empty:
                continue
            pct = row.get('pct', np.nan)
            if np.isnan(pct) or pct < threshold_anotacao_uf:
                continue
            x, y = row.geometry.centroid.x, row.geometry.centroid.y
            axes[1].annotate(
                f"{row[col_uf]}\n{pct:.1f}%",
                xy=(x, y), ha='center', va='center',
                fontsize=7, fontweight='bold', color='black',
                path_effects=[pe.withStroke(linewidth=2.5, foreground='white')]
            )

        # colorbar
        cbar_ax = fig.add_axes([0.2, 0.06, 0.6, 0.025])
        cb = ColorbarBase(cbar_ax, cmap=cmap, norm=norm, orientation='horizontal')
        cb.set_label('Emissões Acumuladas (%)', fontsize=9)

        fig.suptitle(f'Distribuição das emissões por região e estado — {pol}', fontsize=13, fontweight='bold')

        plt.savefig(
            os.path.join(figures, f'mapa_espacial_{pol}.png'),
            dpi=dpi, bbox_inches='tight'
        )
        plt.show()
        plt.close()
        
        
def plot_barras_impacto_por_poluente(
    inv,
    pol_interest,
    figures,
    col_ano='ANO',
    col_impacto='impact',
    figsize=(10, 5),
    dpi=300,
):
    """
    Gera barras empilhadas (major / medium / minor) por ano para cada poluente.

    Parâmetros
    ----------
    inv : pd.DataFrame
        Inventário com colunas de ano, impacto e poluentes.
    pol_interest : list[str]
        Lista de poluentes a plotar.
    figures : str
        Pasta de saída dos PNGs.
    col_ano : str
        Nome da coluna de ano.
    col_impacto : str
        Nome da coluna de classificação (major/medium/minor).
    figsize : tuple
        Tamanho de cada figura.
    dpi : int
        Resolução dos arquivos salvos.
    """
    import os
    import matplotlib.pyplot as plt

    ordem_impacto = ['major', 'medium', 'minor']
    cores_impacto = {
        'major': '#d32f2f',
        'medium': '#f57c00',
        'minor': '#aaaaaa',
    }

    for pol in pol_interest:
        emis = (
            inv.groupby([col_ano, col_impacto])[pol]
            .sum()
            .reset_index()
        )

        anos = sorted(emis[col_ano].unique())

        fig, ax = plt.subplots(figsize=figsize)

        bottom = [0] * len(anos)
        barras_por_impacto = {}

        for impacto in ordem_impacto:
            valores = []
            for ano in anos:
                linha = emis[(emis[col_ano] == ano) & (emis[col_impacto] == impacto)]
                valores.append(linha[pol].values[0] if not linha.empty else 0)

            ax.bar(
                anos, valores,
                bottom=bottom,
                label=impacto.capitalize(),
                color=cores_impacto[impacto],
                width=0.7,
                edgecolor='white',
                linewidth=0.4,
            )
            
            barras_por_impacto[impacto] = {'valores': valores, 'bottom': bottom[:]}
            bottom = [b + v for b, v in zip(bottom, valores)]

        total_por_ano = [sum(barras_por_impacto[imp]['valores'][i] for imp in ordem_impacto)
                         for i in range(len(anos))]

        # % do major dentro da barra (centralizado na fatia)
        for i, (ano, val) in enumerate(zip(anos, barras_por_impacto['major']['valores'])):
            if total_por_ano[i] == 0 or val == 0:
                continue
            pct = val / total_por_ano[i] * 100
            y_centro = barras_por_impacto['major']['bottom'][i] + val / 2
            ax.text(
                ano, y_centro, f'{pct:.1f}%',
                ha='center', va='center',
                fontsize=7, fontweight='bold',
                color='white',
            )

        # % do medium em cima da barra total
        for i, (ano, val) in enumerate(zip(anos, barras_por_impacto['medium']['valores'])):
            if total_por_ano[i] == 0 or val == 0:
                continue
            pct = val / total_por_ano[i] * 100
            y_topo = bottom[i] * 1.01   # levemente acima da barra
            ax.text(
                ano, y_topo, f'{pct:.1f}%',
                ha='center', va='bottom',
                fontsize=7, fontweight='bold',
                color=cores_impacto['medium'],
            )

        ax.set_title(f'Emissões de {pol} por categoria de impacto', fontweight='bold')
        ax.set_ylabel('Toneladas')
        ax.legend(
            loc='upper center',
            bbox_to_anchor=(0.5, -0.05),
            ncol=3,
            frameon=False,
        )
        ax.grid(True, which='major', axis='y', linestyle='-', linewidth=0.5, alpha=0.5)
        ax.set_xticks(anos)
        ax.tick_params(axis='x', rotation=0)

        plt.tight_layout()
        plt.savefig(
            os.path.join(figures, f'barras_impacto_{pol}.png'),
            dpi=dpi, bbox_inches='tight'
        )
        plt.show()
        plt.close()