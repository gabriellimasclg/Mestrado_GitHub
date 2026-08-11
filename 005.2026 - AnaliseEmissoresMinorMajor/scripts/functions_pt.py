
"""
Functions para análise multipoluente do inventário
"""

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.gridspec as gridspec 
from matplotlib.patches import Patch
import matplotlib.patheffects as pe         
from matplotlib.colorbar import ColorbarBase
import matplotlib.ticker as mticker

# Cor fixa por setor agrupado (Combustão externa = cinza, vai por baixo)
# Cor fixa por NFR (o mais frequente, 1.A.2.e, fica cinza = fundo)
SETOR_AGRUPADO_COLORS = {
    # --- 12 NFRs que viram linha própria (corte 5%): paleta de alto contraste ---
    '1.A.2.e.i':   '#e6194B',  # vermelho
    '1.A.2.a':     '#4363d8',  # azul
    '1.A.2.e':     '#3cb44b',  # verde
    '1.A.1.a':     '#f58231',  # laranja
    '1.A.2.f':     '#911eb4',  # roxo
    '2.H.1':       '#000000',  # preto
    '1.A.1.b':     '#00b8d4',  # ciano
    '1.A.2.d':     '#f032e6',  # magenta
    '2.C.1':       '#9A6324',  # marrom
    '1.A.2.g':     '#808000',  # oliva
    '1.A.1.c':     '#1f9e89',  # teal
    '2.B.10.a':    '#800000',  # bordô
    # --- demais NFRs (caem em "Outros" no gráfico de linhas) ---
    '2.H.2':       '#ff7f0e',  # laranja
    '1.A.2.c':     '#d62728',  # vermelho
    '2.C.7.c':     '#9467bd',  # roxo
    '1.A.2.b':     '#bcbd22',  # amarelo-oliva
    '2.A.2':       '#637939',  # verde-oliva
    '2.A.1':       '#7b4173',  # roxo-escuro
    '2.C.2':       '#e7969c',  # rosa-claro
    '2.A.3':       '#a55194',  # magenta
    '2.D.3.d':     '#6b6ecf',  # lilás
    '2.C.3':       '#b5cf6b',  # verde-claro
    '2.C.5':       '#cedb9c',  # verde-pálido
    '1.B.2.a.iv':  '#d6616b',  # coral
    '2.C.6':       '#ce6dbd',  # rosa-magenta
    '2.C.7.a':     '#de9ed6',  # rosa-lavanda
    '1':  "#af4646",
    '2.A':'#43c7d8',
    '2.B':'#3cb44b',
    '2.C':'#f58231',
    '2.D':'#f032e6',
    '2.H':"#FFA600",
    '2':'#4363d8',
}
COR_SETOR_OUTROS = "#cfcfcf"   # cinza-claro para NFR fora do dicionário









#Utilizado
#Utilizado
def plot_mapa_emissoes_por_poluente(
    inv_gdf, br_estado, br_regiao, pol_interest, figures,
    col_uf='SIGLA_UF', col_regiao='NM_REGIAO',
    threshold_anotacao_uf=0, dpi=300,
):
    '''
    Comparação de emissões de poluente por região e por estado.
    '''
    cmap = plt.cm.Reds

    # nome cheio do br_regiao -> sigla igual à do inv_gdf (N/NE/MW/SE/S)
    NOME_TO_SIGLA = {
        'Norte': 'N', 'Nordeste': 'NE',
        'Centro-oeste': 'MW', 'Centro-Oeste': 'MW',
        'Sudeste': 'SE', 'Sul': 'S',
        'N': 'N', 'NE': 'NE', 'MW': 'MW', 'SE': 'SE', 'S': 'S', 'CO': 'MW',
    }
    # coluna-chave no br_regiao já traduzida pra sigla
    br_regiao = br_regiao.copy()
    br_regiao['_reg'] = br_regiao[col_regiao].map(NOME_TO_SIGLA).fillna(br_regiao[col_regiao])

    for pol in pol_interest:

        # emissão por região (inv_gdf -> códigos N/NE/MW/SE/S)
        emis_reg = (
            inv_gdf.groupby(col_regiao)[pol].sum()
            .reset_index().rename(columns={pol: 'emissao', col_regiao: 'reg_key'})
        )
        total = emis_reg['emissao'].sum()
        emis_reg['pct'] = emis_reg['emissao'] / total * 100
        # merge pela SIGLA (br_regiao._reg  ==  emis_reg.reg_key)
        br_reg_plot = br_regiao.merge(emis_reg, left_on='_reg', right_on='reg_key', how='left')

        # emissão por estado (SIGLA_UF casa nos dois)
        emis_uf = (
            inv_gdf.groupby(col_uf)[pol].sum()
            .reset_index().rename(columns={pol: 'emissao'})
        )
        emis_uf['pct'] = emis_uf['emissao'] / total * 100
        br_uf_plot = br_estado.merge(emis_uf, on=col_uf, how='left')

        norm = mcolors.Normalize(vmin=0, vmax=100)

        fig, axes = plt.subplots(
            1, 2, figsize=(14, 6),
            gridspec_kw={'wspace': -0.4}
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
            sigla = row['_reg']
            label = f"{sigla}\n{pct:.1f}%" if not np.isnan(pct) else sigla
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

        fig.suptitle(f'Distribuição das emissões por região e estado — {pol}',
                     fontsize=13, fontweight='bold')

        plt.savefig(os.path.join(figures, f'mapa_espacial_{pol}.png'),
                    dpi=dpi, bbox_inches='tight')
        plt.show()
        plt.close()

        
#utilizado




def adicionar_mapa_regioes(br_estado, ax):
    """Função auxiliar: desenha o mapa das regiões em um 'ax' específico."""
    COLOR_REGIAO = {
        'Norte':        '#B1CBB1',
        'Nordeste':     '#DEB3B2',
        'Centro-oeste': '#FBE3BA',
        'Sudeste':      '#C7C2D9',
        'Sul':          '#C5D5E7',
    }
    MAPA_REGIAO = {
        'AC':'Norte','AP':'Norte','AM':'Norte','PA':'Norte','RO':'Norte','RR':'Norte','TO':'Norte',
        'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste','PB':'Nordeste','PE':'Nordeste','PI':'Nordeste','RN':'Nordeste','SE':'Nordeste',
        'DF':'Centro-oeste','GO':'Centro-oeste','MT':'Centro-oeste','MS':'Centro-oeste',
        'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
        'PR':'Sul','RS':'Sul','SC':'Sul',
    }

    br_estado = br_estado.copy()
    br_estado['regiao'] = br_estado['SIGLA_UF'].map(MAPA_REGIAO)
    br_estado['color']  = br_estado['regiao'].map(COLOR_REGIAO)

    for _, row in br_estado.iterrows():
        color = row['color'] if pd.notna(row['color']) else '#cccccc'
        br_estado[br_estado['SIGLA_UF'] == row['SIGLA_UF']].plot(
            ax=ax, color=color, edgecolor='white', linewidth=0.3
        )

    # ── offsets manuais (dx, dy) para afastar rótulos apertados ──────────────
    OFFSETS = {
        'GO': (-1.0, -1.0),
        'RN': ( 0.6,  1.0),
        'PB': ( 0.5,  0.2),
        'PE': (-0.1, -0.3),
        'AL': ( 1.0, -0.5),
        'SE': ( 0.4, -0.9),
    }

    for _, row in br_estado.iterrows():
        centroid = row['geometry'].centroid
        sigla    = row['SIGLA_UF']
        x, y = centroid.x, centroid.y
        if sigla in OFFSETS:
            dx, dy = OFFSETS[sigla]
            x += dx
            y += dy
        ax.text(x, y, sigla, ha='center', va='center',
                fontsize=10, fontweight='bold', color='black', zorder=5)

    ax.set_axis_off()
    # (sem plt.close() aqui — ele fechava a figura principal e gerava a figura vazia)

# cores por região (compartilhadas entre o mapa e as barras)
COLOR_REGIAO = {
        'Norte':        '#B1CBB1',
        'Nordeste':     '#DEB3B2',
        'Centro-oeste': '#FBE3BA',
        'Sudeste':      '#C7C2D9',
        'Sul':          '#C5D5E7',
    }
MAPA_REGIAO = {
    'AC':'Norte','AP':'Norte','AM':'Norte','PA':'Norte','RO':'Norte','RR':'Norte','TO':'Norte',
    'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste','PB':'Nordeste',
    'PE':'Nordeste','PI':'Nordeste','RN':'Nordeste','SE':'Nordeste',
    'DF':'Centro-oeste','GO':'Centro-oeste','MT':'Centro-oeste','MS':'Centro-oeste',
    'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
    'PR':'Sul','RS':'Sul','SC':'Sul',
}

def plot_barras_poluente_estado(
    inv,
    figures,
    pol_interest,
    col_uf='SIGLA_UF',
    pol_sem_log=(),
    pct_min=0.01,                # % mínima pra escrever o rótulo (agora 0,01%)
    figsize=(8.27, 11.69),
    dpi=300,
    nome_arquivo='barras_poluente_estado.png',
):
    """
    Um painel por poluente (ordenado maior->menor). Sigla do estado em cima
    da barra (sem rotação); % do total do poluente dentro da barra. Log (menos
    pol_sem_log). Cor por região (ref = mapa). A4.
    """
    def _cor_texto(cor):
        r, g, b = mcolors.to_rgb(cor)
        return 'black' if (0.299*r + 0.587*g + 0.114*b) > 0.6 else 'white'

    def _fmt(val, _):
        if val >= 1e6: return f'{val/1e6:.0f}M'
        if val >= 1e3: return f'{val/1e3:.0f}k'
        if val >= 1:   return f'{val:.0f}'
        if val > 0:    return f'{val:g}'        # casas decimais p/ valores < 1 (Pb)
        return '0'

    def _fmt_pct(p):
        if p >= 1:   return f'{p:.0f}%'
        if p >= 0.1: return f'{p:.1f}%'
        return f'{p:.2f}%'                        # ex.: 0,01%

    emis = inv.groupby(col_uf)[list(pol_interest)].sum()

    n_pol = len(pol_interest)
    fig, axes = plt.subplots(n_pol, 1, dpi=dpi, facecolor='white', figsize=figsize)
    axes = np.atleast_1d(axes)

    for ax, pol in zip(axes, pol_interest):
        s = emis[pol].sort_values(ascending=False)
        ufs  = s.index.tolist()
        vals = s.values
        total = vals.sum()
        x = np.arange(len(ufs))
        cores = [COLOR_REGIAO.get(MAPA_REGIAO.get(uf), '#cccccc') for uf in ufs]

        ax.bar(x, vals, color=cores, width=0.86,
               edgecolor='white', linewidth=0.3, zorder=2)

        if pol not in pol_sem_log:
            #ax.set_yscale('log')
            
            pos = vals[vals > 0]
            if len(pos):
                ax.set_ylim(bottom=pos.min() * 0.6, top=pos.max() * 8)
            log_scale = True
        else:
            ax.set_ylim(0, vals.max() * 1.3)
            log_scale = False

        # sigla do estado em cima da barra — sem rotação, afastada, fonte maior
        for xi, uf, v in zip(x, ufs, vals):
            if v <= 0:
                continue
            ax.annotate(uf, (xi, v), textcoords='offset points', xytext=(0, 4),
                        ha='center', va='bottom', fontsize=9, fontweight='bold',
                        color='#222222')

        # % dentro da barra (fonte ~1.3x maior)
        for xi, v, cor in zip(x, vals, cores):
            if total <= 0 or v <= 0:
                continue
            pct = v / total * 100
            if pct < pct_min:
                continue
            y_center = (10 ** ((np.log10(ax.get_ylim()[0]) + np.log10(v)) / 2)
                        if log_scale else v / 2)
            ax.text(xi, y_center, _fmt_pct(pct), ha='center', va='center',
                    fontsize=8, fontweight='bold', rotation=90,
                    color=_cor_texto(cor), zorder=3)

        ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt))
        ax.set_ylabel(pol, rotation=90, ha='center', va='center',
                      fontsize=13, fontweight='bold', labelpad=10)
        ax.tick_params(axis='y', labelsize=8, length=0)

        ax.set_xticks([])
        ax.set_xlim(-0.6, len(x) - 0.4)
        ax.margins(x=0)

        ax.grid(True, axis='y', linestyle='--', linewidth=0.6, alpha=0.5)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_visible(False)

    fig.text(0.01, 0.5, 'Total Emissions (t)', rotation=90,
             ha='center', va='center', fontsize=12)

    plt.tight_layout(rect=[0.02, 0, 1, 1])
    plt.subplots_adjust(hspace=0.15)
    plt.savefig(os.path.join(figures, nome_arquivo),
                dpi=dpi, bbox_inches='tight', facecolor='white')
    plt.show()
    plt.close()


# -*- coding: utf-8 -*-
"""
Função para gerar o mosaico de mapas pixelados (12x12 km ~ 0.1°)
por poluente, com gráfico de barras por região e colorbar individual.

Para adicionar ao functions_pt.py
"""
import os
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.colors as mcolors
from matplotlib.colorbar import ColorbarBase
from shapely.geometry import box





