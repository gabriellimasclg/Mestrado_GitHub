# -*- coding: utf-8 -*-
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


#Utilizado
def plot_mapa_emissoes_por_poluente(
    inv_gdf, br_estado, br_regiao, pol_interest, figures,
    col_uf='SIGLA_UF', col_regiao='NM_REGIAO',
    threshold_anotacao_uf=0, dpi=300,
):
    '''
    Comparação de emissões de poluente por estado, util para descrever
    '''
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
        
#utilizado
def plot_mapas_impacto(    
    inv_gdf,
    br_estado,
    br_regiao,
    figures,
    col_uf='SIGLA_UF',
    col_cnpj='CPF_CNPJ',
    col_impact='impact',
    figsize_total=(9, 4),
    dpi=300,
):
    
    '''
    Mapa do brasil com pontos por impacto por região
    '''
    
    # Configurações e Traduções
    IMPACT_CONFIG = {
        'major':  {'label': 'major', 'color': '#c0392b'},
        'medium': {'label': 'int',   'color': '#e67e22'},
        'minor':  {'label': 'minor', 'color': '#27ae60'},
    }
    IMPACT_ORDER = ['major', 'medium', 'minor']
    
    REGIAO_ORDER = ['Norte', 'Nordeste', 'Centro-oeste', 'Sudeste', 'Sul']
    REGIAO_LABEL = ['N', 'NE', 'MW', 'SE', 'S']
    regiao_to_sigla = dict(zip(REGIAO_ORDER, REGIAO_LABEL))

    # Filtro de emissores únicos
    emissores_unicos = (
        inv_gdf[[col_cnpj, col_uf, col_impact, 'NM_REGIAO', 'geometry']]
        .drop_duplicates(subset=[col_cnpj, col_uf])
        .copy()
    )
    
    minx, miny, maxx, maxy = br_regiao.total_bounds

    fig = plt.figure(figsize=figsize_total, dpi=dpi, facecolor='white')
    gs = gridspec.GridSpec(1, 3, figure=fig, left=0.01, right=1, top=0.95, bottom=0.18, wspace=0)
    buffer = [pe.withStroke(linewidth=1.5, foreground='white')]
    for i, impact_key in enumerate(IMPACT_ORDER):
        ax_map = fig.add_subplot(gs[i])
        conf = IMPACT_CONFIG[impact_key]
        gdf_imp = emissores_unicos[emissores_unicos[col_impact] == impact_key]
        n_total = gdf_imp[col_cnpj].nunique()

        # 1. Desenho do Mapa Base
        br_regiao.plot(ax=ax_map, color='#f2f2f2', edgecolor='none', zorder=1)
        br_regiao.boundary.plot(ax=ax_map, color='#444444', linewidth=0.4, zorder=3)

        # 2. Plotagem dos Pontos (Hotspots por transparência)
        # alpha=0.3 e markersize=0.8 são bons pontos de partida para n~1000-6000
        gdf_imp.plot(
            ax=ax_map, 
            color=conf['color'], 
            markersize=0.8, 
            alpha=0.3, 
            zorder=2
        )

        # 3. Rótulos das Regiões
        for _, row in br_regiao.iterrows():
            if not row.geometry.is_empty:
                xc, yc = row.geometry.centroid.coords[0]
                sigla = regiao_to_sigla.get(row['NM_REGIAO'], row['NM_REGIAO'])
                ax_map.text(
                    xc, yc, sigla, ha='center', va='center', 
                    fontsize=8, color='#333333', fontweight='bold',
                    path_effects=buffer, zorder=4
                )

        ax_map.set_axis_off()
        # Filtra apenas o que não for ilha (ajuste o nome da coluna se necessário)
        # Ou simplesmente ignore pontos muito para a direita (longitude > -35)
        # Filtra apenas o que não for ilha (ajuste o nome da coluna se necessário)
        # Ou simplesmente ignore pontos muito para a direita (longitude > -35)
        continental = br_regiao.cx[: -34.5, :] 
        minx, miny, maxx, maxy = continental.total_bounds
        
        ax_map.set_xlim(minx, maxx)
        ax_map.set_ylim(miny, maxy)

        # 4. Inset: Barras
        ax_bar = ax_map.inset_axes([0.005, 0.05, 0.32, 0.25])
        counts = (
            gdf_imp.groupby('NM_REGIAO')[col_cnpj]
            .nunique()
            .reindex(REGIAO_ORDER, fill_value=0)
        )
        x_pos = np.arange(len(REGIAO_LABEL))
        
        bars = ax_bar.bar(
            x_pos, counts.values, color=conf['color'], 
            alpha=0.9, edgecolor='white', linewidth=0.3
        )
        
        ax_bar.set_xticks(x_pos)
        ax_bar.set_xticklabels(REGIAO_LABEL, fontsize=6)
        ax_bar.tick_params(axis='y', left=False, labelleft=False)
        ax_bar.tick_params(axis='x', length=0, rotation=90)
        ax_bar.spines[['top', 'right', 'left', 'bottom']].set_visible(False)
        ax_bar.patch.set_alpha(0)

        # Título do Inset com Subscrito
        ax_bar.set_title(
            fr"$N_{{{conf['label']}}} = {n_total:,}$", 
            fontsize=8, pad=7, loc='left'
        )

        for bar, val in zip(bars, counts.values):
            if val > 0:
                ax_bar.text(
                    bar.get_x() + bar.get_width()/2, bar.get_height(), f'{val:,}', 
                    ha='center', va='bottom', fontsize=5, fontweight='bold'
                )

    plt.savefig(os.path.join(figures, 'impact_points_map.png'), dpi=dpi, bbox_inches='tight')
    plt.show()
    plt.close()

#utilizado
def plot_barrash_impacto_poluentes(
    inv,
    pol_interest,
    color_pol_interest,
    figures,
    col_ano='ANO',
    col_impact='impact',
    figsize_total=(9, 2),
    dpi=300,
):
    '''
    Gráfico de barras horizontal com emissão por poluente e por ctegoria de emissor
    '''
    IMPACT_COLORS = {
        'major':  '#c0392b',
        'medium': '#e67e22',
        'minor':  '#27ae60',
    }
    IMPACT_ORDER = ['minor', 'medium', 'major']

    emis_ano_impact = (
        inv.groupby([col_ano, col_impact])[pol_interest]
        .sum()
        .reset_index()
    )

    anos = sorted(inv[col_ano].dropna().astype(int).unique())
    y    = np.arange(len(anos))

    n_pols = len(pol_interest)

    # sharey=True — anos aparecem só no primeiro painel
    fig, axes = plt.subplots(1, n_pols, figsize=figsize_total, dpi=dpi, sharey=True)
    if n_pols == 1:
        axes = [axes]

    for pi, (pol, cor) in enumerate(zip(pol_interest, color_pol_interest)):
        ax = axes[pi]
        is_first = (pi == 0)

        lefts    = np.zeros(len(anos))
        seg_vals = {}

        for impact in IMPACT_ORDER:
            df_imp = (
                emis_ano_impact[emis_ano_impact[col_impact] == impact]
                .set_index(col_ano)[pol]
                .reindex(anos, fill_value=0)
                .values.astype(float)
            )
            seg_vals[impact] = df_imp
            ax.barh(
                y, df_imp,
                left=lefts,
                color=IMPACT_COLORS[impact],
                height=0.98,          # <-- barras mais finas
                edgecolor='white',
                linewidth=0.3,
                zorder=2,
            )
            lefts += df_imp

        totais = lefts.copy()

        # % dentro de cada segmento se > 5%
        lefts_pct = np.zeros(len(anos))
        for impact in IMPACT_ORDER:
            vals = seg_vals[impact]
            for i, (val, tot) in enumerate(zip(vals, totais)):
                if tot == 0:
                    continue
                pct = val / tot * 100
                if pct < 25:
                    lefts_pct[i] += val
                    continue
                x_mid = lefts_pct[i] + val / 2
                ax.text(
                    x_mid, y[i],
                    f'{pct:.0f}%',
                    ha='center', va='center',
                    fontsize=7, fontweight='bold',
                    color='white', zorder=3
                )
                lefts_pct[i] += val

        # total à direita de cada barra
        for yi, tot in zip(y, totais):
            ax.text(
                tot * 1.02, yi,
                f'{tot/1e6:.1f}M' if tot >= 1e6 else
                f'{tot/1e3:.0f}k' if tot >= 1e3 else
                f'{tot:.0f}',
                ha='left', va='center',
                fontsize=8, color='#444444',
            )

        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda val, _:
                f'{val/1e6:.1f}M' if val >= 1e6 else
                f'{val/1e3:.0f}k' if val >= 1e3 else
                f'{val:.0f}'
            )
        )
        ax.xaxis.set_major_locator(mticker.MaxNLocator(nbins=4))
        ax.tick_params(axis='x', labelrotation=90)
        
        # anos só no primeiro painel (sharey esconde os demais)
        if is_first:
            ax.set_yticks(y)
            ax.set_yticklabels([str(a) for a in anos], fontsize=10)
        
        ax.tick_params(axis='both', length=0, labelsize=9)
        ax.set_ylim(-0.5, len(anos) - 0.5)

        # nome do poluente como título
        ax.set_title(f'{pol}', fontsize=12, fontweight='bold')
        
        ax.grid(True, axis='x', linestyle='--', linewidth=0.5, alpha=0.4)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_visible(False)

        # inverte Y para o ano mais recente ficar no topo
        ax.invert_yaxis()

        handles = [
            Patch(facecolor=IMPACT_COLORS[k], label=k.capitalize())
            for k in reversed(IMPACT_ORDER)
        ]
        fig.legend(
            handles=handles,
            ncols=3,
            fontsize=9,
            frameon=False,
            loc='upper center',
            bbox_to_anchor=(0.82, -0.07),  # centraliza abaixo de todos os painéis
        )
        fig.text(
            0.5, -0.12,          # x=centro, y=abaixo dos eixos
            'Emissions (ton)',
            ha='center', va='top',
            fontsize=10,
        )
    plt.tight_layout(w_pad=1.0)
    plt.subplots_adjust(bottom=0.12)  # abre espaço embaixo
    plt.savefig(os.path.join(figures,'barras_impacto_poluentes.png'),dpi=dpi, bbox_inches='tight')
    plt.show()
    plt.close()

def plot_barras_estado_poluente(
    inv,
    figures,
    pol_interest,
    col_uf='SIGLA_UF',
    col_ano='ANO',
    top_n=9,
    dpi=300,
):
    COLOR_REGIAO = {
        'Norte':        '#4e9fcc',
        'Nordeste':     '#e0a020',
        'Centro-oeste': '#e05c3a',
        'Sudeste':      '#7b5ea7',
        'Sul':          '#3aaa5c',
    }
    REGIAO_SIGLA = {
        'Norte': 'N', 'Nordeste': 'NE',
        'Centro-oeste': 'CW', 'Sudeste': 'SE', 'Sul': 'S',
    }
    MAPA_REGIAO = {
        'AC':'Norte','AP':'Norte','AM':'Norte','PA':'Norte',
        'RO':'Norte','RR':'Norte','TO':'Norte',
        'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste',
        'PB':'Nordeste','PE':'Nordeste','PI':'Nordeste',
        'RN':'Nordeste','SE':'Nordeste',
        'DF':'Centro-oeste','GO':'Centro-oeste','MT':'Centro-oeste','MS':'Centro-oeste',
        'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
        'PR':'Sul','RS':'Sul','SC':'Sul',
    }
    COLOR_OUTROS = '#aaaaaa'

    n_pol = len(pol_interest)
    fig, axes = plt.subplots(
        1, n_pol,
        figsize=(2.4 * n_pol, 5.2),
        dpi=dpi,
        facecolor='white',
    )
    if n_pol == 1:
        axes = [axes]

    for ax, pol in zip(axes, pol_interest):

        # ── Média anual: soma por estado+ano → média dos anos ─────────────────
        df_anual = (
            inv.groupby([col_uf, col_ano])[pol]
            .sum()
            .reset_index()
        )
        df_mean = (
            df_anual.groupby(col_uf)[pol]
            .mean()
            .reset_index()
            .sort_values(pol, ascending=False)
        )
        df_mean['regiao'] = df_mean[col_uf].map(MAPA_REGIAO)

        total_all = df_mean[pol].sum()

        # top 9 + "Outros" (média dos demais)
        df_top    = df_mean.iloc[:top_n].copy()
        df_outros = df_mean.iloc[top_n:]
        outros_val = df_outros[pol].mean() if len(df_outros) > 0 else 0

        outros_row = pd.DataFrame([{col_uf: 'Outros', pol: outros_val, 'regiao': 'Outros'}])
        df_final = pd.concat([outros_row, df_top], ignore_index=True)
        # crescente para barh (maior no topo)
        df_final = df_final.sort_values(pol, ascending=True).reset_index(drop=True)

        colors = [
            COLOR_OUTROS if r == 'Outros' else COLOR_REGIAO.get(r, '#cccccc')
            for r in df_final['regiao']
        ]

        y_pos = np.arange(len(df_final))
        bars = ax.barh(
            y_pos, df_final[pol],
            color=colors,
            edgecolor='white', linewidth=0.4,
            height=0.65,
        )

        ax.set_xscale('log')
        ax.figure.canvas.draw()  # força o renderer para medir pixels

        # ── % dentro da barra (se couber) senão não mostra ───────────────────
        x_min_log, x_max_log = ax.get_xlim()
        log_range = np.log10(x_max_log) - np.log10(x_min_log)

        for bar, val in zip(bars, df_final[pol]):
            pct = val / total_all * 100
            txt = f'{pct:.1f}%'

            # largura relativa da barra em fração do espaço log
            if val <= 0:
                continue
            bar_log_width = np.log10(val) - np.log10(max(bar.get_x(), x_min_log))
            bar_frac = (np.log10(val + bar.get_x()) - np.log10(x_min_log)) / log_range

            # posição central em escala log
            x_left  = bar.get_x() if bar.get_x() > 0 else x_min_log
            x_right = bar.get_x() + bar.get_width()
            x_center = 10 ** ((np.log10(x_left + 1e-12) + np.log10(x_right)) / 2)

            # testa se o texto cabe: aprox 4 chars ~ 6% do log range
            char_frac = len(txt) * 0.045
            bar_log_frac = (np.log10(x_right) - np.log10(max(x_left, 1e-12))) / log_range

            if bar_log_frac > char_frac:
                ax.text(
                    x_center,
                    bar.get_y() + bar.get_height() / 2,
                    txt,
                    ha='center', va='center',
                    fontsize=6.5, color='white', fontweight='bold',
                )

        ax.set_yticks(y_pos)
        ax.set_yticklabels(df_final[col_uf], fontsize=8, fontweight='bold')
        ax.set_title(pol, fontsize=10, fontweight='bold', pad=6)
        ax.spines[['top', 'right', 'left']].set_visible(False)
        ax.spines['bottom'].set_color('#cccccc')
        ax.tick_params(axis='x', labelsize=6.5, colors='#555555')
        ax.tick_params(axis='y', length=0)
        ax.set_facecolor('white')

    # ── Legenda: regiões em 1 linha ───────────────────────────────────────────
    region_patches = [
        Patch(facecolor=COLOR_REGIAO[reg], label=f'{REGIAO_SIGLA[reg]}')
        for reg in COLOR_REGIAO
    ] + [Patch(facecolor=COLOR_OUTROS, label='Outros')]

    fig.legend(
        handles=region_patches,
        loc='lower center',
        ncol=6,
        fontsize=8,
        frameon=False,
        bbox_to_anchor=(0.5, -0.04),
    )
    fig.text(
        0.5, 0.01,
        'Mean Annual Emission (ton/yr)',
        ha='center', fontsize=9, color='#333333',
    )

    plt.tight_layout(rect=[0, 0.06, 1, 1])
    plt.savefig(
        os.path.join(figures, 'barras_estado_poluente_v2.png'),
        dpi=dpi, bbox_inches='tight', facecolor='white',
    )
    plt.show()
    plt.close()
    

def plot_mapa_regioes(
    br_estado,
    figures,
    dpi=300,
    figsize=(8, 8),
):
    COLOR_REGIAO = {
        'Norte':        '#d0d0d0',
        'Nordeste':     '#a0a0a0',
        'Centro-oeste': '#707070',
        'Sudeste':      '#000000',
        'Sul':          '#383838',
    }
    MAPA_REGIAO = {
        'AC':'Norte','AP':'Norte','AM':'Norte','PA':'Norte',
        'RO':'Norte','RR':'Norte','TO':'Norte',
        'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste',
        'PB':'Nordeste','PE':'Nordeste','PI':'Nordeste',
        'RN':'Nordeste','SE':'Nordeste',
        'DF':'Centro-oeste','GO':'Centro-oeste','MT':'Centro-oeste','MS':'Centro-oeste',
        'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
        'PR':'Sul','RS':'Sul','SC':'Sul',
    }
    # cor do label: preto para cinzas claros, branco para escuros
    LABEL_COLOR = {
        'Norte':        'black',
        'Nordeste':     'black',
        'Centro-oeste': 'white',
        'Sudeste':      'white',
        'Sul':          'white',
    }

    # garante que a coluna de região existe
    br_estado = br_estado.copy()
    br_estado['regiao'] = br_estado['SIGLA_UF'].map(MAPA_REGIAO)
    br_estado['color']  = br_estado['regiao'].map(COLOR_REGIAO)

    fig, ax = plt.subplots(figsize=figsize, dpi=dpi, facecolor='white')

    for _, row in br_estado.iterrows():
        color  = row['color'] if pd.notna(row['color']) else '#cccccc'
        regiao = row['regiao'] if pd.notna(row['regiao']) else ''

        # borda branca onde fundo escuro, preta onde fundo claro
        edge = 'white' if regiao in ['Centro-oeste', 'Sudeste', 'Sul'] else 'black'

        br_estado[br_estado['SIGLA_UF'] == row['SIGLA_UF']].plot(
            ax=ax,
            color=color,
            edgecolor=edge,
            linewidth=0.3,       # borda mais fina
        )

    # ── labels por estado ─────────────────────────────────────────────────────
    for _, row in br_estado.iterrows():
        centroid = row['geometry'].centroid
        regiao   = row['regiao'] if pd.notna(row['regiao']) else ''
        txt_color = LABEL_COLOR.get(regiao, 'black')

        ax.text(
            centroid.x, centroid.y,
            row['SIGLA_UF'],
            ha='center', va='center',
            fontsize=14, fontweight='bold',   # 2x maior
            color=txt_color,
            zorder=5,
        )

    # ── legenda ───────────────────────────────────────────────────────────────
    REGIAO_SIGLA = {
        'Norte': 'N', 'Nordeste': 'NE',
        'Centro-oeste': 'CW', 'Sudeste': 'SE', 'Sul': 'S',
    }
    legend_patches = [
        Patch(
            facecolor=COLOR_REGIAO[reg],
            edgecolor='white' if reg in ['Centro-oeste', 'Sudeste', 'Sul'] else 'black',
            linewidth=0.8,
            label=REGIAO_SIGLA[reg],
        )
        for reg in COLOR_REGIAO
    ]
    ax.legend(
        handles=legend_patches,
        loc='lower left',
        fontsize=14,
        frameon=False,
        bbox_to_anchor=(0.1, 0.15),   # aproxima do mapa
    )
    
    ax.set_axis_off()
    plt.tight_layout()
    plt.savefig(
        os.path.join(figures, 'mapa_regioes.png'),
        dpi=dpi, bbox_inches='tight', facecolor='white',
    )
    plt.show()
    plt.close()


def plot_barras_estado_poluente(
    inv,
    figures,
    pol_interest,
    col_uf='SIGLA_UF',
    col_ano='ANO',
    top_n=9,
    dpi=300,
    figsize_total=(10, 4),
):
    COLOR_REGIAO = {
        'Norte':        '#d0d0d0',
        'Nordeste':     '#a0a0a0',
        'Centro-oeste': '#707070',
        'Sudeste':      '#000000',
        'Sul':          '#383838',
    }
    COLOR_OUTROS = '#ffffff'
    REGIAO_SIGLA = {
        'Norte': 'N', 'Nordeste': 'NE',
        'Centro-oeste': 'CW', 'Sudeste': 'SE', 'Sul': 'S',
    }
    MAPA_REGIAO = {
        'AC':'Norte','AP':'Norte','AM':'Norte','PA':'Norte',
        'RO':'Norte','RR':'Norte','TO':'Norte',
        'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste',
        'PB':'Nordeste','PE':'Nordeste','PI':'Nordeste',
        'RN':'Nordeste','SE':'Nordeste',
        'DF':'Centro-oeste','GO':'Centro-oeste','MT':'Centro-oeste','MS':'Centro-oeste',
        'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
        'PR':'Sul','RS':'Sul','SC':'Sul',
    }

    n_pol = len(pol_interest)
    fig, axes = plt.subplots(
        1, n_pol,
        figsize=figsize_total,
        dpi=dpi,
        sharey=False,
        facecolor='white',
    )
    if n_pol == 1:
        axes = [axes]

    for ax, pol in zip(axes, pol_interest):

        df_anual = (
            inv.groupby([col_uf, col_ano])[pol]
            .sum()
            .reset_index()
        )
        df_mean = (
            df_anual.groupby(col_uf)[pol]
            .mean()
            .reset_index()
            .sort_values(pol, ascending=False)
        )
        df_mean['regiao'] = df_mean[col_uf].map(MAPA_REGIAO)

        total_all = df_mean[pol].sum()

        df_top    = df_mean.iloc[:top_n].copy()
        df_outros = df_mean.iloc[top_n:]
        outros_val = df_outros[pol].mean() if len(df_outros) > 0 else 0

        outros_row = pd.DataFrame([{col_uf: 'Others', pol: outros_val, 'regiao': 'Others'}])
        df_final = pd.concat([outros_row, df_top], ignore_index=True)
        df_final = df_final.sort_values(pol, ascending=True).reset_index(drop=True)

        colors = [
            COLOR_OUTROS if r == 'Others' else COLOR_REGIAO.get(r, '#cccccc')
            for r in df_final['regiao']
        ]

        y_pos = np.arange(len(df_final))
        bars = ax.barh(
            y_pos, df_final[pol],
            color=colors,
            edgecolor=[
                'black' if r == 'Others' else 'white'
                for r in df_final['regiao']
            ],
            linewidth=0.8,
            height=0.85,
            zorder=2,
        )

        totais = df_final[pol].values

        # ── % no centro geométrico da barra ───────────────────────────────────
        x_min = df_final[pol][df_final[pol] > 0].min() * 0.8
        for bar, val, tot_all in zip(bars, df_final[pol], [total_all]*len(df_final)):
            if tot_all == 0 or val <= 0:
                continue
            pct = val / tot_all * 100
            if pct < 5:
                continue
            x_left  = max(bar.get_x(), x_min)
            x_right = bar.get_x() + bar.get_width()
            x_center = 10 ** ((np.log10(x_left) + np.log10(x_right)) / 2)
            # cor do texto: branco para cinzas escuros, preto para claros
            txt_color = 'white' if COLOR_REGIAO.get(
                df_final.loc[df_final[col_uf] == bar.get_label() if hasattr(bar, 'get_label') else df_final.index[list(bars).index(bar)], 'regiao'].values[0]
                if False else df_final.iloc[list(bars).index(bar)]['regiao'], '#000') in ['#303030','#484848','#606060','#888888'] else 'black'
            ax.text(
                x_center,
                bar.get_y() + bar.get_height() / 2,
                f'{pct:.1f}%',
                ha='center', va='center',
                fontsize=9, fontweight='bold',
                color='white', zorder=3,
            )

        # ── total à direita ───────────────────────────────────────────────────
        for yi, val in zip(y_pos, totais):
            ax.text(
                val * 1.02, yi,
                f'{val/1e6:.1f}M' if val >= 1e6 else
                f'{val/1e3:.0f}k' if val >= 1e3 else
                f'{val:.0f}',
                ha='left', va='center',
                fontsize=10, color='#444444',
            )

        # ── Eixo X ────────────────────────────────────────────────────────────
        ax.set_xscale('log')
        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda val, _:
                f'{val/1e6:.1f}M' if val >= 1e6 else
                f'{val/1e3:.0f}k' if val >= 1e3 else
                f'{val:.0f}'
            )
        )
        ax.xaxis.set_minor_locator(mticker.NullLocator())
        ax.tick_params(axis='x', labelrotation=90, which='both')

        ax.set_yticks(y_pos)
        ax.set_yticklabels(df_final[col_uf], fontsize=8, fontweight='bold')
        ax.set_title(pol, fontsize=12, fontweight='bold')

        ax.tick_params(axis='both', length=0, labelsize=9)
        ax.set_ylim(-0.5, len(df_final) - 0.5)

        ax.grid(True, axis='x', linestyle='--', linewidth=0.9, alpha=0.6, which='major')
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_visible(False)

        ax.set_facecolor('white')

    # ── Legenda ───────────────────────────────────────────────────────────────
    region_patches = [
        Patch(facecolor=COLOR_REGIAO[reg], label=REGIAO_SIGLA[reg])
        for reg in COLOR_REGIAO
    ] + [Patch(facecolor=COLOR_OUTROS, label='Others')]

    fig.text(
        0.5, -0.12,
        'Mean Annual Emission (ton/yr)',
        ha='center', va='top',
        fontsize=10,
    )

    plt.tight_layout()
    plt.subplots_adjust(wspace=0.65)   # diminua para aproximar, aumente para afastar
    plt.subplots_adjust(bottom=0.03)
    
    plt.savefig(
        os.path.join(figures, 'barras_estado_poluente_v2.png'),
        dpi=dpi, bbox_inches='tight', facecolor='white',
    )
    plt.show()
    plt.close()
