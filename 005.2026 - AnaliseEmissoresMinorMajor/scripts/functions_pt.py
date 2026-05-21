
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
    REGIAO_LABEL = ['Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul']
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

        IMPACT_LABEL_PT = {'major': 'Alto', 'medium': 'Médio', 'minor': 'Baixo'}
        handles = [
            Patch(facecolor=IMPACT_COLORS[k], label=IMPACT_LABEL_PT[k])
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
            'Emissões (ton)',
            ha='center', va='top',
            fontsize=10,
        )
    plt.tight_layout(w_pad=1.0)
    plt.subplots_adjust(bottom=0.12)  # abre espaço embaixo
    plt.savefig(os.path.join(figures,'barras_impacto_poluentes.png'),dpi=dpi, bbox_inches='tight')
    plt.show()
    plt.close()

def adicionar_mapa_regioes(br_estado, ax):
    """Função auxiliar: desenha o mapa das regiões em um 'ax' específico."""
    COLOR_REGIAO = {
        'Norte':        '#5b8db8',
        'Nordeste':     '#c4a052',
        'Centro-oeste': '#a0785a',
        'Sudeste':      '#7a6e9e',
        'Sul':          '#5a9e82',
    }
    MAPA_REGIAO = {
        'AC':'Norte','AP':'Norte','AM':'Norte','PA':'Norte','RO':'Norte','RR':'Norte','TO':'Norte',
        'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste','PB':'Nordeste','PE':'Nordeste','PI':'Nordeste','RN':'Nordeste','SE':'Nordeste',
        'DF':'Centro-oeste','GO':'Centro-oeste','MT':'Centro-oeste','MS':'Centro-oeste',
        'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
        'PR':'Sul','RS':'Sul','SC':'Sul',
    }
    LABEL_COLOR = {
        'Norte': 'black', 'Nordeste': 'black', 'Centro-oeste': 'white', 'Sudeste': 'white', 'Sul': 'white',
    }

    br_estado = br_estado.copy()
    br_estado['regiao'] = br_estado['SIGLA_UF'].map(MAPA_REGIAO)
    br_estado['color']  = br_estado['regiao'].map(COLOR_REGIAO)

    for _, row in br_estado.iterrows():
        color  = row['color'] if pd.notna(row['color']) else '#cccccc'
        regiao = row['regiao'] if pd.notna(row['regiao']) else ''
        edge = 'white' 

        br_estado[br_estado['SIGLA_UF'] == row['SIGLA_UF']].plot(
            ax=ax, color=color, edgecolor=edge, linewidth=0.3
        )

    # ── offsets manuais (dx, dy) ──────────────────────────────────────────────
    # Valores em graus (aprox.). Positivo move para direita/cima, negativo para esquerda/baixo.
    # Ajuste os números abaixo até ficar perfeito no seu gráfico.
    OFFSETS = {
        'GO': (-1.0,  -1),  # Afasta do DF (esquerda/cima)
        'RN': ( 0.6,  1),  # Joga para a direita e cima
        'PB': ( 0.5,  0.2),  # Joga para a direita
        'PE': ( -0.1, -0.3),  # Joga para a direita e baixo
        'AL': ( 1, -0.5),  # Joga mais para baixo
        'SE': ( 0.4, -0.9),  # Joga ainda mais para baixo
    }

    # ── labels por estado ─────────────────────────────────────────────────────
    for _, row in br_estado.iterrows():
        centroid = row['geometry'].centroid
        sigla    = row['SIGLA_UF']
        regiao   = row['regiao'] if pd.notna(row['regiao']) else ''
        
        # Coordenadas originais
        x, y = centroid.x, centroid.y

        # Aplica o deslocamento se o estado estiver no dicionário
        if sigla in OFFSETS:
            dx, dy = OFFSETS[sigla]
            x += dx
            y += dy

        ax.text(
            x, y,
            sigla,
            ha='center', va='center',
            fontsize=10, fontweight='bold',
            color='black',
            zorder=5,
        )
    
    ax.set_axis_off()


def plot_barras_estado_poluente(
    inv,
    br_estado,
    figures,
    pol_interest,
    col_uf='SIGLA_UF',
    col_ano='ANO',
    top_n=6,
    dpi=300,
    figsize_total=(10, 8),
):
    COLOR_REGIAO = {
        'Norte':        '#5b8db8',
        'Nordeste':     '#c4a052',
        'Centro-oeste': '#a0785a',
        'Sudeste':      '#7a6e9e',
        'Sul':          '#5a9e82',
    }
    COLOR_OUTROS = '#ffffff'
    REGIAO_SIGLA = {
        'Norte': 'Norte', 'Nordeste': 'Nordeste',
        'Centro-oeste': 'Centro-Oeste', 'Sudeste': 'Sudeste', 'Sul': 'Sul',
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

    fig, axes = plt.subplots(
        2,4,
        figsize=figsize_total,
        dpi=dpi,
        sharey=False,
        facecolor='white',
    )
    axes_flat = axes.flatten()
    
    for ax, pol in zip(axes_flat, pol_interest):

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

        outros_row = pd.DataFrame([{col_uf: 'Outros', pol: outros_val, 'regiao': 'Outros'}])
        df_final = pd.concat([outros_row, df_top], ignore_index=True)
        df_final = df_final.sort_values(pol, ascending=True).reset_index(drop=True)

        colors = [
            COLOR_OUTROS if r == 'Outros' else COLOR_REGIAO.get(r, '#cccccc')
            for r in df_final['regiao']
        ]

        y_pos = np.arange(len(df_final))
        bars = ax.barh(
            y_pos, df_final[pol],
            color=colors,
            edgecolor=[
                'black' if r == 'Outros' else 'white'
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
            if pct < 2:
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
                fontsize=11, fontweight='bold',
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
                fontsize=11, color='#444444',
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
        ax.set_yticklabels(df_final[col_uf], fontsize=12, fontweight='bold')
        ax.set_title(pol, fontsize=14, fontweight='bold')

        ax.tick_params(axis='both', length=0, labelsize=12)
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
    ] + [Patch(facecolor=COLOR_OUTROS, label='Outros')]

    fig.text(
        0.5, -0.12,
        'Emissão Anual Média (ton/ano)',
        ha='center', va='top',
        fontsize=10,
    )
    
    for j in range(len(pol_interest), len(axes_flat)):
        axes_flat[j].axis('off')
        
    # ── ADICIONANDO O MAPA FLUTUANTE ──────────────────────────────────────────
    # fig.add_axes([esquerda, base, largura, altura])
    # Valores de 0 a 1. Ajuste esses números para mover o mapa pelo canvas!
    ax_mapa = fig.add_axes([0.72, -0.05, 0.35, 0.5]) 
    
    # Chama a função auxiliar
    adicionar_mapa_regioes(br_estado, ax=ax_mapa)
    
    # ── LEGENDA GERAL ─────────────────────────────────────────────────────────
    # Notei que no seu código original você criou as legendas mas não as plotou.
    # Adicione isso para a legenda geral aparecer na figura:
    region_patches = [
        Patch(facecolor=COLOR_REGIAO[reg], label=REGIAO_SIGLA[reg], 
              edgecolor='black' if reg in ['Centro-oeste', 'Sudeste', 'Sul'] else 'black')
        for reg in COLOR_REGIAO
    ] 
    
    fig.legend(
        handles=region_patches,
        loc='lower right',      # Ou outra posição de sua escolha
        bbox_to_anchor=(1.02, -0.1), 
        ncol=2, 
        frameon=False,
        fontsize=10
    )

    plt.tight_layout()
    plt.subplots_adjust(hspace=0.2, wspace=0.35, bottom=-0.05)
    
    plt.savefig(
        os.path.join(figures, 'barras_estado_poluente_v2.png'),
        dpi=dpi, bbox_inches='tight', facecolor='white',
    )
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


def plot_mosaico_pixels_poluentes(
    inv_gdf,
    br_estado,
    br_regiao,
    pol_interest,
    figures,
    pol_destaque='MP10',            # poluente que vira figura solo grandona
    col_uf='SIGLA_UF',
    col_regiao='NM_REGIAO',
    col_ano='ANO',
    resolucao=0.1,                  # ~12 km em graus (0.1° ≈ 11.1 km)
    limites_grid=None,
    n_cols=3,
    cmap_name='inferno',
    figsize_mosaico=(14, 10),       # 3 cols x 2 linhas (5 mapas + 1 slot vazio)
    figsize_solo=(8, 8),            # figura solo do destaque
    dpi=300,
    nome_mosaico='mosaico_pixels_poluentes.png',
    nome_solo='mapa_destaque_{pol}.png',
):
    """
    Gera DUAS figuras:
      1) Figura solo grandona com o pol_destaque (1 mapa).
      2) Mosaico com TODOS os outros poluentes de pol_interest
         (o destaque NÃO entra no mosaico).

    Cada slot tem mapa BR com pixels ~12x12 km (média anual),
    barras por região (N, NE, CO, SE, S) e colorbar log no canto.

    Parâmetros
    ----------
    pol_destaque : str
        Poluente que vira figura solo. Padrão: 'MP10'.
        Use None pra gerar só o mosaico com todos.
    """

    # ---------- Configurações ----------
    if limites_grid is None:
        limites_grid = {'xmin': -74.0, 'xmax': -34.0, 'ymin': -34.0, 'ymax': 6.0}

    REGIAO_ORDER = ['Norte', 'Nordeste', 'Centro-oeste', 'Sudeste', 'Sul']
    REGIAO_SIGLA = ['N', 'NE', 'CO', 'SE', 'S']

    cmap = plt.get_cmap(cmap_name).copy()
    cmap.set_bad(color='none')

    # ---------- 1. Grid 0.1° ----------
    xmin, xmax = limites_grid['xmin'], limites_grid['xmax']
    ymin, ymax = limites_grid['ymin'], limites_grid['ymax']

    x_coords = np.arange(xmin, xmax, resolucao)
    y_coords = np.arange(ymin, ymax, resolucao)

    cells = [box(x, y, x + resolucao, y + resolucao)
             for x in x_coords for y in y_coords]
    grid = gpd.GeoDataFrame(geometry=cells, crs=inv_gdf.crs)
    grid['lon'] = grid.geometry.centroid.x
    grid['lat'] = grid.geometry.centroid.y
    grid['grid_id'] = grid.index

    # ---------- 2. sjoin pontos -> células (uma vez só) ----------
    cols_keep = ['geometry', col_uf, col_regiao, col_ano] + list(pol_interest)
    cols_keep = [c for c in cols_keep if c in inv_gdf.columns]
    pontos = inv_gdf[cols_keep].copy()

    pontos_na_grade = gpd.sjoin(
        pontos, grid[['geometry', 'grid_id', 'lat', 'lon']],
        how='inner', predicate='within'
    )

    # ---------- 3. Função interna: desenha 1 slot ----------
    def _desenha_slot(ax_map, pol,
                      title_fs=15, bar_label_fs=8, bar_sigla_fs=9,
                      cb_label_fs=10, cb_tick_fs=10,
                      bar_inset=(0.005, 0.04, 0.35, 0.28),
                      cb_inset=(0.86, 0.04, 0.05, 0.5)):
        # média anual por pixel
        anos_existentes = sorted(pontos_na_grade[col_ano].dropna().unique())
        emis_pixel_ano = (
            pontos_na_grade
            .groupby(['grid_id', col_ano])[pol]
            .sum()
            .unstack(col_ano)
            .reindex(columns=anos_existentes, fill_value=0)
        )
        emis_pixel_mean = emis_pixel_ano.mean(axis=1)
        emis_pixel_mean = emis_pixel_mean[emis_pixel_mean > 0]

        if emis_pixel_mean.empty:
            ax_map.set_title(f'{pol} (sem dados)', fontsize=title_fs - 5)
            ax_map.set_axis_off()
            return

        grid_pol = grid[['geometry', 'lat', 'lon', 'grid_id']].merge(
            emis_pixel_mean.rename('emis').reset_index(),
            on='grid_id', how='left'
        )

        # mapa base
        br_regiao.plot(
            ax=ax_map, color='#f2f2f2',
            edgecolor='#888888', linewidth=0.3, zorder=1
        )

        # escala log
        valores = grid_pol['emis'].dropna().values
        vmin = max(valores.min(), 1e-3)
        vmax = valores.max()
        norm = mcolors.LogNorm(vmin=vmin, vmax=vmax)

        # pixels
        grid_pol_plot = grid_pol.dropna(subset=['emis']).copy()
        grid_pol_plot.plot(
            ax=ax_map, column='emis',
            cmap=cmap, norm=norm,
            edgecolor='none', linewidth=0, zorder=2,
        )

        # contorno UFs
        br_estado.boundary.plot(
            ax=ax_map, color='#444444', linewidth=0.3, zorder=3
        )

        # limites BR continental
        continental = br_regiao.cx[:-34.5, :]
        bx_min, by_min, bx_max, by_max = continental.total_bounds
        ax_map.set_xlim(bx_min - 0.5, bx_max + 0.5)
        ax_map.set_ylim(by_min - 0.5, by_max + 0.5)
        ax_map.set_axis_off()

        # título
        ax_map.set_title(pol, fontsize=title_fs, fontweight='bold', pad=4)

        # barras por região (média anual)
        emis_reg_ano = (
            inv_gdf.groupby([col_regiao, col_ano])[pol]
            .sum()
            .reset_index()
        )
        emis_reg = (
            emis_reg_ano.groupby(col_regiao)[pol]
            .mean()
            .reindex(REGIAO_ORDER, fill_value=0)
        )

        ax_bar = ax_map.inset_axes(list(bar_inset))
        x_pos = np.arange(len(REGIAO_ORDER))
        cor_barra = cmap(0.55)

        bars = ax_bar.bar(
            x_pos, emis_reg.values,
            color=cor_barra, alpha=0.95,
            edgecolor='white', linewidth=0.3
        )
        ax_bar.set_xticks(x_pos)
        ax_bar.set_xticklabels(REGIAO_SIGLA, fontsize=bar_sigla_fs, fontweight='bold')
        ax_bar.tick_params(axis='y', left=False, labelleft=False)
        ax_bar.tick_params(axis='x', length=0)
        ax_bar.spines[['top', 'right', 'left', 'bottom']].set_visible(False)
        ax_bar.patch.set_alpha(0)

        ymax_bar = emis_reg.values.max() if emis_reg.values.max() > 0 else 1
        ax_bar.set_ylim(0, ymax_bar * 1.25)
        for bar, val in zip(bars, emis_reg.values):
            if val <= 0:
                continue
            txt = (f'{val/1e6:.1f}M' if val >= 1e6 else
                   f'{val/1e3:.0f}k' if val >= 1e3 else
                   f'{val:.0f}')
            ax_bar.text(
                bar.get_x() + bar.get_width()/2, bar.get_height(),
                txt, ha='center', va='bottom',
                fontsize=bar_label_fs, fontweight='bold', color='#222222'
            )

        # colorbar
        ax_cb = ax_map.inset_axes(list(cb_inset))
        cb = ColorbarBase(ax_cb, cmap=cmap, norm=norm, orientation='vertical')
        cb.set_label('(t/ano) [log]', fontsize=cb_label_fs, labelpad=2)
        cb.ax.tick_params(labelsize=cb_tick_fs, length=2, pad=1)
        cb.outline.set_linewidth(0.5)

    # ---------- 4. FIGURA SOLO (destaque) ----------
    out_paths = []
    if pol_destaque is not None and pol_destaque in pol_interest:
        fig_solo = plt.figure(figsize=figsize_solo, dpi=dpi, facecolor='white')
        ax_solo = fig_solo.add_subplot(1, 1, 1)
        _desenha_slot(
            ax_solo, pol_destaque,
            title_fs=22, bar_label_fs=11, bar_sigla_fs=12,
            cb_label_fs=12, cb_tick_fs=11,
            bar_inset=(0.005, 0.04, 0.30, 0.25),
            cb_inset=(0.87, 0.04, 0.04, 0.45),
        )
        path_solo = os.path.join(figures, nome_solo.format(pol=pol_destaque))
        plt.savefig(path_solo, dpi=dpi, bbox_inches='tight', facecolor='white')
        plt.show()
        plt.close(fig_solo)
        out_paths.append(path_solo)

    # ---------- 5. FIGURA MOSAICO (sem o destaque) ----------
    pol_mosaico = [p for p in pol_interest if p != pol_destaque]
    n_pol = len(pol_mosaico)
    n_rows = int(np.ceil(n_pol / n_cols))

    fig_mos = plt.figure(figsize=figsize_mosaico, dpi=dpi, facecolor='white')
    gs_out = gridspec.GridSpec(
        n_rows, n_cols, figure=fig_mos,
        wspace=0.02, hspace=0.05,
        left=0.02, right=0.99, top=0.97, bottom=0.03,
    )

    for idx, pol in enumerate(pol_mosaico):
        r, c = divmod(idx, n_cols)
        ax_map = fig_mos.add_subplot(gs_out[r, c])
        _desenha_slot(ax_map, pol)

    # desliga slots vazios
    n_slots = n_rows * n_cols
    for idx_empty in range(n_pol, n_slots):
        r, c = divmod(idx_empty, n_cols)
        ax_empty = fig_mos.add_subplot(gs_out[r, c])
        ax_empty.set_axis_off()

    path_mos = os.path.join(figures, nome_mosaico)
    plt.savefig(path_mos, dpi=dpi, bbox_inches='tight', facecolor='white')
    plt.show()
    plt.close(fig_mos)
    out_paths.append(path_mos)

    return out_paths

# -*- coding: utf-8 -*-
"""
Funções para a seção 3.4 — Sectoral Emission Patterns and Pollutant Distribution

  - plot_tabela_top3_setores_estado : Table Y  (3.4.1)
  - plot_heatmap_setor_poluente     : Figure Z (3.4.2)

Adicionar ao functions_pt.py
"""


# ── Configurações compartilhadas ────────────────────────────────────────────

# Agrupamento de setores em macrosetores legíveis
MACRO_MAP = {
    'Combustão externa - indústria':                        'Combustão Industrial',
    'Produção Pública de Eletricidade e Calor':             'Energia e Calor',
    'Produção de cal':                                      'Minerais Não-Metálicos',
    'Combustão na Indústria de Minerais Não-Metálicos':     'Minerais Não-Metálicos',
    'Produção de clínquer e cimento':                       'Minerais Não-Metálicos',
    'Produção de vidro':                                    'Minerais Não-Metálicos',
    'Produção de ferro e aço':                              'Metais',
    'Produção de alumínio':                                 'Metais',
    'Produção de ferroligas':                               'Metais',
    'Produção de chumbo':                                   'Metais',
    'Produção de zinco':                                    'Metais',
    'Produção de cobre':                                    'Metais',
    'Combustão na Indústria de Metais Não-Ferrosos':        'Metais',
    'Produção de Celulose e Papel':                         'Celulose e Papel',
    'Refino de petróleo':                                   'Química e Petroquímica',
}

# Cor por macrossetor (consistente entre as duas figuras)
MACRO_COLORS = {
    'Combustão Industrial':    '#c0392b',
    'Energia e Calor':         '#e67e22',
    'Minerais Não-Metálicos':  '#8e44ad',
    'Metais':                  '#2980b9',
    'Celulose e Papel':        '#27ae60',
    'Química e Petroquímica':  '#f39c12',
    'Outros':                  '#95a5a6',
}

# Poluentes primários associados a cada macrossetor (para a tabela)
MACRO_POLUENTES = {
    'Combustão Industrial':    'MP10, MP2.5, CO, NOx',
    'Energia e Calor':         'NOx, SOx, MP10',
    'Minerais Não-Metálicos':  'NOx, SOx, PTS',
    'Metais':                  'Pb, CO, MP10',
    'Celulose e Papel':        'SOx, MP10, CO',
    'Química e Petroquímica':  'SOx, PTS, NOx',
    'Outros':                  '—',
}

MAPA_REGIAO = {
    'AC':'Norte',  'AP':'Norte',  'AM':'Norte',  'PA':'Norte',
    'RO':'Norte',  'RR':'Norte',  'TO':'Norte',
    'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste',
    'PB':'Nordeste','PE':'Nordeste','PI':'Nordeste',
    'RN':'Nordeste','SE':'Nordeste',
    'DF':'Centro-oeste','GO':'Centro-oeste','MT':'Centro-oeste','MS':'Centro-oeste',
    'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
    'PR':'Sul','RS':'Sul','SC':'Sul',
}

REGIAO_ORDER = ['Norte', 'Nordeste', 'Centro-oeste', 'Sudeste', 'Sul']


def _adicionar_macro(df, col_setor='SETOR'):
    """Adiciona coluna MACRO ao dataframe."""
    df = df.copy()
    df['MACRO'] = df[col_setor].map(MACRO_MAP)
    mask_quimica = df[col_setor].str.startswith('Indústria Química', na=False)
    mask_veic    = df[col_setor].str.startswith('Indústria de veículos', na=False)
    df.loc[mask_quimica, 'MACRO'] = 'Química e Petroquímica'
    df.loc[mask_veic,    'MACRO'] = 'Outros'
    df['MACRO'] = df['MACRO'].fillna('Outros')
    return df


# ── Figura 1: Tabela top 3 setores por estado ───────────────────────────────

def plot_tabela_top3_setores_estado(
    inv,
    figures,
    pol_interest,
    col_uf='SIGLA_UF',
    col_setor='SETOR',
    col_ano='ANO',
    dpi=300,
    figsize=(14, 13),
    nome_arquivo='tabela_top3_setores_estado.png',
):
    """
    Table Y — Top 3 macrosetores emissores por estado,
    com barra de % de emissão total e poluentes associados.

    Layout: tabela visual com uma linha por estado,
    ordenada por região (N → NE → CO → SE → S).
    """

    df = _adicionar_macro(inv, col_setor)

    # converte poluentes
    for p in pol_interest:
        df[p] = pd.to_numeric(df[p], errors='coerce').fillna(0)

    df['EMIS_TOTAL'] = df[pol_interest].sum(axis=1)
    df['REGIAO']     = df[col_uf].map(MAPA_REGIAO)

    # emissão total por estado (média anual)
    emis_estado = (
        df.groupby([col_uf, col_ano])['EMIS_TOTAL'].sum()
          .groupby(col_uf).mean()
    )
    total_nacional = emis_estado.sum()

    # top 3 macrosetores por estado (média anual)
    emis_estado_macro = (
        df.groupby([col_uf, 'MACRO', col_ano])['EMIS_TOTAL']
          .sum()
          .groupby([col_uf, 'MACRO']).mean()
          .reset_index()
          .sort_values([col_uf, 'EMIS_TOTAL'], ascending=[True, False])
    )
    top3 = (
        emis_estado_macro
        .groupby(col_uf)
        .head(3)
        .groupby(col_uf)['MACRO']
        .apply(list)
        .reset_index()
        .rename(columns={'MACRO': 'top3'})
    )

    # % do estado no total nacional
    top3['pct_nac'] = top3[col_uf].map(emis_estado) / total_nacional * 100
    top3['REGIAO']  = top3[col_uf].map(MAPA_REGIAO)

    # ordena por região e depois por % emissão
    top3['REGIAO_ORD'] = top3['REGIAO'].map({r: i for i, r in enumerate(REGIAO_ORDER)})
    top3 = top3.sort_values(['REGIAO_ORD', 'pct_nac'], ascending=[True, False]).reset_index(drop=True)

    # ── Figura ──────────────────────────────────────────────────────────────
    n_estados = len(top3)
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi, facecolor='white')
    ax.set_axis_off()

    # cabeçalho
    cols_header = ['Estado', 'Região', '% Nacional', '1º Setor', '2º Setor', '3º Setor', 'Poluentes Assoc.']
    col_x       = [0.02, 0.10, 0.19, 0.30, 0.47, 0.64, 0.81]
    col_align   = ['left','left','center','left','left','left','left']

    y_top = 0.97
    row_h = (y_top - 0.02) / (n_estados + 1.5)

    # linha de cabeçalho
    for cx, align, label in zip(col_x, col_align, cols_header):
        ax.text(cx, y_top, label,
                ha=align, va='top', fontsize=9, fontweight='bold',
                transform=ax.transAxes, color='white',
                bbox=dict(boxstyle='round,pad=0.15', facecolor='#2c3e50', edgecolor='none'))

    # separador
    ax.plot([0.01, 0.99], [y_top - row_h * 0.6, y_top - row_h * 0.6],
            color='#2c3e50', linewidth=1.5, transform=ax.transAxes, clip_on=False)

    regiao_cores = {
        'Norte': '#5b8db8', 'Nordeste': '#c4a052',
        'Centro-oeste': '#a0785a', 'Sudeste': '#7a6e9e', 'Sul': '#5a9e82',
    }

    for i, row in top3.iterrows():
        y = y_top - row_h * (i + 1.5)
        bg = '#f8f9fa' if i % 2 == 0 else 'white'
        rect_bg = plt.Rectangle([0.01, y - row_h * 0.45], 0.98, row_h * 0.95,
                                 facecolor=bg, edgecolor='none',
                                 transform=ax.transAxes, zorder=0)
        ax.add_patch(rect_bg)

        top3_list = row['top3']

        # Estado
        ax.text(col_x[0], y, row[col_uf],
                ha='left', va='center', fontsize=9, fontweight='bold',
                transform=ax.transAxes)

        # Região (com cor)
        reg = row['REGIAO'] or ''
        ax.text(col_x[1], y, reg,
                ha='left', va='center', fontsize=8,
                color=regiao_cores.get(reg, '#333333'),
                fontweight='bold', transform=ax.transAxes)

        # % Nacional — barra horizontal mini
        pct = row['pct_nac']
        bar_w = 0.08
        bar_x = col_x[2] - 0.01
        ax.barh(y, pct / 100 * bar_w, height=row_h * 0.55,
                left=bar_x, color='#c0392b', alpha=0.7,
                transform=ax.transAxes, zorder=2)
        ax.text(bar_x + bar_w + 0.005, y,
                f'{pct:.1f}%', ha='left', va='center',
                fontsize=8, transform=ax.transAxes, color='#444444')

        # Top 1, 2, 3
        for rank, cx in enumerate([col_x[3], col_x[4], col_x[5]]):
            if rank < len(top3_list):
                setor = top3_list[rank]
                cor   = MACRO_COLORS.get(setor, '#888888')
                ax.text(cx, y, setor,
                        ha='left', va='center', fontsize=8,
                        transform=ax.transAxes,
                        bbox=dict(boxstyle='round,pad=0.2',
                                  facecolor=cor, edgecolor='none', alpha=0.85),
                        color='white', fontweight='bold')

        # Poluentes associados ao 1º setor
        pols_assoc = MACRO_POLUENTES.get(top3_list[0] if top3_list else '', '—')
        ax.text(col_x[6], y, pols_assoc,
                ha='left', va='center', fontsize=8,
                transform=ax.transAxes, color='#333333')

    # legenda macrosetores
    handles = [Patch(facecolor=c, label=s, edgecolor='none')
               for s, c in MACRO_COLORS.items() if s != 'Outros']
    fig.legend(handles=handles, ncols=3, fontsize=8, frameon=False,
               loc='lower center', bbox_to_anchor=(0.5, -0.02))

    fig.suptitle('Top 3 Macrosetores Emissores por Estado — Poluentes Associados',
                 fontsize=12, fontweight='bold', y=0.995)

    plt.savefig(os.path.join(figures, nome_arquivo),
                dpi=dpi, bbox_inches='tight', facecolor='white')
    plt.show()
    plt.close()


# ── Figura 2: Heatmap setor × poluente ──────────────────────────────────────

def plot_heatmap_setor_poluente(
    inv,
    figures,
    pol_interest,
    col_setor='SETOR',
    col_ano='ANO',
    dpi=300,
    figsize=(10, 5),
    nome_arquivo='heatmap_setor_poluente.png',
):
    """
    Figure Z — % da emissão nacional de cada poluente por macrossetor.
    Heatmap com anotação de valor em cada célula.
    Escala de cor independente por poluente (normalização por coluna)
    para não deixar Combustão Industrial ofuscar tudo.
    """

    df = _adicionar_macro(inv, col_setor)
    for p in pol_interest:
        df[p] = pd.to_numeric(df[p], errors='coerce').fillna(0)

    # % nacional por poluente: média anual → soma por macrossetor → normaliza
    emis_macro_ano = (
        df.groupby(['MACRO', col_ano])[pol_interest]
          .sum()
          .groupby('MACRO').mean()        # média entre anos
    )
    total_nac = emis_macro_ano.sum()      # total por poluente
    pct = (emis_macro_ano / total_nac * 100).round(1)

    # ordena macrosetores por emissão total (soma dos poluentes)
    pct['_total'] = pct.sum(axis=1)
    pct = pct.sort_values('_total', ascending=False).drop(columns='_total')
    pct = pct[pol_interest]               # garante ordem das colunas

    macro_order = pct.index.tolist()
    n_macro = len(macro_order)
    n_pol   = len(pol_interest)

    # normalização por coluna (cada poluente tem seu próprio range de cor)
    pct_norm = pct.copy()
    for col in pct.columns:
        col_max = pct[col].max()
        pct_norm[col] = pct[col] / col_max if col_max > 0 else 0

    # ── Figura ──────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi, facecolor='white')

    cmap = plt.get_cmap('YlOrRd')

    for j, pol in enumerate(pol_interest):
        for i, macro in enumerate(macro_order):
            val      = pct.loc[macro, pol]
            val_norm = pct_norm.loc[macro, pol]
            cor      = cmap(val_norm)

            rect = plt.Rectangle([j - 0.5, i - 0.5], 1, 1,
                                  facecolor=cor, edgecolor='white', linewidth=1.2)
            ax.add_patch(rect)

            # texto: branco se fundo escuro, preto se claro
            txt_color = 'white' if val_norm > 0.55 else '#333333'
            txt = f'{val:.1f}%' if val >= 0.1 else '—'
            ax.text(j, i, txt,
                    ha='center', va='center',
                    fontsize=9, fontweight='bold', color=txt_color)

    # etiqueta colorida na esquerda (nome do macrossetor)
    for i, macro in enumerate(macro_order):
        cor_macro = MACRO_COLORS.get(macro, '#888888')
        ax.text(-0.55, i, macro,
                ha='right', va='center', fontsize=9, fontweight='bold',
                color='white',
                bbox=dict(boxstyle='round,pad=0.3',
                          facecolor=cor_macro, edgecolor='none', alpha=0.9),
                transform=ax.transData)

    ax.set_xlim(-0.5, n_pol - 0.5)
    ax.set_ylim(-0.5, n_macro - 0.5)
    ax.set_xticks(range(n_pol))
    ax.set_xticklabels(pol_interest, fontsize=10, fontweight='bold')
    ax.xaxis.set_ticks_position('top')
    ax.xaxis.set_label_position('top')
    ax.set_yticks([])
    ax.tick_params(length=0)

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.set_title(
        'Distribuição setorial das emissões nacionais por poluente (%)',
        fontsize=11, fontweight='bold', pad=28
    )

    # nota rodapé
    fig.text(0.5, -0.02,
             '% calculada sobre a emissão nacional média anual (2017–2023). '
             'Escala de cor normalizada por poluente.',
             ha='center', fontsize=8, color='#666666', style='italic')

    plt.tight_layout()
    plt.savefig(os.path.join(figures, nome_arquivo),
                dpi=dpi, bbox_inches='tight', facecolor='white')
    plt.show()
    plt.close()
