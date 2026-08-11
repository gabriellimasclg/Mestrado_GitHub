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

def plot_mosaico_pixels_poluentes(
    inv_gdf,
    br_estado,
    br_regiao,
    pol_interest,
    figures,          # poluente que vira figura solo grandona
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

    REGIAO_ORDER = ['N', 'NE', 'MW', 'SE', 'S']   # como está no inv_gdf
    REGIAO_SIGLA = ['N', 'NE', 'MW', 'SE', 'S']    # rótulo exibido


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
        ax_bar.set_xticklabels(REGIAO_SIGLA, fontsize=bar_sigla_fs, fontweight='bold',rotation=90)
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

    # ---------- 5. FIGURA MOSAICO (sem o destaque) ----------
    pol_mosaico = [p for p in pol_interest]
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