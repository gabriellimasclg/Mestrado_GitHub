# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 16:19:02 2026

@author: glima
"""

import xarray as xr
import numpy as np
import pandas as pd
import pyproj
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.gridspec as gridspec
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import os
from shapely.geometry import box
import geopandas as gpd


plt.rcParams["font.family"] = "Arial"

EMISSION_SOURCE_COLORS = {
    "emission_ind": "#1f77b4",                    # azul
    "emission_braves_classic": "#d62728",         # vermelho
    "emission_braves_ressuspension": "#7f7f7f",   # cinza
    "emission_braves_refuel": "#f2c300",          # amarelo
    "emission_resid": "#2ca02c",                  # verde
    "emission_finn": "#ff7f0e",                   # laranja
    "emission_windblow": "#8c564b",               # marrom
    "Outros": "#17becf",                          # ciano
}

#%% ── helpers ────────────────────────────────────────────────────────────────

def ioapiCoords(ds):
    # Latlon
    lonI = ds.XORIG
    latI = ds.YORIG
    
    # Cell spacing 
    xcell = ds.XCELL
    ycell = ds.YCELL
    ncols = ds.NCOLS
    nrows = ds.NROWS
    
    lon = np.arange(lonI,(lonI+ncols*xcell),xcell)
    lat = np.arange(latI,(latI+nrows*ycell),ycell)
    
    xv, yv = np.meshgrid(lon,lat)
    return xv,yv,lon,lat

def eqmerc2latlon(ds,xv,yv):

    mapstr = '+proj=merc +a=%s +b=%s +lat_ts=0 +lon_0=%s +units=m +no_defs' % (
              6370000, 6370000, ds.XCENT)
    #p = pyproj.Proj("+proj=merc +lon_0="+str(ds.P_GAM)+" +k=1 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs")
    p = pyproj.Proj(mapstr)
    xlon, ylat = p(xv, yv, inverse=True)
    

    return xlon,ylat

def add_brazil_inverse_mask(ax, brazil, xlon, ylat, pad=1.0, facecolor="white"):
    """
    Plota uma máscara branca fora do Brasil para esconder pixels externos.

    Parameters
    ----------
    ax : matplotlib axis
        Eixo onde o mapa já foi desenhado.
    brazil : GeoDataFrame
        Shapefile do Brasil em EPSG:4326.
    xlon, ylat : array 2D
        Coordenadas lon/lat do grid.
    pad : float
        Margem adicional ao redor da extensão do grid.
    facecolor : str
        Cor da máscara externa.
    """
    # união do Brasil inteiro
    brazil_union = brazil.unary_union

    # extensão total do grid
    xmin = float(np.nanmin(xlon)) - pad
    xmax = float(np.nanmax(xlon)) + pad
    ymin = float(np.nanmin(ylat)) - pad
    ymax = float(np.nanmax(ylat)) + pad

    outer = box(xmin, ymin, xmax, ymax)
    inverse_mask = outer.difference(brazil_union)

    mask_gdf = gpd.GeoDataFrame(geometry=[inverse_mask], crs="EPSG:4326")
    mask_gdf.plot(
        ax=ax,
        color=facecolor,
        edgecolor="none",
        zorder=8
    )

#%% ── helpers para espécies/poluentes ───────────────────────────────────────

def squeeze_var_dim(da):
    if "VAR" in da.dims and da.sizes["VAR"] == 1:
        da = da.squeeze("VAR", drop=True)
    return da

def get_available_species(ds):
    return set(ds.data_vars)


def build_pollutant(ds, pol_name, pollutant_specs, verbose=True):
    """
    Monta um poluente a partir das espécies disponíveis no dataset.
    
    Regras:
    - CO, NO2, SO2: usa diretamente a variável.
    - MP10, MP25: soma apenas as espécies disponíveis.
    - Se nenhuma espécie existir, retorna None.
    """
    if pol_name not in pollutant_specs:
        raise ValueError(f"Poluente '{pol_name}' não está em POLLUTANT_SPECS.")
    
    requested = pollutant_specs[pol_name]
    available = get_available_species(ds)
    
    present = [v for v in requested if v in available]
    missing = [v for v in requested if v not in available]
    
    if verbose:
        print(f"\n--- {pol_name} ---")
        print(f"Espécies pedidas: {len(requested)}")
        print(f"Espécies encontradas: {len(present)}")
        if missing:
            print("Espécies ausentes:")
            print(", ".join(missing))
    
    if len(present) == 0:
        if verbose:
            print(f"{pol_name}: nenhuma espécie disponível. Pulando.")
        return None, present, missing
    
    # soma as espécies disponíveis
    da = squeeze_var_dim(ds[present[0]])
    for var in present[1:]:
        da = da + squeeze_var_dim(ds[var])
    
    da.name = pol_name
    return da, present, missing

#%% ── função: mosaico espacial pixelado ──────────────────────────────────────

def plot_spatial_mosaic(da, pol_name, unit, xlon, ylat, brazil, dims_time, figpath):
    """
    Plota:
    Camada 1, 2, 3, 4, 39, 40
    """
    l1 = da.isel(LAY=0).sum(dim=dims_time).compute()
    l2 = da.isel(LAY=1).sum(dim=dims_time).compute()
    l3 = da.isel(LAY=2).sum(dim=dims_time).compute()
    l4 = da.isel(LAY=3).sum(dim=dims_time).compute()
    l39 = da.isel(LAY=38).sum(dim=dims_time).compute()
    l40 = da.isel(LAY=39).sum(dim=dims_time).compute()
    #lall = da.sum(dim="LAY").sum(dim=dims_time).compute()

    maps = [l1, l2, l3, l4, l39, l40]

    eps = 1e-12
    positive_mins = []
    for m in maps:
        mpos = m.where(m > 0)
        try:
            val = float(mpos.min())
            if np.isfinite(val):
                positive_mins.append(val)
        except Exception:
            pass

    vmin = max(min(positive_mins), eps) if positive_mins else eps
    vmax = max(float(m.max()) for m in maps)

    cmap = plt.colormaps["Spectral_r"].copy()
    norm = colors.LogNorm(vmin=vmin, vmax=vmax)

    fig = plt.figure(figsize=(12, 8))
    gs = gridspec.GridSpec(
        nrows=2,
        ncols=3,
        figure=fig,
        wspace=-0.2,
        hspace=0.15
    )

    axes = [
        fig.add_subplot(gs[0, 0]),
        fig.add_subplot(gs[0, 1]),
        fig.add_subplot(gs[0, 2]),
        fig.add_subplot(gs[1, 0]),
        fig.add_subplot(gs[1, 1]),
        fig.add_subplot(gs[1, 2]),
    ]

    titles = ["Camada 1", "Camada 2", "Camada 3", "Camada 4", "Camada 39", "Camada 40"]

    for ax, data, title in zip(axes, maps, titles):
        m = ax.pcolormesh(
            xlon, ylat, data,
            cmap=cmap,
            norm=norm,
            shading="auto"
        )
        
        add_brazil_inverse_mask(ax=ax, brazil=brazil, xlon=xlon, ylat=ylat, pad=1.0)
        
        brazil.boundary.plot(
            ax=ax,
            color="black",
            linewidth=0.8,
            zorder=10
        )

        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_xlabel("")
        ax.set_ylabel("")

        for spine in ax.spines.values():
            spine.set_visible(False)

        ax.set_title(title, fontsize=12)

    cbar = fig.colorbar(
        m,
        ax=axes,
        orientation="horizontal",
        fraction=0.03,
        pad=0.06
    )
    cbar.set_label(f"{pol_name} acumulado no tempo ({unit}) [log]")

    fig.suptitle(
        f"Distribuição espacial das emissões - {pol_name}",
        fontsize=16,
        fontweight="bold"
    )
    
    plt.savefig(os.path.join(figpath,f'mosaico_emissoes_{pol_name}.png'),dpi=300)
    plt.show()
    
    
#%% ── função: cálculo regional por camada ────────────────────────────────────

def calculate_by_region_lay(da, xlon, ylat, brazil):
    import regionmask

    brazil_reg = brazil.dissolve(by="NM_REGIA", as_index=False)

    da_lay_map = da.sum(dim="TSTEP").compute()

    labels = brazil_reg["NM_REGIA"].astype(str).values

    regions = regionmask.Regions(
        outlines=list(brazil_reg.geometry),
        names=list(labels),
        abbrevs=list(labels),
    )

    mask = regions.mask(
        xr.DataArray(xlon, dims=("ROW", "COL")),
        xr.DataArray(ylat, dims=("ROW", "COL")),
    ).rename("region_id")

    gb = da_lay_map.groupby(mask)

    by_region_lay = (
        gb.sum(dim="stacked_ROW_COL")
          .rename({"region_id": "region"})
          .assign_coords(region=("region", regions.names))
    )

    return by_region_lay

#%% ── função: mosaico regional + perfil vertical ─────────────────────────────

def plot_regional_vertical_profile(by_region_lay, pol_name, unit, brazil, figpath):
    co_sel = by_region_lay.isel(LAY=slice(0, 40))

    vals = co_sel.transpose("LAY", "region").values
    tot  = vals.sum(axis=1)
    pct  = (vals / tot[:, None]) * 100
    layer_pct_total = (tot / tot.sum()) * 100

    lay = np.arange(1, vals.shape[0] + 1)
    region_names = co_sel["region"].values.tolist()

    region_total = by_region_lay.sum(dim="LAY")
    region_pct_total = (region_total / region_total.sum()) * 100
    pct_total_map = dict(zip(region_total["region"].values.tolist(),
                             region_pct_total.values))

    palette = plt.cm.Set2(np.linspace(0, 1, len(region_names)))
    color_map = dict(zip(region_names, palette))

    brazil_reg = brazil.dissolve(by="NM_REGIA", as_index=False)
    brazil_reg["NM_REGIA"] = brazil_reg["NM_REGIA"].astype(str)
    brazil_reg = brazil_reg[brazil_reg["NM_REGIA"].isin(region_names)].copy()
    brazil_reg["color"] = brazil_reg["NM_REGIA"].map(color_map)

    fig = plt.figure(figsize=(11, 7), constrained_layout=True)
    gs = gridspec.GridSpec(1, 2, figure=fig, width_ratios=[1.5, 2.1])

    ax  = fig.add_subplot(gs[0, 1])
    axm = fig.add_subplot(gs[0, 0])

    fig.suptitle(
        f"Análise das emissões\nPoluente {pol_name}",
        fontsize=20,
        fontweight="bold",
        x=0.01,
        y=0.97,
        ha="left"
    )

    ax.set_facecolor("gainsboro")

    left = np.zeros(pct.shape[0])
    for j, reg in enumerate(region_names):
        widths = pct[:, j]
    
        ax.barh(
            lay,
            widths,
            left=left,
            color=color_map[reg],
            height=0.85
        )
    
        for i, w in enumerate(widths):
            if np.isfinite(w) and w >= 10:
                ax.text(
                    left[i] + w / 2,
                    lay[i],
                    f"{w:.0f}%",
                    ha="center",
                    va="center",
                    fontsize=8,
                    color="black",
                    weight="bold"
                )
    
        left += widths

    ax.set_xlim(0, 100)
    ax.set_xlabel("Proporção das emissões por região (%)")
    ax.set_ylabel("Camada da atmosfera")
    ax.grid(True, axis="x", linestyle="--", alpha=0.4)

    ax.set_ylim(0.5, lay[-1] + 0.5)
    ax.set_yticks(np.arange(1, lay[-1] + 1, 1))

    ax2 = ax.twiny()
    line_handle, = ax2.plot(
        tot, lay,
        color="crimson",
        linewidth=3,
        marker="o",
        markersize=5,
        zorder=10
    )

    leg = ax2.legend(
        handles=[line_handle],
        labels=[f"Total de emissões por camada ({unit})"],
        loc="center",
        bbox_to_anchor=(0.76, 1.07),
        frameon=False
    )

    for text in leg.get_texts():
        text.set_color("crimson")

    ax2.tick_params(axis="x", colors="crimson")
    ax2.spines["top"].set_color("crimson")

    brazil_reg.plot(ax=axm, color=brazil_reg["color"], edgecolor="gray", linewidth=0.5)
    axm.set_title("Macro-regiões\n% das emissões totais", x=0.5, y=-0.12)
    axm.set_axis_off()

    for _, row in brazil_reg.iterrows():
        name = row["NM_REGIA"]
        p = row.geometry.representative_point()
        perc = pct_total_map.get(name, np.nan)
        axm.text(
            p.x, p.y,
            f"{name.upper()}\n{perc:.1f}%",
            ha="center", va="center", fontsize=9, weight="bold"
        )

    ax_left = ax.secondary_yaxis("left")
    ax_left.set_yticks(lay)
    ax_left.set_yticklabels([f"{p:.2f}%" for p in layer_pct_total])
    ax.spines["left"].set_position(("outward", 45))
    ax_left.tick_params(axis="y", colors="crimson")

    plt.savefig(os.path.join(figpath,f'emissoes_por_camada_{pol_name}.png'),dpi=300)
    plt.show()
    
 #%% ── helpers temporais IOAPI/CMAQ ───────────────────────────────────────────

def get_ioapi_datetimes(ds):
    """
    Converte TFLAG do padrão IOAPI/CMAQ (<YYYYDDD, HHMMSS>) em DatetimeIndex.
    Usa apenas VAR=0, pois o TFLAG costuma repetir o mesmo tempo para todas as variáveis.
    """
    if "TFLAG" not in ds:
        raise ValueError("O dataset não possui a variável 'TFLAG'.")

    tflag = ds["TFLAG"].isel(VAR=0).compute().values  # shape: (TSTEP, 2)

    dates = tflag[:, 0].astype(int)   # YYYYDDD
    times = tflag[:, 1].astype(int)   # HHMMSS

    datetimes = []
    for yyyyddd, hhmmss in zip(dates, times):
        yyyyddd = f"{yyyyddd:07d}"
        hhmmss = f"{hhmmss:06d}"

        base_date = pd.to_datetime(yyyyddd, format="%Y%j")
        hour = int(hhmmss[0:2])
        minute = int(hhmmss[2:4])
        second = int(hhmmss[4:6])

        dt = base_date + pd.Timedelta(hours=hour, minutes=minute, seconds=second)
        datetimes.append(dt)

    return pd.DatetimeIndex(datetimes)


def build_domain_time_series(da):
    """
    Agrega o poluente no domínio para cada instante de tempo.
    Resultado: série temporal 1D em TSTEP.
    """
    dims_to_sum = [d for d in da.dims if d != "TSTEP"]
    ts = da.sum(dim=dims_to_sum).compute()
    return ts


def summarize_temporal_patterns(ts, datetimes):
    """
    Recebe uma série temporal 1D (TSTEP) e um DatetimeIndex com o mesmo tamanho.
    Retorna estatísticas por:
    - hora do dia
    - dia da semana
    - mês do ano
    """
    values = np.asarray(ts.values).astype(float)

    if len(values) != len(datetimes):
        raise ValueError("O tamanho da série temporal e do vetor de datas não coincide.")

    s = pd.Series(values, index=datetimes)

    # hora do dia
    hour_stats = s.groupby(s.index.hour).agg(["mean", "min", "max"])
    hour_stats = hour_stats.reindex(range(24))

    # dia da semana (0=segunda, 6=domingo)
    weekday_stats = s.groupby(s.index.dayofweek).agg(["mean", "min", "max"])
    weekday_stats = weekday_stats.reindex(range(7))

    # mês do ano (1-12)
    month_stats = s.groupby(s.index.month).agg(["mean", "min", "max"])
    month_stats = month_stats.reindex(range(1, 13))

    return hour_stats, weekday_stats, month_stats


def _plot_time_band(ax, stats_df, x_values, x_labels, title, color="crimson", show_legend=False):
    """
    Plota linha da média com faixa sombreada entre mínimo e máximo.
    """
    mean_vals = stats_df["mean"].values.astype(float)
    min_vals = stats_df["min"].values.astype(float)
    max_vals = stats_df["max"].values.astype(float)

    ax.fill_between(
        x_values, min_vals, max_vals,
        color=color, alpha=0.18, linewidth=0
    )

    ax.plot(
        x_values, mean_vals,
        color=color, linewidth=2.5, marker="o", markersize=4
    )

    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xticks(x_values)
    ax.set_xticklabels(x_labels)
    ax.grid(True, axis="y", linestyle="--", alpha=0.35)
    ax.set_facecolor("white")

    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    if show_legend:
        handles = [
            Line2D([0], [0], color=color, lw=2.5, marker="o", markersize=4, label="Média"),
            Patch(facecolor=color, alpha=0.18, edgecolor="none", label="Intervalo min–max"),
        ]
        ax.legend(
            handles=handles,
            frameon=False,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.05),
            ncol=2
        )
        
#%% ── função: mosaico temporal ───────────────────────────────────────────────

def plot_temporal_mosaic(da, ds, pol_name, unit, xlon, ylat, brazil, figpath, source_name=None):
    """
    Painel com:
    - mapa acumulado no tempo e nas camadas
    - média por hora do dia com faixa min-max
    - média por dia da semana com faixa min-max
    - média por mês do ano com faixa min-max
    """

    # --- 1) mapa acumulado total (tempo + camadas)
    map_dims = [d for d in da.dims if d in ["TSTEP", "LAY"]]
    da_map = da.sum(dim=map_dims).compute()

    positive = da_map.where(da_map > 0)
    try:
        vmin = float(positive.min())
        if not np.isfinite(vmin) or vmin <= 0:
            vmin = 1e-12
    except Exception:
        vmin = 1e-12

    vmax = float(da_map.max())
    if not np.isfinite(vmax) or vmax <= 0:
        vmax = 1.0

    cmap = plt.colormaps["Spectral_r"].copy()
    norm = colors.LogNorm(vmin=vmin, vmax=vmax)

    # --- 2) série temporal do domínio
    datetimes = get_ioapi_datetimes(ds)
    ts = build_domain_time_series(da)

    hour_stats, weekday_stats, month_stats = summarize_temporal_patterns(ts, datetimes)

    # rótulos
    weekday_labels = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    month_labels = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                    "Jul", "Ago", "Set", "Out", "Nov", "Dez"]

    # --- 3) figura
    fig = plt.figure(figsize=(14, 8))
    gs = gridspec.GridSpec(
        nrows=3,
        ncols=2,
        figure=fig,
        width_ratios=[1.5, 1.5],
        height_ratios=[0.6, 0.6, 0.6],
        wspace=0.18,
        hspace=0.38
    )

    ax_map = fig.add_subplot(gs[:, 0])
    ax_map.set_aspect("equal")
    ax_h   = fig.add_subplot(gs[0, 1])
    ax_w   = fig.add_subplot(gs[1, 1])
    ax_m   = fig.add_subplot(gs[2, 1])

    # mapa
    m = ax_map.pcolormesh(
        xlon, ylat, da_map,
        cmap=cmap,
        norm=norm,
        shading="auto"
    )
    
    add_brazil_inverse_mask(ax=ax_map, brazil=brazil, xlon=xlon, ylat=ylat, pad=1.0)
    
    brazil.boundary.plot(
        ax=ax_map,
        color="black",
        linewidth=0.8,
        zorder=10
    )

    ax_map.set_xticks([])
    ax_map.set_yticks([])
    ax_map.set_xlabel("")
    ax_map.set_ylabel("")
    ax_map.set_title(
        "Total acumulado no tempo\ne camadas da atmosfera",
        fontsize=14,
        fontweight="bold"
    )

    for spine in ax_map.spines.values():
        spine.set_visible(False)

    cbar = fig.colorbar(
        m,
        ax=ax_map,
        orientation="horizontal",
        fraction=0.05,
        pad=0
    )
    cbar.set_label(f"{pol_name} acumulado no tempo e nas camadas ({unit}) [log]")
    source_color = EMISSION_SOURCE_COLORS.get(source_name, EMISSION_SOURCE_COLORS["Outros"])
    
    # gráficos temporais
    _plot_time_band(
        ax=ax_h,
        stats_df=hour_stats,
        x_values=np.arange(24),
        x_labels=[str(h) for h in range(24)],
        title="Variação das horas do dia",
        color=source_color,
        show_legend=True
    )
    
    ax_h.set_ylabel(f"Emissões totais ({unit})")

    _plot_time_band(
        ax=ax_w,
        stats_df=weekday_stats,
        x_values=np.arange(7),
        x_labels=weekday_labels,
        title="Dias da semana",
        color=source_color,
        show_legend=True
    )
    ax_w.set_ylabel(f"Emissões totais ({unit})")

    _plot_time_band(
        ax=ax_m,
        stats_df=month_stats,
        x_values=np.arange(1, 13),
        x_labels=month_labels,
        title="Meses do ano",
        color=source_color,
        show_legend=True
    )
    ax_m.set_ylabel(f"Emissões totais ({unit})")

    fig.suptitle(
        f"Análise temporal das emissões - {pol_name}",
        fontsize=18,
        fontweight="bold",
        y=0.98
    )
    
    ax_map.set_anchor("C")
    
    plt.savefig(
        os.path.join(figpath, f"temporal_emissoes_{pol_name}.png"),
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()   
    
#%% ── função: mosaico espacial anual + série histórica ──────────────────────

def plot_annual_spatial_mosaic(da, ds, pol_name, unit, xlon, ylat, brazil, figpath, source_name=None):
    """
    Figura com:
    - mosaico espacial por ano (3 colunas fixas)
    - número de linhas adaptativo
    - série histórica anual na última linha, ocupando toda a largura
    """

    # --- tempo -> anos
    datetimes = get_ioapi_datetimes(ds)
    years = np.array(datetimes.year)
    unique_years = np.sort(np.unique(years))

    if unique_years.size == 0:
        raise ValueError("Nenhum ano encontrado no TFLAG.")

    # --- mapas por ano
    yearly_maps = []
    yearly_totals = []
    
    for year in unique_years:
        idx = np.where(years == year)[0]

        da_year = da.isel(TSTEP=idx)

        # mapa espacial do ano: soma no tempo do ano + soma nas camadas
        dims_map = [d for d in da_year.dims if d in ["TSTEP", "LAY"]]
        da_map = da_year.sum(dim=dims_map).compute()
        yearly_maps.append((year, da_map))

        # total anual do domínio: soma tempo + camadas + espaço
        dims_total = [d for d in da_year.dims if d in ["TSTEP", "LAY", "ROW", "COL"]]
        total_val = float(da_year.sum(dim=dims_total).compute())
        yearly_totals.append(total_val)

    # --- escala comum para todos os mapas
    eps = 1e-12
    positive_mins = []
    vmax_candidates = []

    for _, da_map in yearly_maps:
        mpos = da_map.where(da_map > 0)
        try:
            v = float(mpos.min())
            if np.isfinite(v) and v > 0:
                positive_mins.append(v)
        except Exception:
            pass

        try:
            vmax_candidates.append(float(da_map.max()))
        except Exception:
            pass

    vmin = max(min(positive_mins), eps) if positive_mins else eps
    vmax = max(vmax_candidates) if vmax_candidates else 1.0
    if not np.isfinite(vmax) or vmax <= 0:
        vmax = 1.0

    cmap = plt.colormaps["Spectral_r"].copy()
    norm = colors.LogNorm(vmin=vmin, vmax=vmax)

    # --- layout adaptativo
    n_years = len(unique_years)
    ncols = 3
    nrows_maps = int(np.ceil(n_years / ncols))

    fig_height = 3.6 * nrows_maps + 3.2
    fig = plt.figure(figsize=(14, fig_height))

    gs = gridspec.GridSpec(
        nrows=nrows_maps + 1,
        ncols=ncols,
        figure=fig,
        height_ratios=[1] * nrows_maps + [0.9],
        hspace=0.25,
        wspace=0.06
    )

    # --- subplots dos mapas
    map_axes = []
    for i in range(nrows_maps * ncols):
        r = i // ncols
        c = i % ncols
        ax = fig.add_subplot(gs[r, c])
        map_axes.append(ax)

    mappable = None
    for ax, (year, da_map) in zip(map_axes, yearly_maps):
        mappable = ax.pcolormesh(
            xlon, ylat, da_map,
            cmap=cmap,
            norm=norm,
            shading="auto"
        )
        
        add_brazil_inverse_mask(ax=ax, brazil=brazil, xlon=xlon, ylat=ylat, pad=1.0)
        
        brazil.boundary.plot(
            ax=ax,
            color="black",
            linewidth=0.8,
            zorder=10
        )

        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_xlabel("")
        ax.set_ylabel("")
        ax.set_title(str(year), fontsize=13, fontweight="bold")
        ax.set_aspect("equal")
        ax.set_anchor("C")

        for spine in ax.spines.values():
            spine.set_visible(False)

    # esconder eixos vazios
    for ax in map_axes[n_years:]:
        ax.axis("off")

    # --- série histórica anual
    ax_ts = fig.add_subplot(gs[nrows_maps, :])

    source_color = EMISSION_SOURCE_COLORS.get(source_name, EMISSION_SOURCE_COLORS["Outros"])
    
    ax_ts.plot(
        unique_years,
        yearly_totals,
        color=source_color,
        linewidth=2.8,
        marker="o",
        markersize=6
    )
    
    ax_ts.fill_between(
        unique_years,
        yearly_totals,
        np.zeros_like(yearly_totals, dtype=float),
        color=source_color,
        alpha=0.10
    )
    
    for x, y in zip(unique_years, yearly_totals):
        ax_ts.scatter(x, y, color=source_color, s=35, zorder=5)
        ax_ts.text(
            x, y,
            f" {int(x)}",
            fontsize=9,
            va="bottom",
            ha="left"
        )

    ax_ts.set_title("Série histórica anual", fontsize=13, fontweight="bold")
    ax_ts.set_xlabel("Ano")
    ax_ts.set_ylabel(f"Emissões totais ({unit})")
    ax_ts.grid(True, axis="y", linestyle="--", alpha=0.35)
    ax_ts.set_facecolor("white")

    # ticks inteiros nos anos
    ax_ts.set_xticks(unique_years)
    ax_ts.set_xticklabels([str(y) for y in unique_years])

    for spine in ["top", "right"]:
        ax_ts.spines[spine].set_visible(False)

    # --- colorbar única para os mapas
    cbar = fig.colorbar(
        mappable,
        ax=map_axes[:n_years],
        orientation="horizontal",
        fraction=0.03,
        pad=0.04
    )
    cbar.set_label(f"{pol_name} acumulado no tempo ({unit}) [log]")

    fig.suptitle(
        f"Distribuição espacial anual das emissões - {pol_name}",
        fontsize=18,
        fontweight="bold",
        y=0.98
    )

    plt.savefig(
        os.path.join(figpath, f"mosaico_anual_emissoes_{pol_name}.png"),
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()
    
def plot_regional_total_map(da, pol_name, unit, xlon, ylat, brazil, figpath):
    """
    Plota apenas o mapa das macro-regiões com a porcentagem das emissões totais.
    Indicado para inventários com uma única camada (LAY = 1).
    """
    import regionmask

    # soma no tempo e na(s) camada(s), preservando o espaço
    dims_sum = [d for d in da.dims if d in ["TSTEP", "LAY"]]
    da_map = da.sum(dim=dims_sum).compute()

    brazil_reg = brazil.dissolve(by="NM_REGIA", as_index=False)
    labels = brazil_reg["NM_REGIA"].astype(str).values

    regions = regionmask.Regions(
        outlines=list(brazil_reg.geometry),
        names=list(labels),
        abbrevs=list(labels),
    )

    mask = regions.mask(
        xr.DataArray(xlon, dims=("ROW", "COL")),
        xr.DataArray(ylat, dims=("ROW", "COL")),
    ).rename("region_id")

    gb = da_map.groupby(mask)
    by_region = gb.sum(dim="stacked_ROW_COL").rename({"region_id": "region"})
    by_region = by_region.assign_coords(region=("region", regions.names))

    region_vals = by_region.values.astype(float)
    total = region_vals.sum()

    if total > 0:
        region_pct = (region_vals / total) * 100
    else:
        region_pct = np.zeros_like(region_vals)

    pct_map = dict(zip(by_region["region"].values.tolist(), region_pct))

    palette = plt.cm.Set2(np.linspace(0, 1, len(labels)))
    color_map = dict(zip(labels, palette))

    brazil_reg["NM_REGIA"] = brazil_reg["NM_REGIA"].astype(str)
    brazil_reg["color"] = brazil_reg["NM_REGIA"].map(color_map)

    fig, ax = plt.subplots(figsize=(8, 7))

    brazil_reg.plot(
        ax=ax,
        color=brazil_reg["color"],
        edgecolor="gray",
        linewidth=0.7
    )

    ax.set_title(
        f"Macro-regiões - participação nas emissões totais\n{pol_name}",
        fontsize=15,
        fontweight="bold"
    )
    ax.set_axis_off()

    for _, row in brazil_reg.iterrows():
        name = row["NM_REGIA"]
        p = row.geometry.representative_point()
        perc = pct_map.get(name, np.nan)

        ax.text(
            p.x, p.y,
            f"{name.upper()}\n{perc:.1f}%",
            ha="center", va="center",
            fontsize=10, weight="bold"
        )

    plt.savefig(
        os.path.join(figpath, f"mapa_regional_total_{pol_name}.png"),
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()
  
def plot_source_comparison_mosaic(source_maps, pol_name, unit, brazil, figpath, source_labels=None):
    """
    Cria um mosaico comparando diferentes fontes de emissão para um mesmo poluente.
    """
    if not source_maps:
        print(f"Nenhuma fonte disponível para o poluente {pol_name}.")
        return

    if source_labels is None:
        source_labels = {}

    source_names = list(source_maps.keys())
    nsrc = len(source_names)

    ncols = 3
    nrows = int(np.ceil(nsrc / ncols))

    # escala global entre fontes
    eps = 1e-12
    positive_mins = []
    vmax_candidates = []

    for src in source_names:
        da_map = source_maps[src]["data"]
        mpos = da_map.where(da_map > 0)

        try:
            v = float(mpos.min())
            if np.isfinite(v) and v > 0:
                positive_mins.append(v)
        except Exception:
            pass

        try:
            vmax_candidates.append(float(da_map.max()))
        except Exception:
            pass

    vmin = max(min(positive_mins), eps) if positive_mins else eps
    vmax = max(vmax_candidates) if vmax_candidates else 1.0

    if not np.isfinite(vmax) or vmax <= 0:
        vmax = 1.0

    cmap = plt.colormaps["Spectral_r"].copy()
    norm = colors.LogNorm(vmin=vmin, vmax=vmax)

    fig_height = 3.8 * nrows + 1.2
    fig = plt.figure(figsize=(14, fig_height))
    gs = gridspec.GridSpec(
        nrows=nrows,
        ncols=ncols,
        figure=fig,
        wspace=0.05,
        hspace=0.18
    )

    axes = []
    for i in range(nrows * ncols):
        r = i // ncols
        c = i % ncols
        axes.append(fig.add_subplot(gs[r, c]))

    mappable = None

    for ax, src in zip(axes, source_names):
        da_map = source_maps[src]["data"]
        xlon = source_maps[src]["xlon"]
        ylat = source_maps[src]["ylat"]
        label_name = source_labels.get(src, src)

        mappable = ax.pcolormesh(
            xlon, ylat, da_map,
            cmap=cmap,
            norm=norm,
            shading="auto"
        )

        add_brazil_inverse_mask(ax=ax, brazil=brazil, xlon=xlon, ylat=ylat, pad=1.0)

        brazil.boundary.plot(
            ax=ax,
            color="black",
            linewidth=0.8,
            zorder=10
        )

        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_xlabel("")
        ax.set_ylabel("")
        ax.set_aspect("equal")
        ax.set_anchor("C")
        ax.set_title(label_name, fontsize=12, fontweight="bold")

        for spine in ax.spines.values():
            spine.set_visible(False)

    for ax in axes[nsrc:]:
        ax.axis("off")

    cbar = fig.colorbar(
        mappable,
        ax=axes[:nsrc],
        orientation="horizontal",
        fraction=0.03,
        pad=0.04
    )
    cbar.set_label(f"{pol_name} acumulado no tempo e nas camadas ({unit}) [log]")

    fig.suptitle(
        f"Comparação entre fontes de emissão - {pol_name}",
        fontsize=18,
        fontweight="bold",
        y=0.98
    )

    plt.savefig(
        os.path.join(figpath, f"comparacao_fontes_{pol_name}.png"),
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()

def plot_source_comparison_timeseries(source_series, pol_name, unit, figpath, source_labels=None):
    """
    Plota a comparação temporal entre fontes de emissão para um mesmo poluente.
    """
    if not source_series:
        print(f"Nenhuma série temporal disponível para o poluente {pol_name}.")
        return

    if source_labels is None:
        source_labels = {}

    fig, ax = plt.subplots(figsize=(12, 5))

    for source_name, content in source_series.items():
        time = content["time"]
        values = np.asarray(content["values"], dtype=float)

        values[values <= 0] = np.nan

        color = EMISSION_SOURCE_COLORS.get(source_name, EMISSION_SOURCE_COLORS["Outros"])
        label_name = source_labels.get(source_name, source_name)

        ax.plot(
            time,
            values,
            linewidth=1.6,
            marker="o",
            markersize=3,
            label=label_name,
            color=color
        )

    ax.set_title(
        f"Comparação temporal entre fontes de emissão - {pol_name}",
        fontsize=16,
        fontweight="bold"
    )
    ax.set_xlabel("Tempo")
    ax.set_ylabel(f"Emissões totais ({unit}) [log]")
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(frameon=False, ncol=2)

    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    ax.set_yscale("log")

    plt.savefig(
        os.path.join(figpath, f"comparacao_temporal_fontes_{pol_name}.png"),
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()

def calculate_region_annual_mean(da, ds, xlon, ylat, brazil):
    """
    Calcula, para um poluente e uma fonte, a emissão média anual por macro-região.

    Etapas:
    - separa por ano usando TFLAG
    - soma no tempo dentro de cada ano
    - soma nas camadas
    - agrega espacialmente por macro-região
    - calcula a média entre os anos disponíveis
    """
    import regionmask

    datetimes = get_ioapi_datetimes(ds)
    years = np.array(datetimes.year)
    unique_years = np.sort(np.unique(years))

    if "NM_REGIA" not in brazil.columns:
        raise ValueError("O shapefile precisa ter a coluna 'NM_REGIA'.")

    brazil_reg = brazil.dissolve(by="NM_REGIA", as_index=False)
    labels = brazil_reg["NM_REGIA"].astype(str).values

    regions = regionmask.Regions(
        outlines=list(brazil_reg.geometry),
        names=list(labels),
        abbrevs=list(labels),
    )

    mask = regions.mask(
        xr.DataArray(xlon, dims=("ROW", "COL")),
        xr.DataArray(ylat, dims=("ROW", "COL")),
    ).rename("region_id")

    annual_series = []

    for year in unique_years:
        idx = np.where(years == year)[0]
        da_year = da.isel(TSTEP=idx).sum(dim="TSTEP")

        if "LAY" in da_year.dims:
            da_year = da_year.sum(dim="LAY")

        by_region = (
            da_year.groupby(mask)
            .sum(dim="stacked_ROW_COL")
            .rename({"region_id": "region"})
            .assign_coords(region=("region", regions.names))
        )

        s = pd.Series(
            by_region.values.astype(float),
            index=by_region["region"].values,
            name=year
        )
        annual_series.append(s)

    if not annual_series:
        return pd.Series(dtype=float)

    annual_df = pd.DataFrame(annual_series)
    mean_annual = annual_df.mean(axis=0, skipna=True).fillna(0.0)

    return mean_annual

def plot_region_source_stacked_bars(region_source_means, pol_name, unit, figpath, source_labels=None):
    """
    Plota barras horizontais empilhadas por macro-região, comparando tipos de emissão.

    Parameters
    ----------
    region_source_means : dict
        {
            "fonte_1": pd.Series(index=macro_regiao, values=media_anual),
            "fonte_2": pd.Series(index=macro_regiao, values=media_anual),
            ...
        }

    pol_name : str
        Nome do poluente.

    unit : str
        Unidade.

    figpath : str
        Pasta de saída.

    source_labels : dict, optional
        Dicionário para converter nome interno da fonte em nome bonito.
    """
    if not region_source_means:
        print(f"Nenhum dado regional disponível para {pol_name}.")
        return

    if source_labels is None:
        source_labels = {}

    df = pd.DataFrame(region_source_means).fillna(0.0)

    if df.empty:
        print(f"DataFrame vazio para {pol_name}.")
        return

    df["__total__"] = df.sum(axis=1)
    df = df.sort_values("__total__", ascending=False)
    df = df.drop(columns="__total__")

    source_names = df.columns.tolist()

    source_colors = {
        src: EMISSION_SOURCE_COLORS.get(src, EMISSION_SOURCE_COLORS["Outros"])
        for src in source_names
    }

    fig, ax = plt.subplots(figsize=(11, 5.5))

    y = np.arange(len(df))
    left = np.zeros(len(df))
    row_totals = df.sum(axis=1).values.astype(float)

    for src in source_names:
        vals = df[src].values.astype(float)
        label_name = source_labels.get(src, src)

        ax.barh(
            y,
            vals,
            left=left,
            color=source_colors[src],
            edgecolor="none",
            height=0.8,
            label=label_name
        )

        pct_vals = np.where(row_totals > 0, (vals / row_totals) * 100, 0.0)

        for i, (v, p) in enumerate(zip(vals, pct_vals)):
            if np.isfinite(p) and p >= 10 and v > 0:
                ax.text(
                    left[i] + v / 2,
                    y[i],
                    f"{p:.0f}%",
                    ha="center",
                    va="center",
                    fontsize=8,
                    color="black",
                    weight="bold"
                )

        left += vals

    ax.set_yticks(y)
    ax.set_yticklabels(df.index.tolist(), fontsize=11, color="black")
    ax.invert_yaxis()

    ax.set_xlabel(f"Emissão média anual ({unit})")
    ax.set_title(
        f"Emissões médias anuais por macro-região e tipo de emissão\n{pol_name}",
        fontsize=16,
        fontweight="bold"
    )

    ax.grid(True, axis="x", linestyle="--", alpha=0.35)

    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

    ax.legend(
        title="Tipo de emissão",
        frameon=False,
        loc="lower right"
    )

    plt.savefig(
        os.path.join(figpath, f"barras_regionais_fontes_{pol_name}.png"),
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()