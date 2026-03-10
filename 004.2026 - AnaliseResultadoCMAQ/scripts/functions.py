# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 16:19:02 2026

@author: glima
"""

import xarray as xr
import numpy as np
import pyproj
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.gridspec as gridspec
import os


plt.rcParams["font.family"] = "Arial"

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


#%% ── helpers para espécies/poluentes ───────────────────────────────────────

def squeeze_var_dim(da):
    if "VAR" in da.dims:
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
    Camada 1, 2, 3, 39, 40 e Total
    """
    l1 = da.isel(LAY=0).sum(dim=dims_time).compute()
    l2 = da.isel(LAY=1).sum(dim=dims_time).compute()
    l3 = da.isel(LAY=2).sum(dim=dims_time).compute()
    l39 = da.isel(LAY=38).sum(dim=dims_time).compute()
    l40 = da.isel(LAY=39).sum(dim=dims_time).compute()
    lall = da.sum(dim="LAY").sum(dim=dims_time).compute()

    maps = [l1, l2, l3, l39, l40, lall]

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

    cmap = plt.colormaps["inferno"].copy()
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

    titles = ["Camada 1", "Camada 2", "Camada 3", "Camada 39", "Camada 40", "Total"]

    for ax, data, title in zip(axes, maps, titles):
        m = ax.pcolormesh(
            xlon, ylat, data,
            cmap=cmap,
            norm=norm,
            shading="auto"
        )

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
        ax.barh(
            lay,
            pct[:, j],
            left=left,
            color=color_map[reg],
            height=0.85
        )
        left += pct[:, j]

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