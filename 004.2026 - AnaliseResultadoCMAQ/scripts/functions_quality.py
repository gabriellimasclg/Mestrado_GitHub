# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 14:33:30 2026

@author: glima
"""

# -*- coding: utf-8 -*-

import os

import geopandas as gpd
import matplotlib.colors as colors
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import box

plt.rcParams["font.family"] = "Arial"


#%% ── helpers básicos ────────────────────────────────────────────────────────

def add_brazil_inverse_mask(ax, brazil, xlon, ylat, pad=1.0, facecolor="white"):
    """
    Plota uma máscara branca fora do Brasil para esconder pixels externos.
    """
    brazil_union = brazil.unary_union

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


def squeeze_var_dim(da):
    if "VAR" in da.dims and da.sizes["VAR"] == 1:
        da = da.squeeze("VAR", drop=True)
    return da


#%% ── tempo / poluentes ──────────────────────────────────────────────────────

def get_quality_datetimes(ds):
    """
    Converte TFLAG 1D no formato YYYYMMDDHH para DatetimeIndex.
    """
    if "TFLAG" not in ds:
        raise ValueError("O dataset não possui a variável 'TFLAG'.")

    tflag = ds["TFLAG"].values
    return pd.to_datetime(tflag.astype(str), format="%Y%m%d%H")


def get_quality_pollutants(ds):
    """
    Retorna as variáveis que representam poluentes de qualidade do ar.
    """
    excluded = {"TFLAG", "LAT", "LON"}
    return [v for v in ds.data_vars if v not in excluded]


#%% ── métricas de qualidade ──────────────────────────────────────────────────

def compute_quality_surface_domain_series(da):
    """
    Série temporal do domínio na camada superficial (LAY=0).
    Usa média espacial do domínio.
    """
    if "LAY" in da.dims:
        da = da.isel(LAY=0)

    dims_to_mean = [d for d in da.dims if d not in ["TSTEP"]]
    ts = da.mean(dim=dims_to_mean).compute()
    return ts


def compute_quality_daily_metric(da, pol_name, datetimes):
    """
    Calcula a métrica diária apropriada por poluente, usando a camada superficial.

    Regras:
    - PM10 / PM25 / PMC: média diária
    - NO2: máxima horária diária
    - O3: máxima média móvel de 8h diária
    - padrão fallback: média diária
    """
    ts = compute_quality_surface_domain_series(da)
    values = np.asarray(ts.values, dtype=float)

    if len(values) != len(datetimes):
        raise ValueError("O tamanho da série e das datas não coincide.")

    s = pd.Series(values, index=datetimes)

    pol_upper = pol_name.upper()

    if pol_upper in ["PM10", "PM25", "PMC"]:
        daily = s.resample("D").mean()

    elif pol_upper == "NO2":
        daily = s.resample("D").max()

    elif pol_upper == "O3":
        rolling8 = s.rolling(window=8, min_periods=8).mean()
        daily = rolling8.resample("D").max()

    else:
        daily = s.resample("D").mean()

    return daily


def get_quality_metric_label(pol_name):
    """
    Texto da métrica diária, para títulos/legendas.
    """
    pol_upper = pol_name.upper()

    if pol_upper in ["PM10", "PM25", "PMC"]:
        return "Média diária"
    elif pol_upper == "NO2":
        return "Máxima horária diária"
    elif pol_upper == "O3":
        return "Máxima média móvel de 8h diária"
    else:
        return "Métrica diária"


def compute_quality_annual_mean_map(da):
    """
    Calcula o campo espacial da média anual na camada superficial.
    """
    if "LAY" in da.dims:
        da = da.isel(LAY=0)

    da_map = da.mean(dim="TSTEP").compute()
    return da_map


def compute_quality_annual_domain_mean(da):
    """
    Calcula a média anual do domínio na camada superficial.
    """
    ts = compute_quality_surface_domain_series(da)
    return float(ts.mean().compute() if hasattr(ts.mean(), "compute") else ts.mean())


#%% ── figura principal ───────────────────────────────────────────────────────

def plot_quality_summary(da, ds, pol_name, unit, xlon, ylat, brazil, figpath):
    """
    Figura resumo para qualidade do ar:
    - mapa da média anual na camada superficial
    - série diária da métrica apropriada por poluente
    """
    datetimes = get_quality_datetimes(ds)

    da_map = compute_quality_annual_mean_map(da)
    daily_metric = compute_quality_daily_metric(da, pol_name, datetimes)
    metric_label = get_quality_metric_label(pol_name)
    domain_mean = compute_quality_annual_domain_mean(da)

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

    fig = plt.figure(figsize=(14, 6))
    gs = gridspec.GridSpec(
        nrows=1,
        ncols=2,
        figure=fig,
        width_ratios=[1.15, 1.35],
        wspace=0.18
    )

    ax_map = fig.add_subplot(gs[0, 0])
    ax_ts = fig.add_subplot(gs[0, 1])

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
    ax_map.set_aspect("equal")
    ax_map.set_anchor("C")
    ax_map.set_title(
        f"Média anual - camada superficial\nMédia do domínio = {domain_mean:.3f} {unit}",
        fontsize=13,
        fontweight="bold"
    )

    for spine in ax_map.spines.values():
        spine.set_visible(False)

    cbar = fig.colorbar(
        m,
        ax=ax_map,
        orientation="horizontal",
        fraction=0.05,
        pad=0.03
    )
    cbar.set_label(f"{pol_name} ({unit}) [log]")

    # série diária
    ax_ts.plot(
        daily_metric.index,
        daily_metric.values,
        linewidth=2.0,
        marker="o",
        markersize=4
    )

    ax_ts.set_title(metric_label, fontsize=13, fontweight="bold")
    ax_ts.set_xlabel("Tempo")
    ax_ts.set_ylabel(f"{pol_name} ({unit})")
    ax_ts.grid(True, linestyle="--", alpha=0.35)

    for spine in ["top", "right"]:
        ax_ts.spines[spine].set_visible(False)

    fig.suptitle(
        f"Qualidade do ar - {pol_name}",
        fontsize=18,
        fontweight="bold",
        y=0.98
    )

    plt.savefig(
        os.path.join(figpath, f"quality_summary_{pol_name}.png"),
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()
    
#%%

QUALITY_RULES = {
    "PM10": {"map_label": "Média anual", "daily_rule": "daily_mean"},
    "PM25": {"map_label": "Média anual", "daily_rule": "daily_mean"},
    "NO2": {"map_label": "Média anual", "daily_rule": "daily_max_hour"},
    "O3":   {"map_label": "Média anual", "daily_rule": "daily_mda8"},
}

def compute_quality_daily_metric_field(da, pol_name, datetimes):
    """
    Calcula a métrica diária por pixel na camada superficial.

    Retorna:
    - daily_df: DataFrame (dias x pixels)
    - ny, nx: dimensões espaciais
    """
    if pol_name not in QUALITY_RULES:
        raise ValueError(f"Poluente {pol_name} não está definido em QUALITY_RULES.")

    rule = QUALITY_RULES[pol_name]["daily_rule"]

    if "LAY" in da.dims:
        da = da.isel(LAY=0)

    nt = da.sizes["TSTEP"]
    ny = da.sizes["ROW"]
    nx = da.sizes["COL"]

    arr = da.values.reshape(nt, -1)
    df = pd.DataFrame(arr, index=datetimes)

    if rule == "daily_mean":
        daily_df = df.resample("D").mean()

    elif rule == "daily_max_hour":
        daily_df = df.resample("D").max()

    elif rule == "daily_mda8":
        rolling8 = df.rolling(window=8, min_periods=8).mean()
        daily_df = rolling8.resample("D").max()

    else:
        raise ValueError(f"Regra desconhecida: {rule}")

    return daily_df, ny, nx

def compute_quality_daily_metric_field(da, pol_name, datetimes):
    """
    Calcula a métrica diária por pixel na camada superficial.

    Retorna:
    - daily_df: DataFrame (dias x pixels)
    - ny, nx: dimensões espaciais
    """
    if pol_name not in QUALITY_RULES:
        raise ValueError(f"Poluente {pol_name} não está definido em QUALITY_RULES.")

    rule = QUALITY_RULES[pol_name]["daily_rule"]

    if "LAY" in da.dims:
        da = da.isel(LAY=0)

    nt = da.sizes["TSTEP"]
    ny = da.sizes["ROW"]
    nx = da.sizes["COL"]

    arr = da.values.reshape(nt, -1)
    df = pd.DataFrame(arr, index=datetimes)

    if rule == "daily_mean":
        daily_df = df.resample("D").mean()

    elif rule == "daily_max_hour":
        daily_df = df.resample("D").max()

    elif rule == "daily_mda8":
        rolling8 = df.rolling(window=8, min_periods=8).mean()
        daily_df = rolling8.resample("D").max()

    else:
        raise ValueError(f"Regra desconhecida: {rule}")

    return daily_df, ny, nx

def compute_quality_annual_mean_map(da):
    """
    Campo da média anual (ou média do período disponível) na camada superficial.
    """
    if "LAY" in da.dims:
        da = da.isel(LAY=0)

    return da.mean(dim="TSTEP").compute()


def compute_quality_daily_metric_mean_map(da, pol_name, datetimes):
    """
    Campo espacial da média da métrica diária ao longo do período.
    """
    daily_df, ny, nx = compute_quality_daily_metric_field(da, pol_name, datetimes)
    metric_mean = daily_df.mean(axis=0).values.reshape(ny, nx)
    return metric_mean


def compute_quality_daily_metric_series(da, pol_name, datetimes):
    """
    Série diária da métrica apropriada, agregada no domínio.
    """
    if "LAY" in da.dims:
        da = da.isel(LAY=0)

    dims_to_mean = [d for d in da.dims if d not in ["TSTEP"]]
    ts = da.mean(dim=dims_to_mean).compute()

    s = pd.Series(np.asarray(ts.values, dtype=float), index=datetimes)
    rule = QUALITY_RULES[pol_name]["daily_rule"]

    if rule == "daily_mean":
        return s.resample("D").mean(), "Média diária"

    elif rule == "daily_max_hour":
        return s.resample("D").max(), "Máxima média horária do dia"

    elif rule == "daily_mda8":
        rolling8 = s.rolling(window=8, min_periods=8).mean()
        return rolling8.resample("D").max(), "Máxima média móvel de 8h do dia"

    else:
        raise ValueError(f"Regra desconhecida: {rule}")
        
def plot_quality_legislative_mosaic(da, ds, pol_name, unit, xlon, ylat, brazil, figpath):
    """
    Figura 1x2 para qualidade do ar:
    - mapa da média anual
    - mapa da média da métrica diária regulatória
    """
    datetimes = get_quality_datetimes(ds)

    annual_map = compute_quality_annual_mean_map(da)
    metric_map = compute_quality_daily_metric_mean_map(da, pol_name, datetimes)
    daily_series, metric_label = compute_quality_daily_metric_series(da, pol_name, datetimes)

    annual_arr = np.asarray(annual_map, dtype=float)
    metric_arr = np.asarray(metric_map, dtype=float)

    annual_domain_mean = float(np.nanmean(annual_arr))

    # escala comum entre os dois mapas
    positive_1 = annual_arr[annual_arr > 0]
    positive_2 = metric_arr[metric_arr > 0]

    candidates = []
    if positive_1.size > 0:
        candidates.append(float(np.nanmin(positive_1)))
    if positive_2.size > 0:
        candidates.append(float(np.nanmin(positive_2)))

    vmin = min(candidates) if candidates else 1e-12
    if not np.isfinite(vmin) or vmin <= 0:
        vmin = 1e-12

    vmax = max(float(np.nanmax(annual_arr)), float(np.nanmax(metric_arr)))
    if not np.isfinite(vmax) or vmax <= 0:
        vmax = 1.0

    cmap = plt.colormaps["Spectral_r"].copy()
    norm = colors.LogNorm(vmin=vmin, vmax=vmax)

    fig = plt.figure(figsize=(13, 6))
    gs = gridspec.GridSpec(1, 2, figure=fig, wspace=0.12)

    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    # mapa 1 - média anual
    m1 = ax1.pcolormesh(
        xlon, ylat, annual_arr,
        cmap=cmap, norm=norm, shading="auto"
    )
    add_brazil_inverse_mask(ax=ax1, brazil=brazil, xlon=xlon, ylat=ylat, pad=1.0)
    brazil.boundary.plot(ax=ax1, color="black", linewidth=0.8, zorder=10)

    ax1.set_xticks([])
    ax1.set_yticks([])
    ax1.set_aspect("equal")
    ax1.set_anchor("C")
    ax1.set_title(
        f"Média anual\nMédia do domínio = {annual_domain_mean:.3f} {unit}",
        fontsize=12,
        fontweight="bold"
    )

    # mapa 2 - média da métrica diária regulatória
    m2 = ax2.pcolormesh(
        xlon, ylat, metric_arr,
        cmap=cmap, norm=norm, shading="auto"
    )
    add_brazil_inverse_mask(ax=ax2, brazil=brazil, xlon=xlon, ylat=ylat, pad=1.0)
    brazil.boundary.plot(ax=ax2, color="black", linewidth=0.8, zorder=10)

    ax2.set_xticks([])
    ax2.set_yticks([])
    ax2.set_aspect("equal")
    ax2.set_anchor("C")
    ax2.set_title(
        f"{metric_label}\n(média no período)",
        fontsize=12,
        fontweight="bold"
    )

    for ax in [ax1, ax2]:
        for spine in ax.spines.values():
            spine.set_visible(False)

    cbar = fig.colorbar(
        m2,
        ax=[ax1, ax2],
        orientation="horizontal",
        fraction=0.035,
        pad=0.07
    )
    cbar.set_label(f"{pol_name} ({unit}) [log]")

    fig.suptitle(
        f"Qualidade do ar - {pol_name}",
        fontsize=18,
        fontweight="bold",
        y=0.98
    )

    plt.savefig(
        os.path.join(figpath, f"quality_legislative_mosaic_{pol_name}.png"),
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()










