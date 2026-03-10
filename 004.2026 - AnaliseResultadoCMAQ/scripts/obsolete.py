# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 16:20:22 2026

@author: glima
"""
import xarray as xr
import glob
import os
import numpy as np
import pyproj
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.gridspec as gridspec
import geopandas as gpd



#%% ── carregar dados ─────────────────────────────────────────────────────────
repo_path    = r'C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\004.2026 - AnaliseResultadoCMAQ\inputs\netcdf'
file_pattern = os.path.join(repo_path, "*.nc")
files        = sorted(glob.glob(file_pattern))

ds = xr.open_mfdataset(files, concat_dim='TSTEP', combine='nested', parallel=True)
print("Dataset carregado com sucesso!")

xv,yv,lon,lat = ioapiCoords(ds)
xlon, ylat = eqmerc2latlon(ds, xv, yv)

ds.attrs

#%% ── processar CO ───────────────────────────────────────────────────────────

# 1. Processamento (Soma camadas e média temporal)
# Se 'VAR' existir como dimensão, remova-a (comum em arquivos IOAPI lidos via xarray)
co_data = ds['CO'].sum(dim='LAY').mean(dim='TSTEP').compute()

# 3. Preparar Plot
fig, ax = plt.subplots(figsize=(12, 10))
    
# Criar mapa de cores
cmap = plt.colormaps['inferno'].copy()
norm = colors.LogNorm(vmin=co_data.min() + 1e-6, vmax=co_data.max())

# Plotar os dados
mesh = ax.pcolor(xlon, ylat, co_data, cmap=cmap, norm=norm, shading='auto', alpha=0.9)

# 4. Adicionar Shapefile do Brasil
# Constrói o CRS a partir dos atributos do próprio ds
# O shapefile já vem em EPSG:4326 (lat/lon), então basta plotar sobre as coordenadas xlon/ylat
brazil = gpd.read_file(r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\input_base\BR_UF_2024\BR_UF_2024.shp").to_crs('4326')
brazil.boundary.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1.0, zorder=10)

# Detalhes do gráfico
plt.colorbar(mesh, ax=ax, label='Emissão Média CO (mol/s) [Log]')
ax.set_title('Média Temporal de Emissões de CO (Total das Camadas)')
ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
ax.set_xlim(xlon.min(), xlon.max())
ax.set_ylim(ylat.min(), ylat.max())

plt.tight_layout()
plt.show()

#%% Mosaico de emissões pixelado

# --- shapefile (se ainda não tiver em memória) ---
brazil = gpd.read_file(
    r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\input_base\BR_UF_2024\BR_UF_2024.shp"
).to_crs("EPSG:4326")

# --- CO base ---
co = ds["CO"]
if "VAR" in co.dims:
    co = co.squeeze("VAR", drop=True)

# Dimensões (ajusta automaticamente)
dims_space = [d for d in ["ROW", "COL"] if d in co.dims]
dims_time  = [d for d in ["TSTEP"] if d in co.dims]

# --- mapas acumulados no tempo (por célula) ---
# LAY é 0-based; camada 1 -> isel(LAY=0)
co_l1 = co.isel(LAY=0).sum(dim=dims_time).compute()
co_l2 = co.isel(LAY=1).sum(dim=dims_time).compute()
co_l3 = co.isel(LAY=2).sum(dim=dims_time).compute()
co_l39 = co.isel(LAY=38).sum(dim=dims_time).compute()
co_l40 = co.isel(LAY=39).sum(dim=dims_time).compute()
co_all = co.sum(dim="LAY").sum(dim=dims_time).compute()

# =======================
# FIGURA / LAYOUT
# =======================

fig = plt.figure(figsize=(12, 8))

gs = gridspec.GridSpec(
    nrows=2,
    ncols=3,
    figure=fig,
    wspace=-0.2,
    hspace=0.15
)

# linha superior
ax1 = fig.add_subplot(gs[0, 0])
ax2 = fig.add_subplot(gs[0, 1])
ax3 = fig.add_subplot(gs[0, 2])

# linha inferior
ax4 = fig.add_subplot(gs[1, 0])
ax5 = fig.add_subplot(gs[1, 1])
ax6 = fig.add_subplot(gs[1, 2])


def plot_map(ax, data, title):
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

    # remover eixos
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlabel("")
    ax.set_ylabel("")

    # remover box
    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.set_title(title, fontsize=12)

    return m


# linha superior
m1 = plot_map(ax1, co_l1,  "Camada 1")
m2 = plot_map(ax2, co_l2,  "Camada 2")
m3 = plot_map(ax3, co_l3,  "Camada 3")

# linha inferior
m4 = plot_map(ax4, co_l39, "Camada 39")
m5 = plot_map(ax5, co_l40, "Camada 40")
m6 = plot_map(ax6, co_all, "Total")


# colorbar horizontal
cbar = fig.colorbar(
    m6,
    ax=[ax1, ax2, ax3, ax4, ax5, ax6],
    orientation="horizontal",
    fraction=0.03,
    pad=0.06
)

cbar.set_label("CO acumulado no tempo (mol/s) [log]")

plt.show()

#%% Calculos de base para gráfico

import regionmask

# 1) Dissolver UF -> macro-região
# seu shapefile tem 27 UFs; dissolve junta as UFs em 5 regiões
brazil_reg = brazil.dissolve(by="NM_REGIA", as_index=False)

# 2) CO acumulado no tempo
# soma todos os TSTEP, mantendo a estrutura espacial e vertical
# antes: CO(TSTEP, LAY, ROW, COL)
# depois: CO(LAY, ROW, COL)
co_lay_map = co.sum(dim=dims_time).compute()

# 3) nomes das regiões
labels = brazil_reg["NM_REGIA"].astype(str).values

# cria objeto com os polígonos das regiões
regions = regionmask.Regions(
    outlines=list(brazil_reg.geometry),  # geometrias das regiões
    names=list(labels),                  # nomes completos
    abbrevs=list(labels),                # abreviações
)

# cria uma máscara espacial
# para cada pixel do grid (ROW,COL), diz qual região ele pertence
mask = regions.mask(
    xr.DataArray(xlon, dims=("ROW", "COL")),
    xr.DataArray(ylat, dims=("ROW", "COL")),
)

# renomeia a máscara
# isso vira o nome da dimensão quando fizermos o groupby
mask = mask.rename("region_id")

# agrupa os dados de CO usando a máscara
# todas células com mesmo region_id são agrupadas
gb = co_lay_map.groupby(mask)

# soma os pixels de cada região
# stacked_ROW_COL = todos os pixels do grid empilhados
# então essa soma vira: emissão total da região
co_by_region_lay = (
    gb.sum(dim="stacked_ROW_COL")      # soma pixels da região
      .rename({"region_id": "region"}) # renomeia dimensão
      .assign_coords(region=("region", regions.names)) # coloca nomes das regiões
)

# normaliza para porcentagem dentro de cada camada
# cada camada passa a somar 100%
pct_by_region_lay = (co_by_region_lay / co_by_region_lay.sum(dim="region")) * 100


#%% Mosaico com gráfico na vertical

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

# ====== dados para o gráfico (camadas) ======
co_sel = co_by_region_lay.isel(LAY=slice(0, 40))  # camadas 1..40

vals = co_sel.transpose("LAY", "region").values
tot  = vals.sum(axis=1)
pct  = (vals / tot[:, None]) * 100
layer_pct_total = (tot / tot.sum()) * 100

lay = np.arange(1, vals.shape[0] + 1)  # 1..40
regions = co_sel["region"].values.tolist()

# ====== % das emissões totais (todas as camadas) por região ======
region_total = co_by_region_lay.sum(dim="LAY")
region_pct_total = (region_total / region_total.sum()) * 100
pct_total_map = dict(zip(region_total["region"].values.tolist(),
                         region_pct_total.values))

# ====== cores ======
palette = plt.cm.Set2(np.linspace(0, 1, len(regions)))
color_map = dict(zip(regions, palette))

# ====== preparar mapa por macro-região ======
brazil_reg = brazil.dissolve(by="NM_REGIA", as_index=False)
brazil_reg["NM_REGIA"] = brazil_reg["NM_REGIA"].astype(str)
brazil_reg = brazil_reg[brazil_reg["NM_REGIA"].isin(regions)].copy()
brazil_reg["color"] = brazil_reg["NM_REGIA"].map(color_map)

# ====== MOSAICO: gráfico + mapa ======
fig = plt.figure(figsize=(11, 7), constrained_layout=True)
gs = gridspec.GridSpec(1, 2, figure=fig, width_ratios=[1.5, 2.1])

ax  = fig.add_subplot(gs[0, 1])  # barras + linha
axm = fig.add_subplot(gs[0, 0])  # mapa

# ---- título geral do mosaico
fig.suptitle(
    "Análise das emissões\nPoluente CO",
    fontsize=20,
    fontweight="bold",
    x=0.01,
    y=0.97,
    ha="left"
)

# ---- fundo cinza no gráfico
ax.set_facecolor("gainsboro")

# --- barras 100% empilhadas horizontais ---
left = np.zeros(pct.shape[0])
for j, reg in enumerate(regions):
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

# camada 1 embaixo
ax.set_ylim(0.5, lay[-1] + 0.5)
ax.set_yticks(np.arange(1, lay[-1] + 1, 1))

# --- linha do total bruto (eixo superior) ---
ax2 = ax.twiny()
ax2.set_facecolor("gainsboro")

ax2.plot(
    tot, lay,
    color="crimson",
    linewidth=3,
    marker="o",
    markersize=5,
    zorder=10
)

#ax2.set_xlabel("Total de emissões por camada", color="crimson")
leg = ax2.legend(
    labels=["Total de emissões por camada (mol/s)"],
    loc="center",
    bbox_to_anchor=(0.78, 1.07),  # controla posição
    frameon=False                 # remove borda
)

for text in leg.get_texts():
    text.set_color("crimson")
    
# ====== eixo superior vermelho + símbolo no eixo ======
ax2.tick_params(axis="x", colors="crimson")
ax2.spines["top"].set_color("crimson")

# símbolo vertical no eixo superior direito

# --- mapa ---
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
#ax_left.set_ylabel("% do total de emissões")
ax.spines["left"].set_position(("outward", 45))
ax_left.tick_params(axis="y", colors="crimson")
ax_left.yaxis.label.set_color("crimson")

plt.show()