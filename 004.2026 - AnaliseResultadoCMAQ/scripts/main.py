'''
# ok - emissão em cada camada da atmosfera
# ok - emissão espacial das camadas 1-2-3 da atmosfera (onde tem mais emissões)
# ok - fazer barras empilhadas normalizado por camada da atmosfera sem log por região
# ok - transformar essa última figura em vertical, transformar a figura geral em quadrado
# ok - colocar as unidades mol/s p gases e g/s para sólidos

# altura da chaminé, temperatura da chaminé, velocidade de saída, diametro, vzão
# saber se o que influencia a convecção são fatores da chaminé ou fatores externos
# o quão sensível isso é em cada uma das camadas
'''

import glob
import os

import geopandas as gpd
import xarray as xr

from functions import (
    ioapiCoords,
    eqmerc2latlon,
    squeeze_var_dim,
    build_pollutant,
    plot_spatial_mosaic,
    calculate_by_region_lay,
    plot_regional_vertical_profile,
)

#%% ── caminhos de entrada ────────────────────────────────────────────────────

repo_path = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\004.2026 - AnaliseResultadoCMAQ\inputs\netcdf"
shp_path = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\input_base\BR_UF_2024\BR_UF_2024.shp"
figpath = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\004.2026 - AnaliseResultadoCMAQ\figures"

#%% ── definição dos poluentes ───────────────────────────────────────────────

POLLUTANT_SPECS = {
    "CO": ["CO"],
    "NO2": ["NO2"],
    "SO2": ["SO2"],
    "MP25": [
        "PMFINE_LVPO1", "PMFINE_LVOO1", "PMFINE_SO4", "PMFINE_NO3",
        "PMFINE_CL", "PMFINE_NH4", "PMFINE_NA", "PMFINE_CA",
        "PMFINE_MG", "PMFINE_K", "PMFINE_FE", "PMFINE_AL",
        "PMFINE_SI", "PMFINE_TI", "PMFINE_MN", "PMFINE_H2O",
        "PMFINE_OTHR"
    ],
    "MP10": [
        "PMC",
        "PMFINE_LVPO1", "PMFINE_LVOO1", "PMFINE_SO4", "PMFINE_NO3",
        "PMFINE_CL", "PMFINE_NH4", "PMFINE_NA", "PMFINE_CA",
        "PMFINE_MG", "PMFINE_K", "PMFINE_FE", "PMFINE_AL",
        "PMFINE_SI", "PMFINE_TI", "PMFINE_MN", "PMFINE_H2O",
        "PMFINE_OTHR"
    ],
}

POLLUTANT_UNITS = {
    "CO": "mol/s",
    "NO2": "mol/s",
    "SO2": "mol/s",
    "MP25": "g/s",
    "MP10": "g/s"
}

pollutants = ["NO2", "SO2", "MP10", "MP25", "CO"]

#%% ── carregar dados ────────────────────────────────────────────────────────

file_pattern = os.path.join(repo_path, "*.nc")
files = sorted(glob.glob(file_pattern))

if not files:
    raise FileNotFoundError(f"Nenhum arquivo .nc encontrado em: {repo_path}")

ds = xr.open_mfdataset(
    files,
    concat_dim="TSTEP",
    combine="nested",
    parallel=True
)

print("Dataset carregado com sucesso.")

# coordenadas do grid
xv, yv, lon, lat = ioapiCoords(ds)
xlon, ylat = eqmerc2latlon(ds, xv, yv)

# shapefile do Brasil
brazil = gpd.read_file(shp_path).to_crs("EPSG:4326")

#%% ── dimensões padrão ──────────────────────────────────────────────────────

# usa uma variável de referência só para descobrir as dimensões
ref_da = squeeze_var_dim(ds["CO"]) if "CO" in ds.data_vars else squeeze_var_dim(ds[list(ds.data_vars)[0]])
dims_time = [d for d in ["TSTEP"] if d in ref_da.dims]

if not dims_time:
    raise ValueError("A dimensão TSTEP não foi encontrada no dataset.")

#%% ── loop principal ────────────────────────────────────────────────────────

for pol in pollutants:
    print(f"\n{'=' * 60}")
    print(f"Processando poluente: {pol}")
    print(f"{'=' * 60}")

    da, present, missing = build_pollutant(
        ds=ds,
        pol_name=pol,
        pollutant_specs=POLLUTANT_SPECS,
        verbose=True
    )

    if da is None:
        print(f"{pol}: pulado.")
        continue
    
    unit = POLLUTANT_UNITS[pol]
    
    print(f"{pol}: usando {len(present)} espécie(s).")
    if missing:
        print(f"{pol}: cálculo parcial. Espécies ausentes: {len(missing)}")

    # 1) mosaico espacial pixelado
    plot_spatial_mosaic(
        da=da,
        pol_name=pol,
        unit=unit,
        xlon=xlon,
        ylat=ylat,
        brazil=brazil,
        dims_time=dims_time,
        figpath=figpath
    )

    # 2) cálculo regional por camada
    by_region_lay = calculate_by_region_lay(
        da=da,
        xlon=xlon,
        ylat=ylat,
        brazil=brazil
    )

    # 3) mosaico com macro-regiões + perfil vertical
    plot_regional_vertical_profile(
        by_region_lay=by_region_lay,
        pol_name=pol,
        unit=unit,
        brazil=brazil,
        figpath=figpath
    )