# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 14:33:13 2026

@author: glima
"""

'''
Este script realiza uma análise inicial de dados de qualidade do ar a partir de
arquivos NetCDF gerados após o CMAQ. Os arquivos de uma pasta quality_* são
abertos em conjunto, de modo que cada poluente fique como uma variável do
xarray.Dataset. As coordenadas geográficas são lidas diretamente de LAT e LON,
e o tempo é obtido a partir de TFLAG no formato YYYYMMDDHH.

Para cada poluente disponível, o script gera uma figura resumo contendo:
(i) mapa da média anual na camada superficial; e
(ii) série diária com a métrica adequada ao poluente:
- MP10 / PM25 / PMC: média diária
- NO2: máxima horária diária
- O3: máxima média móvel de 8 horas diária
'''

import glob
import os

import geopandas as gpd
import xarray as xr

from functions_quality import (
    get_quality_pollutants,
    plot_quality_summary,
    plot_quality_legislative_mosaic,
)

#%% ── caminhos de entrada/saída ─────────────────────────────────────────────

quality_base_path = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\004.2026 - AnaliseResultadoCMAQ\inputs\quality_finn"
shp_path = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\input_base\BR_UF_2024\BR_UF_2024.shp"
figures_base_path = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\004.2026 - AnaliseResultadoCMAQ\figures"

quality_name = "quality_finn"
figpath = os.path.join(figures_base_path, quality_name)
os.makedirs(figpath, exist_ok=True)

#%% ── shapefile do Brasil ───────────────────────────────────────────────────

brazil = gpd.read_file(shp_path).to_crs("EPSG:4326")

#%% ── carregar dados ────────────────────────────────────────────────────────

files = sorted(glob.glob(os.path.join(quality_base_path, "*.nc")))

if not files:
    raise FileNotFoundError(f"Nenhum arquivo .nc encontrado em: {quality_base_path}")

print(f"Arquivos encontrados: {len(files)}")

ds = xr.open_mfdataset(files, combine="by_coords")
print("Dataset de qualidade carregado com sucesso.")

# coordenadas já prontas
xlon = ds["LON"].values
ylat = ds["LAT"].values

# poluentes disponíveis
quality_pollutants = get_quality_pollutants(ds)

if not quality_pollutants:
    raise ValueError("Nenhum poluente encontrado no dataset de qualidade.")

print("Poluentes disponíveis:", quality_pollutants)

#%% ── loop por poluente ─────────────────────────────────────────────────────

for pol in quality_pollutants:
    print(f"\n{'=' * 60}")
    print(f"Processando poluente de qualidade: {pol}")
    print(f"{'=' * 60}")

    try:
        da = ds[pol]
        unit = da.attrs.get("units", "N/A")

        print(f"Poluente: {pol}")
        print(f"Unidade: {unit}")
        print(f"Dims: {da.dims}")
        print(f"Shape: {da.shape}")

        plot_quality_summary(
            da=da,
            ds=ds,
            pol_name=pol,
            unit=unit,
            xlon=xlon,
            ylat=ylat,
            brazil=brazil,
            figpath=figpath
        )
        
        plot_quality_legislative_mosaic(
            da=da,
            ds=ds,
            pol_name=pol,
            unit=unit,
            xlon=xlon,
            ylat=ylat,
            brazil=brazil,
            figpath=figpath
        )
                
    except Exception as e:
        print(f"Erro ao processar {pol}: {e}")
        continue

ds.close()
print("\nProcessamento de qualidade concluído.")