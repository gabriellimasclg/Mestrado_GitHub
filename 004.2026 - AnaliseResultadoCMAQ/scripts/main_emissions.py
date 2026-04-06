#AMANHA - fzr correções da ultima conversa com chatgpt

'''
Este script realiza a análise espacial, vertical e temporal de emissões atmosféricas
a partir de arquivos no formato IOAPI/CMAQ (.nc). O código foi estruturado para processar
múltiplas fontes de emissão (ex.: industrial, veicular, residencial e ressuspensão),
organizadas em pastas distintas, aplicando automaticamente o mesmo conjunto de análises
para cada uma delas.

Para cada fonte, os arquivos são carregados em um único xarray.Dataset ao longo da
dimensão temporal (TSTEP), e as espécies químicas são agregadas conforme a definição
de cada poluente (ex.: MP10 e MP2.5). Em seguida, são geradas diferentes visualizações:
(i) mosaicos espaciais por camada atmosférica, quando o inventário possui múltiplas
camadas; (ii) perfis verticais por macro-região, também apenas para inventários
multicamadas; (iii) mapas regionais com participação percentual das emissões totais,
para inventários monocamada; (iv) análises temporais (horas do dia, dias da semana e
meses); e (v) mosaicos espaciais anuais acompanhados de série histórica.

As coordenadas geográficas são derivadas do grid do modelo e convertidas para
latitude/longitude, permitindo sobreposição com o shapefile do Brasil. Os resultados
são salvos automaticamente em subpastas específicas para cada tipo de emissão,
garantindo organização e escalabilidade para diferentes cenários e inventários.
'''

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

from functions_emissions import (
    ioapiCoords,
    eqmerc2latlon,
    squeeze_var_dim,
    build_pollutant,
    plot_spatial_mosaic,
    calculate_by_region_lay,
    plot_regional_vertical_profile,
    plot_temporal_mosaic,
    plot_annual_spatial_mosaic,
    plot_regional_total_map,
    plot_source_comparison_mosaic,
    plot_source_comparison_timeseries,
    get_ioapi_datetimes,
    build_domain_time_series,
    calculate_region_annual_mean,
    plot_region_source_stacked_bars,
)

#%% ── caminhos de entrada/saída ─────────────────────────────────────────────

inputs_base_path = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\004.2026 - AnaliseResultadoCMAQ\inputs"
shp_path = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\input_base\BR_UF_2024\BR_UF_2024.shp"
figures_base_path = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\004.2026 - AnaliseResultadoCMAQ\figures"

# fontes de emissão a processar
EMISSION_SOURCES = {
    "emission_ind": os.path.join(inputs_base_path, "emission_ind"),
    "emission_braves_classic": os.path.join(inputs_base_path, "emission_braves_classic"),
    "emission_braves_refuel": os.path.join(inputs_base_path, "emission_braves_refuel"),
    "emission_braves_ressuspension": os.path.join(inputs_base_path, "emission_braves_ressuspension"),
    "emission_resid": os.path.join(inputs_base_path, "emission_resid"),
    "emission_windblow": os.path.join(inputs_base_path, "emission_windblow"),
    "emission_finn": os.path.join(inputs_base_path, "emission_finn")
}
SOURCE_LABELS = {
    "emission_ind": "Industrial",
    "emission_braves_classic": "Veicular - escapamento",
    "emission_braves_refuel": "Veicular - evaporação/abastecimento",
    "emission_braves_ressuspension": "Veicular - ressuspensão",
    "emission_resid": "Residencial",
    "emission_windblow": "Ressuspensão pelo vento",
    "emission_finn": "Queimadas",
}

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

# fontes que precisam de correção de orientação
ROTATED_SOURCES = [
    "emission_braves_classic",
    "emission_braves_ressuspension"
]

# cache para o mosaico comparativo entre fontes
comparison_maps = {pol: {} for pol in pollutants}
comparison_series = {pol: {} for pol in pollutants}
region_comparison = {pol: {} for pol in pollutants}

#%% ── shapefile do Brasil ───────────────────────────────────────────────────

brazil = gpd.read_file(shp_path).to_crs("EPSG:4326")

#%% ── loop por fonte de emissão ─────────────────────────────────────────────

for source_name, repo_path in EMISSION_SOURCES.items():
    print(f"\n{'#' * 50}")
    print(f"Processando fonte de emissão: {source_name}")
    print(f"{'#' * 50}")
    
    display_name = SOURCE_LABELS.get(source_name, source_name)
    
    if not os.path.isdir(repo_path):
        print(f"Pasta não encontrada. Pulando: {repo_path}")
        continue

    file_pattern = os.path.join(repo_path, "*.nc")
    files = sorted(glob.glob(file_pattern))

    if not files:
        print(f"Nenhum arquivo .nc encontrado em: {repo_path}")
        continue

    print(f"Arquivos encontrados: {len(files)}")

    ds = xr.open_mfdataset(
        files,
        concat_dim="TSTEP",
        combine="nested",
        parallel=True
    )

    print("Dataset carregado com sucesso.")

    # coordenadas do grid
    # coordenadas do grid
    xv, yv, lon, lat = ioapiCoords(ds)
    xlon, ylat = eqmerc2latlon(ds, xv, yv)
    
    # correção global de orientação para fontes invertidas
    if source_name in ROTATED_SOURCES:
        xlon = xlon[::-1, :]
        ylat = ylat[::-1, :]

    # pasta de saída específica da fonte
    figpath = os.path.join(figures_base_path, source_name)
    os.makedirs(figpath, exist_ok=True)

    ### ── dimensões padrão ──────────────────────────────────────────────────

    candidate_vars = [v for v in ds.data_vars if v != "TFLAG"]

    if not candidate_vars:
        print("Nenhuma variável de emissão encontrada além de TFLAG. Pulando esta fonte.")
        ds.close()
        continue
    
    ref_var = "CO" if "CO" in candidate_vars else candidate_vars[0]
    ref_da = squeeze_var_dim(ds[ref_var])
    
    dims_time = [d for d in ["TSTEP"] if d in ref_da.dims]

    if not dims_time:
        print("A dimensão TSTEP não foi encontrada no dataset. Pulando esta fonte.")
        ds.close()
        continue

    ### ── loop por poluente ─────────────────────────────────────────────────

    for pol in pollutants:
        print(f"\n{'=' * 60}")
        print(f"Fonte: {source_name} | Processando poluente: {pol}")
        print(f"{'=' * 60}")

        try:
            da, present, missing = build_pollutant(
                ds=ds,
                pol_name=pol,
                pollutant_specs=POLLUTANT_SPECS,
                verbose=True
            )

            if da is None:
                print(f"{pol}: não disponível em {source_name}. Pulando apenas este poluente.")
                continue

            unit = POLLUTANT_UNITS[pol]

            print(f"{pol}: usando {len(present)} espécie(s).")
            if missing:
                print(f"{pol}: cálculo parcial. Espécies ausentes: {len(missing)}")

            # mapa acumulado para comparação entre fontes
            dims_map_compare = [d for d in da.dims if d in ["TSTEP", "LAY"]]
            da_map_compare = da.sum(dim=dims_map_compare).compute()

            comparison_maps[pol][source_name] = {
                "data": da_map_compare,
                "xlon": xlon,
                "ylat": ylat
            }
            # série temporal agregada do domínio para comparação entre fontes
            datetimes = get_ioapi_datetimes(ds)
            ts_domain = build_domain_time_series(da)
            
            comparison_series[pol][source_name] = {
                "time": datetimes,
                "values": ts_domain.values
            }
            
            # média anual por macro-região para gráfico de barras empilhadas
            region_mean_annual = calculate_region_annual_mean(
                da=da,
                ds=ds,
                xlon=xlon,
                ylat=ylat,
                brazil=brazil
            )
            
            region_comparison[pol][source_name] = region_mean_annual

            nlay = da.sizes.get("LAY", 1)
            print(f"LAY size para {source_name} - {pol}: {nlay}")

            if nlay > 1:
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

                by_region_lay = calculate_by_region_lay(
                    da=da,
                    xlon=xlon,
                    ylat=ylat,
                    brazil=brazil
                )

                plot_regional_vertical_profile(
                    by_region_lay=by_region_lay,
                    pol_name=pol,
                    unit=unit,
                    brazil=brazil,
                    figpath=figpath
                )

            else:
                print(f"{pol}: inventário monocamada. Pulando análises por camada.")

                plot_regional_total_map(
                    da=da,
                    pol_name=pol,
                    unit=unit,
                    xlon=xlon,
                    ylat=ylat,
                    brazil=brazil,
                    figpath=figpath
                )

            plot_temporal_mosaic(
                da=da,
                ds=ds,
                pol_name=pol,
                unit=unit,
                xlon=xlon,
                ylat=ylat,
                brazil=brazil,
                figpath=figpath,
                source_name=source_name
            )
            plot_annual_spatial_mosaic(
                da=da,
                ds=ds,
                pol_name=pol,
                unit=unit,
                xlon=xlon,
                ylat=ylat,
                brazil=brazil,
                figpath=figpath,
                source_name=source_name
            )

        except Exception as e:
            print(f"Erro ao processar {pol} em {source_name}: {e}")
            continue

print("\nProcessamento concluído.")

#%% ── mosaico comparativo entre fontes ──────────────────────────────────────

comparison_figpath = os.path.join(figures_base_path, "_comparacao_fontes")
os.makedirs(comparison_figpath, exist_ok=True)

for pol in pollutants:
    unit = POLLUTANT_UNITS[pol]

    print(f"\nMontando mosaico comparativo entre fontes para {pol}...")
    source_maps = comparison_maps.get(pol, {})
    if source_maps:
       plot_source_comparison_mosaic(
            source_maps=source_maps,
            pol_name=pol,
            unit=unit,
            brazil=brazil,
            figpath=comparison_figpath,
            source_labels=SOURCE_LABELS
        )
    else:
        print(f"Nenhuma fonte disponível para o mosaico de {pol}.")

    print(f"Montando comparação temporal entre fontes para {pol}...")
    source_series = comparison_series.get(pol, {})
    if source_series:
        plot_source_comparison_timeseries(
            source_series=source_series,
            pol_name=pol,
            unit=unit,
            figpath=comparison_figpath,
            source_labels=SOURCE_LABELS
        )
    else:
        print(f"Nenhuma série temporal disponível para {pol}.")

    print(f"Montando barras empilhadas por macro-região para {pol}...")
    region_source_means = region_comparison.get(pol, {})
    if region_source_means:
        plot_region_source_stacked_bars(
            region_source_means=region_source_means,
            pol_name=pol,
            unit=unit,
            figpath=comparison_figpath,
            source_labels=SOURCE_LABELS
        )
    else:
        print(f"Nenhum dado regional disponível para {pol}.")
        
print("\nProcessamento concluído.")

#%% Air quality

import glob
import os

repo_path = r"C:\Users\glima\OneDrive\Documentos\Mestrado_GitHub\004.2026 - AnaliseResultadoCMAQ\inputs\quality_finn"
files = sorted(glob.glob(os.path.join(repo_path, "*.nc")))

print(len(files))
print(files[:5])
print(files[-5:])

ds = xr.open_mfdataset(files, combine="by_coords")
print(ds)

print(ds["TFLAG"])
print(ds["TFLAG"].values[:10])

print(ds["TFLAG"].attrs)

print(ds["NO2"].attrs)
print(ds["O3"].attrs)

print(ds["LAT"])
print(ds["LON"])



