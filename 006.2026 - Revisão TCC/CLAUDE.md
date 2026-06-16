# CLAUDE.md

Guia para o Claude trabalhar neste repositório. **Idioma do projeto e das respostas: PT-BR.**

> **Manutenção:** revise/atualize este arquivo sempre que mexer no código (ou quando o usuário pedir).
> Última revisão: 2026-06-11.

---

## 1. O que é o projeto

Mestrado / TCC — construção de um **inventário de emissões de NMVOC (COV não-metano) da
indústria alimentícia brasileira**, a partir de dados de produção declarados ao IBAMA (CTF/APP),
cruzados com fatores de emissão tier 2 (EMEP/EEA) e comparados com EDGAR e PIA-IBGE.

A emissão é calculada como **`Emissão = Produção tratada × Fator de Emissão`**.

---

## 2. Pipeline e arquivos

O fluxo de **geração do inventário** vive no notebook (canônico) e tem uma versão `.py` mais antiga
que pode estar defasada — quando divergirem, **o notebook manda**.

| Arquivo | Papel |
|---|---|
| `scripts/AlimentíciaRev.ipynb` | **Pipeline canônico** (seções 1–7): base → NFR → unidades → escala → outliers → export |
| `scripts/main_tratDados.py` | Versão script (mais antiga) do mesmo pipeline; Análise 02 ainda usa lógica `p90/np.where` antiga |
| `scripts/functions_TratDados.py` | Funções de tratamento (outliers, filtros, merges, download IBAMA) |
| `scripts/main_AnaliseDados.py` | Análises e gráficos **a partir do CSV V3** (mapas, mosaicos, cubo xarray, comparação EDGAR) |
| `scripts/functions_AnaliseDados.py` | Funções de plotagem usadas pelos `main_*Analise*` |
| `scripts/main_PiaAnalisys.py` | Comparação produção inventário × PIA-IBGE (BIAS por produto) |
| `scripts/clean_text.py` | Helper: remove acento, strip, upper |

**Saída final:** `outputs/inventarioEmissoesIndustriaisIndustriaAlimenticiaBR_V3.csv`
(consumida por `main_AnaliseDados.py` e `main_PiaAnalisys.py`).

### Etapas do pipeline (notebook)
1. Importações e paths
2. Base geral: download IBAMA (CNPJ+coordenadas) + dados de produção
3. Base com NFR + código de produto IBGE (PRODLIST) + produção; mantém só NFR `2.H.2`
4. **Ajuste 01** — indústrias com mais de uma unidade de medida (correção manual de unidades)
5. **Análise 02** — correção de escala de produção (×/÷ potências de 10)
6. **Seção 5B** — `aplicar_verif_usina`: merge com `5_verif_usina.xlsx` (chaves CNPJ+MUNICIPIO+nom_pessoa);
   se `usina == 'não'`, zera **toda** a produção do CNPJ (v4 → v5). A função é definida antes da 6.1,
   mas **aplicada após** o `tratamento_outliers_v3` (v4 só existe depois dele)
7. **Análise 03** — `tratamento_outliers_v3`: filtro de histórico → correção de outliers → preenchimento de lacunas
8. Cálculo da emissão (a partir de `prodtonhl_v5`) e export do CSV V3

---

## 3. Convenção de versionamento das colunas

O tratamento é **rastreável**: cada etapa cria uma nova coluna de produção e uma de status,
preservando as anteriores. Nunca sobrescreva uma versão anterior — crie a próxima.

**Produção** (cada etapa parte da anterior):
- `prodtonhl_v0` — produção × fator de conversão de unidade
- `prodtonhl_v1` — v0 × `fator_escala` (correção manual de escala, Análise 02)
- `prodtonhl_v2` — após filtro de histórico (séries inaptas → 0)
- `prodtonhl_v3` — após correção de outliers
- `prodtonhl_v4` — após preenchimento de lacunas
- `prodtonhl_v5` — **versão final** (verificação manual de usinas: CNPJs com `usina == 'não'` zerados);
  é a usada no cálculo da emissão

**Status:**
- `status_v02` — filtro de alimentos emissores de COV
- `status_v03` — correção de unidades (Ajuste 01)
- `status_v04` — classificação de escala (Análise 02)
- `status_v05` / `fator_escala` — fator de escala aplicado
- `status_v06` — filtragem por histórico (Apto / Histórico Insuficiente / Série Zerada)
- `status_v07` — correção de outliers
- `status_v08` — preenchimento de lacunas
- `status_v09` — verificação manual de usinas (Produção zerada (não é usina) / Usina confirmada /
  Verificação inconclusiva / Sem verificação manual)
- `flag_outlier` — auditoria (True = ponto detectado como outlier no Stage 2)

**Cálculo da emissão** (após `prodtonhl_v5`):
```
Emissão NMCOV (kg)  = prodtonhl_v5 × Value      # 'Value' = fator de emissão (tier 2)
Emissão NMCOV (ton) = Emissão (kg) / 1000        # ton para casar com EDGAR
# CI_lower / CI_upper recebem o mesmo tratamento
```

---

## 4. Loop de revisão manual (importante)

Várias etapas **exportam para `outputs/` → o usuário edita no Excel → reimporta de
`inputs/MaterialGeradoManualmente/`**. Não tente automatizar essas correções manuais; o papel do
código é gerar a planilha de revisão e reincorporar o resultado. Exemplos:
- `4_fatorEscala.xlsx` / `4A_fatorEscala.xlsx` (Análise 02 — fatores de escala)
- `vefirManual_01_unidadesPorProdutoPorProdutor.xlsx` (Ajuste 01 — unidades)
- `1_CodProdutoClassificadoNFR.xlsx` (classificação NFR dos produtos)
- `5_verif_usina.xlsx` (Seção 5B — verificação de usinas; gerada via `AssociarDF_AntigoNovo.ipynb`;
  coluna `usina` vem suja: `sim`/`Sim`/`não`/`duv `/`?`/`NaN` → normalizar com strip+lower+sem acento)

---

## 5. Detecção de outliers — duas camadas

- **Intra-série** (na própria série temporal CNPJ+município+produto): compara cada valor com a
  mediana/mediana-móvel da própria série. Pega **picos** isolados.
- **Cross-section** (vs pares do setor): compara com Q90/Q95 da `categoria`/`tipo_industria_nfr`.
  Pega **emissores consistentemente gigantes** que passam batido na checagem intra-série porque são
  grandes todo ano (`mascara_gigante`: mediana da série > 5× Q95 do setor — Análise 02, notebook).

⚠️ **Checagem de EF implícito (`emissão/produção` vs `Value`) é circular e não detecta nada** —
como a emissão é definida como produção × Value, o EF implícito sempre é igual ao nominal.

---

## 6. Gotchas / convenções

- **CSV final em `encoding='latin1'`** (não utf-8). Ler de volta com o mesmo encoding.
- **CNPJ exportado com prefixo `'`** (`"'" + cnpj`) para preservar zeros à esquerda no Excel;
  ao reler, tratar como string (`dtype={'CNPJ':'string'}`) e re-padronizar com `.str.rjust(14,'0')`.
- **LATITUDE/LONGITUDE** vêm com vírgula decimal no CSV → `.str.replace(',', '.').astype(float)`.
- **CPF vs CNPJ:** mantém-se só registros com 14 dígitos (CNPJ); CPFs são descartados (com log).
- **Filtro NFR:** só `2.H.2` (alimentos/bebidas emissores de COV) segue no pipeline.
- **Categorias de produto** (`tipo_industria_nfr` + `food_color`) vêm de `classificar_produto()`,
  que mapeia `Technology` → {Açucar, Café, Margarina, Bolos/biscoitos, Carnes/Ração, Vinho, Pão,
  Cerveja, Destilados}. Essa função está **duplicada** em vários arquivos — se mudar, mude em todos.
- **Nome do notebook tem acento** (`AlimentíciaRev.ipynb`). No Windows, o console PowerShell/cp1252
  quebra ao imprimir o caminho — para inspecionar/editar o `.ipynb` via Python use `glob.glob('scripts/Aliment*Rev.ipynb')` e `open(..., encoding='utf-8')`.
- **Ambiente:** Windows, Python 3.13, pandas/numpy/geopandas/xarray/matplotlib/seaborn.

---

## 7. Estrutura de pastas

```
repo/
├── figures/      # saídas gráficas (.png, mosaicos, mapas)
├── inputs/       # dados de entrada
│   ├── MaterialBaixado/            # baixados (EF_tier2.csv, EDGAR, etc.)
│   └── MaterialGeradoManualmente/  # planilhas revisadas manualmente (reimportadas)
├── outputs/      # exports do pipeline
│   ├── log/      # logs de rastreabilidade por etapa (log_v01, v02, v03...)
│   └── obsoleto/
└── scripts/      # código (ver tabela na seção 2)
```
