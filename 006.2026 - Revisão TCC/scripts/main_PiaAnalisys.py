
"""
Created on Wed Sep  3 08:52:23 2025

@author: glima
"""
import os
import pandas as pd
import numpy as np
from functions_TratDados import import_treat_export_food_code,conecta_ibama_ef
from functions_AnaliseDados import plot_mosaico_scatter_dfs, plot_producao_empilhada, plot_mosaico_linhas_dfs

import matplotlib.pyplot as plt

plt.rcParams['font.family'] = 'Arial'

#%% tratDados
#fonte dos dados PIA: https://sidra.ibge.gov.br/tabela/7752

repo_path = os.path.dirname(os.getcwd())

#Importei material gerado manualmente
CodProdutoClassificadoNFR = pd.read_excel(os.path.join(repo_path,'inputs','MaterialGeradoManualmente','CodProdutoClassificadoNFR.xlsx'),
                                          dtype={'PRODLIST': str})

#filtrei os alimentos que tem emissões a serem consideradas
#CodProdutoClassificadoNFR = CodProdutoClassificadoNFR[CodProdutoClassificadoNFR['EmissaoNMCOV_NFR'] != 'Não']


caminho_pia = os.path.join(repo_path,'inputs','pia_ibge.xlsx')
pia = pd.read_excel(caminho_pia)

pia[['cod_produto','descricao']] = pia['PRODUTO'].str.split(' ', n=1, expand=True)

pia_unidades = pd.merge(pia,
                        CodProdutoClassificadoNFR[['PRODLIST','Unidade de medida','EmissaoNMCOV_NFR']],
                        how='left',
                        left_on = 'cod_produto',
                        right_on = 'PRODLIST'
                        )

df_limpo = pia_unidades.dropna(subset=['PRODUÇÃO'])

#Lista de valores a serem removidos da coluna 'PRODUÇÃO'
valores_a_remover = ['-', 'X', '...']

#Aplicando o filtro com .isin() e ~
df_limpo = df_limpo[~df_limpo['PRODUÇÃO'].isin(valores_a_remover)]

df_limpo = df_limpo[~df_limpo['EmissaoNMCOV_NFR'].isin(['Não'])]

df_limpo['Unidade de medida'].value_counts()

condicoes = [
    df_limpo['Unidade de medida'] == 'kg',
    df_limpo['Unidade de medida'] == 'mil l'
]

#3. Defina as escolhas correspondentes a cada condição
escolhas = [
    df_limpo['PRODUÇÃO'] / 1000,
    df_limpo['PRODUÇÃO'] * 10
]

#3. Defina as escolhas correspondentes a cada condição
escolhas_unidades = [
    'ton',  #Valor se a primeira condição for Verdadeira
    'hL'    #Valor se a segunda condição for Verdadeira
]

#4. Crie a nova coluna usando np.select()
#O parâmetro 'default' é o valor para o 'else'
df_limpo['PRODUÇÃO_NOVO'] = np.select(condicoes, escolhas, default=df_limpo['PRODUÇÃO'])
df_limpo['UND_NOVO'] = np.select(condicoes, escolhas_unidades, default=df_limpo['Unidade de medida'])

#Base de dados dos fatores de emissão tier 2
eea_ef = pd.read_csv(os.path.join(repo_path, 'inputs','MaterialBaixado', 'EF_tier2.csv'))

df_pia_completo = df_limpo.copy()

#Conexão das bases de dados de apenas os classificados como emissores de NMCOV
df_pia_completo = conecta_ibama_ef(df_limpo,eea_ef,CodProdutoClassificadoNFR)


# Classificar o tipo de bebida
def classificar_produto(codigo):
    if codigo.startswith('Sugar'):
        return 'Açucar','beige'
    elif codigo.startswith('Coffee'):
        return 'Torrefação do café','brown'
    elif codigo.startswith('Margarine'):
        return 'Margarina e gorduras sólidas','yellow'
    elif codigo.startswith('Cakes'):
        return 'Bolos, biscoitos e cereais matinais','grey'
    elif codigo.startswith(('Meat','Animal')):
        return 'Preparação de Carnes e Ração Animal','salmon'
    elif codigo.startswith('Wine'):
        return 'Vinho','purple'
    elif codigo.startswith(('White bread','Wholemeal')):
        return 'Pão','pink'
    elif codigo.startswith('Beer'):
        return 'Cerveja','goldenrod'
    else:
        return 'Destilados','lightblue'

df_pia_completo['tipo_industria_nfr'], df_pia_completo['food_color'] = zip(
    *df_pia_completo['Technology'].map(classificar_produto)
)

df_pia_resumo = df_pia_completo[['ANO','cod_produto','PRODUÇÃO_NOVO','UND_NOVO','Technology','tipo_industria_nfr','food_color']]

#%% analiseDados

# Padronizar unidades no df_pia_resumo
df_pia_resumo['UND_NOVO'] = df_pia_resumo['UND_NOVO'].replace({'t': 'Ton', 'ton': 'Ton', 'TON': 'Ton'})

# Criar um dicionário de produto → unidade
map_unidade = df_pia_resumo.drop_duplicates(subset=['tipo_industria_nfr'])[['tipo_industria_nfr', 'UND_NOVO']].set_index('tipo_industria_nfr')['UND_NOVO'].to_dict()


figpath = os.path.join(repo_path,'figures')

#importar csv com inventário
df_inventario = pd.read_csv(os.path.join(repo_path,'inputs','inventarioEmissoesIndustriaisIndustriaAlimenticiaBR_V3.csv'))
df_inventario['LONGITUDE'] = df_inventario['LONGITUDE'].str.replace(',', '.', regex=False).astype(float)
df_inventario['LATITUDE'] = df_inventario['LATITUDE'].str.replace(',', '.', regex=False).astype(float)

plot_producao_empilhada(
    df_pia_resumo,
    figpath = figpath,
    col_ano="ANO",
    col_valor="PRODUÇÃO_NOVO",
    col_categoria="tipo_industria_nfr",
    col_cor="food_color",
    titulo="Produção Anual de Alimentos"
)

from functions_AnaliseDados import plot_producao_empilhada, plot_mosaico_linhas_dfs, calcular_tabela_bias

plot_mosaico_linhas_dfs(
    df1=df_inventario,
    df2=df_pia_resumo,
    figpath=figpath,
    col_ano1="num_ano",
    col_valor1="prodtonhl_v4",
    col_categoria1="tipo_industria_nfr",
    col_ano2="ANO",
    col_valor2="PRODUÇÃO_NOVO",
    col_categoria2="tipo_industria_nfr",
    titulo="Produção Anual de Alimentos Emissores de NMVOC",
    ncols=3,
    nrows=3,
    figsize=(15, 12),
    map_unidade=map_unidade
)
plot_mosaico_scatter_dfs(
    df1=df_inventario,
    df2=df_pia_resumo,
    figpath=figpath,
    col_ano1="num_ano",
    col_valor1="prodtonhl_v4",
    col_categoria1="tipo_industria_nfr",
    col_ano2="ANO",
    col_valor2="PRODUÇÃO_NOVO",
    col_categoria2="tipo_industria_nfr",
    titulo="Produção Anual de Alimentos Emissores de NMVOC",
    ncols=3,
    nrows=3,
    figsize=(15, 12),
    map_unidade=map_unidade
)


df_bias_long, produtos_ordenados = calcular_tabela_bias(
    df1=df_inventario,
    df2=df_pia_resumo,
    col_ano1="num_ano",
    col_valor1="prodtonhl_v4",
    col_categoria1="tipo_industria_nfr",
    col_ano2="ANO",
    col_valor2="PRODUÇÃO_NOVO",
    col_categoria2="tipo_industria_nfr"
)

df_bias_long.to_excel(r'C:\Users\glima\OneDrive\Documentos\TCC\projeto\outputs\df_bias_produtos.xlsx')

import seaborn as sns
import matplotlib.pyplot as plt
import os
import pandas as pd

# (Assumindo que a função calcular_tabela_bias já foi executada)
# (e que 'df_bias_long' e 'produtos_ordenados' existem)
def plot_mosaico_boxplot_bias(
    df_bias,  # O DataFrame longo (df_bias_long)
    produtos_ordenados,  # A lista de produtos na ordem correta
    figpath,
    titulo="BIAS (Inventário vs PIA-IBGE) por Produto",
    ncols=3, nrows=3, figsize=(14, 12),
    map_unidade=None  # <--- novo parâmetro opcional
):
    """
    Plota um mosaico de boxplots do BIAS para cada produto,
    na mesma ordem do gráfico de correlação, usando cores individuais.
    Adiciona unidade no título, se disponível em map_unidade.
    """
    import os
    import seaborn as sns
    import matplotlib.pyplot as plt

    os.makedirs(figpath, exist_ok=True)
    
    # --- Mapa de cores (fixo, mesmo que antes) ---
    mapa_cores = {
        'Açucar': 'beige',
        'Torrefação do café': 'brown',
        'Margarina e gorduras sólidas': 'yellow',
        'Bolos, biscoitos e cereais matinais': 'grey',
        'Preparação de Carnes e Ração Animal': 'salmon',
        'Vinho': 'purple',
        'Pão': 'pink',
        'Cerveja': 'goldenrod',
        'Destilados': 'lightblue'
    }
    
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=figsize)
    axes = axes.flatten()

    for i, prod in enumerate(produtos_ordenados):
        if i >= nrows * ncols:
            break
        ax = axes[i]

        data_produto = df_bias[df_bias['Produto'] == prod]

        if not data_produto.empty:
            # --- Cor individual ---
            cor_produto = mapa_cores.get(prod, 'grey')
            
            sns.boxplot(y='BIAS', data=data_produto, ax=ax,
                        color=cor_produto, width=0.4)
            
            ax.axhline(y=0, color='red', linestyle='--', linewidth=1.5,
                        label='BIAS Nulo (Perfeito)')

            # --- Título com unidade ---
            unidade = map_unidade.get(prod, '') if map_unidade else ''
            titulo_prod = f"{prod} ({unidade})" if unidade else prod
            ax.set_title(titulo_prod, fontsize=14, weight='bold')

            ax.set_ylabel(None)
            ax.set_xlabel(None)
            ax.tick_params(axis='x', length=0)
            ax.set_xticks([])
            ax.grid(axis='y', linestyle=':', alpha=0.7)
        else:
            unidade = map_unidade.get(prod, '') if map_unidade else ''
            titulo_prod = f"{prod} ({unidade})" if unidade else prod
            ax.set_title(titulo_prod, fontsize=14, weight='bold')
            ax.text(0.5, 0.5, 'Sem dados', ha='center', va='center', transform=ax.transAxes)
            ax.axis('off')

    # Remove eixos vazios
    for j in range(len(produtos_ordenados), nrows * ncols):
        fig.delaxes(axes[j])

    # Rótulo global
    fig.supylabel('BIAS (Inventário - PIA)', fontsize=15, x=0.08)
    
    # Layout e salvamento
    plt.tight_layout(rect=[0.05, 0.0, 1, 0.95], h_pad=0.8, w_pad=0.8)
    plt.savefig(os.path.join(figpath, "mosaico_boxplot_bias_colorido.png"),
                dpi=300, bbox_inches='tight')
    
    print("✅ Mosaico de Boxplot do BIAS (com unidades) gerado com sucesso.")


# --- COMO USAR ---
# (Assumindo que df_bias_long, produtos_ordenados e figpath existem)
plot_mosaico_boxplot_bias(
    df_bias=df_bias_long,
    produtos_ordenados=produtos_ordenados,
    figpath=figpath,
    figsize=(14, 12),
    map_unidade=map_unidade
)