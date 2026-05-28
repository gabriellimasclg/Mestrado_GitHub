#%%============================ Bibliotecas====================================
import os
import pandas as pd
import numpy as np
from functions_TratDados import tratamento_outliers_v3, sinalizar_variacoes_producao_v2, verif_outliers_manual_v02, sinalizar_variacoes_producao, verif_outliers_manual, tratamento_outliers_V2, CNPJAnalysis, download_ibama_ctf_data, ibama_production_data_v1,ibama_production_data_v2, import_treat_export_food_code, agrupar_e_somar_dados, merge_cnpj_prod, conecta_ibama_ef, tratamento_outliers
import warnings

# Ignora especificamente o FutureWarning de "Downcasting" do pandas
warnings.filterwarnings(
    "ignore", 
    category=FutureWarning, 
    message="Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated"
)
#%%===========================Criação da Base Geral============================

# Caminho da pasta do projeto
# Dentro do repo_path deve-se ter as pastas
# repo_path
#   |--figures
#   |--inputs
#   |--outputs
#       |--log
#   |--scripts

repo_path = os.path.dirname(os.getcwd())

# Faz o downloado da base de dados com CNPJ + Coordenadas
df_ibama_cnpj = download_ibama_ctf_data(repo_path)

# Importa DF com Dados de produção com CNPJ + Código de Produto + Produção
df_ibama_prod_v1 = ibama_production_data_v1(repo_path) 
df_ibama_prod_v2 = ibama_production_data_v2(repo_path) 
df_ibama_prod = pd.concat([df_ibama_prod_v1,df_ibama_prod_v2])
df_ibama_prod = agrupar_e_somar_dados(df_ibama_prod)

# DF com Produção + Código de produto + Coordenadas
df_ibama = merge_cnpj_prod(df_ibama_cnpj,df_ibama_prod) #mesclar p obter coordenadas e código de atividade

#contabiliza quantos são CPF (11 dígitos) e quantos são CNPJ (14 dígitos)
CNPJAnalysis(df_ibama)

#Exporta log com dados desconsiderados
df_ibama.to_excel(os.path.join(repo_path,'outputs','log','log_v01_remocaoCPF.xlsx'))

#Remover CPFs (desconsiderável)
df_ibama = df_ibama[df_ibama['mv.num_cpf_cnpj'].str.len() == 14]


#%%============== Base com NFR + Código do Produto + Produção =================

#Base de dados com todos os Códigos de Produto
cod_produto= import_treat_export_food_code(repo_path)

#### ADICIONAR COLUNA "ALIMENTOS E BEBIDAS"
#vou filtrar os códigos de produto de interesse e exportar (itens que se enquadram no 2.h.2 no nfr)
prefixos = ('10', '11')

cod_produto_interesse = cod_produto[
    cod_produto['PRODLIST'].astype(str).str.startswith(prefixos)
]

#exportei para classificar manualmente
cod_produto_interesse.to_excel(os.path.join(repo_path, 'outputs','CodProdutoParaClassificar.xlsx'), index=False)

#Exportei manualmente para a pasta inputs

#Importei material gerado manualmente
CodProdutoClassificadoNFR = pd.read_excel(os.path.join(repo_path,'inputs','MaterialGeradoManualmente','CodProdutoClassificadoNFR.xlsx'),
                                          dtype={'PRODLIST': str})

#filtrei os alimentos que tem emissões a serem consideradas
CodProdutoClassificadoNFR = CodProdutoClassificadoNFR[CodProdutoClassificadoNFR['EmissaoNMCOV_NFR'] != 'Não']

# Base de dados dos fatores de emissão tier 2
eea_ef = pd.read_csv(os.path.join(repo_path, 'inputs','MaterialBaixado', 'EF_tier2.csv'))

# Conexão das bases de dados de apenas os classificados como emissores de NMCOV
df_ibama_EF = conecta_ibama_ef(df_ibama,eea_ef,CodProdutoClassificadoNFR)

#log que indica os removidos por não serem alimentos emissores de COV
# 1. Crie a coluna com o valor padrão para todas as linhas
df_ibama_EF['status_v02'] = 'Dado filtrado'

# 2. Use .loc para encontrar as linhas que batem com a condição e mude o valor apenas nelas
df_ibama_EF.loc[df_ibama_EF['NFR'] == '2.H.2', 'status_v02'] = 'Alimento emissor de COV - Dado considerado'

#Exporta log com dados desconsiderados
df_ibama_EF.to_excel(os.path.join(repo_path,'outputs','log','log_v02_apenasAlimentosEmissoresMantidos.xlsx'))

#Filtrar apenas os produtos com emissão
df_ibama_EF = df_ibama_EF[df_ibama_EF['Table'].notna()]

#%%====Análise 01: Indústrias que reportaram mais de uma unidade de medida=====

#------------------------------------------------------------------------------
# O objetivo desta seção é encontrar os grupos (CNPJ, Município, Produto)
# que possuem mais de uma unidade de medida declarada ao longo do tempo.
#------------------------------------------------------------------------------

# Para cada combinação única de [CNPJ, MUNICIPIO, cod_produto], calcula-se o total de registros.
# A função 'transform' aplica essa contagem a todas as linhas do grupo original,
# criando a coluna 'contagem_total_grupo' para uso posterior na análise.
df_ibama_EF['contagem_total_grupo'] = df_ibama_EF.groupby(
    ['CNPJ', 'MUNICIPIO', 'cod_produto']
)['cod_produto'].transform('count')

# Agrupa-se novamente e conta-se a frequência de cada 'unidade_medida' dentro do mesmo grupo.
# O resultado é um DataFrame ('df_valid_final') que mostra, por exemplo:
# CNPJ A, Produto X -> 'tonelada': 10 vezes, 'unidade': 2 vezes.
df_valid_final = df_ibama_EF.groupby(
    ['CNPJ', 'MUNICIPIO', 'cod_produto', 'contagem_total_grupo']
)['unidade_medida'].value_counts().reset_index(name='contagem_unidade')

# Um grupo é inconsistente se possuir mais de um tipo de unidade de medida.
# A lógica aqui é identificar no 'df_valid_final' os grupos que aparecem mais de uma vez.
grupos_inconsistentes = df_valid_final[df_valid_final.duplicated(
    subset=['CNPJ', 'MUNICIPIO', 'cod_produto'], keep=False
)]

# Gera uma lista limpa e sem duplicatas dos grupos que precisam ser corrigidos manualmente.
df_para_verificar = grupos_inconsistentes[['CNPJ', 'MUNICIPIO', 'cod_produto']] \
    .drop_duplicates() \
    .reset_index(drop=True)

# Exporta a lista de grupos inconsistentes para um arquivo Excel.
# Este arquivo servirá como um guia para a verificação manual.
df_para_verificar.to_excel(os.path.join(repo_path,
                                        'outputs',
                                        'Auxiliar_dfUnidadesInconsistentes_VerifManual.xlsx'),
                           index=False)

#------------------------------------------------------------------------------
# Nesta etapa, o DataFrame principal é preparado e exportado para que as
# correções sejam feitas manualmente em um software de planilha.
#------------------------------------------------------------------------------

# Adiciona colunas para registrar as alterações, preservando os dados originais.
# 'status_v03': Descreve a ação tomada (ex: 'Unidade alterada: ...').
# 'sig_unidmed_novo' e 'qtd_produzida_novo': Receberão os valores corrigidos.
df_ibama_EF['status_v03'] = 'Unidade mantida conforme dado original'
df_ibama_EF['sig_unidmed_novo'] = df_ibama_EF['sig_unidmed']

# Exporta o DataFrame completo. O usuário deverá abrir este arquivo,
# corrigir os valores nas colunas '_novo' e justificar a alteração em 'status_v03'.
df_ibama_EF.to_excel(os.path.join(repo_path,
                                  'outputs',
                                  'vefirManual_01_unidadesPorProdutoPorProdutor.xlsx'))

#------------------------------------------------------------------------------
# Após a correção manual, o arquivo é reimportado para o script e uma
# cópia é salva como log para garantir a rastreabilidade do processo.
#------------------------------------------------------------------------------

# Lê o arquivo Excel que foi modificado manualmente, contendo as unidades padronizadas.
# Este arquivo deve ser previamente salvo na pasta de inputs manuais.
df_ibama_EF_und = pd.read_excel(os.path.join(repo_path,
                                             'inputs',
                                             'MaterialGeradoManualmente',
                                             'vefirManual_01_unidadesPorProdutoPorProdutor.xlsx'),
                                dtype={'cod_produto': 'string','CNPJ':'string'})

# Salva uma cópia do DataFrame já corrigido no diretório de log.
# Esta ação preserva o estado dos dados após a correção manual
df_ibama_EF_und.to_excel(os.path.join(repo_path,
                                      'outputs',
                                      'log',
                                      'log_v03_AnaliseManualUndInconsistente.xlsx'))

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

#Agrupo em grupos de alimentos emissores de COV para análises posteriores
df_ibama_EF_und['tipo_industria_nfr'], df_ibama_EF_und['food_color'] = zip(
    *df_ibama_EF_und['Technology'].map(classificar_produto)
)


#%%============== Ajuste das unidades para calcular produção ==================

#------------------------------------------------------------------------------
# Será criado uma tabela única com todas as combinações de produto e
# unidade de medida existentes nos dados. Essa tabela será usada para inserir
# manualmente os fatores de conversão necessários para padronizar as unidades.
#------------------------------------------------------------------------------

# Define as colunas que identificam uma combinação única de produto/unidade.
colunas_agrupamento = ['cod_produto', 'nom_produto', 'sig_unidmed_novo', 'Unit']

# Agrupa o DataFrame para encontrar todas as combinações únicas e conta
# quantas vezes cada uma aparece. A coluna 'contagem' serve como
# referência de frequência para a análise e preenchimento manual.
df_unidades_bruto = df_ibama_EF_und.groupby(colunas_agrupamento).size().reset_index(name='contagem')

# Renomeio as colunas para não ter duplicidade nos nomes posteriormente
df_unidades_bruto.rename(columns={"cod_produto": "cod_produto_fc",
                                  "nom_produto": "nom_produto_fc",
                                  "sig_unidmed_novo": "sig_unidmed_fc"})

# Exporta a tabela gerada para um arquivo Excel. Este arquivo deve ser
# preenchido manualmente com os fatores de conversão correspondentes.
df_unidades_bruto.to_excel(os.path.join(repo_path, 'outputs','UnidadesFatorConversãoBruto.xlsx'), index=False)

#------------------------------------------------------------------------------
# Após o preenchimento manual, o arquivo com os fatores de conversão é
# importado das pasta de inputs manuais para ser mesclado com a base de dados
# principal.
#------------------------------------------------------------------------------

# Carrega a planilha Excel que agora contém os fatores de conversão preenchidos.
# 'cod_produto_fc' é lido como string para evitar problemas de formatação.
df_unidades = pd.read_excel(os.path.join(repo_path, 'inputs',
                                         'MaterialGeradoManualmente',
                                         'UnidadesFatorConversão.xlsx'),
                            dtype={'cod_produto_fc': str})

#%%===========Geração do DF de produção bruto com unidades adequadas===========

#------------------------------------------------------------------------------
# Mesclo o DataFrame de produção ('df_ibama_EF_und') com os
# fatores de conversão ('df_unidades') para que cada linha de produção tenha
# seu respectivo fator para a padronização da unidade.
#------------------------------------------------------------------------------

# Realiza a junção (merge) dos DataFrames.
# As chaves 'left_on' e 'right_on' conectam o produto e a unidade de cada tabela.
# O 'how='left'' garante que todos os registros de produção originais sejam mantidos
df_producao_bruto = pd.merge(
        left=df_ibama_EF_und,
        right=df_unidades,
        left_on=['cod_produto', 'sig_unidmed_novo'],
        right_on=['cod_produto_fc', 'sig_unidmed_fc'],
        how='left')

# Multiplica a quantidade produzida original pelo seu fator de conversão correspondente.
df_producao_bruto['prodtonhl_v0'] = (df_producao_bruto['qtd_produzida'].astype(float) * df_producao_bruto['fatorConversao'].astype(float))


#%%==============Análise 02: Alteração da escala de produção =================

#------------------------------------------------------------------------------
# Nessa etapa, buscou-se a correção de escalas de produção, dividindo ou multiplicando
# o valor de produção por múltimos de 10, tornando os valores coerentes. Esta etapa foi
# realizada concomitantemente com a exportação da comparação com o PIA, buscando-se
# dados coerentes com a base de dados indicadora.

# Dessa forma, primeiro exportou-se uma base de dados com a produção classificada por
# ser maior ou menor que o Q90 do grupo, e vou analisar os CNPJs classificados como
# Sim. Em seguida, vou indicadar a escala em que deve-se aumentar ou diminuir o
# Valor para ele se tornar coerente com a escala de produção
#------------------------------------------------------------------------------

# 1. Definir as colunas de agrupamento e a coluna de valor
group_cols = 'tipo_industria_nfr'
value_col = 'prodtonhl_v0'
percentil = 0.90

# 2. Calcular o valor do percentil 95 para cada grupo
# Usamos transform() para que o resultado tenha o mesmo índice do df original
p90_grupo = df_producao_bruto.groupby(group_cols)[value_col].transform(lambda x: x.quantile(percentil))

# 3. Adicionar o valor do p95 ao DataFrame para facilitar a análise no Excel
df_producao_bruto['p90_do_grupo_v0'] = p90_grupo

# 4. Filtrar as linhas onde a produção é maior que o p95 do seu grupo
mascara_outliers_p90 = (df_producao_bruto[value_col] > df_producao_bruto['p90_do_grupo_v0'])

df_producao_bruto['status_v04'] = np.where(mascara_outliers_p90==True,
                                             'produção_superior_q90_v0',
                                             'produção_inferior_q90_v0'
                                             )

df_producao_bruto['status_v05'] = 'Fator de Escala Mantido'
df_producao_bruto['fator_escala'] = 1
df_producao_bruto['prod_temp_testes'] = df_producao_bruto['prodtonhl_v0']

#Caso não seja coerente, vou indicar "Aplicação de favor de escala, e colocar um
# valor múltiplo de 10 para multiplicar (pode ser 1/10 e afins)
# caso valor sejam suspeito de não ser produção ou incoerente, pode-se zerar
# este fator de escala.

# Exportar df_prod_notnull como inventário_intermed para verif manual 2
df_producao_bruto.to_excel(os.path.join(repo_path,'outputs','vefirManual_02_fatorEscala.xlsx'), index=False)

df_outliers_para_verificar =df_producao_bruto['mv.num_cpf_cnpj'][mascara_outliers_p90].copy().drop_duplicates()

# Exportar o resultado para um arquivo Excel
df_outliers_para_verificar.to_excel(os.path.join(repo_path,'outputs','Auxiliar_fatorEscala.xlsx'), index=False)

# Importar valores analisados
df_producao_escala = pd.read_excel(os.path.join(repo_path,'inputs',
                                                'MaterialGeradoManualmente','vefirManual_02_fatorEscala.xlsx'),
                                   dtype={'cod_produto':'string','CNPJ':'string'})

df_producao_escala = df_producao_escala.drop(['prod_temp_testes'], axis=1)

df_producao_escala['CNPJ'] = (
    df_producao_escala['CNPJ']
    .astype(str)
    .str.strip()
    .str.rjust(14, '0')
)

#Calcula nova versão de prodtonhl_v0 --> v1
df_producao_escala['prodtonhl_v1'] = df_producao_escala['prodtonhl_v0'] * df_producao_escala['fator_escala']

#%%=====Análise 03: Remoção de outliers, filtro e preenchimento de série ======


#Como ajustei a classificação dos produtos posteriormente, mas o material
# foi feito de maneira manual, devo arrumar aqui

#Agrupo em grupos de alimentos emissores de COV para análises posteriores
df_producao_escala['tipo_industria_nfr'], df_producao_escala['food_color'] = zip(
    *df_producao_escala['Technology'].map(classificar_produto)
)

df_producao_final = tratamento_outliers_v3(df_producao_escala)

#%%
df_inventario = df_producao_final.copy()
df_inventario['Emissão NMCOV (kg)'] = (df_inventario['prodtonhl_v4'] * df_inventario['Value'].astype(float))
df_inventario['Emissão NMCOV CI_lower (kg)'] = (df_inventario['prodtonhl_v4'] * df_inventario['CI_lower'].astype(float))
df_inventario['Emissão NMCOV CI_upper (kg)'] = (df_inventario['prodtonhl_v4'] * df_inventario['CI_upper'].astype(float))

# Como edgar é em ton, vou deixar tudo em toneladas
df_inventario['Emissão NMCOV (ton)'] = df_inventario['Emissão NMCOV (kg)']/1000
df_inventario['Emissão NMCOV CI_lower (ton)'] = df_inventario['Emissão NMCOV CI_lower (kg)']/1000
df_inventario['Emissão NMCOV CI_upper (ton)'] = df_inventario['Emissão NMCOV CI_upper (kg)']/1000

#%% Exportar para realizar análises em outro código

df_inventario['CNPJ'] = "'" + df_inventario['CNPJ'].astype(str)
df_inventario = df_inventario[df_inventario['prodtonhl_v4'] != 0].copy()

df_inventario.to_csv(os.path.join(repo_path,'outputs','inventarioEmissoesIndustriaisIndustriaAlimenticiaBR_V3.csv'), index = False, encoding='latin1')

'''
df_inventario.columns

df_filtrado = df_inventario[['LATITUDE', 'LONGITUDE','SITUACAO CADASTRAL',
                             'num_ano','sig_unidmed','qtd_produzida','NFR', 'Table',
                             'Pollutant','Value', 'Unit_x', 'CI_lower', 'CI_upper',
                             'status_v02','status_v03', 'sig_unidmed_novo','qtd_produzida_novo',
                             'tipo_industria_nfr','fatorConversao','prodtonhl_v0',
                             'p90_do_grupo_v0', 'status_v04', 'status_v05', 'fator_escala','prodtonhl_v1',
                             'prodtonhl_v2', 'prodtonhl_v3', 'prodtonhl_v4',
                             'status_v06', 'status_v07', 'status_v08','Emissão NMCOV (ton)',
                             'Emissão NMCOV CI_lower (ton)','Emissão NMCOV CI_upper (ton)']]

df_filtrado.to_excel(os.path.join(repo_path,'outputs','InventarioFiltrado.xlsx'),
                   index = False)
'''
#%% 
