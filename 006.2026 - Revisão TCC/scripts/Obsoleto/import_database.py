# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 09:01:35 2025

@author: Gabriel
"""
import os
import pandas as pd
from clean_text import clean_text

def ibama_production_data_v1(repo_path):
    """
    Processa dados de produção do IBAMA a partir de arquivos XLSX, limpando e
    consolidando os dados.
    V1 - Base de dados de 2017 até 2023
    Vou pegar apenas até 2020 e mesclar com o outro
    
    Parâmetros:
        repo_path (str): Caminho raiz do repositório onde estão as pastas inputs
        /outputs
    
    Retorna:
        pd.DataFrame: DataFrame com os dados processados
        str: Caminho do arquivo CSV salvo
    """

    # Define os caminhos para as pastas de dados brutos e processados
    raw_dir = os.path.join(repo_path, 'inputs','DadosProduçãoIndustrial')
    processed_dir = os.path.join(repo_path, 'inputs','DadosProduçãoIndustrial')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    try:
        # Caminho completo do arquivo
        file_path = os.path.join(raw_dir, 'DadosProduçãoBruto.xlsx')
        
        # Lê as duas abas do Excel
        aba1 = pd.read_excel(file_path, sheet_name=0, dtype={'mv.num_cpf_cnpj': str}) # 0 para a primeira aba
        aba2 = pd.read_excel(file_path, sheet_name=1, dtype={'mv.num_cpf_cnpj': str}) # 1 para segunda aba
                
        # Combina as abas, deleta as linhas duplicadas
        df_ibama_prod = pd.concat([aba1, aba2], ignore_index=True).drop_duplicates()
        
        # Padronização dos dados com a função clean_text
        df_ibama_clean = df_ibama_prod.copy()
        df_ibama_clean['mv.nom_municipio'] = df_ibama_clean['mv.nom_municipio'].apply(clean_text)
        df_ibama_clean['mv.nom_pessoa'] = df_ibama_clean['mv.nom_pessoa'].apply(clean_text)
        df_ibama_clean['mv.nom_municipio'] = df_ibama_clean['mv.nom_municipio'].str.replace(
            r"SANT'? ?ANA DO LIVRAMENTO", "SANTANA DO LIVRAMENTO", regex=True
            )
        df_ibama_clean['mv.nom_municipio'] = df_ibama_clean['mv.nom_municipio'].str.replace(
            r"PRESIDENTE CASTELLO BRANCO", "PRESIDENTE CASTELO BRANCO", regex=True
            )
        
        #vou pegar apenas os anos de 17 até 20
        df_ibama_clean_17_20 = df_ibama_clean.loc[df_ibama_clean['num_ano']<2021] 
        
        # Salva o resultado
        output_file = os.path.join(processed_dir, 'DadosProduçãoTratadoV1.csv')
        df_ibama_clean_17_20.to_csv(output_file, index=False, encoding='utf-8') #Remove índices
        
        return df_ibama_clean_17_20
        
    # Caso dê erro, informa ao usuário
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo RAPP.xlsx não encontrado em {raw_dir}")
    except Exception as e:
        raise Exception(f"Erro ao processar dados: {str(e)}")
        
        
def ibama_production_data_v2(repo_path):
    """
    Processa dados de produção do IBAMA a partir de arquivos XLSX, limpando e
    consolidando os dados.
    V2 - Base de dados de 2021 até 2024 (+1 ano em relação ao v1, mas começa depois)
    vou pegar inteiro p mesclar com o outro
    
    Parâmetros:
        repo_path (str): Caminho raiz do repositório onde estão as pastas inputs
        /outputs
    
    Retorna:
        pd.DataFrame: DataFrame com os dados processados
        str: Caminho do arquivo CSV salvo
    """

    # Define os caminhos para as pastas de dados brutos e processados
    raw_dir = os.path.join(repo_path, 'inputs','DadosProduçãoIndustrial')
    processed_dir = os.path.join(repo_path, 'inputs','DadosProduçãoIndustrial')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    try:
        # Caminho completo do arquivo
        file_path = os.path.join(raw_dir, 'DadosProduçãoBrutoV2.xlsx')
        
        # Lê as duas abas do Excel
        df_ibama_prod = pd.read_excel(file_path, sheet_name=0, dtype={'mv.num_cpf_cnpj': str}) # 0 para a primeira aba
                
        # Padronização dos dados com a função clean_text
        df_ibama_clean_21_24 = df_ibama_prod.copy()
        df_ibama_clean_21_24['mv.nom_municipio'] = df_ibama_clean_21_24['mv.nom_municipio'].apply(clean_text)
        df_ibama_clean_21_24['mv.nom_pessoa'] = df_ibama_clean_21_24['mv.nom_pessoa'].apply(clean_text)
        df_ibama_clean_21_24['mv.nom_municipio'] = df_ibama_clean_21_24['mv.nom_municipio'].str.replace(
            r"SANT'? ?ANA DO LIVRAMENTO", "SANTANA DO LIVRAMENTO", regex=True
            )
        df_ibama_clean_21_24['mv.nom_municipio'] = df_ibama_clean_21_24['mv.nom_municipio'].str.replace(
            r"PRESIDENTE CASTELLO BRANCO", "PRESIDENTE CASTELO BRANCO", regex=True
            )

        # Salva o resultado
        output_file = os.path.join(processed_dir, 'DadosProduçãoTratadoV2.csv')
        df_ibama_clean_21_24.to_csv(output_file, index=False, encoding='utf-8') #Remove índices
        
        return df_ibama_clean_21_24
        
    # Caso dê erro, informa ao usuário
    except FileNotFoundError:
        raise FileNotFoundError(f"Arquivo RAPP.xlsx não encontrado em {raw_dir}")
    except Exception as e:
        raise Exception(f"Erro ao processar dados: {str(e)}")
        
def import_products_code(repo_path):
    '''
    Importa, limpa e exporta a tabela de códigos de produtos do IBGE (PRODLIST).
    
    Verificar online: https://servicos.ibama.gov.br/ctfcd/manual/html/lista_produtos.htm
    Baixar excel: https://www.ibge.gov.br/estatisticas/metodos-e-classificacoes/classificacoes-e-listas-estatisticas/9153-lista-de-produtos-da-industria.html
   
    A função lê um arquivo Excel, remove cabeçalhos repetidos e linhas
    indesejadas, e retorna um DataFrame pronto para uso.

    Parâmetros:
        repo_path (str): Caminho para a pasta raiz do projeto.

    Retorna:
        pd.DataFrame: Tabela de códigos de produtos limpa.   
    '''
    raw_dir = os.path.join(repo_path, 'inputs','MaterialBaixado')
    processed_dir = os.path.join(repo_path, 'outputs')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    # Caminho do arquivo
    xlsx_path = os.path.join(raw_dir,'CódigosProdutosIBGE.xlsx')

    # Lê o CSV com header na linha 1 (índice 1)
    cod_produto = pd.read_excel(xlsx_path, header=2, dtype={'PRODLIST': str})
    # Remove linhas com 'PRODLIST' ou NaN na coluna 'PRODLIST'
    cod_produto = cod_produto[~cod_produto['PRODLIST'].isin(['PRODLIST'])]
    cod_produto = cod_produto[~cod_produto['PRODLIST'].astype(str).str.startswith('CNAE')]
    cod_produto = cod_produto.dropna(subset=['PRODLIST'])

    # Reinicia o índice
    cod_produto.reset_index(drop=True, inplace=True)
    
    output_file = os.path.join(processed_dir,'CodProdutoParaClassificar.xlsx')
    cod_produto.to_excel(output_file, index=False)
    
    return cod_produto

def import_treat_export_food_code(repo_path):
    '''
    Importa e limpa a tabela de códigos de produtos do IBGE (PRODLIST)
    APENAS PRODUTOS ALIMENTÍCIOS (escopo inicial do TCC).
    
    Verificar online: https://servicos.ibama.gov.br/ctfcd/manual/html/lista_produtos.htm
    Baixar excel: https://www.ibge.gov.br/estatisticas/metodos-e-classificacoes/classificacoes-e-listas-estatisticas/9153-lista-de-produtos-da-industria.html
   
    A função lê um arquivo Excel, remove cabeçalhos repetidos e linhas
    indesejadas, e retorna um DataFrame pronto para uso.

    Parâmetros:
        repo_path (str): Caminho para a pasta raiz do projeto.

    Retorna:
        pd.DataFrame: Tabela de códigos de produtos limpa.   
    '''
    raw_dir = os.path.join(repo_path, 'inputs','MaterialBaixado')
    processed_dir = os.path.join(repo_path, 'outputs')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    # Caminho do arquivo
    xlsx_path = os.path.join(raw_dir,'CódigosProdutosIBGE.xlsx')

    # Lê o CSV com header na linha 1 (índice 1)
    cod_produto = pd.read_excel(xlsx_path, header=2, dtype={'PRODLIST': str})
    # Remove linhas com 'PRODLIST' ou NaN na coluna 'PRODLIST'
    cod_produto = cod_produto[~cod_produto['PRODLIST'].isin(['PRODLIST'])]
    cod_produto = cod_produto[~cod_produto['PRODLIST'].astype(str).str.startswith('CNAE')]
    cod_produto = cod_produto.dropna(subset=['PRODLIST'])

    # Reinicia o índice
    cod_produto.reset_index(drop=True, inplace=True)
       
    return cod_produto
