# -*- coding: utf-8 -*-
"""
Created on Sun Apr 13 11:32:03 2025

@author: Gabriel
"""
import urllib3
import requests
import os
import pandas as pd
from clean_text import clean_text # Importa a função de limpeza de um arquivo separado

# Desabilita avisos de requisições HTTPS sem verificação de certificado
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_ibama_ctf_data(repo_path):
    '''
    Automatiza o processo de download, processamento e consolidação de dados de
    Pessoas Jurídicas do Cadastro Técnico Federal (CTF) do IBAMA.

    O fluxo de trabalho da função é o seguinte:
    1. Cria as estruturas de pastas necessárias ('inputs/dadosIbamaCNPJ_Download'
       para arquivos brutos e 'inputs/dadosIbamaCNPJ_Consolidado' para o final).
    2. Pergunta ao usuário se os arquivos já foram baixados para pular a etapa de download.
    3. Se o download for necessário, itera por todas as Unidades da Federação (UFs),
       baixa o arquivo CSV correspondente e o salva na pasta de dados brutos.
    4. Cada arquivo baixado é processado individualmente para padronizar CNPJs e
       nomes de colunas.
    5. Ao final, todos os dados processados são consolidados em um único DataFrame.
    6. Colunas de texto ('Municipio', 'Razao Social') passam por uma limpeza final.
    7. O DataFrame consolidado é salvo como um único arquivo CSV na pasta de
       dados processados.

    Parâmetros:
        repo_path (str): Caminho para a pasta raiz do projeto. As subpastas de
                         dados serão criadas dentro deste caminho.

    Retorna:
        pandas.DataFrame: Um DataFrame contendo todos os dados consolidados e limpos.
        None: Retorna None se o processo falhar ou se nenhum arquivo for processado.
    '''
    
    # Define os caminhos para as pastas de dados brutos e processados
    raw_dir = os.path.join(repo_path, 'inputs', 'MaterialBaixado','PJ_UF')
    processed_dir = os.path.join(repo_path, 'inputs', 'MaterialBaixado')
    
    # Garante que as pastas de destino existam; se não, elas são criadas
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    # Interage com o usuário para saber se a etapa de download pode ser pulada
    start = input('Você já tem os arquivos baixados? (s/n) ')
    
    # Se o usuário responder 's', o script carrega o arquivo já consolidado
    if start.lower() == 's':
        
        # Caminho para o arquivo final que deveria existir
        final_file_path = os.path.join(processed_dir, "PJ_BR.csv")
        print(f"Carregando arquivo consolidado de: {final_file_path}")
        
        # Lê o CSV consolidado, garantindo a tipagem correta de colunas-chave
        df_final = pd.read_csv(final_file_path,
                               dtype={'CNPJ': str,
                                      'Codigo da atividade': str,
                                      'Codigo da categoria': str},
                               keep_default_na=False)
               
        return df_final
    
    else:
        # Bloco de execução para o download e processamento dos dados
        print("Iniciando o download e processamento dos dados...")
        base_url = "http://dadosabertos.ibama.gov.br/dados/CTF/APP/"
        
        # Lista de todas as Unidades da Federação do Brasil
        ufs = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO',
            'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI',
            'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        
        # Lista vazia para armazenar os DataFrames de cada estado após o processamento
        pessoas_juridicas_br = []
        
        # Itera sobre cada UF para baixar e processar os dados
        for uf in ufs:
            try:
                # Constrói a URL completa para o arquivo CSV do estado atual
                url = f"{base_url}{uf}/pessoasJuridicas.csv"
                print(f"Baixando {uf}...")
                
                # Faz a requisição GET para baixar o arquivo
                # timeout=30: define um tempo limite de 30 segundos
                # verify=False: ignora erros de certificado SSL (usar com cautela)
                response = requests.get(url, verify=False, timeout=30) 
                
                # Verifica se a requisição foi bem-sucedida (status code 2xx)
                response.raise_for_status() 
                
                # Define o caminho completo para salvar o arquivo bruto baixado
                output_path = os.path.join(raw_dir, f"pessoasJuridicas_{uf}.csv")
                
                # Abre o arquivo em modo de escrita binária ('wb') e salva o conteúdo
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                
                # Inicia o processamento do arquivo recém-baixado
                try:
                    # Carrega o arquivo CSV em um DataFrame, especificando o delimitador e os tipos de dados
                    df = pd.read_csv(output_path,
                                     delimiter=';',
                                     dtype={'CNPJ': str,
                                            'Codigo da atividade': str,
                                            'Codigo da categoria': str},
                                     keep_default_na=False)
                    
                    # Padroniza a coluna de CNPJ para ter sempre 14 dígitos, com zeros à esquerda
                    df['CNPJ'] = df['CNPJ'].str.zfill(14) 
                    
                    # Limpa e padroniza os nomes das colunas usando a função externa clean_text
                    df.columns = df.columns.map(clean_text)
                    
                    # Adiciona o DataFrame do estado, já limpo, à lista de consolidação
                    pessoas_juridicas_br.append(df)
                    
                # Captura e informa erros que possam ocorrer durante o processamento do arquivo
                except Exception as e:
                    print(f"Erro ao processar {uf}: {e}")
                    
            # Captura e informa erros relacionados ao download (ex: rede, URL inválida)
            except requests.exceptions.RequestException as e:
                print(f"Erro ao baixar {uf}: {e}")
        
        # Após o loop, verifica se algum dado foi processado antes de consolidar
        if pessoas_juridicas_br:
            print("\nConsolidando todos os dados...")
            # Junta todos os DataFrames da lista em um único DataFrame
            df_final = pd.concat(pessoas_juridicas_br, ignore_index=True)
            
            # Aplica a função de limpeza nas células das colunas 'Municipio' e 'Razao Social'
            df_final['MUNICIPIO'] = df_final['MUNICIPIO'].apply(clean_text)
            df_final['RAZAO SOCIAL'] = df_final['RAZAO SOCIAL'].apply(clean_text)
            df_final['MUNICIPIO'] = df_final['MUNICIPIO'].str.replace(
                r"TRAJANO DE MORAIS", "TRAJANO DE MORAES", regex=True
                )
            
            # Extrair os anos de inicio e fim das datas
            # ANO_INICIO
            df_final['ANO_INICIO'] = (
                df_final['DATA DE INICIO DA ATIVIDADE']
                .fillna('0000')                    # trata valores nulos
                .replace('', '0000')               # trata strings vazias
                .str[-4:]                          # pega os 4 últimos caracteres
                .astype(int)                       # converte para inteiro
            )
            
            # ANO_FIM
            df_final['ANO_FIM'] = (
                df_final['DATA DE TERMINO DA ATIVIDADE']
                .fillna('0000')                    # trata valores nulos
                .replace('', '0000')               # trata strings vazias
                .str[-4:]
                .astype(int)
            )
            #Onde a data de inicio e fim de atividade for vazio, inserir np.nan
   
            # Define o caminho e nome do arquivo final e o salva em formato CSV
            output_file = os.path.join(processed_dir, "PJ_BR.csv")
            df_final.to_csv(output_file, index=False, encoding='utf-8')
            print("Dados baixados e processados com sucesso!")
            
            # Retorna o DataFrame final
            return df_final
            
    # Se a lista 'pessoas_juridicas_br' estiver vazia, retorna None
    print("Nenhum dado foi processado.")
    return None