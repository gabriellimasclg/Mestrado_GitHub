# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 08:25:13 2025

@author: Gabriel
"""

import numpy as np

def CNPJAnalysis(df, cnpj_column='mv.num_cpf_cnpj'):
    """
    Analyzes CNPJ/CPF data in a DataFrame, classifying documents and counting digit lengths.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing the documents to analyze
        cnpj_column (str): Name of the column containing CPF/CNPJ numbers (default: 'mv.num_cpf_cnpj')
    
    Returns:
        dict: A dictionary with the count of documents by digit length
        pd.DataFrame: The original DataFrame with added 'tipo' column (CNPJ/CPF/outro)
    """
    
    # 1. Classify documents by length
    df['tipo'] = np.where(
        df[cnpj_column].str.len() == 14, 'CNPJ',
        np.where(
            df[cnpj_column].str.len() == 11, 'CPF',
            'outro'
        )
    )
    
    # 2. Initialize counter for digit lengths (4-14)
    contagem = {length: 0 for length in range(4, 15)}
    total_documents = len(df)
    
    # 3. Count documents by cleaned digit length
    for doc in df[cnpj_column]:
        # Remove all non-digit characters
        cleaned_doc = ''.join(filter(str.isdigit, str(doc)))
        length = len(cleaned_doc)
        
        if length in contagem:
            contagem[length] += 1
    
    # 4. Verification and reporting
    total_counted = sum(contagem.values())
    
    if total_counted == total_documents:
        print("✅ Todos os documentos foram contabilizados!")
    else:
        print(f"⚠️ Atenção: {total_documents - total_counted} documentos não se enquadram nos tamanhos 4-14 dígitos")
    
    # 5. Detailed report
    print("\nDistribuição de dígitos:")
    for length, quantity in sorted(contagem.items()):
        if quantity > 0:  # Only show lengths that actually appear
            print(f'{quantity:>5} documentos com {length:>2} dígitos ({quantity/total_documents:.1%})')
    
    print(f"\nTotal contado: {total_counted} de {total_documents} documentos\nApenas serão considerados documentos com 14 dígitos (CNPJs)")
    
    return