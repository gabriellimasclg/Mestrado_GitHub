# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 09:09:14 2025

@author: Gabriel
"""

def clean_text(text):
    """
    Limpa uma string: remove acentos, espaços extras e converte para maiúsculo.

    Parâmetros:
        text (str): O texto a ser limpo.

    Retorna:
        str: O texto limpo, ou o valor original se for nulo.
    """
    # Se o valor for nulo (None, NaN), retorna como está para evitar erros.
    if pd.isna(text):
        return text

    # Garante que é string, remove espaços no início/fim e põe em maiúsculo.
    text = str(text).strip().upper()

    # Remove acentuação e caracteres especiais (ex: ç, á -> C, A).
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

    return text
    