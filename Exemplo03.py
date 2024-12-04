import pandas as pd
import polars as pl
from datetime import datetime
import os
import gc
try:
    ENDERECO_DADOS = r'./dados/'

    #hora de inicio
    
    print('Obtendo dados')

    inicio = datetime.now()

    lista_arquivos = []

    # Lista final dos arquivos de dados que vieram do diretorio

    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

    #Pegando os arquivos CSVs do diretorio
    
    for arquivo in lista_dir_arquivos:
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)

    print(lista_arquivos)

#Leitura dos arquivos
    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

    #Leitura de cada um dos dataframes
    df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

    if 'df_bolsa_familia' in locals():
       df_bolsa_familia = pl.concat([df_bolsa_familia, df])
    else:
        df_bolsa_familia =df
    #prints
    print(df_bolsa_familia.head())
    #print(df_bolsa_familia.shape)
    #print(df_bolsa_familia.columns)
    #print(df_bolsa_familia.dtypes)

    print(f'Arquivo {arquivo} procenssado com sucesso!')

    #Criar arquivo Parquet
    df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    #Deletar df_bolsa_familia da memoria
    del df_bolsa_familia
    #Coletar residuo da memoria
    gc.collect()


    
except ImportError as e:
    print('Erro ao obter dados: ', e)