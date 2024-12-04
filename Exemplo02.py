import pandas as pd
import polars as pl
from datetime import datetime
import gc
try:
    ENDERECO_DADOS = r'./dados/'

    #hora de inicio
    
    print('Obtendo dados')

    inicio = datetime.now()

    lista_arquivos = ['202401_NovoBolsaFamilia.csv', '202402_NovoBolsaFamilia.csv']
    
    df_janeiro = pl.read_csv(ENDERECO_DADOS + '202401_NovoBolsaFamilia.csv', separator=';', encoding='iso-8859-1')

    #print(lista_arquivos)
    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')
    
    df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')
     
    if 'df_bolsa_familia' in locals():
       df_bolsa_familia = pl.concat([df_bolsa_familia, df])
    else:
        df_bolsa_familia =df

    #prints
    print(df.head())
    #print(df.shape)
    #print(df.columns)
    #print(df.dtypes)

    #limpar df da memoria
    del df
    #coletar residuos da memoria
    gc. collect()

    hora_impressao = datetime.now()

    print(f'Tempo de execução: {hora_impressao - inicio}')

#Tempo de execução com pandas
#Tempo de execução: 0:01:03.674757
#Tempo de execução com polars
#Tempo de execução: 0:00:44.334474

except ImportError as e:
    print('Erro ao obter dados: ', e)