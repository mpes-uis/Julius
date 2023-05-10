#Ler API Tectrilha

import traceback
import requests
import pandas as pd
import numpy as np
import json
import sqlite3
from datetime import date
import os

#connect to sqlite
conn_tectrilha=sqlite3.connect('tectrilha.db')
type(conn_tectrilha)
cur = conn_tectrilha.cursor()
type(cur)
print('Database Connected!')

#read csv prefeituras
prefeiturasURLS=pd.read_csv('prefeituras.csv')

#read csv assuntos
assuntos_tectrilha=pd.read_csv('assuntos_tectrilha.csv')

#falta licitações
assuntos=[
    "receitas",
    "pessoal",
    "despesa",
    "diarias",
    "convenios",
    "captacoes",
    "bensmoveis",
    "passagens",
    "bensImoveis",
    "contratos",
]

anos=[
    2018,
    2019,
    2020,
    2021,
    2022,
    2023
]

error_list = []

#Cria a tabela leituras no banco (se for a primeira vez: cria a tabela; se a tabela já existir, não insere nenhuma informação)
colunas = ['url', 'assunto', 'prefeitura', 'ano', 'sucess', 'erro', 'data_leitura']
leituras = pd.DataFrame(columns=colunas)
leituras.to_sql('leituras', conn_tectrilha, if_exists='append', index=False)

def readData_tectrilha(url, assunto, ano, prefeitura, parametro, unidadegestora):
    sucess=True
    erro=''
    parametro=parametro.replace("{unidadeGestoraId}",str(int(unidadegestora)))
    parametro=parametro.replace("{exercicio}",str(ano))
    urlcompleta=url + assunto + parametro
    print(urlcompleta)
    print(url)
    print(assunto)
    print(str(ano))
    print(prefeitura)
    print(parametro)
    print(str(int(unidadegestora)))
    try:
        #read url
        response = requests.get(urlcompleta)
        data=response.text

        #data frame
        datanew=pd.read_json(data) 
        df=pd.DataFrame(datanew)
        df.insert(0,"prefeitura",prefeitura)
        print(df)
        
     #verifica se o dado já existe no banco
        try:
            existing_data = pd.read_sql(f"SELECT * FROM {assunto} WHERE prefeitura = '{prefeitura}'", conn_tectrilha)
        except pd.io.sql.DatabaseError:
            traceback.print_exc()
            existing_data = pd.DataFrame(columns=df.columns)
        
        #Faz a junção dos dataframes com a opção indicator=True
        #print(existing_data)
        new_data = df.merge(existing_data, on=list(existing_data.columns), how='left', indicator=True)

        # Seleciona apenas os registros que estão presentes apenas no segundo dataframe (existing_data)
        new_data = new_data[new_data['_merge'] == 'left_only']

        # Remove a coluna _merge que foi adicionada durante a junção
        new_data = new_data.drop(columns='_merge')

        #Insere dados lidos no banco quando não existe informação, quando ela existe apenas adiciona as novas informações (replace)
        if existing_data.empty:
            new_data.to_sql(assunto, conn_tectrilha, if_exists='replace', index=False)
        else:
            new_data.to_sql(assunto, conn_tectrilha, if_exists='append', index=False)

    except Exception as e:
        sucess=False 
        erro=str(e) #mensagem de erro da leitura da url
        print(f"Error reading data from {urlcompleta}")
        error_list.append(f"Error reading data from {urlcompleta}")
        readAndSaveUrl(urlcompleta, assunto, prefeitura, ano, sucess, erro) #salva a url e o respectivo erro

    readAndSaveUrl(urlcompleta, assunto, prefeitura, ano, sucess, erro) #salva a url lida com sucesso

# Lê e salva url e os respectivos resultados
def readAndSaveUrl(urlcompleta, assunto, prefeitura, ano, sucess, erro):
    colunas = ['url','assunto','prefeitura', 'ano','sucess','erro','data_leitura']
    leituras = pd.DataFrame(columns=colunas)
    registro={
         "url":urlcompleta,
         "assunto":assunto,
         "prefeitura":prefeitura,
         "sucess": sucess,
         "erro": erro,
         "data_leitura": date.today()
    }
    leituras=leituras.append(registro, ignore_index=True)
    leituras.to_sql("leituras", conn_tectrilha, if_exists='append', index=False)



def readData_tectrilha_Total():
    for prefeituraId in prefeiturasURLS.index:
        for assuntoid in assuntos_tectrilha.index:
            for ano in anos:
                    if prefeiturasURLS["empresa"][prefeituraId]=="Tectrilha":
                        readData_tectrilha(prefeiturasURLS["url"][prefeituraId], assuntos_tectrilha["assunto"][assuntoid], ano, prefeiturasURLS["prefeitura"][prefeituraId], assuntos_tectrilha["parametros"][assuntoid], prefeiturasURLS["unidadegestora"][prefeituraId])



#readData_tectrilha_Total()