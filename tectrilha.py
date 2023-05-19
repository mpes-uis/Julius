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
#conn=sqlite3.connect('tectrilha.db')
#type(conn)
#cur = conn.cursor()
#type(cur)
#print('Database Connected!')

#read csv prefeituras
prefeiturasURLS=pd.read_csv('prefeituras.csv')

#read csv assuntos
assuntos_tectrilha=pd.read_csv('assuntos_tectrilha.csv')

#read csv meses
#meses=pd.read_csv('meses.csv')

meses=[
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12
]

anos=[
    #2018,
    #2019,
    #2020,
    #2021,
    2022,
    2023
]

#error_list = []

def readData_tectrilha(url, assunto, ano, periodo, prefeitura, parametro, unidadegestora, readAgain, conn):
    sucess=True
    erro=''
    parametro=parametro.replace("{unidadeGestoraId}",str(int(unidadegestora)))
    parametro=parametro.replace("{exercicio}",str(ano))
    parametro=parametro.replace("{periodo}",str(periodo))
    urlcompleta=url + assunto + parametro
    print(urlcompleta)
    #print(urlcompleta)
    #print(url)
    #print(assunto)
    #print(str(ano))
    #print(prefeitura)
    #print(parametro)
    #print(str(int(unidadegestora)))
    if (readAgain == False): #verifica se a url já foi lida, se sim, não lê novamente
        alreadyRead = pd.read_sql(f"SELECT * FROM leituras WHERE url = '{urlcompleta}'", conn)
        if (not alreadyRead.empty):
            return
    try:
        #read url
        response = requests.get(urlcompleta)
        data=response.text

        #data frame
        datanew=pd.read_json(data) 
        df=pd.DataFrame(datanew)
        df.insert(0,"prefeitura",prefeitura)
        #print(df)
        
        #cria dataframe do dado que já existe no banco (da prefeitura e assunto em questão)
        try:
            existing_data = pd.read_sql(f"SELECT * FROM {assunto} WHERE prefeitura = '{prefeitura}'", conn)
        except pd.io.sql.DatabaseError:
            traceback.print_exc()
            existing_data = pd.DataFrame(columns=df.columns)
        
        #Faz a junção dos dataframes com a opção indicator=True
        new_data = df.merge(existing_data, on=list(existing_data.columns), how='left', indicator=True)

        # Seleciona apenas os registros que estão presentes apenas no segundo dataframe (existing_data)
        new_data = new_data[new_data['_merge'] == 'left_only']

        # Remove a coluna _merge que foi adicionada durante a junção
        new_data = new_data.drop(columns='_merge')

        #Insere novos dados da api no banco;
        #quando não existe a tabela, ela é criada automaticamente
        #se os dados já estão presentes no banco, uma tabela vazia será adicionada (nada muda)
        new_data.to_sql(assunto, conn, if_exists='append', index=False)
        
        readAndSaveUrl(urlcompleta, assunto, prefeitura, ano, periodo, sucess, erro, conn) #salva a url lida com sucesso
    except Exception as e:
        sucess=False 
        erro=str(e) #mensagem de erro da leitura da url
        print(f"Error reading data from {urlcompleta}")
        #error_list.append(f"Error reading data from {urlcompleta}")
        readAndSaveUrl(urlcompleta, assunto, prefeitura, ano, periodo, sucess, erro, conn) #salva a url e o respectivo erro

    
# Lê e salva url e os respectivos resultados
def readAndSaveUrl(urlcompleta, assunto, prefeitura, ano, periodo, sucess, erro, conn):
    colunas = ['url','assunto','prefeitura', 'ano', 'periodo', 'sucess','erro','data_leitura']
    leituras = pd.DataFrame(columns=colunas)
    registro={
         "url":urlcompleta,
         "assunto":assunto,
         "prefeitura":prefeitura,
         "ano": ano,
         "periodo": periodo,
         "sucess": sucess,
         "erro": erro,
         "data_leitura": date.today()
    }
    leituras=leituras.append(registro, ignore_index=True)
    leituras.to_sql("leituras", conn, if_exists='append', index=False)


def readData_tectrilha_Total(conn):
    colunas = ['url', 'assunto', 'prefeitura', 'ano', 'periodo', 'sucess', 'erro', 'data_leitura']
    leituras = pd.DataFrame(columns=colunas)
    leituras.to_sql('leituras', conn, if_exists='append', index=False)
    for prefeituraId in prefeiturasURLS.index:
        for assuntoid in assuntos_tectrilha.index:
            if assuntos_tectrilha["assunto"][assuntoid]=="pessoal":
                for ano in anos:
                    for mes in meses:
                        if prefeiturasURLS["empresa"][prefeituraId]=="Tectrilha":
                           readData_tectrilha(prefeiturasURLS["url"][prefeituraId], assuntos_tectrilha["assunto"][assuntoid], ano, mes, prefeiturasURLS["prefeitura"][prefeituraId], assuntos_tectrilha["parametros"][assuntoid], prefeiturasURLS["unidadegestora"][prefeituraId], False, conn)
            if assuntos_tectrilha["assunto"][assuntoid] in ["despesa", "captacoes", "bensmoveis","receitas", "diarias", "convenios", "passagens", "contratos"]:
                for ano in anos:
                    if prefeiturasURLS["empresa"][prefeituraId]=="Tectrilha":
                        readData_tectrilha(prefeiturasURLS["url"][prefeituraId], assuntos_tectrilha["assunto"][assuntoid], ano, 0, prefeiturasURLS["prefeitura"][prefeituraId], assuntos_tectrilha["parametros"][assuntoid], prefeiturasURLS["unidadegestora"][prefeituraId], False, conn)
            if assuntos_tectrilha["assunto"][assuntoid]=="bensimoveis":
                if prefeiturasURLS["empresa"][prefeituraId]=="Tectrilha":
                    readData_tectrilha(prefeiturasURLS["url"][prefeituraId], assuntos_tectrilha["assunto"][assuntoid], 0, 0, prefeiturasURLS["prefeitura"][prefeituraId], assuntos_tectrilha["parametros"][assuntoid], prefeiturasURLS["unidadegestora"][prefeituraId], False, conn)


#readData_tectrilha_Total(conn)