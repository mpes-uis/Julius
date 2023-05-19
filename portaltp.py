#packages

import requests
import pandas as pd
import numpy as np
import json
import sqlite3
import datetime
from datetime import date
import os
import traceback

#read csv prefeituras
prefeiturasURLS=pd.read_csv('prefeituras.csv')
#print(prefeiturasURLS)

#read csv assuntos
assuntos_portaltp=pd.read_csv('assuntos_portaltp.csv')

meses=[
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
]

anos=[
    #2020,
    #2021,
    2022,
    2023
]

def readData_portaltp(url, assunto, ano, mes, prefeitura, readAgain, conn):
    sucess=True
    erro=''
    url_portaltp=url + "json_" + assunto + "?ano=" + str(ano) + "&mes=" + str(mes)
    print(url_portaltp)
    if (readAgain == False): #verifica se a url já foi lida, se sim, não lê novamente
        alreadyRead = pd.read_sql(f"SELECT * FROM leituras WHERE url = '{url_portaltp}'", conn)
        if (not alreadyRead.empty):
            return
    try:
        #read url
        response = requests.get(url_portaltp)
        data=response.text

        #data json
        data_new=data.replace('<string xmlns="http://tempuri.org/">',"")
        data_new=data_new.replace('<?xml version="1.0" encoding="utf-8"?>',"")
        data_new=data_new.replace('</string>',"")

        #data frame
        datanew=pd.read_json(data_new) 
        df=pd.DataFrame(datanew)
        df.insert(0,"prefeitura",prefeitura)
        #print(df)
    
        #verifica se o dado já existe no banco
        try:
            existing_data = pd.read_sql(f"SELECT * FROM {assunto} WHERE prefeitura = '{prefeitura}'", conn)
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

        #Insere novos dados lidos no banco
        new_data.to_sql(assunto, conn, if_exists='append', index=False)

    except Exception as e:
        sucess=False 
        erro=str(e) #mensagem de erro da leitura da url
        print(f"Error reading data from {url_portaltp}")
        readAndSaveUrl(url_portaltp, assunto, prefeitura, ano, mes, sucess, erro, conn) #salva a url e o respectivo erro
        
    readAndSaveUrl(url_portaltp, assunto, prefeitura, ano, mes, sucess, erro, conn) #salva a url lida com sucesso

# Lê e salva url e os respectivos resultados
def readAndSaveUrl(url_portaltp, assunto, prefeitura, ano, mes, sucess, erro, conn):
    colunas = ['url','assunto','prefeitura', 'ano', 'mes', 'sucess','erro','data_leitura']
    leituras = pd.DataFrame(columns=colunas)
    registro={
         "url":url_portaltp,
         "assunto":assunto,
         "prefeitura":prefeitura,
         "ano": ano,
         "mes": mes,
         "sucess": sucess,
         "erro": erro,
         "data_leitura": date.today()
    }
    leituras=leituras.append(registro, ignore_index=True)
    leituras.to_sql("leituras", conn, if_exists='append', index=False)


def readData_portaltp_Total(conn):
    colunas = ['url', 'assunto', 'prefeitura', 'ano', 'mes', 'sucess', 'erro', 'data_leitura']
    leituras = pd.DataFrame(columns=colunas)
    leituras.to_sql('leituras', conn, if_exists='append', index=False)
    for prefeituraId in prefeiturasURLS.index:
        for assuntoid in assuntos_portaltp.index:
            for ano in anos:
                for mes in meses:
                    if mes>= datetime.datetime.now().month and ano>= datetime.datetime.now().year:
                            print("Leitura feita até data atual")
                            break
                    if prefeiturasURLS["empresa"][prefeituraId]=="portaltp":
                        readData_portaltp(prefeiturasURLS["url"][prefeituraId], assuntos_portaltp["assunto"][assuntoid], ano, mes, prefeiturasURLS["prefeitura"][prefeituraId], False, conn)


#def readData_portaltp_Uma_prefeitura(url, nomePrefeitura):
#    for assunto in assuntos:
#        for ano in anos:
#            for mes in meses:
#                #if prefeiturasURLS["empresa"][prefeitura]=="empresa1":
#                readData_portaltp(url, assunto, ano, mes, nomePrefeitura)
    
#readData_portaltp("https://afonsoclaudio-es.portaltp.com.br/api/transparencia.asmx/", "licitacoes", 2016, 1, "Prefeitura de Afonso Cláudio")

#readData_portaltp_Total(prefeiturasURLS.at[0,"url"], assuntos[0], anos[0], meses[0], prefeiturasURLS.at[0,"prefeitura"])
#readData_portaltp_Total()
#print(error_list)
#readData_portaltp_Uma_prefeitura("https://afonsoclaudio-es.portaltp.com.br/api/transparencia.asmx/", "Prefeitura Municipal de Afonso Cláudio")



#def readData_portaltp_Intervalo(url, assunto, ano_inicial, ano_final, mes, prefeitura):
#    for prefeitura in prefeiturasURLS.index:
#        for assunto in assuntos:           
#            while ano_final >= ano_inicial:  
#                for mes in meses:
#                    if prefeiturasURLS["empresa"][prefeitura]=="empresa1":
#                        readData_portaltp(prefeiturasURLS["url"][prefeitura], assunto, ano_inicial, mes, prefeiturasURLS["prefeitura"][prefeitura])
#                ano_inicial += 1


#readData_portaltp_Intervalo(prefeiturasURLS.at[0,"url"], assuntos[0], 2016, 2020, meses[0], prefeiturasURLS.at[0,"prefeitura"])