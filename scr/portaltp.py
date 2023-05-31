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
prefeiturasURLS=pd.read_csv('data/prefeituras.csv')
#print(prefeiturasURLS)

#read csv assuntos
assuntos_portaltp=pd.read_csv('data/assuntos_portaltp.csv')

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
    sucesso=True
    erro=''
    url_portaltp=url + "json_" + assunto + "?ano=" + str(ano) + "&mes=" + str(mes)
    print(url_portaltp)
    if (readAgain == False): #verifica se a url já foi lida, se sim, não lê novamente
        alreadyRead = pd.read_sql(f"SELECT * FROM leituras WHERE url = '{url_portaltp}'", conn)
        if (not alreadyRead.empty):
            return
    try:
        #read url
        response = requests.get(url_portaltp, timeout=10)
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
        sucesso=False 
        erro=str(e) #mensagem de erro da leitura da url
        print(f"Error reading data from {url_portaltp}")
        readAndSaveUrl(url_portaltp, assunto, prefeitura, ano, mes, sucesso, erro, conn) #salva a url e o respectivo erro
        
    readAndSaveUrl(url_portaltp, assunto, prefeitura, ano, mes, sucesso, erro, conn) #salva a url lida com sucesso

# Lê e salva url e os respectivos resultados
def readAndSaveUrl(url_portaltp, assunto, prefeitura, ano, mes, sucesso, erro, conn):
    #print(erro)
    colunas = ['url','assunto','prefeitura', 'ano', 'mes', 'sucesso','erro','dataPrimeiraLeitura', 'dataUltimaAtualizacao']
    leituras = pd.DataFrame(columns=colunas)
    dataHoraAtual = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    registro={
         "url":url_portaltp,
         "assunto":assunto,
         "prefeitura":prefeitura,
         "ano": ano,
         "mes": mes,
         "sucesso": sucesso,
         "erro": erro,
         "dataPrimeiraLeitura": dataHoraAtual,
         "dataUltimaAtualizacao": dataHoraAtual
    }
    registroNoBanco = pd.read_sql(f"SELECT * FROM leituras WHERE url='{url_portaltp}'", conn)
    #print(registroNoBanco)
    cur=conn.cursor()
    if (not registroNoBanco["url"].empty):
        cur.execute(f"UPDATE leituras SET sucesso={sucesso}, erro='{erro}', dataUltimaAtualizacao='{dataHoraAtual}' WHERE url = '{url_portaltp}'")
        conn.commit()
    else:      
        leituras = pd.concat([leituras, pd.DataFrame([registro], columns=colunas)], ignore_index=True)
        leituras.to_sql("leituras", conn, if_exists='append', index=False)
    #teste=pd.read_sql(f"SELECT * FROM leituras WHERE url='{url_portaltp}'", conn)
    #print(teste)


def readData_portaltp_Total(conn):
    colunas = ['url', 'assunto', 'prefeitura', 'ano', 'mes', 'sucesso', 'erro','dataPrimeiraLeitura', 'dataUltimaAtualizacao']
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


#Função que lê urls a partir de um vetor dado (útil quando é necessário reler urls que deram erro)
def readData_Portaltp_ComErro(conn):
    urlsComErro = pd.read_sql(f"SELECT * FROM leituras WHERE sucesso=0", conn)
    #print(urlscomerro)
    for urlComErroId in urlsComErro.index:
        assunto = urlsComErro["assunto"][urlComErroId]
        prefeitura = urlsComErro["prefeitura"][urlComErroId]
        ano = urlsComErro["ano"][urlComErroId]
        mes = urlsComErro["mes"][urlComErroId]
        urlTotal = urlsComErro["url"][urlComErroId]
        url = urlTotal.split("json_")[0]
        print(url)
        readData_portaltp(url, assunto, ano, mes, prefeitura, True, conn)
