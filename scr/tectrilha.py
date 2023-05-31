#Ler API Tectrilha

import traceback
import requests
import pandas as pd
import numpy as np
import json
import sqlite3
import datetime
from datetime import date
import os

#read csv prefeituras
prefeiturasURLS=pd.read_csv('data/prefeituras.csv')

#read csv assuntos
assuntos_tectrilha=pd.read_csv('data/assuntos_tectrilha.csv')

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

def readData_tectrilha(url, assunto, ano, periodo, prefeitura, parametro, unidadegestora, readAgain, conn):
    sucesso=True
    erro=''
    parametro=parametro.replace("{unidadeGestoraId}",str(int(unidadegestora)))
    parametro=parametro.replace("{exercicio}",str(ano))
    parametro=parametro.replace("{periodo}",str(periodo))
    url_tectrilha=url + assunto + parametro
    print(url_tectrilha)
    if (readAgain == False): #verifica se a url já foi lida, se sim, não lê novamente
        alreadyRead = pd.read_sql(f"SELECT * FROM leituras WHERE url = '{url_tectrilha}'", conn)
        if (not alreadyRead.empty):
            return
    try:
        #read url
        response = requests.get(url_tectrilha)
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
        
        readAndSaveUrl(url_tectrilha, assunto, prefeitura, ano, periodo, sucesso, erro, conn) #salva a url lida com sucesso
    except Exception as e:
        sucesso=False 
        erro=str(e) #mensagem de erro da leitura da url
        print(f"Error reading data from {url_tectrilha}")
        readAndSaveUrl(url_tectrilha, assunto, prefeitura, ano, periodo, sucesso, erro, conn) #salva a url e o respectivo erro

# Lê e salva url e os respectivos resultados
def readAndSaveUrl(url_tectrilha, assunto, prefeitura, ano, periodo, sucesso, erro, conn):
    colunas = ['url','assunto','prefeitura', 'ano', 'periodo', 'sucesso','erro','dataPrimeiraLeitura', 'dataUltimaAtualizacao']
    leituras = pd.DataFrame(columns=colunas)
    dataHoraAtual = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    registro={
         "url":url_tectrilha,
         "assunto":assunto,
         "prefeitura":prefeitura,
         "ano": ano,
         "periodo": periodo,
         "sucesso": sucesso,
         "erro": erro,
         "dataPrimeiraLeitura": dataHoraAtual,
         "dataUltimaAtualizacao": dataHoraAtual
    }
    registroNoBanco = pd.read_sql(f"SELECT * FROM leituras WHERE url='{url_tectrilha}'", conn)
    #print(registroNoBanco)
    cur=conn.cursor()
    if (not registroNoBanco["url"].empty):
        cur.execute(f"UPDATE leituras SET sucesso={sucesso}, erro='{erro}', dataUltimaAtualizacao='{dataHoraAtual}' WHERE url = '{url_tectrilha}'")
        conn.commit()
    else:      
        leituras = pd.concat([leituras, pd.DataFrame([registro], columns=colunas)], ignore_index=True)
        leituras.to_sql("leituras", conn, if_exists='append', index=False)

def readData_tectrilha_Total(conn):
    colunas = ['url', 'assunto', 'prefeitura', 'ano', 'periodo', 'sucesso', 'erro','dataPrimeiraLeitura', 'dataUltimaAtualizacao']
    leituras = pd.DataFrame(columns=colunas)
    leituras.to_sql('leituras', conn, if_exists='append', index=False)
    for prefeituraId in prefeiturasURLS.index:
        for assuntoid in assuntos_tectrilha.index:
            if assuntos_tectrilha["assunto"][assuntoid] in ["despesa", "captacoes", "bensmoveis","receitas", "diarias", "convenios", "passagens", "contratos"]:
                for ano in anos:
                    if prefeiturasURLS["empresa"][prefeituraId]=="Tectrilha":
                        readData_tectrilha(prefeiturasURLS["url"][prefeituraId], assuntos_tectrilha["assunto"][assuntoid], ano, 0, prefeiturasURLS["prefeitura"][prefeituraId], assuntos_tectrilha["parametros"][assuntoid], prefeiturasURLS["unidadegestora"][prefeituraId], False, conn)
            if assuntos_tectrilha["assunto"][assuntoid]=="bensimoveis":
                if prefeiturasURLS["empresa"][prefeituraId]=="Tectrilha":
                    readData_tectrilha(prefeiturasURLS["url"][prefeituraId], assuntos_tectrilha["assunto"][assuntoid], 0, 0, prefeiturasURLS["prefeitura"][prefeituraId], assuntos_tectrilha["parametros"][assuntoid], prefeiturasURLS["unidadegestora"][prefeituraId], False, conn)
            if assuntos_tectrilha["assunto"][assuntoid]=="pessoal":
                for ano in anos:
                    for mes in meses:
                        if mes>= datetime.datetime.now().month and ano>= datetime.datetime.now().year:
                            print("Leitura feita até data atual")
                            break
                        if prefeiturasURLS["empresa"][prefeituraId]=="Tectrilha":
                           readData_tectrilha(prefeiturasURLS["url"][prefeituraId], assuntos_tectrilha["assunto"][assuntoid], ano, mes, prefeiturasURLS["prefeitura"][prefeituraId], assuntos_tectrilha["parametros"][assuntoid], prefeiturasURLS["unidadegestora"][prefeituraId], False, conn)

#Função que lê urls a partir de um vetor dado (útil quando é necessário reler urls que deram erro)
def readData_Tectrilha_ComErro(conn):
    urlsComErro = pd.read_sql(f"SELECT * FROM leituras WHERE sucesso=0", conn)
    #print(urlscomerro)
    for urlComErroId in urlsComErro.index:
        assunto = urlsComErro["assunto"][urlComErroId]
        prefeitura = urlsComErro["prefeitura"][urlComErroId]
        ano = urlsComErro["ano"][urlComErroId]
        periodo = urlsComErro["periodo"][urlComErroId]
        unidadegestora = prefeiturasURLS.loc[prefeiturasURLS['prefeitura'] == prefeitura, 'unidadegestora'].values[0]
        urlTotal = urlsComErro["url"][urlComErroId]
        parametro = urlTotal.split(assunto)[1]
        url = urlTotal.split(assunto)[0]
        readData_tectrilha(url, assunto, ano, periodo, prefeitura, parametro, unidadegestora, True, conn)