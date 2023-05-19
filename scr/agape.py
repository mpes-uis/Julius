#API Agape e Alphatec

import traceback
import requests
import pandas as pd
import numpy as np
import json
import sqlite3
#import datetime
from datetime import date
import os

#Conexão com o banco Agape (criada na main)
#conn=sqlite3.connect('agape.db')
#type(conn)
#cur = conn.cursor()
#type(cur)
#print('Database Connected!')

#Lê csv prefeituras (contém nome da prefeitura, município, url base e empresa responsavel pelo portal)
prefeiturasURLS=pd.read_csv('data/prefeituras.csv')
#print(prefeiturasURLS)

#read csv assuntos
assuntos_agape=pd.read_csv('data/assuntos_agape.csv')

#Função que lê a url, incrementa as paginas e usa a função readAndSaveUrl
#Para cada url lida, cria tabela do assunto no banco e verifica se o dado já existe
#Se o dado já existe na tabela correspondente, não insere novamente (para não haver registros duplicados)
#Usando a função readAndSaveUrl, verifica se a url já foi lida e não faz nova leitura
#A função readagain tem o objetivo de fazer com que a mesma url não seja lida novamente
def readData_Agape(url, assunto, prefeitura, readAgain, conn):
    contador = 1
    pag_total = None
    while (contador is not None):
        sucess=True
        erro=''
        url_agape=url + assunto +'?page_size=100&page='+ str(contador)
        print(url_agape)
        if (readAgain == False):
            alreadyRead = pd.read_sql(f"SELECT * FROM leituras WHERE url = '{url_agape}'", conn)
            if (not alreadyRead.empty):
                contador = alreadyRead['proxima'][0]
                continue
        try:
            #Lê url
            response = requests.get(url_agape)
            data=json.loads(response.content.decode('utf-8-sig'))

            #transforma dados lidos em dataframe
            df=pd.DataFrame(data['registros'])
            df.insert(0,"prefeitura",prefeitura)
            #print(df)
            #print(df.columns)

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

            pag_total = data['pagina_total']
        except Exception as e:
            sucess=False 
            erro=str(e) #mensagem de erro da leitura da url
            print(f"Error reading data from {url_agape}")
            readAndSaveUrl(url_agape, assunto, prefeitura, contador, sucess, erro, None, pag_total, conn) #salva a url e o respectivo erro
            break #quando ocorre erro na leitura da url, o contador da página não é iterado e voltamos ao início da função (porque não sabemos se há uma próxima página a ser lida), em vez disso começamos a ler o próximo assunto ou prefeitura)

        readAndSaveUrl(url_agape, assunto, prefeitura, contador, sucess, erro, data['pagina_proxima'],pag_total, conn) #salva a url lida com sucesso
        contador = data['pagina_proxima'] #itera quando há mais páginas a serem lidas, quando não há: o while termina (porque é rebece null/None)
        #print(contador)

#readData_Agape("https://transparencia.cachoeiro.es.gov.br/transparencia/api/", "orcamento_receita", "Cachoeiro de Itapemirim")

# Lê e salva url e os respectivos resultados
def readAndSaveUrl(url_agape, assunto, prefeitura, contador, sucess, erro, proxima, pag_total, conn):
    colunas = ['url','assunto','prefeitura', 'contador','sucess','erro','data_leitura','proxima','pag_total']
    leituras = pd.DataFrame(columns=colunas)
    registro={
         "url":url_agape,
         "assunto":assunto,
         "prefeitura":prefeitura,
         "contador": contador,
         "sucess": sucess,
         "erro": erro,
         "data_leitura": date.today(),
         "proxima": proxima,
         "pag_total": pag_total

    }
    leituras=leituras.append(registro, ignore_index=True)
    leituras.to_sql("leituras", conn, if_exists='append', index=False)

#Função que lê todas as prefeituras e todos os assuntos Agape e Alphatec 1 vez (e não substitui leitura)
def readData_Agape_Total(conn):
    #Cria a tabela leituras no banco (se for a primeira vez: cria a tabela; se a tabela já existir, não insere nenhuma informação)
    colunas = ['url','assunto','prefeitura', 'contador','sucess','erro','data_leitura','proxima','pag_total']
    leituras = pd.DataFrame(columns=colunas)
    leituras.to_sql('leituras', conn, if_exists='append', index=False)
    for id in prefeiturasURLS["id"]:
            for assuntoid in assuntos_agape.index:
                        if prefeiturasURLS["empresa"][id]=="Agape" or prefeiturasURLS["empresa"][id]=="Alphatec" :
                                readData_Agape(prefeiturasURLS["url"][id], assuntos_agape["assunto"][assuntoid], prefeiturasURLS["prefeitura"][id], False, conn)

#readData_Agape_Total(conn)

#Função que lê urls a partir de um vetor dado (útil quando é necessário reler urls que deram erro ou ler novamente urls que foram lidas há muito tempo)
#urlsJaLidas = [] #vetor de urls para serem lidas novamente
#def readAgapenovamente(urlsJaLidas):

