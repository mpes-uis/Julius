"""Welcome to Julius!

This is a simple code created to extract data from all City Transparency Portals of the state of Espírito Santo, Brazil.
Every portal has your own API, but they have similar patterns (depends on the company that create them).
We catalog them in 4 diferent patterns to extract data in a efficient way.

You have 2 ways to execute this code. Using threads or not.
If you choose use threading you can select how API methods (data clusters) u want extract.
A full extract consumes 4gb of you hard disk.

###############################################################

Bem-vindo à Julius!

Este é um código simples criado para extrair dados de todos os Portais de Transparência da Cidade do Estado do Espírito Santo, Brasil.
Cada portal tem sua própria API, mas têm padrões semelhantes (depende da empresa que os cria).
Nós os catalogamos em 4 padrões diferentes para extrair dados de uma maneira eficiente.

Você tem 2 maneiras de executar este código. Usando ou não threads.
Se você escolher usar threading você pode selecionar como os métodos API (clusters de dados) você quer extrair.
Um extrato completo consome 4gb de seu disco rígido.

Todos comentários estão em inglês pq a ideia é esse projeto também ser uma ferramenta de aprendizado e prática

Insira a API da sua cidade e colabore para unificar as fontes de dados públicos no Brasil"""

"""All starts here"""

#import json
#import requests
#import xmltodict
#import pandas as pd
#from concurrent.futures import thread
#from distutils.command.config import config
#import py
from scripts.data import DataPortaltp
from threading import *
import time

"""Select your way
1 - Full Extract
2 - Multithreading (in implementation)"""

way = 2
start = time.time()

def main():
    
    if way == 1:
        print("Partiu!")
        DataPortaltp.extractall()
        end = time.time()
        print('Finished full run in ' + end)
    else:
        print("Bora fast!")
        DataPortaltp.dbclean()
        conn = DataPortaltp.dbstart()
        DataPortaltp.licitacoes(conn)
        DataPortaltp.dbclose(conn)
        end = time.time()
        print('Finished selected packages in ' + end)

if __name__ == "__main__":
    main()