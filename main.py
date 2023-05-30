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


import scr.tectrilha as tectrilha
import scr.agape as agape
import sqlite3
import scr.portaltp as portaltp

def main():
    empresa_escolhida = str(input('Escolha qual empresa você quer rodar: \n 0: Todas \n 1: Tectrilha \n 2: Ágape \n 3: Portaltp \n '))
    funcao_escolhida = str(input('Escolha o que você quer fazer: \n 1: Rodar todo o script (lê apenas urls que não foram lidas ainda) \n 2: Rodar apenas as leituras com erro \n'))

    if(empresa_escolhida=='1'):
        if(funcao_escolhida=='1'):
            #Conexão com o banco tectrilha
            conn_tectrilha=sqlite3.connect('output/tectrilha.db')
            type(conn_tectrilha)
            cur = conn_tectrilha.cursor()
            type(cur)
            print('Tectrilha Connected!')
            tectrilha.readData_tectrilha_Total(conn_tectrilha)
            conn_tectrilha.close()
        if(funcao_escolhida=='2'):
            print("Função em construção")
    if(empresa_escolhida=='2'):
        #Conexão com o banco Agape
        conn_agape=sqlite3.connect('output/agape.db')
        type(conn_agape)
        cur_agape = conn_agape.cursor()
        type(cur_agape)
        print('Agape Connected!')
        if(funcao_escolhida=='1'):
            agape.readData_Agape_Total(conn_agape, cur_agape)
        if(funcao_escolhida=='2'):
            agape.readData_Agape_ComErro(conn_agape,cur_agape)
        conn_agape.close()
    if(empresa_escolhida=='3'):
        #Conexão com o banco portaltp
        conn_portaltp=sqlite3.connect('output/portaltp.db')
        type(conn_portaltp)
        cur_portaltp = conn_portaltp.cursor()
        type(cur_portaltp)
        print('Portaltp Connected!')
        if(funcao_escolhida=='1'):
            portaltp.readData_portaltp_Total(conn_portaltp)   
        if(funcao_escolhida=='2'):
            portaltp.readData_Portaltp_ComErro(conn_portaltp)
        conn_portaltp.close()
    if(empresa_escolhida=='0'):
        if(funcao_escolhida=='1'):
            #Conexão com o banco Agape
            conn_agape=sqlite3.connect('output/agape.db')
            type(conn_agape)
            cur_agape = conn_agape.cursor()
            type(cur_agape)
            print('Agape Connected!')
            agape.readData_Agape_Total(conn_agape,cur_agape)
            conn_agape.close()

            #Conexão com o banco tectrilha
            conn_tectrilha=sqlite3.connect('output/tectrilha.db')
            type(conn_tectrilha)
            cur = conn_tectrilha.cursor()
            type(cur)
            print('Tectrilha Connected!')
            tectrilha.readData_tectrilha_Total(conn_tectrilha)
            conn_tectrilha.close()

            #Conexão com o banco portaltp
            conn_portaltp=sqlite3.connect('output/portaltp.db')
            type(conn_portaltp)
            cur = conn_portaltp.cursor()
            type(cur)
            print('Portaltp Connected!')
            portaltp.readData_portaltp_Total(conn_portaltp)
            conn_portaltp.close()
            
        if(funcao_escolhida=='2'):
            print("Função em construção")
  

if __name__ == "__main__":
    main()
