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


import tectrilha
import agape
#import portaltp

def main():
    empresa_escolhida = str(input('Escolha qual empresa você quer rodar: \n 1: Tectrilha \n 2: Ágape \n 3: Todas \n'))
    funcao_escolhida = str(input('Escolha o que você quer fazer: \n 1: Rodar todo o script \n 2: Rodar apenas as leituras com erro \n3'))

    if(empresa_escolhida=='1'):
        if(funcao_escolhida=='1'):
            tectrilha.readData_tectrilha_Total()
        if(funcao_escolhida=='2'):
            print("Função em construção")
    if(empresa_escolhida=='2'):
        if(funcao_escolhida=='1'):
            conn=sqlite3.connect('agape.db')
            type(conn)
            cur = conn.cursor()
            type(cur)
            print('Database Connected!')
            agape.readData_Agape_Total(conn)
            conn.close()
        if(funcao_escolhida=='2'):
            print("Função em construção")
    if(empresa_escolhida=='3'):
        if(funcao_escolhida=='1'):
            #Conexão com o banco Agape
            conn=sqlite3.connect('agape.db')
            type(conn)
            cur = conn.cursor()
            type(cur)
            print('Database Connected!')
            agape.readData_Agape_Total(conn)
            conn.close()
            tectrilha.readData_tectrilha_Total()
            
        if(funcao_escolhida=='2'):
            print("Função em construção")
  

if __name__ == "__main__":
    main()
