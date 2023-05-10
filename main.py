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
            agape.readData_Agape_Total()
        if(funcao_escolhida=='2'):
            print("Função em construção")
    if(empresa_escolhida=='3'):
        if(funcao_escolhida=='1'):
            tectrilha.readData_tectrilha_Total()
            agape.readData_Agape_Total()
        if(funcao_escolhida=='2'):
            print("Função em construção")
  

if __name__ == "__main__":
    main()
