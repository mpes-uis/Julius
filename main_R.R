# Pacotes necessários
source("scr_r/pacotes.R")

# Dados para realizar as consultas

source("scr_R/dados.R")

# Funções para realizar as consultas

source("scr_R/consultas_API/funcoes_consultas.R")

# Cria o banco SQLite

source("scr_R/gera_bancos/funcoes_gera_banco.R")

fim <- Sys.time()
inicio
fim
difftime(fim, inicio)

