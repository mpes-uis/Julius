# Instale as bibliotecas se ainda não tiver instalado
install.packages(c("httr", "jsonlite"))

# Carregue as bibliotecas
library(httr)
library(jsonlite)
library(httr)
library(xml2)
library(XML)

# URL da API
url <- "https://afonsoclaudio-es.portaltp.com.br/api/transparencia.asmx/json_licitacoes?ano=2020&mes=1"

# Faça a solicitação para obter os dados (assumindo que a API retorna JSON)
response <- GET(url)

data <- (content(response, "text"))

data <- gsub("<\\?xml version=\"1.0\" encoding=\"utf-8\"\\?>\r\n<string xmlns=\"http://tempuri.org/\">", "", data)
data <- gsub("</string>", "", data)

df <- fromJSON(data)


df



consulta <- "SELECT EXISTS (SELECT 1 FROM log WHERE tabelas_adicionadas = 'Anchieta_receitas_pagina_29') AS linha_existe;"
resultado <- dbGetQuery(conn, consulta)


df <- as.data.frame(consulta_portaltp(prefeituras[9,4],
                                      metodos[1,],
                                      periodo[1],
                                      1,
                                      prefeituras_portaltp[1, 3]))




# Obtém uma lista de todas as tabelas no banco de dados
tabelas <- dbListTables(conn)

# Exclui cada tabela do banco de dados
for (tabela in tabelas) {
  dbExecute(conn, paste("DROP TABLE", tabela))
}




a <- consulta_agape(prefeituras_agape[1, 4], assuntos_agape[5,], 18)
