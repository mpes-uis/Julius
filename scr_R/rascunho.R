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
