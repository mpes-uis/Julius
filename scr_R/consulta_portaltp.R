consulta_portaltp <- function(prefeitura, metodo, ano, mes) {

  # montando a url e consultando a API
  url <- paste0(prefeitura,"json_", metodo, "?ano=", ano, "&mes=", mes)
  response <- GET(url)
  
  # convertendo o JSON obtido para xml e armazenando a tabela
  if (response["status_code"] == 200) {
    data <- (content(response, "text"))
    data <- gsub("<\\?xml version=\"1.0\" encoding=\"utf-8\"\\?>\r\n<string xmlns=\"http://tempuri.org/\">", "", data)
    data <- gsub("</string>", "", data)
    df <- fromJSON(data)
  }
   else {
     df <- c()
     }
  
  # retorna a tabela consultada
  return(df)
}



df <- consulta_portaltp("https://SaoMateus-es.portaltp.com.br/api/transparencia.asmx/", metodos[1,], 2022, 8)

