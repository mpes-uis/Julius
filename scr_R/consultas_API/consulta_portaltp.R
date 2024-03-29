consulta_portaltp <- function(API, assunto, ano, mes, municipio) {

  # montando a url e consultando a API
  url <- paste0(API,"json_", assunto, "?ano=", ano, "&mes=", mes)
  response <- GET(url, timeout(5))
  
  # obtendo a data que a consulta foi realizada
  data_consulta <- Sys.time()
  
  # convertendo o JSON obtido para xml e armazenando a tabela
  if (status_code(response) == 200) {
    data <- content(response, "text")
    data <- gsub("<\\?xml version=\"1.0\" encoding=\"utf-8\"\\?>\r\n<string xmlns=\"http://tempuri.org/\">", "", data)
    data <- gsub("</string>", "", data)
    df <- data.frame(fromJSON(data))
  }
   else {
     df <- NULL
   }
  
  if (nrow(df) != 0) {
    # adicionando as data frame a data da consulta e o nome do municipio
    df$data_consulta <- data_consulta
    df$nome_municipio <- municipio
  }
  else {
    df <- NULL
  }

  # retorna a tabela consultada
  return(df)
}
