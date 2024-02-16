consulta_portaltp <- function(API, metodo, ano, mes, municipio) {

  # montando a url e consultando a API
  url <- paste0(API,"json_", metodo, "?ano=", ano, "&mes=", mes)
  response <- GET(url)
  
  # obtendo a data que a consulta foi realizada
  data_consulta <- Sys.time()
  
  # convertendo o JSON obtido para xml e armazenando a tabela
  if (response["status_code"] == 200) {
    data <- (content(response, "text"))
    data <- gsub("<\\?xml version=\"1.0\" encoding=\"utf-8\"\\?>\r\n<string xmlns=\"http://tempuri.org/\">", "", data)
    data <- gsub("</string>", "", data)
    df <- as.data.frame(fromJSON(data))
  }
   else {
     df <- as.data.frame(c())
   }
  
  if (nrow(df) != 0) {
    # adicionando as data frame a data da consulta e o nome do municipio
    df$data_consulta <- data_consulta
    df$nome_municipio <- municipio
  }

  
  # retorna a tabela consultada
  return(df)
}
