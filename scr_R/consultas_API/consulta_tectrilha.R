consulta_tectrilha <- function(API, assunto, parametro, unidade_gestora, ano, mes){
  url <- paste0(API, assunto, parametro)
  url <- sub("\\{unidadeGestoraId\\}", unidade_gestora, url)
  url <- sub("\\{exercicio\\}", ano, url)
  url <- sub("\\{periodo\\}", mes, url)
  
  print(url)
  
  response <- GET(url, timeout(10))
  json <- content(response, "text", encoding = "UTF-8")
  data <- fromJSON(json)
  
  return(data)
}

