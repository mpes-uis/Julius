consulta_tectrilha <- function(API, assunto, parametro, unidade_gestora, ano, mes, municipio){
  url <- paste0(API, assunto, parametro)
  url <- sub("\\{unidadeGestoraId\\}", unidade_gestora, url)
  url <- sub("\\{exercicio\\}", ano, url)
  url <- sub("\\{periodo\\}", mes, url)
  
  
  response <- GET(url, timeout(10))
  data_consulta <- Sys.time()
  
  if (status_code(response) == 200) {

    json <- content(response, "text", encoding = "UTF-8")
    df <- fromJSON(json)
  } 
  else {
    df <- NULL
  }
  
  #if (nrow(df) != 0) {
  #  df$data_consulta <- data_consulta
  #  df$nome_municipio <- municipio
  #}
  return(df)
}

