consulta_agape <- function(API, assunto, pagina, municipio) {
  
  url <- paste0(API, assunto, "?page_size=100&page=", pagina)
  response <- GET(url)
  data_consulta <- Sys.time()
  
  if (status_code(response) == 200) {
    json <- content(response, "text", encoding = "UTF-8")
    data <- fromJSON(json)
    df <- data[["registros"]]
  }
  else {
    df <- NULL
  }
  
  if (length(df) != 0) {
    df$data_consulta <- data_consulta
    df$nome_municipio <- municipio
  }
  
  return(df)
}


#problema: algumas tabelas contém listas dentro delas, a funcao dbwritetable nao suporta esse tipo de objeto