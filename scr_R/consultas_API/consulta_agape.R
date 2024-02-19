consulta_agape <- function(API, assunto){
  #consulta a API, salva a primeira página e verifica o número de páginas
  url <- paste0(API, assunto, "?page_size=100&page=", 1)
  response <- GET(url)
  json <- content(response, "text", encoding = "UTF-8")
  data <- fromJSON(json)
  df <- data[["registros"]]
  paginas <- data[["pagina_total"]]
  cat(1, "de", paginas, "\n")
  
  #consulta as páginas seguintes e anexa ao data frame da primeira
  for(pagina in 2:paginas){
    url <- paste0(API, assunto, "?page_size=100&page=", pagina)
    response <- GET(url)
    json <- content(response, "text", encoding = "UTF-8")
    data <- fromJSON(json)
    df <- rbind(df, data[["registros"]])
    cat(pagina, "de", paginas, "\n")
  }
  return(df)
}
