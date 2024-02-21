consulta_agape <- function(API, assunto, pagina) {
  url <- paste0(API, assunto, "?page_size=100&page=", pagina)
  response <- GET(url)
  
  if (response["status_code"] == 200) {
    json <- content(response, "text", encoding = "UTF-8")
    data <- fromJSON(json)
    df <- data[["registros"]]
    if (length(df) == 0){
      df <- as.data.frame(c())
    }
  }
  
  else {
    df <- as.data.frame(c())
  }
  return(df)
}
