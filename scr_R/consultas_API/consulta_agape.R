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
  
  if(is.null(nrow(df))){
    df <- NULL
  }
  
  df_class <- data.frame(coluna = names(df),
                        classe = sapply(df, class))
  df_list <- df_class[df_class$classe == "list",]
  
  if (nrow(df_list) > 0 ){
    for (i in 1:nrow(df_list)) {
      df <- df[, !names(df) == df_list[i,1]]
    } 
  }
  
  if (length(df) != 0) {
    df$data_consulta <- data_consulta
    df$nome_municipio <- municipio
  }
  return(df)
}


#problema: algumas tabelas contém listas dentro delas, a funcao dbwritetable nao suporta esse tipo de objeto

df <- consulta_agape(prefeituras_agape[2, 4],
                     assuntos_agape[12,], 1,
                     prefeituras_agape[2, 3])


