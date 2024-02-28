gera_agape <- function(prefeituras_agape, assuntos_agape) {
  
  conn <- dbConnect(SQLite(), dbname = "output_R/agape.db")
  log <- data.frame(tabelas_adicionadas = "")
  dbWriteTable(conn, name = "log", value = log, append = TRUE)
  
  for (prefeitura in 1:nrow(prefeituras_agape)) {
    for (assunto in 1:nrow(assuntos_agape)) {
      
      url <- paste0(prefeituras_agape[prefeitura,4], 
                    assuntos_agape[assunto,], 
                    "?page_size=100&page=", 1)
      response <- GET(url)
      
      if (status_code(response) == 200) {
        json <- content(response, "text", encoding = "UTF-8")
        data <- fromJSON(json)
        paginas <- data[["pagina_total"]]
    
      
        for(pagina in 1:paginas) {
        
        nome_tabela <- paste0(prefeituras_agape[prefeitura,3], "_",
                              assuntos_agape[assunto,], "_pagina_", pagina)
        consulta <- paste0("SELECT EXISTS (SELECT 1 FROM log WHERE tabelas_adicionadas = '",
                           nome_tabela, "') AS linha_existe;")
        resultado <- dbGetQuery(conn, consulta)
        
          if (resultado == 0) {
            
            df <- consulta_agape(prefeituras_agape[prefeitura, 4],
                                 assuntos_agape[assunto,], pagina,
                                 prefeituras_agape[prefeitura, 3])
            
            if (length(df) != 0) {
              dbWriteTable(conn, name = assuntos_agape[assunto,], value = df, append = TRUE)
              cat("\033[1;32m", nome_tabela, "de", paginas, "adicionada ao banco agape\n", "\033[0m")
              
              log <- data.frame(tabelas_adicionadas = nome_tabela)
              
              dbWriteTable(conn, name = "log", value = log, append = TRUE)
            }
            else {
              cat("\033[1;34m", nome_tabela, "de", paginas, "esta vazia\n", "\033[0m")
            }
          }
          else {
            cat("\033[1;33m", nome_tabela, "de", paginas, "ja esta no banco agape\n", "\033[0m")
          }
        }
      }
    }
  }
  
  dbDisconnect(conn)
}

#transformar lista numa string e salar a string no lugar da lista

#salvar num banco de dados de grafo

#excluir as colunas

df <- consulta_agape(prefeituras_agape[1, 4],
                     assuntos_agape[6,], 1,
                     prefeituras_agape[1, 3])


url <- paste0(prefeituras_agape[2,4], 
              assuntos_agape[11,], 
              "?page_size=100&page=", 1)
response <- GET(url)
json <- content(response, "text", encoding = "UTF-8")
data <- fromJSON(json)
paginas <- data[["pagina_total"]]
