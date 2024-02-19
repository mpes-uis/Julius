gera_agape <- function(prefeituras_agape, assuntos_agape) {
  
  conn <- dbConnect(SQLite(), dbname = "output_R/agape.db")
  
  for (prefeitura in 1:nrow(prefeituras_agape)) {
    for (assunto in 1:nrow(assuntos_agape)) {
      
      nome_tabela <- paste0(prefeituras_agape[prefeitura,3], "_",
                            assuntos_agape[assunto,])
      
      df <- as.data.frame(consulta_agape(prefeituras_agape[prefeitura, 4],
                                         assuntos_agape[assunto,]))
      
      if (nrow(df) != 0) {
        dbWriteTable(conn, name = assuntos_agape[assunto,], value = df, append = TRUE)
        cat("\033[1;32m", nome_tabela, " adicionada ao banco\n", "\033[0m")
      }
    }
  }
}

gera_agape(prefeituras_agape, assuntos_agape)
