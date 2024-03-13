limpa_banco <- function(banco) {
  conn <- dbConnect(SQLite(), dbname = banco)
  
  tabelas <- dbListTables(conn)
  
  for (tabela in tabelas) {
    dbExecute(conn, paste("DROP TABLE", tabela))
  }
  
  dbDisconnect(conn)
  
  cat("O banco de dados foi limpo com sucesso.\n")
}

limpa_banco("output_R/tectrilha.db")


#conn <- dbConnect(SQLite(), dbname = "output_R/agape.db")

#dbExecute(conn, paste("DROP TABLE", "licitacoes"))
