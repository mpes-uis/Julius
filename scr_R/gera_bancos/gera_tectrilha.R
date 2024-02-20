gera_tectrilha <- function(prefeituras_tectrilha, assuntos_tectrilha, periodo, meses = 1:12) {
  
  conn <- dbConnect(SQLite(), dbname = "output_R/tectrilha.db")
  
  for (ano in periodo){
    for (mes in meses){
      for (prefeitura in 1:nrow(prefeituras_tectrilha)){
        for (assunto in 1:nrow(assuntos_tectrilha)){
          
          nome_tabela <- paste0(prefeituras_tectrilha[prefeitura,3], "_",
                                assuntos_tectrilha[assunto,2], "_",
                                ano, "_",
                                mes)
          
          df <- as.data.frame(consulta_tectrilha(prefeituras_tectrilha[prefeitura,4],
                                                assuntos_tectrilha[assunto,2],
                                                assuntos_tectrilha[assunto,3],
                                                prefeituras_tectrilha[prefeitura,6],
                                                ano, mes))
          
          if (nrow(df) != 0) {
            dbWriteTable(conn, name = assuntos_tectrilha[assunto,2], value = df, append = TRUE)
            cat("\033[1;32m", nome_tabela, " adicionada ao banco tectrilha\n", "\033[0m")
          }
        }
      }
    }
  }
  
  dbDisconnect(conn)
}


inicio_tectrilha <- Sys.time()
gera_tectrilha(prefeituras_tectrilha, assuntos_tectrilha, periodo)
fim_tectrilha <- Sys.time()
tempo_tectrilha <- difftime(fim_tectrilha, inicio_tectrilha)
