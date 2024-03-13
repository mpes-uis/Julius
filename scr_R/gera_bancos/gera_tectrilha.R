gera_tectrilha <- function(prefeituras_tectrilha, assuntos_tectrilha, periodo, meses = 1:12) {
  
  conn <- dbConnect(SQLite(), dbname = "output_R/tectrilha.db")
  log <- data.frame(tabelas_adicionadas = "")
  dbWriteTable(conn, name = "log", value = log, append = TRUE)
  
  for (ano in periodo){
    for (mes in meses){
      for (prefeitura in 1:nrow(prefeituras_tectrilha)){
        for (assunto in 1:nrow(assuntos_tectrilha)){
          
          nome_tabela <- paste0(prefeituras_tectrilha[prefeitura,3], "_",
                                assuntos_tectrilha[assunto,2], "_",
                                ano, "_",
                                mes)
          
          consulta <- paste0("SELECT EXISTS (SELECT 1 FROM log WHERE tabelas_adicionadas = '",
                             nome_tabela, "') AS linha_existe;")
          resultado <- dbGetQuery(conn, consulta)
          
          if (resultado == 0) {
            
            df <- try(consulta_tectrilha(prefeituras_tectrilha[prefeitura,4],
                                         assuntos_tectrilha[assunto,2],
                                         assuntos_tectrilha[assunto,3],
                                         prefeituras_tectrilha[prefeitura,6],
                                         ano, mes,
                                         prefeituras_tectrilha[prefeitura,3]),
                      silent = TRUE)
            
            if (class(df) == "data.frame") {
              dbWriteTable(conn, name = assuntos_tectrilha[assunto,2], value = df, append = TRUE)
              cat("\033[1;32m", nome_tabela, " adicionada ao banco tectrilha\n", "\033[0m")
              
              log <- data.frame(tabelas_adicionadas = nome_tabela)
              
              dbWriteTable(conn, name = "log", value = log, append = TRUE)
            }
            else if (class(df) == "try_error") {
              cat("\033[1;35m", nome_tabela, "demorou pra responder\n", "\033[0m")
            }
            else if (class(df) == "NULL") {
              cat("\033[1;34m", nome_tabela, "esta vazia\n", "\033[0m")
            }
            
          }
          else {
            cat("\033[1;33m", nome_tabela, "ja esta no banco portaltp\n", "\033[0m")
          }
        }
      }
    }
  }
  
  dbDisconnect(conn)
}
