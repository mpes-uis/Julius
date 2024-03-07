gera_portaltp <- function(prefeituras_portaltp, assuntos_portaltp, periodo, meses = 1:12) {
  
  conn <- dbConnect(SQLite(), dbname = "output_R/portaltp.db")
  log <- data.frame(tabelas_adicionadas = "")
  dbWriteTable(conn, name = "log", value = log, append = TRUE)
  
  for (ano in periodo){
    for (mes in meses){
      for (prefeitura in 1:nrow(prefeituras_portaltp)){
        for (assunto in 1:nrow(assuntos_portaltp)){
          
          nome_tabela <- paste0(prefeituras_portaltp[prefeitura,3], "_",
                                assuntos_portaltp[assunto,], "_",
                                ano, "_",
                                mes)
          
          consulta <- paste0("SELECT EXISTS (SELECT 1 FROM log WHERE tabelas_adicionadas = '",
                             nome_tabela, "') AS linha_existe;")
          resultado <- dbGetQuery(conn, consulta)
          
          if (resultado == 0) {
            df <- try(consulta_portaltp(prefeituras_portaltp[prefeitura,4],
                                        assuntos_portaltp[assunto,],
                                        ano,
                                        mes,
                                        prefeituras_portaltp[prefeitura, 3]),
                      silent = TRUE)
            if (class(df) == "data.frame") {
              dbWriteTable(conn, name = assuntos_portaltp[assunto,], value = df, append = TRUE)
              cat("\033[1;32m", nome_tabela, " adicionada ao banco portaltp\n", "\033[0m")
              
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
