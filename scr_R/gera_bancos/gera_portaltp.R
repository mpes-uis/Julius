gera_portaltp <- function(prefeituras_portaltp, assuntos_portaltp, periodo, meses = 1:12) {
  
  conn <- dbConnect(SQLite(), dbname = "output_R/portaltp.db")
  
  for (ano in periodo){
    for (mes in meses){
      for (prefeitura in 1:nrow(prefeituras_portaltp)){
        for (assunto in 1:nrow(assuntos_portaltp)){
          
          nome_tabela <- paste0(prefeituras_portaltp[prefeitura,3], "_",
                                assuntos_portaltp[assunto,], "_",
                                ano, "_",
                                mes)
          
          df <- as.data.frame(consulta_portaltp(prefeituras_portaltp[prefeitura,4],
                                  assuntos_portaltp[assunto,],
                                  ano,
                                  mes,
                                  prefeituras_portaltp[prefeitura, 3]))
          if (nrow(df) != 0) {
            dbWriteTable(conn, name = assuntos_portaltp[assunto,], value = df, append = TRUE)
            cat("\033[1;32m", nome_tabela, " adicionada ao banco\n", "\033[0m")
          }
        }
      }
    }
  }
  
  dbDisconnect(conn)
}

gera_portaltp(prefeituras_portaltp, assuntos_portaltp, periodo)

