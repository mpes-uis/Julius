conn <- dbConnect(SQLite(), dbname = "banco/julius.db")

gera_banco <- function(prefeituras_portaltp, metodos, periodo){
  
  for(ano in periodo){
    for (mes in 1:12){
      for (metodo in 1:nrow(metodos)){
        for (prefeitura in 1:nrow(prefeituras_portaltp)){
      
          nome_tabela <- paste0(prefeituras_portaltp[prefeitura,3], "_",
                              metodos[metodo,], "_",
                              ano, "_",
                              mes)
          
          if (!dbExistsTable(conn, nome_tabela)) {
            df <- as.data.frame(consulta_portaltp(prefeituras_portaltp[prefeitura,4],
                                                  metodos[metodo,],
                                                  ano,
                                                  mes))
            
            if (nrow(df) != 0) {
              dbWriteTable(conn, name = nome_tabela, value = df, overwrite = TRUE)
              cat("\033[1;32m", nome_tabela, " adicionada ao banco\n", "\033[0m")
            }
            else {
              cat("\033[1;31m", nome_tabela, " tabela vazia\n", "\033[0m")
            }
          }
          else {
            cat("\033[1;34m",nome_tabela, " tabela já existe\n", "\033[0m")
          }
          
          df <- c()
        }
      }
    }
  }
}


