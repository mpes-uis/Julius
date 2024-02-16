conn <- dbConnect(SQLite(), dbname = "banco/julius.db")
periodo <- 2020:2024

gera_banco <- function(prefeituras_portaltp, metodos, periodo, meses = 1:12) {
  for (ano in periodo){
    for (mes in meses){
      for (prefeitura in 1:nrow(prefeituras_portaltp)){
        for (metodo in 1:nrow(metodos)){
          
          nome_tabela <- paste0(prefeituras_portaltp[prefeitura,3], "_",
                                metodos[metodo,], "_",
                                ano, "_",
                                mes)
          
          df <- as.data.frame(consulta_portaltp(prefeituras_portaltp[prefeitura,4],
                                  metodos[metodo,],
                                  ano,
                                  mes,
                                  prefeituras_portaltp[prefeitura, 3]))
          if (nrow(df) != 0) {
            dbWriteTable(conn, name = metodos[metodo,], value = df, append = TRUE)
            cat("\033[1;32m", nome_tabela, " adicionada ao banco\n", "\033[0m")
          }
        }
      }
    }
  }
}

gera_banco(prefeituras_portaltp, metodos, periodo)


consulta <- "SELECT * from frota_veiculos"

df <- dbGetQuery(conn, consulta)
