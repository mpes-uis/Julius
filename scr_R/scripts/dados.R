prefeituras <- read.csv("dados/prefeituras_limpo.csv")

prefeituras_portaltp <- subset(prefeituras, empresa == "portaltp")

prefeituras_agape <- subset(prefeituras, empresa == "Agape")

prefeituras_tectrilha <- subset(prefeituras, empresa == "Tectrilha")

prefeituras_alphatec <- subset(prefeituras, empresa == "Alphate")

metodos <- read.csv("dados/APImethods.csv")
