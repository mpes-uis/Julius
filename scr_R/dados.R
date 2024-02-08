prefeituras <- read.csv("dados/prefeituras.csv")

prefeituras_portaltp <- subset(prefeituras, empresa == "portaltp")

metodos <- read.csv("dados/APImethods.csv")
