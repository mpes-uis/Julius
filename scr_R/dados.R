prefeituras <- read.csv("data/prefeituras_limpo.csv")

prefeituras_portaltp <- subset(prefeituras, empresa == "portaltp")

prefeituras_tectrilha <- subset(prefeituras, empresa == "Tectrilha")

prefeituras_agape <- subset(prefeituras, empresa == "Agape")

prefeituras_alphatec <- subset(prefeituras, empresa == "Alphatec")

assuntos_portaltp <- read.csv("data/assuntos_portaltp.csv")

assuntos_agape <- read.csv("data/assuntos_agape.csv")

assuntos_tectrilha <- read.csv("data/assuntos_tectrilha.csv")

periodo <- 2018:2024