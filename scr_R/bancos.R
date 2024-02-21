#banco agape
{
inicio_agape <- Sys.time()
gera_agape(prefeituras_agape, assuntos_agape)
fim_agape <- Sys.time()
tempo_agape <- difftime(fim_agape, inicio_agape)
}

#banco portaltp
{
inicio_portaltp <- Sys.time()
gera_portaltp(prefeituras_portaltp, assuntos_portaltp, periodo)
fim_portaltp <- Sys.time()
tempo_portaltp <- difftime(fim_portaltp, inicio_portaltp)
}

#banco tectrilha
{
inicio_tectrilha <- Sys.time()
gera_tectrilha(prefeituras_tectrilha, assuntos_tectrilha, periodo)
fim_tectrilha <- Sys.time()
tempo_tectrilha <- difftime(fim_tectrilha, inicio_tectrilha)
}



