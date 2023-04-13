# Julius

Robo que captura os dados dos Portais da Transparência e aplica métricas e tipologias conhecidas para averiguar indicadores de improbidade administrativa (pessoas com dois cargos públicos - além do que é assegurado por lei - contratos e compras com preços elevados, etc) ou práticas ruims de portais.

# Sobre o Projeto

Esse projeto começou uma iniciativa dos servidores da Unidade de Inovação do Ministério Público do Estado do Espírito Santo, visando capturar dados para ampliar as possibilidades de investigação de improbidade administrativa, avaliar os dados disponibilizados pelos portais (nos conformes da Lei 12.527/2011) e aplicar tipologias que dão indícios de práticas de corrupção. O nome "Julius" faz referência e homenagem ao personagem Julius da série "Todo Mundo Odeia o Cris", pois a primeira tipologia testada era a verificação de pessoas com dois empregos (porém no caso, acumulação indevida de cargos públicos), e também por conta do segundo nome do primeiro colaborador do projeto, o dev Fabrício Julio Correia de Almeida.

Por ser ferramenta de aprendizado e por utilizar exclusivamente dados públicos governamentais, optamos recomeçar o projeto um iniciativa aberta, recomeçando o desenvolvimento dele de forma desvinculada de qualquer instituição e disponibilizando os códigos aqui no Github. A ideia é a partir do que já funciona, testar, aprimorar, criar e aperfeiçoar os códigos e funções com técnicas de Engenharia de Dados, Inteligência Artificial e boas prátivas de desenvolvimento. Por isso também o Readme e toda documentação (até os comentários dos códigos) estarem em inglês como lingua padrão, aprender inglês sempre é bom.

# Como funciona?

Julius funciona como um algoritmo de automação que captura dados dos Portais da Transparência por meio de suas APIs.

Cada portal da transparência possui API's possibilitando a extração dos dados nele contidos. Essas API's remetem a dados de estrutura semelhante, de acordo com a categoria do que é dispobilizado (remuneração, licitações, contratos, etc).

Em tese, quando todos municípios e entidades governamentais atingirem um "nível máximo" de transparência, todos terão dados similares sendo disponibilizados, variando conforme o tipo de entidade, por exemplo: Câmaras municipais usualmente não possuem estrutura de receitas e arrecadação, empresas públicas tem processos licitatórios diferenciados, etc). Partindo desse princípio que a quantidade de APIs e dados cresce. 

Nem todos os dados são estruturados. Contratos, editais, atas e notas de empenho são exemplos de dados não estruturados. Daí que cresce a complexidade. O que esperamos no médio prazo é isso aqui:



# FAQ Portais da Transparência

Um Portal da Transparência é um site que tem por finalidade veicular dados e informações detalhados sobre a execução orçamentária dos órgãos públicos brasileiros e entidades do terceiro setor que recebem repasses públicos. A ferramenta publica também dados sobre assuntos transversais ou que estejam relacionados à função da maioria desses órgãos.

Uma tendência dos Portais da Transparência brasileiros é a disponibilização de APIs para facilitar a leitura/extração dos dados por meios automatizados, essa boa prática (https://www.portaltransparencia.gov.br/api-de-dados) tem se expandido rapidamente tanto nos portais criados pelos órgãos públicos como pelas empresas prestadoras de serviço. Espera-se que cada portal da transparência posssuirá ao menos uma API para disponibilizar todos os dados exigidos pela Lei de Acesso a Informação.

Os dados contidos em portais são estruturados (tabelas de salários, pagamentos a terceiros, repasses, receitas financeiras) e não estruturados (contratos, editais, notas de empenho), variando de acordo com a esfera pública, tipo de autarquia e outros elementos.

# Documentação
Working on it...

# Como colaborar
Working on it...

# Como fazer rodar
O Julius pode rodar em dois formatos:
1 - Algoritmo completo que executa as funções de pipeline e ETL, focado em aprendizado e teste, pois necessita apenas de um compilador de Python e dos pacotes.
2 - Códigos separados orquestrados pelo Airflow, facilitando o uso por instituições públicas e ONGs. Pode ser acompanhado em:


## Authors

| [<img src="https://github.com/pedropberger.png?size=115" width=115><br><sub>@pedropberger</sub>](https://github.com/pedropberger) | [<img src="https://github.com/mwildemberg.png?size=115" width=115><br><sub>@mwildemberg</sub>](https://github.com/mwildemberg) |
| :---: | :---: |
