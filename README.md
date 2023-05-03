# Julius

Robo que captura os dados dos Portais da Transparência e aplica métricas e tipologias conhecidas para averiguar indicadores de improbidade administrativa (pessoas com dois cargos públicos - além do que é assegurado por lei - contratos e compras com preços elevados, etc) ou práticas ruims de portais.

# Sobre o Projeto

Esse projeto começou uma iniciativa dos servidores da Unidade de Inovação do Ministério Público do Estado do Espírito Santo, visando capturar dados para ampliar as possibilidades de investigação de improbidade administrativa, avaliar os dados disponibilizados pelos portais (nos conformes da Lei 12.527/2011) e aplicar tipologias que dão indícios de práticas de corrupção. O nome "Julius" faz referência e homenagem ao personagem Julius da série "Todo Mundo Odeia o Cris", pois a primeira tipologia testada era a verificação de pessoas com dois empregos (porém no caso, acumulação indevida de cargos públicos), e também por conta do segundo nome do primeiro colaborador do projeto, o dev Fabrício Julio Correia de Almeida.

Por ser ferramenta de aprendizado e por utilizar exclusivamente dados públicos governamentais, optamos recomeçar o projeto um iniciativa aberta, recomeçando o desenvolvimento dele de forma desvinculada de qualquer instituição e disponibilizando os códigos aqui no Github. A ideia é a partir do que já funciona, testar, aprimorar, criar e aperfeiçoar os códigos e funções com técnicas de Engenharia de Dados, Inteligência Artificial e boas prátivas de desenvolvimento. Por isso também o Readme e toda documentação (até os comentários dos códigos) estarem em inglês como lingua padrão, aprender inglês sempre é bom.

# Como funciona?

Julius funciona como um algoritmo de automação que captura dados dos Portais da Transparência por meio de suas APIs.

Cada portal da transparência possui API's possibilitando a extração dos dados nele contidos. Essas API's remetem a dados de estrutura semelhante, de acordo com a categoria do que é dispobilizado (remuneração, licitações, contratos, etc).

Em tese, quando todos municípios e entidades governamentais atingirem um "nível máximo" de transparência, todos terão dados similares sendo disponibilizados, variando conforme o tipo de entidade, por exemplo: Câmaras municipais usualmente não possuem estrutura de receitas e arrecadação, empresas públicas tem processos licitatórios diferenciados, etc). Partindo desse princípio que a quantidade de APIs e dados cresce. 

Nem todos os dados são estruturados. Contratos, editais, atas e notas de empenho são exemplos de dados não estruturados. Daí que cresce a complexidade. Cresce o desafio. Mas trabalhar em cima disso agrega valor aos dados e as ferramentas. Dados não estruturados podem ser armazenados em bancos NoSQL, seus textos podem ser extraídos para utilização e alimentação de aplicações de LPN, por exemplo.

O Julius consome as APIs de todos portais da transparência implementados (até o momento, os 78 municípios do estado do Espírito Santo) e armazena eles em um banco de dados. Esse banco fica disponível para utilização dentro do container docker. Daí você pode fazer download, compartilhar, consumir em suas aplicações ou utilizar como achar melhor.

# Resultados esperados

Dizem por aí que os dados são o petróleo do presente, certo?

Os órgãos de fiscalização (Ministérios Públicos, Tribunais de Contas, CGU, etc) e as próprias prefeituras e câmaras trabalharam com afinco na última década para por em prática e atenderem aos quesitos da Lei de Acesso a Informação, otimizando os portais da transparência para garantir ao povo acesso a dados confiáveis e de qualidade, e possibilitar esse acesso inclusive de forma automatizada (APIs, webservices, etc).

Ou seja, temos milhares de poços desse petróleo do futuro esperando para serem extraídos, tratados, refinados, observados e utilizados em prol da sociedade.

O Julius é um código que só busca dar o primeiro passo. Que é extrair, organizar e armazenar de forma eficiente esse produto.

A ferramenta é em código aberto, todo Stack de dados e aplicativos utilizados aqui será Open Source, e os dados extraídos por ela também são públicos, direito assegurado na Constituição a todos brasileiros.

Depois do Julius, o uso do dado é livre. Mas deixamos como sugestões boas práticas: criar ferramentas para fiscalização e análise de gastos automatizada, análise textual de contratos para identificação de fraudes, criação de sistema para consulta de preços, identificação de práticas de cartel e aprendizado pessoal.

# Como funciona?

Simples, instale o docker (https://www.docker.com) no seu ambiente.

Faça download do arquivo Dockerfile presente na raiz do diretório github.

Coloque o arquivo no diretório que gostar mais.

Execute o seguinte comando:

    YYYYY  Inserir comando docker aqui - FAZER YYYYY

Ponto!

O container criado irá fazer download dos códigos necessários e iniciar sua execução. Em seu storage você poderá encontrar o banco de dados SQLite.

# Desafios

Não existe um padrão universal ou regra de outro para funcionamento ou formato dos portais da transparência. O mesmo vale para suas APIs de consumo. Ou seja, mesmo que todo portal tenha que disponibilizar os dados exigidos na LAI, poderá disponibilizar na forma que quiser, ou lhe for mais prático.

Por exemplo: A API da prefeitura X retorna os dados referentes a folha de pagamento em um arquivo JSON e a prefeitura Y retorna em uma tabela em *.csv, ou, a prefeitura Z traz em sua sessão de contratos após o CNPJ o número do processo licitatório que originou o contrato, já a câmara Z traz apenas o CNPJ.

Nenhum dos casos acima fere a LAI, já que os dados estão disponibilizados. Porém exige que seja feita uma engenharia de dados caso você queira fazer um comparativo ou inserir todos dados em uma mesma base. É esse o quebra cabeça que queremos resolver.

A boa notícia? COMPLEMENTAR TEXTO


# FAQ Portais da Transparência

Um Portal da Transparência é um site que tem por finalidade veicular dados e informações detalhados sobre a execução orçamentária dos órgãos públicos brasileiros e entidades do terceiro setor que recebem repasses públicos. A ferramenta publica também dados sobre assuntos transversais ou que estejam relacionados à função da maioria desses órgãos.

Uma tendência dos Portais da Transparência brasileiros é a disponibilização de APIs para facilitar a leitura/extração dos dados por meios automatizados, essa boa prática (https://www.portaltransparencia.gov.br/api-de-dados) tem se expandido rapidamente tanto nos portais criados pelos órgãos públicos como pelas empresas prestadoras de serviço. Espera-se que cada portal da transparência posssuirá ao menos uma API para disponibilizar todos os dados exigidos pela Lei de Acesso a Informação.

Os dados contidos em portais são estruturados (tabelas de salários, pagamentos a terceiros, repasses, receitas financeiras) e não estruturados (contratos, editais, notas de empenho), variando de acordo com a esfera pública, tipo de autarquia e outros elementos.

Os itens abaixo são comumente encontrados nos portais da transparência, e cada um deles representa uma seção específica com informações sobre determinado assunto relacionado às atividades de uma entidade governamental:

Licitações: são processos administrativos que visam selecionar a melhor proposta para a contratação de serviços, obras ou fornecimento de bens. As informações sobre licitações geralmente incluem datas, modalidades, valores, empresas participantes e vencedoras.

Contratos: são acordos formais firmados entre uma entidade governamental e um terceiro, seja ele uma pessoa física ou jurídica, para a prestação de serviços, obras ou fornecimento de bens. As informações sobre contratos geralmente incluem valores, objeto do contrato, prazos e condições.

Atas: são documentos que registram os principais fatos e decisões de uma reunião ou sessão pública. As informações sobre atas geralmente incluem datas, horários, pauta e resoluções.

Ordem de compras: são documentos emitidos para a aquisição de materiais ou serviços. As informações sobre ordem de compras geralmente incluem datas, valores, fornecedores e descrição dos itens adquiridos.

Materiais entradas: são registros dos materiais recebidos pela entidade governamental, seja por meio de compras, doações ou outras formas de aquisição. As informações sobre materiais entradas geralmente incluem datas, valores, fornecedores e descrição dos itens.

Materiais saídas: são registros dos materiais que deixaram a entidade governamental, seja por meio de transferências, descartes ou outras formas de baixa. As informações sobre materiais saídas geralmente incluem datas, valores e destino dos itens.

Bens consolidado: é um registro dos bens patrimoniais da entidade governamental, tanto móveis quanto imóveis. As informações sobre bens consolidado geralmente incluem descrição, valor, localização e situação dos bens.

Bens móveis: são registros dos bens patrimoniais da entidade governamental que podem ser movidos, como veículos, equipamentos e mobiliário. As informações sobre bens móveis geralmente incluem descrição, valor, número de série e data de aquisição.

Bens imóveis: são registros dos bens patrimoniais da entidade governamental que não podem ser movidos, como terrenos e prédios. As informações sobre bens imóveis geralmente incluem descrição, valor, localização e área construída.

Frota de veículos: é um registro dos veículos utilizados pela entidade governamental, seja para transporte de pessoas, mercadorias ou serviços públicos. As informações sobre frota de veículos geralmente incluem modelo, placa, ano de fabricação e quilometragem.

Orçamento de receitas: é um registro das previsões de receitas da entidade governamental para um determinado período. As informações sobre orçamento de receitas geralmente incluem fontes de arrecadação, valores e período de vigência.

Execução de receitas: é um registro das receitas efetivamente arrecadadas pela entidade governamental em um determinado período. As informações sobre execução de receitas geralmente incluem fontes de arrecadação, valores e período de referência.

Orçamento de Despesas: é a previsão dos gastos que um órgão público terá durante um período determinado. No portal da transparência, essa informação é detalhada por categoria de despesa, programa e unidade orçamentária, possibilitando que o cidadão tenha uma visão ampla dos recursos que serão utilizados e para quais finalidades.

Empenhos: são o registro de um compromisso de gastos assumido pelo órgão público, que será pago posteriormente. Essa informação é importante para que o cidadão possa acompanhar os gastos realizados pelo órgão e verificar se estão dentro do orçamento previsto.

Liquidações: são a confirmação de que uma despesa prevista foi efetivamente realizada pelo órgão público. Essa informação é importante para que o cidadão possa verificar se o recurso público foi utilizado conforme o previsto e se a execução do gasto foi adequada.

Pagamentos: são o registro do efetivo pagamento de uma despesa realizada pelo órgão público. Essa informação é importante para que o cidadão possa acompanhar se os gastos foram pagos dentro do prazo estabelecido e se o valor efetivamente pago corresponde ao valor previsto.

Transferências Extraorçamentárias: são as transferências de recursos financeiros entre entidades governamentais que não estão previstas no orçamento anual. Essa informação é importante para que o cidadão possa verificar se essas transferências estão sendo realizadas de forma adequada e transparente.

Transferências Intraorçamentárias: são as transferências de recursos financeiros entre unidades orçamentárias dentro do mesmo órgão governamental. Essa informação é importante para que o cidadão possa acompanhar como estão sendo distribuídos os recursos dentro do órgão e se estão sendo utilizados de forma eficiente.

Servidores: são as informações sobre os funcionários públicos que trabalham no órgão governamental. No portal da transparência, é possível encontrar dados sobre a estrutura organizacional do órgão, a quantidade de servidores por cargo e os seus salários, entre outras informações. Essa informação é importante para que o cidadão possa avaliar a eficiência e a qualidade dos serviços prestados pelo órgão.



## Authors

| [<img src="https://github.com/pedropberger.png?size=115" width=115><br><sub>@pedropberger</sub>](https://github.com/pedropberger) | [<img src="https://github.com/mwildemberg.png?size=115" width=115><br><sub>@mwildemberg</sub>](https://github.com/mwildemberg) |
| :---: | :---: |
