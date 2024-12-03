# Julius

Ferramenta que captura os dados dos Portais da Transparência e aplica métricas e tipologias conhecidas para averiguar indicadores de improbidade administrativa (pessoas com dois cargos públicos - além do que é assegurado por lei - contratos e compras com preços elevados, etc) ou práticas ruims de portais.

# Sobre o Projeto

Esse projeto iniciou como uma iniciativa dos servidores da Unidade de Inovação do Ministério Público do Estado do Espírito Santo para aplicar tecnicas de ciências de dados e automação, ainda em 2018. A ideia era extrair dados dos portais para ampliar possibilidades de investigação de improbidade administrativa, avaliar os dados disponibilizados pelos portais (nos conformes da Lei 12.527/2011) e aplicar tipologias que dão indícios de práticas de corrupção.

O nome "Julius" faz referência e homenagem ao personagem Julius da série "Todo Mundo Odeia o Cris", pois a primeira tipologia testada era a verificação de pessoas com dois empregos (porém no caso, acumulação indevida de cargos públicos), e também por coincidentemente ser o segundo nome do primeiro colaborador do projeto, o dev Fabrício Julius Correia de Almeida.

Por ser uma ferramenta de útil aprendizado e por utilizar exclusivamente dados públicos governamentais, em 2023 optamos recomeçar o projeto um iniciativa aberta, com desenvolvimento e compartilhamento desvinculado de burcracias e disponibilizando os códigos aqui no Github. A ideia é a partir do que já funciona, testar, aprimorar, criar e aperfeiçoar os códigos e funções com técnicas de Engenharia de Dados, Inteligência Artificial e boas prátivas de desenvolvimento.

Os códigos são simples e as ferramentas são as mais básicas disponíveis para engenharia de dados, possibilitando que seja simples de replicar e um ambiente de aprendizagem para quem quiser aprender, seja ele servidor público, universitário, aluno de algum curso online ou só um entusiasta.

# Como funciona?

Julius funciona como um algoritmo de automação que captura dados dos Portais da Transparência por meio de suas APIs.

Cada portal da transparência possui API's possibilitando a extração dos dados nele contidos. Essas API's remetem a dados de estrutura semelhante, de acordo com a categoria do que é dispobilizado (remuneração, licitações, contratos, etc).

Em tese, quando todos municípios e entidades governamentais atingirem um "nível máximo" de transparência, todos terão dados similares sendo disponibilizados, variando conforme o tipo de entidade, por exemplo: Câmaras municipais usualmente não possuem estrutura de receitas e arrecadação, empresas públicas tem processos licitatórios diferenciados, etc). Partindo desse princípio que a quantidade de APIs e dados cresce. 

Nem todos os dados são estruturados. Contratos, editais, atas e notas de empenho são exemplos de dados não estruturados. Daí que cresce a complexidade. Cresce o desafio. Mas trabalhar em cima disso agrega valor aos dados e as ferramentas. Dados não estruturados podem ser armazenados em bancos NoSQL, seus textos podem ser extraídos para utilização e alimentação de aplicações de LPN, por exemplo.

O Julius consome as APIs de todos portais da transparência implementados (até o momento, os 78 municípios do estado do Espírito Santo) e armazena eles em um banco de dados. Esse banco fica disponível para utilização dentro do container docker. Daí você pode fazer download, compartilhar, consumir em suas aplicações ou utilizar como achar melhor.

![image](https://user-images.githubusercontent.com/98188778/235966779-ca954c0f-3407-4c7c-b548-2feb677fe1fc.png)


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

Ative seu o seu shell de linha de comando (Bash, PowerShell, CMD) no diretório em que está o dockerfile.

Execute o seguinte comando para criar um volume compartilhado, onde poderá posteriormente salvar/cambiar arquivos com o aplicativo:

    docker volume create dados

Crie a imagem do container a partir do dockerfile:

    docker build -t julius-img .
    
E finalmente, coloque o container em operação:

    docker run -d --name julius julius-img

Pronto!

O container criado fara do download dos códigos necessários e iniciará a execução do aplicativo. Em seu storage você poderá encontrar o banco de dados SQLite. Agora é só aguardar algumas horas e os dados estarão salvos no banco.

Os comandos acima não são uma regra, você pode personalizar o volume, a imagem e o containter utilizando outros comandos docker que achar interessantes.

# Desafio

Não existe um padrão universal ou regra de outro para funcionamento ou formato dos portais da transparência. O mesmo vale para suas APIs de consumo. Ou seja, mesmo que todo portal tenha que disponibilizar os dados exigidos na LAI, poderá disponibilizar na forma que quiser, ou lhe for mais prático.

Por exemplo: A API da prefeitura X retorna os dados referentes a folha de pagamento em um arquivo JSON e a prefeitura Y retorna em uma tabela em *.csv, ou, a prefeitura Z traz em sua sessão de contratos após o CNPJ o número do processo licitatório que originou o contrato, já a câmara Z traz apenas o CNPJ.

Nenhum dos casos acima fere a LAI, já que os dados estão disponibilizados. Porém exige que seja feita uma engenharia de dados caso você queira fazer um comparativo ou inserir todos dados em uma mesma base. É esse o quebra cabeça que queremos resolver.

A boa notícia? por sorte ou acaso, poucas empresas/formatos são aplicados a diversos portais. No estado do Espírito Santo, por exemplo, temos 3 modelos de portais para os 78 municípios. Então basta escrever o código para puxar os dados da API de um município, que você consegue replicar para uma dezena deles.

*Queremos completar o Brasil. Já temos 78 municípios, faltam só 5.490! Bora trabalhar juntos nessa?*

# Desafios computacionais

Você é dev? ajude a gente a resolver esses problemas.

- Multithreading: Levam algumas boas horas para concluir a extração de dados. Isso pq limitamos do dados a partir de 2020. Multithreading, processos paralelos e outras formas de execução de código em paralelo aparentemente seriam a solução. Mas quando tentei implementar tivemos o IP bloqueado obviamente por muitos requests oriundos de uma mesma fonte.

- Organização do código: Ainda estamos aprendendo a escrever códigos lindos e legíveis. Qualquer implementação (ou dica) que aprimore a estética e leitura do código é bem vinda. Até organizar as pastas de forma eficiente foi difícil de decidir.

- Colaboração: Merge, branch, pull, tudo isso é novidade para a gente. Precisamos de boas almas para nos guiar em como fazer isso melhor para evoluirmos a ferramenta.

- Modularização: O projeto se inicia com 78 portais de 4 formatos diferentes. Como podemos otimizar o código de forma que as pessoas possam adicionar mais portais e formatos e incluir isso na aplicação sem bagunçar o funcionamento? Temos um plano, mas sei que tem pessoas bem mais preparadas e experientes para isso.

- Dados não estruturados: Alguns portais trazem arquivos em pdf, word e texto para disponibilizar na íntegra os contratos, editais de licitação, notas de empenho, etc. Isso é um mundo para ser explorado. Seja aplicando um algoritmo de OCR para extrair texto de pdfs ou em pensar e decidir a melhor forma de armazenar e trabalhar com esses dados.

# Todo

Migrando pro Projects

# Portais da Transparência

Um Portal da Transparência é um site que tem por finalidade veicular dados e informações detalhados sobre a execução orçamentária dos órgãos públicos brasileiros e entidades do terceiro setor que recebem repasses públicos. A ferramenta publica também dados sobre assuntos transversais ou que estejam relacionados à função da maioria desses órgãos.

Uma tendência dos Portais da Transparência brasileiros é a disponibilização de APIs para facilitar a leitura/extração dos dados por meios automatizados, essa boa prática (https://www.portaltransparencia.gov.br/api-de-dados) tem se expandido rapidamente tanto nos portais criados pelos órgãos públicos como pelas empresas prestadoras de serviço. Espera-se que cada portal da transparência posssuirá ao menos uma API para disponibilizar todos os dados exigidos pela Lei de Acesso a Informação.

Os dados contidos em portais são estruturados (tabelas de salários, pagamentos a terceiros, repasses, receitas financeiras) e não estruturados (contratos, editais, notas de empenho), variando de acordo com a esfera pública, tipo de autarquia e outros elementos.

Os itens abaixo são comumente encontrados nos portais da transparência, e cada um deles representa uma seção específica com informações sobre determinado assunto relacionado às atividades de uma entidade governamental:

- Licitações: são processos administrativos que visam selecionar a melhor proposta para a contratação de serviços, obras ou fornecimento de bens. As informações sobre licitações geralmente incluem datas, modalidades, valores, empresas participantes e vencedoras.

- Contratos: são acordos formais firmados entre uma entidade governamental e um terceiro, seja ele uma pessoa física ou jurídica, para a prestação de serviços, obras ou fornecimento de bens. As informações sobre contratos geralmente incluem valores, objeto do contrato, prazos e condições.

- Atas: são documentos que registram os principais fatos e decisões de uma reunião ou sessão pública. As informações sobre atas geralmente incluem datas, horários, pauta e resoluções.

- Ordem de compras: são documentos emitidos para a aquisição de materiais ou serviços. As informações sobre ordem de compras geralmente incluem datas, valores, fornecedores e descrição dos itens adquiridos.

- Materiais entradas: são registros dos materiais recebidos pela entidade governamental, seja por meio de compras, doações ou outras formas de aquisição. As informações sobre materiais entradas geralmente incluem datas, valores, fornecedores e descrição dos itens.

- Materiais saídas: são registros dos materiais que deixaram a entidade governamental, seja por meio de transferências, descartes ou outras formas de baixa. As informações sobre materiais saídas geralmente incluem datas, valores e destino dos itens.

- Bens consolidado: é um registro dos bens patrimoniais da entidade governamental, tanto móveis quanto imóveis. As informações sobre bens consolidado geralmente incluem descrição, valor, localização e situação dos bens.

- Bens móveis: são registros dos bens patrimoniais da entidade governamental que podem ser movidos, como veículos, equipamentos e mobiliário. As informações sobre bens móveis geralmente incluem descrição, valor, número de série e data de aquisição.

- Bens imóveis: são registros dos bens patrimoniais da entidade governamental que não podem ser movidos, como terrenos e prédios. As informações sobre bens imóveis geralmente incluem descrição, valor, localização e área construída.

- Frota de veículos: é um registro dos veículos utilizados pela entidade governamental, seja para transporte de pessoas, mercadorias ou serviços públicos. As informações sobre frota de veículos geralmente incluem modelo, placa, ano de fabricação e quilometragem.

- Orçamento de receitas: é um registro das previsões de receitas da entidade governamental para um determinado período. As informações sobre orçamento de receitas geralmente incluem fontes de arrecadação, valores e período de vigência.

- Execução de receitas: é um registro das receitas efetivamente arrecadadas pela entidade governamental em um determinado período. As informações sobre execução de receitas geralmente incluem fontes de arrecadação, valores e período de referência.

- Orçamento de Despesas: é a previsão dos gastos que um órgão público terá durante um período determinado. No portal da transparência, essa informação é detalhada por categoria de despesa, programa e unidade orçamentária, possibilitando que o cidadão tenha uma visão ampla dos recursos que serão utilizados e para quais finalidades.

- Empenhos: são o registro de um compromisso de gastos assumido pelo órgão público, que será pago posteriormente. Essa informação é importante para que o cidadão possa acompanhar os gastos realizados pelo órgão e verificar se estão dentro do orçamento previsto.

- Liquidações: são a confirmação de que uma despesa prevista foi efetivamente realizada pelo órgão público. Essa informação é importante para que o cidadão possa verificar se o recurso público foi utilizado conforme o previsto e se a execução do gasto foi adequada.

- Pagamentos: são o registro do efetivo pagamento de uma despesa realizada pelo órgão público. Essa informação é importante para que o cidadão possa acompanhar se os gastos foram pagos dentro do prazo estabelecido e se o valor efetivamente pago corresponde ao valor previsto.

- Transferências Extraorçamentárias: são as transferências de recursos financeiros entre entidades governamentais que não estão previstas no orçamento anual. Essa informação é importante para que o cidadão possa verificar se essas transferências estão sendo realizadas de forma adequada e transparente.

- Transferências Intraorçamentárias: são as transferências de recursos financeiros entre unidades orçamentárias dentro do mesmo órgão governamental. Essa informação é importante para que o cidadão possa acompanhar como estão sendo distribuídos os recursos dentro do órgão e se estão sendo utilizados de forma eficiente.

- Servidores: são as informações sobre os funcionários públicos que trabalham no órgão governamental. No portal da transparência, é possível encontrar dados sobre a estrutura organizacional do órgão, a quantidade de servidores por cargo e os seus salários, entre outras informações. Essa informação é importante para que o cidadão possa avaliar a eficiência e a qualidade dos serviços prestados pelo órgão.


## Colaboradores

| [<img src="https://github.com/pedropberger.png?size=115" width=115>](https://github.com/pedropberger) <br> [@pedropberger](https://github.com/pedropberger) | [<img src="https://github.com/mwildemberg.png?size=115" width=115>](https://github.com/mwildemberg) <br> [@mwildemberg](https://github.com/mwildemberg) | [<img src="https://github.com/iaraarruda.png?size=115" width=115>](https://github.com/iaraarruda) <br> [@iaraarruda](https://github.com/iaraarruda) | [<img src="https://github.com/xMerisio.png?size=115" width=115>](https://github.com/xMerisio) <br> [@xMerisio](https://github.com/xMerisio) | [<img src="https://github.com/HeitorQuartezani.png?size=115" width=115>](https://github.com/HeitorQuartezani) <br> [@HeitorQuartezani](https://github.com/HeitorQuartezani) |
|:---:|:---:|:---:|:---:|:---:|

Ajude a gente a evoluir e entre pro grupo.

