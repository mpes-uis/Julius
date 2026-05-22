# Catálogo de Provedores de API

> Documento vivo. Atualizar quando descobrir mais sobre uma API, ou ao adicionar um provider novo.

Cada Portal da Transparência das prefeituras do Espírito Santo é fornecido por uma empresa/plataforma. Os padrões de API diferem entre elas.

## Resumo

| Provider | Status v2 | Municípios | Padrão de URL |
|---|---|---|---|
| [`portaltp`](#portaltp) | a implementar (Fase 2) | ~55 | `https://{slug}-es.portaltp.com.br/api/...` |
| [`tectrilha`](#tectrilha) | a implementar (Fase 3) | 7 | `https://wstransparencia.{municipio}.es.gov.br/api/...` |
| [`agape`](#agape) | a implementar (Fase 3) | ~5 | `https://...transparencia/api/...` |
| [`alphatec`](#alphatec) | a implementar (Fase 3) | ~3 | `https://...transparencia/api/...` |
| [`cr2`](#cr2) | a implementar (Fase 4) | 1 | `https://{municipio}.cr2.site/` |
| [`portalfacil`](#portalfacil) | a implementar (Fase 4) | ~3 | `https://dadosabertos-portalfacil.azurewebsites.net/{id}` |

---

## PortalTP

**Status:** documentação descoberta, a implementar.

**Doc oficial:** disponível em cada portal no path `/api/dadosabertos.aspx`. Exemplo: [https://santamariadejetiba-es.portaltp.com.br/api/dadosabertos.aspx](https://santamariadejetiba-es.portaltp.com.br/api/dadosabertos.aspx).

### Padrões de URL coexistentes

Há **duas gerações** de API no PortalTP:

**Padrão A (legado):** `{base}/{Grupo}/Get{Recurso}?ano=YYYY&mes=MM`
- Ex: `https://afonsoclaudio-es.portaltp.com.br/api/Compras/GetLicitacoes?ano=2024&mes=01`
- Usado no código v1 (legacy).

**Padrão B (novo, documentado):** `{base}/api/{grupo}/api-{recurso}.aspx?Ano=YYYY&Mes=MM&Formato=JSON`
- Ex: `https://santamariadejetiba-es.portaltp.com.br/api/compras/api-licitacoes.aspx?Ano=2024&Mes=01&Formato=JSON`

> **A definir na Fase 2:** testar ambos padrões em uma prefeitura real e escolher. Preferência pelo padrão B (documentado oficialmente).

### Endpoints conhecidos (padrão B)

| Grupo | Path | Conteúdo |
|---|---|---|
| Compras | `api-licitacoes.aspx` | Licitações |
| Compras | `api-contratos.aspx` | Contratos e aditivos |
| Compras | `api-convenios.aspx` | Convênios |
| Compras | `api-atas.aspx` | Atas |
| Compras | `api-contratosFiscais.aspx` | Fiscais de contrato |
| Compras | `api-ordemcompras.aspx` | Ordens de compra |
| Compras | `api-dispensasemergenciais.aspx` | Dispensas emergenciais |
| Compras | `api-contratosemergenciais.aspx` | Contratos emergenciais |
| Despesas | `api-empenhos.aspx` | Empenhos e favorecidos |
| Despesas | `api-liquidacoes.aspx` | Liquidações |
| Despesas | `api-pagamentos.aspx` | Pagamentos |
| Despesas | `api-ordemcronologica.aspx` | Ordem cronológica |
| Despesas | `api-ordemcronologicaAPagar.aspx` | A pagar |
| Despesas | `api-programaacao.aspx` | Programas/projetos |
| Despesas | `api-restospagar.aspx` | Restos a pagar |
| Despesas | `api-subvencoes.aspx` | Subvenções |
| Despesas | `api-diarias.aspx` | Diárias |
| Despesas | `api-obras.aspx` | Obras |
| Despesas | `api-despesasemergenciais.aspx` | Despesas emergenciais |
| Orçamento | `api-orcamentoreceitas.aspx` | Orçamento de receitas |
| Orçamento | `api-orcamentodespesas.aspx` | Orçamento de despesas |
| Receitas | `api-execucaoreceitas.aspx` | Receita realizada |
| Receitas | `api-dividaativa.aspx` | Dívida ativa |
| Materiais | `api-entradas.aspx` | Entradas de estoque |
| Materiais | `api-saidas.aspx` | Saídas de estoque |
| Materiais | `api-patrimonio.aspx` | Patrimônio |
| Bens | `api-moveis.aspx` | Bens móveis |
| Bens | `api-imoveis.aspx` | Bens imóveis |
| Frota | `api-veiculos.aspx` | Frota |
| Repasses | `api-conveniosfirmados.aspx` | Convênios firmados |
| Repasses | `api-conveniosrecebidos.aspx` | Convênios recebidos |
| Repasses | `api-extraorcamentarias.aspx` | Extra-orçamentárias |
| Repasses | `api-intraorcamentarias.aspx` | Intra-orçamentárias |
| Repasses | `api-duodecimo.aspx` | Duodécimo |
| Pessoal | `api-servidores.aspx` | Servidores |
| Pessoal | `api-servidoresSecretaria.aspx` | Servidores por secretaria |
| Pessoal | `api-planocarreiras.aspx` | Plano de cargos |
| Pessoal | `api-estagiarios.aspx` | Estagiários |
| Pessoal | `api-cargosvagas.aspx` | Cargos e vagas |
| Pessoal | `api-cargosconfianca.aspx` | Cargos comissionados |

### Parâmetros comuns

| Param | Valores | Obrigatório |
|---|---|---|
| `Ano` | `All`, `2017`-`2026` | sim |
| `Mês` | `All`, `Janeiro`-`Dezembro` | depende do endpoint |
| `Formato` | `JSON`, `TXT`, `CSV`, `XML` | recomendado: `JSON` |

### Outras notas

- Sem autenticação.
- Sem paginação documentada.
- Encoding: `utf-8` (alguns endpoints retornam BOM — usar `utf-8-sig`).
- Licença dos dados: CC0 1.0.
- Resposta: array JSON de objetos. Se vazio, array vazio (não 404).

---

## Tectrilha

**Status:** doc não coletada formalmente, mas inferência do código legado.

**Padrão de URL:** `https://wstransparencia.{municipio}.es.gov.br/api/{recurso}?unidadeGestoraId={id}&exercicio={ano}`

### Particularidades

- Parâmetro `unidadeGestoraId` específico — vem do CSV `data/prefeituras.csv` (coluna `unidadegestora`).
- Frequência: anual (não tem `mes`). Exceção: endpoint `pessoal` tem `&periodo=`.
- Endpoint `bensImoveis` não tem parâmetros (sem query string).

### Endpoints conhecidos (de [legacy/data/assuntos_tectrilha.csv](legacy/data/assuntos_tectrilha.csv))

| Recurso | Parâmetros |
|---|---|
| `contratos` | `unidadeGestoraId`, `exercicio` |
| `despesa` | `exercicio` |
| `diarias` | `unidadeGestoraId`, `exercicio` |
| `convenios` | `unidadeGestoraId`, `exercicio` |
| `captacoes` | `exercicio` |
| `bensmoveis` | `exercicio` |
| `passagens` | `unidadeGestoraId`, `exercicio` |
| `bensImoveis` | (nenhum) |
| `pessoal` | `exercicio`, `periodo` |
| `receitas` | `unidadeGestoraId`, `exercicio` |

> **A coletar na Fase 3:** doc oficial, lista completa de endpoints, lista completa de unidades gestoras por município.

---

## Agape

**Status:** doc não coletada formalmente.

**Padrão de URL:** observado no legado — base no portal de cada prefeitura, endpoints em snake_case.

### Endpoints conhecidos (de [legacy/data/endpoints_agape.txt](legacy/data/endpoints_agape.txt))

`orcamento_receita`, `orcamento_despesa`, `receitas`, `empenhos`, `liquidacoes`, `pagamentos`, `extra`, `licitacoes`, `atas`, `contratos`, `servidores`, `diarias_passagens`, `documentos`, `atos_controle`, `notasfiscais`, `auxilio_alimentacao`, `patrimonios`.

### Parâmetros

- `ano`, `mes` (no padrão `?ano=YYYY&mes=MM`).

> **A coletar na Fase 3.**

---

## Alphatec

**Status:** doc não coletada formalmente.

Mesmo padrão do Agape segundo o legado — provavelmente herdam ou compartilham infra. Implementação v2 deve herdar de `Agape` se confirmado.

> **A coletar na Fase 3.**

---

## CR2

**Status:** não implementado, **sem doc**.

**Exemplo:** [https://conceicaodabarra.cr2.site/](https://conceicaodabarra.cr2.site/) (única prefeitura usando, Conceição da Barra).

> **A investigar na Fase 4.** Possivelmente não tem API REST tradicional — pode requerer scraping ou descoberta de endpoints internos.

---

## PortalFacil

**Status:** não implementado, **sem doc**.

**Exemplos:**
- Dores do Rio Preto: `https://dadosabertos-portalfacil.azurewebsites.net/391`
- Iconha: `https://dadosabertos-portalfacil.azurewebsites.net/209`
- Itapemirim: `https://dadosabertos-portalfacil.azurewebsites.net/438`

URL parametrizada por ID numérico de município.

> **A investigar na Fase 4.** Provavelmente uma API REST baseada no ID.
