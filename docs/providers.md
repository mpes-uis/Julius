# Catálogo de Provedores de API

> Documento vivo. Atualizar quando descobrir mais sobre uma API, ou ao adicionar um provider novo.

Cada Portal da Transparência das prefeituras do Espírito Santo é fornecido por uma empresa/plataforma. Os padrões de API diferem entre elas.

## Resumo

| Provider | Status v2 | Municípios | Padrão de URL |
|---|---|---|---|
| [`portaltp`](#portaltp) | ✅ implementado (Fase 2) | 60 | `https://{slug}-es.portaltp.com.br/api/{Grupo}/Get{Recurso}?ano=YYYY&mes=MM` |
| [`tectrilha`](#tectrilha) | ✅ implementado (Fase 3) | 6 | `https://wstransparencia.{municipio}.es.gov.br/api/{recurso}?exercicio=YYYY` |
| [`agape`](#agape) | ✅ implementado (Fase 3) | 5 | `https://...transparencia/api/{recurso}?ano=YYYY&mes=MM&pagina=N` |
| [`alphatec`](#alphatec) | ✅ implementado (Fase 3) | 3 | Idêntico ao Agape |
| [`cr2`](#cr2) | ⏸️ adiado — sem API acessível | 1 | WordPress sem REST API de transparência exposta |
| [`portalfacil`](#portalfacil) | ⏸️ adiado — sem API acessível | 3 | Angular SPA; URL de backend não descoberta sem DevTools |

**Cobertura atual:** 74/78 municípios (95%). Fases 0–3 concluídas.

---

## PortalTP

**Status:** ✅ implementado (Fase 2).

**Doc oficial:** disponível em cada portal no path `/api/dadosabertos.aspx`. Exemplo: `https://santamariadejetiba-es.portaltp.com.br/api/dadosabertos.aspx`.

### Padrão de URL (confirmado)

Há duas gerações de API no PortalTP. Testou-se ambas contra Afonso Cláudio em fev/2024:

- **Padrão A (legado — em uso na v2):** `{base}/{Grupo}/Get{Recurso}?ano=YYYY&mes=MM`
  - Ex: `https://afonsoclaudio-es.portaltp.com.br/api/Compras/GetLicitacoes?ano=2024&mes=01`
  - **Retorna JSON corretamente.** Escolhido para a v2 por compatibilidade testada.
- **Padrão B (novo, documentado):** `{base}/api/{grupo}/api-{recurso}.aspx?Ano=YYYY&Mes=MM&Formato=JSON`
  - Alguns portais redirecionam silenciosamente para o Padrão A.

O YAML `data/endpoints/portaltp.yaml` usa paths do Padrão A (ex: `Compras/GetLicitacoes`).

### Endpoints implementados

47 endpoints em `data/endpoints/portaltp.yaml`, agrupados por frequência:

**Mensais** (parâmetros `ano` + `mes`): `licitacoes`, `contratos`, `convenios`, `atas`, `contratos_fiscais`, `ordem_compras`, `dispensas_emergenciais`, `contratos_emergenciais`, `empenhos`, `liquidacoes`, `pagamentos`, `ordem_cronologica`, `ordem_cronologica_a_pagar`, `programacao`, `restos_pagar`, `subvencoes`, `diarias`, `obras`, `despesas_emergenciais`, `execucao_receitas`, `divida_ativa`, `entradas_estoque`, `saidas_estoque`, `bens_moveis`, `veiculos`, `convenios_firmados`, `convenios_recebidos`, `extra_orcamentarias`, `intra_orcamentarias`, `duodecimo`, `servidores`, `servidores_secretaria`, `estagiarios`.

**Anuais** (parâmetro `ano` apenas): `orcamento_receita`, `orcamento_despesa`, `patrimonio`, `bens_imoveis`, `plano_carreiras`, `cargos_vagas`, `cargos_confianca`.

### Parâmetros

| Param | Tipo | Obrigatório |
|---|---|---|
| `ano` | inteiro (ex: `2024`) | sempre |
| `mes` | dois dígitos com zero (ex: `01`) | só endpoints mensais |

### Notas técnicas

- Sem autenticação.
- Sem paginação — retorna array completo.
- Encoding: `utf-8-sig` (BOM) em alguns endpoints. O `parse()` trata via `decode("utf-8-sig")`.
- Resposta vazia: array `[]` (não 404).
- Resposta unitária: às vezes retorna dict em vez de array — `parse()` normaliza para `list[dict]`.
- Licença: CC0 1.0.

---

## Tectrilha

**Status:** ✅ implementado (Fase 3).

**Doc oficial:** `https://wstransparencia.{municipio}.es.gov.br/Help` (ex: Vitória em `https://wstransparencia.vitoria.es.gov.br/Help`).

**Municípios (6):** Vitória, Vila Velha, Serra, Cariacica, Viana, Guarapari.

### Padrão de URL

```
https://wstransparencia.{municipio}.es.gov.br/{recurso}?exercicio=YYYY[&unidadeGestoraId=N]
```

O provider strips o sufixo `/api` da URL de base para montar os paths corretamente
(ex: `https://wstransparencia.vitoria.es.gov.br/api` → base `https://wstransparencia.vitoria.es.gov.br`).

### Particularidades

- **`exercicio`** em vez de `ano`. Sem parâmetro `mes` na maioria dos endpoints.
- **`unidadeGestoraId`**: inteiro da coluna `unidadegestora` em `data/prefeituras.csv`. Obrigatório em alguns endpoints (flag `requires_ug: true` no YAML).
- **`periodo=MM`**: endpoint `pessoal` usa mês como `periodo` (flag `frequency: monthly` no YAML).
- **`bensImoveis`**: snapshot sem parâmetros — flag `no_year_param: true` no YAML.
- **Endpoint `licitacoes`**: usa **POST** com body JSON `{"unidadeGestoraId": X, "exercicio": YYYY}`. Detecção automática pelo path tail em `_POST_PATHS = frozenset({"licitacoes"})`.

### Endpoints (12 em `data/endpoints/tectrilha.yaml`)

| Recurso | Método | `requires_ug` | `no_year_param` | Frequência |
|---|---|---|---|---|
| `contratos` | GET | ✓ | — | anual |
| `despesa` | GET | — | — | anual |
| `diarias` | GET | ✓ | — | anual |
| `convenios` | GET | ✓ | — | anual |
| `captacoes` | GET | — | — | anual |
| `bensmoveis` | GET | — | — | anual |
| `passagens` | GET | ✓ | — | anual |
| `bensImoveis` | GET | — | ✓ | anual |
| `pessoal` | GET | — | — | mensal |
| `receitas` | GET | ✓ | — | anual |
| `licitacoes` | POST | ✓ | — | anual |
| `atas` | GET | ✓ | — | anual |

---

## Agape

**Status:** ✅ implementado (Fase 3).

**Municípios (5):** Cachoeiro de Itapemirim, Colatina, Linhares, São Mateus, Brejetuba.

### Padrão de URL

```
{base}/{recurso}?ano=YYYY[&mes=MM][&pagina=N]
```

### Paginação

Agape é o único provider paginado (`paginated: ClassVar[bool] = True`). Estrutura da resposta:

```json
{
  "pagina_total": 42,
  "pagina_atual": 1,
  "registro_total": 420,
  "registros": [...]
}
```

O `fetch_all()` itera páginas via `&pagina=N` até `pagina >= pagina_total`. Cap configurável em `_MAX_PAGES = 5000`. Retry por página via tenacity.

### Endpoints (16 em `data/endpoints/agape.yaml`)

**Mensais:** `orcamento_receita`, `orcamento_despesa`, `receitas`, `empenhos`, `liquidacoes`, `pagamentos`, `extra`, `notasfiscais`, `auxilio_alimentacao`.

**Anuais:** `licitacoes`, `atas`, `contratos`, `servidores`, `diarias_passagens`, `documentos`, `atos_controle`, `patrimonios`.

### Notas técnicas

- Brejetuba retorna 500 para `servidores`/`notasfiscais` e 404 para `auxilio_alimentacao` — problema no servidor da prefeitura, não no Julius. O runner registra como falha no checkpoint e segue.
- Resposta sem paginação (array simples): `parse()` trata ambos os formatos.

---

## Alphatec

**Status:** ✅ implementado (Fase 3).

**Municípios (3):** Sooretama, Ibiraçu, Itaguaçu.

Mesma estrutura de API do Agape (paginação idêntica, mesmos endpoints). Implementado como subclasse de `Agape` com YAML próprio (`data/endpoints/alphatec.yaml` — cópia do agape.yaml).

```python
class Alphatec(Agape):
    name: ClassVar[str] = "alphatec"
```

---

## CR2

**Status:** ⏸️ adiado — API inacessível sem engenharia reversa com browser.

**Municípios (1):** Conceição da Barra (`https://conceicaodabarra.cr2.site/`).

### Diagnóstico (2026-06-05)

CR2 é WordPress puro com portal de transparência montado via plugin ou tabelas customizadas. Não expõe REST API de transparência com padrão público documentado. Os dados aparecem em tabelas HTML renderizadas pelo PHP, sem endpoint JSON identificável via request direto.

### Como desbloquear

1. Abrir DevTools no Chrome/Firefox.
2. Ir para `conceicaodabarra.cr2.site`, navegar até "Licitações" ou "Contratos".
3. Na aba Network, filtrar por XHR/Fetch ou JSON.
4. Identificar URL de backend e parâmetros.
5. Criar `data/endpoints/cr2.yaml` e `julius/providers/cr2.py`.

---

## PortalFacil

**Status:** ⏸️ adiado — API inacessível sem engenharia reversa com browser.

**Municípios (3):** Dores do Rio Preto (ID 391), Iconha (ID 209), Itapemirim (ID 438).

**Base URL observada:** `https://dadosabertos-portalfacil.azurewebsites.net/{id}`

### Diagnóstico (2026-06-05)

Angular SPA com servidor IIS que redireciona todas as rotas para `index.html`. GET direto no padrão `/{id}/licitacoes` retorna a página HTML do SPA. A URL da API backend está embutida no JavaScript minificado sem padrão claro.

### Como desbloquear

1. Abrir DevTools no Chrome/Firefox.
2. Acessar `https://dadosabertos-portalfacil.azurewebsites.net/391` (Dores do Rio Preto).
3. Navegar para uma seção de dados (Licitações, Contratos).
4. Na aba Network, filtrar por XHR/Fetch, capturar URL e parâmetros da API.
5. Criar `data/endpoints/portalfacil.yaml` e `julius/providers/portalfacil.py`.
