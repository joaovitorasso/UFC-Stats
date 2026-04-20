# Documentação Técnica — UFC Lakehouse Pipeline

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Estrutura de Arquivos](#2-estrutura-de-arquivos)
3. [Configuração](#3-configuração)
4. [Etapa 1 — Coleta e Bronze (arquivos)](#4-etapa-1--coleta-e-bronze-arquivos)
   - [4.1 Ponto de entrada](#41-ponto-de-entrada)
   - [4.2 Coleta do índice de eventos](#42-coleta-do-índice-de-eventos)
   - [4.3 Download do HTML dos eventos](#43-download-do-html-dos-eventos)
   - [4.4 Bronze de eventos](#44-bronze-de-eventos)
   - [4.5 Bronze de lutas](#45-bronze-de-lutas)
   - [4.6 Download do HTML dos lutadores](#46-download-do-html-dos-lutadores)
   - [4.7 Bronze de lutadores](#47-bronze-de-lutadores)
5. [Etapa 2 — Carga no Banco (SQL Server)](#5-etapa-2--carga-no-banco-sql-server)
   - [5.1 Ponto de entrada do loader](#51-ponto-de-entrada-do-loader)
   - [5.2 Leitura dos arquivos bronze](#52-leitura-dos-arquivos-bronze)
   - [5.3 Preparação bronze para o banco](#53-preparação-bronze-para-o-banco)
   - [5.4 Upsert bronze no banco](#54-upsert-bronze-no-banco)
   - [5.5 Construção da Silver](#55-construção-da-silver)
   - [5.6 Construção da Gold (métricas)](#56-construção-da-gold-métricas)
   - [5.7 Controle de execução (ETL)](#57-controle-de-execução-etl)
6. [Arquitetura de Dados — Camadas](#6-arquitetura-de-dados--camadas)
7. [Estrutura de Pastas em Disco](#7-estrutura-de-pastas-em-disco)
8. [Tabelas no Banco](#8-tabelas-no-banco)
9. [Utilitários Internos](#9-utilitários-internos)
10. [Fluxo Completo em Diagrama](#10-fluxo-completo-em-diagrama)

---

## 1. Visão Geral

Este projeto é um pipeline de dados estilo **Lakehouse** para coletar e estruturar estatísticas do site [ufcstats.com](http://ufcstats.com). O pipeline é dividido em dois comandos independentes:

| Comando | O que faz |
|---|---|
| `ufc-pipeline run all --dt YYYY-MM-DD` | Acessa o site, baixa HTML, gera arquivos JSONL em disco |
| `python scripts/load_lakehouse_sqlserver.py --dt YYYY-MM-DD` | Lê os JSONL, processa e carrega no SQL Server |

Os dados percorrem as seguintes camadas:

```
[ufcstats.com]
      ↓  HTTP
[RAW — HTML em disco]
      ↓  parsing
[BRONZE — JSONL em disco]
      ↓  pandas
[BRONZE — tabelas SQL Server]
      ↓  deduplicação
[SILVER — tabelas SQL Server]
      ↓  agregação
[GOLD — métricas SQL Server]
```

---

## 2. Estrutura de Arquivos

```
ufc-lakehouse/
├── .env                              # Credenciais e configurações locais (não vai pro git)
├── .env.example                      # Modelo do .env
├── pyproject.toml                    # Definição do pacote Python
│
├── configs/
│   ├── settings.yaml                 # Configurações do pipeline (limites, delays, etc.)
│   ├── sources.yaml                  # URLs de origem (ufcstats.com)
│   └── logging.yaml                  # Configuração de logs
│
├── scripts/
│   ├── load_lakehouse_sqlserver.py   # Loader: JSONL → SQL Server
│   └── migrar_banco.sql              # Script de migração de nomes de tabelas
│
└── src/ufc_pipeline/
    ├── config.py                     # Carrega settings.yaml + .env → dataclass PipelineConfig
    ├── http.py                       # Cliente HTTP com rate limit e retry
    ├── ids.py                        # Geração de IDs (SHA1, hash5)
    ├── io.py                         # Leitura/escrita de arquivos (JSON, JSONL, texto)
    ├── parsing.py                    # Helpers de HTML (BeautifulSoup) + parse de datas UFC
    ├── banco.py                      # Conexão SQL Server + metadados ETL
    ├── logger.py                     # Setup de logging via YAML
    ├── pipeline.py                   # CLI + orquestrador das etapas de coleta
    └── coleta/
        ├── eventos.py                # Coleta e parsing de eventos
        ├── lutas.py                  # Coleta e parsing de lutas
        └── lutadores.py              # Coleta e parsing de lutadores
```

---

## 3. Configuração

### `configs/settings.yaml`

Controla o comportamento do pipeline:

```yaml
data_dir: ./data                  # pasta raiz dos dados em disco

http:
  timeout_seconds: 20             # timeout por requisição HTTP
  retries: 4                      # tentativas em caso de erro
  polite_delay_seconds: 0.6       # pausa entre requisições (respeito ao servidor)
  max_requests_per_minute: 80     # limite de taxa

pipeline:
  events_lookback_days: 30        # janela incremental: busca eventos dos últimos 30 dias
  full_load_if_db_empty: true     # se banco vazio, faz carga completa histórica
  limit_events: 0                 # 0 = sem limite (útil para testes: colocar ex. 5)
  limit_fights_per_event: 0       # 0 = sem limite
  limit_fighters: 0               # 0 = sem limite
```

### `configs/sources.yaml`

Define as URLs de origem:

```yaml
ufcstats:
  completed_events_url: "http://ufcstats.com/statistics/events/completed?page=all"
  upcoming_events_url:  "http://ufcstats.com/statistics/events/upcoming?page=all"
```

### `.env`

Credenciais e overrides locais. As variáveis mais importantes:

```env
UFC_TARGET_SERVER=localhost\SQLEXPRESS
UFC_TARGET_DATABASE=UFC_Lakehouse
UFC_TARGET_USER=seu_usuario
UFC_TARGET_PASSWORD=sua_senha
UFC_BRONZE_SCHEMA=bronze
UFC_SILVER_SCHEMA=silver
UFC_GOLD_SCHEMA=gold
```

---

## 4. Etapa 1 — Coleta e Bronze (arquivos)

Comando de execução:
```bash
ufc-pipeline run all --dt 2026-04-11
```

### 4.1 Ponto de entrada

**Arquivo:** `src/ufc_pipeline/pipeline.py` → função `main()`

O CLI recebe os argumentos (`run`, `all`, `--dt`) e chama `executar_estagio("bronze", repo_root, dt="2026-04-11")`.

A função `executar_estagio` orquestra as chamadas em ordem:

```
1. eventos.coletar_index()
2. eventos.baixar_html()
3. eventos.gerar_bronze()
4. lutas.gerar_bronze()
5. lutadores.baixar_html()
6. lutadores.gerar_bronze()
```

No final imprime um JSON com os caminhos de todos os artefatos gerados.

---

### 4.2 Coleta do índice de eventos

**Arquivo:** `src/ufc_pipeline/coleta/eventos.py` → função `coletar_index()`

**O que faz:**

1. Instancia o `HttpClient` (com rate limit e retry configurados em `settings.yaml`)
2. Decide se a carga será **completa** ou **incremental**:
   - Consulta `etl.controle_pipeline` no SQL Server via `banco.py`
   - Se nunca rodou antes (sem sucesso anterior) → **carga completa** (busca todo o histórico)
   - Se já rodou → **incremental** (filtra apenas eventos dos últimos `events_lookback_days` dias)
3. Faz dois GETs:
   - `http://ufcstats.com/statistics/events/completed?page=all` → todos os eventos realizados
   - `http://ufcstats.com/statistics/events/upcoming?page=all` → eventos futuros agendados
4. Parseia o HTML de cada página via `_parse_tabela_eventos()`:
   - Localiza a tabela `<table class="b-statistics__table-events">`
   - Para cada linha extrai: nome do evento, URL, data, local
   - Gera um `event_id` via SHA1 da URL do evento
   - Gera um `event_id_hash5` (5 dígitos numéricos) para uso visual/operacional
5. Aplica filtro incremental se necessário (corta eventos mais antigos que a janela)
6. Salva o resultado em:

```
data/raw/events_index/dt=2026-04-11/events_index.jsonl
```

**Estrutura de cada linha do JSONL:**
```json
{
  "event_id": "a3f8c1d2e5b4...",        // SHA1 da URL do evento
  "event_id_hash5": "42371",            // hash visual de 5 dígitos
  "name": "UFC 310: Pantoja vs. Asakura",
  "event_url": "http://ufcstats.com/event-details/abc123",
  "event_date": "2024-12-07",
  "location": "Las Vegas, Nevada, USA",
  "status": "completed"                 // ou "upcoming"
}
```

Também salva um arquivo de metadados em:
```
data/raw/_meta/runs/{run_id}.json
```

---

### 4.3 Download do HTML dos eventos

**Arquivo:** `src/ufc_pipeline/coleta/eventos.py` → função `baixar_html()`

**O que faz:**

1. Lê o `events_index.jsonl` gerado no passo anterior
2. Para cada evento, faz GET na `event_url`
3. Salva o HTML bruto em:

```
data/raw/html/events/dt=2026-04-11/{event_id}.html
```

O HTML de cada evento contém a lista de todas as lutas daquele card, com links para as páginas individuais de cada luta. Esse HTML será consumido no passo 4.5.

---

### 4.4 Bronze de eventos

**Arquivo:** `src/ufc_pipeline/coleta/eventos.py` → função `gerar_bronze()`

**O que faz:**

1. Lê o `events_index.jsonl`
2. Adiciona o campo `ingested_at` (timestamp UTC do momento da ingestão)
3. Salva em:

```
data/bronze/events/dt=2026-04-11/events.jsonl
```

Esta é a camada Bronze de eventos — mesmos campos do índice, agora com o timestamp de quando foram processados. Este arquivo é usado pelo loader SQL Server para popular `bronze.eventos`.

---

### 4.5 Bronze de lutas

**Arquivo:** `src/ufc_pipeline/coleta/lutas.py` → função `gerar_bronze()`

**O que faz — é a etapa mais complexa da coleta:**

1. Lê o `events_index.jsonl` para saber quais eventos processar
2. Para cada evento, lê o HTML salvo em `data/raw/html/events/dt=.../{event_id}.html`
3. Chama `_parse_links_lutas()` no HTML do evento:
   - Extrai o nome do evento (`<h2 class="b-content__title">`)
   - Extrai a lista de lutas via `<tr class="b-fight-details__table-row" data-link="...">`:
     - URL de cada luta individual
     - Código de bônus (Fight of the Night=1, Performance=2, Submission=3, KO=4) — detectado pelas imagens na linha
     - Tipo de bout (normal ou title bout) — detectado pelo ícone do cinturão
4. Para cada luta, faz GET na URL individual e chama `_parse_pagina_luta()`:

   **O que extrai da página de uma luta:**

   - **Cabeçalho dos lutadores** (`_parse_cabecalho_lutadores`):
     - Nome, URL do perfil, `fighter_id` (SHA1 da URL), resultado (W/L/NC/D)
   - **Meta da luta** (`_parse_meta_luta`):
     - Método de término (ex.: "KO/TKO", "Submission", "Decision - Unanimous")
     - Round, tempo, formato de tempo, árbitro
   - **Golpes significativos por round** (`_parse_golpes_por_round`):
     - Localiza a seção "Significant Strikes" na página
     - Para cada round de cada lutador: `sig_str` (ex.: "45 of 78"), `sig_str_pct`, head, body, leg, distance, clinch, ground

5. Monta o dicionário final da luta e adiciona `ingested_at`

6. **Durante o processamento das lutas, coleta o índice de lutadores**: cada lutador encontrado tem seu `fighter_id`, `name` e `profile_url` registrados num dicionário (deduplicado por `fighter_id`)

7. Salva dois arquivos:

```
data/bronze/fights/dt=2026-04-11/fights.jsonl       ← todas as lutas
data/bronze/fighters_index/dt=2026-04-11/fighters_index.jsonl  ← índice de lutadores
```

**Estrutura de cada luta no JSONL:**
```json
{
  "fight_id": "b7d3a...",           // SHA1(event_id|red_url|blue_url|bout_order)
  "fight_id_hash5": "19283",
  "event_id": "a3f8c...",
  "event_id_hash5": "42371",
  "event_name": "UFC 310: Pantoja vs. Asakura",
  "fight_url": "http://ufcstats.com/fight-details/xyz",
  "bout_order": "1",                // posição no card (1 = luta principal)
  "bonus_code": 1,                  // null ou 1-4
  "bout": "title bout",             // ou "normal"
  "method": "Decision - Unanimous",
  "round": "5",
  "time": "5:00",
  "time_format": "5 Rnd (5-5-5-5-5)",
  "referee": "Marc Goddard",
  "fighters": [
    {
      "name": "Alexandre Pantoja",
      "profile_url": "http://ufcstats.com/fighter-details/abc",
      "fighter_id": "c9f1e...",
      "fighter_id_hash5": "73921",
      "result": "W",
      "rounds_sig_strikes": {
        "1": { "sig_str": "23 of 40", "sig_str_pct": "57%", "head": "10 of 18", ... },
        "2": { ... },
        ...
      }
    },
    { ... }   // lutador adversário
  ],
  "ingested_at": "2026-04-11T14:32:01Z"
}
```

**Estrutura do índice de lutadores:**
```json
{
  "fighter_id": "c9f1e...",
  "fighter_id_hash5": "73921",
  "name": "Alexandre Pantoja",
  "profile_url": "http://ufcstats.com/fighter-details/abc",
  "ingested_at": "2026-04-11T14:32:01Z"
}
```

---

### 4.6 Download do HTML dos lutadores

**Arquivo:** `src/ufc_pipeline/coleta/lutadores.py` → função `baixar_html()`

**O que faz:**

1. Lê o `fighters_index.jsonl` gerado no passo anterior
2. Para cada lutador, faz GET na `profile_url`
3. Salva o HTML em:

```
data/raw/html/fighters/dt=2026-04-11/{fighter_id}.html
```

---

### 4.7 Bronze de lutadores

**Arquivo:** `src/ufc_pipeline/coleta/lutadores.py` → função `gerar_bronze()`

**O que faz:**

1. Lê o `fighters_index.jsonl`
2. Para cada lutador, lê o HTML salvo em `data/raw/html/fighters/.../{fighter_id}.html`
3. Chama `_parse_pagina_lutador()` que extrai:
   - **Nome** (`<span class="b-content__title-highlight">`)
   - **Cartel** (`<span class="b-content__title-record">`) — ex.: "Record: 28-4-0"
   - **Bio** (`<ul class="b-list__box-list">`): altura, peso, alcance, stance, data de nascimento
   - **Histórico de lutas** (tabela `b-fight-details__table_type_event-details`): todas as lutas do lutador com resultado, adversário, evento, método, round, tempo

4. Adiciona `ingested_at` e salva em:

```
data/bronze/fighters/dt=2026-04-11/fighters.jsonl
```

**Estrutura de cada lutador no JSONL:**
```json
{
  "fighter_id": "c9f1e...",
  "fighter_id_hash5": "73921",
  "name": "Alexandre Pantoja",
  "profile_url": "http://ufcstats.com/fighter-details/abc",
  "bio": {
    "record": "28-4-0",
    "height": "5' 4\"",
    "weight": "125 lbs.",
    "reach": "65\"",
    "stance": "Orthodox",
    "dob": "Apr 06, 1990"
  },
  "fights": [
    {
      "result": "W",
      "fight_url": "http://ufcstats.com/fight-details/...",
      "fighter": "Alexandre Pantoja",
      "opponent": "Kai Asakura",
      "event_name": "UFC 310",
      "event_date": "December 7, 2024",
      "method_short": "Decision",
      "method_detail": "Unanimous",
      "round": "5",
      "time": "5:00",
      "title_bout": true,
      ...
    }
  ],
  "ingested_at": "2026-04-11T14:32:01Z"
}
```

---

## 5. Etapa 2 — Carga no Banco (SQL Server)

Comando de execução:
```bash
python scripts/load_lakehouse_sqlserver.py --dt 2026-04-11
```

**Arquivo:** `scripts/load_lakehouse_sqlserver.py` → função `main()`

---

### 5.1 Ponto de entrada do loader

1. Carrega `.env` com as credenciais do banco
2. Monta o objeto `ConexaoBanco` (de `banco.py`) com servidor, schemas, usuário, senha
3. Garante que as tabelas de controle ETL existam (`etl.controle_pipeline`, `etl.auditoria_carga`)
4. Verifica se já houve execução anterior bem-sucedida para determinar o `load_mode`:
   - `initial_full` → primeira vez, banco vazio
   - `incremental` → execuções subsequentes
5. Registra o início da execução em `etl.auditoria_carga`
6. Cria o engine SQLAlchemy via ODBC

---

### 5.2 Leitura dos arquivos bronze

Lê os três JSONL gerados na Etapa 1, para a partição `dt` informada:

```python
data/bronze/events/dt=2026-04-11/events.jsonl    → DataFrame bronze_events_src
data/bronze/fights/dt=2026-04-11/fights.jsonl    → DataFrame bronze_fights_src
data/bronze/fighters/dt=2026-04-11/fighters.jsonl → DataFrame bronze_fighters_src
```

---

### 5.3 Preparação bronze para o banco

Cada DataFrame passa por uma função `_prepare_bronze_*()` que:

- Adiciona colunas `*_hash5` (versão numérica de 5 dígitos dos IDs)
- Garante tipos corretos (ex.: `bonus_code` como Int64 nullable)
- Serializa campos aninhados (listas/dicts) como JSON string:
  - `fighters` → `fighters_json` (NVARCHAR(MAX) no banco)
  - `bio` → `bio_json` (NVARCHAR(MAX))
  - `fights` → `fights_json` (NVARCHAR(MAX))
- Adiciona `load_dt` (a data da partição passada no `--dt`)
- Adiciona `payload_json` (linha inteira serializada como JSON — backup do dado bruto)
- Seleciona e ordena as colunas finais

---

### 5.4 Upsert bronze no banco

Cada DataFrame preparado é enviado ao banco via `_upsert_by_key()`, que internamente usa `_merge_sqlserver_by_key()`:

**Como funciona o upsert:**

1. Verifica se a tabela destino já existe no banco
2. Se **não existe**: cria a tabela e insere diretamente
3. Se **existe**:
   - Cria uma tabela de staging temporária `__stg_{tabela}_{uuid}`
   - Insere todos os dados novos no staging
   - Executa um `MERGE` SQL:
     - Se a chave já existe na tabela destino → **UPDATE** dos campos não-chave
     - Se a chave não existe → **INSERT**
   - Remove a tabela de staging

**Chaves de upsert:**
- `bronze.eventos` → chave: `event_id`
- `bronze.lutas` → chave: `fight_id`
- `bronze.lutadores` → chave: `fighter_id`

---

### 5.5 Construção da Silver

**Função:** `_build_silver_from_bronze()`

A Silver é gerada **a partir dos dados já no banco** (não dos arquivos em disco). Isso garante que a Silver sempre reflita o acumulado histórico, não só a partição do dia.

O loader lê as tabelas bronze completas do banco e aplica:

**Para eventos (`silver.eventos`):**
- `_dedupe_latest()`: mantém apenas o registro mais recente por `event_id`
  (ordenado por `ingested_at` desc — garante que reprocessamentos não dupliquem)
- Seleciona colunas finais sem `payload_json`

**Para lutas (`silver.lutas`):**
- `_dedupe_latest()` por `fight_id`
- Garante tipo correto de `bonus_code` e `bout`
- Mantém `fighters_json` como string (JSON das estatísticas dos dois lutadores)

**Para lutadores (`silver.lutadores`):**
- `_dedupe_latest()` por `fighter_id`
- Extrai e parseia o cartel de `bio_json`:
  - `_extract_record_text()`: lê o campo `record` dentro do JSON da bio
  - `_parse_record()`: transforma "28-4-0 (1 NC)" em 4 campos numéricos:
    - `wins = 28`
    - `losses = 4`
    - `draws = 0`
    - `no_contest = 1`

Resultado upsertado em `silver.eventos`, `silver.lutas`, `silver.lutadores`.

---

### 5.6 Construção da Gold (métricas)

**Função:** `_build_gold_metrics()`

Gera métricas agregadas a partir da Silver. O processo:

**Expansão dos lutadores por luta:**

A coluna `fighters_json` de `silver.lutas` contém uma lista com os dois lutadores. O código expande cada linha em 2 linhas (uma por lutador), criando um DataFrame `ff_enriched` com:
- `event_id`, `fight_id`, `fighter_id`, `fighter_name`
- `method`, `round_int`, `fight_time_seconds`
- `result` (W/L/D/NC)

**Estatísticas de golpes por round:**

Expande `rounds_sig_strikes` de cada lutador, parseia `sig_str` (ex.: "45 of 78" → `sig_str_landed = 45`) e agrega por luta:
- `sig_str_landed_total`: total de golpes significativos na luta
- `sig_str_pct_avg`: média de precisão
- `rounds_fought`: rounds disputados

**Métricas por evento (`gold.metricas_eventos`):**

| Coluna | Descrição |
|---|---|
| `m01_total_fights` | Total de lutas no evento |
| `m02_total_fighters` | Total de lutadores no evento |
| `m03_total_finishes` | Lutas finalizadas (não decisão) |
| `m04_total_decisions` | Lutas por decisão |
| `m05_total_submissions` | Finalizações por submission |
| `m06_total_ko_tko` | Finalizações por KO/TKO |
| `m07_avg_round` | Média de rounds disputados |
| `m08_avg_fight_time_seconds` | Duração média das lutas (segundos) |
| `m09_avg_sig_str_landed_per_fighter` | Média de golpes significativos por lutador |
| `m10_avg_sig_str_pct_per_fighter` | Média de precisão de golpes |
| `m11_finish_rate_pct` | % de lutas finalizadas |

**Métricas por lutador (`gold.metricas_lutadores`):**

| Coluna | Descrição |
|---|---|
| `m01_fights_total` | Total de lutas no histórico |
| `m02_wins` | Vitórias |
| `m03_losses` | Derrotas |
| `m04_draws` | Empates |
| `m05_no_contests` | No Contests |
| `m06_finish_wins` | Vitórias por finalização |
| `m07_decision_wins` | Vitórias por decisão |
| `m08_avg_round_reached` | Média de rounds disputados |
| `m09_avg_fight_time_seconds` | Duração média das lutas |
| `m10_avg_sig_str_landed` | Média de golpes significativos por luta |
| `m11_avg_sig_str_pct` | Média de precisão de golpes |
| `m12_win_rate_pct` | % de vitórias |

---

### 5.7 Controle de execução (ETL)

**Arquivo:** `src/ufc_pipeline/banco.py`

Após carga bem-sucedida:
- `marcar_sucesso()`: atualiza `etl.controle_pipeline` com `last_success_dt = dt`
- `registrar_fim()`: atualiza `etl.auditoria_carga` com `status = 'success'`

Em caso de erro:
- `registrar_fim()`: registra `status = 'failed'` com a mensagem de erro

**Tabela `etl.controle_pipeline`** — uma linha por pipeline:
```
pipeline_name    | last_success_dt | last_run_at_utc
ufc_lakehouse    | 2026-04-11      | 2026-04-11 14:45:00
```

**Tabela `etl.auditoria_carga`** — uma linha por execução:
```
audit_id | pipeline_name | run_id       | run_dt     | load_mode    | status  | started_at_utc      | ended_at_utc        | message
1        | ufc_lakehouse | a3b1c2d4e5f6 | 2026-04-11 | initial_full | success | 2026-04-11 14:32:00 | 2026-04-11 14:45:00 | mode=initial_full
2        | ufc_lakehouse | f9e8d7c6b5a4 | 2026-04-12 | incremental  | success | 2026-04-12 09:10:00 | 2026-04-12 09:12:00 | mode=incremental
```

---

## 6. Arquitetura de Dados — Camadas

### Diferença entre as camadas

| Camada | Onde vive | Formato | Propósito |
|---|---|---|---|
| **RAW (disco)** | `data/raw/html/` | `.html` | Snapshot bruto do site. Permite reprocessar sem acessar a internet novamente |
| **BRONZE (disco)** | `data/bronze/` | `.jsonl` | Dado estruturado linha a linha. Fonte de verdade para carga no banco |
| **BRONZE (banco)** | `bronze.*` | Tabelas SQL | Histórico acumulado, com payload JSON preservado. Suporta reprocessamento |
| **SILVER (banco)** | `silver.*` | Tabelas SQL | Dado limpo, deduplicado, com cartel parseado. Para consultas analíticas |
| **GOLD (banco)** | `gold.*` | Tabelas SQL | Métricas prontas para consumo em dashboards (Power BI, etc.) |
| **ETL (banco)** | `etl.*` | Tabelas SQL | Controle de estado e auditoria de execuções |

---

## 7. Estrutura de Pastas em Disco

```
data/
├── raw/
│   ├── events_index/
│   │   └── dt=2026-04-11/
│   │       └── events_index.jsonl        ← lista de eventos (completed + upcoming)
│   ├── html/
│   │   ├── events/
│   │   │   └── dt=2026-04-11/
│   │   │       ├── {event_id}.html       ← HTML do card de cada evento
│   │   │       └── ...
│   │   └── fighters/
│   │       └── dt=2026-04-11/
│   │           ├── {fighter_id}.html     ← HTML do perfil de cada lutador
│   │           └── ...
│   └── _meta/
│       └── runs/
│           ├── {run_id}.json             ← metadados da coleta de eventos
│           ├── {run_id}_event_html.json  ← metadados do download de HTMLs
│           └── {run_id}_fighter_html.json
│
└── bronze/
    ├── events/
    │   └── dt=2026-04-11/
    │       └── events.jsonl              ← eventos com ingested_at
    ├── fights/
    │   └── dt=2026-04-11/
    │       └── fights.jsonl              ← lutas com estatísticas completas
    ├── fighters/
    │   └── dt=2026-04-11/
    │       └── fighters.jsonl            ← perfis completos dos lutadores
    ├── fighters_index/
    │   └── dt=2026-04-11/
    │       └── fighters_index.jsonl      ← índice intermediário (id + url)
    └── _meta/
        └── quality/
            ├── {run_id}_bronze_events_meta.json
            ├── {run_id}_bronze_fights_meta.json
            └── {run_id}_bronze_fighters_meta.json
```

---

## 8. Tabelas no Banco

### `bronze.eventos`
| Coluna | Tipo | Descrição |
|---|---|---|
| `event_id` | NVARCHAR | SHA1 da URL do evento (PK lógica) |
| `event_id_hash5` | NVARCHAR | Hash visual de 5 dígitos |
| `name` | NVARCHAR | Nome do evento |
| `event_url` | NVARCHAR | URL no ufcstats.com |
| `event_date` | NVARCHAR | Data (YYYY-MM-DD) |
| `location` | NVARCHAR | Cidade, estado, país |
| `status` | NVARCHAR | "completed" ou "upcoming" |
| `ingested_at` | NVARCHAR | Timestamp de ingestão |
| `load_dt` | NVARCHAR | Partição (--dt do comando) |
| `payload_json` | NVARCHAR(MAX) | Linha completa serializada |

### `bronze.lutas`
| Coluna | Tipo | Descrição |
|---|---|---|
| `fight_id` | NVARCHAR | SHA1(event_id\|red_url\|blue_url\|bout_order) |
| `fight_id_hash5` | NVARCHAR | Hash visual de 5 dígitos |
| `event_id` | NVARCHAR | FK para evento |
| `event_id_hash5` | NVARCHAR | Hash visual do evento |
| `event_name` | NVARCHAR | Nome do evento |
| `fight_url` | NVARCHAR | URL da luta |
| `bout_order` | NVARCHAR | Posição no card |
| `bonus_code` | BIGINT | 1=FOTN, 2=POTN, 3=Sub, 4=KO |
| `bout` | NVARCHAR | "normal" ou "title bout" |
| `method` | NVARCHAR | Método de término |
| `round` | NVARCHAR | Round de término |
| `time` | NVARCHAR | Tempo de término |
| `time_format` | NVARCHAR | Formato (ex.: "5 Rnd (5-5-5-5-5)") |
| `referee` | NVARCHAR | Nome do árbitro |
| `fighters_json` | NVARCHAR(MAX) | JSON com array dos dois lutadores + stats |
| `ingested_at` | NVARCHAR | Timestamp de ingestão |
| `load_dt` | NVARCHAR | Partição |
| `payload_json` | NVARCHAR(MAX) | Linha completa serializada |

### `bronze.lutadores`
| Coluna | Tipo | Descrição |
|---|---|---|
| `fighter_id` | NVARCHAR | SHA1 da URL do perfil |
| `fighter_id_hash5` | NVARCHAR | Hash visual |
| `name` | NVARCHAR | Nome do lutador |
| `profile_url` | NVARCHAR | URL no ufcstats.com |
| `bio_json` | NVARCHAR(MAX) | JSON com altura, peso, alcance, stance, dob, record |
| `fights_json` | NVARCHAR(MAX) | JSON com histórico de lutas |
| `ingested_at` | NVARCHAR | Timestamp de ingestão |
| `load_dt` | NVARCHAR | Partição |
| `payload_json` | NVARCHAR(MAX) | Linha completa serializada |

### `silver.lutadores` — campos adicionais vs bronze
| Coluna | Tipo | Descrição |
|---|---|---|
| `record_text` | NVARCHAR | Ex.: "28-4-0 (1 NC)" |
| `wins` | BIGINT | Vitórias |
| `losses` | BIGINT | Derrotas |
| `draws` | BIGINT | Empates |
| `no_contest` | BIGINT | No Contests |

### `etl.controle_pipeline`
| Coluna | Descrição |
|---|---|
| `pipeline_name` | Nome da pipeline (PK) |
| `last_success_dt` | Data da última execução bem-sucedida |
| `last_run_at_utc` | Timestamp da última execução |
| `updated_at_utc` | Timestamp da última atualização do registro |

### `etl.auditoria_carga`
| Coluna | Descrição |
|---|---|
| `audit_id` | ID auto-incremento (PK) |
| `pipeline_name` | Nome da pipeline |
| `run_id` | UUID hexadecimal de 12 chars da execução |
| `run_dt` | Data da partição processada |
| `load_mode` | "initial_full" ou "incremental" |
| `status` | "running", "success" ou "failed" |
| `started_at_utc` | Início da execução |
| `ended_at_utc` | Fim da execução |
| `message` | Mensagem de sucesso ou erro |

---

## 9. Utilitários Internos

### `ids.py` — geração de IDs

| Função | Entrada | Saída | Uso |
|---|---|---|---|
| `sha1(text)` | string | hex 40 chars | ID principal de eventos, lutas, lutadores |
| `hash5(text)` | string | 5 dígitos | ID visual/operacional (não é PK) |
| `event_id_from_url(url)` | URL | SHA1 | ID de evento a partir da URL |
| `fighter_id_from_url(url)` | URL | SHA1 | ID de lutador a partir da URL |
| `fight_id(event_id, red, blue, bout)` | 4 strings | SHA1 | ID único de luta |
| `short_id_from_id(id)` | SHA1 | 5 dígitos | Versão curta de qualquer ID |

### `http.py` — cliente HTTP

- `RateLimiter`: controla o intervalo mínimo entre requisições (evita ban do servidor)
- `HttpClient`: sessão `requests` com retry automático para erros 429/500/502/503/504 e delay educado entre chamadas

### `banco.py` — SQL Server

- `ConexaoBanco`: dataclass com todas as configurações de conexão e mapeamento de tabelas
- `montar_conn_str()`: monta a string ODBC automaticamente, detectando o melhor driver instalado
- Funções ETL usam queries **parametrizadas** (`?`) para evitar SQL injection

### `parsing.py` — HTML

- `clean(text)`: normaliza espaços em branco de textos extraídos do HTML
- `safe_select_one(parent, css)`: `select_one` que não lança exceção
- `safe_attr(tag, attr)`: lê atributo de uma tag com segurança
- `parse_ufc_date(s)`: converte "May 11, 2024" → `date(2024, 5, 11)`

---

## 10. Fluxo Completo em Diagrama

```
[ufcstats.com]
    │
    ├─ GET /events/completed  ──┐
    └─ GET /events/upcoming   ──┴──► parse_tabela_eventos()
                                        │
                                        ▼
                              events_index.jsonl
                              (data/raw/events_index/)
                                        │
                                        ▼
                              GET {event_url} para cada evento
                                        │
                                        ▼
                              {event_id}.html
                              (data/raw/html/events/)
                                        │
                                        ├──► bronze/events/events.jsonl
                                        │
                                        └──► parse_links_lutas()
                                                 │
                                                 ▼
                                        GET {fight_url} para cada luta
                                                 │
                                                 ▼
                                        parse_pagina_luta()
                                                 │
                                        ┌────────┴────────┐
                                        ▼                 ▼
                              fights.jsonl         fighters_index.jsonl
                              (bronze/fights/)     (bronze/fighters_index/)
                                                          │
                                                          ▼
                                                 GET {profile_url} para cada lutador
                                                          │
                                                          ▼
                                                 {fighter_id}.html
                                                 (raw/html/fighters/)
                                                          │
                                                          ▼
                                                 parse_pagina_lutador()
                                                          │
                                                          ▼
                                                 fighters.jsonl
                                                 (bronze/fighters/)

──────────────────────────────────────────── load_lakehouse_sqlserver.py ──

    events.jsonl  ──► _prepare_bronze_events()  ──► UPSERT bronze.eventos
    fights.jsonl  ──► _prepare_bronze_fights()  ──► UPSERT bronze.lutas
    fighters.jsonl ──► _prepare_bronze_fighters() ──► UPSERT bronze.lutadores

    READ bronze.eventos + bronze.lutas + bronze.lutadores (acumulado no banco)
                                        │
                                        ▼
                            _build_silver_from_bronze()
                            (deduplicação por ingested_at)
                                        │
                            ┌───────────┼───────────┐
                            ▼           ▼           ▼
                     silver.eventos  silver.lutas  silver.lutadores
                                        │
                                        ▼
                            _build_gold_metrics()
                            (expansão lutadores + agregação)
                                        │
                            ┌───────────┴───────────┐
                            ▼                       ▼
                   gold.metricas_eventos   gold.metricas_lutadores

                                        │
                                        ▼
                          etl.controle_pipeline  (last_success_dt)
                          etl.auditoria_carga    (log da execução)
```
