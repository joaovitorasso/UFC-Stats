# Guia do Codigo - UFC Lakehouse

## 1. Visao geral do fluxo

Pipeline de dados:

1. `raw`: coleta HTML e metadados da fonte.
2. `bronze` (arquivos): gera JSONL em `data/bronze/...`.
3. `raw` (database): salva JSON bruto por linha em tabelas `raw.*`.
4. `bronze/silver/gold` (database): loader SQL Server (pandas) sobe e transforma.
5. `etl` (database): controle de estado e auditoria das execucoes.

Comando de execucao (coleta + bronze arquivos):

```bash
ufc-pipeline run all
```

Comando de carga no banco:

```bash
python scripts/load_lakehouse_sqlserver.py --dt 2026-02-08 --data-root ./data
```

## 2. Arquivos principais e responsabilidade

### `scripts/load_lakehouse_sqlserver.py`

Responsavel por:

- ler JSONs da bronze (`data/bronze/...`)
- gravar camada `raw` no banco (payload bruto por linha)
- subir bronze no banco
- gerar silver a partir da bronze
- gerar gold (metricas) a partir da silver

Blocos importantes:

- `_prepare_raw_lines`: monta tabela raw com payload JSON bruto.
- `_prepare_bronze_*`: padroniza colunas bronze e cria hash5.
- `_build_silver_from_bronze`: deduplica e estrutura silver.
- `_build_gold_metrics`: calcula metricas por evento e por lutador.
- `_upsert_by_key`: orquestra upsert por chave.
- `_merge_sqlserver_by_key`: no SQL Server faz `MERGE` (insert + update) com staging table.
- `main()`: orquestra toda a carga.

Controle de execucao (SQL Server):

- cria/garante tabelas `etl.pipeline_control` e `etl.load_audit`.
- define `load_mode` automaticamente:
  - `initial_full`: sem sucesso anterior em `etl.pipeline_control`.
  - `incremental`: com sucesso anterior.
- grava inicio/fim/status da execucao em `etl.load_audit`.
- ao sucesso, atualiza `etl.pipeline_control` com `last_success_dt`.

### `src/ufc_pipeline/common/lakehouse_target.py`

Configuracao central do destino (abstracao de infra):

- SQL Server (ODBC)
- schemas por camada (`raw`, `bronze`, `silver`, `gold`)
- nomes de tabela por camada

### `src/ufc_pipeline/orchestration/cli.py`

Entrada de linha de comando do pipeline de coleta (`ufc-pipeline ...`).

### `src/ufc_pipeline/orchestration/runner.py`

Executa estagios (`raw`, `bronze`) chamando os pipelines corretos.

### `src/ufc_pipeline/pipelines/*.py`

Implementacao dos estagios de coleta e geracao de arquivos bronze.

### `src/ufc_pipeline/domain/*`

Parsers/extratores de eventos, lutas e lutadores.

## 3. Onde alterar cada tipo de requisito

### A) Mudar destino (SQL Server)

Edite:

- `.env`
- `src/ufc_pipeline/common/lakehouse_target.py` (se precisar mudar comportamento default)

Variaveis-chave:
- `UFC_TARGET_SERVER`
- `UFC_TARGET_DATABASE`
- `UFC_TARGET_USER`
- `UFC_TARGET_PASSWORD`
- `UFC_TARGET_ENCRYPT`
- `UFC_TARGET_TRUST_SERVER_CERTIFICATE`
- `UFC_TARGET_ODBC_DRIVER`
- `UFC_TARGET_ODBC_EXTRA`
- `UFC_RAW_SCHEMA`
- `UFC_BRONZE_SCHEMA`
- `UFC_SILVER_SCHEMA`
- `UFC_GOLD_SCHEMA`

### B) Mudar nome das tabelas

Edite:

- `src/ufc_pipeline/common/lakehouse_target.py` em `table_map`

### C) Mudar schema das camadas

Edite:

- `.env`: `UFC_RAW_SCHEMA`, `UFC_BRONZE_SCHEMA`, `UFC_SILVER_SCHEMA`, `UFC_GOLD_SCHEMA`
- opcionalmente defaults em `LakehouseTarget`

### D) Mudar logica da Silver (limpeza/colunas)

Edite:

- `scripts/load_lakehouse_sqlserver.py` em `_build_silver_from_bronze`

Exemplos:

- parse de `record_text` (wins/losses/draws/no_contest)
- deduplicacao por chave

### E) Mudar logica da Gold (metricas)

Edite:

- `scripts/load_lakehouse_sqlserver.py` em `_build_gold_metrics`

Exemplos:

- incluir/remover metricas
- mudar formulas de agregacao

### F) Mudar regra de incremental

Edite:

- `scripts/load_lakehouse_sqlserver.py` em `_upsert_by_key`
- `scripts/load_lakehouse_sqlserver.py` em `_merge_sqlserver_by_key` (comportamento insert/update)
- `src/ufc_pipeline/common/pipeline_state.py` (estado da pipeline e auditoria)
- e, para janela de coleta, `configs/settings.yaml` (`pipeline.events_lookback_days`)

### G) Mudar formato do hash ID (5 digitos)

Edite:

- `scripts/load_lakehouse_sqlserver.py` em `_hash5`

### H) Mudar estrutura da camada RAW no banco

Edite:

- `scripts/load_lakehouse_sqlserver.py` em `_prepare_raw_lines`
- `src/ufc_pipeline/common/lakehouse_target.py` (`table_map` para `raw_*`)

Colunas atuais da RAW (database):

- `raw_sha1`
- `raw_id_hash5`
- `entity`
- `source_file`
- `load_dt`
- `payload_json`

## 4. Arquivos de configuracao

### `.env`

Configuracao efetiva por ambiente (recomendado para operacao).

Variavel util de controle:

- `UFC_PIPELINE_NAME`: nome logico da pipeline para `etl.pipeline_control`/`etl.load_audit`.

### `.env.example`

Modelo de variaveis; use como referencia para criar/atualizar o `.env`.

### `pyproject.toml`

Config do projeto Python:

- dependencias
- entrypoint `ufc-pipeline`

## 5. Comandos uteis

Rodar coleta:

```bash
ufc-pipeline run all
```

Rodar so raw:

```bash
ufc-pipeline run raw
```

Rodar so bronze (arquivos):

```bash
ufc-pipeline run bronze
```

Carregar no SQL Server:

```bash
python scripts/load_lakehouse_sqlserver.py --server "localhost\SQLEXPRESS" --database UFC_Lakehouse --dt 2026-02-08 --data-root ./data --user ufc_loader --password "SUA_SENHA" --trust-server-certificate
```

Consultar tabelas por camada (padrao atual):

```sql
select top 10 * from raw.events_json;
select top 10 * from bronze.events_raw;
select top 10 * from silver.events;
select top 10 * from gold.event_metrics;
select top 10 * from etl.pipeline_control;
select top 50 * from etl.load_audit order by audit_id desc;
```

## 6. Checklist de troubleshooting

### Erro driver ODBC SQL Server

- confirmar driver instalado (ex.: `ODBC Driver 17 for SQL Server`)
- usar `UFC_TARGET_ODBC_DRIVER` ou `--odbc-driver` se necessario

### Erro de autenticacao SQL

- confirmar usuario/senha SQL
- confirmar Mixed Mode no SQL Server

### Erro tabela nao encontrada

- conferir schema atual (`raw/bronze/silver/gold`)
- conferir se houve migracao de nomes

### Execucao em modo errado (inicial x incremental)

- conferir `etl.pipeline_control` para o `pipeline_name`
- conferir `UFC_PIPELINE_NAME` (loader) e `pipeline.pipeline_name` (`configs/settings.yaml`)

## 7. Resumo rapido de alteracoes

- Infra/destino: `lakehouse_target.py` + `.env`
- Camada raw/bronze/silver/gold no banco: `load_lakehouse_sqlserver.py`
- Coleta/parsing: `src/ufc_pipeline/pipelines` e `src/ufc_pipeline/domain`

## 8. Nomes de tabela atuais (SQL Server)

- `raw.events_json`
- `raw.fights_json`
- `raw.fighters_json`
- `bronze.events_raw`
- `bronze.fights_raw`
- `bronze.fighters_raw`
- `silver.events`
- `silver.fights`
- `silver.fighters`
- `gold.event_metrics`
- `gold.fighter_metrics`
- `etl.pipeline_control`
- `etl.load_audit`
