# ufc-lakehouse

Lakehouse pipeline para coleta de dados UFC e carga Raw -> Bronze -> Silver -> Gold em SQL Server (sem PySpark).

Fonte: `ufcstats.com` via `requests` + `BeautifulSoup`.
O projeto salva snapshots HTML em `data/raw/html` para reprocessamento sem nova coleta.

## Estrutura

- `src/ufc_pipeline/`: codigo da aplicacao
- `configs/`: configuracoes de fonte, pipeline e logging
- `scripts/`: loaders e utilitarios
- `data/`: saidas por camada (ignorada no Git)

## Camadas

- `RAW`: indice de eventos + HTML de eventos/lutadores + metadados
- `BRONZE (arquivos)`: jsonl de eventos/lutas/lutadores
- `RAW (banco)`: json bruto por linha (`raw.*_json`)
- `BRONZE (banco)`: espelho estruturado do json (`bronze.*_raw`)
- `SILVER (banco)`: dados curados derivados da Bronze (`silver.*`)
- `GOLD (banco)`: metricas analiticas derivadas da Silver (`gold.*`)
- `ETL (controle)`: estado e auditoria da pipeline (`etl.pipeline_control`, `etl.load_audit`)

## Como rodar

1. Criar ambiente e instalar dependencias:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
pip install -e ".[sqlserver]"  # necessario para o loader SQL Server
```

2. Copiar `.env.example` para `.env` e ajustar variaveis, se necessario.

3. Executar pipeline:

```bash
# recomendado: script instalado do pacote
ufc-pipeline run all

# alternativa equivalente
python -m ufc_pipeline run all

# por estagio
ufc-pipeline run raw
ufc-pipeline run bronze
```

4. Carregar no SQL Server (sem PySpark):

```bash
python scripts/load_lakehouse_sqlserver.py --dt 2026-02-08 --data-root .\data
```

## Saidas

- `data/raw/...`: snapshots e metadados (`run_id`, `dt`)
- `data/bronze/...`: eventos, lutas e lutadores em `jsonl`
- `data/bronze/fighters_index/...`: indice intermediario de lutadores para coleta RAW

## Observacoes

- O HTML do site pode mudar; por isso a camada RAW persiste snapshots.
- Controle de taxa e timeout fica em `configs/settings.yaml`.
- `data/` nao e versionado por padrao (`.gitignore`).

## Loader SQL Server (sem PySpark)

O loader oficial e em Python (pandas + ODBC):

- `scripts/load_lakehouse_sqlserver.py`

Fluxo implementado no banco:

1. Raw: salva JSON bruto por linha em `raw.events_json`, `raw.fights_json`, `raw.fighters_json`.
2. Bronze: espelha JSONs estruturados em `bronze.events_raw`, `bronze.fights_raw`, `bronze.fighters_raw`.
3. Silver: criada a partir da Bronze (`silver.events`, `silver.fights`, `silver.fighters`).
4. Gold: metricas em `gold.event_metrics` e `gold.fighter_metrics`.
5. ETL state: controle de sucesso e auditoria de execucao em `etl.*`.

Comportamento de carga:

- SQL Server usa `MERGE` por chave (insert + update), com staging table temporaria.
- Se nao houver sucesso anterior em `etl.pipeline_control` para o `pipeline_name`, o modo e `initial_full`.
- Com sucesso anterior, o modo e `incremental`.
- Cada execucao registra auditoria em `etl.load_audit` com `run_id`, status, inicio/fim e mensagem.

Exemplo com SQL Server via argumentos:

```bash
python scripts/load_lakehouse_sqlserver.py --server "localhost\SQLEXPRESS" --database UFC_Lakehouse --dt 2026-02-08 --data-root .\data --trust-server-certificate
```

Exemplo com autenticacao SQL:

```bash
python scripts/load_lakehouse_sqlserver.py --server "localhost\SQLEXPRESS" --database UFC_Lakehouse --dt 2026-02-08 --data-root .\data --user sa --password "<senha>" --trust-server-certificate
```

Configuracao central de destino:

- `src/ufc_pipeline/common/lakehouse_target.py`: dataclass unica com credenciais, schemas e nomes de tabelas do SQL Server.
- Variaveis em `.env` (`UFC_TARGET_*`) alimentam essa dataclass.
- `UFC_TARGET_ODBC_DRIVER` e opcional para escolher o driver ODBC.
- `UFC_TARGET_ODBC_EXTRA` permite parametros ODBC extras (ex.: `MARS_Connection=Yes`).

Schemas por camada (SQL Server):

- `UFC_RAW_SCHEMA` (default: `raw`)
- `UFC_BRONZE_SCHEMA` (default: `bronze`)
- `UFC_SILVER_SCHEMA` (default: `silver`)
- `UFC_GOLD_SCHEMA` (default: `gold`)

Observacao:

- Garanta um driver ODBC do SQL Server instalado (ex.: `ODBC Driver 17 for SQL Server`).

## Configuracoes importantes

- `pipeline.pipeline_name`: nome logico da pipeline (default `ufc_lakehouse`)
- `pipeline.events_lookback_days`: janela incremental de eventos concluidos (default `30` dias)
- `pipeline.full_load_if_db_empty`: se `true`, sem sucesso anterior faz carga historica completa no `run all`
- `UFC_PIPELINE_NAME`: nome logico da pipeline usado no loader SQL (default `ufc_lakehouse`)

## Regra inicial x incremental

1. `run all` (coleta): decide `full load` ou incremental pela tabela `etl.pipeline_control`.
2. Loader SQL: tambem decide `initial_full`/`incremental` por `etl.pipeline_control` e grava auditoria em `etl.load_audit`.
3. Para forcar uma nova carga inicial completa, limpe o estado em `etl.pipeline_control` para o pipeline.

## Tabelas atuais (SQL Server)

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
