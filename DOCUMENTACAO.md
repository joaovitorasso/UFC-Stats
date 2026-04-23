# Documentacao Tecnica (versao simples)

## Objetivo

Pipeline de dados do UFC Stats com foco em simplicidade:

- `raw`: arquivos HTML/JSON em disco
- `bronze`: arquivos JSONL em disco
- `silver`: tabelas no SQL Server

## Etapa 1 - Coleta

Dois modos de execucao:

**Carga inicial** — coleta todos os eventos sem restricao de data:

```bash
python src\pipeline.py --initial
```

**Carga incremental** — coleta apenas eventos dentro do `events_lookback_days` configurado em `settings.yaml`:

```bash
python src\pipeline.py --incremental
```

Opcional: forcar uma particao especifica em qualquer modo:

```bash
python src\pipeline.py --incremental --dt 2026-04-20
```

Saidas:

- `data/raw/indice_eventos/dt=.../indice_eventos.jsonl`
- `data/raw/html/eventos/dt=.../*.html`
- `data/raw/html/lutas/dt=.../*.html`
- `data/raw/html/lutadores/dt=.../*.html`
- `data/bronze/eventos/dt=.../eventos.jsonl`
- `data/bronze/lutas_completed/dt=.../lutas_completed.jsonl`
- `data/bronze/lutas_upcoming/dt=.../lutas_upcoming.jsonl`
- `data/bronze/lutadores/dt=.../lutadores.jsonl`

## Etapa 2 - Carga Silver

Comando:

```bash
python src\silver_loader.py --data-root .\data
```

Opcional por particao:

```bash
python src\silver_loader.py --dt 2026-04-20 --data-root .\data
```

Tabelas geradas:

- `silver.eventos`
- `silver.lutas`
- `silver.lutadores`
- `silver.historico_lutas` (1 linha por lutador por round da luta)
- `silver.dim_lutador` (dimensão com `id_lutador` sequencial e estável por `fighter_id`)
- `silver.dim_evento` (dimensão com `id_evento` sequencial e estável por `event_id`)
- `silver.dim_luta` (dimensão com `id_luta` sequencial e estável por `fight_id`)
- `silver.dim_bonus` (dimensão de `codigo_bonus` e `descricao_bonus`)

## Configuracoes

Arquivo `.env`:

- `UFC_TARGET_SERVER`
- `UFC_TARGET_DATABASE`
- `UFC_SILVER_SCHEMA`
- `UFC_TARGET_USER` / `UFC_TARGET_PASSWORD` (opcional)
- `UFC_TARGET_ENCRYPT`
- `UFC_TARGET_TRUST_SERVER_CERTIFICATE`
- `UFC_TARGET_ODBC_DRIVER` (opcional)
- `UFC_TARGET_ODBC_EXTRA` (opcional)

Arquivo `configs/settings.yaml`:

- `data_dir`
- `http.*`
- `pipeline.events_lookback_days`
- `pipeline.limit_events`
- `pipeline.limit_fights_per_event`
- `pipeline.limit_fighters`
