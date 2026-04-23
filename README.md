# UFC Stats Pipeline (Raw -> Bronze -> Silver)

Pipeline simples para coletar dados do UFC Stats e carregar apenas a camada `silver` no SQL Server.

## Fluxo

1. `raw` (arquivo): salva HTML/JSON em `data/raw`.
2. `bronze` (arquivo): gera JSONL em `data/bronze`.
3. `silver` (banco): carrega tabelas `silver.eventos`, `silver.lutas`, `silver.lutadores`.
4. `silver` (banco): gera também `silver.historico_lutas` (histórico por lutador por round).
5. `silver` (banco): mantém dimensões `silver.dim_lutador`, `silver.dim_evento`, `silver.dim_luta` e `silver.dim_bonus`.

## Estrutura

- `src/pipeline.py`: orquestracao da pipeline
- `src/coleta.py`: coleta e parsing (eventos, lutas, lutadores)
- `src/utils.py`: funcoes utilitarias (ids, io e parsing simples)
- `src/silver_loader.py`: carga bronze -> silver
- `configs/`: configuracoes
- `data/`: arquivos locais gerados pela pipeline

## Como rodar

1. Criar ambiente:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements-sqlserver.txt
```

2. Copiar `.env.example` para `.env` e ajustar conexao SQL Server.

3. Executar coleta:

```bash
# Carga inicial — coleta todos os eventos sem restricao de data
python src\pipeline.py --initial

# Carga incremental — respeita o lookback configurado em settings.yaml
python src\pipeline.py --incremental
```

Opcional: forcar uma particao especifica:

```bash
python src\pipeline.py --incremental --dt 2026-04-20
```

4. Carregar silver no SQL Server:

```bash
python src\silver_loader.py --data-root .\data
```

Opcional: carregar apenas uma particao:

```bash
python src\silver_loader.py --dt 2026-04-20 --data-root .\data
```
