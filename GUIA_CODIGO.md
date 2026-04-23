# Guia Rapido do Codigo

## Arquivos principais

- `src/pipeline.py`: orquestra coleta `raw` e geracao `bronze`.
- `src/coleta.py`: coleta/parsing de eventos, lutas e lutadores.
- `src/utils.py`: helpers unificados (ids, io e parsing).
- `src/silver_loader.py`: carga simples de `bronze` para `silver`.

## Como funciona

1. Coleta do site e salva HTML/JSON em `data/raw`.
2. Parse e geracao de JSONL em `data/bronze`.
3. Loader le os JSONL e grava as tabelas `silver` no SQL Server.
4. As dimensões `silver.dim_lutador`, `silver.dim_evento` e `silver.dim_luta` garantem IDs sequenciais e estáveis no incremental.

## Comandos

```bash
# Carga inicial (sem filtro de data)
python src\pipeline.py --initial

# Carga incremental (lookback configurado em settings.yaml)
python src\pipeline.py --incremental

# Carga silver no SQL Server
python src\silver_loader.py --data-root .\data
```
