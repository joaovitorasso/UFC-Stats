"""
Predição de resultado de luta UFC.

Uso:
    python scripts/prever_luta.py "Lutador A" "Lutador B"

Requer:
    - Views analíticas criadas (scripts/criar_views_analiticas.sql)
    - Variáveis de ambiente de conexão configuradas (.env)
    - pip install pandas pyodbc sqlalchemy python-dotenv pyyaml
"""
from __future__ import annotations

import sys
import os

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ufc_pipeline.banco import ConexaoBanco  # type: ignore
from ufc_pipeline.predicao import (  # type: ignore
    DadosLutador,
    _v,
    carregar_dados,
    criar_engine,
    calcular_score_final,
    imprimir_relatorio,
    analise_estilo,
    analise_round_a_round,
    analise_curva_fadiga,
    analise_indice_cardio,
    analise_timing_finalizacao,
    analise_adaptacao_oponente,
    analise_perfil_pressao,
    analise_troca_dano,
    analise_controle_solo,
    analise_mapa_alvos,
    analise_medias_apostas,
    analise_probabilidade_metodo,
    analise_tendencia,
    analise_declinio_etario,
    analise_pico_round,
    analise_poder_nocaute,
    analise_durabilidade,
    analise_crescimento_intra_luta,
    analise_atividade_recente,
)

load_dotenv()

# Alias para compatibilidade com prever_evento.py
_criar_engine = criar_engine


def main() -> None:
    if len(sys.argv) < 3:
        print("Uso: python scripts/prever_luta.py \"Lutador A\" \"Lutador B\"")
        sys.exit(1)

    nome_a = sys.argv[1]
    nome_b = sys.argv[2]

    banco  = ConexaoBanco.do_env()
    engine = criar_engine(banco)

    print(f"Carregando dados: {nome_a}...")
    a = carregar_dados(engine, nome_a)
    print(f"Carregando dados: {nome_b}...")
    b = carregar_dados(engine, nome_b)
    print("Calculando análises...\n")

    analises = {
        "estilo":                 analise_estilo(a, b),
        "round_a_round":          analise_round_a_round(a, b),
        "curva_fadiga":           analise_curva_fadiga(a, b),
        "indice_cardio":          analise_indice_cardio(a, b),
        "timing_finalizacao":     analise_timing_finalizacao(a, b),
        "adaptacao_oponente":     analise_adaptacao_oponente(a, b),
        "perfil_pressao":         analise_perfil_pressao(a, b),
        "troca_dano":             analise_troca_dano(a, b),
        "controle_solo":          analise_controle_solo(a, b),
        "mapa_alvos":             analise_mapa_alvos(a, b),
        "medias_apostas":         analise_medias_apostas(a, b),
        "probabilidade_metodo":   analise_probabilidade_metodo(a, b),
        "tendencia":              analise_tendencia(a, b),
        "declinio_etario":        analise_declinio_etario(a, b),
        "pico_round":             analise_pico_round(a, b),
        "poder_nocaute":          analise_poder_nocaute(a, b),
        "durabilidade":           analise_durabilidade(a, b),
        "crescimento_intra_luta": analise_crescimento_intra_luta(a, b),
        "atividade_recente":      analise_atividade_recente(a, b),
    }

    score = calcular_score_final(a, b, analises)
    imprimir_relatorio(a, b, analises, score)


if __name__ == "__main__":
    main()
