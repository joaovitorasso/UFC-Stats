"""
Módulo: Estilo e Matchup
Peso padrão: 1/5

Classifica o estilo predominante de cada lutador e descreve o matchup.
Usado como contexto narrativo; contribui pouco no score numérico pois
não distingue habilidade relativa, apenas archétipos táticos.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 1


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    estilo_a = a.stats.get("estilo_predominante", "Desconhecido")
    estilo_b = b.stats.get("estilo_predominante", "Desconhecido")
    matchup_desc = {
        ("Striker", "Striker"):           "Troca de pé — favorecer quem tem maior precisão e volume.",
        ("Striker", "Grappler"):           f"{a.nome} prefere distância; {b.nome} vai buscar o solo.",
        ("Grappler", "Striker"):           f"{a.nome} vai buscar o solo; {b.nome} prefere distância.",
        ("Grappler", "Grappler"):          "Disputa de grappling — takedowns e submissões serão decisivos.",
        ("Pressão/Clinch", "Striker"):     f"{a.nome} usa clinch; {b.nome} vai tentar manter distância.",
        ("Striker", "Pressão/Clinch"):     f"{a.nome} vai tentar manter distância do clinch de {b.nome}.",
    }.get((estilo_a, estilo_b), f"{estilo_a} vs {estilo_b}")
    return {
        "estilo_a": estilo_a,
        "estilo_b": estilo_b,
        "matchup": matchup_desc,
    }
