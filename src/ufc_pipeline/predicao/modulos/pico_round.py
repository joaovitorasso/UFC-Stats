"""
Módulo: Pico de Round — Taxa de Vitória por Round
Peso padrão: 3/5

Analisa em qual round cada lutador tem a maior taxa de vitórias
e finalizações. Revela se é um "starter" rápido (domina no R1)
ou um "finisher" tardio (melhora com o passar da luta).
Cruzar os picos dos dois lutadores indica quem terá o timing
favorável no confronto específico.
"""
from __future__ import annotations

import pandas as pd

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 3


def _analise_lutador(d: DadosLutador) -> dict:
    hist = d.historico
    if hist.empty:
        return {
            "pico_round": None,
            "wr_por_round": {},
            "finish_rate_por_round": {},
            "perfil": "sem dados",
        }

    wins = hist[hist["resultado"].isin(["win", "W"])].copy()
    wins = wins.dropna(subset=["round_fim"])
    wins["round_fim"] = wins["round_fim"].apply(lambda x: int(x) if x is not None else None)
    wins = wins.dropna(subset=["round_fim"])

    if wins.empty:
        return {"pico_round": None, "wr_por_round": {}, "finish_rate_por_round": {}, "perfil": "sem dados"}

    # Win rate por round: vitórias terminadas no round X / total de lutas
    total = max(len(hist), 1)
    wr_por_round = {}
    finish_rate  = {}
    for rnd, grp in wins.groupby("round_fim"):
        wr_por_round[int(rnd)]   = round(len(grp) / total * 100, 1)
        finishes = grp["method_short"].str.contains("KO|TKO|Sub", na=False).sum()
        finish_rate[int(rnd)]    = round(int(finishes) / max(len(grp), 1) * 100, 1)

    pico = max(wr_por_round, key=wr_por_round.__getitem__) if wr_por_round else None

    # Perfil: starter rápido, dominância tardia ou constante
    wr_r1 = wr_por_round.get(1, 0)
    wr_r3 = max(wr_por_round.get(3, 0), wr_por_round.get(4, 0), wr_por_round.get(5, 0))
    if wr_r1 >= 20 and wr_r1 > wr_r3:
        perfil = "starter rápido"
    elif wr_r3 >= 15 and wr_r3 > wr_r1:
        perfil = "dominância tardia"
    else:
        perfil = "constante"

    return {
        "pico_round":             pico,
        "wr_por_round":           wr_por_round,
        "finish_rate_por_round":  finish_rate,
        "perfil":                 perfil,
    }


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    info_a = _analise_lutador(a)
    info_b = _analise_lutador(b)

    # Vantagem: quem tem maior win rate no seu pico de round
    def score_pico(info: dict) -> float:
        wr = info.get("wr_por_round", {})
        return max(wr.values()) if wr else 0.0

    sa = score_pico(info_a)
    sb = score_pico(info_b)

    # Conflito de picos: se os picos se sobrepõem, quem domina naquele round
    pico_a = info_a.get("pico_round")
    pico_b = info_b.get("pico_round")
    conflito = None
    if pico_a is not None and pico_b is not None:
        if pico_a == pico_b:
            conflito = f"Ambos dominam no R{pico_a} — round crítico para a luta"
        elif pico_a < pico_b:
            conflito = f"{a.nome} pressiona cedo (R{pico_a}); {b.nome} busca rounds tardios (R{pico_b})"
        else:
            conflito = f"{b.nome} pressiona cedo (R{pico_b}); {a.nome} busca rounds tardios (R{pico_a})"

    return {
        a.nome:     info_a,
        b.nome:     info_b,
        "conflito_de_picos": conflito,
        "vantagem":  a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
    }
