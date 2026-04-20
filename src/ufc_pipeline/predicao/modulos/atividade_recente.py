"""
Módulo: Atividade e Forma Recente
Peso padrão: 3/5

Compara o desempenho das últimas 3 lutas com as 3 anteriores,
medindo a tendência de win rate e o intervalo médio entre lutas.

Lutadores que vêm de uma sequência ruim recente — mesmo que tenham
bom histórico geral — representam maior risco, especialmente se
o rival está em boa sequência. Distingue de `tendencia` por focar
em janela temporal absoluta (últimas 3) em vez de ponderação por luta.
"""
from __future__ import annotations

import pandas as pd

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 3


def _win_rate(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return round(df["resultado"].isin(["win", "W"]).sum() / max(len(df), 1) * 100, 1)


def _analise_lutador(d: DadosLutador) -> dict:
    hist = d.historico.copy()
    if hist.empty:
        return {
            "wr_ultimas_3":    None,
            "wr_anteriores_3": None,
            "delta_wr":        None,
            "intervalo_medio_dias": None,
            "forma": "sem dados",
            "score": 50.0,
        }

    hist["data_dt"] = pd.to_datetime(hist["data_evento"], errors="coerce")
    hist_ord = hist.sort_values("data_dt", ascending=False)

    ultimas_3   = hist_ord.head(3)
    anteriores_3 = hist_ord.iloc[3:6]

    wr_rec  = _win_rate(ultimas_3)
    wr_ant  = _win_rate(anteriores_3) if not anteriores_3.empty else None
    delta   = round(wr_rec - wr_ant, 1) if wr_ant is not None else None

    # Intervalo médio entre lutas (dias)
    datas_validas = hist_ord["data_dt"].dropna().head(6)
    if len(datas_validas) >= 2:
        intervalos = datas_validas.diff(-1).abs().dropna()
        intervalo_medio = round(intervalos.dt.days.mean(), 0)
    else:
        intervalo_medio = None

    # Forma atual
    if wr_rec >= 66:
        forma = "boa forma"
    elif wr_rec == 0 and len(ultimas_3) >= 2:
        forma = "má forma"
    elif delta is not None and delta >= 20:
        forma = "em ascensão"
    elif delta is not None and delta <= -20:
        forma = "em queda"
    else:
        forma = "regular"

    # Score: win rate recente pesado mais o delta de tendência
    score = wr_rec
    if delta is not None:
        score += delta * 0.3
    score = max(0.0, min(100.0, round(score, 1)))

    return {
        "wr_ultimas_3":         wr_rec,
        "wr_anteriores_3":      wr_ant,
        "delta_wr":             delta,
        "intervalo_medio_dias": intervalo_medio,
        "forma":                forma,
        "score":                score,
    }


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    info_a = _analise_lutador(a)
    info_b = _analise_lutador(b)

    sa = info_a["score"]
    sb = info_b["score"]

    return {
        a.nome:    info_a,
        b.nome:    info_b,
        "vantagem": a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
    }
