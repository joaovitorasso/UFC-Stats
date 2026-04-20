"""
Módulo: Declínio Etário
Peso padrão: 4/5

Correlaciona desempenho com idade e fase da carreira.
Divide o histórico em três fases (início, meio, fim) e mede
a evolução do win rate. Também calcula a idade atual e sinaliza
se o lutador está em queda de rendimento nos últimos 3 anos.
Alto peso pois declínio físico/técnico é um dos fatores mais
subavaliados por apostadores e fãs.
"""
from __future__ import annotations

from datetime import datetime, date

import pandas as pd

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 4

_FORMATOS_DATA = ["%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%d/%m/%Y"]


def _parse_data(valor: str | None) -> date | None:
    if not valor:
        return None
    for fmt in _FORMATOS_DATA:
        try:
            return datetime.strptime(valor.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _calcular_idade(dob: date | None) -> int | None:
    if dob is None:
        return None
    hoje = date.today()
    return hoje.year - dob.year - ((hoje.month, hoje.day) < (dob.month, dob.day))


def _win_rate(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    wins = df["resultado"].isin(["win", "W"]).sum()
    return round(wins / max(len(df), 1) * 100, 1)


def _analise_lutador(d: DadosLutador) -> dict:
    dob    = _parse_data(d.stats.get("data_nascimento"))
    idade  = _calcular_idade(dob)

    hist = d.historico.copy()
    if hist.empty:
        return {
            "idade": idade,
            "wr_fase_inicio": None,
            "wr_fase_meio":   None,
            "wr_fase_fim":    None,
            "wr_ultimos_3anos": None,
            "em_declinio": False,
            "tendencia": "sem dados",
        }

    hist["data_dt"] = pd.to_datetime(hist["data_evento"], errors="coerce")
    hist_ord = hist.dropna(subset=["data_dt"]).sort_values("data_dt")
    n = len(hist_ord)

    # Fases da carreira
    if n >= 6:
        t = n // 3
        wr_ini  = _win_rate(hist_ord.iloc[:t])
        wr_meio = _win_rate(hist_ord.iloc[t:2*t])
        wr_fim  = _win_rate(hist_ord.iloc[2*t:])
    else:
        wr_ini = wr_meio = wr_fim = None

    # Win rate nos últimos 3 anos
    tres_anos_atras = pd.Timestamp(date.today().replace(year=date.today().year - 3))
    recente = hist_ord[hist_ord["data_dt"] >= tres_anos_atras]
    wr_recente = _win_rate(recente) if not recente.empty else None

    # Detecta declínio: fim < início em ≥15pp e/ou últimos 3 anos < carreira geral
    em_declinio = False
    wr_geral = _win_rate(hist_ord)
    if wr_fim is not None and wr_ini is not None and (wr_ini - wr_fim) >= 15:
        em_declinio = True
    if wr_recente is not None and wr_geral > 0 and (wr_geral - wr_recente) >= 15:
        em_declinio = True

    if wr_fim is not None and wr_ini is not None:
        if wr_fim > wr_ini + 5:
            tendencia = "em ascensão"
        elif wr_ini - wr_fim >= 15:
            tendencia = "em declínio"
        else:
            tendencia = "estável"
    else:
        tendencia = "indeterminado"

    return {
        "idade":              idade,
        "wr_fase_inicio":     wr_ini,
        "wr_fase_meio":       wr_meio,
        "wr_fase_fim":        wr_fim,
        "wr_ultimos_3anos":   wr_recente,
        "em_declinio":        em_declinio,
        "tendencia":          tendencia,
    }


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    info_a = _analise_lutador(a)
    info_b = _analise_lutador(b)

    # Vantagem vai para quem está em melhor fase da carreira
    # Penaliza declínio, favorece ascensão
    def score_trajetoria(info: dict) -> float:
        s = 50.0
        if info["em_declinio"]:
            s -= 20
        if info["tendencia"] == "em ascensão":
            s += 15
        elif info["tendencia"] == "em declínio":
            s -= 10
        wr_fim = info.get("wr_fase_fim")
        if wr_fim is not None:
            s += (wr_fim - 50) * 0.3
        return s

    sa = score_trajetoria(info_a)
    sb = score_trajetoria(info_b)

    return {
        a.nome: info_a,
        b.nome: info_b,
        "score_trajetoria_a": round(sa, 1),
        "score_trajetoria_b": round(sb, 1),
        "vantagem": a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
    }
