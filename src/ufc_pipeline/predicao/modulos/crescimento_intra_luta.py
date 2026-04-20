"""
Módulo: Crescimento Intra-Luta
Peso padrão: 2/5

Analisa se o lutador melhora ou piora o desempenho à medida que
a luta avança, comparando o volume de golpes nos rounds iniciais
vs tardios da curva de fadiga.

Perfis identificados:
- "cresce com a luta": volume aumenta do R1 para R3+
- "diminui com a luta": volume cai significativamente
- "consistente": mantém volume estável ao longo da luta
- "explosivo/declinante": alto R1, queda abrupta depois

Relevante para prever se uma luta longa favorece A ou B.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 2


def _analise_lutador(d: DadosLutador) -> dict:
    cf = d.curva_fadiga
    if cf.empty:
        return {
            "golpes_r1":    None,
            "golpes_r2":    None,
            "golpes_r3plus": None,
            "variacao_r1_r3": None,
            "perfil":       "sem dados",
            "score":        50.0,
        }

    def _media_round(rnd: int) -> float | None:
        row = cf[cf["round_number"] == rnd]
        return float(row["media_golpes"].iloc[0]) if not row.empty else None

    def _media_rounds_tardios() -> float | None:
        tardios = cf[cf["round_number"] >= 3]
        if tardios.empty:
            return None
        return float(tardios["media_golpes"].mean())

    r1    = _media_round(1)
    r2    = _media_round(2)
    r3p   = _media_rounds_tardios()

    if r1 is None:
        return {
            "golpes_r1": None, "golpes_r2": r2, "golpes_r3plus": r3p,
            "variacao_r1_r3": None, "perfil": "sem dados", "score": 50.0,
        }

    variacao = round(((r3p or r1) - r1) / max(r1, 1) * 100, 1) if r3p is not None else None

    if variacao is None:
        perfil = "indeterminado"
        score  = 50.0
    elif variacao >= 10:
        perfil = "cresce com a luta"
        score  = 65.0
    elif variacao <= -20:
        perfil = "diminui com a luta"
        score  = 35.0
    elif r1 is not None and r1 >= 8 and variacao < 0:
        perfil = "explosivo/declinante"
        score  = 55.0
    else:
        perfil = "consistente"
        score  = 55.0

    return {
        "golpes_r1":     round(r1, 1) if r1 is not None else None,
        "golpes_r2":     round(r2, 1) if r2 is not None else None,
        "golpes_r3plus": round(r3p, 1) if r3p is not None else None,
        "variacao_r1_r3": variacao,
        "perfil":        perfil,
        "score":         score,
    }


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    info_a = _analise_lutador(a)
    info_b = _analise_lutador(b)

    sa = info_a["score"]
    sb = info_b["score"]

    # Complementaridade: "cresce" vs "diminui" = A vencerá nos rounds tardios
    nota_complementar = None
    pa = info_a.get("perfil", "")
    pb = info_b.get("perfil", "")
    if pa == "cresce com a luta" and pb == "diminui com a luta":
        nota_complementar = f"{a.nome} melhora quando {b.nome} declina — favorável em lutas longas"
    elif pb == "cresce com a luta" and pa == "diminui com a luta":
        nota_complementar = f"{b.nome} melhora quando {a.nome} declina — favorável em lutas longas"

    return {
        a.nome:               info_a,
        b.nome:               info_b,
        "nota_complementar":  nota_complementar,
        "vantagem":           a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
    }
