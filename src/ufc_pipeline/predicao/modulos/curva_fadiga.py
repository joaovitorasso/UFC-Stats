"""
Módulo: Curva de Fadiga / Cardio
Peso padrão: 3/5

Mede a queda de volume ofensivo do R1 para o R3+.
Peso moderado: cardio é decisivo em lutas longas mas irrelevante
quando há finalização precoce.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador, _v

PESO_PADRAO: int = 3


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    cardio_a = _v(a.stats, "indice_cardio", 100.0)
    cardio_b = _v(b.stats, "indice_cardio", 100.0)
    return {
        "indice_cardio_a": cardio_a,
        "indice_cardio_b": cardio_b,
        "vantagem_cardio": (
            a.nome if cardio_a > cardio_b else (b.nome if cardio_b > cardio_a else "Empate")
        ),
        "interpretacao": (
            f"{a.nome} mantém {cardio_a:.0f}% do volume no R3+. "
            f"{b.nome} mantém {cardio_b:.0f}%."
        ),
    }
