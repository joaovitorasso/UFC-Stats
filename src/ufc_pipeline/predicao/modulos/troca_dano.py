"""
Módulo: Troca de Dano
Peso padrão: 4/5

Calcula o saldo médio de golpes significativos (dados - recebidos)
e a razão de eficiência por round. Alto peso pois quem sai vencedor
nas trocas tem vantagem direta tanto em pontos quanto em segurança.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 4


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def saldo_medio(d: DadosLutador) -> float:
        if d.troca_dano.empty:
            return 0.0
        return float(d.troca_dano["media_saldo"].mean())

    def razao_media(d: DadosLutador) -> float:
        if d.troca_dano.empty:
            return 1.0
        return float(d.troca_dano["media_razao"].dropna().mean() or 1.0)

    sa = saldo_medio(a)
    sb = saldo_medio(b)
    ra = razao_media(a)
    rb = razao_media(b)
    return {
        "saldo_medio_a":  round(sa, 2),
        "saldo_medio_b":  round(sb, 2),
        "razao_troca_a":  round(ra, 3),
        "razao_troca_b":  round(rb, 3),
        "vantagem_troca": a.nome if sa > sb else (b.nome if sb > sa else "Empate"),
    }
