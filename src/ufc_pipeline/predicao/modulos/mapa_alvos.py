"""
Módulo: Mapa de Alvos
Peso padrão: 2/5

Analisa a distribuição de golpes por zona (cabeça/corpo/perna) e cruza
as preferências ofensivas de cada lutador. Peso baixo pois é descritivo;
o impacto estratégico depende do estilo do oponente.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador

PESO_PADRAO: int = 2


def analisar(a: DadosLutador, b: DadosLutador) -> dict:
    def resumo(d: DadosLutador) -> dict:
        if d.mapa_alvos.empty:
            return {"pct_head": 0, "pct_body": 0, "pct_leg": 0, "alvo_principal": "N/A"}
        totais = d.mapa_alvos.sum(numeric_only=True)
        total  = max(float(totais.get("total_sig", 1)), 1)
        ph     = round(float(totais.get("total_head", 0)) / total * 100, 1)
        pb     = round(float(totais.get("total_body", 0)) / total * 100, 1)
        pl     = round(float(totais.get("total_leg",  0)) / total * 100, 1)
        alvo   = max({"Cabeça": ph, "Corpo": pb, "Perna": pl}, key=lambda k: {"Cabeça": ph, "Corpo": pb, "Perna": pl}[k])
        return {"pct_head": ph, "pct_body": pb, "pct_leg": pl, "alvo_principal": alvo}

    return {a.nome: resumo(a), b.nome: resumo(b)}
