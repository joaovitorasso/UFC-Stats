"""
Tipos e utilitários base compartilhados pelos módulos de predição.
"""
from __future__ import annotations

import pandas as pd
from dataclasses import dataclass, field


@dataclass
class DadosLutador:
    nome: str
    stats: dict = field(default_factory=dict)
    curva_fadiga: pd.DataFrame = field(default_factory=pd.DataFrame)
    troca_dano: pd.DataFrame = field(default_factory=pd.DataFrame)
    timing_finalizacao: pd.DataFrame = field(default_factory=pd.DataFrame)
    mapa_alvos: pd.DataFrame = field(default_factory=pd.DataFrame)
    prob_metodo: dict = field(default_factory=dict)
    historico: pd.DataFrame = field(default_factory=pd.DataFrame)


def _v(d: dict, key: str, default: float = 0.0) -> float:
    """Lê um valor numérico do dicionário de stats com fallback seguro."""
    val = d.get(key)
    return float(val) if val is not None else default
