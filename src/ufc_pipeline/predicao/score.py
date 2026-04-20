"""
Score final ponderado pelos pesos de cada módulo de análise.
Os pesos são lidos de configs/settings.yaml (seção predicao.pesos).
Se não encontrar o arquivo, usa os PESO_PADRAO de cada módulo.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from ufc_pipeline.predicao.base import DadosLutador, _v
from ufc_pipeline.predicao.modulos import PESOS_PADRAO


def _carregar_pesos() -> dict[str, float]:
    """
    Lê pesos do configs/settings.yaml.
    Sobe a árvore de diretórios a partir deste arquivo para encontrar o repo root.
    Retorna PESOS_PADRAO se o arquivo não for encontrado ou a seção estiver ausente.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "configs" / "settings.yaml"
        if candidate.exists():
            try:
                import yaml
                with open(candidate, encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)
                pesos_cfg = (cfg or {}).get("predicao", {}).get("pesos", {})
                if pesos_cfg:
                    merged = dict(PESOS_PADRAO)
                    merged.update({k: float(v) for k, v in pesos_cfg.items()})
                    return merged
            except Exception:
                pass
            break
    return {k: float(v) for k, v in PESOS_PADRAO.items()}


def calcular_score_final(
    a: DadosLutador,
    b: DadosLutador,
    analises: dict[str, Any],
    pesos: dict[str, float] | None = None,
) -> dict:
    """
    Score composto ponderado pelos pesos de cada dimensão.
    Retorna probabilidades, vencedor previsto, método e confiança.
    """
    if pesos is None:
        pesos = _carregar_pesos()

    pontos_a = 0.0
    pontos_b = 0.0

    # Dimensões com campo de vantagem direto (nome do vencedor)
    dims_vantagem = [
        # Módulos originais
        ("curva_fadiga",           "vantagem_cardio"),
        ("indice_cardio",          "vantagem"),
        ("timing_finalizacao",     "finalizador"),
        ("perfil_pressao",         "maior_pressao"),
        ("troca_dano",             "vantagem_troca"),
        ("controle_solo",          "dominante_solo"),
        ("tendencia",              "melhor_momento"),
        ("pico_round",             "vantagem"),
        ("poder_nocaute",          "vantagem"),
        ("crescimento_intra_luta", "vantagem"),
        ("atividade_recente",      "vantagem"),
        # Novos módulos com campo próprio
        ("declinio_etario",        "vantagem"),
        ("durabilidade",           "mais_duravel"),
    ]
    for dim, campo in dims_vantagem:
        peso = pesos.get(dim, PESOS_PADRAO.get(dim, 1))
        venc = analises.get(dim, {}).get(campo)
        if venc == a.nome:
            pontos_a += peso
        elif venc == b.nome:
            pontos_b += peso

    # Round a round: conta rounds vencidos ponderado pelo peso
    peso_rr = pesos.get("round_a_round", PESOS_PADRAO.get("round_a_round", 4))
    rr      = analises.get("round_a_round", {})
    rv_a    = rr.get("rounds_vantagem_a", 0)
    rv_b    = rr.get("rounds_vantagem_b", 0)
    if rv_a > rv_b:
        pontos_a += peso_rr
    elif rv_b > rv_a:
        pontos_b += peso_rr

    # Médias de apostas: usa probabilidade implícita ponderada pelo peso
    peso_ap   = pesos.get("medias_apostas", PESOS_PADRAO.get("medias_apostas", 3))
    ap        = analises.get("medias_apostas", {})
    prob_ap_a = float(ap.get("probabilidade_implicita_a", 50))
    prob_ap_b = float(ap.get("probabilidade_implicita_b", 50))
    pontos_a += prob_ap_a / 100 * peso_ap
    pontos_b += prob_ap_b / 100 * peso_ap

    total  = max(pontos_a + pontos_b, 0.01)
    prob_a = round(pontos_a / total * 100, 1)
    prob_b = round(pontos_b / total * 100, 1)

    # Método mais provável para o vencedor previsto
    pm = analises.get("probabilidade_metodo", {})

    def _metodo_provavel(nome: str) -> str:
        opcoes = {
            "KO/TKO":     float(pm.get(f"vitoria_{nome}_por_KO", 0)),
            "Submission": float(pm.get(f"vitoria_{nome}_por_Sub", 0)),
            "Decisão":    float(pm.get(f"vitoria_{nome}_por_Decisao", 0)),
        }
        if any(v > 0 for v in opcoes.values()):
            return max(opcoes, key=opcoes.__getitem__)
        return "Decisão"

    vencedor_previsto = a.nome if prob_a >= prob_b else b.nome
    metodo_previsto   = _metodo_provavel(vencedor_previsto)
    diferenca         = abs(prob_a - prob_b)

    return {
        f"probabilidade_{a.nome}": prob_a,
        f"probabilidade_{b.nome}": prob_b,
        "vencedor_previsto": vencedor_previsto,
        "metodo_previsto":   metodo_previsto,
        "confianca":         "Alta" if diferenca > 20 else ("Média" if diferenca > 10 else "Baixa"),
    }
