"""
Módulos de análise para predição de lutas.
Cada módulo expõe:
    PESO_PADRAO: int   — peso de 1 a 5 (configurável em configs/settings.yaml)
    analisar(a, b) -> dict
"""
from ufc_pipeline.predicao.modulos import (
    estilo,
    round_a_round,
    curva_fadiga,
    indice_cardio,
    timing_finalizacao,
    adaptacao_oponente,
    perfil_pressao,
    troca_dano,
    controle_solo,
    mapa_alvos,
    medias_apostas,
    probabilidade_metodo,
    tendencia,
    declinio_etario,
    pico_round,
    poder_nocaute,
    durabilidade,
    crescimento_intra_luta,
    atividade_recente,
)

# Pesos padrão de cada módulo (fallback quando settings.yaml não define)
PESOS_PADRAO: dict[str, int] = {
    "round_a_round":          round_a_round.PESO_PADRAO,
    "troca_dano":             troca_dano.PESO_PADRAO,
    "declinio_etario":        declinio_etario.PESO_PADRAO,
    "curva_fadiga":           curva_fadiga.PESO_PADRAO,
    "indice_cardio":          indice_cardio.PESO_PADRAO,
    "perfil_pressao":         perfil_pressao.PESO_PADRAO,
    "controle_solo":          controle_solo.PESO_PADRAO,
    "tendencia":              tendencia.PESO_PADRAO,
    "medias_apostas":         medias_apostas.PESO_PADRAO,
    "atividade_recente":      atividade_recente.PESO_PADRAO,
    "pico_round":             pico_round.PESO_PADRAO,
    "poder_nocaute":          poder_nocaute.PESO_PADRAO,
    "durabilidade":           durabilidade.PESO_PADRAO,
    "timing_finalizacao":     timing_finalizacao.PESO_PADRAO,
    "adaptacao_oponente":     adaptacao_oponente.PESO_PADRAO,
    "mapa_alvos":             mapa_alvos.PESO_PADRAO,
    "probabilidade_metodo":   probabilidade_metodo.PESO_PADRAO,
    "crescimento_intra_luta": crescimento_intra_luta.PESO_PADRAO,
    "estilo":                 estilo.PESO_PADRAO,
}
