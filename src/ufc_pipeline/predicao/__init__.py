"""
Pacote de predição de lutas UFC.

Estrutura:
    base.py              — DadosLutador, _v
    consultas.py         — queries ao banco + carregar_dados
    score.py             — calcular_score_final (lê pesos do settings.yaml)
    relatorio.py         — imprimir_relatorio
    modulos/             — uma análise por arquivo, cada uma com PESO_PADRAO

Módulos disponíveis (peso padrão):
    round_a_round          4   troca_dano             4   declinio_etario        4
    curva_fadiga           3   indice_cardio          3   perfil_pressao         3
    controle_solo          3   tendencia              3   medias_apostas         3
    atividade_recente      3   pico_round             3   poder_nocaute          3
    durabilidade           3   timing_finalizacao     2   adaptacao_oponente     2
    mapa_alvos             2   probabilidade_metodo   2   crescimento_intra_luta 2
    estilo                 1

Os pesos podem ser sobrescritos em configs/settings.yaml:

    predicao:
      pesos:
        round_a_round: 5
        declinio_etario: 3
        ...
"""
from ufc_pipeline.predicao.base import DadosLutador, _v
from ufc_pipeline.predicao.consultas import carregar_dados, criar_engine
from ufc_pipeline.predicao.score import calcular_score_final, _carregar_pesos
from ufc_pipeline.predicao.relatorio import imprimir_relatorio
from ufc_pipeline.predicao.executor import executar_predicao_luta, salvar_predicoes

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

# Aliases de conveniência para os scripts de CLI
analise_estilo               = estilo.analisar
analise_round_a_round        = round_a_round.analisar
analise_curva_fadiga         = curva_fadiga.analisar
analise_indice_cardio        = indice_cardio.analisar
analise_timing_finalizacao   = timing_finalizacao.analisar
analise_adaptacao_oponente   = adaptacao_oponente.analisar
analise_perfil_pressao       = perfil_pressao.analisar
analise_troca_dano           = troca_dano.analisar
analise_controle_solo        = controle_solo.analisar
analise_mapa_alvos           = mapa_alvos.analisar
analise_medias_apostas       = medias_apostas.analisar
analise_probabilidade_metodo = probabilidade_metodo.analisar
analise_tendencia            = tendencia.analisar
analise_declinio_etario      = declinio_etario.analisar
analise_pico_round           = pico_round.analisar
analise_poder_nocaute        = poder_nocaute.analisar
analise_durabilidade         = durabilidade.analisar
analise_crescimento_intra_luta = crescimento_intra_luta.analisar
analise_atividade_recente    = atividade_recente.analisar

__all__ = [
    "DadosLutador", "_v",
    "carregar_dados", "criar_engine",
    "calcular_score_final", "_carregar_pesos",
    "imprimir_relatorio",
    "analise_estilo",
    "analise_round_a_round",
    "analise_curva_fadiga",
    "analise_indice_cardio",
    "analise_timing_finalizacao",
    "analise_adaptacao_oponente",
    "analise_perfil_pressao",
    "analise_troca_dano",
    "analise_controle_solo",
    "analise_mapa_alvos",
    "analise_medias_apostas",
    "analise_probabilidade_metodo",
    "analise_tendencia",
    "analise_declinio_etario",
    "analise_pico_round",
    "analise_poder_nocaute",
    "analise_durabilidade",
    "analise_crescimento_intra_luta",
    "analise_atividade_recente",
]
