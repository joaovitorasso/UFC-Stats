"""
Formatação e impressão do relatório de predição no terminal.
"""
from __future__ import annotations

from ufc_pipeline.predicao.base import DadosLutador, _v

SEP       = "=" * 70
SEP_MINOR = "-" * 70


def _header(titulo: str) -> str:
    return f"\n{SEP}\n  {titulo}\n{SEP}"


def _secao(titulo: str) -> str:
    return f"\n{SEP_MINOR}\n  {titulo}\n{SEP_MINOR}"


def imprimir_relatorio(a: DadosLutador, b: DadosLutador, analises: dict, score: dict) -> None:
    print(_header(f"PREDIÇÃO: {a.nome.upper()} vs {b.nome.upper()}"))

    # ── Perfil dos lutadores ──────────────────────────────────────────────────
    print(_secao("PERFIL DOS LUTADORES"))
    for d in [a, b]:
        s = d.stats
        wins   = int(_v(s, "vitorias"))
        losses = int(_v(s, "derrotas"))
        draws  = int(_v(s, "empates"))
        print(f"  {d.nome}:")
        print(f"    Cartel:  {wins}W - {losses}L - {draws}D  |  Win Rate: {_v(s,'win_rate_pct'):.1f}%")
        print(f"    Estilo:  {s.get('estilo_predominante','?')}  |  Stance: {s.get('stance','?')}")
        altura_cm  = s.get("altura_cm")
        alcance_cm = s.get("alcance_cm")
        print(f"    Altura:  {f'{altura_cm:.1f} cm' if altura_cm else '?'}  "
              f"|  Alcance: {f'{alcance_cm:.1f} cm' if alcance_cm else '?'}")
        print(f"    Média golpes/round: {_v(s,'media_golpes_por_round'):.1f}  "
              f"|  Precisão: {_v(s,'media_precisao'):.1f}%")
        print(f"    Índice cardio: {_v(s,'indice_cardio',100):.0f}%  "
              f"|  TDs/luta: {_v(s,'media_takedowns'):.1f}  "
              f"|  KDs/luta: {_v(s,'media_knockdowns'):.2f}")
        print()

    # ── Estilo e matchup ──────────────────────────────────────────────────────
    print(_secao("1. ANÁLISE DE ESTILO — MATCHUP"))
    est = analises["estilo"]
    print(f"  {a.nome}: {est['estilo_a']}")
    print(f"  {b.nome}: {est['estilo_b']}")
    print(f"  → {est['matchup']}")

    # ── Round a round ─────────────────────────────────────────────────────────
    print(_secao("2. ROUND A ROUND — VOLUME DE GOLPES"))
    rr = analises["round_a_round"]
    for rnd, info in rr["por_round"].items():
        barra_a = "█" * int(info["golpes_a"] / 2)
        barra_b = "█" * int(info["golpes_b"] / 2)
        print(f"  {rnd}: {a.nome[:15]:<15} {info['golpes_a']:5.1f} {barra_a}")
        print(f"      {b.nome[:15]:<15} {info['golpes_b']:5.1f} {barra_b}")
    print(f"  Rounds vencidos → {a.nome}: {rr['rounds_vantagem_a']}  |  {b.nome}: {rr['rounds_vantagem_b']}")

    # ── Curva de fadiga ───────────────────────────────────────────────────────
    print(_secao("3. CURVA DE FADIGA / CARDIO"))
    cf = analises["curva_fadiga"]
    print(f"  {cf['interpretacao']}")
    print(f"  Vantagem cardio: {cf['vantagem_cardio']}")
    ic = analises["indice_cardio"]
    print(f"  Score condicionamento → {a.nome}: {ic['score_a']}  |  {b.nome}: {ic['score_b']}")

    # ── Timing de finalização ─────────────────────────────────────────────────
    print(_secao("4. TIMING DE FINALIZAÇÃO"))
    tf = analises["timing_finalizacao"]
    for nome in [a.nome, b.nome]:
        info = tf.get(nome, {})
        print(f"  {nome}: {info.get('finalizacoes',0)} finalizações  "
              f"| Método: {info.get('metodo_principal','N/A')}  "
              f"| Round mais comum: R{info.get('round_principal','?')}")
    print(f"  Maior finalizador: {tf['finalizador']}")

    # ── Adaptação a oponentes ─────────────────────────────────────────────────
    print(_secao("5. ADAPTAÇÃO — HISTÓRICO GERAL"))
    ad = analises["adaptacao_oponente"]
    for d in [a, b]:
        info = ad.get(d.nome, {})
        print(f"  {d.nome}: {info.get('total',0)} lutas  "
              f"| Win rate: {info.get('win_rate',0):.1f}%  "
              f"| KOs dados: {info.get('ko_dado',0)}  "
              f"| Subs dados: {info.get('sub_dado',0)}")

    # ── Perfil de pressão ─────────────────────────────────────────────────────
    print(_secao("6. PERFIL DE PRESSÃO"))
    pp = analises["perfil_pressao"]
    print(f"  {a.nome}: {pp['score_pressao_a']:.2f}  |  {b.nome}: {pp['score_pressao_b']:.2f}")
    print(f"  Maior pressão: {pp['maior_pressao']}")

    # ── Troca de dano ─────────────────────────────────────────────────────────
    print(_secao("7. TROCA DE DANO"))
    td = analises["troca_dano"]
    print(f"  Saldo médio de golpes → {a.nome}: {td['saldo_medio_a']:+.1f}  "
          f"|  {b.nome}: {td['saldo_medio_b']:+.1f}")
    print(f"  Razão golpes dados/recebidos → {a.nome}: {td['razao_troca_a']:.2f}  "
          f"|  {b.nome}: {td['razao_troca_b']:.2f}")
    print(f"  Vantagem na troca: {td['vantagem_troca']}")

    # ── Controle de solo ──────────────────────────────────────────────────────
    print(_secao("8. CONTROLE DE SOLO / GRAPPLING"))
    cs = analises["controle_solo"]
    print(f"  Score grappling → {a.nome}: {cs['score_grappling_a']:.2f}  "
          f"|  {b.nome}: {cs['score_grappling_b']:.2f}")
    print(f"  TDs/luta → {a.nome}: {cs['takedowns_a']:.1f}  |  {b.nome}: {cs['takedowns_b']:.1f}")
    print(f"  Subs/luta → {a.nome}: {cs['subs_a']:.1f}  |  {b.nome}: {cs['subs_b']:.1f}")
    print(f"  Dominante no solo: {cs['dominante_solo']}")

    # ── Mapa de alvos ─────────────────────────────────────────────────────────
    print(_secao("9. MAPA DE ALVOS"))
    ma = analises["mapa_alvos"]
    for d in [a, b]:
        info = ma.get(d.nome, {})
        print(f"  {d.nome}: Cabeça {info.get('pct_head',0):.1f}%  "
              f"| Corpo {info.get('pct_body',0):.1f}%  "
              f"| Perna {info.get('pct_leg',0):.1f}%  "
              f"→ Alvo: {info.get('alvo_principal','?')}")

    # ── Médias de apostas ─────────────────────────────────────────────────────
    print(_secao("10. MÉDIAS DE APOSTAS (Proxy Histórico)"))
    ap = analises["medias_apostas"]
    print(f"  Probabilidade implícita → {a.nome}: {ap['probabilidade_implicita_a']:.1f}%  "
          f"|  {b.nome}: {ap['probabilidade_implicita_b']:.1f}%")
    print(f"  Nota: {ap['nota']}")

    # ── Probabilidade de método ───────────────────────────────────────────────
    print(_secao("11. PROBABILIDADE DE MÉTODO DE VITÓRIA"))
    pm = analises["probabilidade_metodo"]
    print(f"  {a.nome} vence por KO/TKO:    {pm.get(f'vitoria_{a.nome}_por_KO',0):.1f}%")
    print(f"  {a.nome} vence por Submission: {pm.get(f'vitoria_{a.nome}_por_Sub',0):.1f}%")
    print(f"  {a.nome} vence por Decisão:    {pm.get(f'vitoria_{a.nome}_por_Decisao',0):.1f}%")
    print()
    print(f"  {b.nome} vence por KO/TKO:    {pm.get(f'vitoria_{b.nome}_por_KO',0):.1f}%")
    print(f"  {b.nome} vence por Submission: {pm.get(f'vitoria_{b.nome}_por_Sub',0):.1f}%")
    print(f"  {b.nome} vence por Decisão:    {pm.get(f'vitoria_{b.nome}_por_Decisao',0):.1f}%")

    # ── Análise de tendência ──────────────────────────────────────────────────
    print(_secao("12. TENDÊNCIA — ÚLTIMAS 5 LUTAS"))
    tr = analises["tendencia"]
    for d in [a, b]:
        info = tr.get(d.nome, {})
        print(f"  {d.nome}: {info.get('sequencia','?')}  "
              f"({info.get('vitorias_recentes',0)}V-{info.get('derrotas_recentes',0)}D)  "
              f"| Momentum: {info.get('momentum',0):+d}")
    print(f"  Melhor momento: {tr['melhor_momento']}")

    # ── Declínio etário ───────────────────────────────────────────────────────
    if "declinio_etario" in analises:
        print(_secao("13. DECLÍNIO ETÁRIO — CORRELAÇÃO DESEMPENHO × CARREIRA"))
        de = analises["declinio_etario"]
        for d in [a, b]:
            info = de.get(d.nome, {})
            idade = info.get("idade")
            tendencia_str = info.get("tendencia", "?")
            declinio = " ⚠ EM DECLÍNIO" if info.get("em_declinio") else ""
            print(f"  {d.nome}: {f'{idade} anos' if idade else 'idade desconhecida'}  "
                  f"| Carreira: {tendencia_str}{declinio}")
            ini = info.get("wr_fase_inicio")
            mei = info.get("wr_fase_meio")
            fim = info.get("wr_fase_fim")
            if ini is not None:
                print(f"    Win rate por fase → Início: {ini:.0f}%  Meio: {mei:.0f}%  Fim: {fim:.0f}%")
            wr3 = info.get("wr_ultimos_3anos")
            if wr3 is not None:
                print(f"    Win rate últimos 3 anos: {wr3:.0f}%")
        print(f"  Melhor fase atual: {de.get('vantagem','?')}")

    # ── Pico de round ─────────────────────────────────────────────────────────
    if "pico_round" in analises:
        print(_secao("14. PICO DE ROUND — TAXA DE VITÓRIA POR ROUND"))
        pr = analises["pico_round"]
        for d in [a, b]:
            info = pr.get(d.nome, {})
            pico = info.get("pico_round")
            perfil = info.get("perfil", "?")
            wr_rounds = info.get("wr_por_round", {})
            wr_str = "  ".join(f"R{r}:{v:.0f}%" for r, v in sorted(wr_rounds.items()))
            print(f"  {d.nome}: pico no R{pico or '?'} | perfil: {perfil}")
            if wr_str:
                print(f"    {wr_str}")
        conflito = pr.get("conflito_de_picos")
        if conflito:
            print(f"  → {conflito}")

    # ── Poder de nocaute ──────────────────────────────────────────────────────
    if "poder_nocaute" in analises:
        print(_secao("15. PODER DE NOCAUTE"))
        pn = analises["poder_nocaute"]
        for d in [a, b]:
            info = pn.get(d.nome, {})
            print(f"  {d.nome}: {info.get('ko_na_carreira',0)} KOs na carreira  "
                  f"| KO rate: {info.get('ko_rate_pct',0):.1f}%  "
                  f"| Finish rate: {info.get('finish_rate_pct',0):.1f}%  "
                  f"| KDs/luta: {info.get('media_knockdowns',0):.2f}  "
                  f"| Vezes KO'd: {info.get('vezes_ko_sofrido',0)}")
        print(f"  Maior poder de nocaute: {pn.get('vantagem','?')}")

    # ── Durabilidade ──────────────────────────────────────────────────────────
    if "durabilidade" in analises:
        print(_secao("16. DURABILIDADE / QUEIXO"))
        du = analises["durabilidade"]
        for d in [a, b]:
            info = du.get(d.nome, {})
            print(f"  {d.nome}: {info.get('perfil_durabilidade','?')}  "
                  f"| {info.get('pct_distancia',0):.0f}% lutas a distância  "
                  f"| {info.get('pct_derrotas_ko',0):.0f}% derrotas por KO  "
                  f"| Score: {info.get('score_durabilidade',0):.0f}")
        print(f"  Mais durável: {du.get('mais_duravel','?')}")

    # ── Crescimento intra-luta ────────────────────────────────────────────────
    if "crescimento_intra_luta" in analises:
        print(_secao("17. CRESCIMENTO INTRA-LUTA"))
        ci = analises["crescimento_intra_luta"]
        for d in [a, b]:
            info = ci.get(d.nome, {})
            r1  = info.get("golpes_r1")
            r3p = info.get("golpes_r3plus")
            var = info.get("variacao_r1_r3")
            print(f"  {d.nome}: {info.get('perfil','?')}  "
                  f"| R1: {r1 or '?'}  R3+: {r3p or '?'}  "
                  f"| Variação: {f'{var:+.1f}%' if var is not None else '?'}")
        nota = ci.get("nota_complementar")
        if nota:
            print(f"  → {nota}")

    # ── Atividade e forma recente ─────────────────────────────────────────────
    if "atividade_recente" in analises:
        print(_secao("18. ATIVIDADE E FORMA RECENTE"))
        ar = analises["atividade_recente"]
        for d in [a, b]:
            info = ar.get(d.nome, {})
            wr3  = info.get("wr_ultimas_3")
            wr_p = info.get("wr_anteriores_3")
            delta = info.get("delta_wr")
            forma = info.get("forma", "?")
            intervalo = info.get("intervalo_medio_dias")
            delta_str = f" ({delta:+.0f}pp)" if delta is not None else ""
            int_str   = f" | Intervalo médio: {intervalo:.0f} dias" if intervalo else ""
            print(f"  {d.nome}: {forma}  "
                  f"| Últimas 3: {wr3 if wr3 is not None else '?'}%{delta_str}"
                  f"{int_str}")
        print(f"  Melhor forma atual: {ar.get('vantagem','?')}")

    # ── Predição final ────────────────────────────────────────────────────────
    print(_header("PREDIÇÃO FINAL"))
    print(f"  {a.nome:<30} {score[f'probabilidade_{a.nome}']:.1f}%")
    print(f"  {b.nome:<30} {score[f'probabilidade_{b.nome}']:.1f}%")
    print()
    print(f"  ► VENCEDOR PREVISTO:  {score['vencedor_previsto']}")
    print(f"  ► MÉTODO PREVISTO:    {score['metodo_previsto']}")
    print(f"  ► CONFIANÇA:          {score['confianca']}")
    print(f"\n{SEP}\n")

    est = analises["estilo"]
    print("  ANÁLISE DE ESTILO E CHAVE DA LUTA:")
    print(f"  {est['matchup']}")
    ma_a = analises["mapa_alvos"].get(a.nome, {})
    ma_b = analises["mapa_alvos"].get(b.nome, {})
    print(f"\n  {a.nome} ataca principalmente: {ma_a.get('alvo_principal','?')}")
    print(f"  {b.nome} ataca principalmente: {ma_b.get('alvo_principal','?')}")
    print(f"\n{SEP}\n")
    print("  AVISO: Esta predição é baseada em estatísticas históricas e")
    print("  não considera lesões, condição física no dia, corner, peso etc.")
    print(f"\n{SEP}\n")
