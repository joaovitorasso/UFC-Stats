-- ============================================================
-- Views Analíticas — UFC Lakehouse
-- Execute este script no banco UFC_Lakehouse para criar
-- as views que alimentam o sistema de predição.
-- ============================================================

-- ============================================================
-- VIEW 1: Desempenho por round (VIEW BASE — todas dependem desta)
-- ============================================================
CREATE OR ALTER VIEW gold.v_desempenho_por_round AS
SELECT
    l.id_luta,
    l.id_evento,
    l.nome_evento,
    e.data_evento,
    l.metodo,
    TRY_CAST(l.[round] AS INT)    AS rounds_total,
    l.tempo                        AS fight_time,
    l.tipo_luta,
    COALESCE(ll.id_lutador, mf.id)      AS id_lutador,
    ll.nome_lutador,
    ll.resultado,
    ll.round_number,

    ISNULL(ll.sig_str_landed, 0)        AS sig_str_landed,
    ISNULL(ll.sig_str_tentados, 0)      AS sig_str_tentados,
    ISNULL(ll.sig_str_pct, 0)           AS sig_str_pct,
    ISNULL(ll.head_landed, 0)           AS head_landed,
    ISNULL(ll.body_landed, 0)           AS body_landed,
    ISNULL(ll.leg_landed, 0)            AS leg_landed,
    ISNULL(ll.distance_landed, 0)       AS distance_landed,
    ISNULL(ll.clinch_landed, 0)         AS clinch_landed,
    ISNULL(ll.ground_landed, 0)         AS ground_landed

FROM silver.lutas l
LEFT JOIN silver.eventos e ON l.id_evento = e.id_evento
JOIN silver.lutas_lutadores ll
    ON ll.id_luta = l.id_luta
LEFT JOIN etl.mapa_ids mf ON mf.hash_id = ll.fighter_id_hash AND mf.tipo = 'lutador'
WHERE ll.round_number > 0;
GO

-- ============================================================
-- VIEW 2: Curva de Fadiga
-- ============================================================
CREATE OR ALTER VIEW gold.v_curva_fadiga AS
SELECT
    id_lutador,
    nome_lutador,
    round_number,
    COUNT(DISTINCT id_luta)                            AS rounds_disputados,
    ROUND(AVG(CAST(sig_str_landed AS FLOAT)), 2)       AS media_golpes,
    ROUND(AVG(sig_str_pct), 2)                         AS media_precisao,
    ROUND(AVG(CAST(head_landed     AS FLOAT)), 2)      AS media_head,
    ROUND(AVG(CAST(body_landed     AS FLOAT)), 2)      AS media_body,
    ROUND(AVG(CAST(leg_landed      AS FLOAT)), 2)      AS media_leg,
    ROUND(AVG(CAST(distance_landed AS FLOAT)), 2)      AS media_distancia,
    ROUND(AVG(CAST(clinch_landed   AS FLOAT)), 2)      AS media_clinch,
    ROUND(AVG(CAST(ground_landed   AS FLOAT)), 2)      AS media_ground
FROM gold.v_desempenho_por_round
GROUP BY id_lutador, nome_lutador, round_number;
GO

-- ============================================================
-- VIEW 3: Troca de Dano por Round
-- ============================================================
CREATE OR ALTER VIEW gold.v_troca_dano AS
SELECT
    a.id_luta,
    a.id_evento,
    a.nome_evento,
    a.data_evento,
    a.id_lutador,
    a.nome_lutador,
    a.resultado,
    a.round_number,
    a.sig_str_landed                                                    AS golpes_dados,
    b.sig_str_landed                                                    AS golpes_recebidos,
    CASE WHEN b.sig_str_landed > 0
         THEN ROUND(CAST(a.sig_str_landed AS FLOAT) / b.sig_str_landed, 3)
         ELSE NULL END                                                  AS razao_troca,
    a.sig_str_landed - b.sig_str_landed                                 AS saldo_golpes
FROM gold.v_desempenho_por_round a
JOIN gold.v_desempenho_por_round b
    ON  a.id_luta     = b.id_luta
    AND a.round_number = b.round_number
    AND a.id_lutador  <> b.id_lutador;
GO

-- ============================================================
-- VIEW 4: Timing de Finalização
-- ============================================================
CREATE OR ALTER VIEW gold.v_timing_finalizacao AS
WITH llt_fight AS (
    SELECT
        ll.id_luta,
        ll.fighter_id_hash,
        MAX(ll.id_lutador)   AS id_lutador,
        MAX(ll.nome_lutador) AS nome_lutador,
        MAX(ll.resultado)    AS resultado
    FROM silver.lutas_lutadores ll
    GROUP BY ll.id_luta, ll.fighter_id_hash
)
SELECT
    COALESCE(llt.id_lutador, mf.id)                                     AS id_lutador,
    llt.nome_lutador,
    l.metodo,
    TRY_CAST(l.[round] AS INT)                                          AS round_fim,
    l.tempo                                                             AS tempo_fim,
    ISNULL(TRY_CAST(
        CASE WHEN CHARINDEX(':', l.tempo) > 0
        THEN LEFT(l.tempo, CHARINDEX(':', l.tempo) - 1)
        ELSE NULL END
    AS INT), 0)                                                         AS minutos_decorridos,
    COUNT(*)                                                            AS quantidade
FROM silver.lutas l
JOIN llt_fight llt ON llt.id_luta = l.id_luta
LEFT JOIN etl.mapa_ids mf ON mf.hash_id = llt.fighter_id_hash AND mf.tipo = 'lutador'
WHERE llt.resultado = 'W'
  AND l.metodo IS NOT NULL
  AND l.metodo NOT LIKE 'Decision%'
GROUP BY
    COALESCE(llt.id_lutador, mf.id), llt.nome_lutador,
    l.metodo, l.[round], l.tempo;
GO

-- ============================================================
-- VIEW 5: Mapa de Alvos por Round
-- ============================================================
CREATE OR ALTER VIEW gold.v_mapa_alvos AS
SELECT
    id_lutador,
    nome_lutador,
    round_number,
    SUM(head_landed)                                                    AS total_head,
    SUM(body_landed)                                                    AS total_body,
    SUM(leg_landed)                                                     AS total_leg,
    SUM(sig_str_landed)                                                 AS total_sig,
    CASE WHEN SUM(sig_str_landed) > 0
         THEN ROUND(100.0 * SUM(head_landed) / SUM(sig_str_landed), 1) ELSE 0 END AS pct_head,
    CASE WHEN SUM(sig_str_landed) > 0
         THEN ROUND(100.0 * SUM(body_landed) / SUM(sig_str_landed), 1) ELSE 0 END AS pct_body,
    CASE WHEN SUM(sig_str_landed) > 0
         THEN ROUND(100.0 * SUM(leg_landed)  / SUM(sig_str_landed), 1) ELSE 0 END AS pct_leg
FROM gold.v_desempenho_por_round
GROUP BY id_lutador, nome_lutador, round_number;
GO

-- ============================================================
-- VIEW 6: Probabilidade de Método de Vitória
-- ============================================================
CREATE OR ALTER VIEW gold.v_probabilidade_metodo AS
WITH llt_fight AS (
    SELECT
        ll.id_luta,
        ll.fighter_id_hash,
        MAX(ll.id_lutador)   AS id_lutador,
        MAX(ll.nome_lutador) AS nome_lutador,
        MAX(ll.resultado)    AS resultado
    FROM silver.lutas_lutadores ll
    GROUP BY ll.id_luta, ll.fighter_id_hash
)
SELECT
    COALESCE(llt.id_lutador, mf.id)                                                     AS id_lutador,
    llt.nome_lutador,
    COUNT(*)                                                                            AS total_lutas,
    SUM(CASE WHEN llt.resultado = 'W' THEN 1 ELSE 0 END)                               AS total_vitorias,
    SUM(CASE WHEN llt.resultado = 'L' THEN 1 ELSE 0 END)                               AS total_derrotas,

    SUM(CASE WHEN llt.resultado = 'W' AND l.metodo LIKE '%KO%'         THEN 1 ELSE 0 END) AS v_ko_tko,
    SUM(CASE WHEN llt.resultado = 'W' AND l.metodo LIKE '%Submission%' THEN 1 ELSE 0 END) AS v_sub,
    SUM(CASE WHEN llt.resultado = 'W' AND l.metodo LIKE '%Decision%'   THEN 1 ELSE 0 END) AS v_decisao,

    SUM(CASE WHEN llt.resultado = 'L' AND l.metodo LIKE '%KO%'         THEN 1 ELSE 0 END) AS d_ko_tko,
    SUM(CASE WHEN llt.resultado = 'L' AND l.metodo LIKE '%Submission%' THEN 1 ELSE 0 END) AS d_sub,
    SUM(CASE WHEN llt.resultado = 'L' AND l.metodo LIKE '%Decision%'   THEN 1 ELSE 0 END) AS d_decisao,

    CASE WHEN SUM(CASE WHEN llt.resultado='W' THEN 1 ELSE 0 END) > 0
         THEN ROUND(100.0 * SUM(CASE WHEN llt.resultado='W' AND l.metodo LIKE '%KO%'         THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN llt.resultado='W' THEN 1 ELSE 0 END),0),1)
         ELSE 0 END AS pct_v_ko_tko,
    CASE WHEN SUM(CASE WHEN llt.resultado='W' THEN 1 ELSE 0 END) > 0
         THEN ROUND(100.0 * SUM(CASE WHEN llt.resultado='W' AND l.metodo LIKE '%Submission%' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN llt.resultado='W' THEN 1 ELSE 0 END),0),1)
         ELSE 0 END AS pct_v_sub,
    CASE WHEN SUM(CASE WHEN llt.resultado='W' THEN 1 ELSE 0 END) > 0
         THEN ROUND(100.0 * SUM(CASE WHEN llt.resultado='W' AND l.metodo LIKE '%Decision%'   THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN llt.resultado='W' THEN 1 ELSE 0 END),0),1)
         ELSE 0 END AS pct_v_decisao,

    CASE WHEN SUM(CASE WHEN llt.resultado='L' THEN 1 ELSE 0 END) > 0
         THEN ROUND(100.0 * SUM(CASE WHEN llt.resultado='L' AND l.metodo LIKE '%KO%'         THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN llt.resultado='L' THEN 1 ELSE 0 END),0),1)
         ELSE 0 END AS pct_d_ko_tko,
    CASE WHEN SUM(CASE WHEN llt.resultado='L' THEN 1 ELSE 0 END) > 0
         THEN ROUND(100.0 * SUM(CASE WHEN llt.resultado='L' AND l.metodo LIKE '%Submission%' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN llt.resultado='L' THEN 1 ELSE 0 END),0),1)
         ELSE 0 END AS pct_d_sub,
    CASE WHEN SUM(CASE WHEN llt.resultado='L' THEN 1 ELSE 0 END) > 0
         THEN ROUND(100.0 * SUM(CASE WHEN llt.resultado='L' AND l.metodo LIKE '%Decision%'   THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN llt.resultado='L' THEN 1 ELSE 0 END),0),1)
         ELSE 0 END AS pct_d_decisao

FROM silver.lutas l
JOIN llt_fight llt ON llt.id_luta = l.id_luta
LEFT JOIN etl.mapa_ids mf ON mf.hash_id = llt.fighter_id_hash AND mf.tipo = 'lutador'
GROUP BY COALESCE(llt.id_lutador, mf.id), llt.nome_lutador;
GO

-- ============================================================
-- VIEW 7: Histórico de Luta por Lutador
-- ============================================================
CREATE OR ALTER VIEW gold.v_historico_lutador AS
WITH lt_latest AS (
    SELECT
        bl.fighter_id,
        bl.nome,
        bl.lutas_json,
        ROW_NUMBER() OVER (
            PARTITION BY bl.fighter_id
            ORDER BY TRY_CAST(bl.ingerido_em AS DATETIME2) DESC, bl.dt_carga DESC
        ) AS rn
    FROM bronze.lutadores bl
    WHERE bl.lutas_json IS NOT NULL
)
SELECT
    ml.id                                             AS id_lutador,
    lt.nome                                           AS nome_lutador,
    fh.resultado,
    fh.oponente,
    fh.data_evento,
    ISNULL(TRY_CAST(fh.kd  AS INT), 0)               AS knockdowns,
    ISNULL(TRY_CAST(fh.str AS INT), 0)               AS strikes_total,
    ISNULL(TRY_CAST(fh.td  AS INT), 0)               AS takedowns,
    ISNULL(TRY_CAST(fh.sub AS INT), 0)               AS tentativas_sub,
    fh.method_short,
    fh.method_detail,
    TRY_CAST(fh.round AS INT)                         AS round_fim,
    fh.fight_time,
    fh.title_bout
FROM lt_latest lt
JOIN etl.mapa_ids ml ON ml.hash_id = lt.fighter_id AND ml.tipo = 'lutador'
CROSS APPLY OPENJSON(lt.lutas_json) WITH (
    resultado     NVARCHAR(20)  '$.result',
    oponente      NVARCHAR(200) '$.opponent',
    data_evento   NVARCHAR(100) '$.event_date',
    kd            NVARCHAR(20)  '$.kd',
    str           NVARCHAR(20)  '$.str',
    td            NVARCHAR(20)  '$.td',
    sub           NVARCHAR(20)  '$.sub',
    method_short  NVARCHAR(100) '$.method_short',
    method_detail NVARCHAR(100) '$.method_detail',
    round         NVARCHAR(10)  '$.round',
    fight_time    NVARCHAR(20)  '$.time',
    title_bout    BIT           '$.title_bout'
) fh
WHERE lt.rn = 1;
GO

-- ============================================================
-- VIEW 8: Estatísticas Completas do Lutador
-- ============================================================
CREATE OR ALTER VIEW gold.v_estatisticas_lutador AS
WITH round_stats AS (
    SELECT
        id_lutador,
        nome_lutador,
        COUNT(DISTINCT id_luta)                           AS lutas_com_round_data,
        COUNT(*)                                          AS total_rounds,
        ROUND(AVG(CAST(sig_str_landed AS FLOAT)), 2)      AS media_golpes_por_round,
        ROUND(AVG(sig_str_pct), 2)                        AS media_precisao,
        ROUND(AVG(CAST(head_landed     AS FLOAT)), 2)     AS media_head,
        ROUND(AVG(CAST(body_landed     AS FLOAT)), 2)     AS media_body,
        ROUND(AVG(CAST(leg_landed      AS FLOAT)), 2)     AS media_leg,
        ROUND(AVG(CAST(distance_landed AS FLOAT)), 2)     AS media_distancia,
        ROUND(AVG(CAST(clinch_landed   AS FLOAT)), 2)     AS media_clinch,
        ROUND(AVG(CAST(ground_landed   AS FLOAT)), 2)     AS media_ground,
        ROUND(AVG(CASE WHEN round_number = 1  THEN CAST(sig_str_landed AS FLOAT) END), 2) AS media_golpes_r1,
        ROUND(AVG(CASE WHEN round_number >= 3 THEN CAST(sig_str_landed AS FLOAT) END), 2) AS media_golpes_r3plus
    FROM gold.v_desempenho_por_round
    GROUP BY id_lutador, nome_lutador
),
hist_stats AS (
    SELECT
        id_lutador,
        COUNT(*)                                           AS total_lutas_hist,
        ROUND(AVG(CAST(takedowns     AS FLOAT)), 2)        AS media_takedowns,
        ROUND(AVG(CAST(knockdowns    AS FLOAT)), 2)        AS media_knockdowns,
        ROUND(AVG(CAST(tentativas_sub AS FLOAT)), 2)       AS media_tentativas_sub,
        SUM(CASE WHEN resultado IN ('win','W') THEN 1 ELSE 0 END)  AS vitorias_hist,
        SUM(CASE WHEN resultado IN ('loss','L') THEN 1 ELSE 0 END) AS derrotas_hist
    FROM gold.v_historico_lutador
    GROUP BY id_lutador
)
SELECT
    lt.id_lutador,
    lt.nome                                                 AS nome_lutador,
    lt.vitorias,
    lt.derrotas,
    lt.empates,
    lt.sem_resultado,
    lt.altura_cm,
    lt.alcance_cm,
    lt.peso_lbs                                             AS peso,
    lt.stance                                               AS stance,
    lt.data_nascimento                                      AS data_nascimento,
    CASE WHEN (lt.vitorias + lt.derrotas) > 0
         THEN ROUND(100.0 * lt.vitorias / (lt.vitorias + lt.derrotas), 1) ELSE 0 END AS win_rate_pct,
    r.lutas_com_round_data,
    r.total_rounds,
    r.media_golpes_por_round,
    r.media_precisao,
    r.media_head, r.media_body, r.media_leg,
    r.media_distancia, r.media_clinch, r.media_ground,
    r.media_golpes_r1,
    r.media_golpes_r3plus,
    CASE WHEN r.media_golpes_r1 > 0
         THEN ROUND(r.media_golpes_r3plus / r.media_golpes_r1 * 100, 1)
         ELSE NULL END                                      AS indice_cardio,
    CASE
        WHEN r.media_ground   >= r.media_distancia AND r.media_ground   >= r.media_clinch THEN 'Grappler'
        WHEN r.media_clinch   >= r.media_distancia AND r.media_clinch   >= r.media_ground THEN 'Pressão/Clinch'
        ELSE 'Striker'
    END                                                     AS estilo_predominante,
    h.media_takedowns,
    h.media_knockdowns,
    h.media_tentativas_sub
FROM silver.lutadores lt
LEFT JOIN round_stats r ON lt.id_lutador = r.id_lutador
LEFT JOIN hist_stats  h ON lt.id_lutador = h.id_lutador;
GO

-- ============================================================
-- Verificação
-- ============================================================
SELECT 'v_desempenho_por_round'   AS view_name, COUNT(*) AS registros FROM gold.v_desempenho_por_round
UNION ALL
SELECT 'v_curva_fadiga',           COUNT(*) FROM gold.v_curva_fadiga
UNION ALL
SELECT 'v_troca_dano',             COUNT(*) FROM gold.v_troca_dano
UNION ALL
SELECT 'v_timing_finalizacao',     COUNT(*) FROM gold.v_timing_finalizacao
UNION ALL
SELECT 'v_mapa_alvos',             COUNT(*) FROM gold.v_mapa_alvos
UNION ALL
SELECT 'v_probabilidade_metodo',   COUNT(*) FROM gold.v_probabilidade_metodo
UNION ALL
SELECT 'v_historico_lutador',      COUNT(*) FROM gold.v_historico_lutador
UNION ALL
SELECT 'v_estatisticas_lutador',   COUNT(*) FROM gold.v_estatisticas_lutador;
