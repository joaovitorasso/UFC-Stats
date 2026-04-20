-- ============================================================
-- Mapa de IDs Sequenciais — UFC Lakehouse
-- Criado automaticamente pelo loader via etl.mapa_ids.
-- Execute este script para criar views de conveniência
-- que expõem informações de mapeamento hash→ID inteiro.
-- ============================================================

-- ============================================================
-- VIEW: Mapa completo de IDs (hash <-> inteiro)
-- ============================================================
CREATE OR ALTER VIEW etl.v_mapa_ids AS
SELECT
    m.id,
    m.tipo,
    m.hash_id,
    m.nome,
    m.criado_em
FROM etl.mapa_ids m;
GO

-- ============================================================
-- VIEW: Eventos com hash (para referência cruzada)
-- ============================================================
CREATE OR ALTER VIEW etl.v_eventos_com_hash AS
SELECT
    e.id_evento,
    m.hash_id   AS hash_evento,
    e.nome,
    e.data_evento,
    e.local,
    e.status
FROM silver.eventos e
JOIN etl.mapa_ids m ON m.id = e.id_evento AND m.tipo = 'evento';
GO

-- ============================================================
-- VIEW: Lutadores com hash (para referência cruzada)
-- ============================================================
CREATE OR ALTER VIEW etl.v_lutadores_com_hash AS
SELECT
    lt.id_lutador,
    m.hash_id   AS hash_lutador,
    lt.nome,
    lt.cartel_texto,
    lt.vitorias,
    lt.derrotas,
    lt.empates,
    lt.sem_resultado,
    lt.altura_cm,
    lt.alcance_cm
FROM silver.lutadores lt
JOIN etl.mapa_ids m ON m.id = lt.id_lutador AND m.tipo = 'lutador';
GO

-- ============================================================
-- VIEW: Lutas com hash (para referência cruzada)
-- ============================================================
CREATE OR ALTER VIEW etl.v_lutas_com_hash AS
SELECT
    l.id_luta,
    ml.hash_id  AS hash_luta,
    l.id_evento,
    me.hash_id  AS hash_evento,
    l.nome_evento,
    l.ordem_luta,
    l.tipo_luta,
    l.metodo,
    l.[round],
    l.tempo,
    l.arbitro
FROM silver.lutas l
JOIN etl.mapa_ids ml ON ml.id = l.id_luta   AND ml.tipo = 'luta'
JOIN etl.mapa_ids me ON me.id = l.id_evento  AND me.tipo = 'evento';
GO

-- ============================================================
-- VIEW: Predições com hashes (para referência cruzada)
-- Criada apenas se gold.predicoes_evento já existir.
-- Execute prever_evento.py primeiro para criar a tabela,
-- depois rode este bloco novamente.
-- ============================================================
IF OBJECT_ID('gold.predicoes_evento', 'U') IS NOT NULL
BEGIN
    EXEC('
    CREATE OR ALTER VIEW etl.v_predicoes_com_hash AS
    SELECT
        p.id_luta,
        ml.hash_id                              AS hash_luta,
        p.id_evento,
        me.hash_id                              AS hash_evento,
        p.nome_evento,
        p.data_evento,
        p.ordem_luta,
        p.tipo_luta,
        p.id_lutador_a,
        ma.hash_id                              AS hash_lutador_a,
        p.nome_lutador_a,
        p.id_lutador_b,
        mb.hash_id                              AS hash_lutador_b,
        p.nome_lutador_b,
        p.probabilidade_a,
        p.probabilidade_b,
        p.id_lutador_vencedor,
        mv.hash_id                              AS hash_vencedor,
        p.vencedor_previsto,
        p.metodo_previsto,
        p.confianca,
        p.estilo_a,
        p.estilo_b,
        p.indice_cardio_a,
        p.indice_cardio_b,
        p.win_rate_a,
        p.win_rate_b,
        p.predicao_em,
        p.run_id
    FROM gold.predicoes_evento p
    LEFT JOIN etl.mapa_ids ml ON ml.id = p.id_luta            AND ml.tipo = ''luta''
    LEFT JOIN etl.mapa_ids me ON me.id = p.id_evento           AND me.tipo = ''evento''
    LEFT JOIN etl.mapa_ids ma ON ma.id = p.id_lutador_a        AND ma.tipo = ''lutador''
    LEFT JOIN etl.mapa_ids mb ON mb.id = p.id_lutador_b        AND mb.tipo = ''lutador''
    LEFT JOIN etl.mapa_ids mv ON mv.id = p.id_lutador_vencedor AND mv.tipo = ''lutador'';
    ');
    PRINT 'View etl.v_predicoes_com_hash criada.';
END
ELSE
BEGIN
    PRINT 'AVISO: gold.predicoes_evento nao existe ainda.';
    PRINT 'Execute prever_evento.py e depois rode este script novamente.';
END
GO

-- ============================================================
-- Exemplos de uso
-- ============================================================
-- Buscar evento pelo ID inteiro:
--   SELECT * FROM silver.eventos WHERE id_evento = 1
--
-- Ver hash de um evento pelo ID:
--   SELECT * FROM etl.v_eventos_com_hash WHERE id_evento = 1
--
-- Buscar predições de um evento pelo ID inteiro:
--   SELECT * FROM gold.predicoes_evento WHERE id_evento = 1
--
-- Ver totais por tipo no mapa:
--   SELECT tipo, COUNT(*) AS total FROM etl.mapa_ids GROUP BY tipo
-- ============================================================
