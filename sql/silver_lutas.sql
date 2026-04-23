IF OBJECT_ID(N'[silver].[lutas]', N'U') IS NOT NULL
    DROP TABLE [silver].[lutas];

CREATE TABLE [silver].[lutas] (
    id_luta       INT          NULL,
    id_evento     INT          NULL,
    nome_evento   VARCHAR(300) NULL,
    status_evento VARCHAR(100) NULL,
    ordem_luta    VARCHAR(20)  NULL,
    codigo_bonus  INT          NULL,
    tipo_luta     VARCHAR(40)  NULL,
    metodo        VARCHAR(120) NULL,
    round         VARCHAR(20)  NULL,
    tempo         VARCHAR(20)  NULL,
    formato_tempo VARCHAR(80)  NULL,
    arbitro       VARCHAR(120) NULL,
    ingerido_em   VARCHAR(10)  NULL,
    dt_particao   VARCHAR(10)  NULL
);
