IF OBJECT_ID(N'[silver].[historico_lutas]', N'U') IS NOT NULL
    DROP TABLE [silver].[historico_lutas];

CREATE TABLE [silver].[historico_lutas] (
    id_lutador    INT          NULL,
    id_luta       INT          NULL,
    id_evento     INT          NULL,
    nome_lutador  VARCHAR(200) NULL,
    resultado     VARCHAR(20)  NULL,
    nome_evento   VARCHAR(300) NULL,
    ordem_luta    VARCHAR(20)  NULL,
    codigo_bonus  INT          NULL,
    tipo_luta     VARCHAR(40)  NULL,
    metodo        VARCHAR(120) NULL,
    round_final   VARCHAR(20)  NULL,
    round_num     INT          NULL,
    tempo         VARCHAR(20)  NULL,
    formato_tempo VARCHAR(80)  NULL,
    arbitro       VARCHAR(120) NULL,
    luta_titulo   INT          NULL,
    sig_str       VARCHAR(40)  NULL,
    sig_str_pct   VARCHAR(20)  NULL,
    head          VARCHAR(40)  NULL,
    body          VARCHAR(40)  NULL,
    leg           VARCHAR(40)  NULL,
    distance      VARCHAR(40)  NULL,
    clinch        VARCHAR(40)  NULL,
    ground        VARCHAR(40)  NULL,
    ingerido_em   VARCHAR(10)  NULL,
    dt_particao   VARCHAR(10)  NULL
);
