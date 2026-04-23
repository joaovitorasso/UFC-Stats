IF OBJECT_ID(N'[silver].[lutadores]', N'U') IS NOT NULL
    DROP TABLE [silver].[lutadores];

CREATE TABLE [silver].[lutadores] (
    id_lutador      INT          NULL,
    nome            VARCHAR(200) NULL,
    cartel          VARCHAR(50)  NULL,
    altura          INT          NULL,
    peso            FLOAT        NULL,
    alcance         INT          NULL,
    stance          VARCHAR(50)  NULL,
    data_nascimento VARCHAR(40)  NULL,
    slpm            FLOAT        NULL,
    str_acc_pct     VARCHAR(20)  NULL,
    sapm            FLOAT        NULL,
    str_def_pct     VARCHAR(20)  NULL,
    td_avg_15min    FLOAT        NULL,
    td_acc_pct      VARCHAR(20)  NULL,
    td_def_pct      VARCHAR(20)  NULL,
    sub_avg_15min   FLOAT        NULL,
    ingerido_em     VARCHAR(10)  NULL,
    dt_particao     VARCHAR(10)  NULL
);
