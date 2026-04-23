IF OBJECT_ID(N'[silver].[eventos]', N'U') IS NOT NULL
    DROP TABLE [silver].[eventos];

CREATE TABLE [silver].[eventos] (
    id_evento    INT          NULL,
    nome         VARCHAR(300) NULL,
    data_evento  VARCHAR(10)  NULL,
    local        VARCHAR(300) NULL,
    status       VARCHAR(100) NULL,
    ingerido_em  VARCHAR(10)  NULL,
    dt_particao  VARCHAR(10)  NULL
);
