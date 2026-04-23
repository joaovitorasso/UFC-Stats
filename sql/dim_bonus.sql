IF OBJECT_ID(N'[silver].[dim_bonus]', N'U') IS NULL
    CREATE TABLE [silver].[dim_bonus] (
        codigo_bonus    INT           NOT NULL PRIMARY KEY,
        descricao_bonus VARCHAR(120)  NOT NULL,
        criado_em       DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),
        atualizado_em   DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
    );
