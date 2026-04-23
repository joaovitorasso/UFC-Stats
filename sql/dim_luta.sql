IF OBJECT_ID(N'[silver].[dim_luta]', N'U') IS NULL
    CREATE TABLE [silver].[dim_luta] (
        id_luta      INT          IDENTITY(1,1) NOT NULL PRIMARY KEY,
        fight_id     VARCHAR(40)  NOT NULL UNIQUE,
        id_evento    INT          NULL,
        fight_url    VARCHAR(300) NULL,
        nome_evento  VARCHAR(300) NULL,
        criado_em    DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        atualizado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
