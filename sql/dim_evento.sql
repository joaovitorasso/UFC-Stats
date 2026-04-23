IF OBJECT_ID(N'[silver].[dim_evento]', N'U') IS NULL
    CREATE TABLE [silver].[dim_evento] (
        id_evento    INT          IDENTITY(1,1) NOT NULL PRIMARY KEY,
        event_id     VARCHAR(40)  NOT NULL UNIQUE,
        nome_evento  VARCHAR(300) NULL,
        event_url    VARCHAR(300) NULL,
        criado_em    DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        atualizado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );

IF COL_LENGTH(N'[silver].[dim_evento]', N'event_url') IS NULL
    ALTER TABLE [silver].[dim_evento] ADD event_url VARCHAR(300) NULL;
