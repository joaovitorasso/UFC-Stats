IF OBJECT_ID(N'[silver].[dim_lutador]', N'U') IS NULL
    CREATE TABLE [silver].[dim_lutador] (
        id_lutador    INT          IDENTITY(1,1) NOT NULL PRIMARY KEY,
        fighter_id    VARCHAR(40)  NOT NULL UNIQUE,
        nome_lutador  VARCHAR(200) NULL,
        url_perfil    VARCHAR(300) NULL,
        criado_em     DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        atualizado_em DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );

IF COL_LENGTH(N'[silver].[dim_lutador]', N'url_perfil') IS NULL
    ALTER TABLE [silver].[dim_lutador] ADD url_perfil VARCHAR(300) NULL;
