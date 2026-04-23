IF OBJECT_ID(N'[silver].[pipeline_runs]', N'U') IS NULL
    CREATE TABLE [silver].[pipeline_runs] (
        id           INT          IDENTITY(1,1) NOT NULL PRIMARY KEY,
        run_id       VARCHAR(12)  NOT NULL UNIQUE,
        dt           VARCHAR(10)  NOT NULL,
        tipo_carga   VARCHAR(20)  NOT NULL,
        estagio      VARCHAR(20)  NOT NULL,
        status       VARCHAR(20)  NOT NULL,
        iniciado_em  DATETIME2(0) NOT NULL,
        concluido_em DATETIME2(0) NULL
    );
