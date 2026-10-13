-- Azure SQL — tabla de auditoría del CDC (cdc-control-api)
CREATE TABLE dbo.CDC_AUDIT (
    AUDIT_ID        BIGINT IDENTITY(1,1) PRIMARY KEY,
    EVENT_TS        DATETIME2(3)   NOT NULL,
    APP             NVARCHAR(60)   NOT NULL,
    STAGE           NVARCHAR(20)   NOT NULL,   -- PUBLISHED · COMMITTED · DLQ · RECONCILE
    TABNAME         NVARCHAR(40)   NULL,
    CORRELATION_ID  NVARCHAR(64)   NULL,
    RECORDS         INT            NULL,       -- filas de resumen (correlationId NULL) llevan el conteo
    DETAIL          NVARCHAR(MAX)  NULL
);
CREATE INDEX IX_CDC_AUDIT_CORR  ON dbo.CDC_AUDIT (CORRELATION_ID);
CREATE INDEX IX_CDC_AUDIT_TABLE ON dbo.CDC_AUDIT (TABNAME, STAGE, EVENT_TS);
