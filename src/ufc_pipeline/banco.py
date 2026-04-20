import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class ConexaoBanco:
    """Configuracao de conexao e mapeamento de tabelas do SQL Server."""

    server: str = r"localhost\SQLEXPRESS"
    database: str = "UFC_Lakehouse"
    schema: str = "dbo"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    user: str | None = None
    password: str | None = None
    encrypt: bool = False
    trust_server_certificate: bool = True
    odbc_driver: str | None = None
    odbc_extra: str | None = None

    table_map: dict = field(
        default_factory=lambda: {
            "bronze_eventos": "eventos",
            "bronze_lutas": "lutas",
            "bronze_lutadores": "lutadores",
            "silver_eventos": "eventos",
            "silver_lutas": "lutas",
            "silver_lutas_lutadores": "lutas_lutadores",
            "silver_lutadores": "lutadores",
            "gold_metricas_eventos": "metricas_eventos",
            "gold_metricas_lutadores": "metricas_lutadores",
        }
    )

    def _schema_para_chave(self, key: str) -> str:
        if key.startswith("bronze_"):
            return self.bronze_schema
        if key.startswith("silver_"):
            return self.silver_schema
        if key.startswith("gold_"):
            return self.gold_schema
        return self.schema

    def tabela(self, key: str) -> str:
        name = self.table_map[key]
        schema = self._schema_para_chave(key)
        return f"{schema}.{name}"

    @classmethod
    def do_env(cls) -> "ConexaoBanco":
        return cls(
            server=os.getenv("UFC_TARGET_SERVER", r"localhost\SQLEXPRESS"),
            database=os.getenv("UFC_TARGET_DATABASE", "UFC_Lakehouse"),
            schema=os.getenv("UFC_TARGET_SCHEMA", "dbo"),
            bronze_schema=os.getenv("UFC_BRONZE_SCHEMA", "bronze"),
            silver_schema=os.getenv("UFC_SILVER_SCHEMA", "silver"),
            gold_schema=os.getenv("UFC_GOLD_SCHEMA", "gold"),
            user=os.getenv("UFC_TARGET_USER") or None,
            password=os.getenv("UFC_TARGET_PASSWORD") or None,
            encrypt=os.getenv("UFC_TARGET_ENCRYPT", "false").strip().lower() in {"1", "true", "yes"},
            trust_server_certificate=os.getenv("UFC_TARGET_TRUST_SERVER_CERTIFICATE", "true").strip().lower()
            in {"1", "true", "yes"},
            odbc_driver=os.getenv("UFC_TARGET_ODBC_DRIVER") or None,
            odbc_extra=os.getenv("UFC_TARGET_ODBC_EXTRA") or None,
        )


# ── ODBC ─────────────────────────────────────────────────────────────────────

def resolver_driver_odbc(banco: ConexaoBanco) -> str | None:
    if banco.odbc_driver:
        return banco.odbc_driver
    try:
        import pyodbc
        drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
        preferidos = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
        ]
        for name in preferidos:
            if name in drivers:
                return name
        nao_legados = [d for d in drivers if d != "SQL Server"]
        if nao_legados:
            return sorted(nao_legados)[-1]
        if drivers:
            return drivers[-1]
    except Exception:
        return None
    return None


def montar_conn_str(banco: ConexaoBanco, *, driver: str | None = None) -> str | None:
    driver = driver or resolver_driver_odbc(banco)
    if not driver:
        return None
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={banco.server}",
        f"DATABASE={banco.database}",
    ]
    if banco.user:
        parts.append(f"UID={banco.user}")
        parts.append(f"PWD={banco.password or ''}")
    else:
        parts.append("Trusted_Connection=Yes")
    parts.append(f"Encrypt={'yes' if banco.encrypt else 'no'}")
    parts.append(f"TrustServerCertificate={'yes' if banco.trust_server_certificate else 'no'}")
    parts.append("Connection Timeout=5")
    extra = (banco.odbc_extra or "").strip()
    if extra:
        parts.append(extra.strip().strip(";"))
    return ";".join(parts)


# ── SQL interno ───────────────────────────────────────────────────────────────

def _run_sql(banco: ConexaoBanco, query: str, params=()) -> tuple[int, str, str]:
    try:
        import pyodbc
    except Exception:
        return 1, "", "pyodbc_nao_instalado"

    conn_str = montar_conn_str(banco)
    if not conn_str:
        return 1, "", "sem_driver_odbc"

    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
    except Exception as exc:
        return 1, "", str(exc)

    cursor = conn.cursor()
    linhas: list[str] = []
    try:
        cursor.execute(query, params)
        while True:
            if cursor.description:
                rows = cursor.fetchall()
                for row in rows:
                    linhas.append(" ".join("" if v is None else str(v) for v in row))
            try:
                tem_proximo = cursor.nextset()
            except Exception:
                tem_proximo = False
            if not tem_proximo:
                break
    except Exception as exc:
        return 1, "", str(exc)
    finally:
        cursor.close()
        conn.close()
    return 0, "\n".join(linhas).strip(), ""


def _primeiro_int(texto: str) -> int | None:
    m = re.search(r"(-?\d+)", texto or "")
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


# ── Metadados ETL ─────────────────────────────────────────────────────────────

def garantir_tabelas_etl(banco: ConexaoBanco) -> None:
    query = (
        "SET NOCOUNT ON; "
        "IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl'); "
        "IF OBJECT_ID('etl.controle_pipeline','U') IS NULL "
        "BEGIN "
        "CREATE TABLE etl.controle_pipeline ("
        "pipeline_name NVARCHAR(200) NOT NULL PRIMARY KEY, "
        "last_success_dt DATE NULL, "
        "last_run_at_utc DATETIME2(0) NULL, "
        "updated_at_utc DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()"
        "); "
        "END; "
        "IF OBJECT_ID('etl.auditoria_carga','U') IS NULL "
        "BEGIN "
        "CREATE TABLE etl.auditoria_carga ("
        "audit_id BIGINT IDENTITY(1,1) PRIMARY KEY, "
        "pipeline_name NVARCHAR(200) NOT NULL, "
        "run_id NVARCHAR(64) NOT NULL, "
        "run_dt DATE NOT NULL, "
        "load_mode NVARCHAR(20) NOT NULL, "
        "status NVARCHAR(20) NOT NULL, "
        "started_at_utc DATETIME2(0) NOT NULL, "
        "ended_at_utc DATETIME2(0) NULL, "
        "message NVARCHAR(2000) NULL"
        "); "
        "END; "
        "IF OBJECT_ID('etl.logs','U') IS NULL "
        "BEGIN "
        "CREATE TABLE etl.logs ("
        "log_id BIGINT IDENTITY(1,1) PRIMARY KEY, "
        "logged_at_utc DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(), "
        "run_id NVARCHAR(64) NULL, "
        "origem NVARCHAR(50) NOT NULL, "
        "nivel NVARCHAR(10) NOT NULL, "
        "logger_name NVARCHAR(200) NULL, "
        "mensagem NVARCHAR(MAX) NOT NULL, "
        "exc_info NVARCHAR(MAX) NULL"
        "); "
        "END; "
        "IF OBJECT_ID('etl.mapa_ids','U') IS NULL "
        "BEGIN "
        "CREATE TABLE etl.mapa_ids ("
        "id BIGINT IDENTITY(1,1) PRIMARY KEY, "
        "tipo NVARCHAR(20) NOT NULL, "
        "hash_id NVARCHAR(200) NOT NULL, "
        "nome NVARCHAR(400) NULL, "
        "criado_em DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(), "
        "CONSTRAINT uq_mapa_ids UNIQUE (tipo, hash_id)"
        "); "
        "END;"
    )
    codigo, _, erro = _run_sql(banco, query)
    if codigo != 0:
        raise RuntimeError(f"Falha ao criar tabelas ETL: {erro}")


def teve_sucesso_anterior(banco: ConexaoBanco, pipeline_name: str) -> bool | None:
    query = (
        "SET NOCOUNT ON; "
        "IF OBJECT_ID('etl.controle_pipeline','U') IS NULL BEGIN SELECT 0; RETURN; END; "
        "SELECT CASE WHEN EXISTS ("
        "  SELECT 1 FROM etl.controle_pipeline WHERE pipeline_name=? AND last_success_dt IS NOT NULL"
        ") THEN 1 ELSE 0 END;"
    )
    codigo, saida, _ = _run_sql(banco, query, (pipeline_name,))
    if codigo != 0:
        return None
    val = _primeiro_int(saida)
    if val is None:
        return None
    return val == 1


def registrar_inicio(banco: ConexaoBanco, *, pipeline_name: str, run_id: str, run_dt: str, load_mode: str) -> None:
    query = (
        "SET NOCOUNT ON; "
        "INSERT INTO etl.auditoria_carga (pipeline_name, run_id, run_dt, load_mode, status, started_at_utc) "
        "VALUES (?, ?, ?, ?, 'running', SYSUTCDATETIME());"
    )
    _run_sql(banco, query, (pipeline_name, run_id, run_dt, load_mode))


def registrar_fim(banco: ConexaoBanco, *, run_id: str, status: str, message: str | None = None) -> None:
    query = (
        "SET NOCOUNT ON; "
        "UPDATE etl.auditoria_carga "
        "SET status=?, ended_at_utc=SYSUTCDATETIME(), message=? "
        "WHERE run_id=?;"
    )
    _run_sql(banco, query, (status, (message or "")[:1900], run_id))


def marcar_sucesso(banco: ConexaoBanco, *, pipeline_name: str, run_dt: str) -> None:
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    query = (
        "SET NOCOUNT ON; "
        "MERGE etl.controle_pipeline AS tgt "
        "USING (SELECT ? AS pipeline_name, CAST(? AS DATE) AS last_success_dt, CAST(? AS DATETIME2(0)) AS last_run_at_utc) AS src "
        "ON tgt.pipeline_name = src.pipeline_name "
        "WHEN MATCHED THEN UPDATE SET "
        "tgt.last_success_dt = src.last_success_dt, "
        "tgt.last_run_at_utc = src.last_run_at_utc, "
        "tgt.updated_at_utc = SYSUTCDATETIME() "
        "WHEN NOT MATCHED THEN INSERT (pipeline_name, last_success_dt, last_run_at_utc, updated_at_utc) "
        "VALUES (src.pipeline_name, src.last_success_dt, src.last_run_at_utc, SYSUTCDATETIME());"
    )
    codigo, _, erro = _run_sql(banco, query, (pipeline_name, run_dt, now_utc))
    if codigo != 0:
        raise RuntimeError(f"Falha ao atualizar controle_pipeline: {erro}")


def banco_tem_dados(banco: ConexaoBanco) -> bool | None:
    """Verifica se o banco ja tem dados importados (bronze ou silver)."""
    query = (
        "SET NOCOUNT ON; "
        "DECLARE @tem_dados BIT = 0; "
        "IF OBJECT_ID('bronze.eventos','U') IS NOT NULL AND EXISTS (SELECT 1 FROM bronze.eventos) SET @tem_dados = 1; "
        "IF OBJECT_ID('silver.eventos','U') IS NOT NULL AND EXISTS (SELECT 1 FROM silver.eventos) SET @tem_dados = 1; "
        "SELECT CAST(@tem_dados AS INT);"
    )
    try:
        import pyodbc
    except Exception:
        return None

    conn_str = montar_conn_str(banco)
    if not conn_str:
        return None
    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
    except Exception:
        return None

    cursor = conn.cursor()
    linhas: list[str] = []
    try:
        cursor.execute(query)
        while True:
            if cursor.description:
                rows = cursor.fetchall()
                for row in rows:
                    linhas.append(" ".join("" if v is None else str(v) for v in row))
            try:
                tem_proximo = cursor.nextset()
            except Exception:
                tem_proximo = False
            if not tem_proximo:
                break
    except Exception:
        return None
    finally:
        cursor.close()
        conn.close()

    valor = _primeiro_int("\n".join(linhas).strip())
    if valor is None:
        return None
    return valor == 1
