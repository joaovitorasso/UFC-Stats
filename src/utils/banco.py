import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ConexaoBanco:
    """Configuracao basica de conexao com SQL Server."""

    server: str = r"localhost\SQLEXPRESS"
    database: str = "UFC_Lakehouse"
    silver_schema: str = "silver"
    user: str | None = None
    password: str | None = None
    encrypt: bool = False
    trust_server_certificate: bool = True
    odbc_driver: str | None = None
    odbc_extra: str | None = None

    @classmethod
    def do_env(cls) -> "ConexaoBanco":
        return cls(
            server=os.getenv("UFC_TARGET_SERVER", r"localhost\SQLEXPRESS"),
            database=os.getenv("UFC_TARGET_DATABASE", "UFC_Lakehouse"),
            silver_schema=os.getenv("UFC_SILVER_SCHEMA", "silver"),
            user=os.getenv("UFC_TARGET_USER") or None,
            password=os.getenv("UFC_TARGET_PASSWORD") or None,
            encrypt=os.getenv("UFC_TARGET_ENCRYPT", "false").strip().lower() in {"1", "true", "yes"},
            trust_server_certificate=os.getenv("UFC_TARGET_TRUST_SERVER_CERTIFICATE", "true").strip().lower()
            in {"1", "true", "yes"},
            odbc_driver=os.getenv("UFC_TARGET_ODBC_DRIVER") or None,
            odbc_extra=os.getenv("UFC_TARGET_ODBC_EXTRA") or None,
        )


def resolver_driver_odbc(banco: ConexaoBanco) -> str | None:
    if banco.odbc_driver:
        return banco.odbc_driver
    try:
        import pyodbc

        drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    except Exception:
        return None

    preferidos = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
    ]
    for nome in preferidos:
        if nome in drivers:
            return nome
    if drivers:
        return drivers[-1]
    return None


def montar_conn_str(banco: ConexaoBanco) -> str | None:
    driver = resolver_driver_odbc(banco)
    if not driver:
        return None

    partes = [
        f"DRIVER={{{driver}}}",
        f"SERVER={banco.server}",
        f"DATABASE={banco.database}",
    ]
    if banco.user:
        partes.append(f"UID={banco.user}")
        partes.append(f"PWD={banco.password or ''}")
    else:
        partes.append("Trusted_Connection=Yes")
    partes.append(f"Encrypt={'yes' if banco.encrypt else 'no'}")
    partes.append(f"TrustServerCertificate={'yes' if banco.trust_server_certificate else 'no'}")

    extra = (banco.odbc_extra or "").strip().strip(";")
    if extra:
        partes.append(extra)

    return ";".join(partes)
