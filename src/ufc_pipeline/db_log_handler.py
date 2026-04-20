"""
Handler de logging que grava mensagens na tabela etl.logs do SQL Server.

Características:
- Conexão lazy (abre só no primeiro log)
- Nunca lança exceção (um handler não pode crashar a aplicação)
- Thread-safe via lock interno do logging.Handler
- Grava: nível, logger, mensagem, traceback (se houver), run_id, origem
"""

from __future__ import annotations

import logging
import traceback


class DbLogHandler(logging.Handler):
    """Handler que persiste registros de log em etl.logs via pyodbc."""

    def __init__(
        self,
        conn_str: str,
        *,
        run_id: str,
        origem: str,
        level: int = logging.INFO,
    ) -> None:
        super().__init__(level)
        self._conn_str = conn_str
        self._run_id = run_id
        self._origem = origem
        self._conn = None

    # ── conexão lazy ──────────────────────────────────────────────────────────

    def _get_conn(self):
        if self._conn is not None:
            try:
                self._conn.cursor().execute("SELECT 1")
                return self._conn
            except Exception:
                self._conn = None
        try:
            import pyodbc
            self._conn = pyodbc.connect(self._conn_str, autocommit=True)
        except Exception:
            self._conn = None
        return self._conn

    # ── emit ──────────────────────────────────────────────────────────────────

    def emit(self, record: logging.LogRecord) -> None:
        try:
            conn = self._get_conn()
            if conn is None:
                return

            mensagem = self.format(record)
            # Limita para não estourar NVARCHAR(MAX) em casos absurdos
            mensagem = mensagem[:8000]

            exc_text: str | None = None
            if record.exc_info:
                exc_text = "".join(traceback.format_exception(*record.exc_info))[:4000]

            sql = (
                "INSERT INTO etl.logs "
                "(run_id, origem, nivel, logger_name, mensagem, exc_info) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            conn.execute(sql, (
                self._run_id,
                self._origem,
                record.levelname,
                record.name,
                mensagem,
                exc_text,
            ))
        except Exception:
            # Nunca propagar exceção de dentro de um handler
            self.handleError(record)

    def close(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        finally:
            self._conn = None
            super().close()
