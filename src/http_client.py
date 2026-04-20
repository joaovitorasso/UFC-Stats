import time
import logging
import threading
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class HttpConfig:
    user_agent: str = "Mozilla/5.0 (ufc-lakehouse)"
    timeout_seconds: int = 20
    retries: int = 4
    backoff_factor: float = 0.6
    polite_delay_seconds: float = 0.6
    max_requests_per_minute: int = 80
    workers: int = 4


class RateLimiter:
    """Rate limiter thread-safe por reserva de slot de tempo."""

    def __init__(self, cfg: HttpConfig) -> None:
        self.cfg = cfg
        self._min_interval = 60.0 / max(cfg.max_requests_per_minute, 1)
        self._next_slot = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            # Reserva o próximo slot disponível para esta thread
            slot = max(now, self._next_slot)
            self._next_slot = slot + max(self._min_interval, float(self.cfg.polite_delay_seconds))
            sleep_for = slot - now
        if sleep_for > 0:
            time.sleep(sleep_for)


class HttpClient:
    def __init__(self, cfg: HttpConfig) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": cfg.user_agent})

        retry = Retry(
            total=cfg.retries,
            backoff_factor=cfg.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.limiter = RateLimiter(cfg)

    def get_text(self, url: str, *, allow_redirects: bool = True) -> str:
        self.limiter.wait()
        log.debug("GET %s", url)
        try:
            r = self.session.get(url, timeout=self.cfg.timeout_seconds, allow_redirects=allow_redirects)
        except requests.exceptions.Timeout:
            log.error("TIMEOUT (%ds): %s", self.cfg.timeout_seconds, url)
            raise
        except requests.exceptions.ConnectionError as exc:
            log.error("ERRO CONEXÃO %s: %s", url, exc)
            raise
        except Exception as exc:
            log.error("ERRO HTTP %s: %s", url, exc)
            raise
        log.debug("HTTP %s (%d bytes) <- %s", r.status_code, len(r.content), url)
        if r.status_code != 200:
            raise requests.HTTPError(f"HTTP {r.status_code} for {url}")
        return r.text
