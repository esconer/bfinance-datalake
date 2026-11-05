from __future__ import annotations

import asyncio
import json as _json
import logging
from dataclasses import dataclass, field

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    httpx = None  # type: ignore
    HAS_HTTPX = False

try:
    from curl_cffi.requests import AsyncSession as CurlAsyncSession
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

logger = logging.getLogger("datalake.http")

CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

ARCHIVE_HEADERS = {
    "User-Agent": CHROME_UA,
    "Accept": "text/csv,text/plain,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

BSE_API_HEADERS = {
    "User-Agent": CHROME_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bseindia.com/",
}

NIFTY_INDICES_HEADERS = {
    "User-Agent": CHROME_UA,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.niftyindices.com",
    "Referer": "https://www.niftyindices.com/reports/historical-data",
}

NSE_API_HEADERS = {
    "User-Agent": CHROME_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-financial-results",
}

_IMPERSONATE_PROFILES = ("chrome124", "chrome120", "chrome116")
_PROFILE_USER_AGENTS = {
    "chrome124": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "chrome120": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "chrome116": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/116.0.0.0 Safari/537.36"
    ),
}


@dataclass
class SimpleHTTPResponse:
    status_code: int
    content: bytes
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        try:
            return self.content.decode("utf-8-sig")
        except UnicodeDecodeError:
            # AMFI and some legacy NSE/BSE pages are Windows-1252, not UTF-8.
            return self.content.decode("cp1252", errors="replace")

    def json(self):
        return _json.loads(self.text)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class ResilientHTTPClient:
    """
    Async HTTP client using `curl_cffi` Chrome TLS impersonation for ALL requests
    (bypassing Akamai/Cloudflare JA3 blocks on NSE, BSE, NiftyIndices, and AMFI).
    """

    def __init__(self) -> None:
        if HAS_HTTPX:
            limits = httpx.Limits(max_keepalive_connections=20, max_connections=32)
            timeout = httpx.Timeout(connect=15.0, read=180.0, write=30.0, pool=30.0)
            self.client = httpx.AsyncClient(limits=limits, follow_redirects=True, timeout=timeout)
        else:
            self.client = None
        self._profile_idx = 0
        self.curl_session = (
            CurlAsyncSession(impersonate=_IMPERSONATE_PROFILES[0], timeout=45.0) if HAS_CURL_CFFI else None
        )
        self._nse_primed = False
        self._bse_primed = False
        self._nifty_primed = False
        self._nsearchives_fail_streak = 0
        self._prime_lock = asyncio.Lock()

    def _active_ua(self) -> str:
        prof = _IMPERSONATE_PROFILES[self._profile_idx % len(_IMPERSONATE_PROFILES)]
        return _PROFILE_USER_AGENTS.get(prof, CHROME_UA)

    def _align_headers(self, headers: dict[str, str]) -> dict[str, str]:
        out = dict(headers)
        if "User-Agent" in out:
            out["User-Agent"] = self._active_ua()
        return out

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()
        if self.curl_session is not None:
            await self.curl_session.close()

    async def __aenter__(self) -> "ResilientHTTPClient":
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    async def get(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict | None = None,
        retries: int = 3,
        prime_nse: bool = False,
        prime_bse: bool = False,
        timeout: float = 60.0,
    ):
        is_nsearchives = "nsearchives.nseindia.com" in url
        if is_nsearchives:
            if self._nsearchives_fail_streak >= 8:
                raise RuntimeError("nsearchives.nseindia.com is unreachable from this runner IP")
            retries = min(retries, 2)
            timeout = min(timeout, 6.0)

        base_headers = headers or ARCHIVE_HEADERS
        is_exchange = any(k in url for k in ("nseindia.com", "bseindia.com", "niftyindices.com"))
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                if prime_nse or "www.nseindia.com" in url:
                    if not self._nse_primed:
                        await self._prime_host("nse")
                if prime_bse or "api.bseindia.com" in url:
                    if not self._bse_primed:
                        await self._prime_host("bse")

                req_headers = self._align_headers(base_headers)
                if self.curl_session is not None and is_exchange:
                    c_resp = await self.curl_session.get(
                        url, headers=req_headers, params=params, allow_redirects=True, timeout=timeout
                    )
                    resp = SimpleHTTPResponse(
                        status_code=c_resp.status_code,
                        content=c_resp.content,
                        headers=dict(c_resp.headers),
                    )
                elif self.client is not None:
                    h_resp = await self.client.get(url, headers=req_headers, params=params, timeout=timeout)
                    resp = SimpleHTTPResponse(
                        status_code=h_resp.status_code,
                        content=h_resp.content,
                        headers=dict(h_resp.headers),
                    )
                elif self.curl_session is not None:
                    c_resp = await self.curl_session.get(
                        url, headers=req_headers, params=params, allow_redirects=True, timeout=timeout
                    )
                    resp = SimpleHTTPResponse(
                        status_code=c_resp.status_code,
                        content=c_resp.content,
                        headers=dict(c_resp.headers),
                    )
                else:
                    raise RuntimeError("curl_cffi or httpx required for live network calls")

                if resp.status_code in (401, 403):
                    if prime_nse or "nseindia.com" in url:
                        self._nse_primed = False
                        await asyncio.sleep(0.5 if is_nsearchives else 1.5 * (attempt + 1))
                        await self._prime_host("nse", reset_session=(attempt > 0))
                        continue
                    if prime_bse or "bseindia.com" in url:
                        self._bse_primed = False
                        await asyncio.sleep(1.5 * (attempt + 1))
                        await self._prime_host("bse", reset_session=(attempt > 0))
                        continue

                if resp.status_code in (429, 500, 502, 503, 504):
                    await asyncio.sleep(1.5 * (2 ** attempt))
                    continue

                enc = (resp.headers.get("Content-Encoding") or resp.headers.get("content-encoding") or "").lower()
                expected_len = resp.headers.get("Content-Length") or resp.headers.get("content-length")
                if not enc and expected_len and expected_len.isdigit() and len(resp.content) < int(expected_len):
                    raise RuntimeError(
                        f"Truncated HTTP download from {url}: got {len(resp.content)} of {expected_len} bytes"
                    )

                if is_nsearchives:
                    if resp.status_code in (200, 404):
                        self._nsearchives_fail_streak = 0
                    else:
                        self._nsearchives_fail_streak += 1
                return resp
            except Exception as exc:
                last_exc = exc
                if is_nsearchives and attempt + 1 >= retries:
                    self._nsearchives_fail_streak += 1
                    break
                await asyncio.sleep(0.5 if is_nsearchives else 1.5 * (2 ** attempt))
        if is_nsearchives:
            self._nsearchives_fail_streak += 1
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Exhausted retries for {url}")

    async def post(
        self,
        url: str,
        json_body: dict,
        headers: dict[str, str] | None = None,
        retries: int = 3,
        prime_nifty: bool = False,
        timeout: float = 60.0,
    ):
        base_headers = headers or NIFTY_INDICES_HEADERS
        last_exc: Exception | None = None
        for attempt in range(retries):
            try:
                if prime_nifty or "niftyindices.com" in url:
                    if not self._nifty_primed:
                        await self._prime_host("nifty")

                req_headers = self._align_headers(base_headers)
                if self.curl_session is not None:
                    c_resp = await self.curl_session.post(
                        url, json=json_body, headers=req_headers, allow_redirects=False, timeout=timeout
                    )
                    resp = SimpleHTTPResponse(
                        status_code=c_resp.status_code,
                        content=c_resp.content,
                        headers=dict(c_resp.headers),
                    )
                elif self.client is not None:
                    h_resp = await self.client.post(url, json=json_body, headers=req_headers, timeout=timeout)
                    resp = SimpleHTTPResponse(
                        status_code=h_resp.status_code,
                        content=h_resp.content,
                        headers=dict(h_resp.headers),
                    )
                else:
                    raise RuntimeError("curl_cffi or httpx required for POST calls")

                if resp.status_code in (301, 302, 401, 403) and ("niftyindices.com" in url or prime_nifty):
                    self._nifty_primed = False
                    await asyncio.sleep(1.5 * (attempt + 1))
                    await self._prime_host("nifty", reset_session=(attempt > 0))
                    continue
                if resp.status_code in (429, 500, 502, 503, 504):
                    await asyncio.sleep(2.0 * (2 ** attempt))
                    continue
                return resp
            except Exception as exc:
                last_exc = exc
                await asyncio.sleep(2.0 * (2 ** attempt))
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Exhausted POST retries for {url}")

    async def _prime_host(self, target: str, reset_session: bool = False) -> None:
        async with self._prime_lock:
            if reset_session and HAS_CURL_CFFI and self.curl_session is not None:
                try:
                    await self.curl_session.close()
                except Exception:
                    pass
                self._profile_idx = (self._profile_idx + 1) % len(_IMPERSONATE_PROFILES)
                self.curl_session = CurlAsyncSession(
                    impersonate=_IMPERSONATE_PROFILES[self._profile_idx], timeout=45.0
                )
                self._nse_primed = False
                self._bse_primed = False
                self._nifty_primed = False

            prime_headers = {
                "User-Agent": self._active_ua(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
            try:
                if target == "nse" and not self._nse_primed:
                    url = "https://www.nseindia.com"
                    if self.curl_session is not None:
                        r = await self.curl_session.get(url, headers=prime_headers, timeout=30.0)
                        if r.status_code == 200:
                            try:
                                await self.curl_session.get(
                                    "https://www.nseindia.com/market-data/live-equity-market",
                                    headers={**prime_headers, "Referer": "https://www.nseindia.com/"},
                                    timeout=30.0,
                                )
                            except Exception:
                                pass
                    elif self.client is not None:
                        r = await self.client.get(url, headers=prime_headers)
                    else:
                        return
                    if r.status_code == 200:
                        self._nse_primed = True
                elif target == "bse" and not self._bse_primed:
                    url = "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w"
                    try:
                        if self.curl_session is not None:
                            await self.curl_session.get(
                                url, headers={**prime_headers, "Referer": "https://www.bseindia.com/"}, timeout=5.0
                            )
                        elif self.client is not None:
                            await self.client.get(
                                url, headers={**prime_headers, "Referer": "https://www.bseindia.com/"}, timeout=5.0
                            )
                    except Exception:
                        pass
                    self._bse_primed = True
                elif target == "nifty" and not self._nifty_primed:
                    url = "https://www.niftyindices.com/reports/historical-data"
                    if self.curl_session is not None:
                        r = await self.curl_session.get(url, headers=prime_headers, timeout=30.0)
                    elif self.client is not None:
                        r = await self.client.get(url, headers=prime_headers)
                    else:
                        return
                    if r.status_code == 200:
                        self._nifty_primed = True
            except Exception as exc:
                logger.debug("[Prime %s] %s", target, exc)
