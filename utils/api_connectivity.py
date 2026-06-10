"""
DNS + TCP reachability for EVENT_API_BASE_URL before long ingest runs.

Matches how v1/v2 call the Vivenu API (HTTPS); does not require a valid bearer token.
"""

from __future__ import annotations

import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from urllib.parse import urlparse


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
        return max(1, v)
    except ValueError:
        return default


def _load_env() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass


def _default_port(scheme: str) -> int:
    if scheme == "http":
        return 80
    return 443


def check_event_api_reachable(
    *,
    dns_timeout: float | None = None,
    connect_timeout: float | None = None,
    max_attempts: int | None = None,
) -> tuple[bool, str]:
    """
    Resolve the API host from EVENT_API_BASE_URL and open a TCP connection to the HTTPS port.

    Defaults are more tolerant of slow or flaky networks than short HTTP client timeouts.
    Override with env: CONNECTIVITY_DNS_TIMEOUT, CONNECTIVITY_TCP_TIMEOUT, CONNECTIVITY_CHECK_ATTEMPTS.

    Returns (success, human-readable message for stdout or logs).
    """
    _load_env()
    if dns_timeout is None:
        dns_timeout = _env_float("CONNECTIVITY_DNS_TIMEOUT", 25.0)
    if connect_timeout is None:
        connect_timeout = _env_float("CONNECTIVITY_TCP_TIMEOUT", 25.0)
    if max_attempts is None:
        max_attempts = _env_int("CONNECTIVITY_CHECK_ATTEMPTS", 3)

    base = (os.getenv("EVENT_API_BASE_URL") or "").strip().rstrip("/")
    if not base:
        return False, (
            "EVENT_API_BASE_URL is not set. "
            "Set it in .env or the container environment before running ingest."
        )

    parsed = urlparse(base if "://" in base else f"https://{base}")
    host = (parsed.hostname or "").strip()
    if not host:
        return False, f"Could not parse hostname from EVENT_API_BASE_URL={base!r}"

    scheme = (parsed.scheme or "https").lower()
    port = parsed.port or _default_port(scheme)

    header = (
        f"Checking API host {host!r} (from EVENT_API_BASE_URL)… "
        f"(up to {max_attempts} attempt(s), DNS {dns_timeout:.0f}s, TCP {connect_timeout:.0f}s)"
    )
    lines: list[str] = [header]

    def resolve() -> list[str]:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        addrs = sorted({item[4][0] for item in infos})
        return addrs

    last_failure: str | None = None
    for attempt in range(1, max_attempts + 1):
        attempt_lines: list[str] = []
        if max_attempts > 1:
            attempt_lines.append(f"Attempt {attempt}/{max_attempts}")

        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                fut = pool.submit(resolve)
                addrs = fut.result(timeout=dns_timeout)
        except FuturesTimeout:
            last_failure = "\n".join(
                attempt_lines
                + [
                    f"DNS: timed out after {dns_timeout:.0f}s for {host!r}.",
                    "If Postman works on your Mac but this fails in Docker, check container DNS "
                    "(e.g. docker-compose `dns:` or Docker Desktop / VPN settings).",
                ]
            )
        except socket.gaierror as e:
            last_failure = "\n".join(
                attempt_lines
                + [
                    f"DNS failed for {host!r}: {e}.",
                    "Postman uses your Mac resolver; this process uses the container/host resolver.",
                ]
            )
        except OSError as e:
            last_failure = "\n".join(attempt_lines + [f"DNS error for {host!r}: {e}"])
        else:
            attempt_lines.append(
                f"DNS: OK ({', '.join(addrs[:4])}{'…' if len(addrs) > 4 else ''})"
            )
            try:
                with socket.create_connection((host, port), timeout=connect_timeout):
                    pass
            except TimeoutError:
                last_failure = "\n".join(
                    attempt_lines
                    + [
                        f"TCP connect to {host}:{port} timed out after {connect_timeout:.0f}s.",
                    ]
                )
            except OSError as e:
                last_failure = "\n".join(
                    attempt_lines + [f"TCP connect to {host}:{port} failed: {e}"]
                )
            else:
                attempt_lines.append(f"TCP {port}: OK")
                lines.extend(attempt_lines)
                lines.append("API host is reachable from this environment.")
                return True, "\n".join(lines)

        lines.append(last_failure or "Connectivity check failed.")
        if attempt < max_attempts:
            delay = 1.5 * attempt
            lines.append(f"Retrying in {delay:.1f}s…")
            time.sleep(delay)

    return False, "\n".join(lines)


if __name__ == "__main__":
    ok, report = check_event_api_reachable()
    print(report)
    raise SystemExit(0 if ok else 1)
