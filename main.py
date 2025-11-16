import os
import time
import threading
import logging
from typing import Any, Dict, List, Tuple

from datetime import datetime, timezone
import requests
from fastapi import FastAPI, HTTPException

# ============================================================
# Logging
# ============================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("candle-service")

# ============================================================
# Env Vars
# ============================================================
COINBASE_PRODUCT = os.getenv("COINBASE_PRODUCT", "BTC-USD")

# Single shared env for all TFs & counts
# Example: "1m:400,5m:200,15m:60,1h:40,1d:20,1w:6"
BTC_INTERVALS_RAW = os.getenv(
    "BTC_INTERVALS",
    "5m:50,15m:25,1h:15",  # sensible default matching old behavior
)

REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "30"))
WORKER_ENABLED = os.getenv("WORKER_ENABLED", "true").lower() == "true"

# Coinbase-supported granularities (seconds)
GRANULARITY_MAP: Dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "1d": 86400,
    # "1w" is NOT directly supported by Coinbase; we'll log & skip it.
}

# ============================================================
# Parse BTC_INTERVALS
# ============================================================
def parse_intervals(raw: str) -> List[Tuple[str, int]]:
    """
    Parse BTC_INTERVALS env string like:
      "1m:400,5m:200,15m:60,1h:40,1d:20,1w:6"
    into: [("1m", 400), ("5m", 200), ...]
    """
    out: List[Tuple[str, int]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            logger.warning(f"Invalid BTC_INTERVALS entry (missing ':'): {part}")
            continue
        tf, cnt = part.split(":", 1)
        tf = tf.strip()
        try:
            count = int(cnt.strip())
        except ValueError:
            logger.warning(f"Invalid candle count in BTC_INTERVALS for {tf}: {cnt}")
            continue
        out.append((tf, count))
    if not out:
        raise RuntimeError("BTC_INTERVALS parsed to empty list; please fix env.")
    return out


INTERVAL_CONFIG: List[Tuple[str, int]] = parse_intervals(BTC_INTERVALS_RAW)
logger.info(f"Using BTC_INTERVALS: {INTERVAL_CONFIG}")

# ============================================================
# Global State
# ============================================================
# candles_state: { "5m": [ {ts, open, high, low, close, volume}, ... ], ... }
candles_state: Dict[str, List[Dict[str, Any]]] = {}
last_refresh_ts: Dict[str, int] = {}  # per-interval last refresh epoch ms

# ============================================================
# FastAPI App
# ============================================================
app = FastAPI(title="BTC Candle Service", version="2.0.0")

# ============================================================
# Helpers
# ============================================================
def iso8601_from_epoch(sec: int) -> str:
    return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()


def fetch_coinbase_candles(interval: str, count: int) -> List[Dict[str, Any]]:
    """
    Fetch candles from Coinbase for a given interval & count.
    We compute start/end based on 'count' * granularity seconds.
    """
    gran = GRANULARITY_MAP.get(interval)
    if gran is None:
        # Unsupported interval, e.g. 1w
        logger.warning(
            f"Interval '{interval}' not supported directly by Coinbase; skipping."
        )
        return []

    now_sec = int(time.time())
    window_sec = gran * count

    start_sec = now_sec - window_sec
    params = {
        "granularity": gran,
        "start": iso8601_from_epoch(start_sec),
        "end": iso8601_from_epoch(now_sec),
    }

    url = f"https://api.exchange.coinbase.com/products/{COINBASE_PRODUCT}/candles"
    try:
        r = requests.get(url, params=params, timeout=10)
    except Exception as e:
        logger.error(f"Error fetching {interval} candles from Coinbase: {e}")
        raise HTTPException(status_code=502, detail="Failed to reach Coinbase.")

    if r.status_code != 200:
        logger.error(
            f"Non-200 from Coinbase ({interval}): {r.status_code} {r.text}"
        )
        raise HTTPException(status_code=502, detail="Coinbase returned error.")

    # Coinbase format: [ [ time, low, high, open, close, volume ], ... ] (newest first)
    raw = r.json()
    if not isinstance(raw, list):
        logger.error(f"Unexpected Coinbase response format for {interval}: {raw}")
        raise HTTPException(status_code=502, detail="Bad format from Coinbase.")

    # Sort oldest→newest
    raw_sorted = sorted(raw, key=lambda x: x[0])

    candles: List[Dict[str, Any]] = []
    for row in raw_sorted:
        if not isinstance(row, list) or len(row) < 6:
            continue
        t, low, high, open_, close, vol = row
        candles.append(
            {
                "ts": int(t) * 1000,  # convert seconds → ms
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(vol),
            }
        )

    # Trim to 'count' just in case
    if len(candles) > count:
        candles = candles[-count:]

    return candles


def refresh_all_candles():
    global candles_state, last_refresh_ts

    symbol = COINBASE_PRODUCT
    summary_parts = []

    for interval, count in INTERVAL_CONFIG:
        try:
            c = fetch_coinbase_candles(interval, count)
        except HTTPException:
            # Already logged; keep old data if any
            continue
        except Exception as e:
            logger.exception(f"Unexpected error refreshing {interval}: {e}")
            continue

        candles_state[interval] = c
        last_refresh_ts[interval] = int(time.time() * 1000)
        summary_parts.append(f"{interval}({len(c)})")

    if summary_parts:
        logger.info(
            f"Refreshed candles for {symbol}: " + ", ".join(summary_parts)
        )
    else:
        logger.warning("No intervals refreshed this cycle.")


def worker_loop():
    if not WORKER_ENABLED:
        logger.info("Background worker disabled by WORKER_ENABLED env.")
        return

    logger.info("Worker loop started. Refreshing candles periodically...")
    while True:
        try:
            refresh_all_candles()
        except Exception as e:
            logger.exception(f"Error in refresh_all_candles: {e}")
        time.sleep(REFRESH_SECONDS)

# ============================================================
# FastAPI Events & Endpoints
# ============================================================
@app.on_event("startup")
def on_startup():
    logger.info("Starting BTC Candle Service (Coinbase)...")
    # Initial refresh so /candles is not empty
    try:
        refresh_all_candles()
    except Exception as e:
        logger.exception(f"Initial refresh error: {e}")

    if WORKER_ENABLED:
        t = threading.Thread(target=worker_loop, daemon=True)
        t.start()
        logger.info("Background worker loop enabled.")
    else:
        logger.info("Background worker loop not started (disabled).")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "product": COINBASE_PRODUCT,
        "intervals_config": INTERVAL_CONFIG,
        "last_refresh_ts": last_refresh_ts,
        "worker_enabled": WORKER_ENABLED,
        "refresh_seconds": REFRESH_SECONDS,
    }


@app.get("/candles")
def get_candles():
    """
    Return current candle buffer for all configured intervals.
    Shape is designed for Service 2 compatibility.
    """
    if not candles_state:
        raise HTTPException(status_code=503, detail="No candle data available yet.")

    intervals_out: List[Dict[str, Any]] = []
    for interval, _count in INTERVAL_CONFIG:
        candles = candles_state.get(interval, [])
        # If an interval is unsupported (e.g. 1w), it may be empty; that's ok.
        intervals_out.append(
            {
                "interval": interval,
                "candles": candles,
            }
        )

    return {
        "symbol": COINBASE_PRODUCT,
        "intervals": intervals_out,
    }
