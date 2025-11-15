import os
import time
import threading
import logging
from typing import Dict, Any, List

import requests
from fastapi import FastAPI, HTTPException

# ========== Logging Setup ==========
LOG_LEVEL = os.getenv("LOG_LEVEL", "info").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("candle-service")

LOG_REQUESTS = os.getenv("LOG_REQUESTS", "false").lower() == "true"
SAVE_LAST_RESPONSE = os.getenv("SAVE_LAST_RESPONSE", "false").lower() == "true"

# ========== Env & Config ==========
DATA_PROVIDER = os.getenv("DATA_PROVIDER", "binance")
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.binance.com")
API_TIMEOUT = float(os.getenv("API_TIMEOUT", "5"))

SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
SYMBOL_HUMAN = os.getenv("SYMBOL_HUMAN", "BTC-USD")

FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "30"))
ENABLE_WORKER_LOOP = os.getenv("ENABLE_WORKER_LOOP", "true").lower() == "true"


def load_intervals_from_env() -> List[Dict[str, Any]]:
    """
    Reads INTERVAL_1..4 and CANDLES_1..4 from env and returns a list of configs.
    Example:
      INTERVAL_1=5m, CANDLES_1=50
      INTERVAL_2=15m, CANDLES_2=25
      INTERVAL_3=1h, CANDLES_3=15
    """
    intervals = []
    for i in range(1, 5):
        interval = os.getenv(f"INTERVAL_{i}", "").strip()
        candles_str = os.getenv(f"CANDLES_{i}", "").strip()

        if not interval or not candles_str:
            continue

        try:
            limit = int(candles_str)
        except ValueError:
            logger.warning(f"Invalid CANDLES_{i} value: {candles_str}, skipping.")
            continue

        intervals.append({"interval": interval, "limit": limit})

    if not intervals:
        logger.warning("No intervals configured via INTERVAL_1..4 and CANDLES_1..4.")

    return intervals


INTERVAL_CONFIGS = load_intervals_from_env()

# Global in-memory candle cache
CANDLE_CACHE: Dict[str, Dict[str, Any]] = {}
LAST_FULL_RESPONSE: Dict[str, Any] = {}  # optional raw storage if enabled
LAST_FETCH_TIME: float = 0.0

app = FastAPI(title="BTC Candle Service", version="1.0.0")


# ========== Binance Fetch Logic ==========

def fetch_binance_klines(symbol: str, interval: str, limit: int) -> List[Dict[str, Any]]:
    """
    Fetch klines from Binance and normalize to:
      {
        "ts": <open time ms>,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float
      }
    """
    url = f"{API_BASE_URL}/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    if LOG_REQUESTS:
        logger.info(f"Requesting {url} with params={params}")

    try:
        resp = requests.get(url, params=params, timeout=API_TIMEOUT)
    except Exception as e:
        logger.error(f"Error calling Binance: {e}")
        raise

    if resp.status_code != 200:
        logger.error(f"Non-200 from Binance: {resp.status_code} {resp.text}")
        raise HTTPException(
            status_code=502,
            detail=f"Binance API error {resp.status_code}"
        )

    data = resp.json()

    if not isinstance(data, list):
        logger.error(f"Unexpected Binance response format: {data}")
        raise HTTPException(
            status_code=502,
            detail="Unexpected Binance response format"
        )

    normalized = []
    for item in data:
        # Binance kline format:
        # [
        #   0 open time (ms),
        #   1 open,
        #   2 high,
        #   3 low,
        #   4 close,
        #   5 volume,
        #   ...
        # ]
        try:
            open_time = int(item[0])
            open_price = float(item[1])
            high_price = float(item[2])
            low_price = float(item[3])
            close_price = float(item[4])
            volume = float(item[5])
        except (ValueError, TypeError, IndexError) as e:
            logger.warning(f"Failed to parse kline item: {item} ({e})")
            continue

        normalized.append({
            "ts": open_time,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
        })

    return normalized


def refresh_all_candles():
    """
    Fetches candles for all configured intervals and updates CANDLE_CACHE.
    """
    global CANDLE_CACHE, LAST_FULL_RESPONSE, LAST_FETCH_TIME

    if not INTERVAL_CONFIGS:
        logger.warning("No intervals configured; skipping refresh.")
        return

    intervals_payload = []

    for cfg in INTERVAL_CONFIGS:
        interval = cfg["interval"]
        limit = cfg["limit"]

        try:
            candles = fetch_binance_klines(SYMBOL, interval, limit)
        except HTTPException as he:
            logger.error(f"HTTPException fetching {interval}: {he.detail}")
            continue
        except Exception as e:
            logger.error(f"Error fetching {interval} candles: {e}")
            continue

        CANDLE_CACHE[interval] = {
            "interval": interval,
            "limit": limit,
            "candles": candles,
        }

        intervals_payload.append(CANDLE_CACHE[interval])

    server_time = int(time.time() * 1000)
    LAST_FETCH_TIME = time.time()

    full_response = {
        "symbol": SYMBOL,
        "provider": DATA_PROVIDER,
        "server_time": server_time,
        "intervals": intervals_payload,
    }

    if SAVE_LAST_RESPONSE:
        LAST_FULL_RESPONSE = full_response

    logger.info(
        f"Refreshed candles for {SYMBOL}: "
        + ", ".join([f"{c['interval']}({len(c['candles'])})" for c in intervals_payload])
    )


def worker_loop():
    """
    Background thread loop that refreshes candles every FETCH_INTERVAL_SECONDS.
    """
    logger.info(
        f"Worker loop started. Refreshing every {FETCH_INTERVAL_SECONDS} seconds."
    )
    while True:
        try:
            refresh_all_candles()
        except Exception as e:
            logger.exception(f"Unexpected error in worker loop: {e}")
        time.sleep(FETCH_INTERVAL_SECONDS)


# ========== FastAPI Events & Routes ==========

@app.on_event("startup")
def on_startup():
    logger.info("Starting BTC Candle Service...")

    # Initial fetch so /candles has data on first request
    try:
        refresh_all_candles()
    except Exception as e:
        logger.error(f"Initial candle fetch failed: {e}")

    if ENABLE_WORKER_LOOP:
        t = threading.Thread(target=worker_loop, daemon=True)
        t.start()
        logger.info("Background worker loop enabled.")
    else:
        logger.info("Background worker loop is disabled by env.")


@app.get("/health")
def health():
    """
    Health check endpoint.
    """
    return {
        "status": "ok",
        "provider": DATA_PROVIDER,
        "symbol": SYMBOL,
        "symbol_human": SYMBOL_HUMAN,
        "intervals_configured": [
            {"interval": c["interval"], "limit": c["limit"]} for c in INTERVAL_CONFIGS
        ],
        "last_fetch_time": LAST_FETCH_TIME,
    }


@app.get("/candles")
def get_candles():
    """
    Returns the latest cached candles for all configured intervals.
    If cache is empty, tries a synchronous refresh.
    """
    if not INTERVAL_CONFIGS:
        raise HTTPException(
            status_code=500,
            detail="No intervals configured. Check INTERVAL_1..4 and CANDLES_1..4.",
        )

    # Ensure we have data
    if not CANDLE_CACHE:
        logger.warning("CANDLE_CACHE empty on /candles request, attempting refresh...")
        try:
            refresh_all_candles()
        except Exception as e:
            logger.error(f"Failed to refresh candles on demand: {e}")
            raise HTTPException(
                status_code=502,
                detail="Failed to fetch candles from upstream provider.",
            )

    intervals_payload = []
    for cfg in INTERVAL_CONFIGS:
        interval = cfg["interval"]
        cached = CANDLE_CACHE.get(interval)

        if not cached:
            # if one interval is missing, treat as error
            logger.warning(f"No cache for interval {interval}, attempting partial refresh...")
            try:
                candles = fetch_binance_klines(SYMBOL, interval, cfg["limit"])
                cached = {
                    "interval": interval,
                    "limit": cfg["limit"],
                    "candles": candles,
                }
                CANDLE_CACHE[interval] = cached
            except Exception as e:
                logger.error(f"Failed partial refresh for {interval}: {e}")
                raise HTTPException(
                    status_code=502,
                    detail=f"Missing data for interval {interval} and failed to refetch.",
                )

        intervals_payload.append(cached)

    server_time = int(time.time() * 1000)

    return {
        "symbol": SYMBOL,
        "provider": DATA_PROVIDER,
        "server_time": server_time,
        "intervals": intervals_payload,
    }