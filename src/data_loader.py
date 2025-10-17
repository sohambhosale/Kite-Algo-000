"""
src/data_loader.py

Responsibilities:
- Fetch historical 1-minute OHLCV (via KiteConnect object) in safe batches with retry/backoff.
- Canonicalize and validate candle data.
- Persist cleaned data as parquet partitioned by symbol/date.
- Load concatenated data frames for a requested range.
- Maintain an in-memory rolling buffer of last-N candles per symbol and provide a
  thread-safe merge function to update the active minute candle with incoming ticks.
- Provide a unified get_inputs() interface for feature engineering & model layers.

Notes:
- Internal canonical time is UTC.
- Parquet files saved per-symbol per-day: <PROCESSED_DIR>/<SYMBOL>/<YYYY-MM-DD>.parquet
- Requires 'pyarrow' for parquet I/O.
"""

from __future__ import annotations
import os
import time
import json
import math
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

# ========================== Logger Setup ==========================
logger = logging.getLogger("data_loader")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(message)s"))
    logger.addHandler(ch)

# ========================== Configuration ==========================
PROJECT_ROOT = Path(os.getenv("EPAT_PROJECT_ROOT", r"C:\EPAT_Project"))
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "1min_stock_data"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
META_FILE = "_meta.json"

CANONICAL_COLS = [
    "timestamp", "open", "high", "low", "close", "volume",
    "vwap", "trades", "log_return", "source", "imputed"
]

_INMEM_CANDLES: Dict[str, pd.DataFrame] = {}
_INMEM_LOCKS: Dict[str, threading.Lock] = {}
_INMEM_MAX_LEN = 2000

KITE_FETCH_MAX_DAYS_PER_CALL = 5
KITE_RETRY_ATTEMPTS = 5
KITE_RETRY_BACKOFF = 2.0

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# ========================== Helper Functions ==========================

def _ensure_lock(symbol: str) -> threading.Lock:
    if symbol not in _INMEM_LOCKS:
        _INMEM_LOCKS[symbol] = threading.Lock()
    return _INMEM_LOCKS[symbol]


def _utcify(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _atomic_write_parquet(df: pd.DataFrame, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp.parquet")
    df.to_parquet(tmp, index=False)
    tmp.replace(dest)


def _symbol_folder(symbol: str) -> Path:
    return PROCESSED_DIR / symbol.upper()


def _meta_path(symbol: str) -> Path:
    return _symbol_folder(symbol) / META_FILE


def _read_meta(symbol: str) -> dict:
    p = _meta_path(symbol)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return {}
    return {}


def _write_meta(symbol: str, meta: dict) -> None:
    p = _meta_path(symbol)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(meta, default=str))

# ========================== Canonicalization ==========================

def canonicalize_df(df: pd.DataFrame, source: str = "kite") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=CANONICAL_COLS)

    df = df.copy()
    possible_ts = [c for c in df.columns if "time" in c.lower()]
    if "timestamp" not in df.columns and possible_ts:
        df = df.rename(columns={possible_ts[0]: "timestamp"})

    for col in ["open", "high", "low", "close", "volume"]:
        if col not in df.columns:
            if col.upper() in df.columns:
                df = df.rename(columns={col.upper(): col})
            else:
                df[col] = np.nan

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(float)

    if "vwap" not in df.columns:
        df["vwap"] = df[["open", "high", "low", "close"]].mean(axis=1)
    if "trades" not in df.columns:
        df["trades"] = np.nan

    df["imputed"] = (df["volume"] == 0) | df[["open", "high", "low", "close"]].isnull().any(axis=1)
    df["log_return"] = np.log(df["close"].replace(0, np.nan)).diff().fillna(0)
    df = df[CANONICAL_COLS]
    df["source"] = source
    return df.reset_index(drop=True)

# ========================== Parquet I/O ==========================

def _parquet_path_for(symbol: str, dt: datetime) -> Path:
    folder = _symbol_folder(symbol)
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{_date_str(dt)}.parquet"


def save_candle_df(symbol: str, df: pd.DataFrame, partition_by_day: bool = True) -> None:
    if df is None or df.empty:
        return
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if partition_by_day:
        df["date"] = df["timestamp"].dt.tz_convert(timezone.utc).dt.date
        for date, group in df.groupby("date"):
            _atomic_write_parquet(group.drop(columns=["date"]),
                                  _parquet_path_for(symbol, datetime.combine(date, datetime.min.time(), tzinfo=timezone.utc)))
    else:
        _atomic_write_parquet(df, _symbol_folder(symbol) / "all.parquet")

    meta = _read_meta(symbol)
    meta["last_saved"] = datetime.now(timezone.utc).isoformat()
    _write_meta(symbol, meta)
    logger.info(f"Saved candles for {symbol} (rows={len(df)})")


def list_available_dates(symbol: str) -> List[str]:
    folder = _symbol_folder(symbol)
    if not folder.exists():
        return []
    dates = []
    for p in folder.glob("*.parquet"):
        try:
            datetime.strptime(p.stem, "%Y-%m-%d")
            dates.append(p.stem)
        except Exception:
            continue
    return sorted(dates)


def load_candle_df_for_date(symbol: str, date: str) -> pd.DataFrame:
    p = _symbol_folder(symbol) / f"{date}.parquet"
    if not p.exists():
        return pd.DataFrame(columns=CANONICAL_COLS)
    df = pd.read_parquet(p)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df[CANONICAL_COLS]


def get_1min_df(symbol: str, start_dt: Optional[datetime] = None, end_dt: Optional[datetime] = None) -> pd.DataFrame:
    dates = list_available_dates(symbol)
    if not dates:
        return pd.DataFrame(columns=CANONICAL_COLS)

    if start_dt is None and end_dt is None:
        to_load = dates
    else:
        start_dt = _utcify(start_dt or datetime.min.replace(tzinfo=timezone.utc))
        end_dt = _utcify(end_dt or datetime.now(timezone.utc))
        to_load = [d for d in dates if start_dt.date() <= datetime.strptime(d, "%Y-%m-%d").date() <= end_dt.date()]

    dfs = [load_candle_df_for_date(symbol, d) for d in to_load]
    if not dfs:
        return pd.DataFrame(columns=CANONICAL_COLS)
    df = pd.concat(dfs, ignore_index=True).sort_values("timestamp")
    df = df[df["timestamp"].between(start_dt, end_dt)]
    return df.reset_index(drop=True)

# ========================== Kite Historical Fetch ==========================

def _retry_call(func, *args, attempts: int = KITE_RETRY_ATTEMPTS, backoff: float = KITE_RETRY_BACKOFF, **kwargs):
    for attempt in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{attempts} failed: {e}")
            time.sleep(backoff ** attempt)
    raise Exception("Max retry attempts reached")


def fetch_history_kite(kite, instrument_token: int, start_dt: datetime, end_dt: datetime,
                       interval: str = "minute") -> pd.DataFrame:
    start_dt = _utcify(start_dt)
    end_dt = _utcify(end_dt)
    if end_dt <= start_dt:
        return pd.DataFrame(columns=CANONICAL_COLS)

    out_frames = []
    cur = start_dt
    while cur < end_dt:
        chunk_end = min(end_dt, cur + timedelta(days=KITE_FETCH_MAX_DAYS_PER_CALL))
        start_iso, end_iso = cur.isoformat(), chunk_end.isoformat()

        def _call_hist():
            if hasattr(kite, "historical_data"):
                return kite.historical_data(instrument_token, start_iso, end_iso, interval)
            elif hasattr(kite, "historical"):
                return kite.historical(instrument_token, start_iso, end_iso, interval)
            raise AttributeError("kite object lacks historical_data method")

        payload = _retry_call(_call_hist)
        df = pd.DataFrame(payload)
        if not df.empty:
            if "date" in df.columns:
                df.rename(columns={"date": "timestamp"}, inplace=True)
            df = canonicalize_df(df)
            out_frames.append(df)
        cur = chunk_end + timedelta(seconds=1)

    if not out_frames:
        return pd.DataFrame(columns=CANONICAL_COLS)
    return pd.concat(out_frames, ignore_index=True).sort_values("timestamp")

# ========================== Backfill & Live Cache ==========================

def backfill_history(kite, token_map: Dict[str, int], start_dt: datetime, end_dt: datetime,
                     interval: str = "minute") -> Dict[str, List[Path]]:
    results = {}
    for symbol, token in token_map.items():
        try:
            logger.info(f"Backfilling {symbol} from {start_dt} to {end_dt}")
            df = fetch_history_kite(kite, token, start_dt, end_dt, interval)
            if not df.empty:
                save_candle_df(symbol, df)
                results[symbol] = [p for p in _symbol_folder(symbol).glob("*.parquet")]
            else:
                results[symbol] = []
        except Exception as e:
            logger.exception(f"Error backfilling {symbol}: {e}")
            results[symbol] = []
        time.sleep(0.5)
    return results


def load_last_n_candles(symbol: str, n: int = 500) -> pd.DataFrame:
    dates = list_available_dates(symbol)
    if not dates:
        return pd.DataFrame(columns=CANONICAL_COLS)
    dfs, count = [], 0
    for d in reversed(dates):
        df = load_candle_df_for_date(symbol, d)
        if df.empty:
            continue
        dfs.append(df)
        count += len(df)
        if count >= n:
            break
    if not dfs:
        return pd.DataFrame(columns=CANONICAL_COLS)
    return pd.concat(dfs, ignore_index=True).sort_values("timestamp").tail(n)


def init_live_buffer(symbol: str, n: int = 500) -> None:
    lock = _ensure_lock(symbol)
    with lock:
        if symbol not in _INMEM_CANDLES or _INMEM_CANDLES[symbol] is None:
            df = load_last_n_candles(symbol, n)
            if not df.empty:
                _INMEM_CANDLES[symbol] = df.set_index("timestamp")
    logger.info(f"Initialized live buffer for {symbol}")


def get_live_candles(symbol: str) -> pd.DataFrame:
    lock = _ensure_lock(symbol)
    with lock:
        df = _INMEM_CANDLES.get(symbol)
        if df is None:
            init_live_buffer(symbol)
            df = _INMEM_CANDLES.get(symbol)
        return df.copy() if df is not None else pd.DataFrame(columns=CANONICAL_COLS)

# ========================== Merge Tick (Live Update) ==========================

def merge_tick_to_latest_candle(symbol: str, tick: dict) -> pd.Series:
    ts_raw = tick.get("timestamp") or tick.get("time") or tick.get("datetime")
    ts = pd.to_datetime(ts_raw or datetime.now(timezone.utc), utc=True)
    bucket = ts.replace(second=0, microsecond=0)
    symbol = symbol.upper()

    lock = _ensure_lock(symbol)
    with lock:
        if symbol not in _INMEM_CANDLES or _INMEM_CANDLES[symbol] is None:
            init_live_buffer(symbol)
        df = _INMEM_CANDLES[symbol]

        last_price = float(tick.get("last_price") or tick.get("ltp") or tick.get("price") or 0.0)
        vol = float(tick.get("volume") or 0.0)

        if bucket in df.index:
            row = df.loc[bucket].copy()
            row["high"] = max(row["high"], last_price)
            row["low"] = min(row["low"], last_price)
            row["close"] = last_price
            row["volume"] += vol
            row["vwap"] = np.nanmean([row["open"], row["high"], row["low"], row["close"]])
            df.loc[bucket] = row
            _INMEM_CANDLES[symbol] = df
            return row
        else:
            open_p = df["close"].iloc[-1] if not df.empty else last_price
            new_row = {
                "timestamp": bucket, "open": open_p, "high": max(open_p, last_price),
                "low": min(open_p, last_price), "close": last_price, "volume": vol,
                "vwap": np.nanmean([open_p, last_price]), "trades": np.nan,
                "log_return": np.log(last_price) - np.log(open_p) if open_p > 0 else 0,
                "source": "live", "imputed": False,
            }
            new_df = pd.DataFrame([new_row]).set_index("timestamp")
            df = pd.concat([df, new_df]).sort_index().tail(_INMEM_MAX_LEN)
            _INMEM_CANDLES[symbol] = df
            return new_df.iloc[0]

# ========================== Unified Input Fetcher ==========================

def get_inputs(
    symbols: list[str],
    lookback: int = 500,
    include_live: bool = True,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
) -> dict[str, pd.DataFrame]:
    """
    Retrieve ready-to-use OHLCV inputs for a list of symbols.
    """
    results = {}
    for symbol in symbols:
        symbol = symbol.upper()
        try:
            if start_dt or end_dt:
                hist_df = get_1min_df(symbol, start_dt, end_dt)
            else:
                hist_df = load_last_n_candles(symbol, n=lookback)
            if include_live:
                live_df = get_live_candles(symbol)
                if not live_df.empty:
                    live_df = live_df.reset_index()
                    live_df["timestamp"] = pd.to_datetime(live_df["timestamp"], utc=True)
                    hist_df = pd.concat([hist_df, live_df], ignore_index=True)
                    hist_df = hist_df.drop_duplicates(subset=["timestamp"], keep="last")

            hist_df = canonicalize_df(hist_df)
            hist_df = hist_df.sort_values("timestamp").reset_index(drop=True)
            if lookback and len(hist_df) > lookback:
                hist_df = hist_df.tail(lookback)
            results[symbol] = hist_df

        except Exception as e:
            logger.exception(f"Error fetching inputs for {symbol}: {e}")
            results[symbol] = pd.DataFrame(columns=CANONICAL_COLS)
    return results
