import asyncio
import threading
import hashlib
import json
import math
import sqlite3
import struct
import traceback
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Dict, Any
from logging.handlers import RotatingFileHandler

import numpy as np
import aiohttp
from fastapi import FastAPI, WebSocket, Query, Response
from starlette.websockets import WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Usa orjson se disponibile, altrimenti json standard
try:
    import orjson as _json_lib
    def json_dumps(obj):
        try:
            return _json_lib.dumps(obj)
        except TypeError:
            return json.dumps(obj, default=str).encode('utf-8')
except ImportError:
    import json as _json_lib
    def json_dumps(obj):
        return json.dumps(obj, default=str).encode('utf-8')

import logging

# ============================================================
# Logging — RotatingFileHandler (5 × 10 MB), no unbounded growth
# ============================================================
_log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')

_file_handler = RotatingFileHandler(
    "market_aggregator.log",
    maxBytes=10 * 1024 * 1024,   # 10 MB per file
    backupCount=5,
    encoding='utf-8'                 # keep last 5 rotated files
)
_file_handler.setFormatter(_log_formatter)

_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_formatter)

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _stream_handler])
logger = logging.getLogger("aggregator")

import config
import data_fetcher
import data_processor
import bot_analyzer
import heatmap_engine

# Istanza globale del nuovo heatmap manager
heatmap_manager = heatmap_engine.HeatmapManager()

# ============================================================
# Database
# ============================================================
# FIX: ogni funzione di scrittura apre la sua connessione per evitare
#      "database is locked" quando le chiamate arrivano da asyncio.to_thread.
DB_PATH = "market_history.db"

def _open_db() -> sqlite3.Connection:
    """Apre una connessione SQLite configurata con WAL + NORMAL sync."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15.0)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA cache_size=-16000')   # 16 MB page cache
    conn.execute('PRAGMA temp_store=MEMORY')
    return conn


def init_db():
    conn = _open_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            ts REAL, p REAL, q REAL, buy INTEGER
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ts ON trades(ts)')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS candles (
            ts REAL PRIMARY KEY, o REAL, h REAL, l REAL, c REAL, v REAL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
            ts REAL PRIMARY KEY, data TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS heatmap_buckets (
            bucket_ts REAL,
            tf TEXT,
            data TEXT,
            PRIMARY KEY (bucket_ts, tf)
        )
    ''')
    # Retrocompatibilità — non più scritta dal nuovo engine
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS heatmap_points (
            ts REAL,
            p REAL,
            v REAL,
            tf TEXT,
            side TEXT,
            PRIMARY KEY (ts, p, tf, side)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_hm_tf_ts ON heatmap_points(tf, ts)')
    conn.commit()
    conn.close()

# Inizializza lo schema; usa poi connessioni separate per ogni scrittura
init_db()

# Connessione di SOLA LETTURA per query HTTP/API — protetta da lock
_read_conn = _open_db()
_read_lock = threading.Lock()


def save_trades_to_disk(trades_list):
    """Scrittura trade su DB — connessione dedicata per thread."""
    if not trades_list:
        return
    try:
        conn = _open_db()
        data = [(t['ts'], t['p'], t['q'], 1 if t['buy'] else 0) for t in trades_list]
        conn.executemany('INSERT INTO trades VALUES (?, ?, ?, ?)', data)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Errore salvataggio trade DB: %s", e)


def prune_old_trades_db(keep_days: int = 7):
    try:
        cutoff = datetime.now(timezone.utc).timestamp() - keep_days * 3600
        conn = _open_db()
        conn.execute('DELETE FROM trades WHERE ts < ?', (cutoff,))
        deleted = conn.execute('SELECT changes()').fetchone()[0]
        conn.commit()
        if deleted > 5000:
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        conn.close()
        if deleted > 0:
            logger.info("Pruning DB: rimossi %s trade più vecchi di %sgg", deleted, keep_days)
    except Exception as e:
        logger.error("Errore pruning trades DB: %s", e)


def save_candles_to_db(candles):
    if not candles:
        return
    try:
        conn = _open_db()
        data = [(c['ts'], c['o'], c['h'], c['l'], c['c'], c['v']) for c in candles]
        conn.executemany('INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?)', data)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Errore salvataggio candele: %s", e)


def save_orderbook_snapshot(ts: float, book: dict):
    try:
        payload = json.dumps(book, separators=(',', ':'))
        conn = _open_db()
        conn.execute('INSERT OR REPLACE INTO orderbook_snapshots VALUES (?, ?)', (ts, payload))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("Errore salvataggio snapshot OB: %s", e)


def prune_old_snapshots(hours: float):
    try:
        cutoff = datetime.now(timezone.utc).timestamp() - hours * 3600
        conn = _open_db()
        conn.execute('DELETE FROM orderbook_snapshots WHERE ts < ?', (cutoff,))
        conn.execute('DELETE FROM candles WHERE ts < ?', (cutoff - 86400,))
        conn.commit()
        # WAL checkpoint periodico per non lasciare crescere il WAL file
        conn.execute('PRAGMA wal_checkpoint(PASSIVE)')
        conn.close()
    except Exception as e:
        logger.error("Errore pruning snapshot: %s", e)


# ============================================================
# Stato applicazione
# FIX: clients_lock → asyncio.Lock per evitare deadlock event loop
# ============================================================

@dataclass
class AppState:
    trades_lock: threading.Lock       = field(default_factory=threading.Lock)
    candles_lock: threading.Lock      = field(default_factory=threading.Lock)
    book_lock: threading.Lock         = field(default_factory=threading.Lock)
    metrics_lock: threading.Lock      = field(default_factory=threading.Lock)
    # FIX: asyncio.Lock — non blocca l'event loop durante await send_text/send_bytes
    clients_lock: asyncio.Lock        = field(default_factory=asyncio.Lock)

    trades_history: List[dict]        = field(default_factory=list)
    candles: dict                     = field(default_factory=dict) # Ora è un dizionario { "15m": [...], "1h": [...] }
    delta_history: List[tuple]        = field(default_factory=list)
    book_history: List[dict]          = field(default_factory=list)
    trade_delta_history: List[tuple]  = field(default_factory=list)
    nvec_history: List[tuple]         = field(default_factory=list)
    bot_history: dict                 = field(default_factory=dict) # { "15m": [...], "1h": [...] }
    ob_power_history: List[tuple]     = field(default_factory=list)
    current_book: Dict[str, dict]     = field(default_factory=lambda: {"bids": {}, "asks": {}})
    last_price: float                 = 0.0
    absolute_baseline: float          = None
    current_tf: str                   = config.TIMEFRAME
    _last_nvec: float                 = 0.0

    show_ai_levels: bool              = True
    heatmap_static_mode: bool         = False
    spotlight_mode: bool              = True
    show_clusters: bool               = False
    show_line_chart: bool             = False
    show_nvec: bool                   = False
    show_moving_avg: bool             = True

    clusters_cache: dict              = field(default_factory=dict)
    bot_cache: dict                   = field(default_factory=dict)
    vp_db_cache: dict                 = field(default_factory=dict)
    vp_cache: dict                    = field(default_factory=dict)
    footprint_cache: dict             = field(default_factory=dict)
    vp_lookback: float                = 24.0

    last_json_hashes: dict            = field(default_factory=dict)
    last_hm_calc_time: float          = 0.0
    hm_force_broadcast: set           = field(default_factory=set)
    active_connections: dict          = field(default_factory=dict)

state = AppState()

# ============================================================
# Utility
# ============================================================

def downsample_timegrid(data, max_pts, t_min, t_max):
    if not data or t_max <= t_min:
        return []
    if len(data) <= max_pts:
        return data
    interval = (t_max - t_min) / (max_pts - 1) if max_pts > 1 else 0
    result = []
    data_idx = 0
    for i in range(max_pts):
        target_t = t_min + i * interval
        while data_idx < len(data) - 1 and abs(data[data_idx+1][0] - target_t) < abs(data[data_idx][0] - target_t):
            data_idx += 1
        if data_idx < len(data):
            result.append(data[data_idx])
    return result


def _append_quantized(history: list, ts: float, value: float, bucket_sec: float = 2.0):
    bucket_ts = round(ts / bucket_sec) * bucket_sec
    if history and abs(history[-1][0] - bucket_ts) < bucket_sec * 0.5:
        history[-1] = (bucket_ts, value)
    else:
        history.append((bucket_ts, value))


def _append_quantized_ms(history: list, ts_ms: float, value, bucket_ms: float = 2000.0):
    bucket_ts = round(ts_ms / bucket_ms) * bucket_ms
    if history and abs(history[-1][0] - bucket_ts) < bucket_ms * 0.5:
        if isinstance(value, list):
            history[-1] = [bucket_ts] + value
        else:
            history[-1] = (bucket_ts, value)
    else:
        if isinstance(value, list):
            history.append([bucket_ts] + value)
        else:
            history.append((bucket_ts, value))


def _calc_vp_ram_pure(trades_history: list, vp_db_cache: tuple, current_tf: str) -> list:
    step = config.ORDERBOOK_STEP_BY_TF.get(current_tf, 10.0)
    vp: dict = {}
    db_cache, cutoff_ts = vp_db_cache if vp_db_cache else ({}, 0.0)
    if db_cache:
        for p_bucket, vols in db_cache.items():
            vp[p_bucket] = {"b": float(vols["b"]), "s": float(vols["s"])}
    for t in trades_history:
        if t['ts'] < cutoff_ts:
            continue
        p_bucket = math.floor(t['p'] / step) * step
        if p_bucket not in vp:
            vp[p_bucket] = {"b": 0.0, "s": 0.0}
        if t['buy']:
            vp[p_bucket]["b"] += t['q']
        else:
            vp[p_bucket]["s"] += t['q']
    return [[p, round(v["b"], 4), round(v["s"], 4)] for p, v in sorted(vp.items())]


def calculate_nvec(ts_now=None):
    if not state.trades_history or not state.current_book or state.last_price <= 0:
        return state._last_nvec
    if ts_now is None:
        ts_now = datetime.now(timezone.utc).timestamp()

    lookback = 60.0
    weighted_delta = 0.0
    with state.trades_lock:
        trades_copy = list(state.trades_history)
    for t in reversed(trades_copy):
        age = ts_now - t['ts']
        if age > lookback:
            break
        w = math.exp(-age / 30.0)
        weighted_delta += (t["q"] if t["buy"] else -t["q"]) * w

    atr = 150.0
    with state.candles_lock:
        # 🟢 FIX: Estraiamo la lista dal dizionario usando il TF di default
        default_candles = state.candles.get(config.TIMEFRAME, [])
        if default_candles and len(default_candles) > 10:
            recent = default_candles[-14:]
            atr = sum(abs(c['h'] - c['l']) for c in recent) / len(recent)

    range_limit = max(atr * 0.5, 30.0)

    bids = state.current_book.get("bids", {})
    asks = state.current_book.get("asks", {})

    if weighted_delta > 0:
        opposite_liq = sum(v for p, v in asks.items() if 0 < (p - state.last_price) <= range_limit)
    else:
        opposite_liq = sum(v for p, v in bids.items() if 0 < (state.last_price - p) <= range_limit)

    denominator = opposite_liq if opposite_liq > 1.0 else (abs(weighted_delta) + 1.0)
    ratio = weighted_delta / denominator
    target = ratio * 100.0
    alpha = 0.15
    nvec = state._last_nvec + alpha * (target - state._last_nvec)
    nvec = max(-100.0, min(100.0, nvec))
    state._last_nvec = nvec
    return round(nvec, 2)


# ============================================================
# Calcoli CPU-bound (clusters)
# ============================================================

def _calc_clusters_pure(candles, trades_history, current_tf=None):
    buy_t_data = ([], [], [])
    sell_t_data = ([], [], [])
    if trades_history:
        max_age = 2 * 3600 if current_tf in ("1m","5m","15m") else 6 * 3600
        max_ts = max(t['ts'] for t in trades_history)
        cutoff = max_ts - max_age
        visible_trades = [t for t in trades_history if t['ts'] >= cutoff]
        if visible_trades:
            t_buckets_sec = {
                "1m": 4.0, "5m": 60.0, "15m": 180.0,
                "1h": 600.0, "4h": 1800.0, "1d": 3600.0
            }
            tf = current_tf or config.TIMEFRAME
            t_bucket = t_buckets_sec.get(tf, 60.0)
            p_bucket = 1.0
            current_min_delta = config.MIN_DELTA_BTC.get(tf, 10.0)
            clusters = {}
            for t in visible_trades:
                tb = math.floor(t['ts'] / t_bucket) * t_bucket
                pb = math.floor(t['p'] / p_bucket) * p_bucket
                key = (tb, pb)
                if key not in clusters:
                    clusters[key] = {"b": 0.0, "s": 0.0}
                if t['buy']:
                    clusters[key]["b"] += t['q']
                else:
                    clusters[key]["s"] += t['q']
            buy_ts, buy_p, buy_s = [], [], []
            sell_ts, sell_p, sell_s = [], [], []
            for (tb, pb), v in clusters.items():
                threshold = current_min_delta
                tot_vol = v["b"] + v["s"]
                if tot_vol <= 0: continue
                buy_ratio = v["b"] / tot_vol
                sell_ratio = v["s"] / tot_vol
                if v["b"] >= threshold and buy_ratio >= 0.6:
                    buy_ts.append(tb); buy_p.append(pb); buy_s.append(v["b"])
                elif v["s"] >= threshold and sell_ratio >= 0.6:
                    sell_ts.append(tb); sell_p.append(pb); sell_s.append(v["s"])
            return (buy_ts, buy_p, buy_s), (sell_ts, sell_p, sell_s)
    return ([], [], []), ([], [], [])


def _calc_clusters_db_pure(current_tf, oldest_ram_ts):
    if oldest_ram_ts is None:
        return ([], [], []), ([], [], [])

    t_buckets_sec = {
        "1m": 4.0, "5m": 60.0, "15m": 180.0,
        "1h": 600.0, "4h": 1800.0, "1d": 3600.0
    }
    tf = current_tf or config.TIMEFRAME
    t_bucket = t_buckets_sec.get(tf, 60.0)
    p_bucket = 1.0
    current_min_delta = config.MIN_DELTA_BTC.get(tf, 10.0)
    max_age = 2 * 3600 if tf in ("1m","5m","15m") else 6 * 3600
    cutoff = oldest_ram_ts - max_age

    try:
        conn = _open_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                CAST(ts / ? AS INTEGER) * ? as t_bucket,
                CAST(p / ? AS INTEGER) * ? as p_bucket,
                SUM(CASE WHEN buy = 1 THEN q ELSE 0 END) as vol_buy,
                SUM(CASE WHEN buy = 0 THEN q ELSE 0 END) as vol_sell
            FROM trades
            WHERE ts >= ? AND ts < ?
            GROUP BY t_bucket, p_bucket
        """, (t_bucket, t_bucket, p_bucket, p_bucket, cutoff, oldest_ram_ts))
        rows = cursor.fetchall()
        conn.close()
    except Exception as e:
        logger.error("Errore query cluster DB: %s", e)
        return ([], [], []), ([], [], [])

    clusters = {}
    for tb, pb, buy_vol, sell_vol in rows:
        key = (tb, pb)
        if key not in clusters:
            clusters[key] = {"b": 0.0, "s": 0.0}
        clusters[key]["b"] += buy_vol
        clusters[key]["s"] += sell_vol

    buy_ts, buy_p, buy_s = [], [], []
    sell_ts, sell_p, sell_s = [], [], []

    for (tb, pb), v in clusters.items():
        threshold = current_min_delta
        tot_vol = v["b"] + v["s"]
        if tot_vol <= 0:
            continue
        buy_ratio = v["b"] / tot_vol
        sell_ratio = v["s"] / tot_vol
        if v["b"] >= threshold and buy_ratio >= 0.6:
            buy_ts.append(tb); buy_p.append(pb); buy_s.append(v["b"])
        elif v["s"] >= threshold and sell_ratio >= 0.6:
            sell_ts.append(tb); sell_p.append(pb); sell_s.append(v["s"])

    return (buy_ts, buy_p, buy_s), (sell_ts, sell_p, sell_s)


def merge_clusters(a, b):
    if not a[0] and not b[0]:
        return ([], [], [])
    merged = {}
    for ts, p, s in zip(a[0], a[1], a[2]):
        key = (ts, p)
        merged[key] = merged.get(key, 0) + s
    for ts, p, s in zip(b[0], b[1], b[2]):
        key = (ts, p)
        merged[key] = merged.get(key, 0) + s
    ts_out, p_out, s_out = [], [], []
    for (t, p), s in merged.items():
        ts_out.append(t)
        p_out.append(p)
        s_out.append(s)
    return (ts_out, p_out, s_out)


def _calc_vp_db_pure(lookback_hours, step, max_ts_in_ram=None):
    cutoff_ts = datetime.now(timezone.utc).timestamp() - (lookback_hours * 3600)
    vp_db = {}
    try:
        conn = _open_db()
        cursor = conn.cursor()
        if max_ts_in_ram is not None and max_ts_in_ram > cutoff_ts:
            cursor.execute('''
                SELECT CAST(p / ? AS INTEGER) * ? as bucket,
                       SUM(CASE WHEN buy = 1 THEN q ELSE 0 END),
                       SUM(CASE WHEN buy = 0 THEN q ELSE 0 END)
                FROM trades WHERE ts >= ? AND ts < ?
                GROUP BY bucket
            ''', (step, step, cutoff_ts, max_ts_in_ram))
        else:
            cursor.execute('''
                SELECT CAST(p / ? AS INTEGER) * ? as bucket,
                       SUM(CASE WHEN buy = 1 THEN q ELSE 0 END),
                       SUM(CASE WHEN buy = 0 THEN q ELSE 0 END)
                FROM trades WHERE ts >= ?
                GROUP BY bucket
            ''', (step, step, cutoff_ts))
        for row in cursor.fetchall():
            vp_db[row[0]] = {"b": row[1], "s": row[2]}
        conn.close()
    except Exception as e:
        logger.error("Errore VP DB: %s", e)
    return vp_db, cutoff_ts


async def restore_state_from_db():
    """Carica trade, candele e heatmap buckets dal DB per accelerare l'avvio."""
    try:
        conn = _open_db()
        cursor = conn.cursor()
        cursor.execute('SELECT ts, o, h, l, c, v FROM candles ORDER BY ts DESC LIMIT ?', (config.CANDLE_LIMIT,))
        rows = cursor.fetchall()
        loaded_candles = [{"ts": row[0], "o": row[1], "h": row[2], "l": row[3], "c": row[4], "v": row[5]}
                          for row in reversed(rows)]
        if loaded_candles:
            with state.candles_lock:
                # Inseriamo le candele caricate dal DB nel TF di default
                state.candles[config.TIMEFRAME] = loaded_candles
                logger.info("Caricate %s candele dal DB", len(loaded_candles))

        cursor.execute('SELECT ts, p, q, buy FROM trades ORDER BY ts DESC LIMIT ?', (config.MAX_TRADES_HISTORY,))
        trade_rows = cursor.fetchall()
        loaded_trades = [{"ts": row[0], "p": row[1], "q": row[2], "buy": bool(row[3])}
                         for row in reversed(trade_rows)]
        if loaded_trades:
            with state.trades_lock:
                state.trades_history = loaded_trades
                logger.info("Caricati %s trade dal DB", len(loaded_trades))
        conn.close()

        # Caricamento PROGRESSIVO della heatmap in background per non bloccare il server
        async def _load_hm_bg():
            for tf in config.HEATMAP_TF_ACTIVE:
                # Carica 12 ore (coerente con config.HEATMAP_DB_KEEP_HOURS) invece di 24
                loaded = await asyncio.to_thread(heatmap_manager.load_from_db, tf, hours=12)
                if loaded:
                    logger.info("Caricati %s bucket heatmap %s dal DB", len(loaded), tf)
                
                # Pausa vitale di 1.5 secondi tra un timeframe e l'altro.
                # Permette al server di respirare e rispondere ai "ping" dei WebSocket!
                await asyncio.sleep(1.5)

        # Lancia il caricamento sganciandolo dal thread principale
        asyncio.create_task(_load_hm_bg())

    except Exception as e:
        logger.error("Errore ripristino: %s", e)


# ============================================================
# Motore di background
# FIX: book_history cappato a BOOK_HISTORY_MAX (300) invece di 50,000
# ============================================================

async def background_engine():
    logger.info("Motore di Background avviato")
    await restore_state_from_db()

    update_tick = 0
    async with aiohttp.ClientSession() as http_session:
        while True:
            try:
                update_tick += 1
                is_heavy_tick = (update_tick % 2 == 0)
                current_ts = datetime.now(timezone.utc).timestamp()

                # 1. Capiamo SUBITO quali TF ci servono
                active_tfs = set(prefs.get("tf", config.TIMEFRAME) for prefs in state.active_connections.values())
                if not active_tfs:
                    active_tfs = {config.TIMEFRAME}

                # 2. Passiamo active_tfs al data_fetcher
                raw = await data_fetcher.fetch_all_async(http_session, fetch_klines=is_heavy_tick, active_tfs=active_tfs)

                price = 0.0
                current_book = None
                new_trades = []

                if raw["aggregated"]:
                    new_trades = data_processor.process_trades(raw["aggregated"])
                    current_book = data_processor.process_book(raw["aggregated"], state.current_tf)
                    
                    # FIX: Passiamo le candele del TF di default al processore del prezzo
                    default_candles = state.candles.get(config.TIMEFRAME) if state.candles else None
                    price = data_processor.extract_live_price(raw["aggregated"], default_candles)

                # 3. Candele (Ora gestiamo un dizionario)
                if raw["candles"]:
                    with state.candles_lock:
                        for tf, c_data in raw["candles"].items():
                            state.candles[tf] = data_processor.process_candles(c_data)

                # Trades
                if new_trades:
                    unique_new_trades = []
                    with state.trades_lock:
                        existing_ids = {(t['ts'], t['p'], t['q']) for t in state.trades_history[-1500:]}
                        for nt in new_trades:
                            trade_sig = (nt['ts'], nt['p'], nt['q'])
                            if trade_sig not in existing_ids:
                                state.trades_history.append(nt)
                                existing_ids.add(trade_sig)
                                unique_new_trades.append(nt)
                        if len(state.trades_history) > config.MAX_TRADES_HISTORY:
                            overflow = len(state.trades_history) - config.MAX_TRADES_HISTORY
                            state.trades_history = state.trades_history[overflow:]

                    if unique_new_trades:
                        await asyncio.to_thread(save_trades_to_disk, unique_new_trades)

                # Orderbook + heatmap bucket-based
                if current_book:
                    with state.book_lock:
                        state.current_book = current_book
                        if price > 0:
                            state.last_price = price
                        else:
                            with state.candles_lock:
                                default_candles = state.candles.get(config.TIMEFRAME, [])
                                if default_candles:
                                    try: state.last_price = float(default_candles[-1]['c'])
                                    except: pass

                        state.book_history.append({
                            "ts": current_ts,
                            "bids": current_book.get("bids", {}),
                            "asks": current_book.get("asks", {})
                        })
                        # FIX: book_history cappato a 300 — bot_analyzer usa max 180 entries.
                        # In precedenza il cap era 50,000 (per il TF "1d"), causando 1-2 GB RAM.
                        if len(state.book_history) > config.BOOK_HISTORY_MAX:
                            state.book_history = state.book_history[-config.BOOK_HISTORY_MAX:]

                        # Metriche derivate
                        with state.metrics_lock:
                            tot_bids = sum(current_book.get("bids", {}).values())
                            tot_asks = sum(current_book.get("asks", {}).values())
                            curr_delta = tot_bids - tot_asks
                            if state.absolute_baseline is None:
                                state.absolute_baseline = curr_delta
                            _append_quantized(state.delta_history, current_ts, curr_delta)

                            # trade delta (ultimi 60 secondi)
                            with state.trades_lock:
                                lookback_ts = current_ts - 60.0
                                td_val = 0.0
                                for t in reversed(state.trades_history):
                                    if t['ts'] < lookback_ts: break
                                    td_val += t['q'] if t['buy'] else -t['q']
                            _append_quantized(state.trade_delta_history, current_ts, td_val)

                            # NVEC
                            nvec_now = calculate_nvec(current_ts)
                            _append_quantized(state.nvec_history, current_ts, nvec_now)

                            # OB power ±50$
                            range_limit = 50.0
                            local_bids = sum(v for p, v in current_book.get("bids", {}).items()
                                             if (state.last_price - p) <= range_limit and p <= state.last_price)
                            local_asks = sum(v for p, v in current_book.get("asks", {}).items()
                                             if (p - state.last_price) <= range_limit and p >= state.last_price)
                            ob_power = local_bids - local_asks
                            _append_quantized(state.ob_power_history, current_ts, ob_power)

                            # Pruning unico: 25 minuti di storia per tutte le serie
                            _cutoff = current_ts - 25 * 60
                            state.delta_history        = [x for x in state.delta_history        if x[0] >= _cutoff]
                            state.trade_delta_history  = [x for x in state.trade_delta_history  if x[0] >= _cutoff]
                            state.nvec_history         = [x for x in state.nvec_history         if x[0] >= _cutoff]
                            state.ob_power_history     = [x for x in state.ob_power_history     if x[0] >= _cutoff]

                # Accumulo heatmap bucket-based per TUTTI i TF attivi
                if current_book and not state.heatmap_static_mode:
                    for tf in config.HEATMAP_TF_ACTIVE:
                        heatmap_manager.update_from_book_snapshot(
                            tf, current_ts,
                            current_book.get("bids", {}),
                            current_book.get("asks", {})
                        )

                if raw["candles"] is not None and is_heavy_tick:
                    with state.candles_lock:
                        # Salviamo sul DB solo il TF di default per non violare la Primary Key
                        default_candles = state.candles.get(config.TIMEFRAME)
                        if default_candles:
                            await asyncio.to_thread(save_candles_to_db, default_candles.copy())

                if current_book and update_tick % config.OB_SNAPSHOT_INTERVAL_TICKS == 0:
                    await asyncio.to_thread(save_orderbook_snapshot, current_ts, current_book)

                # Pruning DB periodico (~ogni 75 secondi a 4 Hz)
                if update_tick % 300 == 0:
                    await asyncio.to_thread(prune_old_snapshots, config.MAX_OB_SNAPSHOT_AGE_HOURS)
                    await asyncio.to_thread(heatmap_manager.prune_db, config.HEATMAP_DB_KEEP_HOURS)

                # Salva heatmap buckets su DB ogni ~15 secondi (4 Hz → ogni 60 tick)
                if update_tick % 60 == 0:
                    for tf in config.HEATMAP_TF_ACTIVE:
                        await asyncio.to_thread(heatmap_manager.save_to_db, tf)
                        await asyncio.to_thread(
                            heatmap_manager.prune_memory, tf,
                            config.HEATMAP_PRUNE_MEMORY_HOURS,
                            config.HEATMAP_MAX_BUCKETS_MEMORY
                        )

          
                # Calcoli pesanti (solo tick pari)
                if is_heavy_tick:
                    with state.book_lock:
                        book_hist_snap = list(state.book_history)
                    with state.trades_lock:
                        trades_snap = list(state.trades_history)
                    with state.metrics_lock:
                        td_hist_snap = list(state.trade_delta_history)

                    show_ai = state.show_ai_levels

                    # 1. Quali TF stanno guardando gli utenti in questo momento?
                    active_tfs = set(prefs.get("tf", config.TIMEFRAME) for prefs in state.active_connections.values())
                    if not active_tfs:
                        active_tfs = {config.TIMEFRAME} # Default di sicurezza

                    # 2. Calcola la matematica PER OGNI timeframe richiesto
                    for cur_tf in active_tfs:

                        # Estraiamo le candele specifiche per QUESTO timeframe
                        with state.candles_lock:
                            candles_snap = list(state.candles.get(cur_tf, []))

                        if cur_tf in config.ORDERBOOK_FILTER_TFS:
                            clean_bh = [{"ts": s["ts"],
                                         "bids": {p: v for p, v in s.get("bids", {}).items() if v <= config.ORDERBOOK_BIG_ORDER_THRESHOLD},
                                         "asks": {p: v for p, v in s.get("asks", {}).items() if v <= config.ORDERBOOK_BIG_ORDER_THRESHOLD}}
                                        for s in book_hist_snap]
                        else:
                            clean_bh = book_hist_snap

                        bot_calc = await asyncio.to_thread(bot_analyzer.get_live_probability, clean_bh, trades_snap, state.last_price, show_ai, "LAB", 0.001, td_hist_snap, candles_snap)
                        clusters_res = await asyncio.to_thread(_calc_clusters_pure, candles_snap, trades_snap, cur_tf)
                        oldest_ram_ts = trades_snap[0]['ts'] if trades_snap else None
                        db_clusters = await asyncio.to_thread(_calc_clusters_db_pure, cur_tf, oldest_ram_ts)
                        buy_db, sell_db = db_clusters
                        buy_ram, sell_ram = clusters_res
                        clusters_res = (merge_clusters(buy_ram, buy_db), merge_clusters(sell_ram, sell_db))

                        v_step = config.ORDERBOOK_STEP_BY_TF.get(cur_tf, 10.0)
                        ram_duration_hours = (current_ts - trades_snap[0]['ts']) / 3600 if trades_snap else 0
                        use_db = state.vp_lookback > ram_duration_hours
                        if use_db: vp_db_res = await asyncio.to_thread(_calc_vp_db_pure, state.vp_lookback, v_step, oldest_ram_ts)
                        else: vp_db_res = ({}, current_ts - state.vp_lookback * 3600)
                        vp_ram_res = await asyncio.to_thread(_calc_vp_ram_pure, trades_snap, vp_db_res, cur_tf)

                        fp_data = {}
                        if trades_snap:
                            tf_to_sec = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
                            candle_sec = tf_to_sec.get(cur_tf, 300)
                            fp_step = config.ORDERBOOK_STEP_BY_TF.get(cur_tf, 10.0)
                            for t in trades_snap:
                                c_ts = int(math.floor(t['ts'] / candle_sec) * candle_sec)
                                p_bucket = math.floor(t['p'] / fp_step) * fp_step
                                if c_ts not in fp_data: fp_data[c_ts] = {}
                                if p_bucket not in fp_data[c_ts]: fp_data[c_ts][p_bucket] = [0.0, 0.0]
                                if t['buy']: fp_data[c_ts][p_bucket][0] += t['q']
                                else: fp_data[c_ts][p_bucket][1] += t['q']
                        clean_fp = {ts: [[p, round(v[0],2), round(v[1],2)] for p, v in p_dict.items()] for ts, p_dict in fp_data.items()}

                        # 3. Salva i risultati NELLA CHIAVE DEL TF
                        with state.metrics_lock:
                            state.clusters_cache[cur_tf] = clusters_res
                            state.bot_cache[cur_tf] = bot_calc
                            state.vp_db_cache[cur_tf] = vp_db_res
                            state.vp_cache[cur_tf] = vp_ram_res
                            state.footprint_cache[cur_tf] = clean_fp
                            
                            direction = bot_calc.get("direction", "NEUTRALE")
                            prob_val = bot_calc.get("prob", 50.0)
                            raw_prob = (prob_val if "LONG" in direction else (100 - prob_val if "SHORT" in direction else 50.0))
                            
                            if cur_tf not in state.bot_history: state.bot_history[cur_tf] = []
                            _append_quantized_ms(state.bot_history[cur_tf], current_ts * 1000, [raw_prob, direction, bot_calc.get("target", 0), bot_calc.get("support", 0), 0.0])
                            _cutoff_ms = (current_ts - 45 * 60) * 1000
                            state.bot_history[cur_tf] = [x for x in state.bot_history[cur_tf] if x[0] >= _cutoff_ms]

                await broadcast_update()
                await asyncio.sleep(0.25)

            except Exception as e:
                logger.exception("Engine Loop Error")
                await asyncio.sleep(1)


# ============================================================
# Broadcast
# FIX: async with state.clients_lock (asyncio.Lock) invece di with (threading.Lock)
#      per non bloccare l'event loop durante await send_text / send_bytes
# ============================================================

_last_broadcast_hm_map = {}
_broadcast_tick_counter = 0

_HEATMAP_BROADCAST_INTERVAL_BY_TF = {
    "1m": 1, "5m": 4, "15m": 8, "1h": 20, "4h": 40, "1d": 120
}


async def _build_payload_for_tf(tf):
    """Costruisce il payload JSON e l'heatmap binaria per un timeframe specifico."""
    price = state.last_price
    if price <= 0:
        with state.candles_lock:
            tf_candles = state.candles.get(tf, [])
            if tf_candles:
                try: price = float(tf_candles[-1]['c'])
                except: pass

    with state.book_lock:
        book = state.current_book
    with state.metrics_lock:
        bot_calc = dict(state.bot_cache.get(tf, {}))
        ob_power_last = round(state.ob_power_history[-1][1], 2) if state.ob_power_history else 0
        nvec_val = state.nvec_history[-1][1] if state.nvec_history else state._last_nvec

        HIST_WINDOW_SEC_DELTA = 20 * 60
        HIST_PTS: int = 600
        _cutoff_delta = datetime.now(timezone.utc).timestamp() - HIST_WINDOW_SEC_DELTA
        _cutoff_ms_delta = _cutoff_delta * 1000

        recent_delta   = [x for x in state.delta_history       if x[0] >= _cutoff_delta]
        recent_td      = [x for x in state.trade_delta_history  if x[0] >= _cutoff_delta]
        recent_nvec    = [x for x in state.nvec_history         if x[0] >= _cutoff_delta]
        recent_ob_pwr  = [x for x in state.ob_power_history     if x[0] >= _cutoff_delta]
        recent_bot     = [x for x in state.bot_history.get(tf, []) if x[0] >= _cutoff_ms_delta]

        delta_hist   = [[int(x[0]*1000), round(x[1],2)] for x in recent_delta]   if recent_delta   else []
        td_hist      = [[int(x[0]*1000), round(x[1],2)] for x in recent_td]      if recent_td      else []
        nvec_hist    = [[int(x[0]*1000), round(x[1],2)] for x in recent_nvec]    if recent_nvec    else []
        ob_pwr_hist  = [[int(x[0]*1000), round(x[1],2)] for x in recent_ob_pwr]  if recent_ob_pwr  else []
        bot_hist     = [[int(x[0]), round(x[1],2)] for x in recent_bot[-HIST_PTS:]] if recent_bot  else []

    with state.candles_lock:
        tf_candles = state.candles.get(tf, [])
        candles_payload = [dict(c) for c in tf_candles[-config.CANDLE_LIMIT:]] if tf_candles else []

    # Orderbook filtrato
    raw_bids = book.get("bids", {})
    raw_asks = book.get("asks", {})
    if tf in config.ORDERBOOK_FILTER_TFS:
        filtered_bids = {p: v for p, v in raw_bids.items() if v <= config.ORDERBOOK_BIG_ORDER_THRESHOLD}
        filtered_asks = {p: v for p, v in raw_asks.items() if v <= config.ORDERBOOK_BIG_ORDER_THRESHOLD}
    else:
        filtered_bids = raw_bids
        filtered_asks = raw_asks

    orderbook_payload = {
        "bids": sorted([[p, round(v,4)] for p,v in filtered_bids.items()], reverse=True)[:10000],
        "asks": sorted([[p, round(v,4)] for p,v in filtered_asks.items()])[:10000]
    }
    spread = (round(min(raw_asks.keys(), default=0) - max(raw_bids.keys(), default=0), 2)
              if raw_asks and raw_bids else 0)

    vp_payload = list(state.vp_cache.get(tf, []))

    # MA
    ma_data = []
    if state.show_moving_avg:
        with state.candles_lock:
            tf_candles = state.candles.get(tf, [])
            if len(tf_candles) >= 50:
                closes = np.array([c['c'] for c in tf_candles])
                weights = np.ones(50)/50
                sma = np.convolve(closes, weights, mode='valid')
                times = [c['ts'] for c in tf_candles[49:]]
                ma_data = [[int(t*1000), round(c,2)] for t,c in zip(times, sma)]

    # Dense bull / bear
    dense_bull, dense_bear = 0, 0
    with state.metrics_lock:
        # 🟢 FIX: Dobbiamo estrarre la lista specifica del timeframe richiesto
        bot_hist_tf = state.bot_history.get(tf, []) 
        
        if len(bot_hist_tf) > 10:
            b_size = 10.0
            bull_scores = defaultdict(float)
            bear_scores = defaultdict(float)
            now_ts = datetime.now(timezone.utc).timestamp()
            
            # 🟢 FIX: Iteriamo su bot_hist_tf, non sul dizionario globale
            recent = [x for x in bot_hist_tf if (now_ts - (x[0]/1000)) <= 3600]
            total = len(recent)
            if total > 0:
                for i, x in enumerate(recent):
                    w = (i/total)**3
                    if len(x) >= 6:
                        if x[3] > 0: bull_scores[round(x[3]/b_size)*b_size] += w
                        if x[4] > 0: bear_scores[round(x[4]/b_size)*b_size] += w
                if bull_scores: dense_bull = max(bull_scores, key=bull_scores.get)
                if bear_scores: dense_bear = max(bear_scores, key=bear_scores.get)

    # Cluster
    clusters_buy, clusters_sell = [], []
    if state.clusters_cache.get(tf):
        buy_c, sell_c = state.clusters_cache[tf]
        if buy_c[0]:
            clusters_buy = [[int(t*1000), p, round(v,4)] for t,p,v in zip(buy_c[0], buy_c[1], buy_c[2])]
        if sell_c[0]:
            clusters_sell = [[int(t*1000), p, round(v,4)] for t,p,v in zip(sell_c[0], sell_c[1], sell_c[2])]

    orderbook_step = config.ORDERBOOK_STEP_BY_TF.get(tf, 10)
    bucket_duration = config.HEATMAP_BUCKET_DURATION_BY_TF.get(tf, 300)
    hm_max_vol = {g: round(heatmap_manager.store.get_max_volume_by_group(g), 2)
                  for g in config.HEATMAP_COLOR_GROUP_NAMES}

    payload = {
        "type": "update",
        "price": price,
        "candles": candles_payload,
        "orderbook_step": orderbook_step,
        "volume_profile": vp_payload,
        "nvec": nvec_val,
        "nvec_history": nvec_hist,
        "delta_history": delta_hist,
        "trade_delta_history": td_hist,
        "ob_power": ob_power_last,
        "ob_power_history": ob_pwr_hist,
        "orderbook": orderbook_payload,
        "footprint": state.footprint_cache.get(tf, {}),
        "clusters": {"buy": clusters_buy, "sell": clusters_sell},
        "bot": bot_calc,
        "bot_history": bot_hist,
        "ma": ma_data,
        "show_ai_levels": state.show_ai_levels,
        "show_clusters": state.show_clusters,
        "show_nvec": state.show_nvec,
        "show_moving_avg": state.show_moving_avg,
        "show_line_chart": state.show_line_chart,
        "current_tf": tf,
        "spread": spread,
        "dense_bull": round(dense_bull,2) if dense_bull else 0,
        "dense_bear": round(dense_bear,2) if dense_bear else 0,
        "heatmap_bucket_duration": bucket_duration,
        "heatmap_max_vol": hm_max_vol,
        "vp_step": orderbook_step,
        "colors": {
            "candle_up": config.COLOR_CANDLE_UP,
            "candle_down": config.COLOR_CANDLE_DOWN,
            "cluster_buy": config.COLOR_TRADE_BUY,
            "cluster_sell": config.COLOR_TRADE_SELL
        },
        "heatmap_colors": {
            "min_volume": config.HEATMAP_MIN_VOLUME_THRESHOLD,
            "vol_low": config.HEATMAP_VOLUME_LOW,
            "vol_medium": config.HEATMAP_VOLUME_MEDIUM,
            "vol_high": config.HEATMAP_VOLUME_HIGH,
            "cold_start": config.HEATMAP_COLOR_COLD_START,
            "cold_mid": config.HEATMAP_COLOR_COLD_MID,
            "cold_end": config.HEATMAP_COLOR_COLD_END,
            "hot_start": config.HEATMAP_COLOR_HOT_START,
            "hot_mid": config.HEATMAP_COLOR_HOT_MID,
            "hot_end": config.HEATMAP_COLOR_HOT_END
        },
    }

    json_bytes = json_dumps(payload)
    json_hash = hashlib.md5(json_bytes).hexdigest()
    json_payload = json_bytes.decode('utf-8') if isinstance(json_bytes, bytes) else json_bytes

    # Heatmap binaria differenziale
    hm_binary = None
    force = tf in state.hm_force_broadcast
    interval = _HEATMAP_BROADCAST_INTERVAL_BY_TF.get(tf, 4)

    if force:
        state.hm_force_broadcast.discard(tf)
        hm_binary = heatmap_manager.build_binary_full_sync(tf)
    elif _broadcast_tick_counter % interval == 0:
        hm_binary = heatmap_manager.build_binary_updates(tf)

    return json_payload, json_hash, hm_binary


async def broadcast_update():
    global _broadcast_tick_counter
    _broadcast_tick_counter += 1

    if not state.active_connections:
        return

    # 1. Raccogliamo quali timeframe stanno guardando gli utenti
    active_tfs = set(prefs.get("tf", config.TIMEFRAME) for prefs in state.active_connections.values())

    payloads = {}
    hashes = {}
    hm_binaries = {}

    # 2. Pre-calcoliamo i dati SOLO per i timeframe richiesti
    for tf in active_tfs:
        json_payload, json_hash, hm_binary = await _build_payload_for_tf(tf)
        payloads[tf] = json_payload
        hashes[tf] = json_hash
        hm_binaries[tf] = hm_binary

    # 3. Spediamo a ciascun utente il pacchetto del suo TF
    async with state.clients_lock:
        disconnected = []
        for ws, prefs in list(state.active_connections.items()):
            client_tf = prefs.get("tf", config.TIMEFRAME)
            try:
                if hashes.get(client_tf) != state.last_json_hashes.get(client_tf):
                    await ws.send_text(payloads.get(client_tf))
                    await asyncio.sleep(0)
                if hm_binaries.get(client_tf):
                    await ws.send_bytes(hm_binaries.get(client_tf))
            except (RuntimeError, ConnectionResetError, BrokenPipeError):
                disconnected.append(ws)
            except Exception:
                disconnected.append(ws)

        for ws in disconnected:
            if ws in state.active_connections:
                del state.active_connections[ws]

        # Aggiorniamo gli hash per i TF trasmessi
        for tf in active_tfs:
            state.last_json_hashes[tf] = hashes.get(tf)



# ============================================================
# FastAPI app
# ============================================================

@asynccontextmanager
async def lifespan(_app: FastAPI):
    await data_fetcher.ws_manager.start()
    task = asyncio.create_task(background_engine())
    logger.info("Aggregatore avviato — versione produzione 24h")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logger.info("Aggregatore fermato.")

app = FastAPI(lifespan=lifespan)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def get():
    return FileResponse("static/index.html")

@app.get("/health")
async def health():
    """Health-check endpoint per il monitoraggio (systemd, uptime robot, ecc.)."""
    return {
        "status": "ok",
        "price": state.last_price,
        "clients": len(state.active_connections),
        "trades_in_ram": len(state.trades_history),
        "book_history_len": len(state.book_history),
        "current_tf": state.current_tf,
        "uptime_ts": datetime.now(timezone.utc).isoformat(),
    }

import os
import glob
from fastapi.responses import FileResponse

@app.get("/api/datasets")
async def list_datasets():
    """Elenca tutti i file CSV di Machine Learning disponibili sul server."""
    try:
        # Cerca tutti i file che iniziano con ml_data_ e finiscono con .csv
        files = glob.glob("ml_data_*.csv")
        # Ordina per data (dal più recente al più vecchio)
        files.sort(key=os.path.getmtime, reverse=True)
        
        # Aggiungiamo anche le dimensioni del file in MB per comodità
        file_info = []
        for f in files:
            size_mb = os.path.getsize(f) / (1024 * 1024)
            file_info.append({"filename": f, "size_mb": round(size_mb, 2)})
            
        return {"status": "ok", "datasets": file_info}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/download_dataset/{filename}")
async def download_dataset(filename: str):
    """Permette il download diretto del file CSV."""
    # Sicurezza: controlla che il file esista e sia davvero un nostro CSV
    if os.path.exists(filename) and filename.startswith("ml_data_") and filename.endswith(".csv"):
        return FileResponse(
            path=filename, 
            filename=filename, 
            media_type='text/csv'
        )
    return {"status": "error", "message": "File non trovato o accesso negato."}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    async with state.clients_lock:
        state.active_connections[websocket] = {"tf": config.TIMEFRAME}
    logger.info("Client WS connesso. Totale: %s", len(state.active_connections))

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action", "")
            if action == "ping":
                try:
                    await websocket.send_json({"type": "pong"})
                except Exception:
                    pass
                continue

            if action == "change_tf":
                new_tf = data.get("tf", config.TIMEFRAME)
                # FIX: Salviamo il TF SOLO per questo utente. Non tocchiamo config.TIMEFRAME!
                async with state.clients_lock:
                    if websocket in state.active_connections:
                        state.active_connections[websocket]["tf"] = new_tf
                state.hm_force_broadcast.add(new_tf)
                state.last_hm_calc_time = 0.0
            elif action == "toggle_ai":
                state.show_ai_levels = not state.show_ai_levels
            elif action == "toggle_heatmap":
                state.heatmap_static_mode = not state.heatmap_static_mode
            elif action == "toggle_spotlight":
                state.spotlight_mode = not state.spotlight_mode
            elif action == "toggle_clusters":
                state.show_clusters = not state.show_clusters
            elif action == "toggle_nvec":
                state.show_nvec = not state.show_nvec
            elif action == "toggle_ma":
                state.show_moving_avg = not state.show_moving_avg
            elif action == "toggle_line":
                state.show_line_chart = not state.show_line_chart
            elif action == "change_vp_lookback":
                state.vp_lookback = data.get("hours", 24)

    except WebSocketDisconnect as e:
        if e.code in (1000, 1001):
            logger.debug("Client WS disconnesso (code=%s).", e.code)
        else:
            logger.warning("Client WS disconnesso anomalo (code=%s, reason=%s).", e.code, e.reason)
    except Exception as e:
        logger.warning("Client WS errore: %s: %s", type(e).__name__, e)
    finally:
        async with state.clients_lock:
            if websocket in state.active_connections:
                del state.active_connections[websocket]
        logger.info("Client WS disconnesso. Rimasti: %s", len(state.active_connections))


@app.get("/api/replay")
async def replay_endpoint(
    start: float = Query(...),
    end: float = Query(...),
    tf: str = Query("5m", pattern="^(1m|5m|15m|1h|4h|1d)$")
):
    try:
        conn = _open_db()
        cursor = conn.cursor()
        cursor.execute('SELECT ts, p, q, buy FROM trades WHERE ts BETWEEN ? AND ? ORDER BY ts ASC', (start, end))
        trades_data = [{"ts": row[0], "p": row[1], "q": row[2], "buy": bool(row[3])} for row in cursor.fetchall()]
        cursor.execute('SELECT ts, o, h, l, c, v FROM candles WHERE ts BETWEEN ? AND ? ORDER BY ts ASC', (start, end))
        candles_data = [{"ts": row[0], "o": row[1], "h": row[2], "l": row[3], "c": row[4], "v": row[5]}
                        for row in cursor.fetchall()]
        cursor.execute('SELECT ts, data FROM orderbook_snapshots WHERE ts BETWEEN ? AND ? ORDER BY ts ASC', (start, end))
        snapshots = []
        for row in cursor.fetchall():
            try:
                s = json.loads(row[1])
                s["ts"] = row[0]
                snapshots.append(s)
            except: continue
        conn.close()
        return {"trades": trades_data, "candles": candles_data, "orderbook_snapshots": snapshots, "tf": tf}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/heatmap_history")
async def get_heatmap_history(tf: str, hours: float = Query(24.0)):
    try:
        cutoff = datetime.now(timezone.utc).timestamp() - (hours * 3600)
        mem_buckets = heatmap_manager.store.get_all_buckets_since(tf, cutoff)
        if mem_buckets:
            bids = []
            asks = []
            for bucket_ts, prices in mem_buckets:
                for price, (b_vol, a_vol) in prices.items():
                    if b_vol > 0:
                        bids.append([bucket_ts * 1000, price, round(b_vol, 2)])
                    if a_vol > 0:
                        asks.append([bucket_ts * 1000, price, round(a_vol, 2)])
            return {"tf": tf, "bids": bids, "asks": asks}

        # Fallback al DB
        conn = _open_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT bucket_ts, data FROM heatmap_buckets WHERE tf = ? AND bucket_ts >= ? ORDER BY bucket_ts",
            (tf, cutoff)
        )
        rows = cursor.fetchall()
        conn.close()

        bids = []
        asks = []
        for bucket_ts, json_data in rows:
            try:
                payload = json.loads(json_data)
                for p_str, vols in payload.items():
                    p = float(p_str)
                    b = float(vols.get("b", 0.0))
                    a = float(vols.get("a", 0.0))
                    if b > 0:
                        bids.append([bucket_ts * 1000, p, round(b, 2)])
                    if a > 0:
                        asks.append([bucket_ts * 1000, p, round(a, 2)])
            except Exception:
                continue
        return {"tf": tf, "bids": bids, "asks": asks}
    except Exception as e:
        logger.error("Errore API heatmap: %s", e)
        return {"tf": tf, "bids": [], "asks": [], "error": str(e)}


@app.get("/api/heatmap_snapshot_bin")
async def get_heatmap_snapshot_bin(tf: str):
    mem_buckets = heatmap_manager.store.get_all_buckets_since(tf, 0.0)
    if not mem_buckets:
        loaded = await asyncio.to_thread(
            heatmap_manager.load_from_db, tf, hours=float(config.HEATMAP_DB_KEEP_HOURS)
        )
        if loaded:
            logger.info("Snapshot bin %s: caricati %d bucket dal DB", tf, len(loaded))

    bin_data = heatmap_manager.build_binary_full_sync(tf)
    if not bin_data:
        seq = heatmap_manager.store.seq
        tf_code = heatmap_engine.TF_CODE_MAP.get(tf, 0)
        bin_data = struct.pack("<IBI", seq, tf_code, 0)
    return Response(bin_data, media_type="application/octet-stream")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=config.SERVER_HOST,
        port=config.SERVER_PORT,
        reload=False,
        # Log uvicorn separato dal nostro logger per evitare duplicati
        log_config=None,
        access_log=False,
        ws="wsproto"
    )
