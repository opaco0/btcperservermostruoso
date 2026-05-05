import csv
import time
import os
import logging
import threading
import queue
from datetime import datetime

logger = logging.getLogger("aggregator.data_logger")

CSV_HEADERS = [
    "timestamp", "current_price",
    "ob_score", "heatmap_score",
    "trade_score", "trade_delta_score", "trade_delta_trend",
    "trade_delta_current", "momentum", "atr",
    # Feature avanzate aggiunte
    "spread_bps", "vol_ratio",
    "time_sin", "time_cos",
    "dist_ma50", "rsi_14"
]

_log_queue = queue.Queue()

def get_today_filename():
    date_str = datetime.now().strftime("%Y-%m-%d")
    return f"ml_data_{date_str}.csv"

def init_csv(filename):
    if not os.path.isfile(filename):
        try:
            with open(filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(CSV_HEADERS)
            logger.info(f"Creato nuovo file di training per oggi: {filename}")
        except Exception as e:
            logger.error(f"Errore creazione CSV: {e}")

def _writer_worker():
    while True:
        try:
            item = _log_queue.get()
            if item is None:
                break

            batch = [item]
            while not _log_queue.empty():
                try:
                    batch.append(_log_queue.get_nowait())
                except queue.Empty:
                    break

            current_filename = get_today_filename()
            init_csv(current_filename)

            with open(current_filename, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(batch)

        except Exception as e:
            logger.error(f"Errore scrittura asincrona CSV: {e}")

_worker_thread = threading.Thread(target=_writer_worker, daemon=True)
_worker_thread.start()

def log_features(
    price, ob_score, hm_score, tr_score, td_score,
    td_trend, td_current, momentum, atr,
    spread_bps=0.0, vol_ratio=0.5,
    time_sin=0.0, time_cos=1.0,
    dist_ma50=0.0, rsi_14=50.0
):
    row = [
        int(time.time()), round(price, 2),
        round(ob_score, 4), round(hm_score, 4),
        round(tr_score, 4), round(td_score, 4), round(td_trend, 4),
        round(td_current, 4), round(momentum, 4), round(atr, 2),
        round(spread_bps, 4), round(vol_ratio, 4),
        round(time_sin, 6), round(time_cos, 6),
        round(dist_ma50, 4), round(rsi_14, 2)
    ]
    _log_queue.put(row)
