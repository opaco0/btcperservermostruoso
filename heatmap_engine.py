"""
Heatmap Engine — bucket-based, differential broadcast.

Ogni snapshot orderbook viene accumulato in bucket di tempo fissi.
Il broadcast invia SOLO i bucket modificati dall'ultimo ciclo.
"""
import math
import json
import time
import logging
import sqlite3
import struct
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Set, Tuple, List, Optional

import config

logger = logging.getLogger("aggregator.heatmap_engine")

# ---------------------------------------------------------------------------
# Costanti di formato binario
# ---------------------------------------------------------------------------
TF_CODE_MAP = {"1m": 1, "5m": 2, "15m": 3, "1h": 4, "4h": 5, "1d": 6}
TF_CODE_INV = {v: k for k, v in TF_CODE_MAP.items()}


class BucketStore:
    """
    Memorizza i volumi per (bucket_tempo, bucket_prezzo).
    Struttura interna:
        store[tf][bucket_ts][price_bucket] = (bid_vol, ask_vol)
    """

    def __init__(self):
        # tf -> {bucket_ts: {price: [bid_vol, ask_vol]}}
        self._store: Dict[str, Dict[float, Dict[float, List[float]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))
        )
        # tf -> set(bucket_ts) modificati dall'ultimo flush
        self._dirty: Dict[str, Set[float]] = defaultdict(set)
        # tf -> ultimo bucket_ts processato (per sapere quale bucket è "live")
        self._current_bucket: Dict[str, float] = {}
        # group (fast/medium/slow) -> volume massimo (bid+ask) mai visto
        # I TF nello stesso gruppo condividono lo stesso max per normalizzazione colori
        self._max_volume: Dict[str, float] = defaultdict(float)
        # 3.2 Sequence Number — incrementato ad ogni ingest valido
        self.seq = 0

    def clear_tf(self, tf: str):
        """Reset completo per un TF (es. cambio timeframe client)."""
        if tf in self._store:
            del self._store[tf]
        self._dirty[tf] = set()
        self._current_bucket.pop(tf, None)

    def ingest_snapshot(self, tf: str, snapshot_ts: float, bids: dict, asks: dict,
                        price_step: float, bucket_duration: float,
                        big_order_threshold: Optional[float] = None):
        """
        Accumula un orderbook snapshot nei bucket.
        bids/asks sono dict {price: qty}.
        """
        # 3.5 Guardrail: timestamp negativo o assurdo
        if snapshot_ts < 0:
            return

        bucket_ts = math.floor(snapshot_ts / bucket_duration) * bucket_duration
        self._current_bucket[tf] = bucket_ts

        store_tf = self._store[tf]
        bucket = store_tf[bucket_ts]
        dirty_set = self._dirty[tf]

        # 3.3 Flag differenziale: traccia se almeno una cella è cambiata
        changed = False
        tf_max = 0.0

        # --- MODIFICA: Iniziamo tracciando i prezzi letti per azzerare quelli scomparsi ---
        processed_prices = set()

        # Bids
        for p, qty in bids.items():
            if big_order_threshold and qty > big_order_threshold:
                continue
            pb = math.floor(p / price_step) * price_step
            processed_prices.add(pb)
            
            # Sostituiamo (=) invece di sommare (+=)
            if bucket[pb][0] != qty:
                bucket[pb][0] = qty
                changed = True
                
            tf_max = max(tf_max, bucket[pb][0] + bucket[pb][1])

        # Asks
        for p, qty in asks.items():
            if big_order_threshold and qty > big_order_threshold:
                continue
            pb = math.floor(p / price_step) * price_step
            processed_prices.add(pb)
            
            # Sostituiamo (=) invece di sommare (+=)
            if bucket[pb][1] != qty:
                bucket[pb][1] = qty
                changed = True
                
            tf_max = max(tf_max, bucket[pb][0] + bucket[pb][1])
            
        # PULIZIA FANTASMI: Se un livello c'era prima ma ora è vuoto/cancellato, lo azzeriamo
        for pb in list(bucket.keys()):
            if pb not in processed_prices:
                if bucket[pb][0] != 0.0 or bucket[pb][1] != 0.0:
                    bucket[pb][0] = 0.0
                    bucket[pb][1] = 0.0
                    changed = True
                    
        if changed:
            dirty_set.add(bucket_ts)
            group = config.HEATMAP_COLOR_GROUP_BY_TF.get(tf, "fast")
            if tf_max > self._max_volume[group]:
                self._max_volume[group] = tf_max

        # Incrementa seq anche se non changed: il client deve sapere che il server è vivo
        self.seq += 1

    def get_dirty_buckets(self, tf: str, clear: bool = True) -> List[Tuple[float, Dict[float, Tuple[float, float]]]]:
        """
        Restituisce i bucket modificati per il TF richiesto.
        Se clear=True, svuota il set dirty.
        """
        dirty_ts = self._dirty.get(tf, set())
        if not dirty_ts:
            return []

        store_tf = self._store.get(tf, {})
        result = []
        for bucket_ts in dirty_ts:
            bucket = store_tf.get(bucket_ts, {})
            if bucket:
                snapshot = {p: (v[0], v[1]) for p, v in bucket.items() if v[0] > 0 or v[1] > 0}
                if snapshot:
                    result.append((bucket_ts, snapshot))

        if clear:
            self._dirty[tf] = set()
        return result

    def get_all_buckets_since(self, tf: str, since_ts: float) -> List[Tuple[float, Dict[float, Tuple[float, float]]]]:
        """Usato per costruire lo snapshot iniziale o lo storico API."""
        store_tf = self._store.get(tf, {})
        result = []
        for bucket_ts, bucket in sorted(store_tf.items()):
            if bucket_ts >= since_ts:
                snapshot = {p: (v[0], v[1]) for p, v in bucket.items() if v[0] > 0 or v[1] > 0}
                if snapshot:
                    result.append((bucket_ts, snapshot))
        return result

    def get_max_volume(self, tf: str) -> float:
        """Restituisce il volume massimo del GRUPPO di risoluzione del TF dato."""
        group = config.HEATMAP_COLOR_GROUP_BY_TF.get(tf, "fast")
        return self._max_volume.get(group, 0.0)

    def get_max_volume_by_group(self, group: str) -> float:
        """Restituisce il volume massimo per il gruppo colore specificato."""
        return self._max_volume.get(group, 0.0)

    def get_all_max_volumes(self) -> Dict[str, float]:
        """Restituisce un dizionario {group: max_volume} per tutti i gruppi colore."""
        return {g: self._max_volume.get(g, 0.0) for g in config.HEATMAP_COLOR_GROUP_NAMES}

    def prune_old_buckets(self, tf: str, keep_seconds: float, max_buckets: int = 500):
        """
        3.4 Ring Buffer + Pruning.
        Elimina bucket più vecchi del limite temporale E mantiene un cap massimo in memoria.
        """
        store_tf = self._store.get(tf)
        if not store_tf:
            return

        # Prune basato sul tempo
        cutoff = time.time() - keep_seconds
        old_keys = [k for k in store_tf if k < cutoff]
        for k in old_keys:
            del store_tf[k]

        # 3.4 Memory Leak Guard: pruning basato su numero massimo di bucket
        while len(store_tf) > max_buckets:
            oldest = min(store_tf.keys())
            del store_tf[oldest]

        # Pulisce i dirty orfani in modo sicuro (solo bucket che ancora esistono)
        self._dirty[tf] = {b for b in self._dirty.get(tf, set()) if b in store_tf}

    def serialize_for_db(self, tf: str) -> List[Tuple[float, str, str]]:
        """
        Restituisce tutti i bucket di un TF pronti per il DB.
        Ritorna lista di (bucket_ts, tf, json_data).
        """
        rows = []
        store_tf = self._store.get(tf, {})
        for bucket_ts, bucket in sorted(store_tf.items()):
            payload = {str(p): {"b": round(v[0], 4), "a": round(v[1], 4)}
                       for p, v in bucket.items()}
            if payload:
                rows.append((bucket_ts, tf, json.dumps(payload, separators=(",", ":"))))
        return rows


class HeatmapManager:
    """
    Facade che unisce BucketStore, persistenza DB e protocollo binario.
    """

    def __init__(self, db_path: str = "market_history.db"):
        self.store = BucketStore()
        self.db_path = db_path
        # Tabella creata da init_db() in server.py — nessuna duplicazione necessaria

    # -----------------------------------------------------------------------
    # Ingest
    # -----------------------------------------------------------------------
    def update_from_book_snapshot(self, tf: str, ts: float, bids: dict, asks: dict):
        """
        Entry point chiamato dal background engine ad ogni snapshot orderbook.
        """
        price_step = config.ORDERBOOK_STEP_BY_TF.get(tf, 5.0)
        bucket_dur = config.HEATMAP_BUCKET_DURATION_BY_TF.get(tf, 300)
        big_th = config.ORDERBOOK_BIG_ORDER_THRESHOLD if tf in config.ORDERBOOK_FILTER_TFS else None
        self.store.ingest_snapshot(tf, ts, bids, asks, price_step, bucket_dur, big_th)

    # -----------------------------------------------------------------------
    # Differential broadcast
    # -----------------------------------------------------------------------
    def build_binary_updates(self, tf: str) -> Optional[bytes]:
        """
        Costruisce il pacchetto binario con i bucket dirty per il TF.
        Formato:
            [seq: uint32]
            [tf_code: uint8]
            [n_buckets: uint32]
            per ogni bucket:
                [bucket_ts: float64]
                [n_prices: uint32]
                per ogni prezzo:
                    [price: float32]
                    [bid_vol: float32]
                    [ask_vol: float32]
        Ritorna bytes o None se non ci sono update.
        """
        dirty = self.store.get_dirty_buckets(tf, clear=True)
        if not dirty:
            return None

        tf_code = TF_CODE_MAP.get(tf, 0)
        n_buckets = len(dirty)

        buf = bytearray()
        buf.extend(struct.pack("<I", self.store.seq))   # <--- 3.2 Header: Seq
        buf.extend(struct.pack("<B", tf_code))
        buf.extend(struct.pack("<I", n_buckets))

        for bucket_ts, prices in dirty:
            buf.extend(struct.pack("<d", bucket_ts))          # float64
            n_prices = len(prices)
            buf.extend(struct.pack("<I", n_prices))
            for price, (bid_vol, ask_vol) in sorted(prices.items()):
                buf.extend(struct.pack("<fff", float(price), float(bid_vol), float(ask_vol)))

        return bytes(buf)

    def build_binary_full_sync(self, tf: str, since_ts: float = 0.0) -> Optional[bytes]:
        """
        Snapshot completo per un TF (utile al cambio TF o riconnessione).
        """
        all_buckets = self.store.get_all_buckets_since(tf, since_ts)
        if not all_buckets:
            return None

        tf_code = TF_CODE_MAP.get(tf, 0)
        n_buckets = len(all_buckets)

        buf = bytearray()
        buf.extend(struct.pack("<I", self.store.seq))   # <--- 3.2 Header: Seq
        buf.extend(struct.pack("<B", tf_code))
        buf.extend(struct.pack("<I", n_buckets))

        for bucket_ts, prices in all_buckets:
            buf.extend(struct.pack("<d", bucket_ts))
            n_prices = len(prices)
            buf.extend(struct.pack("<I", n_prices))
            for price, (bid_vol, ask_vol) in sorted(prices.items()):
                buf.extend(struct.pack("<fff", float(price), float(bid_vol), float(ask_vol)))

        return bytes(buf)

    # -----------------------------------------------------------------------
    # Persistenza
    # -----------------------------------------------------------------------
    def save_to_db(self, tf: str):
        """Flush dei bucket correnti su DB (REPLACE)."""
        rows = self.store.serialize_for_db(tf)
        if not rows:
            return
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT OR REPLACE INTO heatmap_buckets (bucket_ts, tf, data) VALUES (?, ?, ?)",
                rows
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning("Errore salvataggio heatmap DB: %s", e)

    def load_from_db(self, tf: str, hours: float = 24.0) -> List[Tuple[float, Dict[float, Tuple[float, float]]]]:
        """
        Carica bucket storici dal DB e li reidrata nella memoria del manager.
        Ritorna la lista per eventuale invio diretto al client.
        """
        cutoff = datetime.now(timezone.utc).timestamp() - (hours * 3600)
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT bucket_ts, data FROM heatmap_buckets WHERE tf = ? AND bucket_ts >= ? ORDER BY bucket_ts",
                (tf, cutoff)
            )
            rows = cursor.fetchall()
            conn.close()
        except Exception as e:
            logger.warning("Errore caricamento heatmap DB: %s", e)
            return []

        result = []
        group = config.HEATMAP_COLOR_GROUP_BY_TF.get(tf, "fast")
        price_step = config.ORDERBOOK_STEP_BY_TF.get(tf, 5.0)
        bucket_dur = config.HEATMAP_BUCKET_DURATION_BY_TF.get(tf, 300)
        for bucket_ts, json_data in rows:
            try:
                payload = json.loads(json_data)
                bids_snap: dict = {}
                asks_snap: dict = {}
                parsed: dict = {}
                for p_str, vols in payload.items():
                    p = float(p_str)
                    b = float(vols.get("b", 0.0))
                    a = float(vols.get("a", 0.0))
                    if b > 0:
                        bids_snap[p] = b
                    if a > 0:
                        asks_snap[p] = a
                    parsed[p] = (b, a)
                # Reidrata tramite l'API pubblica (evita accesso diretto a _store privato)
                self.store.ingest_snapshot(tf, bucket_ts, bids_snap, asks_snap,
                                           price_step, bucket_dur)
                if parsed:
                    result.append((bucket_ts, parsed))
            except Exception:
                continue
        return result

    def prune_db(self, keep_hours: float = 72.0):
        """Pulisce i bucket vecchi dal DB."""
        cutoff = datetime.now(timezone.utc).timestamp() - (keep_hours * 3600)
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM heatmap_buckets WHERE bucket_ts < ?", (cutoff,))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning("Errore pruning heatmap DB: %s", e)

    def prune_memory(self, tf: str, keep_hours: float = 6.0, max_buckets: int = 500):
        """Pulisce i bucket vecchi dalla memoria RAM."""
        self.store.prune_old_buckets(tf, keep_hours * 3600, max_buckets)
