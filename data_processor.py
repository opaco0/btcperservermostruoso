import math
import config
from datetime import datetime

def process_candles(raw):
    return [{"ts": float(c[0])/1000, "o": float(c[1]), "h": float(c[2]), "l": float(c[3]), "c": float(c[4]), "v": float(c[5])} for c in raw]

def process_trades(aggregated_list):
    out = []
    for entry in aggregated_list:
        source, raw = entry["source"], entry["trades"]
        if not raw:
            continue

        for t in raw:
            raw_ts = t["ts"]
            try:
                if isinstance(raw_ts, str):
                    ts_val = datetime.fromisoformat(raw_ts.replace('Z', '+00:00')).timestamp()
                else:
                    ts_val = float(raw_ts) / 1000.0 if float(raw_ts) > 1e11 else float(raw_ts)
            except Exception:
                ts_val = datetime.now().timestamp()

            try:
                out.append({
                    "ts": ts_val,
                    "p": float(t["p"]),
                    "q": float(t["q"]),
                    "buy": bool(t["buy"])
                })
            except (KeyError, ValueError, TypeError):
                continue

    return out

def process_book(aggregated_list, current_tf=None):
    """
    MODIFICA: ora accetta current_tf per usare ORDERBOOK_STEP_BY_TF.
    """
    merged_bids = {}
    merged_asks = {}

    # Bucket dinamico
    bucket_size = config.ORDERBOOK_STEP_BY_TF.get(current_tf, config.PRICE_BUCKET_SIZE)

    for entry in aggregated_list:
        source, book = entry["source"], entry["book"]
        if not book:
            continue

        bids, asks = [], []
        try:
            if isinstance(book, dict) and "bids" in book and isinstance(book["bids"], list):
                bids, asks = book["bids"], book["asks"]
            elif "okx" in source:
                pair_data = book.get("data", [{}])[0]
                bids, asks = pair_data.get("bids", []), pair_data.get("asks", [])
        except Exception:
            continue

        for b in bids:
            try:
                p = math.floor(float(b[0]) / bucket_size) * bucket_size
                merged_bids[p] = merged_bids.get(p, 0) + float(b[1])
            except Exception:
                continue

        for a in asks:
            try:
                p = math.floor(float(a[0]) / bucket_size) * bucket_size
                merged_asks[p] = merged_asks.get(p, 0) + float(a[1])
            except Exception:
                continue

    return {"bids": merged_bids, "asks": merged_asks}

def extract_live_price(raw_aggregated, candles_fallback=None):
    """Estrae il prezzo corrente cercando su tutti gli exchange disponibili."""
    price = 0.0

    if raw_aggregated:
        entry_map = {entry["source"]: entry for entry in raw_aggregated}

        # MODIFICA: prezzo solo da Bybit
        sources_to_try = ["bybit_futures"]

        for source in sources_to_try:
            entry = entry_map.get(source)
            if not entry:
                continue

            trades = entry.get("trades", [])
            if isinstance(trades, list) and len(trades) > 0:
                try:
                    for t in reversed(trades):
                        p = t.get("p") if isinstance(t, dict) else None
                        if p is not None and float(p) > 0:
                            price = float(p)
                            break
                except (ValueError, TypeError):
                    pass
                if price > 0:
                    break

            if price <= 0:
                book = entry.get("book", {})
                bids = book.get("bids", [])
                asks = book.get("asks", [])

                best_bid = None
                best_ask = None

                for p, q in bids:
                    try:
                        pf = float(p)
                        if best_bid is None or pf > best_bid:
                            best_bid = pf
                    except (ValueError, TypeError):
                        continue

                for p, q in asks:
                    try:
                        pf = float(p)
                        if best_ask is None or pf < best_ask:
                            best_ask = pf
                    except (ValueError, TypeError):
                        continue

                if best_bid is not None and best_ask is not None and best_ask > best_bid > 0:
                    price = (best_bid + best_ask) / 2.0
                    break

    if price <= 0 and candles_fallback and isinstance(candles_fallback, list) and len(candles_fallback) > 0:
        try:
            price = float(candles_fallback[-1]['c'])
        except (ValueError, TypeError, KeyError):
            pass

    return price
