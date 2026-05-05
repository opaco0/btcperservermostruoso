import logging
import math
import os
import xgboost as xgb
import pandas as pd
import numpy as np
import json
from datetime import datetime

logger = logging.getLogger("aggregator.bot_analyzer")
import config
import data_logger

# ============================================================
# --- CONFIGURAZIONE AI: LETTURA SOGLIA DINAMICA (JSON) ---
# ============================================================
AI_CONFIG_PATH = "ai_config.json"
CONFIDENCE_THRESHOLD = 0.60

if os.path.exists(AI_CONFIG_PATH):
    try:
        with open(AI_CONFIG_PATH, "r") as f:
            CONFIDENCE_THRESHOLD = json.load(f).get("confidence_threshold", 0.60)
            logger.info(f"[AI] Soglia caricata da config: {CONFIDENCE_THRESHOLD:.0%}")
    except Exception as e:
        logger.error(f"[ERRORE] Impossibile leggere {AI_CONFIG_PATH}: {e}")

MODEL_PATH = "trading_model.json"
model_ai = None

if os.path.exists(MODEL_PATH):
    try:
        model_ai = xgb.Booster()
        model_ai.load_model(MODEL_PATH)
        logger.info("[AI] Modello caricato con successo")
    except Exception as e:
        logger.error(f"[ERRORE] Caricamento modello AI: {e}")

def _calculate_rsi(series: pd.Series, period: int = 14) -> float:
    if len(series) <= period: return 50.0
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1] if not np.isnan(rsi.iloc[-1]) else 50.0

def _calculate_atr(candles: list, period: int = 14) -> float:
    if not candles or len(candles) <= period: return 150.0
    tr_list = []
    for i in range(1, len(candles)):
        c = candles[i]
        prev_c = candles[i-1]
        tr = max(abs(c['h'] - c['l']), abs(c['h'] - prev_c['c']), abs(c['l'] - prev_c['c']))
        tr_list.append(tr)
    recent_tr = tr_list[-period:]
    if len(recent_tr) < period: return sum(recent_tr) / len(recent_tr)
    atr = sum(recent_tr[:period]) / period
    for tr in recent_tr:
        atr = ((atr * (period - 1)) + tr) / period
    return atr

def _analyze_trade_delta_trend(trade_delta_history: list) -> dict:
    if not trade_delta_history or len(trade_delta_history) < 5:
        return {"score": 0.5, "trend": 0.0, "current_delta": 0.0, "weighted_avg": 0.0, "momentum": 0.0}
    recent = trade_delta_history[-60:] if len(trade_delta_history) > 60 else list(trade_delta_history)
    deltas = [x[1] for x in recent]
    current_delta = deltas[-1]
    n = len(deltas)
    weights = [(i / max(n - 1, 1)) ** 1.5 for i in range(n)]
    weight_sum = sum(weights)
    weighted_avg = sum(d * w for d, w in zip(deltas, weights)) / weight_sum if weight_sum > 0 else 0.0
    momentum = current_delta - weighted_avg
    raw_score = 0.5 + math.tanh(weighted_avg / 200.0) * 0.5
    trend_adjustment = math.tanh(momentum / 100.0) * 0.15
    final_score = max(0.05, min(0.95, raw_score + trend_adjustment))
    if len(deltas) >= 10:
        first_half = sum(deltas[:len(deltas) // 2]) / max(len(deltas) // 2, 1)
        second_half = sum(deltas[len(deltas) // 2:]) / max(len(deltas) - len(deltas) // 2, 1)
        trend = math.tanh((second_half - first_half) / 150.0)
    else:
        trend = 0.0
    return {"score": final_score, "trend": trend, "current_delta": current_delta, "weighted_avg": weighted_avg, "momentum": momentum}

def _adjust_levels_with_trade_delta(target, support, current_price, delta_analysis):
    if not delta_analysis or target <= 0 or support <= 0 or current_price <= 0: return target, support
    momentum = delta_analysis.get("momentum", 0.0)
    trend = delta_analysis.get("trend", 0.0)
    combined_force = math.tanh((momentum * 0.6 + trend * 100.0) / 200.0)
    influence = config.TRADE_DELTA_INFLUENCE_FACTOR
    dist_up = target - current_price
    dist_down = current_price - support
    if combined_force > 0:
        target = target + dist_up * combined_force * influence
        support = support + dist_down * combined_force * influence * 0.5
    else:
        support = support + dist_down * combined_force * influence
        target = target + dist_up * combined_force * influence * 0.5
    return round(target, 2), round(support, 2)

def get_live_probability(book_history: list, trades: list, current_price_futures: float = None, calculate_levels: bool = True, mode: str = "LAB", strategy_range: float = 0.001, trade_delta_history: list = None, candles: list = None) -> dict:
    try:
        if not book_history or not book_history[-1].get("bids"):
            return {"direction": "NEUTRALE", "prob": 50.0, "color": "#f59e0b", "target": 0, "support": 0, "target2": 0, "support2": 0, "trade_delta_current": 0.0}

        current_book = book_history[-1]
        bids = current_book.get("bids", {})
        asks = current_book.get("asks", {})
        best_bid = max(bids.keys(), default=0)
        best_ask = min(asks.keys(), default=0)
        if best_bid == 0 or best_ask == 0:
            return {"direction": "NEUTRALE", "prob": 50.0, "color": "#f59e0b", "target": 0, "support": 0, "target2": 0, "support2": 0, "trade_delta_current": 0.0}

        current_price = current_price_futures if (current_price_futures and current_price_futures > 0) else (best_bid + best_ask) / 2.0

        def calc_gravity(b_dict, a_dict, price):
            b_grav, a_grav = 0.0, 0.0
            epsilon = 0.5
            range_limit = float('inf') if mode == "LAB" else price * strategy_range
            for p, vol in b_dict.items():
                dist = abs(price - p)
                if 0 < dist < range_limit: b_grav += vol / ((dist + epsilon) ** 1.2)
            for p, vol in a_dict.items():
                dist = abs(p - price)
                if 0 < dist < range_limit: a_grav += vol / ((dist + epsilon) ** 1.2)
            return b_grav, a_grav

        cur_b_grav, cur_a_grav = calc_gravity(bids, asks, current_price)
        tot_cur_grav = cur_b_grav + cur_a_grav
        ob_score = (cur_b_grav / tot_cur_grav) if tot_cur_grav > 0 else 0.5

        lookback_idx = max(0, len(book_history) - 180)
        past_book = book_history[lookback_idx]
        past_b_grav, past_a_grav = calc_gravity(past_book.get("bids", {}), past_book.get("asks", {}), current_price)

        bid_change = (cur_b_grav - past_b_grav) / past_b_grav if past_b_grav > 0 else 0
        ask_change = (cur_a_grav - past_a_grav) / past_a_grav if past_a_grav > 0 else 0
        evolution_delta = bid_change - ask_change
        heatmap_score = 0.5 + (math.tanh(evolution_delta * 2.0) * 0.5)

        if trades:
            buy_power = sum(t["q"] * 1.5 if t["q"] > 0.5 else t["q"] for t in trades if t["buy"])
            sell_power = sum(t["q"] * 1.5 if t["q"] > 0.5 else t["q"] for t in trades if not t["buy"])
            tot_power = buy_power + sell_power
            trade_score = (buy_power / tot_power) if tot_power > 0 else 0.5
        else:
            trade_score = 0.5

        delta_analysis = _analyze_trade_delta_trend(trade_delta_history or [])
        trade_delta_score = delta_analysis["score"]

        # --- FEATURE ENGINEERING AVANZATE ---
        spread = best_ask - best_bid
        spread_bps = (spread / current_price * 10000) if current_price > 0 else 0.0

        if trades:
            buy_vol = sum(t["q"] for t in trades if t["buy"])
            tot_vol = sum(t["q"] for t in trades)
            vol_ratio = (buy_vol / tot_vol) if tot_vol > 0 else 0.5
        else:
            vol_ratio = 0.5

        now_utc = datetime.utcnow()
        hour_fraction = now_utc.hour + (now_utc.minute / 60.0)
        time_sin = math.sin(2 * math.pi * hour_fraction / 24.0)
        time_cos = math.cos(2 * math.pi * hour_fraction / 24.0)

        dist_ma50, rsi_14 = 0.0, 50.0
        if candles and len(candles) >= 50:
            closes = pd.Series([c['c'] for c in candles])
            ma50 = closes.rolling(window=50).mean().iloc[-1]
            if ma50 > 0: dist_ma50 = ((current_price - ma50) / ma50) * 100.0
            rsi_14 = _calculate_rsi(closes, period=14)

        current_atr_val = _calculate_atr(candles) if candles else 150.0

        prob_percentage = 50.0
        final_direction = "NEUTRALE"

        if model_ai:
            features_dict = {
                "ob_score": [ob_score],
                "heatmap_score": [heatmap_score],
                "trade_score": [trade_score],
                "trade_delta_score": [trade_delta_score],
                "trade_delta_trend": [delta_analysis.get("trend", 0.0)],
                "trade_delta_current": [delta_analysis.get("current_delta", 0.0)],
                "momentum": [delta_analysis.get("momentum", 0.0)],
                "atr": [current_atr_val],
                #"spread_bps": [spread_bps],
                #"vol_ratio": [vol_ratio],
                #"time_sin": [time_sin],
                #"time_cos": [time_cos],
                #"dist_ma50": [dist_ma50],
                #"rsi_14": [rsi_14],
            }
            dmatrix = xgb.DMatrix(pd.DataFrame(features_dict))
            preds = model_ai.predict(dmatrix)
            
            if preds.ndim == 1:
                prob_short, prob_neutro, prob_long = 1.0 - float(preds[0]), 0.0, float(preds[0])
            else:
                row = preds[0]
                if len(row) == 3: prob_short, prob_neutro, prob_long = row
                elif len(row) == 2: prob_short, prob_long = row; prob_neutro = 0.0
                else: prob_short, prob_neutro, prob_long = 0, 1, 0

            # Uso la costante aggiornata dinamicamente
            if prob_long > CONFIDENCE_THRESHOLD and prob_long > prob_short:
                prob_percentage = prob_long * 100
                final_direction = "LONG"
            elif prob_short > CONFIDENCE_THRESHOLD and prob_short > prob_long:
                prob_percentage = 100 - (prob_short * 100) 
                final_direction = "SHORT"
            else:
                prob_percentage = 50.0 
                final_direction = "NEUTRALE"
        else:
            ob_w, hm_w = 0.25, 0.35
            tr_w = max(0.0, 0.40 - config.TRADE_DELTA_WEIGHT)
            total_w = ob_w + hm_w + tr_w + config.TRADE_DELTA_WEIGHT
            final_score = (ob_score * (ob_w/total_w)) + (heatmap_score * (hm_w/total_w)) + (trade_score * (tr_w/total_w)) + (trade_delta_score * (config.TRADE_DELTA_WEIGHT/total_w))
            prob_percentage = final_score * 100
            if prob_percentage > 60: final_direction = "LONG"
            elif prob_percentage < 40: final_direction = "SHORT"
            else: final_direction = "NEUTRALE"

        actual_target, actual_support, actual_target2, actual_support2 = 0, 0, 0, 0

        if calculate_levels:
            current_atr = current_atr_val  # Riuso il valore già calcolato sopra
            dead_zone = max(20.0, current_atr * 0.3)
            max_search_dist = min(current_atr * 3.0, 300.0)
            limit_up = current_price + max_search_dist
            limit_down = current_price - max_search_dist

            valid_bids = {p: v for p, v in bids.items() if limit_down < p < (current_price - dead_zone)}
            valid_asks = {p: v for p, v in asks.items() if (current_price + dead_zone) < p < limit_up}
            cluster_window = max(10.0, current_atr * 0.20)

            def find_best_cluster_peak(order_dict, window=cluster_window):
                if not order_dict: return None
                levels = sorted(order_dict.keys())
                best_vol = 0
                best_peak_price = None
                for start_p in levels:
                    end_p = start_p + window
                    cluster_vol = sum(v for p, v in order_dict.items() if start_p <= p <= end_p)
                    if cluster_vol > best_vol:
                        best_vol = cluster_vol
                        cluster_items = {p: v for p, v in order_dict.items() if start_p <= p <= end_p}
                        best_peak_price = max(cluster_items.items(), key=lambda x: x[1])[0]
                return best_peak_price

            ai_support_raw = find_best_cluster_peak(valid_bids)
            ai_target_raw = find_best_cluster_peak(valid_asks)

            default_dist = max(30.0, current_atr * 0.8)
            actual_support = ai_support_raw if ai_support_raw else current_price - default_dist
            actual_target = ai_target_raw if ai_target_raw else current_price + default_dist
            actual_target, actual_support = _adjust_levels_with_trade_delta(actual_target, actual_support, current_price, delta_analysis)

            dist_up = abs(actual_target - current_price)
            dist_down = abs(current_price - actual_support)
            is_valid_trade = False
            
            if prob_percentage > 60:
                if dist_up >= dist_down: is_valid_trade = True
            elif prob_percentage < 40:
                if dist_down >= dist_up: is_valid_trade = True

            else:
                if ai_target_raw:
                    valid_asks_2 = {p: v for p, v in valid_asks.items() if p > ai_target_raw + 50}
                    actual_target2 = find_best_cluster_peak(valid_asks_2) or 0
                if ai_support_raw:
                    valid_bids_2 = {p: v for p, v in valid_bids.items() if p < ai_support_raw - 50}
                    actual_support2 = find_best_cluster_peak(valid_bids_2) or 0

        current_momentum = delta_analysis.get("momentum", 0.0)
        
        data_logger.log_features(
            price=current_price, ob_score=ob_score, hm_score=heatmap_score,
            tr_score=trade_score, td_score=trade_delta_score, td_trend=delta_analysis.get("trend", 0.0),
            td_current=delta_analysis.get("current_delta", 0.0), momentum=current_momentum, atr=current_atr_val,
            spread_bps=spread_bps, vol_ratio=vol_ratio, time_sin=time_sin, time_cos=time_cos,
            dist_ma50=dist_ma50, rsi_14=rsi_14
        )

        result = {
            "direction": "NEUTRALE", "prob": max(prob_percentage, 100 - prob_percentage), "color": "#f59e0b",
            "target": actual_target, "target2": actual_target2, "support": actual_support, "support2": actual_support2,
            "trade_delta_score": trade_delta_score, "trade_delta_trend": delta_analysis.get("trend", 0.0),
            "trade_delta_current": delta_analysis.get("current_delta", 0.0), "trade_delta_weight_applied": config.TRADE_DELTA_WEIGHT
        }

        if prob_percentage > 60:
            result.update({"direction": "LONG", "prob": prob_percentage, "color": "#22c55e", "target": actual_target, "support": actual_support, "target2": actual_target2})
        elif prob_percentage < 40:
            result.update({"direction": "SHORT", "prob": 100 - prob_percentage, "color": "#ef4444", "target": actual_support, "support": actual_target, "target2": actual_support2})
        
        model_date = "Standard Mode"
        if model_ai is not None and os.path.exists(MODEL_PATH):
            mtime = os.path.getmtime(MODEL_PATH)
            model_date = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')

        result["ai_status"] = {"active": True if model_ai is not None else False, "last_train": model_date}
        return result

    except Exception as e:
        logger.exception("Errore in get_live_probability")
        return {"direction": "NEUTRALE", "prob": 50.0, "color": "#f59e0b", "target": 0, "target2": 0, "support": 0, "support2": 0, "trade_delta_current": 0.0}
