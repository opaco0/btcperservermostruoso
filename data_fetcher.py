# data_fetcher.py
import json
import asyncio
from collections import defaultdict
import aiohttp
import logging
import config


logger = logging.getLogger("aggregator.data_fetcher")

class MultiExchangeWSManager:
    def __init__(self):
        self.books = {
            "kraken_spot": {"bids": {}, "asks": {}},
            "bybit_futures": {"bids": {}, "asks": {}},
            "binance_futures": {"bids": {}, "asks": {}},
            "okx_futures": {"bids": {}, "asks": {}},
            "coinbase_spot": {"bids": {}, "asks": {}}
        }
        self.trades = {
            "kraken_spot": [],
            "bybit_futures": [],
            "binance_futures": [],
            "okx_futures": [],
            "coinbase_spot": []
        }
        self.exchange_locks = defaultdict(asyncio.Lock)
        # MODIFICA: limite massimo di trade accumulabili in coda prima del prelievo
        self.max_trades_queue = 5000

    async def start(self):
        asyncio.create_task(self._kraken_loop())
        asyncio.create_task(self._bybit_loop())
        asyncio.create_task(self._binance_depth_loop())
        asyncio.create_task(self._binance_trades_loop())
        asyncio.create_task(self._okx_loop())
        asyncio.create_task(self._coinbase_loop())

    async def _update_book(self, exchange, bids, asks, is_snapshot=False):
        async with self.exchange_locks[exchange]:
            if is_snapshot:
                self.books[exchange]["bids"] = {float(p): float(q) for p, q in bids}
                self.books[exchange]["asks"] = {float(p): float(q) for p, q in asks}
            else:
                for p, q in bids:
                    p_f, q_f = float(p), float(q)
                    if q_f == 0:
                        self.books[exchange]["bids"].pop(p_f, None)
                    else:
                        self.books[exchange]["bids"][p_f] = q_f
                for p, q in asks:
                    p_f, q_f = float(p), float(q)
                    if q_f == 0:
                        self.books[exchange]["asks"].pop(p_f, None)
                    else:
                        self.books[exchange]["asks"][p_f] = q_f

    async def _kraken_loop(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(config.KRAKEN_WS_URL, heartbeat=30.0) as ws:
                        await ws.send_json({
                            "method": "subscribe",
                            "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 1000}
                        })
                        await ws.send_json({
                            "method": "subscribe",
                            "params": {"channel": "trade", "symbol": ["BTC/USD"]}
                        })

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if "channel" in data:
                                    if data["channel"] == "book":
                                        for item in data["data"]:
                                            b = [(x["price"], x["qty"]) for x in item.get("bids", [])]
                                            a = [(x["price"], x["qty"]) for x in item.get("asks", [])]
                                            await self._update_book(
                                                "kraken_spot", b, a,
                                                is_snapshot=(data["type"] == "snapshot")
                                            )
                                    elif data["channel"] == "trade":
                                        async with self.exchange_locks["kraken_spot"]:
                                            for t in data["data"]:
                                                self.trades["kraken_spot"].append({
                                                    "p": t["price"], "q": t["qty"],
                                                    "ts": t["timestamp"], "buy": t["side"] == "buy"
                                                })
                                            # MODIFICA: limita coda
                                            if len(self.trades["kraken_spot"]) > self.max_trades_queue:
                                                self.trades["kraken_spot"] = self.trades["kraken_spot"][-self.max_trades_queue:]
            except Exception as e:
                logger.warning("Kraken WS riconnessione: %s", e)
            await asyncio.sleep(3)

    async def _bybit_loop(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(config.BYBIT_WS_URL, heartbeat=30.0) as ws:
                        await ws.send_json({
                            "op": "subscribe",
                            "args": ["orderbook.500.BTCUSDT", "publicTrade.BTCUSDT"]
                        })

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if "topic" in data:
                                    if "orderbook" in data["topic"]:
                                        d = data["data"]
                                        await self._update_book(
                                            "bybit_futures", d.get("b", []), d.get("a", []),
                                            is_snapshot=(data.get("type") == "snapshot")
                                        )
                                    elif "publicTrade" in data["topic"]:
                                        async with self.exchange_locks["bybit_futures"]:
                                            for t in data["data"]:
                                                self.trades["bybit_futures"].append({
                                                    "p": t["p"], "q": t["v"],
                                                    "ts": t["T"], "buy": t["S"].lower() == "buy"
                                                })
                                            # MODIFICA: limita coda
                                            if len(self.trades["bybit_futures"]) > self.max_trades_queue:
                                                self.trades["bybit_futures"] = self.trades["bybit_futures"][-self.max_trades_queue:]
            except Exception as e:
                logger.warning("Bybit WS riconnessione: %s", e)
            await asyncio.sleep(3)

    async def _binance_depth_loop(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    logger.info("Scarico Orderbook completo iniziale...")
                    async with session.get(
                        "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
                    ) as r:
                        if r.status == 200:
                            snap_data = await r.json()
                            await self._update_book(
                                "binance_futures",
                                snap_data.get("bids", []), snap_data.get("asks", []),
                                is_snapshot=True
                            )

                    async with session.ws_connect(config.BINANCE_WS_URL, heartbeat=30.0) as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                await self._update_book(
                                    "binance_futures",
                                    data.get("b", []), data.get("a", []),
                                    is_snapshot=False
                                )
            except Exception as e:
                logger.warning("Binance Depth WS riconnessione: %s", e)
            await asyncio.sleep(3)

    async def _binance_trades_loop(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(config.BINANCE_TRADES_URL, heartbeat=30.0) as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                async with self.exchange_locks["binance_futures"]:
                                    self.trades["binance_futures"].append({
                                        "p": data["p"], "q": data["q"],
                                        "ts": data["T"], "buy": not data["m"]
                                    })
                                    # Cap to max_trades_queue (consistent with other exchanges)
                                    if len(self.trades["binance_futures"]) > self.max_trades_queue:
                                        self.trades["binance_futures"] = self.trades["binance_futures"][-self.max_trades_queue:]
            except Exception as e:
                logger.warning("Binance Trades WS riconnessione: %s", e)
            await asyncio.sleep(3)


    async def _okx_loop(self):
        """OKX WebSocket: orderbook + trades per BTC-USDT-SWAP"""
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(config.OKX_WS_URL, heartbeat=30.0) as ws:
                        await ws.send_json({
                            "op": "subscribe",
                            "args": [
                                {"channel": "books", "instId": "BTC-USDT-SWAP"},
                                {"channel": "trades", "instId": "BTC-USDT-SWAP"}
                            ]
                        })
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if "arg" in data and "data" in data:
                                    channel = data["arg"].get("channel", "")
                                    action = data.get("action", "")
                                    payload = data["data"][0] if data["data"] else {}

                                    if channel == "books":
                                        bids_raw = payload.get("bids", [])
                                        asks_raw = payload.get("asks", [])
                                        bids = [[b[0], b[1]] for b in bids_raw if len(b) >= 2]
                                        asks = [[a[0], a[1]] for a in asks_raw if len(a) >= 2]
                                        await self._update_book(
                                            "okx_futures", bids, asks,
                                            is_snapshot=(action == "snapshot")
                                        )
                                    elif channel == "trades":
                                        async with self.exchange_locks["okx_futures"]:
                                            for t in payload if isinstance(payload, list) else [payload]:
                                                if not t:
                                                    continue
                                                self.trades["okx_futures"].append({
                                                    "p": t.get("px", t.get("price", 0)),
                                                    "q": t.get("sz", t.get("size", 0)),
                                                    "ts": t.get("ts", t.get("time", 0)),
                                                    "buy": (t.get("side", "") == "buy")
                                                })
                                            if len(self.trades["okx_futures"]) > self.max_trades_queue:
                                                self.trades["okx_futures"] = self.trades["okx_futures"][-self.max_trades_queue:]
            except Exception as e:
                logger.warning("OKX WS riconnessione: %s", e)
            await asyncio.sleep(3)

    async def _coinbase_loop(self):
        """Coinbase WebSocket: level2 orderbook + matches trades per BTC-USD"""
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(config.COINBASE_WS_URL, heartbeat=30.0) as ws:
                        await ws.send_json({
                            "type": "subscribe",
                            "product_ids": ["BTC-USD"],
                            "channels": ["level2", "matches"]
                        })
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                msg_type = data.get("type", "")

                                if msg_type == "snapshot":
                                    bids = data.get("bids", [])
                                    asks = data.get("asks", [])
                                    await self._update_book(
                                        "coinbase_spot",
                                        [[b[0], b[1]] for b in bids],
                                        [[a[0], a[1]] for a in asks],
                                        is_snapshot=True
                                    )
                                elif msg_type == "l2update":
                                    bid_updates = []
                                    ask_updates = []
                                    for change in data.get("changes", []):
                                        if len(change) >= 3:
                                            side, price, qty = change[0], change[1], change[2]
                                            if side == "buy":
                                                bid_updates.append([price, qty])
                                            else:
                                                ask_updates.append([price, qty])
                                    if bid_updates or ask_updates:
                                        await self._update_book(
                                            "coinbase_spot", bid_updates, ask_updates,
                                            is_snapshot=False
                                        )
                                elif msg_type == "match":
                                    async with self.exchange_locks["coinbase_spot"]:
                                        self.trades["coinbase_spot"].append({
                                            "p": data.get("price", 0),
                                            "q": data.get("size", 0),
                                            "ts": data.get("time", ""),
                                            "buy": data.get("side", "").lower() == "buy"
                                        })
                                        if len(self.trades["coinbase_spot"]) > self.max_trades_queue:
                                            self.trades["coinbase_spot"] = self.trades["coinbase_spot"][-self.max_trades_queue:]
            except Exception as e:
                logger.warning("Coinbase WS riconnessione: %s", e)
            await asyncio.sleep(3)

    async def get_data(self, exchange):
        async with self.exchange_locks[exchange]:
            book = {
                "bids": list(self.books[exchange]["bids"].items()),
                "asks": list(self.books[exchange]["asks"].items())
            }
            trades_copy = list(self.trades[exchange])
            self.trades[exchange] = []
            return book, trades_copy


ws_manager = MultiExchangeWSManager()


async def fetch_exchange_data_async(session, ex, market_type):
    source_id = f"{ex}_{market_type}"
    try:
        if source_id in ws_manager.books:
            book, trades = await ws_manager.get_data(source_id)
            return source_id, book, trades

        book, trades = {}, []
        if ex == "okx":
            inst = "BTC-USDT-SWAP" if market_type == "futures" else "BTC-USDT"
            async with session.get(f"{config.OKX_BASE}/books", params={"instId": inst, "sz": 400}) as r:
                if r.status == 200:
                    book = await r.json()
            async with session.get(f"{config.OKX_BASE}/trades", params={"instId": inst}) as r:
                if r.status == 200:
                    trades = await r.json()
        return source_id, book, trades
    except Exception as e:
        logger.error("Errore fetch %s: %s", source_id, e)
        return source_id, {}, []

async def fetch_all_async(session, fetch_klines=True, active_tfs=None):
    if active_tfs is None:
        active_tfs = {config.TIMEFRAME}

    candles_dict = {}
    
    if fetch_klines:
        # Funzione interna per scaricare un singolo TF
        async def fetch_tf_candles(tf):
            async with session.get(
                f"{config.BINANCE_FUTURES_BASE}/klines",
                params={"symbol": "BTCUSDT", "interval": tf, "limit": config.CANDLE_LIMIT}
            ) as r:
                if r.status == 200:
                    return tf, await r.json()
                return tf, None

        # Scarichiamo tutti i timeframe richiesti IN PARALLELO
        tf_tasks = [fetch_tf_candles(tf) for tf in active_tfs]
        tf_results = await asyncio.gather(*tf_tasks, return_exceptions=True)
        
        for res in tf_results:
            if not isinstance(res, Exception):
                tf, data = res
                if data:
                    candles_dict[tf] = data

    targets = [("binance", "futures"), ("bybit", "futures"), ("kraken", "spot"), ("okx", "futures"), ("coinbase", "spot")]
    tasks = [fetch_exchange_data_async(session, ex, mt) for ex, mt in targets]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    aggregated = []
    for res in results:
        if isinstance(res, Exception):
            logger.error("Task fallita: %s", res)
            continue
        sid, book, trades = res
        if book or trades:
            aggregated.append({"source": sid, "book": book, "trades": trades})

    # Restituiamo il dizionario delle candele
    return {"candles": candles_dict, "aggregated": aggregated}