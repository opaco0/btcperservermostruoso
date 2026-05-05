# config.py — versione produzione (4 vCPU / 8 GB RAM)
#http://127.0.0.1:8000
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8000

MAX_TRADES_HISTORY = 60000
CANDLE_LIMIT = 800

# ============================================================
# FIX CRITICO: book_history RAM cap
# book_history serve SOLO al bot_analyzer che usa max 180 entries.
# Il valore precedente (max HEATMAP_MAX_BOOK_HISTORY_BY_TF = 50,000)
# causava 1-2 GB di RAM per i soli snapshot orderbook.
# ============================================================
BOOK_HISTORY_MAX = 300   # 300 snapshot × ~30 KB = ~9 MB max

# ============================================================
# Heatmap bucket-based
# ============================================================
HEATMAP_BUCKET_DURATION_BY_TF = {
    "1m": 5,
    "5m": 10,
    "15m": 20,
    "1h": 80,
    "4h": 200,
    "1d": 500
}

HEATMAP_MAX_BUCKETS_MEMORY = 500
HEATMAP_SAVE_INTERVAL_SECONDS = 60
HEATMAP_PRUNE_MEMORY_HOURS = 12
HEATMAP_DB_KEEP_HOURS = 12

# Snapshot orderbook in RAM per TF — NON più usato per cap book_history
# (mantenuto per retrocompatibilità con heatmap_engine.py)
HEATMAP_MAX_BOOK_HISTORY_BY_TF = {
    "1m": 300, "5m": 300, "15m": 300,
    "1h": 300, "4h": 300, "1d": 300
}
HEATMAP_TF_ACTIVE = ["1m", "5m", "15m", "1h", "4h", "1d"]
HM_MAX_POINTS_PER_TF = {
    "1m": 25000, "5m": 40000, "15m": 60000,
    "1h": 90000, "4h": 90000, "1d": 90000
}
HM_MIN_VOLUME_TF = {
    "1m": 0.5, "5m": 0.5, "15m": 0.4,
    "1h": 0.4, "4h": 0.4, "1d": 0.4
}

HEATMAP_COLOR_GROUP_BY_TF = {
    "1m": "fast", "5m": "fast",
    "15m": "medium", "1h": "medium",
    "4h": "slow", "1d": "slow"
}
HEATMAP_COLOR_GROUP_NAMES = ["fast", "medium", "slow"]

# ============================================================
# Visual
# ============================================================
VP_LOOKBACK_HOURS = 24

COLOR_LEVEL_1 = "#012201"
COLOR_LEVEL_2 = "#023802"
COLOR_LEVEL_3 = "#026202"
COLOR_LEVEL_4 = "#2EAD0B"
COLOR_LEVEL_5 = "#04BB72"
COLOR_LEVEL_6 = "#34F799"

COLOR_CANDLE_UP   = "#681683"
COLOR_CANDLE_DOWN = "#a70006"
COLOR_TRADE_BUY   = "#00ffff"
COLOR_TRADE_SELL  = "#8b0202"
COLOR_BG          = "#0a0a0f"
COLOR_BG_PANEL    = "#111118"
COLOR_BG_TOOLBAR  = "#16161f"
COLOR_BORDER      = "#2a2e39"
COLOR_FG          = "#e8e8ed"
COLOR_FG_DIM      = "#6b7280"
COLOR_FG_MUTED    = "#4b5563"

FONT_FAMILY = "sans-serif"

# ============================================================
# Exchange APIs
# ============================================================
BINANCE_BASE         = "https://api4.binance.com/api/v3"
BINANCE_FUTURES_BASE = "https://fapi.binance.com/fapi/v1"
COINBASE_BASE        = "https://api.exchange.coinbase.com/products/BTC-USD"
KRAKEN_BASE          = "https://api.kraken.com/0/public"
BYBIT_BASE           = "https://api.bybit.com/v5/market"
OKX_BASE             = "https://www.okx.com/api/v5/market"

BINANCE_WS_URL   = "wss://fstream.binance.com/ws/btcusdt@depth@100ms"
BINANCE_TRADES_URL = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
BYBIT_WS_URL     = "wss://stream.bybit.com/v5/public/linear"
KRAKEN_WS_URL    = "wss://ws.kraken.com/v2"
OKX_WS_URL       = "wss://ws.okx.com:8443/ws/v5/public"
COINBASE_WS_URL  = "wss://ws-feed.exchange.coinbase.com"

PRICE_BUCKET_SIZE = 5.0
TIMEFRAME = "5m"

# ============================================================
# Clusters / Bot
# ============================================================
MIN_DELTA_BTC = {
    "1m": 500.0, "5m": 1000.0, "15m": 1500.0,
    "1h": 2000.0, "4h": 4000.0, "1d": 8000.0
}
TRADE_CLUSTER_RESOLUTION = 60
CLUSTER_SIZE_FACTOR = 0.07

ORDERBOOK_STEP_BY_TF = {
    "1m": 5.0, "5m": 5.0, "15m": 5.0,
    "1h": 5.0, "4h": 5.0, "1d": 5.0,
}

TRADE_DELTA_WEIGHT               = 0.35
TRADE_DELTA_TREND_WINDOW_MIN     = 7
TRADE_DELTA_SIGNIFICANT_THRESHOLD = 50.0
TRADE_DELTA_INFLUENCE_FACTOR     = 0.3

CLUSTER_SCALE_MIN         = 0.6
CLUSTER_SCALE_MAX         = 8.0
CLUSTER_ZOOM_REFERENCE_RANGE = 5000
ZOOM_REDRAW_INTERVAL_MS   = 100

ORDERBOOK_BIG_ORDER_THRESHOLD = 1000.0
ORDERBOOK_FILTER_TFS = ["15m", "1h", "4h", "1d"]

# ============================================================
# Replay / Snapshots
# ============================================================
OB_SNAPSHOT_INTERVAL_TICKS   = 45
MAX_OB_SNAPSHOT_AGE_HOURS    = 6

# ============================================================
# Heatmap Color Thresholds
# ============================================================
HEATMAP_MIN_VOLUME_THRESHOLD = 0.4
HEATMAP_VOLUME_LOW    = 100.0
HEATMAP_VOLUME_MEDIUM = 500.0
HEATMAP_VOLUME_HIGH   = 1000.0

HEATMAP_COLOR_COLD_START = "#0d1f0d"
HEATMAP_COLOR_COLD_MID   = "#166534"
HEATMAP_COLOR_COLD_END   = "#4ade80"
HEATMAP_COLOR_HOT_START  = "#ef4444"
HEATMAP_COLOR_HOT_MID    = "#fb923c"
HEATMAP_COLOR_HOT_END    = "#ffffff"
