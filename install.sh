#!/usr/bin/env bash
# install.sh — Setup e gestione del Market Aggregator in produzione
# Uso:
#   ./install.sh install    → prima installazione
#   ./install.sh update     → aggiorna i file senza perdere il DB
#   ./install.sh start      → avvia il servizio
#   ./install.sh stop       → ferma il servizio
#   ./install.sh restart    → riavvia il servizio
#   ./install.sh status     → stato e log recenti
#   ./install.sh logs       → segue i log in tempo reale
#   ./install.sh backup     → backup del database

set -euo pipefail

# ── Configurazione ────────────────────────────────────────────────────────
INSTALL_DIR="/opt/market_aggregator"
SERVICE_NAME="market-aggregator"
PYTHON_MIN="3.10"
CURRENT_USER="${SUDO_USER:-$(whoami)}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Funzioni ──────────────────────────────────────────────────────────────

check_root() {
    [[ $EUID -eq 0 ]] || error "Esegui con sudo: sudo ./install.sh $1"
}

check_python() {
    local py
    py=$(python3 --version 2>&1 | awk '{print $2}')
    info "Python trovato: $py"
    python3 -c "import sys; sys.exit(0 if sys.version_info >= (3,10) else 1)" \
        || error "Richiesto Python >= ${PYTHON_MIN}. Trovato: $py"
}

install_system_deps() {
    info "Aggiornamento pacchetti di sistema..."
    apt-get update -qq
    # Aggiunto libgomp1 per XGBoost
    apt-get install -y -qq python3 python3-pip python3-venv curl sqlite3 libgomp1
}

create_venv() {
    info "Creazione virtual environment in ${INSTALL_DIR}/venv ..."
    python3 -m venv "${INSTALL_DIR}/venv"
    "${INSTALL_DIR}/venv/bin/pip" install --upgrade pip -q
    "${INSTALL_DIR}/venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q
    info "Dipendenze installate."
}

copy_files() {
    info "Copia file applicazione in ${INSTALL_DIR} ..."
    mkdir -p "${INSTALL_DIR}/static"

    # AGGIORNATO: inserito ai_config.json nella lista
    for f in server.py config.py data_fetcher.py data_processor.py \
              bot_analyzer.py heatmap_engine.py data_logger.py \
              requirements.txt trading_model.json ai_config.json; do
        if [[ -f "$f" ]]; then
            cp "$f" "${INSTALL_DIR}/"
        else
            # Non diamo errore per il modello o la config AI, potrebbero non esserci ancora
            if [[ "$f" != "trading_model.json" && "$f" != "ai_config.json" ]]; then
                warn "File non trovato: $f"
            fi
        fi
    done

    # Frontend
    [[ -f "static/index.html" ]] && cp "static/index.html" "${INSTALL_DIR}/static/" \
        || { [[ -f "index.html" ]] && cp "index.html" "${INSTALL_DIR}/static/index.html"; }

    chown -R "${CURRENT_USER}:${CURRENT_USER}" "${INSTALL_DIR}"
    info "File copiati."
}

install_service() {
    info "Installazione servizio systemd..."

    # Aggiusta User nel service file con l'utente corrente
    sed "s/^User=ubuntu$/User=${CURRENT_USER}/" market-aggregator.service \
        | sed "s/^Group=ubuntu$/Group=${CURRENT_USER}/" \
        > /etc/systemd/system/${SERVICE_NAME}.service

    systemctl daemon-reload
    systemctl enable "${SERVICE_NAME}"
    info "Servizio installato e abilitato all'avvio."
}

cmd_install() {
    check_root "install"
    check_python
    install_system_deps
    mkdir -p "${INSTALL_DIR}"
    copy_files
    create_venv
    install_service
    systemctl start "${SERVICE_NAME}"
    sleep 2
    systemctl status "${SERVICE_NAME}" --no-pager || true
    info "✅  Installazione completata. Il server gira su http://0.0.0.0:8000"
    info "    Per i log: sudo ./install.sh logs"
}

cmd_update() {
    check_root "update"
    info "Aggiornamento in corso (il DB viene preservato)..."
    systemctl stop "${SERVICE_NAME}" 2>/dev/null || true
    copy_files
    # Aggiorna solo le dipendenze Python (non distrugge il venv)
    "${INSTALL_DIR}/venv/bin/pip" install -r "${INSTALL_DIR}/requirements.txt" -q
    systemctl start "${SERVICE_NAME}"
    sleep 2
    systemctl status "${SERVICE_NAME}" --no-pager || true
    info "✅  Aggiornamento completato."
}

cmd_start()   { check_root "start";   systemctl start   "${SERVICE_NAME}"; info "Avviato."; }
cmd_stop()    { check_root "stop";    systemctl stop    "${SERVICE_NAME}"; info "Fermato."; }
cmd_restart() { check_root "restart"; systemctl restart "${SERVICE_NAME}"; info "Riavviato."; }

cmd_status() {
    systemctl status "${SERVICE_NAME}" --no-pager || true
    echo ""
    info "Health check:"
    curl -s http://127.0.0.1:8000/health 2>/dev/null | python3 -m json.tool || \
        warn "Server non raggiungibile su :8000"
}

cmd_logs() {
    info "Log in tempo reale (Ctrl+C per uscire)..."
    journalctl -u "${SERVICE_NAME}" -f --no-pager
}

cmd_backup() {
    local ts
    ts=$(date +%Y%m%d_%H%M%S)
    local dest="${INSTALL_DIR}/backups"
    mkdir -p "$dest"
    if [[ -f "${INSTALL_DIR}/market_history.db" ]]; then
        # Usa la modalità backup sicura di sqlite3 (hot backup)
        sqlite3 "${INSTALL_DIR}/market_history.db" \
            ".backup '${dest}/market_history_${ts}.db'"
        info "Backup creato: ${dest}/market_history_${ts}.db"
        # Mantieni solo gli ultimi 7 backup
        ls -t "${dest}"/market_history_*.db | tail -n +8 | xargs rm -f 2>/dev/null || true
    else
        warn "Database non trovato in ${INSTALL_DIR}"
    fi
}

# ── Dispatcher ────────────────────────────────────────────────────────────
CMD="${1:-help}"
case "$CMD" in
    install) cmd_install ;;
    update)  cmd_update ;;
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_restart ;;
    status)  cmd_status ;;
    logs)    cmd_logs ;;
    backup)  cmd_backup ;;
    *)
        echo "Uso: sudo ./install.sh {install|update|start|stop|restart|status|logs|backup}"
        exit 1
        ;;
esac
