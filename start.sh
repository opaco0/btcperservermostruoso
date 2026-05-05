#!/usr/bin/env bash
# start.sh — avvio rapido con nohup (alternativa al servizio systemd)
# Utile per: test veloci, VPS senza systemd, ambienti Docker.
#
# Uso:
#   ./start.sh          → avvia in background e scrive PID in market.pid
#   ./start.sh stop     → ferma il processo tramite PID
#   ./start.sh restart  → stop + start
#   ./start.sh status   → controlla se il processo gira

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${SCRIPT_DIR}/market.pid"
LOG_FILE="${SCRIPT_DIR}/market_aggregator.log"
VENV_PYTHON="${SCRIPT_DIR}/venv/Scripts/python"

# Usa il Python del venv se esiste, altrimenti python3 di sistema
if [[ -x "${VENV_PYTHON}" ]]; then
    PYTHON="${VENV_PYTHON}"
else
    PYTHON="python3"
fi

start() {
    if [[ -f "${PID_FILE}" ]]; then
        PID=$(cat "${PID_FILE}")
        if kill -0 "${PID}" 2>/dev/null; then
            echo "⚠️  Il server è già in esecuzione (PID ${PID})."
            echo "   Usa './start.sh restart' per riavviarlo."
            exit 0
        else
            echo "PID file obsoleto rimosso."
            rm -f "${PID_FILE}"
        fi
    fi

    echo "▶  Avvio Market Aggregator..."
    cd "${SCRIPT_DIR}"

    # Installa dipendenze se venv non esiste
    if [[ ! -x "${VENV_PYTHON}" ]]; then
        echo "Virtual environment non trovato. Creazione..."
        python3 -m venv venv
        venv/Scripts/python -m pip install -q --upgrade pip
        venv/Scripts/python -m pip install -q -r requirements.txt
        echo "Dipendenze installate."
    fi

    nohup "${PYTHON}" server.py >> "${LOG_FILE}" 2>&1 &
    echo $! > "${PID_FILE}"
    sleep 1

    if kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
        echo "✅  Server avviato. PID: $(cat "${PID_FILE}")"
        echo "   Log: tail -f ${LOG_FILE}"
        echo "   Stop: ./start.sh stop"
    else
        echo "❌  Il server non è partito. Controlla ${LOG_FILE} per gli errori."
        rm -f "${PID_FILE}"
        exit 1
    fi
}

stop() {
    if [[ ! -f "${PID_FILE}" ]]; then
        echo "Nessun PID file trovato. Il server non sta girando (o è stato avviato manualmente)."
        return
    fi
    PID=$(cat "${PID_FILE}")
    if kill -0 "${PID}" 2>/dev/null; then
        echo "⏹  Arresto server (PID ${PID})..."
        kill -TERM "${PID}"
        # Attende fino a 10 secondi
        for i in $(seq 1 10); do
            sleep 1
            kill -0 "${PID}" 2>/dev/null || break
        done
        # Forza se ancora vivo
        kill -0 "${PID}" 2>/dev/null && kill -KILL "${PID}" 2>/dev/null || true
        rm -f "${PID_FILE}"
        echo "✅  Server fermato."
    else
        echo "Il processo non esiste più. Rimuovo PID file."
        rm -f "${PID_FILE}"
    fi
}

status() {
    if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
        PID=$(cat "${PID_FILE}")
        echo "✅  Server in esecuzione (PID ${PID})"
        # Memoria usata dal processo
        RSS=$(ps -o rss= -p "${PID}" 2>/dev/null | awk '{printf "%.0f MB", $1/1024}') || RSS="N/A"
        echo "   RAM: ${RSS}"
        echo "   Health:"
        curl -s http://127.0.0.1:8000/health 2>/dev/null | python3 -m json.tool 2>/dev/null || \
            echo "   (endpoint /health non ancora raggiungibile)"
    else
        echo "❌  Server non in esecuzione."
    fi
}

CMD="${1:-start}"
case "$CMD" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; sleep 1; start ;;
    status)  status ;;
    logs)    tail -f "${LOG_FILE}" ;;
    *)
        echo "Uso: ./start.sh {start|stop|restart|status|logs}"
        exit 1
        ;;
esac
