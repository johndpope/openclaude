#!/bin/bash
# OpenClaude gRPC server control script
# Bind to docker bridge IP so containers can reach it (not LAN).

set -euo pipefail

GRPC_HOST="${GRPC_HOST:-172.18.0.1}"
GRPC_PORT="${GRPC_PORT:-50051}"
LOG_FILE="${LOG_FILE:-/tmp/openclaude-grpc.log}"
PID_FILE="${PID_FILE:-/tmp/openclaude-grpc.pid}"
BUN_BIN="$(command -v bun 2>/dev/null || echo /home/johndpope/.bun/bin/bun)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

start() {
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "gRPC server already running (PID $(cat "$PID_FILE")). Use 'stop' first or 'restart'."
    return 1
  fi

  echo "Starting OpenClaude gRPC server on ${GRPC_HOST}:${GRPC_PORT} ..."
  cd "$PROJECT_DIR"

  GRPC_HOST="$GRPC_HOST" GRPC_PORT="$GRPC_PORT" nohup "$BUN_BIN" run dev:grpc \
    > "$LOG_FILE" 2>&1 &
  PID=$!
  echo "$PID" > "$PID_FILE"

  # Wait up to 10s for listener
  for i in $(seq 1 10); do
    sleep 1
    if ss -ltn 2>/dev/null | grep -q "${GRPC_HOST}:${GRPC_PORT}"; then
      echo "Ready — PID $PID listening on ${GRPC_HOST}:${GRPC_PORT}"
      return 0
    fi
  done

  echo "Timed out waiting for listener. Check log: $LOG_FILE"
  tail -5 "$LOG_FILE"
  return 1
}

stop() {
  if [ -f "$PID_FILE" ]; then
    PID="$(cat "$PID_FILE")"
    kill "$PID" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "Stopped PID $PID"
  else
    # Fallback: find by process name
    PIDS="$(pgrep -f "bun.*start-grpc" 2>/dev/null || true)"
    if [ -n "$PIDS" ]; then
      echo "$PIDS" | xargs kill 2>/dev/null || true
      echo "Stopped: $PIDS"
    else
      echo "No gRPC server process found."
    fi
  fi

  # Wait for port to free
  for i in $(seq 1 5); do
    if ! ss -ltn 2>/dev/null | grep -q ":${GRPC_PORT}"; then
      echo "Port $GRPC_PORT released."
      return 0
    fi
    sleep 1
  done
  echo "Port $GRPC_PORT still in use — may be lingering."
}

status() {
  if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    PID="$(cat "$PID_FILE")"
    echo "Running — PID $PID on ${GRPC_HOST}:${GRPC_PORT}"
    ss -ltn 2>/dev/null | grep ":${GRPC_PORT}" || echo "(no listener on ${GRPC_PORT})"
    return 0
  fi

  PIDS="$(pgrep -f "bun.*start-grpc" 2>/dev/null || true)"
  if [ -n "$PIDS" ]; then
    echo "Running (no PID file) — PIDs: $PIDS"
    ss -ltn 2>/dev/null | grep ":${GRPC_PORT}" || echo "(no listener on ${GRPC_PORT})"
    return 0
  fi

  echo "Stopped."
  return 1
}

case "${1:-status}" in
  start)   start ;;
  stop)    stop ;;
  restart) stop; sleep 1; start ;;
  status)  status ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    echo "  GRPC_HOST=${GRPC_HOST} GRPC_PORT=${GRPC_PORT}"
    exit 1
    ;;
esac
