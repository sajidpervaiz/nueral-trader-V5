#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# NUERAL-TRADER-5 — Testnet Live Trading Launcher
# ═══════════════════════════════════════════════════════════════════════════════
# Usage:
#   1. Get API keys from https://testnet.binancefuture.com
#   2. Set env vars:
#        export BINANCE_API_KEY="your-testnet-key"
#        export BINANCE_API_SECRET="your-testnet-secret"
#   3. Run: bash scripts/start_testnet.sh
# ═══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# ── Pre-flight checks ────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════╗"
echo "║  NUERAL-TRADER-5 — TESTNET LIVE TRADING         ║"
echo "║  Exchange: Binance Futures TESTNET               ║"
echo "║  Budget:   \$50 (fake money)                      ║"
echo "║  Leverage: 5x                                    ║"
echo "║  Symbols:  BTC, ETH, SOL                         ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Check API keys
if [[ -z "${BINANCE_API_KEY:-}" ]] || [[ "$BINANCE_API_KEY" == *'${'* ]]; then
    echo "❌ BINANCE_API_KEY not set!"
    echo ""
    echo "Get your testnet keys at: https://testnet.binancefuture.com"
    echo "Then run:"
    echo "  export BINANCE_API_KEY='your-key-here'"
    echo "  export BINANCE_API_SECRET='your-secret-here'"
    exit 1
fi

if [[ -z "${BINANCE_API_SECRET:-}" ]] || [[ "$BINANCE_API_SECRET" == *'${'* ]]; then
    echo "❌ BINANCE_API_SECRET not set!"
    exit 1
fi

echo "✅ API keys detected"

# Kill any existing instance
kill -9 "$(pgrep -f 'python main.py')" 2>/dev/null || true
kill -9 "$(lsof -ti:8000)" 2>/dev/null || true
sleep 1

# Activate venv if available
if [[ -f ".venv/bin/activate" ]]; then
    source .venv/bin/activate
fi

# Set required env vars
export NT_CONFIG_PATH="config/settings.testnet.yaml"
export LIVE_TRADING_CONFIRMED="true"

echo "🚀 Starting testnet live trading..."
echo "   Config: $NT_CONFIG_PATH"
echo "   Dashboard: http://localhost:8000"
echo "   Logs: /tmp/server_testnet.log"
echo ""
echo "Press Ctrl+C to stop"
echo ""

# Run in foreground so Ctrl+C works
python main.py 2>&1 | tee /tmp/server_testnet.log
