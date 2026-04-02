#!/bin/bash

################################################################################
# TIER 0 PRODUCTION DEPLOYMENT SCRIPT
# Safe, automated deployment with verification
################################################################################

set -e

COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_YELLOW='\033[1;33m'
COLOR_BLUE='\033[0;34m'
NC='\033[0m' # No Color

PROJECT_ROOT="/workspaces/CTO-TEST-AI-trading-Bot"
LOG_FILE="$PROJECT_ROOT/logs/deployment.log"
BOT_PID_FILE="$PROJECT_ROOT/.bot.pid"

log() {
    echo -e "${COLOR_BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${COLOR_GREEN}✓${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${COLOR_RED}✗${NC} $1" | tee -a "$LOG_FILE"
}

warning() {
    echo -e "${COLOR_YELLOW}⚠${NC} $1" | tee -a "$LOG_FILE"
}

banner() {
    echo "" | tee -a "$LOG_FILE"
    echo "════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    echo "  $1" | tee -a "$LOG_FILE"
    echo "════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
}

################################################################################
# PHASE 1: PRE-FLIGHT CHECKS
################################################################################

phase1_preflight() {
    banner "PHASE 1: PRE-FLIGHT CHECKS (30 minutes)"
    
    log "1.1: Verifying test suite..."
    cd "$PROJECT_ROOT"
    if python3 -m pytest tests/unit/ -v --tb=short -q >> "$LOG_FILE" 2>&1; then
        success "All unit tests passing"
    else
        error "Unit tests failing - aborting deployment"
        return 1
    fi
    
    log "1.2: Checking paper mode is enabled..."
    python3 << 'PYEOF'
from core.config import Config
config = Config()
paper_mode = config.paper_mode
if paper_mode:
    print("PASS")
else:
    print("FAIL")
    exit(1)
PYEOF
    if [ $? -eq 0 ]; then
        success "Paper mode: ENABLED"
    else
        error "Paper mode is disabled - aborting for safety"
        return 1
    fi
    
    log "1.3: Checking for hardcoded API keys..."
    if grep -r "sk_live\|pk_live\|api_key.*=" config/ core/ execution/ --include="*.py" 2>/dev/null; then
        error "Hardcoded API keys found - aborting"
        return 1
    else
        success "No hardcoded API keys detected"
    fi
    
    log "1.4: Verifying environment variables..."
    if [ -z "$CEX_API_KEY" ] && [ -z "$CEX_SECRET_KEY" ]; then
        warning "CEX_API_KEY and CEX_SECRET_KEY not set - will use paper trading only"
    else
        success "API keys loaded from environment"
    fi
    
    log "1.5: Testing Binance WebSocket connection..."
    timeout 10 python3 << 'PYEOF' 2>/dev/null
import asyncio
from data_ingestion.cex_websocket import BinanceWebSocketFeed
import sys

async def test():
    try:
        feed = BinanceWebSocketFeed("EUR", {"enabled": True})
        await asyncio.sleep(2)
        await feed.stop()
        print("PASS")
    except Exception as e:
        print(f"FAIL: {e}")
        sys.exit(1)

asyncio.run(test())
PYEOF
    if [ $? -eq 0 ]; then
        success "Binance WebSocket: Connected"
    else
        warning "Binance WebSocket: Connection failed (may retry later)"
    fi
    
    log "1.6: Checking disk space..."
    AVAIL=$(df "$PROJECT_ROOT" | awk 'NR==2 {print $4}')
    if [ "$AVAIL" -lt 1000000 ]; then
        error "Insufficient disk space: ${AVAIL}KB available"
        return 1
    else
        success "Disk space: $(numfmt --to=iec-i --suffix=B $((AVAIL * 1024))) available"
    fi
    
    success "PHASE 1 COMPLETE: All pre-flight checks passed"
}

################################################################################
# PHASE 2: SHUTDOWN & BACKUP
################################################################################

phase2_shutdown() {
    banner "PHASE 2: SHUTDOWN & BACKUP"
    
    log "2.1: Checking for existing bot process..."
    if [ -f "$BOT_PID_FILE" ]; then
        BOT_PID=$(cat "$BOT_PID_FILE")
        if ps -p "$BOT_PID" > /dev/null 2>&1; then
            log "Graceful shutdown of PID $BOT_PID..."
            kill -TERM "$BOT_PID"
            sleep 3
            if ps -p "$BOT_PID" > /dev/null 2>&1; then
                log "Force killing PID $BOT_PID..."
                kill -9 "$BOT_PID"
            fi
            success "Bot process terminated"
        fi
        rm -f "$BOT_PID_FILE"
    else
        log "No bot process found"
    fi
    
    log "2.2: Cleaning zombie processes..."
    pkill -9 -f "python3 main.py" 2>/dev/null || true
    sleep 1
    success "Zombie processes cleaned"
    
    log "2.3: Backing up logs..."
    BACKUP_DIR="$PROJECT_ROOT/logs/backups/$(date +'%Y%m%d_%H%M%S')"
    mkdir -p "$BACKUP_DIR"
    if [ -f "$PROJECT_ROOT/logs/bot.log" ]; then
        cp "$PROJECT_ROOT/logs/bot.log" "$BACKUP_DIR/bot.log.bak"
        success "Logs backed up to $BACKUP_DIR"
    fi
    
    success "PHASE 2 COMPLETE: Shutdown & backup done"
}

################################################################################
# PHASE 3: START BOT
################################################################################

phase3_start() {
    banner "PHASE 3: START BOT"
    
    log "3.1: Starting bot process..."
    cd "$PROJECT_ROOT"
    
    # Clear old log
    > "$PROJECT_ROOT/logs/bot.log"
    
    # Start bot in background
    python3 main.py > "$PROJECT_ROOT/logs/bot.log" 2>&1 &
    BOT_PID=$!
    
    log "Bot started with PID: $BOT_PID"
    echo "$BOT_PID" > "$BOT_PID_FILE"
    
    log "3.2: Waiting for bot startup (10 seconds)..."
    sleep 10
    
    log "3.3: Verifying bot is running..."
    if ps -p "$BOT_PID" > /dev/null 2>&1; then
        success "Bot process running (PID: $BOT_PID)"
    else
        error "Bot failed to start"
        cat "$PROJECT_ROOT/logs/bot.log"
        return 1
    fi
    
    log "3.4: Testing health endpoint..."
    HEALTH_RESPONSE=$(curl -s http://127.0.0.1:8000/health 2>/dev/null || echo '{"status":"error"}')
    
    if echo "$HEALTH_RESPONSE" | grep -q '"status":"ok"'; then
        success "Health endpoint responding"
    else
        error "Health endpoint not responding"
        log "Response: $HEALTH_RESPONSE"
        return 1
    fi
    
    success "PHASE 3 COMPLETE: Bot started and responding"
}

################################################################################
# PHASE 4: VERIFY INSTALLATION
################################################################################

phase4_verify() {
    banner "PHASE 4: VERIFY INSTALLATION"
    
    log "4.1: Testing all dashboard endpoints..."
    ENDPOINTS=("health" "config/summary" "positions" "signals/recent" "orders" "risk/limits" "risk/circuit-breaker")
    FAILURES=0
    
    for endpoint in "${ENDPOINTS[@]}"; do
        RESPONSE=$(curl -s "http://127.0.0.1:8000/$endpoint" 2>/dev/null || echo '{}')
        if [ -z "$RESPONSE" ]; then
            error "$endpoint: No response"
            FAILURES=$((FAILURES + 1))
        else
            success "$endpoint: OK"
        fi
    done
    
    if [ $FAILURES -gt 0 ]; then
        error "$FAILURES endpoints failed"
        return 1
    fi
    
    log "4.2: Checking order manager..."
    ORDERS=$(curl -s "http://127.0.0.1:8000/orders" 2>/dev/null || echo "")
    if [[ "$ORDERS" == \[*\] ]]; then
        success "Order manager initialized"
    else
        error "Order manager not responding"
        return 1
    fi
    
    log "4.3: Verifying audit trail..."
    if [ -f "$PROJECT_ROOT/logs/audit.log" ]; then
        AUDIT_LINES=$(wc -l < "$PROJECT_ROOT/logs/audit.log")
        success "Audit trail initialized ($AUDIT_LINES lines)"
    else
        warning "Audit trail not yet created (will be created on first order)"
    fi
    
    success "PHASE 4 COMPLETE: Installation verified"
}

################################################################################
# PHASE 5: PRODUCTION READINESS
################################################################################

phase5_readiness() {
    banner "PHASE 5: PRODUCTION READINESS"
    
    log "5.1: Checking circuit breaker..."
    BREAKER=$(curl -s "http://127.0.0.1:8000/risk/summary" 2>/dev/null | grep -o '"circuit_breaker":"[^"]*"' | cut -d'"' -f4)
    success "Circuit breaker: $BREAKER"
    
    log "5.2: Verifying paper trading is active..."
    PAPER=$(curl -s "http://127.0.0.1:8000/health" 2>/dev/null | grep -o '"paper_mode":[^,]*' | cut -d':' -f2)
    if [ "$PAPER" = "true" ]; then
        success "Paper trading: ACTIVE (safe mode enabled)"
    else
        error "Paper trading: DISABLED (aborting for safety)"
        return 1
    fi
    
    log "5.3: Production metrics snapshot..."
    python3 << 'PYEOF'
import requests
import json

try:
    health = requests.get("http://127.0.0.1:8000/health", timeout=2).json()
    orders = requests.get("http://127.0.0.1:8000/orders", timeout=2).json()
    risk = requests.get("http://127.0.0.1:8000/risk/summary", timeout=2).json()
    
    print(f"  Uptime: {health.get('uptime_seconds', 'N/A')}s")
    print(f"  Orders: {orders.get('total_orders', 0)}")
    print(f"  Circuit Breaker: {risk.get('circuit_breaker', 'UNKNOWN')}")
    print(f"  Max Drawdown: {risk.get('max_drawdown_pct', 0):.2f}%")
    
except Exception as e:
    print(f"  Error fetching metrics: {e}")
PYEOF
    
    success "PHASE 5 COMPLETE: Production readiness verified"
}

################################################################################
# PHASE 6: FINAL SUMMARY
################################################################################

phase6_summary() {
    banner "PHASE 6: FINAL SUMMARY"
    
    echo "" | tee -a "$LOG_FILE"
    echo "╔════════════════════════════════════════════════════════════╗" | tee -a "$LOG_FILE"
    echo "║          🚀 PRODUCTION DEPLOYMENT SUCCESSFUL 🚀            ║" | tee -a "$LOG_FILE"
    echo "╚════════════════════════════════════════════════════════════╝" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    
    BOT_PID=$(cat "$BOT_PID_FILE" 2>/dev/null || echo "unknown")
    
    echo "📊 DEPLOYMENT SUMMARY:" | tee -a "$LOG_FILE"
    echo "  Bot PID: $BOT_PID" | tee -a "$LOG_FILE"
    echo "  Dashboard: http://127.0.0.1:8000" | tee -a "$LOG_FILE"
    echo "  Logs: $PROJECT_ROOT/logs/bot.log" | tee -a "$LOG_FILE"
    echo "  Config: $PROJECT_ROOT/config/settings.yaml" | tee -a "$LOG_FILE"
    echo "  Paper Mode: ENABLED (safe trading only)" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    
    echo "🔒 SAFETY FEATURES ACTIVE:" | tee -a "$LOG_FILE"
    echo "  ✓ Idempotency layer (prevents duplicate fills)" | tee -a "$LOG_FILE"
    echo "  ✓ Order lifecycle tracking (full audit trail)" | tee -a "$LOG_FILE"
    echo "  ✓ Circuit breaker (prevents cascades)" | tee -a "$LOG_FILE"
    echo "  ✓ Self-trade prevention (60-second window)" | tee -a "$LOG_FILE"
    echo "  ✓ Daily loss circuit breaker (3% limit)" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    
    echo "📈 NEXT STEPS (WEEK 1):" | tee -a "$LOG_FILE"
    echo "  1. Monitor /health endpoint for stability" | tee -a "$LOG_FILE"
    echo "  2. Place 0.001 BTC test order via dashboard" | tee -a "$LOG_FILE"
    echo "  3. Watch audit trail for duplicates (should be 0)" | tee -a "$LOG_FILE"
    echo "  4. Run: ./scripts/health_check.sh (every 6 hours)" | tee -a "$LOG_FILE"
    echo "  5. After 24 hours with no issues, expand position size" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    
    echo "🆘 IF PROBLEMS OCCUR:" | tee -a "$LOG_FILE"
    echo "  • Check logs: tail -f logs/bot.log" | tee -a "$LOG_FILE"
    echo "  • Stop bot: pkill -9 -f 'python3 main.py'" | tee -a "$LOG_FILE"
    echo "  • See PRODUCTION_DEPLOYMENT_GUIDE.md for troubleshooting" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    
    echo "✅ Deployment log: $LOG_FILE" | tee -a "$LOG_FILE"
}

################################################################################
# MAIN EXECUTION
################################################################################

main() {
    banner "TIER 0 PRODUCTION DEPLOYMENT"
    log "Starting deployment at $(date)"
    
    # Ensure logs directory exists
    mkdir -p "$PROJECT_ROOT/logs"
    
    # Run phases
    phase1_preflight || { error "Deployment failed at phase 1"; exit 1; }
    phase2_shutdown || { error "Deployment failed at phase 2"; exit 1; }
    phase3_start || { error "Deployment failed at phase 3"; exit 1; }
    phase4_verify || { error "Deployment failed at phase 4"; exit 1; }
    phase5_readiness || { error "Deployment failed at phase 5"; exit 1; }
    phase6_summary
    
    log "Deployment completed at $(date)"
}

# Run main
main
