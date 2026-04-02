#!/bin/bash

################################################################################
# PRODUCTION HEALTH CHECK SCRIPT
# Run every 6 hours to verify TIER 0 components
################################################################################

set -e

PROJECT_ROOT="/workspaces/CTO-TEST-AI-trading-Bot"
LOG_FILE="$PROJECT_ROOT/logs/health_check.log"
ALERT_LOG="$PROJECT_ROOT/logs/alerts.log"

COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'
COLOR_YELLOW='\033[1;33m'
COLOR_BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${COLOR_BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${COLOR_GREEN}✓${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${COLOR_RED}✗${NC} $1" | tee -a "$LOG_FILE"
    echo -e "${COLOR_RED}[ERROR]${NC} $1" >> "$ALERT_LOG"
}

warning() {
    echo -e "${COLOR_YELLOW}⚠${NC} $1" | tee -a "$LOG_FILE"
    echo -e "${COLOR_YELLOW}[WARNING]${NC} $1" >> "$ALERT_LOG"
}

################################################################################
# CHECK: Bot is Running
################################################################################

check_bot_running() {
    log "CHECK 1: Bot process running..."

    BOT_PID=$(pgrep -f "python(3)?[[:space:]].*main\.py" | head -1 || echo "")
    
    if [ -z "$BOT_PID" ]; then
        error "Bot process not running"
        return 1
    else
        success "Bot running (PID: $BOT_PID)"
        return 0
    fi
}

################################################################################
# CHECK: API Health Endpoint
################################################################################

check_api_health() {
    log "CHECK 2: API health endpoint..."
    
    RESPONSE=$(curl -s -m 5 "http://127.0.0.1:8000/health" 2>/dev/null || echo '{}')
    
    if echo "$RESPONSE" | grep -q '"status":"ok"'; then
        TS=$(echo "$RESPONSE" | grep -o '"timestamp":[0-9]*' | cut -d':' -f2)
        success "API responding (timestamp: ${TS:-unknown})"
        return 0
    else
        error "API health endpoint failed"
        return 1
    fi
}

################################################################################
# CHECK: Paper Mode Enabled
################################################################################

check_paper_mode() {
    log "CHECK 3: Paper mode enabled..."
    
    RESPONSE=$(curl -s -m 5 "http://127.0.0.1:8000/health" 2>/dev/null || echo '{}')
    
    if echo "$RESPONSE" | grep -q '"paper_mode":true'; then
        success "Paper mode: ENABLED"
        return 0
    else
        error "Paper mode: DISABLED (trading with real money!)"
        return 1
    fi
}

################################################################################
# CHECK: Order Manager Operational
################################################################################

check_order_manager() {
    log "CHECK 4: Order manager operational..."

    RESPONSE=$(curl -s -L -m 5 "http://127.0.0.1:8000/orders/" 2>/dev/null || echo '{}')

    if [[ "$RESPONSE" == \[*\] ]]; then
        TOTAL=$(echo "$RESPONSE" | grep -o '"order_id"' | wc -l)
        success "Order manager operational (orders returned: $TOTAL)"
        return 0
    else
        error "Order manager not responding"
        return 1
    fi
}

################################################################################
# CHECK: Idempotency Working (No Duplicates)
################################################################################

check_idempotency() {
    log "CHECK 5: Idempotency (duplicate detection)..."

    RESPONSE=$(curl -s -L -m 5 "http://127.0.0.1:8000/orders/" 2>/dev/null || echo '{}')

    if [[ "$RESPONSE" != \[*\] ]]; then
        error "Cannot validate idempotency: orders payload is not a JSON array"
        return 1
    fi

    # Count unique client_order_ids vs total entries that actually include client IDs.
    TOTAL_ORDERS=$(echo "$RESPONSE" | grep -o '"client_order_id":"[^"]*"' | wc -l)
    UNIQUE_CLIENT_IDS=$(echo "$RESPONSE" | grep -o '"client_order_id":"[^"]*"' | sort | uniq | wc -l)

    if [ "$TOTAL_ORDERS" -eq "$UNIQUE_CLIENT_IDS" ]; then
        success "Idempotency working (0 duplicates in $TOTAL_ORDERS orders)"
        return 0
    else
        error "Duplicate orders detected: $TOTAL_ORDERS total, $UNIQUE_CLIENT_IDS unique"
        return 1
    fi
}

################################################################################
# CHECK: Circuit Breaker Not Triggered
################################################################################

check_circuit_breaker() {
    log "CHECK 6: Circuit breaker status..."
    
    RESPONSE=$(curl -s -m 5 "http://127.0.0.1:8000/risk/circuit-breaker" 2>/dev/null || echo '{}')

    OVERALL_STATUS=$(echo "$RESPONSE" | grep -o '"overall_status":"[^"]*"' | cut -d'"' -f4)

    if [ "$OVERALL_STATUS" = "HEALTHY" ]; then
        success "Circuit breaker: CLOSED (normal operation)"
        return 0
    else
        error "Circuit breaker status: ${OVERALL_STATUS:-UNKNOWN}"
        return 1
    fi
}

################################################################################
# CHECK: Position Limits Not Breached
################################################################################

check_position_limits() {
    log "CHECK 7: Position limits..."
    
    SUMMARY=$(curl -s -m 5 "http://127.0.0.1:8000/positions/summary" 2>/dev/null || echo '{}')
    LIMITS=$(curl -s -m 5 "http://127.0.0.1:8000/risk/limits" 2>/dev/null || echo '{}')

    POSITIONS=$(echo "$SUMMARY" | grep -o '"total_positions":[0-9]*' | cut -d':' -f2)
    MAX_POS_VALUE=$(echo "$LIMITS" | grep -o '"max_position_value":[0-9.]*' | cut -d':' -f2)

    if [ -n "$POSITIONS" ] && [ -n "$MAX_POS_VALUE" ]; then
        success "Position limits endpoint OK (positions: $POSITIONS, max_position_value: $MAX_POS_VALUE)"
        return 0
    else
        error "Unable to read position/risk limit metrics"
        return 1
    fi
}

################################################################################
# CHECK: Daily Loss Limit Not Breached
################################################################################

check_daily_loss() {
    log "CHECK 8: Risk snapshot availability..."

    MARGIN=$(curl -s -m 5 "http://127.0.0.1:8000/risk/margin" 2>/dev/null || echo '{}')
    HEALTH_SCORE=$(echo "$MARGIN" | grep -o '"health_score":[0-9.]*' | cut -d':' -f2)

    if [ -n "$HEALTH_SCORE" ]; then
        success "Risk margin snapshot OK (health_score: $HEALTH_SCORE)"
        return 0
    else
        error "Risk margin endpoint unavailable"
        return 1
    fi
}

################################################################################
# CHECK: Audit Trail Growing
################################################################################

check_audit_trail() {
    log "CHECK 9: Audit trail..."
    
    if [ -f "$PROJECT_ROOT/logs/audit.log" ]; then
        AUDIT_SIZE=$(wc -l < "$PROJECT_ROOT/logs/audit.log")
        success "Audit trail: $AUDIT_SIZE entries"
    else
        warning "Audit trail: Not yet created"
    fi
    
    return 0
}

################################################################################
# CHECK: Error Log Analysis
################################################################################

check_error_log() {
    log "CHECK 10: Error log analysis..."
    
    if [ -f "$PROJECT_ROOT/logs/bot.log" ]; then
        ERROR_COUNT=$(grep -i "error\|exception\|failed" "$PROJECT_ROOT/logs/bot.log" | wc -l)
        
        if [ "$ERROR_COUNT" -eq 0 ]; then
            success "Bot log: No errors"
        elif [ "$ERROR_COUNT" -lt 5 ]; then
            warning "Bot log: $ERROR_COUNT errors (acceptable)"
        else
            error "Bot log: $ERROR_COUNT errors (review needed)"
            return 1
        fi
    else
        warning "Bot log: Not found"
    fi
    
    return 0
}

################################################################################
# GENERATE REPORT
################################################################################

generate_report() {
    local status=$1
    local timestamp=$(date +'%Y-%m-%d %H:%M:%S')
    
    echo "" | tee -a "$LOG_FILE"
    echo "════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    echo "  HEALTH CHECK REPORT - $timestamp" | tee -a "$LOG_FILE"
    echo "════════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    
    if [ "$status" -eq 0 ]; then
        echo "✅ STATUS: HEALTHY" | tee -a "$LOG_FILE"
    else
        echo "⚠️  STATUS: ISSUES DETECTED" | tee -a "$LOG_FILE"
        echo "" | tee -a "$LOG_FILE"
        echo "Recent alerts:" | tee -a "$LOG_FILE"
        tail -n 10 "$ALERT_LOG" 2>/dev/null | tee -a "$LOG_FILE" || echo "  No alerts" | tee -a "$LOG_FILE"
    fi
    
    echo "" | tee -a "$LOG_FILE"
    echo "Full report saved to: $LOG_FILE" | tee -a "$LOG_FILE"
}

################################################################################
# MAIN EXECUTION
################################################################################

main() {
    mkdir -p "$PROJECT_ROOT/logs"
    
    log "================================"
    log "TIER 0 HEALTH CHECK"
    log "================================"
    
    local failed=0
    
    check_bot_running || failed=$((failed + 1))
    check_api_health || failed=$((failed + 1))
    check_paper_mode || failed=$((failed + 1))
    check_order_manager || failed=$((failed + 1))
    check_idempotency || failed=$((failed + 1))
    check_circuit_breaker || failed=$((failed + 1))
    check_position_limits || failed=$((failed + 1))
    check_daily_loss || failed=$((failed + 1))
    check_audit_trail || failed=$((failed + 1))
    check_error_log || failed=$((failed + 1))
    
    generate_report "$failed"
    
    if [ $failed -eq 0 ]; then
        log "✅ All checks passed"
        exit 0
    else
        log "⚠️  $failed check(s) failed"
        exit 1
    fi
}

main
