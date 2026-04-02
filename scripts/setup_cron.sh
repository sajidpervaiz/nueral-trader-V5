#!/bin/bash

################################################################################
# AUTOMATED CRON JOB SETUP
# Configure health checks to run automatically every 6 hours
################################################################################

PROJECT_ROOT="/workspaces/CTO-TEST-AI-trading-Bot"
CRON_LOG="$PROJECT_ROOT/logs/cron_setup.log"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$CRON_LOG"
}

mkdir -p "$PROJECT_ROOT/logs"

log "================================"
log "CRON JOB SETUP"
log "================================"

# Make scripts executable
log "Making scripts executable..."
chmod +x "$PROJECT_ROOT/scripts/deploy_tier0.sh"
chmod +x "$PROJECT_ROOT/scripts/health_check.sh"

# Create cron job (runs at 0 AM, 6 AM, 12 PM, 6 PM)
log "Creating cron job..."

cat > /tmp/tier0_cron.txt << 'EOF'
# TIER 0 Production Health Checks
# Run every 6 hours
0 0,6,12,18 * * * cd /workspaces/CTO-TEST-AI-trading-Bot && /bin/bash scripts/health_check.sh >> logs/cron.log 2>&1
EOF

# Add to crontab (if not already added)
if crontab -l 2>/dev/null | grep -q "TIER 0 Production Health"; then
    log "Cron job already exists"
else
    log "Adding cron job to crontab..."
    cat /tmp/tier0_cron.txt >> /tmp/new_cron.txt
    crontab -l 2>/dev/null >> /tmp/new_cron.txt 2>/dev/null || true
    crontab /tmp/new_cron.txt
    log "✓ Cron job installed"
fi

log "Cron job setup complete"
log "View cront ab with: crontab -l"
