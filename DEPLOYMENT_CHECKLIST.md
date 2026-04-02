# 🎯 TIER 0 DEPLOYMENT CHECKLIST
## Step-by-Step Live Trading Activation

---

## 📋 PRE-DEPLOYMENT (Day Before)

### Preparation Tasks - 30 minutes

- [ ] **Read the docs** (mandatory)
  ```
  Read these files in order:
  1. README_DEPLOYMENT.md (5 min) - overview
  2. DEPLOYMENT_SUMMARY.md (10 min) - what you're getting
  3. PRODUCTION_DEPLOYMENT_GUIDE.md (15 min) - detailed steps
  ```

- [ ] **Verify environment**
  ```bash
  python3 --version
  # Expected: Python 3.12.x
  
  pip list | grep pytest
  # Expected: pytest installed
  ```

- [ ] **Run existing tests** (should all pass)
  ```bash
  cd /workspaces/CTO-TEST-AI-trading-Bot
  pytest tests/unit/ -q
  # Expected: 49 passed
  ```

- [ ] **Save this checklist**
  ```bash
  # Print it or save to desktop
  cat DEPLOYMENT_CHECKLIST.md > ~/deployment.txt
  ```

- [ ] **Prepare emergency contacts**
  - [ ] Your personal phone number
  - [ ] On-call engineer contact
  - [ ] IT support email

---

## 🚀 DEPLOYMENT DAY (Morning)

### Phase 1: Pre-Flight - 5 minutes

- [ ] **Close all applications**
  - Close browser tabs (might interfere with localhost:8000)
  - Close other services that use port 8000

- [ ] **Check disk space**
  ```bash
  df /workspaces/CTO-TEST-AI-trading-Bot
  # Expected: >1GB available
  ```

- [ ] **Verify API keys are NOT in any terminal**
  ```bash
  # Make sure you never type real API keys in terminal
  # They should ONLY be in environment variables
  export CEX_API_KEY="xxx"  # Only if you want real keys
  echo $CEX_API_KEY         # Verify it's set
  ```

---

### Phase 2: Execute Deployment - 10 minutes

- [ ] **Run deployment script**
  ```bash
  cd /workspaces/CTO-TEST-AI-trading-Bot
  bash scripts/deploy_tier0.sh
  ```

  **While it's running:**
  - Watch for colored output (GREEN ✓ is good, RED ✗ is bad)
  - Each phase should complete with ✓ CHECK PASSED
  - Do NOT interrupt (let it finish)
  - Total time: ~10 minutes

- [ ] **Verify SUCCESSFUL message**
  ```
  Expected final output:
  ════════════════════════════════════════════════════════════
            🚀 PRODUCTION DEPLOYMENT SUCCESSFUL 🚀
  ════════════════════════════════════════════════════════════
  ```

- [ ] **Take note of bot PID**
  ```
  Bot PID: XXXXX (you'll see this in output)
  ```

---

### Phase 3: Verify Live - 5 minutes

- [ ] **Test health endpoint**
  ```bash
  curl -s http://127.0.0.1:8000/health | jq .
  
  # Expected output:
  # {
  #   "status": "ok",
  #   "uptime_seconds": 120,
  #   "paper_mode": true
  # }
  ```

- [ ] **Verify paper mode is ON**
  ```bash
  curl -s http://127.0.0.1:8000/health | jq .paper_mode
  # Expected: true
  ```

- [ ] **Check dashboard is accessible**
  Open browser: http://127.0.0.1:8000
  - You should see the dashboard
  - Should show "Paper Trading Mode ACTIVE"
  - All metrics should be visible

---

### Phase 4: First Trade Test - 10 minutes

- [ ] **Place test order** (0.001 BTC - smallest safe unit)
  ```bash
  curl -X POST http://127.0.0.1:8000/orders/place \
    -H "Content-Type: application/json" \
    -d '{
      "exchange": "binance",
      "symbol": "BTC/USDT",
      "side": "buy",
      "quantity": 0.001,
      "price": 42000.0
    }'
  
  # You should get back:
  # {
  #   "order_id": "order_123abc",
  #   "client_order_id": "client_123abc",
  #   "status": "PENDING"
  # }
  ```

- [ ] **Take note of order_id**
  ```
  Your order_id: ____________
  ```

- [ ] **Check order appears in system**
  ```bash
  curl -s http://127.0.0.1:8000/orders | jq '.orders[] | {order_id, status}'
  # Should show your order with status PENDING
  ```

- [ ] **Verify audit trail recorded it**
  ```bash
  tail -5 logs/audit.log
  # Should show: ORDER_CREATED | order_123abc | BTC/USDT | BUY
  ```

---

## ✅ POST-DEPLOYMENT (After First Test)

### Verification - 5 minutes

- [ ] **Check no duplicate orders**
  ```bash
  curl -s http://127.0.0.1:8000/orders | jq '.total_orders'
  # Expected: 1 (or small number equivalent to your tests)
  
  curl -s http://127.0.0.1:8000/orders | jq '.orders | group_by(.client_order_id) | map(select(length > 1)) | length'
  # Expected: 0 (no duplicates!)
  ```

- [ ] **Check circuit breaker status**
  ```bash
  curl -s http://127.0.0.1:8000/risk/summary | jq '.circuit_breaker'
  # Expected: "CLOSED"
  ```

- [ ] **Run health check**
  ```bash
  bash scripts/health_check.sh
  # Expected: ✅ STATUS: HEALTHY
  ```

---

### Final Sign-Off - 5 minutes

- [ ] **Read final safety message**
  ```
  IMPORTANT: You are now live with real paper trading.
  
  Safety guarantees active:
  ✓ Idempotency - prevents duplicate fills
  ✓ Order tracking - 100% audit trail
  ✓ Circuit breaker - prevents cascade failures
  ✓ Risk limits - position size capped
  ✓ Paper mode - no real money at risk
  ```

- [ ] **Keep runbook visible**
  ```bash
  # Print or bookmark for daily use
  cat OPERATIONAL_RUNBOOK.md > ~/runbook.txt
  ```

- [ ] **Set up automatic monitoring**
  ```bash
  bash scripts/setup_cron.sh
  # This installs health checks to run every 6 hours
  ```

- [ ] **Save logs location**
  ```
  Logs saved to: /workspaces/CTO-TEST-AI-trading-Bot/logs/
  - bot.log (main log)
  - audit.log (order audit trail)
  - alerts.log (anomalies)
  - health_check.log (monitoring)
  ```

---

## 📊 WEEK 1: MONITORING

### Daily Checklist (5 minutes per day)

**Morning (6 AM):**
```bash
bash scripts/health_check.sh
# Should show: ✅ All checks passed
```

**Midday (12 PM):**
```bash
curl -s http://127.0.0.1:8000/health | jq .
# Should show: status = "ok"
```

**Evening (6 PM):**
```bash
curl -s http://127.0.0.1:8000/performance | jq '.daily_pnl'
# Check if positive (winning) or negative (losing)
```

### Track These Metrics

Create a simple tracking sheet (optional):

```
Date    | Uptime | Duplicates | P&L      | Notes
--------|--------|------------|----------|----------
2024-04-01 | 99.9%  | 0         | +$50.00  | Deployment day
2024-04-02 | 100%   | 0         | +$120.00 | All systems OK
...
```

### Success Criteria (After 7 Days)

- [ ] **Uptime > 99%** (max 2-3 hours downtime)
- [ ] **Zero duplicates** (no double-fills)
- [ ] **Error rate < 1%** (< 10 errors per 1000 API calls)
- [ ] **Paper mode always ON** (safe trading)
- [ ] **Audit trail 100% complete** (all fills logged)
- [ ] **Circuit breaker never triggered** (no cascades)

**If ALL pass:** → Ready to scale position size 10x in Week 2 ✅

---

## 🚨 EMERGENCY PROCEDURES

### Bot Not Responding

```bash
# 1. Check if running
ps aux | grep "python3 main.py"

# 2. If not running, restart
pkill -9 -f "python3 main.py" 2>/dev/null || true
sleep 5
python3 main.py > logs/bot.log 2>&1 &

# 3. Wait for startup
sleep 10

# 4. Verify
curl -s http://127.0.0.1:8000/health
```

### High Error Rate

```bash
# 1. Check recent errors
tail -50 logs/bot.log | grep -i error

# 2. Check health
bash scripts/health_check.sh

# 3. If many errors, restart
pkill -9 -f "python3 main.py"
sleep 10
python3 main.py > logs/bot.log 2>&1 &
```

### Duplicate Orders Detected

```bash
# 1. STOP immediately
pkill -9 -f "python3 main.py"

# 2. Check what happened
curl -s http://127.0.0.1:8000/orders | jq '.orders'

# 3. DO NOT restart until you understand what happened

# 4. Contact support
echo "CRITICAL: Duplicate orders detected at $(date)" | mail -s "URGENT" support@email.com
```

---

## ✅ FINAL CHECKLIST

Before you declare success, verify:

**Code**
- [ ] All 49 unit tests pass
- [ ] Idempotency tests pass (7/7)
- [ ] Order manager tests pass (6/6)
- [ ] Circuit breaker tests pass (4/4)

**Deployment**
- [ ] Deployment script runs successfully
- [ ] Health endpoint returns 200 OK
- [ ] Paper mode is on

**Operations**
- [ ] Health check script runs successfully
- [ ] Cron jobs set up (auto-monitoring)
- [ ] Emergency contacts saved

**Safety**
- [ ] Can view audit trail
- [ ] Circuit breaker not triggered
- [ ] No duplicate orders found
- [ ] Risk limits enforced

**Documentation**
- [ ] Have printed or saved:
  - [ ] OPERATIONAL_RUNBOOK.md
  - [ ] PRODUCTION_DEPLOYMENT_GUIDE.md
  - [ ] This checklist

---

## 🎉 YOU'RE LIVE!

**Deployment Complete** ✅

You now have:
- Production-safe trading system ✓
- Automated deployment ✓
- 24/7 monitoring ✓
- Emergency runbooks ✓
- Complete documentation ✓

**Monitor this week, scale next week.**

**Questions?** See:
- **How to operate** → OPERATIONAL_RUNBOOK.md
- **What to monitor** → PRODUCTION_DEPLOYMENT_GUIDE.md, Week 1 section
- **If something breaks** → OPERATIONAL_RUNBOOK.md, Incident Response section

---

**Deployment Signature**

I have completed the TIER 0 deployment and verified:

```
Signed by: _______________________
Date: ____________________________
Time: ____________________________

Systems verified: ✅ All
Ready to trade: ✅ Yes
Monitoring active: ✅ Yes
Emergency contacts: ✅ Yes
```

---

**Save this checklist for future reference.**

**Next read: OPERATIONAL_RUNBOOK.md (for daily operations)**

**Good luck! 🚀**
