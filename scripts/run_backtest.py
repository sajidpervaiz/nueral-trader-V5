#!/usr/bin/env python3
"""
Backtest runner — fetches BTC/USDT 1h data via ccxt and runs full
strategy/risk/execution pipeline to produce metrics report.

Usage:
    python scripts/run_backtest.py [--symbol BTC/USDT:USDT] [--timeframe 1h] [--months 6]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from analysis.technical import TechnicalIndicators
from engine.fast_backtester import FastBacktester, MonteCarloSimulator, WalkForwardOptimizer

_indicators = TechnicalIndicators()


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    return _indicators.compute_all(df)


def fetch_ohlcv(symbol: str, timeframe: str, months: int) -> pd.DataFrame:
    """Fetch OHLCV data from Binance via ccxt."""
    import ccxt

    exchange = ccxt.binanceusdm({"enableRateLimit": True})
    all_data: list[list] = []
    since_ms = int((time.time() - months * 30 * 86400) * 1000)
    limit = 1000

    logger.info("Fetching {} {} candles for {} months...", symbol, timeframe, months)

    while True:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=limit)
        except Exception as exc:
            logger.warning("Fetch error: {} — retrying with fallback", exc)
            # Fallback: try spot if futures fails
            try:
                exchange2 = ccxt.binance({"enableRateLimit": True})
                spot_symbol = symbol.replace(":USDT", "")
                ohlcv = exchange2.fetch_ohlcv(spot_symbol, timeframe, since=since_ms, limit=limit)
            except Exception as exc2:
                logger.error("Fallback also failed: {}", exc2)
                break

        if not ohlcv:
            break

        all_data.extend(ohlcv)
        since_ms = ohlcv[-1][0] + 1

        if len(ohlcv) < limit:
            break

        time.sleep(exchange.rateLimit / 1000.0)

    if not all_data:
        logger.error("No data fetched for {}", symbol)
        return pd.DataFrame()

    df = pd.DataFrame(all_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("timestamp", inplace=True)
    df = df[~df.index.duplicated(keep="first")]
    df.sort_index(inplace=True)

    logger.info("Fetched {} candles from {} to {}", len(df), df.index[0], df.index[-1])
    return df


def generate_strategy_signals(df: pd.DataFrame) -> pd.Series:
    """Dual MA crossover + RSI confirmation strategy."""
    df = compute_indicators(df)

    signals = pd.Series(0.0, index=df.index)

    # EMA 12/26 for crossover (standard MACD-based)
    ema_fast = df["close"].ewm(span=12, adjust=False).mean()
    ema_slow = df["close"].ewm(span=26, adjust=False).mean()
    rsi = df.get("rsi_14", pd.Series(50.0, index=df.index))

    # EMA crossover
    cross_up = (ema_fast > ema_slow) & (ema_fast.shift(1) <= ema_slow.shift(1))
    cross_down = (ema_fast < ema_slow) & (ema_fast.shift(1) >= ema_slow.shift(1))

    # RSI confirmation
    signals[cross_up & (rsi < 70)] = 1.0
    signals[cross_down & (rsi > 30)] = -1.0

    # Hold signal until reversal
    position = 0.0
    hold_signals = signals.copy()
    for i in range(len(signals)):
        if signals.iloc[i] != 0:
            position = signals.iloc[i]
        hold_signals.iloc[i] = position

    return hold_signals


def buy_and_hold_benchmark(df: pd.DataFrame, initial_capital: float) -> dict[str, float]:
    """Simple buy-and-hold benchmark."""
    if len(df) < 2:
        return {"total_return": 0.0, "max_drawdown": 0.0, "sharpe_ratio": 0.0}

    returns = df["close"].pct_change().dropna()
    cumulative = (1 + returns).cumprod()
    peak = cumulative.cummax()
    drawdown = (peak - cumulative) / peak
    max_dd = float(drawdown.max())

    total_return = float(cumulative.iloc[-1] - 1)
    sharpe = float(returns.mean() / returns.std() * np.sqrt(252 * 24)) if returns.std() > 0 else 0.0

    return {
        "total_return": total_return,
        "max_drawdown": max_dd,
        "sharpe_ratio": sharpe,
    }


def sma_crossover_benchmark(df: pd.DataFrame, initial_capital: float) -> dict[str, float]:
    """Baseline SMA 20/50 crossover benchmark."""
    sma_fast = df["close"].rolling(20).mean()
    sma_slow = df["close"].rolling(50).mean()
    signals = pd.Series(0.0, index=df.index)
    signals[sma_fast > sma_slow] = 1.0
    signals[sma_fast <= sma_slow] = -1.0

    bt = FastBacktester(initial_capital=initial_capital)
    result = bt.run(df, signals, symbol="BTC/USDT-benchmark")
    return result.to_dict()


def generate_report(
    result: dict,
    bh_bench: dict,
    sma_bench: dict,
    mc_result: dict,
    wfo_results: list,
    symbol: str,
    timeframe: str,
    data_range: str,
    n_candles: int,
) -> str:
    """Generate a comprehensive backtest report."""

    report_lines = [
        "=" * 72,
        "  NEURAL-TRADER-5  |  BACKTEST REPORT",
        "=" * 72,
        f"  Symbol:     {symbol}",
        f"  Timeframe:  {timeframe}",
        f"  Data Range: {data_range}",
        f"  Candles:    {n_candles}",
        f"  Generated:  {datetime.now(timezone.utc).isoformat()}",
        "=" * 72,
        "",
        "── Strategy Performance ────────────────────────────────────────────────",
        f"  Total Return:     {result['total_return']:>10.2%}",
        f"  Sharpe Ratio:     {result['sharpe_ratio']:>10.2f}",
        f"  Sortino Ratio:    {result['sortino_ratio']:>10.2f}",
        f"  Max Drawdown:     {result['max_drawdown']:>10.2%}",
        f"  Win Rate:         {result['win_rate']:>10.2%}",
        f"  Profit Factor:    {result['profit_factor']:>10.2f}",
        f"  Total Trades:     {result['num_trades']:>10d}",
        f"  Avg PnL/Trade:    ${result['avg_pnl']:>9.2f}",
        "",
        "── Benchmark: Buy & Hold ──────────────────────────────────────────────",
        f"  Total Return:     {bh_bench['total_return']:>10.2%}",
        f"  Max Drawdown:     {bh_bench['max_drawdown']:>10.2%}",
        f"  Sharpe Ratio:     {bh_bench['sharpe_ratio']:>10.2f}",
        "",
        "── Benchmark: SMA 20/50 Crossover ─────────────────────────────────────",
        f"  Total Return:     {sma_bench['total_return']:>10.2%}",
        f"  Max Drawdown:     {sma_bench['max_drawdown']:>10.2%}",
        f"  Sharpe Ratio:     {sma_bench['sharpe_ratio']:>10.2f}",
        f"  Win Rate:         {sma_bench['win_rate']:>10.2%}",
        f"  Profit Factor:    {sma_bench['profit_factor']:>10.2f}",
        "",
        "── Monte Carlo Simulation ({} runs) ───────────────────────────────────".format(
            mc_result.get("runs", 0)
        ),
        f"  Probability of Ruin:   {mc_result.get('probability_of_ruin', 0):>8.2%}",
        f"  Ruin Threshold:        {mc_result.get('ruin_threshold_pct', 0):>8.2%}",
        f"  Median Final Equity:   ${mc_result.get('median_final_equity', 0):>12,.2f}",
        f"  5th Pctile Equity:     ${mc_result.get('p5_final_equity', 0):>12,.2f}",
        f"  95th Pctile Equity:    ${mc_result.get('p95_final_equity', 0):>12,.2f}",
        f"  Median Max Drawdown:   {mc_result.get('median_max_drawdown', 0):>8.2%}",
        f"  95th Pctile Drawdown:  {mc_result.get('p95_max_drawdown', 0):>8.2%}",
        "",
    ]

    if wfo_results:
        report_lines.append("── Walk-Forward Validation ─────────────────────────────────────────────")
        for i, wfo in enumerate(wfo_results):
            wfo_d = wfo.to_dict() if hasattr(wfo, "to_dict") else wfo
            report_lines.append(
                f"  Split {i+1}: return={wfo_d['total_return']:>8.2%}  "
                f"sharpe={wfo_d['sharpe_ratio']:>6.2f}  "
                f"dd={wfo_d['max_drawdown']:>8.2%}  "
                f"trades={wfo_d['num_trades']}"
            )
        report_lines.append("")

    # Warnings
    report_lines.append("── Warnings ────────────────────────────────────────────────────────────")
    warnings_found = False
    if result["sharpe_ratio"] < 0.5:
        report_lines.append("  ⚠ LOW Sharpe ratio — strategy may not be profitable after costs")
        warnings_found = True
    if result["max_drawdown"] > 0.20:
        report_lines.append("  ⚠ HIGH max drawdown — consider tighter risk controls")
        warnings_found = True
    if result["win_rate"] < 0.40:
        report_lines.append("  ⚠ LOW win rate — ensure profit factor compensates")
        warnings_found = True
    if result["num_trades"] < 30:
        report_lines.append("  ⚠ LOW trade count — results may not be statistically significant")
        warnings_found = True
    if mc_result.get("probability_of_ruin", 0) > 0.05:
        report_lines.append("  ⚠ RUIN probability > 5% — reduce position sizing")
        warnings_found = True
    if result["profit_factor"] < 1.0:
        report_lines.append("  ⚠ NEGATIVE expectancy — strategy loses money")
        warnings_found = True
    if not warnings_found:
        report_lines.append("  ✓ No critical warnings")

    report_lines.extend([
        "",
        "── Commands ────────────────────────────────────────────────────────────",
        "  Backtest: python scripts/run_backtest.py --months 6",
        "  Paper:    python main.py  (paper_mode=true in settings.yaml)",
        "  Live:     Set paper_mode=false, testnet=false, provide API keys",
        "",
        "=" * 72,
    ])

    return "\n".join(report_lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Neural Trader 5 — Backtest Runner")
    parser.add_argument("--symbol", default="BTC/USDT:USDT", help="Symbol to backtest")
    parser.add_argument("--timeframe", default="1h", help="Candle timeframe")
    parser.add_argument("--months", type=int, default=6, help="Months of historical data")
    parser.add_argument("--capital", type=float, default=100_000, help="Initial capital")
    parser.add_argument("--mc-runs", type=int, default=1000, help="Monte Carlo simulation runs")
    parser.add_argument("--wfo-splits", type=int, default=5, help="Walk-forward splits")
    parser.add_argument("--output", default="reports/backtest_report.txt", help="Output file")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True,
               format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")

    # 1. Fetch data
    df = fetch_ohlcv(args.symbol, args.timeframe, args.months)
    if df.empty:
        logger.error("No data — aborting")
        sys.exit(1)

    data_range = f"{df.index[0].strftime('%Y-%m-%d')} → {df.index[-1].strftime('%Y-%m-%d')}"

    # 2. Generate signals
    logger.info("Generating strategy signals...")
    signals = generate_strategy_signals(df)

    # 3. Run backtest
    logger.info("Running backtest...")
    bt = FastBacktester(
        initial_capital=args.capital,
        commission_pct=0.0004,
        slippage_pct=0.0002,
    )
    result = bt.run(df, signals, symbol=args.symbol)
    result_dict = result.to_dict()
    logger.info(
        "Strategy: return={:.2%} sharpe={:.2f} dd={:.2%} trades={} win={:.0%}",
        result.total_return, result.sharpe_ratio, result.max_drawdown,
        result.num_trades, result.win_rate,
    )

    # 4. Benchmarks
    logger.info("Computing benchmarks...")
    bh_bench = buy_and_hold_benchmark(df, args.capital)
    sma_bench = sma_crossover_benchmark(df, args.capital)
    logger.info("Buy&Hold: return={:.2%}  SMA20/50: return={:.2%}",
                bh_bench["total_return"], sma_bench["total_return"])

    # 5. Monte Carlo simulation
    logger.info("Running Monte Carlo ({} runs)...", args.mc_runs)
    mc = MonteCarloSimulator(n_runs=args.mc_runs)
    mc_result = mc.run(result.trades, initial_capital=args.capital)
    logger.info("P(Ruin)={:.2%}  Median equity=${:,.0f}",
                mc_result.get("probability_of_ruin", 0),
                mc_result.get("median_final_equity", 0))

    # 6. Walk-forward validation
    logger.info("Running walk-forward validation ({} splits)...", args.wfo_splits)
    wfo = WalkForwardOptimizer(bt, n_splits=args.wfo_splits)

    def signal_fn(train_df: pd.DataFrame, test_df: pd.DataFrame) -> pd.Series:
        return generate_strategy_signals(test_df)

    wfo_results = wfo.run(df, signal_fn, symbol=args.symbol)

    # 7. Generate report
    report = generate_report(
        result=result_dict,
        bh_bench=bh_bench,
        sma_bench=sma_bench,
        mc_result=mc_result,
        wfo_results=wfo_results,
        symbol=args.symbol,
        timeframe=args.timeframe,
        data_range=data_range,
        n_candles=len(df),
    )

    # Save report
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report)
    logger.info("Report saved to {}", output_path)

    # Also save JSON metrics
    json_path = output_path.with_suffix(".json")
    json_data = {
        "strategy": result_dict,
        "benchmark_buy_hold": bh_bench,
        "benchmark_sma_crossover": sma_bench,
        "monte_carlo": mc_result,
        "walk_forward": [
            r.to_dict() if hasattr(r, "to_dict") else r
            for r in wfo_results
        ],
        "metadata": {
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "months": args.months,
            "capital": args.capital,
            "candles": len(df),
            "data_range": data_range,
            "generated": datetime.now(timezone.utc).isoformat(),
        },
    }
    json_path.write_text(json.dumps(json_data, indent=2, default=str))
    logger.info("JSON metrics saved to {}", json_path)

    # Print report to stdout
    print(report)


if __name__ == "__main__":
    main()
