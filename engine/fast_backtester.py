from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

try:
    import neural_trader_rust as _rust
    _RUST_AVAILABLE = True
except ImportError:
    _RUST_AVAILABLE = False


@dataclass
class BacktestTrade:
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    entry_time: int
    exit_time: int
    pnl: float
    pnl_pct: float
    hold_periods: int


@dataclass
class BacktestResult:
    total_return: float
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown: float
    win_rate: float
    profit_factor: float
    num_trades: int
    avg_pnl: float
    trades: list[BacktestTrade] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_return": self.total_return,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "max_drawdown": self.max_drawdown,
            "win_rate": self.win_rate,
            "profit_factor": self.profit_factor,
            "num_trades": self.num_trades,
            "avg_pnl": self.avg_pnl,
        }


class FastBacktester:
    """Backtester with optional Rust-accelerated inner loop."""

    def __init__(
        self,
        initial_capital: float = 100_000,
        commission_pct: float = 0.0004,
        slippage_pct: float = 0.0002,
    ) -> None:
        self._capital = initial_capital
        self._commission = commission_pct
        self._slippage = slippage_pct

    def run(
        self,
        df: pd.DataFrame,
        signals: pd.Series,
        symbol: str = "UNKNOWN",
    ) -> BacktestResult:
        if _RUST_AVAILABLE:
            try:
                return self._run_rust(df, signals, symbol)
            except Exception as exc:
                logger.debug("Rust backtester error (falling back): {}", exc)
        return self._run_python(df, signals, symbol)

    def _run_python(
        self,
        df: pd.DataFrame,
        signals: pd.Series,
        symbol: str,
    ) -> BacktestResult:
        equity = self._capital
        peak_equity = equity
        max_dd = 0.0
        trades: list[BacktestTrade] = []
        position: dict[str, Any] | None = None

        close_prices = df["close"].values
        signal_values = signals.reindex(df.index).fillna(0).values
        timestamps = df.index.astype(np.int64) // 10**9 if hasattr(df.index, "astype") else list(range(len(df)))

        for i, (price, sig, ts) in enumerate(zip(close_prices, signal_values, timestamps)):
            if position is not None:
                should_exit = False
                if position["direction"] == "long" and sig < 0:
                    should_exit = True
                elif position["direction"] == "short" and sig > 0:
                    should_exit = True

                if should_exit:
                    exit_price = price * (1 - self._slippage if position["direction"] == "long" else 1 + self._slippage)
                    entry_price = position["entry_price"]
                    if position["direction"] == "long":
                        pnl_pct = (exit_price - entry_price) / entry_price
                    else:
                        pnl_pct = (entry_price - exit_price) / entry_price
                    pnl_pct -= 2 * self._commission
                    trade_pnl = equity * position["size_pct"] * pnl_pct
                    equity += trade_pnl
                    peak_equity = max(peak_equity, equity)
                    dd = (peak_equity - equity) / peak_equity
                    max_dd = max(max_dd, dd)

                    trades.append(BacktestTrade(
                        symbol=symbol,
                        direction=position["direction"],
                        entry_price=entry_price,
                        exit_price=exit_price,
                        entry_time=int(position["entry_time"]),
                        exit_time=int(ts),
                        pnl=trade_pnl,
                        pnl_pct=pnl_pct,
                        hold_periods=i - position["entry_idx"],
                    ))
                    position = None

            if position is None and abs(sig) > 0.5:
                direction = "long" if sig > 0 else "short"
                entry_price = price * (1 + self._slippage if direction == "long" else 1 - self._slippage)
                position = {
                    "direction": direction,
                    "entry_price": entry_price,
                    "entry_time": ts,
                    "entry_idx": i,
                    "size_pct": 0.02,
                }

        if not trades:
            return BacktestResult(0, 0, 0, 0, 0, 0, 0, 0)

        total_return = (equity - self._capital) / self._capital
        pnls = [t.pnl for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        win_rate = len(wins) / len(pnls)
        profit_factor = abs(sum(wins)) / abs(sum(losses)) if losses else float("inf")

        daily_returns = pd.Series(pnls) / self._capital
        sharpe = float(daily_returns.mean() / daily_returns.std() * np.sqrt(252)) if daily_returns.std() > 0 else 0.0
        downside = daily_returns[daily_returns < 0].std()
        sortino = float(daily_returns.mean() / downside * np.sqrt(252)) if downside > 0 else 0.0

        return BacktestResult(
            total_return=total_return,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            max_drawdown=max_dd,
            win_rate=win_rate,
            profit_factor=profit_factor,
            num_trades=len(trades),
            avg_pnl=float(np.mean(pnls)),
            trades=trades,
        )

    def _run_rust(self, df: pd.DataFrame, signals: pd.Series, symbol: str) -> BacktestResult:
        prices = df["close"].tolist()
        sigs = signals.reindex(df.index).fillna(0).tolist()
        result = _rust.FastBacktester.run(
            prices=prices,
            signals=sigs,
            initial_capital=self._capital,
            commission_pct=self._commission,
            slippage_pct=self._slippage,
        )
        return BacktestResult(
            total_return=result["total_return"],
            sharpe_ratio=result["sharpe_ratio"],
            sortino_ratio=result["sortino_ratio"],
            max_drawdown=result["max_drawdown"],
            win_rate=result["win_rate"],
            profit_factor=result["profit_factor"],
            num_trades=result["num_trades"],
            avg_pnl=result["avg_pnl"],
        )


class WalkForwardOptimizer:
    def __init__(
        self,
        backtester: FastBacktester,
        n_splits: int = 5,
        train_pct: float = 0.7,
    ) -> None:
        self._bt = backtester
        self._n_splits = n_splits
        self._train_pct = train_pct

    def run(
        self,
        df: pd.DataFrame,
        signal_fn: Any,
        symbol: str = "UNKNOWN",
    ) -> list[BacktestResult]:
        total_len = len(df)
        split_size = total_len // self._n_splits
        results = []

        for i in range(self._n_splits):
            start = i * split_size
            end = (i + 1) * split_size if i < self._n_splits - 1 else total_len
            split_df = df.iloc[start:end].copy()
            train_len = int(len(split_df) * self._train_pct)
            test_df = split_df.iloc[train_len:]

            if len(test_df) < 5:
                continue

            try:
                signals = signal_fn(split_df.iloc[:train_len], test_df)
                result = self._bt.run(test_df, signals, symbol=symbol)
                results.append(result)
                logger.debug(
                    "WFO split {}/{}: return={:.2%} sharpe={:.2f}",
                    i + 1, self._n_splits, result.total_return, result.sharpe_ratio
                )
            except Exception as exc:
                logger.warning("WFO split {} error: {}", i + 1, exc)

        return results


class MonteCarloSimulator:
    """Monte Carlo simulation over trade sequences to estimate probability of ruin."""

    def __init__(self, n_runs: int = 1000, ruin_threshold_pct: float = 0.50) -> None:
        self._n_runs = n_runs
        self._ruin_threshold = ruin_threshold_pct

    def run(self, trades: list[BacktestTrade], initial_capital: float = 100_000) -> dict[str, Any]:
        if not trades:
            return {"error": "no_trades", "runs": 0}

        pnl_pcts = np.array([t.pnl_pct for t in trades])
        n_trades = len(pnl_pcts)
        ruin_count = 0
        final_equities: list[float] = []
        max_drawdowns: list[float] = []
        rng = np.random.default_rng(42)

        for _ in range(self._n_runs):
            # Shuffle trade order
            shuffled = rng.permutation(pnl_pcts)
            equity = initial_capital
            peak = equity
            max_dd = 0.0

            for pnl_pct in shuffled:
                equity *= (1.0 + pnl_pct)
                if equity > peak:
                    peak = equity
                dd = (peak - equity) / peak if peak > 0 else 0.0
                max_dd = max(max_dd, dd)

                if dd >= self._ruin_threshold:
                    ruin_count += 1
                    break

            final_equities.append(equity)
            max_drawdowns.append(max_dd)

        final_arr = np.array(final_equities)
        dd_arr = np.array(max_drawdowns)

        return {
            "runs": self._n_runs,
            "trades_per_run": n_trades,
            "probability_of_ruin": ruin_count / self._n_runs,
            "ruin_threshold_pct": self._ruin_threshold,
            "ruin_count": ruin_count,
            "median_final_equity": float(np.median(final_arr)),
            "mean_final_equity": float(np.mean(final_arr)),
            "p5_final_equity": float(np.percentile(final_arr, 5)),
            "p95_final_equity": float(np.percentile(final_arr, 95)),
            "median_max_drawdown": float(np.median(dd_arr)),
            "p95_max_drawdown": float(np.percentile(dd_arr, 95)),
            "mean_max_drawdown": float(np.mean(dd_arr)),
        }
