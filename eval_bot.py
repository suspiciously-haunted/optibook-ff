"""
Evaluation bot — live PnL / Sharpe / drawdown tracker for Optibook traders.

Two ways to use it
------------------
1. Wrap any trader (scrapes PnL lines from stdout, prints live metrics):

       python eval_bot.py delta_future_3.py
       python eval_bot.py combined.py
       python eval_bot.py --report-every 15 delta_future_2.py

2. Import inside a trading script:

       from eval_bot import EvalBot
       ev = EvalBot(report_every=30.0)
       ev.record(exchange.get_pnl())
       ev.maybe_report()          # throttled log line
       print(ev.format_summary()) # final report

3. Score a finished run CSV (combined.py pospnl.csv):

       python eval_bot.py --csv export_merged_v4/run_MMDD_HHMMSS/pospnl.csv
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Optional


# Match common PnL log formats from our traders.
PNL_PATTERNS = [
    re.compile(r"Current PnL:\s*([+-]?\d+(?:\.\d+)?)", re.I),
    re.compile(r"\bpnl\s*=\s*([+-]?\d+(?:\.\d+)?)", re.I),
    re.compile(r"\bPnL:\s*([+-]?\d+(?:\.\d+)?)", re.I),
]


@dataclass
class Metrics:
    n_samples: int = 0
    elapsed_s: float = 0.0
    start_pnl: float = 0.0
    current_pnl: float = 0.0
    increase: float = 0.0          # current - start
    pct_increase: Optional[float] = None  # vs |start| when start != 0
    peak_pnl: float = 0.0
    trough_pnl: float = 0.0
    max_drawdown: float = 0.0      # peak - trough after that peak (cash)
    max_drawdown_pct: Optional[float] = None
    pnl_per_min: float = 0.0
    pnl_per_hour: float = 0.0
    volatility: float = 0.0        # std of period ΔPnL
    mean_period_pnl: float = 0.0
    sharpe: Optional[float] = None           # mean/std of ΔPnL (sample)
    sharpe_per_hour: Optional[float] = None  # annualized to 1 hour of samples
    win_rate: Optional[float] = None         # fraction of positive ΔPnL periods
    avg_sample_interval_s: float = 0.0


@dataclass
class EvalBot:
    """Accumulate PnL samples and compute performance metrics."""

    report_every: float = 30.0
    out_dir: Optional[str] = None
    label: str = "eval"
    min_return_samples: int = 5
    _t0: float = field(default_factory=time.time, init=False)
    _times: list[float] = field(default_factory=list, init=False)
    _pnls: list[float] = field(default_factory=list, init=False)
    _last_report: float = field(default=0.0, init=False)
    _csv_path: Optional[str] = field(default=None, init=False)
    _csv_file: Optional[object] = field(default=None, init=False)
    _csv_writer: Optional[csv.writer] = field(default=None, init=False)

    def __post_init__(self):
        if self.out_dir is None:
            stamp = datetime.now().strftime("%m%d_%H%M%S")
            self.out_dir = os.path.join("eval_runs", f"{self.label}_{stamp}")
        os.makedirs(self.out_dir, exist_ok=True)
        self._csv_path = os.path.join(self.out_dir, "pnl_samples.csv")
        self._csv_file = open(self._csv_path, "w", newline="")
        self._csv_writer = csv.writer(self._csv_file)
        self._csv_writer.writerow(["t", "elapsed_s", "pnl", "increase", "drawdown"])

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record(self, pnl: float, t: Optional[float] = None) -> Metrics:
        now = time.time() if t is None else t
        if not self._pnls:
            self._t0 = now
        self._times.append(now)
        self._pnls.append(float(pnl))

        m = self.compute()
        if self._csv_writer is not None:
            self._csv_writer.writerow([
                f"{now:.3f}",
                f"{m.elapsed_s:.3f}",
                f"{m.current_pnl:.4f}",
                f"{m.increase:.4f}",
                f"{m.max_drawdown:.4f}",
            ])
            self._csv_file.flush()
        return m

    def record_from_line(self, line: str) -> Optional[Metrics]:
        pnl = parse_pnl_line(line)
        if pnl is None:
            return None
        return self.record(pnl)

    def close(self):
        if self._csv_file is not None:
            try:
                self._csv_file.close()
            except Exception:
                pass
            self._csv_file = None

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def compute(self) -> Metrics:
        if not self._pnls:
            return Metrics()

        times = self._times
        pnls = self._pnls
        n = len(pnls)
        elapsed = max(times[-1] - self._t0, 1e-9)
        start = pnls[0]
        current = pnls[-1]
        increase = current - start
        pct = (increase / abs(start) * 100.0) if start != 0 else None

        peak = pnls[0]
        max_dd = 0.0
        trough_after_peak = pnls[0]
        for p in pnls:
            if p > peak:
                peak = p
                trough_after_peak = p
            dd = peak - p
            if dd > max_dd:
                max_dd = dd
                trough_after_peak = p

        max_dd_pct = (max_dd / abs(peak) * 100.0) if peak != 0 else None

        deltas = [pnls[i] - pnls[i - 1] for i in range(1, n)]
        mean_d = statistics.fmean(deltas) if deltas else 0.0
        vol = statistics.pstdev(deltas) if len(deltas) >= 2 else 0.0
        win_rate = (
            sum(1 for d in deltas if d > 0) / len(deltas) if deltas else None
        )

        avg_dt = elapsed / max(n - 1, 1)
        sharpe = None
        sharpe_h = None
        if len(deltas) >= self.min_return_samples and vol > 1e-12:
            sharpe = mean_d / vol
            # Scale sample Sharpe to a 1-hour horizon (not calendar-year —
            # Optibook eval windows are ~30 minutes).
            periods_per_hour = 3600.0 / max(avg_dt, 1e-9)
            sharpe_h = sharpe * math.sqrt(periods_per_hour)

        return Metrics(
            n_samples=n,
            elapsed_s=elapsed,
            start_pnl=start,
            current_pnl=current,
            increase=increase,
            pct_increase=pct,
            peak_pnl=peak,
            trough_pnl=min(pnls),
            max_drawdown=max_dd,
            max_drawdown_pct=max_dd_pct,
            pnl_per_min=increase / (elapsed / 60.0),
            pnl_per_hour=increase / (elapsed / 3600.0),
            volatility=vol,
            mean_period_pnl=mean_d,
            sharpe=sharpe,
            sharpe_per_hour=sharpe_h,
            win_rate=win_rate,
            avg_sample_interval_s=avg_dt,
        )

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def maybe_report(self, force: bool = False) -> Optional[str]:
        now = time.time()
        if not force and (now - self._last_report) < self.report_every:
            return None
        if len(self._pnls) < 1:
            return None
        self._last_report = now
        line = self.format_summary(short=True)
        print(line, flush=True)
        return line

    def format_summary(self, short: bool = False) -> str:
        m = self.compute()
        if m.n_samples == 0:
            return "[EVAL] no samples yet"

        sharpe_s = f"{m.sharpe:.3f}" if m.sharpe is not None else "n/a"
        sharpe_h = f"{m.sharpe_per_hour:.2f}" if m.sharpe_per_hour is not None else "n/a"
        pct_s = f"{m.pct_increase:+.2f}%" if m.pct_increase is not None else "n/a"
        wr_s = f"{m.win_rate * 100:.0f}%" if m.win_rate is not None else "n/a"
        mins = m.elapsed_s / 60.0

        if short:
            return (
                f"[EVAL] pnl={m.current_pnl:+.2f}  Δ={m.increase:+.2f} ({pct_s})  "
                f"peak={m.peak_pnl:+.2f}  dd={m.max_drawdown:.2f}  "
                f"sharpe={sharpe_s} (1h={sharpe_h})  "
                f"rate={m.pnl_per_min:+.2f}/min  "
                f"win={wr_s}  n={m.n_samples}  t={mins:.1f}m"
            )

        lines = [
            "=" * 60,
            f" EVAL SUMMARY  ({self.label})",
            "=" * 60,
            f"  samples          {m.n_samples}",
            f"  elapsed          {mins:.2f} min  ({m.elapsed_s:.1f}s)",
            f"  start PnL        {m.start_pnl:+.2f}",
            f"  current PnL      {m.current_pnl:+.2f}",
            f"  increase (Δ)     {m.increase:+.2f}  ({pct_s})",
            f"  peak PnL         {m.peak_pnl:+.2f}",
            f"  trough PnL       {m.trough_pnl:+.2f}",
            f"  max drawdown     {m.max_drawdown:.2f}"
            + (f"  ({m.max_drawdown_pct:.2f}% of peak)" if m.max_drawdown_pct is not None else ""),
            f"  PnL / min        {m.pnl_per_min:+.2f}",
            f"  PnL / hour       {m.pnl_per_hour:+.2f}",
            f"  mean Δ / sample  {m.mean_period_pnl:+.4f}",
            f"  vol (σ Δ)        {m.volatility:.4f}",
            f"  Sharpe (sample)  {sharpe_s}",
            f"  Sharpe (1h)      {sharpe_h}",
            f"  win rate         {wr_s}",
            f"  avg interval     {m.avg_sample_interval_s:.2f}s",
            f"  samples CSV      {self._csv_path}",
            "=" * 60,
        ]
        return "\n".join(lines)

    def write_summary(self, path: Optional[str] = None) -> str:
        path = path or os.path.join(self.out_dir, "summary.txt")
        text = self.format_summary(short=False)
        with open(path, "w") as f:
            f.write(text + "\n")
        return path


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def parse_pnl_line(line: str) -> Optional[float]:
    for pat in PNL_PATTERNS:
        m = pat.search(line)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return None
    return None


def load_pospnl_csv(path: str) -> EvalBot:
    """Load combined.py-style pospnl.csv (columns: t, pnl, ...)."""
    ev = EvalBot(label=os.path.basename(os.path.dirname(path)) or "csv", report_every=1e9)
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                t = float(row["t"])
                pnl = float(row["pnl"])
            except (KeyError, ValueError):
                continue
            ev.record(pnl, t=t)
    return ev


def run_wrapped(script_and_args: list[str], report_every: float, label: str) -> int:
    """Run another python script, scrape PnL from its stdout, report live."""
    if not script_and_args:
        raise SystemExit("Pass a trader script, e.g. python eval_bot.py delta_future_3.py")

    script = script_and_args[0]
    if not os.path.isfile(script):
        raise SystemExit(f"Script not found: {script}")

    cmd = [sys.executable, "-u", *script_and_args]
    ev = EvalBot(report_every=report_every, label=label or os.path.splitext(os.path.basename(script))[0])
    print(f"[EVAL] wrapping: {' '.join(cmd)}", flush=True)
    print(f"[EVAL] writing samples to {ev._csv_path}", flush=True)
    print(f"[EVAL] report every {report_every:.0f}s", flush=True)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    rc = 1
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            if ev.record_from_line(line) is not None:
                ev.maybe_report()
        rc = proc.wait()
    except KeyboardInterrupt:
        print("\n[EVAL] interrupted — stopping trader...", flush=True)
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        rc = 130
    finally:
        summary = ev.format_summary(short=False)
        print(summary, flush=True)
        path = ev.write_summary()
        print(f"[EVAL] summary written to {path}", flush=True)
        ev.close()
    return rc


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate a running Optibook trader (PnL, Sharpe, drawdown, increase).",
    )
    parser.add_argument(
        "script",
        nargs="*",
        help="Trader script + args to wrap, e.g. delta_future_3.py",
    )
    parser.add_argument(
        "--csv",
        dest="csv_path",
        help="Score an existing pospnl.csv instead of wrapping a script",
    )
    parser.add_argument(
        "--report-every",
        type=float,
        default=30.0,
        help="Seconds between live EVAL lines (default: 30)",
    )
    parser.add_argument(
        "--label",
        default="",
        help="Name for this eval run folder",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.csv_path:
        ev = load_pospnl_csv(args.csv_path)
        print(ev.format_summary(short=False))
        path = ev.write_summary()
        print(f"[EVAL] summary written to {path}")
        ev.close()
        return 0

    if not args.script:
        parser.print_help()
        return 2

    return run_wrapped(args.script, report_every=args.report_every, label=args.label)


if __name__ == "__main__":
    raise SystemExit(main())
