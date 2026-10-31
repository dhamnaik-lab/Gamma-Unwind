"""CLI: `python -m gammad <levels|universe|zones|watchlists|lists|calendar|serve|tray> ...`. Logs: stderr + file."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date

from gammad import __version__
from gammad.log import get_logger, setup_logging

log = get_logger("gammad.cli")


def _date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {s!r}") from exc


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="gammad", description="GAMMA-UNWIND daemon tools")
    p.add_argument("--version", action="version", version=f"gammad {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)

    lv = sub.add_parser("levels", help="mark support/resistance for the F&O universe (Step 1)")
    lv.add_argument("--date", type=_date, required=True)
    lv.add_argument("--symbols", default=None, help="comma-separated subset, e.g. RELIANCE,TCS")
    lv.add_argument("--ref", choices=("ltp", "close", "prevclose"), default="close")

    sub.add_parser("universe", help="list the F&O stock universe")

    zn = sub.add_parser("zones", help="show live zones and levels for one symbol")
    zn.add_argument("symbol")
    zn.add_argument("--date", type=_date, required=True)

    wl = sub.add_parser("watchlists", help="build the OTM watchlists from today's levels (Step 2)")
    wl.add_argument("--date", type=_date, required=True)
    wl.add_argument("--kind", choices=("0910", "0920", "refresh", "1315", "manual"), default="manual")
    wl.add_argument("--symbols", default=None, help="comma-separated subset, e.g. RELIANCE,TCS")
    wl.add_argument("--ref", choices=("ltp", "close", "prevclose"), default="ltp", help="which mark_levels run to read")

    ls = sub.add_parser("lists", help="show the D5 lists (list, side, expiry) for a date")
    ls.add_argument("--date", type=_date, required=True)
    ls.add_argument("--expiries", type=_dates, default=None, help="offline, e.g. 2026-09-29,2026-10-27")
    ls.add_argument("--symbol", default=None, help="which cached underlying to read (default: any)")

    cl = sub.add_parser("calendar", help="NSE trading-day facts for a date, and DTE to an expiry")
    cl.add_argument("--date", type=_date, required=True)
    cl.add_argument("--expiry", type=_date, default=None)

    sv = sub.add_parser("serve", help="run the supervisor: scheduler, live feed, alerts, dashboard API, tray")
    sv.add_argument("--no-tray", action="store_true", help="no system-tray icon (headless / console)")

    sub.add_parser("tray", help="tray supervisor (D66): runs `serve --no-tray` as a child; Start/Stop, Quit")
    return p


def _dates(s: str) -> list[date]:
    return [_date(x.strip()) for x in s.split(",") if x.strip()]


def _cmd_levels(args: argparse.Namespace) -> int:
    from gammad.jobs import mark_levels

    symbols = [s for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    summary = mark_levels.run(args.date, ref=args.ref, symbols=symbols)
    log.info("levels summary: %s", summary)
    return 0 if summary.get("status") in ("ok", "partial") else 1


def _cmd_universe() -> int:
    from gammad.broker.dhan import DhanAdapter
    from gammad.jobs.mark_levels import load_universe

    rows = load_universe(DhanAdapter.from_settings())
    for r in rows:
        log.info("%-14s eq_id=%-8d lot=%d", r.symbol, r.eq_security_id, r.lot_size)
    log.info("universe count: %d", len(rows))
    return 0


def _cmd_zones(args: argparse.Namespace) -> int:
    from gammad.broker.dhan import DhanAdapter, candle_date
    from gammad.config import settings
    from gammad.core.calendar import is_trading_day
    from gammad.core.day_bar import with_day_bar
    from gammad.core.levels import pick_levels_daily  # T-309: the stored levels' rule
    from gammad.core.zones import ZoneParams, compute_zones
    from gammad.jobs.mark_levels import fetch_bars

    cfg = settings()
    adapter = DhanAdapter.from_settings()
    sym = args.symbol.strip().upper()
    sid = adapter.scrip_master().eq_security_id(sym)
    if sid is None:
        log.error("zones: no NSE_EQ security id for %s", sym)
        return 2
    bars = fetch_bars(adapter, sid, args.date, "close", cfg.zones.daily_bars)
    if is_trading_day(args.date):
        # T-221: the same late-daily-candle completion as mark_levels(ref=close)
        bars, how = with_day_bar(adapter, sid, bars, args.date)
        bars = bars[-cfg.zones.daily_bars :]
        log.info("zones: %s day bar = %s", args.date, how)
    if not bars:
        log.error("zones: no candles for %s up to %s", sym, args.date)
        return 2
    state = compute_zones(bars, ZoneParams.from_config(cfg.zones))
    log.info(
        "%s bars=%d (%s..%s) atr=%.4f", sym, len(bars), candle_date(bars[0]), candle_date(bars[-1]), state.atr_last
    )
    for z in state.live_sell:
        log.info("  SELL %.2f-%.2f formed=%s", z.bottom, z.top, candle_date(bars[z.formed_bar_index]))
    for z in state.live_buy:
        log.info("  BUY  %.2f-%.2f formed=%s", z.bottom, z.top, candle_date(bars[z.formed_bar_index]))
    lv = pick_levels_daily(state, bars[-1].close)
    log.info(
        "  ref=%.2f support=%s (%.2f%%) resistance=%s (%.2f%%)",
        lv.ref_price,
        f"{lv.support.bottom:.2f}-{lv.support.top:.2f}" if lv.support else "none",
        lv.dist_support_pct or 0.0,
        f"{lv.resistance.bottom:.2f}-{lv.resistance.top:.2f}" if lv.resistance else "none",
        lv.dist_resistance_pct or 0.0,
    )
    return 0


def _cmd_watchlists(args: argparse.Namespace) -> int:
    from gammad.jobs import build_watchlists

    symbols = [s for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    try:
        summary = build_watchlists.run(args.date, args.kind, ref=args.ref, symbols=symbols)
    except build_watchlists.NoLevels as exc:
        log.error("watchlists: %s (run `levels --date %s --ref %s` first)", exc, args.date, args.ref)
        return 1
    log.info("watchlists summary: %s", summary)
    return 0 if summary.get("status") in ("ok", "partial") else 1


def _cmd_lists(args: argparse.Namespace) -> int:
    from gammad.config import settings
    from gammad.core.expiry import current_expiry, dte, lists_for
    from gammad.jobs.build_watchlists import expiry_cache_path, load_expiry_cache

    exps: list[date] | None = args.expiries
    if exps is None:
        path = expiry_cache_path(settings().paths.data_dir, args.date)
        cache = load_expiry_cache(path)
        if args.symbol:
            entry = cache.get(args.symbol.strip().upper())
            exps = entry[1] if entry else None
        else:
            distinct = {tuple(e) for _, e in cache.values()}
            if len(distinct) > 1:
                log.error("lists: %d expiry sets cached in %s; pass --symbol", len(distinct), path.name)
                return 2
            exps = list(next(iter(distinct))) if distinct else None
        if not exps:
            log.error("lists: no cached expiries for %s (%s); pass --expiries", args.date, path.name)
            return 2
    specs = lists_for(args.date, exps)
    cur = current_expiry(exps, args.date)
    log.info(
        "lists for %s (expiries %s): current %s DTE=%d",
        args.date,
        ", ".join(e.isoformat() for e in sorted(exps)),
        cur,
        dte(args.date, cur),
    )
    for s in specs:
        log.info("  %-13s %s %s %s", s.list_name, s.side, s.expiry_date, s.series)
    return 0


def _cmd_calendar(args: argparse.Namespace) -> int:
    from gammad.core import calendar as cal
    from gammad.core.expiry import dte

    d: date = args.date
    log.info(
        "%s trading_day=%s prev=%s next=%s",
        d,
        cal.is_trading_day(d),
        cal.prev_trading_day(d),
        cal.next_trading_day(d),
    )
    if args.expiry is not None:
        log.info("DTE %s -> %s = %d", d, args.expiry, dte(d, args.expiry))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    from gammad.broker.base import BrokerAuthError

    args = build_parser().parse_args(argv)
    if args.cmd in ("tray", "serve"):  # T-198 (D165(3)): the live service never starts from the development tree
        from gammad import REPO_ROOT
        from gammad.edition import EDITION
        from gammad.install import start_refusal

        refusal = start_refusal(REPO_ROOT, EDITION)
        if refusal is not None:
            raise SystemExit(refusal)
    if args.cmd == "tray":  # logs to logs/tray.log: two processes rotating one gammad.log fail on Windows
        from gammad.tray.supervisor import main as tray_main

        return tray_main()
    setup_logging()
    try:
        if args.cmd == "levels":
            return _cmd_levels(args)
        if args.cmd == "universe":
            return _cmd_universe()
        if args.cmd == "zones":
            return _cmd_zones(args)
        if args.cmd == "watchlists":
            return _cmd_watchlists(args)
        if args.cmd == "lists":
            return _cmd_lists(args)
        if args.cmd == "calendar":
            return _cmd_calendar(args)
        if args.cmd == "serve":
            from gammad.app import serve

            return serve(tray=not args.no_tray)
    except BrokerAuthError as exc:
        log.error("broker auth failed: %s", exc)
        return 3
    except LookupError as exc:  # CalendarYearMissing / ExpiryMissing: loud, never a guess
        log.error("%s: %s: %s", args.cmd, type(exc).__name__, exc)
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())
