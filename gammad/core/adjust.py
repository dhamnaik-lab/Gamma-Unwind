"""Corporate-action adjustment of daily bars (T-311 prep, CX-28): split-only (the primary basis, D462) and
dividend-adjusted modes over nse-corp-actions/1 rows. Not wired into any consumer yet.

Provenance (D363): the gap-test helpers are ported byte-identical from the sibling project's bot/sentinel/core/adjust.py
(sha256 a6412f3d0a8f0f503d4f0f794a1f347cfc94b4868b5abfc05b6fd4ad3151d5dc); parse_dividend is ported byte-identical from
docs/research/2026-10-14-zone-exits/scripts/zx_data.py (blob 348b02807a01).
"""
from __future__ import annotations

import bisect
import itertools
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

#: A gap wider than this is a corporate action, not a price move.
SPLIT_UP, SPLIT_DOWN = 1.35, 0.74

#: NSE's widest ordinary band, plus a little slack for the ex-date print.
BAND = 0.205

#: The sanity check for a payout, standing in the same place `snap_ratio`'s
#: band test stands for a ratio: a "dividend" past this fraction of the cum
#: price is far likelier to be a number read out of the wrong field than a
#: real one. Three weeks of the live feed put the largest genuine payout at
#: 4.18% of price (CHENNPETRO, Rs 54 against 1292.30) and PTC's 11.31% is
#: the extreme an interim reaches, so 25% never touches a real payout — but
#: it does refuse the failure this guard exists for, a subscription price or
#: a face value read as cash (SHANTIGOLD's "Premium Rs 205" against a
#: 216.95 close is 94% of it).
MAX_PAYOUT = 0.25


#: "Rs 23", "Re 0.45", "Rs1.50", "Rs  3" — NSE's own spacing is not stable.
_AMOUNT = re.compile(r"(?:Rs|Re|INR|₹)\s*\.?\s*([0-9][0-9,]*(?:\.[0-9]+)?)",
                     re.IGNORECASE)

#: The payout must be quoted per SHARE. An InvIT or REIT quotes per unit and
#: blends interest, dividend and capital repayment into one figure, which is
#: not a number you can subtract from an equity's close.
_PER_SHARE = re.compile(r"per\s+(?:equity\s+)?sh", re.IGNORECASE)

_RATIOS = [1.2, 1.25, 4 / 3, 1.5, 5 / 3, 2, 2.5, 3, 3.5, 4, 5, 6, 7,
           7.5, 8, 9, 10, 11, 15, 20, 25, 50, 100]


def snap_ratio(f: float, *, prev_close: float = 0.0, high: float = 0.0,
               low: float = 0.0) -> float:
    """Nearest standard corporate-action ratio that is physically possible.

    When the ex-date bar's own high/low are supplied, a candidate is only
    accepted if it puts that bar back inside the circuit band. Without
    that test an ambiguous measurement silently mis-scales every earlier
    bar by 20% (NARMADA: 5/3 chosen where the truth was 2.0).
    """
    inv = f < 1
    x = 1 / f if inv else f

    def ok(cand: float) -> bool:
        if not (prev_close and high and low):
            return True
        base = prev_close / cand if not inv else prev_close * cand
        if base <= 0:
            return False
        return (low / base - 1) >= -BAND and (high / base - 1) <= BAND

    # Distance normalised by the MEASUREMENT, not the candidate, so the
    # choice is not biased toward small ratios.
    for cand in sorted(_RATIOS, key=lambda r: abs(r - x) / x):
        if abs(cand - x) / x > 0.12:
            break
        if ok(cand):
            x = cand
            break
    return 1 / x if inv else x


@dataclass(frozen=True)
class ExAction:
    """One corporate action whose ex-date falls on `ex_date`.

    `amount` is None whenever the payout could not be read with certainty.
    That is emphatically not the same as "no action here": the ex-date is
    still known, and a caller that cannot adjust must still be able to say
    the move spans one.
    """
    symbol: str
    ex_date: str
    label: str
    subject: str
    amount: float | None = None

    @property
    def is_cash(self) -> bool:
        return self.amount is not None


def parse_payout(label: str, subject: str) -> float | None:
    """Cash per share from a corporate-actions subject, or None if unsure.

    Every rejection below is a row the live feed actually produced in the
    three weeks of history this was written against:

      * label is not Dividend — "Rights 19:295 @ Premium Rs 205/-" carries a
        rupee figure that is a subscription price, and subtracting it from
        SHANTIGOLD would have knocked 94% off the stock.
      * subject ends in the truncation mark — it is stored clipped to 50
        characters, so "Dividend - Rs 525 Per Share & Special Dividend Rs…"
        is missing part of its own total and the visible part is not it.
      * two amounts — a composite payout whose parts would have to be summed,
        and summing a list you can only half see is how a number gets
        invented.
      * no "per share" — an InvIT distribution is quoted per unit.

    163 of the 183 dividend rows in that history parse; the 20 that do not
    are all genuinely ambiguous, which is the point.
    """
    if label != "Dividend":
        return None
    subject = subject.strip()
    if subject.endswith("…"):
        return None
    if not _PER_SHARE.search(subject):
        return None
    found = _AMOUNT.findall(subject)
    if len(found) != 1:
        return None
    amount = _f(found[0].replace(",", ""))
    return amount if amount > 0 else None


def guarded_pct(prev_close, close, *, action: ExAction | None = None,
                open_=0.0, high=0.0, low=0.0) -> tuple[float | None, bool]:
    """One session's % move with the ex-date drop taken out. -> (pct, adjusted).

    The two corrections run in sequence rather than as alternatives, because
    a stock can go ex on a payout and a split on the same date and each is
    blind to the other: the payout is measured data NSE published, the ratio
    is inferred from a gap that a payout is far too small to open.

    `adjusted` says the number was corrected. It does NOT say whether an
    action existed — the caller holds `action` and knows that already, and
    the pair it cares about is the awkward one: an action with adjusted
    False, meaning a raw figure it must present as spanning an ex-date
    rather than as a market move.
    """
    prev_close, close = _f(prev_close), _f(close)
    if prev_close <= 0 or close <= 0:
        return None, False

    base, adjusted = prev_close, False

    payout = action.amount if action is not None else None
    if payout and payout < base * MAX_PAYOUT:
        base -= payout
        adjusted = True

    # The open is the gap the ratio test wants; the close stands in only when
    # there isn't one, as it does for a pre-open print in `core/preopen.py`.
    ref = _f(open_) or close
    ratio = base / ref
    if ratio > SPLIT_UP or ratio < SPLIT_DOWN:
        snapped = snap_ratio(ratio, prev_close=base, high=_f(high) or ref,
                             low=_f(low) or ref)
        if snapped not in (0, 1):
            base /= snapped
            adjusted = True

    return (close / base - 1) * 100, adjusted


def adjust_rows(rows):
    """Back-adjust OHLC in place for every corporate action found.

    `rows` is oldest-first, each a mapping with at least close/open and
    ideally high/low/prev_close/volume. Returns (rows, events).
    """
    events = []
    n = len(rows)
    for i in range(1, n):
        prev_close = _f(rows[i].get("prev_close"))
        op = _f(rows[i].get("open")) or _f(rows[i].get("close"))
        if not prev_close or not op:
            continue
        ratio = prev_close / op
        if not (ratio > SPLIT_UP or ratio < SPLIT_DOWN):
            continue
        ratio = snap_ratio(ratio, prev_close=prev_close,
                           high=_f(rows[i].get("high")) or op,
                           low=_f(rows[i].get("low")) or op)
        if ratio in (0, 1):
            continue
        for j in range(i):
            for key in ("open", "high", "low", "close", "prev_close"):
                if rows[j].get(key):
                    rows[j][key] = _f(rows[j][key]) / ratio
            if rows[j].get("volume"):
                rows[j]["volume"] = _f(rows[j]["volume"]) * ratio
        events.append((rows[i].get("trade_date"), round(ratio, 4)))
    return rows, events


def _f(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def factor_between(rows, from_index: int = 0) -> float:
    """Cumulative adjustment factor applied after `from_index`.

    Multiply an old raw close by this to put it on today's scale.
    """
    factor = 1.0
    for i in range(max(1, from_index + 1), len(rows)):
        prev_close = _f(rows[i].get("prev_close"))
        op = _f(rows[i].get("open")) or _f(rows[i].get("close"))
        if not prev_close or not op:
            continue
        ratio = prev_close / op
        if ratio > SPLIT_UP or ratio < SPLIT_DOWN:
            factor *= snap_ratio(ratio, prev_close=prev_close,
                                 high=_f(rows[i].get("high")) or op,
                                 low=_f(rows[i].get("low")) or op)
    return factor


_DIV_SPLIT = re.compile(r"/|&|\+|\band\b", re.IGNORECASE)      # clause separators of an NSE purpose text
_DIV_AMT = re.compile(r"R[se]\.?\s*-?\s*([0-9]+(?:\.[0-9]+)?)")  # 'Rs 12', 'Re 0.50', 'Rs - 20', 'Rs -10', 'Rs.5'


def parse_dividend(purpose: Any) -> float | None:
    """The cash dividend in Rs per share of an NSE corporate-action purpose text: the amounts of every clause that
    names a dividend, summed ('Dividend - Rs 160 Per Share & Special Dividend - Rs 375' -> 535; 'Interim Dividend - Rs
    6 Per Share Special Interim Dividend - Rs 10 Per Share' -> 16); clauses without 'divid' (AGM, interest, principal)
    are ignored. None = no dividend clause or no amount ('Interim Dividend')."""
    text = str(purpose or "")
    if "divid" not in text.lower():
        return None
    amts: list[float] = []
    for clause in _DIV_SPLIT.split(text):
        if "divid" in clause.lower():
            amts += [float(x) for x in _DIV_AMT.findall(clause)]
    return sum(amts) if amts else None


MODES = ("split", "div")
DIV_MAX_YIELD = 0.5


@dataclass(frozen=True)
class Adjusted:
    rows: list
    mode: str
    asof: str
    splits: tuple
    dividends: tuple
    unparsed: tuple
    skipped: dict


def _day(v):
    return v if isinstance(v, date) else date.fromisoformat(str(v))


def _suffix(steps):
    """steps = [(ex, ratio)] sorted by ex -> q(d) = the product of the ratios with ex > d, built from the LAST step."""
    exs = [ex for ex, _r in steps]
    suffix = [1.0] * (len(steps) + 1)
    for i in range(len(steps) - 1, -1, -1):
        suffix[i] = suffix[i + 1] * steps[i][1]
    return lambda d: suffix[bisect.bisect_right(exs, d)]


def adjust_bars(bars, actions, *, symbol: str, asof, mode: str = "split") -> Adjusted:
    """Copy as-traded OHLC onto the requested basis, using only actions known by asof."""
    if mode not in MODES:
        raise ValueError(f"Unknown adjustment mode: {mode!r}")
    asof = _day(asof)
    days = [_day(b["trade_date"]) for b in bars]
    if any(left >= right for left, right in itertools.pairwise(days)):
        raise ValueError("trade_dates must be strictly increasing")
    if days and days[-1] > asof:
        raise ValueError("Last bar is after asof")

    symbol = symbol.strip().upper()
    groups = {}
    amounts = {}
    eligible = []
    unparsed = set()
    skipped = {"div_unparsed": 0, "div_no_prev_row": 0, "div_implausible": 0}
    for a in actions:
        if a["symbol"].strip().upper() != symbol:
            continue
        ex = _day(a["ex_date"])
        if ex > asof:
            continue
        eligible.append((ex, a))
        if a["kind"] != "other":
            groups.setdefault(ex, 1.0)
            if a["factor"] is None or a["parse"] != "ok":
                unparsed.add(ex)
            else:
                groups[ex] *= float(a["factor"])
    dividends = []
    if mode == "div":
        for ex, a in eligible:
            if a["kind"] != "other" or "divid" not in a["purpose"].lower():
                continue
            amt = parse_dividend(a["purpose"])
            if amt is None or not amt > 0:
                skipped["div_unparsed"] += 1
            else:
                amounts[ex] = amounts.get(ex, 0.0) + amt
        for ex, amt in sorted(amounts.items()):
            i = bisect.bisect_left(days, ex) - 1
            if i < 0:
                skipped["div_no_prev_row"] += 1
                continue
            pc = float(bars[i]["close"])
            if not pc > 0 or amt / pc >= DIV_MAX_YIELD:
                skipped["div_implausible"] += 1
                continue
            dividends.append((ex, amt, pc, 1.0 - amt / pc))
    s = _suffix(sorted(groups.items()))
    q = _suffix([(ex, ratio) for ex, _amt, _pc, ratio in dividends])
    rows = []
    for b, d in zip(bars, days, strict=True):
        row = dict(b)
        for k in ("open", "high", "low", "close"):
            v = float(b[k]) * s(d)
            row[k] = v * q(d) if mode == "div" else v
        rows.append(row)
    return Adjusted(
        rows=rows, mode=mode, asof=asof.isoformat(),
        splits=tuple((ex.isoformat(), factor) for ex, factor in sorted(groups.items())),
        dividends=tuple((ex.isoformat(), amt, pc, ratio) for ex, amt, pc, ratio in dividends),
        unparsed=tuple(ex.isoformat() for ex in sorted(unparsed)), skipped=skipped,
    )
