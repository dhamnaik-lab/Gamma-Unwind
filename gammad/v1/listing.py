"""Side-specific scheduling; only listing may retain legacy pending defaults."""
from gammad.core.v1_params import PendingPick


def _ready(params, side):
    try:
        for key in ("cadence", "band_pct", "method"):
            params.get(f"{side}.list.{key}")
    except PendingPick:
        return False
    return True


def scan_marks(params, side, legacy_marks):
    return params.scan_marks(side) if _ready(params, side) else tuple(legacy_marks)


def refresh_marks(params, legacy_marks):
    return tuple(sorted(set(scan_marks(params, "PE", legacy_marks)) |
                        set(scan_marks(params, "CE", legacy_marks))))


def band_pct(params, side, legacy_band):
    return params.band_pct(side) if _ready(params, side) else legacy_band


def lists_side(params, side, hhmm, legacy_marks):
    return hhmm in scan_marks(params, side, legacy_marks)
