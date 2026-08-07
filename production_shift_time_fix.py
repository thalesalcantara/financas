from __future__ import annotations

import re
from datetime import date, datetime, timedelta

import performance_query_override as query_override
import production_scale_patch as patch
import performance_ui_fix as perf

flow = patch.flow
upgrade = patch.upgrade
TZ = patch.TZ

_DATE_BR = re.compile(r"(?<!\d)(\d{2})/(\d{2})/(\d{2,4})(?!\d)")
_DATE_ISO = re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)")
_TIME_TOKEN = re.compile(r"(?<!\d)(2[0-3]|[01]?\d)(?:\s*(?::|h)\s*([0-5]\d))?(?!\d)", re.I)


def _norm_time_fixed(raw) -> str:
    text = str(raw or "").strip().lower()
    match = _TIME_TOKEN.search(text)
    if not match:
        return ""
    hour = int(match.group(1))
    minute = int(match.group(2) or 0)
    return f"{hour:02d}:{minute:02d}"


def _times_from_text_fixed(raw) -> tuple[str, str]:
    matches = list(_TIME_TOKEN.finditer(str(raw or "")))
    if not matches:
        return "", ""

    def fmt(match):
        return f"{int(match.group(1)):02d}:{int(match.group(2) or 0):02d}"

    start = fmt(matches[0])
    end = fmt(matches[-1]) if len(matches) > 1 else ""
    return start, end


def _minutes_fixed(raw) -> int | None:
    value = _norm_time_fixed(raw)
    if not value:
        return None
    hour, minute = map(int, value.split(":"))
    return hour * 60 + minute


# Corrige o parser na origem para todos os módulos que usam coopex_upgrade.
# O erro antigo interpretava o "00" de 22:00 como um horário separado.
upgrade._norm_time = _norm_time_fixed
upgrade._times_from_text = _times_from_text_fixed
upgrade._minutes = _minutes_fixed


def exact_scale_date(scale, today: date | None = None) -> date | None:
    """Retorna a data realmente gravada na escala.

    Ex.: `07/08/26-SEX` continua sendo 07/08/2026. A data não é movida para
    outra sexta-feira. Só registros legados sem data válida usam o weekday.
    """
    raw = str(getattr(scale, "data", "") or "").strip()

    m = _DATE_BR.search(raw)
    if m:
        day, month, year = map(int, m.groups())
        if year < 100:
            year += 2000
        try:
            return date(year, month, day)
        except ValueError:
            return None

    m = _DATE_ISO.search(raw)
    if m:
        year, month, day = map(int, m.groups())
        try:
            return date(year, month, day)
        except ValueError:
            return None

    parsed = upgrade._parse_date(raw)
    if parsed:
        return parsed

    today = today or datetime.now(TZ).date()
    weekday = patch._weekday_zero_based(scale)
    if weekday is None:
        return None
    monday = today - timedelta(days=today.weekday())
    return monday + timedelta(days=int(weekday))


def matches_exact_day(scale, day: date) -> bool:
    return exact_scale_date(scale, day) == day


def _filter_active_window(rows):
    """Mantém a semana atual e pendências recentes sem misturar escalas antigas."""
    today = datetime.now(TZ).date()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    start = today - timedelta(days=31)
    result = []
    for scale in rows:
        d = exact_scale_date(scale, today)
        if d and start <= d <= sunday:
            result.append(scale)
    return result


patch._current_week_date = exact_scale_date
flow._scale_date = exact_scale_date
perf._matches_day = matches_exact_day

_original_patch_coop_scales = patch._coop_scales
_original_patch_rest_scales = patch._rest_scales


def coop_scales_active_window(coop):
    return _filter_active_window(_original_patch_coop_scales(coop))


def rest_scales_active_window(rest):
    return _filter_active_window(_original_patch_rest_scales(rest))


patch._coop_scales = coop_scales_active_window
patch._rest_scales = rest_scales_active_window
query_override.perf._matches_day = matches_exact_day


def _validate_parser() -> None:
    examples = {
        "11:00 às 15:00": ("11:00", "15:00"),
        "18:00 às 22:00": ("18:00", "22:00"),
        "17:30 às 22:30": ("17:30", "22:30"),
        "18:00 ÀS 22:00": ("18:00", "22:00"),
        "11:00-14:00/15:00-17:00": ("11:00", "17:00"),
        "19:00 às 07:00": ("19:00", "07:00"),
    }
    for raw, expected in examples.items():
        parsed = upgrade._times_from_text(raw)
        if parsed != expected:
            raise RuntimeError(f"Parser de horario incorreto: {raw!r} -> {parsed!r}; esperado {expected!r}")


_validate_parser()
