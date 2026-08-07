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


def exact_scale_date(scale, today: date | None = None) -> date | None:
    """Retorna a data realmente gravada na escala.

    Escalas como `07/08/26-SEX` precisam continuar sendo 07/08/2026.
    Elas nunca devem ser convertidas para outra sexta-feira só porque o dia
    da semana é igual. O fallback por weekday é exclusivo para registros
    legados que realmente não possuem uma data válida.
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

    # Compatibilidade: somente escalas antigas sem data real usam o weekday.
    today = today or datetime.now(TZ).date()
    weekday = patch._weekday_zero_based(scale)
    if weekday is None:
        return None
    monday = today - timedelta(days=today.weekday())
    return monday + timedelta(days=int(weekday))


def matches_exact_day(scale, day: date) -> bool:
    return exact_scale_date(scale, day) == day


def _filter_current_week(rows):
    today = datetime.now(TZ).date()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    result = []
    for scale in rows:
        d = exact_scale_date(scale, today)
        if d and monday <= d <= sunday:
            result.append(scale)
    return result


# As rotinas de produção consultam estes nomes em tempo de execução.
patch._current_week_date = exact_scale_date
flow._scale_date = exact_scale_date
perf._matches_day = matches_exact_day

_original_patch_coop_scales = patch._coop_scales
_original_patch_rest_scales = patch._rest_scales


def coop_scales_current_week(coop):
    return _filter_current_week(_original_patch_coop_scales(coop))


def rest_scales_current_week(rest):
    return _filter_current_week(_original_patch_rest_scales(rest))


patch._coop_scales = coop_scales_current_week
patch._rest_scales = rest_scales_current_week

# Mantém as buscas indexadas no painel principal, mas a correspondência de
# data passa a ser exata. Assim um filtro de ontem continua encontrando ontem.
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
