from __future__ import annotations

from datetime import date

from werkzeug.datastructures import ImmutableMultiDict

import admin_ui_v6 as ui

app = ui.app
BUILD = "20260807-1120"


def _replace_request_args(values: dict[str, str]) -> None:
    """Altera apenas os parâmetros usados pela rota atual, sem redirecionamento."""
    from flask import request

    current = request.args.copy()
    changed = False
    for key, value in values.items():
        if key not in current or current.get(key) in (None, ""):
            current[key] = value
            changed = True
    if changed:
        request.__dict__["args"] = ImmutableMultiDict(current)


@app.before_request
def _admin_v7_light_defaults():
    from flask import request

    endpoint = request.endpoint or ""
    if endpoint == "admin_dashboard":
        tab = (request.args.get("tab") or "lancamentos").strip().lower()
        # Resumo padrão = somente hoje. Filtros informados pelo usuário continuam
        # sendo respeitados e podem trazer semana, mês ou qualquer período.
        if tab == "resumo" and not any(
            request.args.get(k)
            for k in ("data_inicio", "data_fim", "resumo_inicio", "resumo_fim")
        ):
            today = date.today().isoformat()
            _replace_request_args({"data_inicio": today, "data_fim": today})

    elif endpoint == "admin_avaliacoes":
        # A tela continua com todos os filtros e indicadores, mas renderiza no
        # máximo 50 avaliações por página para não transferir 200 linhas sempre.
        if not request.args.get("per_page"):
            _replace_request_args({"per_page": "50"})
