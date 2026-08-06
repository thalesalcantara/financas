from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from functools import wraps
from types import SimpleNamespace

from flask import redirect, request, session, url_for
from sqlalchemy import or_

import production_scale_patch as backend

app = backend.app
flow = backend.flow
Cooperado = backend.Cooperado
Restaurante = backend.Restaurante
Lancamento = backend.Lancamento
ProducaoCooperado = backend.ProducaoCooperado
Escala = backend.Escala
TZ = backend.TZ


def _norm_name(value) -> str:
    return " ".join(str(value or "").replace("_", " ").split())


def _coop_scales(coop):
    target = backend._norm(coop.nome)
    rows, seen = [], set()
    candidates = (
        Escala.query.filter(
            or_(
                Escala.cooperado_id == coop.id,
                Escala.cooperado_nome.isnot(None),
            )
        )
        .order_by(Escala.id.asc())
        .limit(2500)
        .all()
    )
    for scale in candidates:
        by_name = bool(target and backend._norm(scale.cooperado_nome) == target)
        if (scale.cooperado_id == coop.id or by_name) and scale.id not in seen:
            seen.add(scale.id)
            rows.append(scale)
    return rows


def _rest_scales(rest):
    target = backend._norm(rest.nome)
    rows, seen = [], set()
    candidates = (
        Escala.query.filter(
            or_(
                Escala.restaurante_id == rest.id,
                Escala.contrato.isnot(None),
            )
        )
        .order_by(Escala.id.asc())
        .limit(2500)
        .all()
    )
    for scale in candidates:
        contract = backend._norm(scale.contrato)
        by_name = bool(
            target and contract and
            (target == contract or target in contract or contract in target)
        )
        if (scale.restaurante_id == rest.id or by_name) and scale.id not in seen:
            seen.add(scale.id)
            rows.append(scale)
    return rows


backend._coop_scales = _coop_scales
backend._rest_scales = _rest_scales


def _matches_day(scale, day: date) -> bool:
    exact = backend.upgrade._parse_date(scale.data)
    if exact:
        return exact == day
    weekday = flow._weekday_for_scale(scale)
    return weekday is not None and int(weekday) == day.weekday()


def _timeline(coop, start: date, end: date):
    if end < start:
        start, end = end, start
    if (end - start).days > 62:
        end = start + timedelta(days=62)

    now = datetime.now(TZ)
    scales = _coop_scales(coop)
    restaurants = backend._restaurant_map(scales)

    productions = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.cooperado_id == coop.id,
            ProducaoCooperado.data >= start,
            ProducaoCooperado.data <= end,
        )
        .order_by(ProducaoCooperado.id.desc())
        .all()
    )
    launches = (
        Lancamento.query.filter(
            Lancamento.cooperado_id == coop.id,
            Lancamento.data >= start,
            Lancamento.data <= end,
        )
        .order_by(Lancamento.id.desc())
        .all()
    )

    prod_scale_day, prod_slot, launches_day = {}, {}, {}
    for item in productions:
        if item.escala_id:
            prod_scale_day[(item.escala_id, item.data)] = item
        prod_slot[(
            item.restaurante_id,
            item.data,
            backend.upgrade._norm_time(item.hora_inicio),
            backend.upgrade._norm_time(item.hora_fim),
        )] = item
    for launch in launches:
        launches_day.setdefault((launch.restaurante_id, launch.data), []).append(launch)

    result, linked_launch_ids = [], set()
    day = start
    while day <= end:
        for scale in scales:
            if not _matches_day(scale, day):
                continue

            rest = restaurants.get(scale.id)
            start_time, end_time = backend.upgrade._times_from_text(scale.horario)
            end_at = flow._end_at(day, start_time, end_time)
            finished = bool(end_at and now >= end_at)
            if day < now.date() and not end_at:
                finished = True

            production = prod_scale_day.get((scale.id, day))
            if not production and rest:
                production = prod_slot.get((rest.id, day, start_time, end_time))

            launch = None
            if rest:
                for candidate in launches_day.get((rest.id, day), []):
                    if backend.upgrade._overlap(
                        candidate.hora_inicio,
                        candidate.hora_fim,
                        start_time,
                        end_time,
                    ):
                        launch = candidate
                        linked_launch_ids.add(candidate.id)
                        break

            total = float(
                (launch.valor if launch else None)
                or (production.valor_total if production else 0)
                or 0
            )
            quantity = int(
                (launch.qtd_entregas if launch else None)
                or (production.qtd_entregas if production else 0)
                or 0
            )

            if launch or (production and production.status == "aprovada"):
                color, label, can_submit = "green", "Aprovada", False
            elif production and production.status == "pendente" and total > 0:
                color, label, can_submit = "yellow", "Enviada · aguardando aprovação", False
            elif production and production.status == "recusada":
                color, label, can_submit = "red", "Recusada · bloqueada", False
            elif not finished:
                color, label, can_submit = "muted", "Bloqueada até o fim do horário", False
            else:
                color, label, can_submit = "red", "Pendente de lançamento", bool(rest)

            result.append(SimpleNamespace(
                kind="scale",
                escala=scale,
                restaurante=rest,
                data=day,
                inicio=start_time,
                fim=end_time,
                finalizada=finished,
                producao=production,
                lancamento=launch,
                valor_total=total,
                qtd_entregas=quantity,
                color=color,
                status_label=label,
                pode_lancar=can_submit,
                avaliada=bool(
                    launch and getattr(launch, "minha_avaliacao", None) is not None
                ),
            ))
        day += timedelta(days=1)

    for launch in launches:
        if launch.id in linked_launch_ids:
            continue
        result.append(SimpleNamespace(
            kind="launch",
            escala=None,
            restaurante=getattr(launch, "restaurante", None),
            data=launch.data,
            inicio=backend.upgrade._norm_time(launch.hora_inicio),
            fim=backend.upgrade._norm_time(launch.hora_fim),
            finalizada=True,
            producao=None,
            lancamento=launch,
            valor_total=float(launch.valor or 0),
            qtd_entregas=int(launch.qtd_entregas or 0),
            color="green",
            status_label="Produção registrada",
            pode_lancar=False,
            avaliada=bool(getattr(launch, "minha_avaliacao", None) is not None),
        ))

    result.sort(key=lambda x: (
        x.data or date.max,
        x.inicio or "",
        0 if x.kind == "scale" else 1,
        getattr(getattr(x, "escala", None), "id", 0) or 0,
    ))
    return result


if not app.extensions.get("coopex_ui_v3_context"):
    @app.context_processor
    def _coopex_ui_context():
        role = (session.get("user_tipo") or "").strip().lower()
        context = {
            "coopex_rest_display_name": "ESTABELECIMENTO",
            "coopex_rest_pending_rows": [],
            "coopex_coop_timeline": [],
            "coopex_filter_start": None,
            "coopex_filter_end": None,
        }
        try:
            if role == "restaurante":
                rest = Restaurante.query.filter_by(
                    usuario_id=session.get("user_id")
                ).first()
                if rest:
                    context["coopex_rest_display_name"] = _norm_name(rest.nome)
                    if request.endpoint == "portal_restaurante":
                        rows = backend._rest_scale_rows(rest)
                        context["coopex_rest_pending_rows"] = [
                            row for row in rows
                            if row.producao
                            and row.producao.status == "pendente"
                            and float(row.producao.valor_total or 0) > 0
                        ]

            elif role == "cooperado" and request.endpoint == "portal_cooperado":
                coop = Cooperado.query.filter_by(
                    usuario_id=session.get("user_id")
                ).first()
                if coop:
                    today = datetime.now(TZ).date()
                    start = backend.upgrade._parse_date(
                        request.args.get("data_inicio")
                    ) or today
                    end = backend.upgrade._parse_date(
                        request.args.get("data_fim")
                    ) or start
                    if end < start:
                        start, end = end, start
                    context.update(
                        coopex_coop_timeline=_timeline(coop, start, end),
                        coopex_filter_start=start,
                        coopex_filter_end=end,
                    )
        except Exception:
            app.logger.exception("Falha ao montar a sequência diária de produção")
        return context

    app.extensions["coopex_ui_v3_context"] = True


if not app.extensions.get("coopex_ui_v3_redirect"):
    original = app.view_functions.get("coop_producao")
    if original:
        @wraps(original)
        def _coop_submit_and_return(*args, **kwargs):
            response = original(*args, **kwargs)
            if request.method == "POST" and request.form.get("return_to") == "painel":
                params = {"active_tab": "producoes"}
                if request.form.get("data_inicio"):
                    params["data_inicio"] = request.form["data_inicio"]
                if request.form.get("data_fim"):
                    params["data_fim"] = request.form["data_fim"]
                return redirect(url_for("portal_cooperado", **params))
            return response

        app.view_functions["coop_producao"] = _coop_submit_and_return
        app.view_functions["coop_producao_nova"] = _coop_submit_and_return
    app.extensions["coopex_ui_v3_redirect"] = True


def _transform_restaurant(source: str) -> str:
    source = re.sub(
        r'<a\s+data-coopex-producoes="1".*?</a>\s*',
        "",
        source,
        count=1,
        flags=re.S,
    )
    old_brand = '<div class="brand"><i class="bi bi-building"></i><span>Portal do Estabelecimento</span></div>'
    new_brand = """<div class="brand coopex-brand-welcome">
        <i class="bi bi-shop"></i>
        <span class="coopex-brand-copy">
          <small>SEJA BEM-VINDO</small>
          <strong>{{ coopex_rest_display_name|default('ESTABELECIMENTO') }}</strong>
        </span>
      </div>"""
    source = source.replace(old_brand, new_brand, 1)

    css_tag = '<link rel="stylesheet" href="{{ url_for(\'static\', filename=\'css/restaurante_v3.css\') }}">'
    if "restaurante_v3.css" not in source:
        source = source.replace("</head>", "  " + css_tag + "\n</head>", 1)

    marker = '<div class="row g-4 align-items-start">'
    if "_rest_approvals_tabs.html" not in source and marker in source:
        source = source.replace(
            marker,
            "        {% include '_rest_approvals_tabs.html' %}\n        " + marker,
            1,
        )

    js_tag = '<script src="{{ url_for(\'static\', filename=\'js/restaurante_v3.js\') }}"></script>'
    if "restaurante_v3.js" not in source:
        source = source.replace("</body>", js_tag + "\n</body>", 1)
    return source


def _transform_coop(source: str) -> str:
    css_tag = '<link rel="stylesheet" href="{{ url_for(\'static\', filename=\'css/cooperado_producao_v3.css\') }}">'
    if "cooperado_producao_v3.css" not in source:
        source = source.replace("</head>", "  " + css_tag + "\n</head>", 1)

    marker = '<section class="tab-pane-custom" id="tab-producoes">'
    if "_coop_timeline.html" not in source and marker in source:
        source = source.replace(
            marker,
            marker + "\n      {% include '_coop_timeline.html' %}",
            1,
        )
    return source


if not app.extensions.get("coopex_ui_v3_loader"):
    loader = app.jinja_loader
    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "restaurante_dashboard.html":
            source = _transform_restaurant(source)
        elif template == "painel_cooperado.html":
            source = _transform_coop(source)
        return source, filename, uptodate

    loader.get_source = get_source
    app.extensions["coopex_ui_v3_loader"] = True
    app.jinja_env.cache.clear()
