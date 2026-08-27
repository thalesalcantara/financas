from __future__ import annotations

import calendar
from datetime import date
from types import SimpleNamespace

from flask import render_template_string, request, url_for

import app as legacy
import coop_expense_control_v16 as v16
import coop_expense_management_v17 as v18

app = legacy.app
db = legacy.db
DespesaCooperativa = legacy.DespesaCooperativa


_OLD_KPIS = '''<div class="v18-kpis"><div class="alv8-kpi"><span>Total geral</span><strong>R$ {{total|brl}}</strong></div><div class="alv8-kpi"><span>Total para rateio</span><strong>R$ {{total_rateio|brl}}</strong></div><div class="alv8-kpi"><span>Total fixo</span><strong>R$ {{fixo|brl}}</strong></div><div class="alv8-kpi"><span>Total variável</span><strong>R$ {{variavel|brl}}</strong></div><div class="alv8-kpi"><span>Não vai para rateio</span><strong>R$ {{nao_rateio|brl}}</strong></div></div>'''

_NEW_KPIS = '''<div class="v18-kpis" style="grid-template-columns:repeat(4,minmax(0,1fr))"><div class="alv8-kpi"><span>Total fixo</span><strong>R$ {{fixo|brl}}</strong></div><div class="alv8-kpi"><span>Total variável</span><strong>R$ {{variavel|brl}}</strong></div><div class="alv8-kpi"><span>Total geral</span><strong>R$ {{total|brl}}</strong></div><div class="alv8-kpi"><span>Não vai para o rateio</span><strong>R$ {{nao_rateio|brl}}</strong></div></div>'''

_TEMPLATE = v18._TEMPLATE.replace(_OLD_KPIS, _NEW_KPIS)


def _render_v19():
    v16._require("ver")
    try:
        v18._ensure_recurring()
    except Exception:
        db.session.rollback()
        app.logger.exception("V19 recorrência")

    today = date.today()
    first = date(today.year, today.month, 1)
    last = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
    inicio = v16._parse_date(request.args.get("inicio")) or first
    fim = v16._parse_date(request.args.get("fim")) or last
    if fim < inicio:
        inicio, fim = fim, inicio

    expenses = (
        DespesaCooperativa.query
        .filter(DespesaCooperativa.data >= inicio, DespesaCooperativa.data <= fim)
        .order_by(DespesaCooperativa.data.desc(), DespesaCooperativa.id.desc())
        .all()
    )
    ids = [d.id for d in expenses]
    ctls = {
        x.despesa_id: x
        for x in v16.DespesaCoopControleV16.query.filter(v16.DespesaCoopControleV16.despesa_id.in_(ids)).all()
    } if ids else {}
    recs = {
        x.despesa_id: x
        for x in v18.DespesaCoopRecorrenciaV17.query.filter(v18.DespesaCoopRecorrenciaV17.despesa_id.in_(ids)).all()
    } if ids else {}

    rows = []
    fixo = 0.0
    variavel = 0.0
    nao_rateio = 0.0

    for d in expenses:
        ctl = ctls.get(d.id)
        tipo = ctl.tipo if ctl else None
        val = float(d.valor or 0)
        if tipo == "fixa":
            fixo += val
        elif tipo == "variavel":
            variavel += val
        elif tipo == "nao_rateio":
            nao_rateio += val
        rows.append(SimpleNamespace(
            desp=d,
            tipo=tipo,
            pago=bool(ctl and ctl.pago),
            data_pagamento=ctl.data_pagamento if ctl else None,
            mensal=bool(recs.get(d.id) and recs[d.id].repete_mensalmente),
        ))

    # Regra financeira: Total Geral considera somente despesas que participam do rateio.
    total = fixo + variavel

    user = legacy._usuario_logado()
    master = bool(user and getattr(user, "is_master", False))
    perms = {}
    if user and (user.tipo or "").lower() == "admin":
        perms = (
            {a: {x: True for x in ("ver", "criar", "editar", "excluir")} for a in legacy.ADMIN_ABAS}
            if master else legacy.get_admin_permissions_map(user.id)
        )

    return render_template_string(
        _TEMPLATE,
        rows=rows,
        total=round(total, 2),
        total_rateio=round(total, 2),
        fixo=round(fixo, 2),
        variavel=round(variavel, 2),
        nao_rateio=round(nao_rateio, 2),
        inicio=inicio,
        fim=fim,
        today=today,
        can_create=v16._allowed("criar"),
        can_edit=v16._allowed("editar"),
        can_delete=v16._allowed("excluir"),
        build="20260827-v19",
        active_tab="despesas",
        admin_nav_master=master,
        admin_nav_perms=perms,
        admin_nav_home_url=url_for("admin_light_summary"),
    )


v16._render_expenses_v16 = _render_v19
app.logger.info("V19: Total Geral = Fixo + Variável; Não rateio separado.")
