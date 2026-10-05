from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime, timedelta
from types import SimpleNamespace

from flask import abort, flash, redirect, render_template, request, session, url_for
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError

import production_scale_flow as flow

app = flow.app
db = flow.db
upgrade = flow.upgrade
Escala = flow.Escala
Cooperado = flow.Cooperado
Restaurante = flow.Restaurante
Lancamento = flow.Lancamento
ProducaoCooperado = flow.ProducaoCooperado
TZ = flow.TZ


def _role_is(role: str) -> bool:
    return (session.get("user_tipo") or "").strip().lower() == role


def _norm(value) -> str:
    text = unicodedata.normalize("NFD", str(value or ""))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("_", " ").casefold()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", text).split())


def _weekday_zero_based(scale) -> int | None:
    helper = getattr(flow.legacy, "_weekday_from_data_str", None)
    if helper:
        try:
            value = helper(scale.data)
            if value is not None:
                value = int(value)
                if 1 <= value <= 7:
                    return value - 1
                if 0 <= value <= 6:
                    return value
        except Exception:
            pass
    text = _norm(scale.data)
    return next((value for key, value in flow._WEEKDAYS.items() if _norm(key) in text), None)


def _current_week_date(scale, today: date) -> date | None:
    monday = today - timedelta(days=today.weekday())
    parsed = None
    parser = getattr(flow.legacy, "_parse_data_escala_str", None)
    if parser:
        try:
            parsed = parser(scale.data)
        except Exception:
            parsed = None
    if not parsed:
        parsed = upgrade._parse_date(scale.data)
    weekday = parsed.weekday() if parsed else _weekday_zero_based(scale)
    if weekday is None:
        return None
    return monday + timedelta(days=int(weekday))


def _scale_belongs_to_coop(scale, coop) -> bool:
    if scale.cooperado_id:
        return scale.cooperado_id == coop.id
    return bool(_norm(scale.cooperado_nome) and _norm(scale.cooperado_nome) == _norm(coop.nome))


flow._weekday_for_scale = _weekday_zero_based
flow._scale_date = _current_week_date
flow._scale_belongs_to_coop = _scale_belongs_to_coop


def _restaurant_map(scales):
    restaurants = Restaurante.query.order_by(Restaurante.nome.asc()).all()
    by_id = {r.id: r for r in restaurants}
    normalized = [(_norm(r.nome), r) for r in restaurants if _norm(r.nome)]
    normalized.sort(key=lambda item: len(item[0]), reverse=True)
    result = {}
    for scale in scales:
        restaurant = by_id.get(scale.restaurante_id) if scale.restaurante_id else None
        contract = _norm(scale.contrato)
        if not restaurant and contract:
            restaurant = next(
                (r for name, r in normalized if name == contract or name in contract or contract in name),
                None,
            )
        result[scale.id] = restaurant
    return result


def _coop_scales(coop):
    target = _norm(coop.nome)
    candidates = (
        Escala.query.filter(
            or_(
                Escala.cooperado_id == coop.id,
                Escala.cooperado_nome.isnot(None),
            )
        )
        .order_by(Escala.id.asc())
        .limit(1200)
        .all()
    )
    rows = []
    seen = set()
    for scale in candidates:
        belongs = scale.cooperado_id == coop.id
        if not belongs and not scale.cooperado_id:
            belongs = bool(target and _norm(scale.cooperado_nome) == target)
        if belongs and scale.id not in seen:
            seen.add(scale.id)
            rows.append(scale)
    return rows


def _coop_scale_rows(coop):
    now = datetime.now(TZ)
    today = now.date()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    scales = _coop_scales(coop)
    restaurants = _restaurant_map(scales)

    productions = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.cooperado_id == coop.id,
            ProducaoCooperado.data >= monday,
            ProducaoCooperado.data <= sunday,
        )
        .order_by(ProducaoCooperado.id.desc())
        .all()
    )
    by_scale = {p.escala_id: p for p in productions if p.escala_id}
    by_slot = {
        (
            p.restaurante_id,
            p.data,
            upgrade._norm_time(p.hora_inicio),
            upgrade._norm_time(p.hora_fim),
        ): p
        for p in productions
    }
    launches = (
        Lancamento.query.filter(
            Lancamento.cooperado_id == coop.id,
            Lancamento.data >= monday,
            Lancamento.data <= sunday,
        )
        .order_by(Lancamento.id.desc())
        .all()
    )
    launches_by_day = {}
    for launch in launches:
        launches_by_day.setdefault((launch.restaurante_id, launch.data), []).append(launch)

    result = []
    dirty = False
    for scale in scales:
        data_ref = _current_week_date(scale, today)
        restaurant = restaurants.get(scale.id)
        start, end = upgrade._times_from_text(scale.horario)
        end_at = flow._end_at(data_ref, start, end)
        finished = bool(end_at and now >= end_at)
        if data_ref and data_ref < today and not end_at:
            finished = True

        production = by_scale.get(scale.id)
        if not production and restaurant and data_ref:
            production = by_slot.get((restaurant.id, data_ref, start, end))
        launch = flow._find_launch(launches_by_day, restaurant.id, data_ref, start, end) if restaurant and data_ref else None

        if production and launch and production.status != "aprovada":
            production.status = "aprovada"
            production.lancamento_id = launch.id
            production.valor_total = float(launch.valor or production.valor_total or 0)
            production.qtd_entregas = int(launch.qtd_entregas or production.qtd_entregas or 0)
            production.valor_unitario = round(production.valor_total / production.qtd_entregas, 6) if production.qtd_entregas else 0
            production.decidido_em = datetime.utcnow()
            dirty = True

        total = float((launch.valor if launch else None) or (production.valor_total if production else 0) or 0)
        quantity = int((launch.qtd_entregas if launch else None) or (production.qtd_entregas if production else 0) or 0)

        if launch or (production and production.status == "aprovada"):
            color, label, can_submit = "green", "Lançada", False
        elif production and production.status == "pendente" and total > 0:
            color, label, can_submit = "green", "Informada · aguardando aprovação", False
        elif production and production.status == "recusada":
            color, label, can_submit = "red", "Recusada pelo estabelecimento · bloqueada", False
        elif not finished:
            color, label, can_submit = "red", "Pendente · libera após o horário", False
        else:
            color, label, can_submit = "red", "Pendente · informar produção", bool(restaurant)

        result.append(SimpleNamespace(
            escala=scale,
            restaurante=restaurant,
            data=data_ref,
            inicio=start,
            fim=end,
            fim_em=end_at,
            finalizada=finished,
            producao=production,
            lancamento=launch,
            valor_total=total,
            qtd_entregas=quantity,
            color=color,
            status_label=label,
            pode_lancar=can_submit,
        ))

    if dirty:
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
    result.sort(key=lambda row: (row.data or date.max, row.inicio or "", row.escala.id))
    return result


flow._coop_scale_rows = _coop_scale_rows
_original_coop_producao_flow = flow.coop_producao_flow


def coop_producao_locked_flow():
    if request.method == "POST" and _role_is("cooperado"):
        coop = flow._coop_current()
        scale_id = request.form.get("escala_id", type=int)
        if scale_id:
            refused = ProducaoCooperado.query.filter_by(
                cooperado_id=coop.id,
                escala_id=scale_id,
                status="recusada",
            ).first()
            if refused:
                flash("Esta produção foi recusada e está bloqueada para novo envio.", "warning")
                return redirect(url_for("coop_producao"))
    return _original_coop_producao_flow()


def coop_producao_edit_locked(item_id: int):
    if not _role_is("cooperado"):
        return redirect(url_for("login"))
    coop = flow._coop_current()
    item = ProducaoCooperado.query.filter_by(id=item_id, cooperado_id=coop.id).first_or_404()
    if item.status == "recusada":
        flash("Esta produção foi recusada e permanece bloqueada.", "warning")
    else:
        flash("Esta produção não pode ser alterada pelo cooperado.", "warning")
    return redirect(url_for("coop_producao"))


def _rest_scales(rest):
    rest_name = _norm(rest.nome)
    candidates = (
        Escala.query.filter(
            or_(
                Escala.restaurante_id == rest.id,
                Escala.contrato.isnot(None),
            )
        )
        .order_by(Escala.id.asc())
        .limit(1600)
        .all()
    )
    result = []
    seen = set()
    for scale in candidates:
        contract = _norm(scale.contrato)
        belongs = scale.restaurante_id == rest.id
        if not belongs and not scale.restaurante_id and contract:
            belongs = contract == rest_name or rest_name in contract or contract in rest_name
        if belongs and scale.id not in seen:
            seen.add(scale.id)
            result.append(scale)
    return result


def _rest_scale_rows(rest):
    now = datetime.now(TZ)
    today = now.date()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    scales = _rest_scales(rest)

    coops = Cooperado.query.order_by(Cooperado.nome.asc()).all()
    coops_by_id = {c.id: c for c in coops}
    coops_by_name = {_norm(c.nome): c for c in coops if _norm(c.nome)}

    productions = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.data >= monday,
            ProducaoCooperado.data <= sunday,
        )
        .order_by(ProducaoCooperado.id.desc())
        .all()
    )
    by_scale = {p.escala_id: p for p in productions if p.escala_id}
    launches = (
        Lancamento.query.filter(
            Lancamento.restaurante_id == rest.id,
            Lancamento.data >= monday,
            Lancamento.data <= sunday,
        )
        .order_by(Lancamento.id.desc())
        .all()
    )
    launch_by_day = {}
    for launch in launches:
        launch_by_day.setdefault((launch.cooperado_id, launch.data), []).append(launch)

    result = []
    for scale in scales:
        data_ref = _current_week_date(scale, today)
        coop = coops_by_id.get(scale.cooperado_id) if scale.cooperado_id else coops_by_name.get(_norm(scale.cooperado_nome))
        start, end = upgrade._times_from_text(scale.horario)
        end_at = flow._end_at(data_ref, start, end)
        finished = bool(end_at and now >= end_at)
        production = by_scale.get(scale.id)
        launch = None
        if coop and data_ref:
            for candidate in launch_by_day.get((coop.id, data_ref), []):
                if upgrade._overlap(candidate.hora_inicio, candidate.hora_fim, start, end):
                    launch = candidate
                    break

        total = float((launch.valor if launch else None) or (production.valor_total if production else 0) or 0)
        if launch or (production and production.status == "aprovada"):
            color, label = "green", "Lançada"
        elif production and production.status == "pendente" and total > 0:
            color, label = "green", "Informada pelo cooperado · confira"
        elif production and production.status == "recusada":
            color, label = "red", "Recusada · estabelecimento pode lançar"
        else:
            color, label = "red", "Pendente · estabelecimento pode lançar agora"

        can_launch = bool(
            coop
            and not launch
            and not (production and production.status == "aprovada")
            and not (production and production.status == "pendente" and total > 0)
        )
        result.append(SimpleNamespace(
            escala=scale,
            cooperado=coop,
            data=data_ref,
            inicio=start,
            fim=end,
            fim_em=end_at,
            finalizada=finished,
            producao=production,
            lancamento=launch,
            valor_total=total,
            color=color,
            status_label=label,
            pode_lancar=can_launch,
        ))

    result.sort(key=lambda row: (row.data or date.max, row.inicio or "", row.escala.id))
    return result


flow._rest_scale_rows = _rest_scale_rows


def rest_producoes_flow():
    denied = flow._deny("restaurante")
    if denied:
        return denied
    rest = flow._rest_current()
    expected = _rest_scale_rows(rest)
    pending = [
        row.producao
        for row in expected
        if row.producao and row.producao.status == "pendente" and row.producao.valor_total > 0
    ]
    recent = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.status.in_(["aprovada", "recusada"]),
        )
        .order_by(ProducaoCooperado.decidido_em.desc(), ProducaoCooperado.id.desc())
        .limit(50)
        .all()
    )
    display_name = " ".join((rest.nome or "ESTABELECIMENTO").replace("_", " ").split())
    return render_template(
        "rest_producoes_pendentes.html",
        restaurante=rest,
        display_name=display_name,
        pendentes=pending,
        recentes=recent,
        previstas=expected,
    )


def rest_launch_scale_anytime(scale_id: int):
    denied = flow._deny("restaurante")
    if denied:
        return denied
    rest = flow._rest_current()
    row = next((item for item in _rest_scale_rows(rest) if item.escala.id == scale_id), None)
    if not row:
        abort(404)
    if not row.cooperado:
        flash("A escala não possui cooperado válido.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))
    if row.lancamento or (row.producao and row.producao.status == "aprovada"):
        flash("Essa produção já foi lançada. Nenhuma duplicidade foi criada.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))
    if row.producao and row.producao.status == "pendente" and row.producao.valor_total > 0:
        flash("O cooperado já informou. Confira, altere se necessário e aprove.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))

    quantity = request.form.get("qtd_entregas", type=int) or 0
    total = upgrade._money(request.form.get("valor_total"))
    if quantity <= 0 or total <= 0:
        flash("Informe a quantidade e o valor total.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))

    existing = upgrade._find_existing_launch(rest.id, row.cooperado.id, row.data, row.inicio, row.fim)
    if existing:
        flash("Essa produção já foi lançada. Nenhuma duplicidade foi criada.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))

    launch = Lancamento(
        restaurante_id=rest.id,
        cooperado_id=row.cooperado.id,
        descricao="Produção lançada pelo estabelecimento",
        valor=total,
        data=row.data,
        hora_inicio=row.inicio,
        hora_fim=row.fim,
        qtd_entregas=quantity,
    )
    db.session.add(launch)
    db.session.flush()

    item = row.producao
    old_status = item.status if item else None
    if not item:
        item = ProducaoCooperado(
            cooperado_id=row.cooperado.id,
            restaurante_id=rest.id,
            escala_id=row.escala.id,
            data=row.data,
            hora_inicio=row.inicio,
            hora_fim=row.fim,
        )
        db.session.add(item)
        db.session.flush()
    item.lancamento_id = launch.id
    item.qtd_entregas = quantity
    item.valor_total = total
    item.valor_unitario = round(total / quantity, 6)
    item.descricao = "Produção lançada pelo estabelecimento"
    item.status = "aprovada"
    item.motivo_recusa = None
    item.decidido_em = datetime.utcnow()
    upgrade._history(item, old_status=old_status, new_status="aprovada", reason="Lançamento direto pelo estabelecimento")

    try:
        db.session.commit()
        flash("Produção lançada sem duplicidade.", "success")
    except IntegrityError:
        db.session.rollback()
        flash("Essa produção já existe. Nenhuma duplicidade foi criada.", "warning")
    return redirect(url_for("rest_producoes_pendentes"))


def rest_approve_adjusted(item_id: int):
    if not _role_is("restaurante"):
        return redirect(url_for("login"))
    rest = flow._rest_current()
    try:
        item = upgrade._lock_production(item_id)
        if not item or item.restaurante_id != rest.id:
            abort(404)
        if item.status != "pendente":
            flash("Esta produção já foi tratada.", "warning")
            return redirect(url_for("rest_producoes_pendentes"))

        quantity = request.form.get("qtd_entregas", type=int)
        if quantity is None:
            quantity = int(item.qtd_entregas or 0)
        total = upgrade._money(request.form.get("valor_total"), item.valor_total)
        if quantity <= 0 or total <= 0:
            flash("Informe quantidade e valor total válidos.", "warning")
            return redirect(url_for("rest_producoes_pendentes"))

        existing = upgrade._find_existing_launch(
            item.restaurante_id,
            item.cooperado_id,
            item.data,
            item.hora_inicio,
            item.hora_fim,
        )
        old_status = item.status
        if existing:
            item.lancamento_id = existing.id
            item.qtd_entregas = int(existing.qtd_entregas or quantity)
            item.valor_total = float(existing.valor or total)
            item.valor_unitario = round(item.valor_total / item.qtd_entregas, 6) if item.qtd_entregas else 0
            message = "A produção já estava lançada e foi vinculada sem duplicidade."
        else:
            launch = Lancamento(
                restaurante_id=item.restaurante_id,
                cooperado_id=item.cooperado_id,
                descricao="Produção aprovada pelo estabelecimento",
                valor=total,
                data=item.data,
                hora_inicio=item.hora_inicio,
                hora_fim=item.hora_fim,
                qtd_entregas=quantity,
            )
            db.session.add(launch)
            db.session.flush()
            item.lancamento_id = launch.id
            item.qtd_entregas = quantity
            item.valor_total = total
            item.valor_unitario = round(total / quantity, 6)
            message = "Valor final aprovado e lançado no financeiro."

        item.status = "aprovada"
        item.motivo_recusa = None
        item.decidido_em = datetime.utcnow()
        upgrade._history(item, old_status=old_status, new_status="aprovada", reason="Valor final definido pelo estabelecimento")
        db.session.commit()
        flash(message, "success")
    except IntegrityError:
        db.session.rollback()
        flash("Essa produção já foi lançada. Nenhuma duplicidade foi criada.", "warning")
    return redirect(url_for("rest_producoes_pendentes"))


def _install_dashboard_template_patch():
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_producao_patched", False):
        return
    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "restaurante_dashboard.html":
            nav_marker = '<a data-target="lancamentos"'
            if 'data-coopex-producoes="1"' not in source and nav_marker in source:
                production_link = '''<a data-coopex-producoes="1" href="{{ url_for('rest_producoes_pendentes') }}">
          <i class="bi bi-clipboard-check"></i><span>Produções da Semana</span>
        </a>
        '''
                source = source.replace(nav_marker, production_link + nav_marker, 1)
            main_marker = '<main class="content">'
            if 'coopex-welcome-bar' not in source and main_marker in source:
                welcome = '''
      <div class="coopex-welcome-bar mb-3" style="background:#fff;border:1px solid var(--border);border-left:5px solid var(--royal);border-radius:16px;padding:13px 16px;box-shadow:0 6px 18px rgba(33,65,217,.06)">
        <div style="font-size:.75rem;font-weight:800;color:var(--muted);text-transform:uppercase;letter-spacing:.05em">Bem-vindo</div>
        <div style="font-size:1.05rem;font-weight:800;color:var(--royal)">{{ ((rest.nome if rest is defined and rest else (restaurante.nome if restaurante is defined and restaurante else 'ESTABELECIMENTO'))|replace('_',' ')) }}</div>
      </div>
'''
                source = source.replace(main_marker, main_marker + welcome, 1)
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_producao_patched = True
    app.jinja_env.cache.clear()


_install_dashboard_template_patch()

flow.coop_producao_flow = coop_producao_locked_flow
flow.rest_producoes_flow = rest_producoes_flow
flow.rest_launch_scale = rest_launch_scale_anytime
app.view_functions["coop_producao"] = coop_producao_locked_flow
app.view_functions["coop_producao_nova"] = coop_producao_locked_flow
app.view_functions["coop_producao_editar"] = coop_producao_edit_locked
app.view_functions["rest_producoes_pendentes"] = rest_producoes_flow
app.view_functions["rest_lancar_producao_escala"] = rest_launch_scale_anytime
app.view_functions["rest_producao_aprovar"] = rest_approve_adjusted
