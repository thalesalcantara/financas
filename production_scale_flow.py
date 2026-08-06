from __future__ import annotations

from datetime import date, datetime, time, timedelta
from types import SimpleNamespace

from flask import abort, flash, redirect, render_template, request, session, url_for
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError

import coopex_upgrade as upgrade

app = upgrade.app
db = upgrade.db
legacy = upgrade.legacy
Usuario = upgrade.Usuario
Cooperado = upgrade.Cooperado
Restaurante = upgrade.Restaurante
Lancamento = upgrade.Lancamento
Escala = upgrade.Escala
TrocaSolicitacao = upgrade.TrocaSolicitacao
ProducaoCooperado = upgrade.ProducaoCooperado
TZ = upgrade.TZ

_WEEKDAYS = {
    "segunda": 0,
    "terca": 1,
    "terça": 1,
    "quarta": 2,
    "quinta": 3,
    "sexta": 4,
    "sabado": 5,
    "sábado": 5,
    "domingo": 6,
}


def _deny(role: str):
    if (session.get("user_tipo") or "").strip().lower() != role:
        return redirect(url_for("login"))
    return None


def _coop_current() -> Cooperado:
    return Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()


def _rest_current() -> Restaurante:
    return Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()


def _scale_belongs_to_coop(scale: Escala, coop: Cooperado) -> bool:
    if scale.cooperado_id:
        return scale.cooperado_id == coop.id
    return (scale.cooperado_nome or "").strip().casefold() == (coop.nome or "").strip().casefold()


def _weekday_for_scale(scale: Escala) -> int | None:
    helper = getattr(legacy, "_weekday_from_data_str", None)
    if helper:
        try:
            value = helper(scale.data)
            if value is not None:
                return int(value)
        except Exception:
            pass
    text = str(scale.data or "").strip().casefold()
    return next((value for key, value in _WEEKDAYS.items() if key in text), None)


def _scale_date(scale: Escala, today: date) -> date | None:
    parsed = upgrade._parse_date(scale.data)
    if parsed:
        return parsed
    weekday = _weekday_for_scale(scale)
    if weekday is None:
        return None
    monday = today - timedelta(days=today.weekday())
    return monday + timedelta(days=weekday)


def _end_at(scale_date: date | None, start: str, end: str) -> datetime | None:
    if not scale_date or not end:
        return None
    end_minutes = upgrade._minutes(end)
    if end_minutes is None:
        return None
    start_minutes = upgrade._minutes(start)
    result = datetime.combine(scale_date, time(end_minutes // 60, end_minutes % 60), tzinfo=TZ)
    if start_minutes is not None and end_minutes <= start_minutes:
        result += timedelta(days=1)
    return result


def _resolve_restaurants(scales: list[Escala]) -> dict[int, Restaurante | None]:
    restaurants = Restaurante.query.filter(Restaurante.ativo.is_(True)).order_by(Restaurante.nome.asc()).all()
    by_id = {r.id: r for r in restaurants}
    by_name = {(r.nome or "").strip().casefold(): r for r in restaurants}
    result: dict[int, Restaurante | None] = {}
    for scale in scales:
        restaurant = by_id.get(scale.restaurante_id) if scale.restaurante_id else None
        if not restaurant:
            contract = " ".join(str(scale.contrato or "").split()).casefold()
            restaurant = by_name.get(contract)
            if not restaurant and contract:
                restaurant = next((r for name, r in by_name.items() if contract in name or name in contract), None)
        result[scale.id] = restaurant
    return result


def _find_launch(launches_by_day: dict, rest_id: int, data_ref: date, start: str, end: str):
    for launch in launches_by_day.get((rest_id, data_ref), []):
        if upgrade._overlap(upgrade._norm_time(launch.hora_inicio), upgrade._norm_time(launch.hora_fim), start, end):
            return launch
    return None


def _coop_scale_rows(coop: Cooperado):
    now = datetime.now(TZ)
    today = now.date()
    scales = (
        Escala.query.filter(
            or_(
                Escala.cooperado_id == coop.id,
                func.lower(func.trim(Escala.cooperado_nome)) == (coop.nome or "").strip().lower(),
            )
        )
        .order_by(Escala.id.asc())
        .limit(120)
        .all()
    )
    rest_by_scale = _resolve_restaurants(scales)
    dated = [(scale, _scale_date(scale, today)) for scale in scales]
    valid_dates = [d for _, d in dated if d]
    date_min, date_max = (min(valid_dates), max(valid_dates)) if valid_dates else (today, today)

    productions = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.cooperado_id == coop.id,
            ProducaoCooperado.data >= date_min,
            ProducaoCooperado.data <= date_max,
        )
        .order_by(ProducaoCooperado.id.desc())
        .all()
    )
    by_scale = {p.escala_id: p for p in productions if p.escala_id}
    by_slot = {
        (p.restaurante_id, p.data, upgrade._norm_time(p.hora_inicio), upgrade._norm_time(p.hora_fim)): p
        for p in productions
    }
    launches = (
        Lancamento.query.filter(
            Lancamento.cooperado_id == coop.id,
            Lancamento.data >= date_min,
            Lancamento.data <= date_max,
        )
        .order_by(Lancamento.id.desc())
        .all()
    )
    launches_by_day: dict[tuple[int, date], list[Lancamento]] = {}
    for launch in launches:
        launches_by_day.setdefault((launch.restaurante_id, launch.data), []).append(launch)

    rows = []
    dirty = False
    for scale, data_ref in dated:
        restaurant = rest_by_scale.get(scale.id)
        start, end = upgrade._times_from_text(scale.horario)
        end_at = _end_at(data_ref, start, end)
        finished = bool(end_at and now >= end_at)
        if data_ref and data_ref < today and not end_at:
            finished = True
        production = by_scale.get(scale.id)
        if not production and restaurant and data_ref:
            production = by_slot.get((restaurant.id, data_ref, start, end))
        launch = _find_launch(launches_by_day, restaurant.id, data_ref, start, end) if restaurant and data_ref else None

        if production and launch and production.status != "aprovada":
            production.status = "aprovada"
            production.lancamento_id = launch.id
            production.valor_total = float(launch.valor or production.valor_total or 0)
            production.qtd_entregas = int(launch.qtd_entregas or production.qtd_entregas or 0)
            production.valor_unitario = round(production.valor_total / production.qtd_entregas, 2) if production.qtd_entregas else 0
            production.decidido_em = datetime.utcnow()
            dirty = True

        total = float((launch.valor if launch else None) or (production.valor_total if production else 0) or 0)
        quantity = int((launch.qtd_entregas if launch else None) or (production.qtd_entregas if production else 0) or 0)
        if launch or (production and production.status == "aprovada"):
            color, label, can_submit = "green", "Lançada", False
        elif production and production.status == "pendente" and total > 0:
            color, label, can_submit = "green", "Informada · aguardando aprovação", False
        elif production and production.status == "recusada":
            color, label, can_submit = "red", "Correção solicitada", finished
        elif not finished:
            color, label, can_submit = "red", "Pendente · libera após o horário", False
        else:
            color, label, can_submit = "red", "Pendente · informar produção", bool(restaurant)

        rows.append(SimpleNamespace(
            escala=scale, restaurante=restaurant, data=data_ref, inicio=start, fim=end,
            fim_em=end_at, finalizada=finished, producao=production, lancamento=launch,
            valor_total=total, qtd_entregas=quantity, color=color,
            status_label=label, pode_lancar=can_submit,
        ))

    if dirty:
        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
    rows.sort(key=lambda row: (row.data or date.max, row.inicio or "", row.escala.id))
    return rows


def coop_producao_flow():
    denied = _deny("cooperado")
    if denied:
        return denied
    coop = _coop_current()
    rows = _coop_scale_rows(coop)

    if request.method == "POST":
        scale_id = request.form.get("escala_id", type=int)
        scale = db.session.get(Escala, scale_id) if scale_id else None
        if not scale or not _scale_belongs_to_coop(scale, coop):
            flash("Escala inválida.", "danger")
            return redirect(url_for("coop_producao"))
        row = next((item for item in rows if item.escala.id == scale.id), None)
        if not row or not row.restaurante:
            flash("Essa escala não está vinculada a um estabelecimento.", "warning")
            return redirect(url_for("coop_producao"))
        if not row.finalizada:
            flash("A produção só pode ser aberta depois que o horário terminar.", "warning")
            return redirect(url_for("coop_producao"))
        if row.lancamento or (row.producao and row.producao.status == "aprovada"):
            flash("Essa produção já foi lançada. Nenhuma duplicidade foi criada.", "warning")
            return redirect(url_for("coop_producao"))
        if row.producao and row.producao.status == "pendente" and row.producao.valor_total > 0:
            flash("A produção já foi informada e aguarda o estabelecimento.", "warning")
            return redirect(url_for("coop_producao"))

        quantity = request.form.get("qtd_entregas", type=int) or 0
        total = upgrade._money(request.form.get("valor_total"))
        if quantity <= 0 or total <= 0:
            flash("Informe a quantidade e o valor total da produção.", "warning")
            return redirect(url_for("coop_producao", escala_id=scale.id))
        existing_launch = upgrade._find_existing_launch(row.restaurante.id, coop.id, row.data, row.inicio, row.fim)
        if existing_launch:
            flash("O estabelecimento já lançou essa produção. Nenhuma duplicidade foi criada.", "warning")
            return redirect(url_for("coop_producao"))

        item = row.producao
        old_status = item.status if item else None
        if not item:
            item = ProducaoCooperado(
                cooperado_id=coop.id,
                restaurante_id=row.restaurante.id,
                escala_id=scale.id,
                data=row.data,
                hora_inicio=row.inicio,
                hora_fim=row.fim,
            )
            db.session.add(item)
            db.session.flush()
        item.qtd_entregas = quantity
        item.valor_total = total
        item.valor_unitario = round(total / quantity, 6)
        item.descricao = "Produção informada pelo cooperado"
        item.status = "pendente"
        item.motivo_recusa = None
        item.decidido_em = None
        item.revisao = int(item.revisao or 0) + 1
        upgrade._history(item, old_status=old_status, new_status="pendente", reason="Produção informada após o término do horário")
        try:
            db.session.commit()
            flash("Produção informada. O estabelecimento já pode conferir e aprovar.", "success")
        except IntegrityError:
            db.session.rollback()
            flash("Essa produção já existe. Nenhuma duplicidade foi criada.", "warning")
        return redirect(url_for("coop_producao"))

    selected_id = request.args.get("escala_id", type=int)
    selected = next((row for row in rows if row.escala.id == selected_id), None)
    if selected and not selected.pode_lancar:
        selected = None
    return render_template("coop_producao.html", cooperado=coop, previsoes=rows, selecionada=selected, now=datetime.now(TZ))


def coop_agenda_flow():
    denied = _deny("cooperado")
    if denied:
        return denied
    coop = _coop_current()
    rows = _coop_scale_rows(coop)
    selected_day = (request.args.get("dia") or "").strip().lower()
    days, items, seen = [], [], set()
    for row in rows:
        label = str(row.escala.data or "Sem dia").strip() or "Sem dia"
        key = label.lower()
        if key not in seen:
            seen.add(key)
            days.append(label)
        if not selected_day or key == selected_day:
            items.append(row)
    other_coops = (
        Cooperado.query.join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Usuario.ativo.is_(True), Cooperado.id != coop.id)
        .order_by(Cooperado.nome.asc()).all()
    )
    sent = TrocaSolicitacao.query.filter_by(solicitante_id=coop.id).order_by(TrocaSolicitacao.id.desc()).limit(30).all()
    received_count = TrocaSolicitacao.query.filter_by(destino_id=coop.id, status="pendente").count()
    return render_template(
        "coop_agenda.html", cooperado=coop, itens=items, dias=days,
        selected_day=selected_day, outros_cooperados=other_coops,
        trocas_enviadas=sent, trocas_recebidas_count=received_count,
        today=datetime.now(TZ).date(),
    )


def _scale_coop(scale: Escala, coops_by_id, coops_by_name):
    if scale.cooperado_id:
        return coops_by_id.get(scale.cooperado_id)
    return coops_by_name.get((scale.cooperado_nome or "").strip().casefold())


def _rest_scale_rows(rest: Restaurante):
    now = datetime.now(TZ)
    today = now.date()
    candidates = Escala.query.order_by(Escala.id.asc()).limit(500).all()
    rest_name = (rest.nome or "").strip().casefold()
    scales = []
    for scale in candidates:
        contract = " ".join(str(scale.contrato or "").split()).casefold()
        if scale.restaurante_id == rest.id or (contract and (contract in rest_name or rest_name in contract)):
            scales.append(scale)
    coops = Cooperado.query.order_by(Cooperado.nome.asc()).all()
    coops_by_id = {c.id: c for c in coops}
    coops_by_name = {(c.nome or "").strip().casefold(): c for c in coops}
    dates = [_scale_date(scale, today) for scale in scales]
    valid_dates = [d for d in dates if d]
    date_min, date_max = (min(valid_dates), max(valid_dates)) if valid_dates else (today, today)
    productions = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.data >= date_min,
            ProducaoCooperado.data <= date_max,
        ).order_by(ProducaoCooperado.id.desc()).all()
    )
    by_scale = {p.escala_id: p for p in productions if p.escala_id}
    launches = (
        Lancamento.query.filter(
            Lancamento.restaurante_id == rest.id,
            Lancamento.data >= date_min,
            Lancamento.data <= date_max,
        ).order_by(Lancamento.id.desc()).all()
    )
    launch_by_day = {}
    for launch in launches:
        launch_by_day.setdefault((launch.cooperado_id, launch.data), []).append(launch)

    rows = []
    for scale, data_ref in zip(scales, dates):
        coop = _scale_coop(scale, coops_by_id, coops_by_name)
        start, end = upgrade._times_from_text(scale.horario)
        end_at = _end_at(data_ref, start, end)
        finished = bool(end_at and now >= end_at)
        if data_ref and data_ref < today and not end_at:
            finished = True
        prod = by_scale.get(scale.id)
        launch = None
        if coop and data_ref:
            for candidate in launch_by_day.get((coop.id, data_ref), []):
                if upgrade._overlap(candidate.hora_inicio, candidate.hora_fim, start, end):
                    launch = candidate
                    break
        total = float((launch.valor if launch else None) or (prod.valor_total if prod else 0) or 0)
        if launch or (prod and prod.status == "aprovada"):
            color, label = "green", "Lançada"
        elif prod and prod.status == "pendente" and total > 0:
            color, label = "green", "Informada pelo cooperado"
        elif not finished:
            color, label = "red", "Pendente · libera após o horário"
        else:
            color, label = "red", "Pendente · estabelecimento pode lançar"
        rows.append(SimpleNamespace(
            escala=scale, cooperado=coop, data=data_ref, inicio=start, fim=end,
            finalizada=finished, producao=prod, lancamento=launch,
            valor_total=total, color=color, status_label=label,
            pode_lancar=bool(finished and coop and not launch and not (prod and prod.valor_total > 0)),
        ))
    rows.sort(key=lambda row: (row.data or date.max, row.inicio or "", row.escala.id))
    return rows


def rest_producoes_flow():
    denied = _deny("restaurante")
    if denied:
        return denied
    rest = _rest_current()
    expected = _rest_scale_rows(rest)
    pending = [row.producao for row in expected if row.producao and row.producao.status == "pendente" and row.producao.valor_total > 0]
    recent = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.status.in_(["aprovada", "recusada"]),
        ).order_by(ProducaoCooperado.decidido_em.desc(), ProducaoCooperado.id.desc()).limit(50).all()
    )
    return render_template("rest_producoes_pendentes.html", restaurante=rest, pendentes=pending, recentes=recent, previstas=expected)


def rest_launch_scale(scale_id: int):
    denied = _deny("restaurante")
    if denied:
        return denied
    rest = _rest_current()
    row = next((item for item in _rest_scale_rows(rest) if item.escala.id == scale_id), None)
    if not row:
        abort(404)
    if not row.finalizada:
        flash("A produção só pode ser lançada depois que o horário terminar.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))
    if not row.cooperado:
        flash("A escala não possui cooperado válido.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))
    if row.lancamento:
        flash("Essa produção já foi lançada. Nenhuma duplicidade foi criada.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))
    if row.producao and row.producao.status == "pendente" and row.producao.valor_total > 0:
        flash("O cooperado já informou essa produção. Apenas confira e aprove.", "warning")
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
        restaurante_id=rest.id, cooperado_id=row.cooperado.id,
        descricao="Produção lançada pelo estabelecimento", valor=total,
        data=row.data, hora_inicio=row.inicio, hora_fim=row.fim,
        qtd_entregas=quantity,
    )
    db.session.add(launch)
    db.session.flush()
    item = row.producao
    old_status = item.status if item else None
    if not item:
        item = ProducaoCooperado(
            cooperado_id=row.cooperado.id, restaurante_id=rest.id,
            escala_id=row.escala.id, data=row.data,
            hora_inicio=row.inicio, hora_fim=row.fim,
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


app.view_functions["coop_producao"] = coop_producao_flow
app.view_functions["coop_producao_nova"] = coop_producao_flow
app.view_functions["coop_agenda"] = coop_agenda_flow
app.view_functions["rest_producoes_pendentes"] = rest_producoes_flow

if "rest_lancar_producao_escala" not in app.view_functions:
    app.add_url_rule(
        "/rest/producoes/escala/<int:scale_id>/lancar",
        endpoint="rest_lancar_producao_escala",
        view_func=rest_launch_scale,
        methods=["POST"],
    )
