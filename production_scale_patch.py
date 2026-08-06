from __future__ import annotations

from datetime import datetime

from flask import abort, flash, redirect, render_template, request, session, url_for
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

import production_scale_flow as flow

app = flow.app
db = flow.db
upgrade = flow.upgrade
Restaurante = flow.Restaurante
Lancamento = flow.Lancamento
ProducaoCooperado = flow.ProducaoCooperado
TZ = flow.TZ


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
    text = str(scale.data or "").strip().casefold()
    return next((value for key, value in flow._WEEKDAYS.items() if key in text), None)


# O sistema legado representa segunda=1 e domingo=7. O fluxo novo trabalha
# com weekday do Python (segunda=0), portanto a conversão é obrigatória.
flow._weekday_for_scale = _weekday_zero_based


def _role_is(role: str) -> bool:
    return (session.get("user_tipo") or "").strip().lower() == role


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
        upgrade._history(
            item,
            old_status=old_status,
            new_status="aprovada",
            reason="Valor final definido pelo estabelecimento",
        )
        db.session.commit()
        flash(message, "success")
    except IntegrityError:
        db.session.rollback()
        flash("Essa produção já foi lançada. Nenhuma duplicidade foi criada.", "warning")
    return redirect(url_for("rest_producoes_pendentes"))


_legacy_portal = app.view_functions.get("portal_restaurante")


def portal_restaurante_light():
    if not _role_is("restaurante"):
        return redirect(url_for("login"))

    requested_view = (request.args.get("view") or "").strip().lower()
    if requested_view not in {"", "home", "inicio"}:
        if _legacy_portal:
            return _legacy_portal()
        abort(404)

    rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first()
    if not rest:
        return "Seu usuário não está vinculado a um estabelecimento.", 403
    if getattr(rest, "eh_farmacia", False) and _legacy_portal:
        return _legacy_portal()

    today = datetime.now(TZ).date()
    pending_count = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.status == "pendente",
            ProducaoCooperado.valor_total > 0,
        ).count()
    )
    today_total, today_count = (
        db.session.query(
            func.coalesce(func.sum(Lancamento.valor), 0.0),
            func.count(Lancamento.id),
        )
        .filter(
            Lancamento.restaurante_id == rest.id,
            Lancamento.data == today,
        )
        .one()
    )
    display_name = " ".join((rest.nome or "ESTABELECIMENTO").replace("_", " ").split())
    return render_template(
        "restaurante_home_leve.html",
        rest=rest,
        display_name=display_name,
        pending_count=int(pending_count or 0),
        today_total=float(today_total or 0),
        today_count=int(today_count or 0),
        today=today,
    )


app.view_functions["rest_producao_aprovar"] = rest_approve_adjusted
if _legacy_portal:
    app.view_functions["portal_restaurante"] = portal_restaurante_light
