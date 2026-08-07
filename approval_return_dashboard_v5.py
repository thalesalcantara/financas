from __future__ import annotations

from datetime import datetime

from flask import abort, flash, redirect, request, session, url_for
from sqlalchemy.exc import IntegrityError

import production_scale_patch as patch

app = patch.app
db = patch.db
upgrade = patch.upgrade
Lancamento = patch.Lancamento
ProducaoCooperado = patch.ProducaoCooperado
Cooperado = patch.Cooperado


def _back_to_launch():
    params = {"view": "lancar"}
    data_inicio = (request.form.get("data_inicio") or request.args.get("data_inicio") or "").strip()
    data_fim = (request.form.get("data_fim") or request.args.get("data_fim") or "").strip()
    if data_inicio:
        params["data_inicio"] = data_inicio
    if data_fim:
        params["data_fim"] = data_fim
    return redirect(url_for("portal_restaurante", **params))


def rest_approve_return_dashboard(item_id: int):
    if (session.get("user_tipo") or "").strip().lower() != "restaurante":
        return redirect(url_for("login"))

    rest = patch.flow._rest_current()
    try:
        item = upgrade._lock_production(item_id)
        if not item or item.restaurante_id != rest.id:
            abort(404)

        coop = db.session.get(Cooperado, item.cooperado_id)
        coop_name = " ".join(((coop.nome if coop else "COOPERADO") or "COOPERADO").replace("_", " ").split())

        if item.status != "pendente":
            flash(
                f"A produção do cooperado {coop_name} já foi tratada. Não é necessário realizar outro lançamento referente a esta produção.",
                "info",
            )
            return _back_to_launch()

        quantity = request.form.get("qtd_entregas", type=int)
        if quantity is None:
            quantity = int(item.qtd_entregas or 0)
        total = upgrade._money(request.form.get("valor_total"), item.valor_total)
        if quantity <= 0 or total <= 0:
            flash("Informe quantidade e valor total válidos.", "warning")
            return _back_to_launch()

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

        flash(
            f"A produção do cooperado {coop_name} foi aprovada e já foi lançada normalmente. Não é necessário realizar outro lançamento referente a esta produção.",
            "success",
        )
    except IntegrityError:
        db.session.rollback()
        flash(
            "Esta produção já possui lançamento. Nenhum lançamento duplicado foi criado.",
            "warning",
        )

    return _back_to_launch()


app.view_functions["rest_producao_aprovar"] = rest_approve_return_dashboard
