from __future__ import annotations

from datetime import datetime

from flask import abort, flash, redirect, request, session, url_for

import operational_rules_v5 as ops

app = ops.app
upgrade = ops.upgrade
db = upgrade.db
ProducaoCooperado = upgrade.ProducaoCooperado


def _role_is(role: str) -> bool:
    return (session.get("user_tipo") or "").strip().lower() == role


def rest_reject_and_lock(item_id: int):
    if not _role_is("restaurante"):
        return redirect(url_for("login"))

    rest = ops.Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    item = upgrade._lock_production(item_id)
    if not item or item.restaurante_id != rest.id:
        abort(404)

    if item.status != "pendente":
        flash("Esta produção já foi tratada.", "warning")
    else:
        reason = (request.form.get("motivo") or "").strip() or (
            "Recusada pelo estabelecimento. Novo envio do cooperado bloqueado."
        )
        old_status = item.status
        item.status = "recusada"
        item.motivo_recusa = reason
        item.decidido_em = datetime.utcnow()
        upgrade._history(
            item,
            old_status=old_status,
            new_status="recusada",
            reason=reason,
        )
        db.session.commit()
        flash(
            "Produção recusada. O cooperado não pode reenviar esta escala; somente o estabelecimento pode lançar o valor.",
            "info",
        )

    if request.form.get("return_dashboard") == "1":
        params = {"view": "lancar"}
        data_inicio = (request.form.get("data_inicio") or "").strip()
        data_fim = (request.form.get("data_fim") or "").strip()
        if data_inicio:
            params["data_inicio"] = data_inicio
        if data_fim:
            params["data_fim"] = data_fim
        return redirect(url_for("portal_restaurante", **params))

    return redirect(url_for("rest_producoes_pendentes"))


# A rota já existe no módulo principal; substituímos apenas a função final.
app.view_functions["rest_producao_recusar"] = rest_reject_and_lock
