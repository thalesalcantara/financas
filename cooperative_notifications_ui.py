from __future__ import annotations

import re
from datetime import datetime, timedelta

from flask import flash, g, has_request_context, jsonify, redirect, request, session, url_for
from sqlalchemy import text as sa_text

import production_shift_time as shifts
import production_ui as perf

app = shifts.patch.app
legacy = shifts.patch.flow.legacy
role_required = legacy.role_required
db = shifts.patch.db
Usuario = shifts.patch.flow.Usuario
Cooperado = shifts.patch.Cooperado
Restaurante = shifts.patch.Restaurante
Escala = shifts.patch.Escala
Lancamento = shifts.patch.Lancamento
ProducaoCooperado = shifts.patch.ProducaoCooperado
TZ = shifts.patch.TZ
BUILD = "20260807-1458"

_NO_SHOW_PREFIX = "NAO_COMPARECEU|"
_SUBSTITUTE_PREFIX = "SUBSTITUIDO|"


if "coop_latest_incoming_evaluation" not in app.view_functions:
    @app.get("/api/coop/avaliacoes/latest", endpoint="coop_latest_incoming_evaluation")
    def coop_latest_incoming_evaluation():
        if (session.get("user_tipo") or "").strip().lower() != "cooperado":
            return jsonify(ok=False, latest_id=0), 403

        coop = legacy.request_cooperado()
        if not coop:
            return jsonify(ok=False, latest_id=0), 404

        try:
            row = db.session.execute(
                sa_text(
                    "SELECT id, estrelas_geral, criado_em "
                    "FROM avaliacoes WHERE cooperado_id=:coop_id "
                    "ORDER BY id DESC LIMIT 1"
                ),
                {"coop_id": coop.id},
            ).mappings().first()
        except Exception:
            db.session.rollback()
            app.logger.exception("Falha ao consultar a última avaliação do cooperado")
            return jsonify(ok=False, cooperado_id=coop.id, latest_id=0), 500

        return jsonify(
            ok=True,
            cooperado_id=coop.id,
            latest_id=int(row["id"] if row else 0),
            estrelas=float(row["estrelas_geral"] or 0) if row else 0,
            criado_em=(row["criado_em"].isoformat() if row and row["criado_em"] else None),
        )


def _rest_current():
    return legacy.request_restaurante()


def _active_substitute_rows():
    return (
        db.session.query(Cooperado.id, Cooperado.nome)
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Usuario.ativo.is_(True))
        .order_by(Cooperado.nome.asc())
        .all()
    )


def _scale_belongs_to_rest(scale, rest) -> bool:
    if not scale or not rest:
        return False
    if scale.restaurante_id == rest.id:
        return True
    contract = shifts.patch._norm(scale.contrato)
    rest_name = shifts.patch._norm(rest.nome)
    return bool(contract and rest_name and (contract == rest_name or rest_name in contract or contract in rest_name))


def _coops_by_normalized_name():
    if has_request_context() and hasattr(g, "_notifications_coops_by_name"):
        return g._notifications_coops_by_name
    result = {}
    for coop in Cooperado.query.order_by(Cooperado.nome.asc()).all():
        key = shifts.patch._norm(coop.nome)
        if key:
            result[key] = coop
    if has_request_context():
        g._notifications_coops_by_name = result
    return result


def _scale_coop(scale):
    if scale.cooperado_id:
        coop = db.session.get(Cooperado, scale.cooperado_id)
        if coop:
            return coop
    target = shifts.patch._norm(scale.cooperado_nome)
    if not target:
        return None
    return _coops_by_normalized_name().get(target)


def _block_scale_for_coop(rest, scale, coop, reason: str):
    today = datetime.now(TZ).date()
    data_ref = shifts.exact_scale_date(scale, today)
    if not data_ref:
        raise ValueError("Não foi possível identificar a data desta escala.")

    start_time, end_time = shifts.patch.upgrade._times_from_text(scale.horario)
    start_time = shifts.patch.upgrade._norm_time(start_time) or ""
    end_time = shifts.patch.upgrade._norm_time(end_time) or ""

    item = (
        ProducaoCooperado.query.filter_by(escala_id=scale.id, cooperado_id=coop.id)
        .order_by(ProducaoCooperado.id.desc())
        .first()
    )
    if not item:
        item = ProducaoCooperado.query.filter_by(
            cooperado_id=coop.id,
            restaurante_id=rest.id,
            data=data_ref,
            hora_inicio=start_time,
            hora_fim=end_time,
        ).order_by(ProducaoCooperado.id.desc()).first()

    if item and (item.status == "aprovada" or item.lancamento_id):
        raise ValueError("Esta produção já foi lançada e não pode ser marcada como ausência.")

    old_status = item.status if item else None
    if not item:
        item = ProducaoCooperado(
            cooperado_id=coop.id,
            restaurante_id=rest.id,
            escala_id=scale.id,
            data=data_ref,
            hora_inicio=start_time,
            hora_fim=end_time,
            qtd_entregas=0,
            valor_unitario=0,
            valor_total=0,
        )
        db.session.add(item)
        db.session.flush()

    item.escala_id = scale.id
    item.status = "recusada"
    item.motivo_recusa = reason
    item.decidido_em = datetime.utcnow()
    item.atualizado_em = datetime.utcnow()
    try:
        shifts.patch.upgrade._history(
            item,
            old_status=old_status,
            new_status="recusada",
            reason=reason,
        )
    except Exception:
        app.logger.exception("Falha ao registrar histórico da pendência da escala %s", scale.id)
    return item


if "rest_pendencia_nao_compareceu" not in app.view_functions:
    @app.post(
        "/portal/restaurante/pendencia/<int:scale_id>/nao-compareceu",
        endpoint="rest_pendencia_nao_compareceu",
    )
    def rest_pendencia_nao_compareceu(scale_id: int):
        if (session.get("user_tipo") or "").strip().lower() != "restaurante":
            return redirect(url_for("login"))

        rest = _rest_current()
        scale = Escala.query.get(scale_id)
        if not rest or not scale or not _scale_belongs_to_rest(scale, rest):
            flash("Pendência não localizada para este estabelecimento.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))

        coop = _scale_coop(scale)
        if not coop:
            flash("Não foi possível identificar o cooperado desta escala.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))

        try:
            reason = f"{_NO_SHOW_PREFIX}{coop.nome}|Marcado pelo estabelecimento"
            _block_scale_for_coop(rest, scale, coop, reason)
            db.session.commit()
            flash(
                f"{coop.nome} foi marcado como não compareceu nesta escala. A produção ficou bloqueada para o cooperado.",
                "info",
            )
        except ValueError as exc:
            db.session.rollback()
            flash(str(exc), "warning")
        except Exception:
            db.session.rollback()
            app.logger.exception("Falha ao marcar não comparecimento da escala %s", scale_id)
            flash("Não foi possível marcar o não comparecimento.", "danger")

        return redirect(url_for("portal_restaurante", view="lancar"))


if "rest_pendencia_trocar_cooperado" not in app.view_functions:
    @app.post(
        "/portal/restaurante/pendencia/<int:scale_id>/trocar-cooperado",
        endpoint="rest_pendencia_trocar_cooperado",
    )
    def rest_pendencia_trocar_cooperado(scale_id: int):
        if (session.get("user_tipo") or "").strip().lower() != "restaurante":
            return redirect(url_for("login"))

        rest = _rest_current()
        scale = Escala.query.get(scale_id)
        if not rest or not scale or not _scale_belongs_to_rest(scale, rest):
            flash("Pendência não localizada para este estabelecimento.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))

        original = _scale_coop(scale)
        new_id = request.form.get("cooperado_id", type=int)
        substitute = (
            Cooperado.query.join(Usuario, Cooperado.usuario_id == Usuario.id)
            .filter(Cooperado.id == new_id, Usuario.ativo.is_(True))
            .first()
            if new_id else None
        )
        if not original or not substitute:
            flash("Selecione um cooperado ativo para substituir.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))
        if substitute.id == original.id:
            flash("Escolha um cooperado diferente do cooperado original.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))

        try:
            reason = f"{_SUBSTITUTE_PREFIX}{original.nome}|{substitute.nome}|Substituição informada pelo estabelecimento"
            _block_scale_for_coop(rest, scale, original, reason)
            scale.cooperado_id = substitute.id
            scale.cooperado_nome = substitute.nome
            db.session.commit()
            flash(
                f"A escala de {original.nome} foi transferida para {substitute.nome}. O lançamento desta pendência agora deve ser feito para o substituto.",
                "success",
            )
        except ValueError as exc:
            db.session.rollback()
            flash(str(exc), "warning")
        except Exception:
            db.session.rollback()
            app.logger.exception("Falha ao substituir cooperado da escala %s", scale_id)
            flash("Não foi possível trocar o cooperado desta pendência.", "danger")

        return redirect(url_for("portal_restaurante", view="lancar"))


def _week_pending_rows(rest, scales_source=None):
    """Pendências vencidas da semana, separadas por escala/turno."""
    now = datetime.now(TZ)
    today = now.date()
    monday = today - timedelta(days=today.weekday())

    source = scales_source if scales_source is not None else perf._rest_scales_indexed(rest)
    scales = [
        scale
        for scale in source
        if (lambda d: bool(d and monday <= d <= today))(shifts.exact_scale_date(scale, today))
    ]
    if not scales:
        return []

    coop_ids = {s.cooperado_id for s in scales if s.cooperado_id}
    coops_by_id = {
        c.id: c for c in Cooperado.query.filter(Cooperado.id.in_(coop_ids)).all()
    } if coop_ids else {}

    need_name = any(not s.cooperado_id and s.cooperado_nome for s in scales)
    coops_by_name = {}
    if need_name:
        coops_by_name = _coops_by_normalized_name()

    productions = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.data >= monday,
            ProducaoCooperado.data <= today,
        )
        .order_by(ProducaoCooperado.id.desc())
        .all()
    )
    by_scale_coop = {}
    by_slot = {}
    for production in productions:
        if production.escala_id:
            by_scale_coop.setdefault((production.escala_id, production.cooperado_id), production)
        slot = (
            production.cooperado_id,
            production.data,
            shifts.patch.upgrade._norm_time(production.hora_inicio),
            shifts.patch.upgrade._norm_time(production.hora_fim),
        )
        by_slot.setdefault(slot, production)

    launches = (
        Lancamento.query.filter(
            Lancamento.restaurante_id == rest.id,
            Lancamento.data >= monday,
            Lancamento.data <= today,
        )
        .order_by(Lancamento.id.desc())
        .all()
    )
    launches_by_day = {}
    for launch in launches:
        launches_by_day.setdefault((launch.cooperado_id, launch.data), []).append(launch)

    result = []
    for scale in scales:
        data_ref = shifts.exact_scale_date(scale, today)
        if not data_ref:
            continue

        coop = coops_by_id.get(scale.cooperado_id) if scale.cooperado_id else coops_by_name.get(shifts.patch._norm(scale.cooperado_nome))
        if not coop:
            continue

        start_time, end_time = shifts.patch.upgrade._times_from_text(scale.horario)
        end_at = shifts.flow._end_at(data_ref, start_time, end_time)

        if data_ref == today:
            if not end_at or now < end_at:
                continue

        launch = None
        for candidate in launches_by_day.get((coop.id, data_ref), []):
            if shifts.patch.upgrade._overlap(
                candidate.hora_inicio,
                candidate.hora_fim,
                start_time,
                end_time,
            ):
                launch = candidate
                break
        if launch:
            continue

        production = by_scale_coop.get((scale.id, coop.id)) or by_slot.get(
            (
                coop.id,
                data_ref,
                shifts.patch.upgrade._norm_time(start_time),
                shifts.patch.upgrade._norm_time(end_time),
            )
        )
        if production and production.status == "aprovada":
            continue
        if (
            production
            and production.status == "recusada"
            and str(production.motivo_recusa or "").startswith(_NO_SHOW_PREFIX)
        ):
            continue

        sent = bool(production and production.status == "pendente" and float(production.valor_total or 0) > 0)
        result.append({
            "cooperado_id": coop.id,
            "cooperado_nome": coop.nome,
            "turno": scale.turno or "—",
            "horario": scale.horario or (f"{start_time} às {end_time}" if start_time and end_time else "—"),
            "contrato": scale.contrato or rest.nome,
            "data": data_ref.strftime("%d/%m/%Y"),
            "data_iso": data_ref.isoformat(),
            "hora_inicio": shifts.patch.upgrade._norm_time(start_time) or "",
            "hora_fim": shifts.patch.upgrade._norm_time(end_time) or "",
            "escala_id": scale.id,
            "aguardando_aprovacao": sent,
        })

    result.sort(key=lambda item: (item["data"], item["horario"], item["cooperado_nome"].lower()))
    return result



def _shift_period_label(start_time) -> str:
    """Rótulo curto para múltiplos horários no mesmo dia."""
    try:
        s = shifts.patch.upgrade._norm_time(start_time) or ""
        hh = int(str(s).split(":", 1)[0])
        return "dia" if hh < 17 else "noite"
    except Exception:
        return "turno"


def _today_coop_launch_state(rest, pending_rows, scales_source=None):
    """Estado de lançamento de cada cooperado considerando SOMENTE as escalas de hoje."""
    now = datetime.now(TZ)
    today = now.date()

    scales = []
    source = scales_source if scales_source is not None else perf._rest_scales_indexed(rest)
    for scale in source:
        d = shifts.exact_scale_date(scale, today)
        if d == today:
            scales.append(scale)

    if not scales:
        return {}

    coop_ids = {s.cooperado_id for s in scales if s.cooperado_id}
    coops_by_id = {
        c.id: c for c in Cooperado.query.filter(Cooperado.id.in_(coop_ids)).all()
    } if coop_ids else {}
    coops_by_name = _coops_by_normalized_name()

    launches = (
        Lancamento.query.filter(
            Lancamento.restaurante_id == rest.id,
            Lancamento.data == today,
        )
        .order_by(Lancamento.id.desc())
        .all()
    )
    launches_by_coop = {}
    for launch in launches:
        launches_by_coop.setdefault(launch.cooperado_id, []).append(launch)

    pend_today_by_coop = {}
    for p in pending_rows or []:
        if p.get("data_iso") == today.isoformat():
            pend_today_by_coop.setdefault(int(p["cooperado_id"]), []).append(p)
    for rows in pend_today_by_coop.values():
        rows.sort(key=lambda x: (x.get("hora_inicio") or "", x.get("hora_fim") or ""))

    per_coop_scales = {}
    for scale in scales:
        coop = coops_by_id.get(scale.cooperado_id) if scale.cooperado_id else coops_by_name.get(shifts.patch._norm(scale.cooperado_nome))
        if not coop:
            continue
        start_time, end_time = shifts.patch.upgrade._times_from_text(scale.horario)
        per_coop_scales.setdefault(coop.id, []).append((scale, start_time, end_time))

    result = {}
    for coop_id, rows in per_coop_scales.items():
        rows.sort(key=lambda item: shifts.patch.upgrade._norm_time(item[1]) or "")
        launched_periods = []
        for scale, start_time, end_time in rows:
            found = False
            for launch in launches_by_coop.get(coop_id, []):
                if shifts.patch.upgrade._overlap(
                    launch.hora_inicio,
                    launch.hora_fim,
                    start_time,
                    end_time,
                ):
                    found = True
                    break
            if found:
                launched_periods.append(_shift_period_label(start_time))

        pends = pend_today_by_coop.get(coop_id, [])
        first_pending = pends[0] if pends else None

        if first_pending:
            status_label = "Com pendência"
            status_kind = "danger"
        elif launched_periods:
            if len(launched_periods) >= len(rows):
                status_label = "Lançamento OK"
                status_kind = "success"
            elif len(set(launched_periods)) == 1:
                status_label = "Lançada " + launched_periods[0]
                status_kind = "warning"
            else:
                status_label = "Produção lançada"
                status_kind = "warning"
        else:
            status_label = "Escalado"
            status_kind = "success"

        result[coop_id] = {
            "status_label": status_label,
            "status_kind": status_kind,
            "tem_producao_hoje": bool(launched_periods),
            "pendente_hoje": bool(first_pending),
            "pend_data": (first_pending or {}).get("data_iso", ""),
            "pend_inicio": (first_pending or {}).get("hora_inicio", ""),
            "pend_fim": (first_pending or {}).get("hora_fim", ""),
        }
    return result



@app.get("/api/rest/pendencias-semana", endpoint="rest_pending_week_state")
@role_required("restaurante")
def rest_pending_week_state():
    try:
        rest = legacy.request_restaurante()
        if not rest:
            return jsonify(ok=False, pendencias=[], status={}), 404
        indexed_scales = perf._rest_scales_indexed(rest)
        pending_rows = _week_pending_rows(rest, indexed_scales)
        status_map = _today_coop_launch_state(rest, pending_rows, indexed_scales)
        for p in pending_rows:
            try:
                p["nao_compareceu_url"] = url_for("rest_pendencia_nao_compareceu", scale_id=p.get("escala_id"))
                p["trocar_url"] = url_for("rest_pendencia_trocar_cooperado", scale_id=p.get("escala_id"))
            except Exception:
                p["nao_compareceu_url"] = ""
                p["trocar_url"] = ""
        return jsonify(ok=True, pendencias=pending_rows, status={str(k): v for k, v in status_map.items()})
    except Exception:
        db.session.rollback()
        app.logger.exception("Falha ao carregar pendências assíncronas")
        return jsonify(ok=False, pendencias=[], status={}), 500


@app.context_processor
def _coopex_week_pending_context():
    # Mantém o primeiro carregamento do estabelecimento leve.
    # Pendências/status são buscados por AJAX logo após a tela aparecer.
    substitutes = []
    if (session.get("user_tipo") or "").strip().lower() == "restaurante" and request.endpoint == "portal_restaurante":
        try:
            substitutes = [
                {"id": int(coop_id), "nome": nome}
                for coop_id, nome in _active_substitute_rows()
            ]
        except Exception:
            db.session.rollback()
            substitutes = []
    return {
        "coopex_rest_week_pending_rows": [],
        "coopex_rest_today_status_map": {},
        "coopex_rest_substitute_coops": substitutes,
    }