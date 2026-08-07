from __future__ import annotations

import re
from datetime import datetime, timedelta

from flask import jsonify, request, session
from sqlalchemy import text as sa_text

import menu_horizontal_enforcer as menu
import production_shift_time_fix as shifts

app = menu.app
db = shifts.patch.db
Cooperado = shifts.patch.Cooperado
Restaurante = shifts.patch.Restaurante
Lancamento = shifts.patch.Lancamento
ProducaoCooperado = shifts.patch.ProducaoCooperado
TZ = shifts.patch.TZ
BUILD = "20260807-1009"


if "coop_latest_incoming_evaluation" not in app.view_functions:
    @app.get("/api/coop/avaliacoes/latest", endpoint="coop_latest_incoming_evaluation")
    def coop_latest_incoming_evaluation():
        if (session.get("user_tipo") or "").strip().lower() != "cooperado":
            return jsonify(ok=False, latest_id=0), 403

        coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first()
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


def _week_pending_rows(rest):
    """Pendências vencidas da semana, separadas por escala/turno."""
    now = datetime.now(TZ)
    today = now.date()
    monday = today - timedelta(days=today.weekday())

    scales = [
        scale
        for scale in shifts.query_override._rest_scales_indexed(rest)
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
        for coop in Cooperado.query.order_by(Cooperado.nome.asc()).all():
            key = shifts.patch._norm(coop.nome)
            if key:
                coops_by_name[key] = coop

    productions = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.data >= monday,
            ProducaoCooperado.data <= today,
        )
        .order_by(ProducaoCooperado.id.desc())
        .all()
    )
    by_scale = {p.escala_id: p for p in productions if p.escala_id}
    by_slot = {
        (
            p.cooperado_id,
            p.data,
            shifts.patch.upgrade._norm_time(p.hora_inicio),
            shifts.patch.upgrade._norm_time(p.hora_fim),
        ): p
        for p in productions
    }

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

        production = by_scale.get(scale.id) or by_slot.get((coop.id, data_ref, start_time, end_time))
        if production and production.status == "aprovada":
            continue

        sent = bool(production and production.status == "pendente" and float(production.valor_total or 0) > 0)
        result.append({
            "cooperado_id": coop.id,
            "cooperado_nome": coop.nome,
            "turno": scale.turno or "—",
            "horario": scale.horario or (f"{start_time} às {end_time}" if start_time and end_time else "—"),
            "contrato": scale.contrato or rest.nome,
            "data": data_ref.strftime("%d/%m/%Y"),
            "escala_id": scale.id,
            "aguardando_aprovacao": sent,
        })

    result.sort(key=lambda item: (item["data"], item["horario"], item["cooperado_nome"].lower()))
    return result


@app.context_processor
def _coopex_week_pending_context():
    context = {"coopex_rest_week_pending_rows": []}
    if (session.get("user_tipo") or "").strip().lower() != "restaurante":
        return context
    if request.endpoint != "portal_restaurante":
        return context
    if (request.args.get("view") or "lancar").strip().lower() != "lancar":
        return context

    try:
        rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first()
        if rest:
            context["coopex_rest_week_pending_rows"] = _week_pending_rows(rest)
    except Exception:
        db.session.rollback()
        app.logger.exception("Falha ao montar pendências semanais do estabelecimento")
    return context


def _install_coop_ui() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_coop_v4_ui", False):
        return

    original_get_source = loader.get_source

    css_tag = """<link rel="stylesheet" href="{{ url_for('static', filename='css/cooperado_v4.css', v='__BUILD__') }}">""".replace("__BUILD__", BUILD)
    js_tag = """<script src="{{ url_for('static', filename='js/cooperado_v4.js', v='__BUILD__') }}"></script>""".replace("__BUILD__", BUILD)

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)

        if template == "painel_cooperado.html":
            if "cooperado_v4.css" not in source:
                source = source.replace("</head>", "  " + css_tag + "\n</head>", 1)
            if "cooperado_v4.js" not in source:
                source = source.replace("</body>", js_tag + "\n</body>", 1)

            production_card_pattern = re.compile(
                r'(<section class="tab-pane-custom" id="tab-producoes">.*?'
                r'{%\s*for\s+lanc\s+in\s+producoes\s*%}\s*)'
                r'<div class="item-card">',
                re.S,
            )
            source = production_card_pattern.sub(
                r'\1<div class="item-card coopex-production-card">',
                source,
                count=1,
            )

            title = '<div class="item-title">{{ lanc.descricao }}</div>'
            state = title + """
                <div class="d-flex flex-wrap gap-1 align-items-center">
                  <span class="coopex-prod-state"><i class="bi bi-check-circle-fill"></i> Produção aprovada</span>
                  {% if lanc.minha_avaliacao is not none %}
                    <span class="coopex-prod-state"><i class="bi bi-star-fill"></i> Avaliada · {{ lanc.minha_avaliacao }}/5</span>
                  {% else %}
                    <span class="coopex-prod-state pending-rating"><i class="bi bi-star"></i> Avaliação pendente</span>
                  {% endif %}
                </div>"""
            source = source.replace(title, state, 1)

        elif template == "coop_producao.html":
            source = source.replace(
                '<form method="post" class="form">',
                '<form method="post" class="form" data-coopex-production-submit="1">',
                1,
            )
            if "cooperado_v4.js" not in source:
                source = source.replace("</body>", js_tag + "\n</body>", 1)

        elif template == "restaurante_dashboard.html":
            source = source.replace(
                "{% set _pendencias = pendencias_lancamento if pendencias_lancamento is defined and pendencias_lancamento is not none else [] %}",
                "{% set _pendencias = coopex_rest_week_pending_rows|default([]) %}",
                1,
            )
            source = source.replace(
                "Estes cooperados estão com horário encerrado e ainda não tiveram produção lançada no dia.",
                "Pendências vencidas da semana permanecem aqui até o estabelecimento lançar ou aprovar o valor enviado pelo cooperado.",
                1,
            )
            source = source.replace(
                "Nenhum cooperado com horário encerrado está pendente de lançamento no momento.",
                "Nenhuma escala vencida da semana está pendente de lançamento ou aprovação.",
                1,
            )

        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_coop_v4_ui = True
    app.jinja_env.cache.clear()


_install_coop_ui()
