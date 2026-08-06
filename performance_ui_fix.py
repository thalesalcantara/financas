from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

from flask import request, session, url_for
from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload

import production_ui_patch as ui

app = ui.app
backend = ui.backend
flow = ui.flow
db = backend.db
Escala = backend.Escala
Cooperado = backend.Cooperado
Restaurante = backend.Restaurante
Lancamento = backend.Lancamento
ProducaoCooperado = backend.ProducaoCooperado
TZ = backend.TZ


def _raw_variants(value: str) -> list[str]:
    raw = " ".join(str(value or "").strip().split()).casefold()
    variants = {raw, raw.replace("_", " "), raw.replace(" ", "_")}
    return [v for v in variants if v]


def _coop_scales_fast(coop):
    variants = _raw_variants(coop.nome)
    conditions = [Escala.cooperado_id == coop.id]
    if variants:
        conditions.append(func.lower(func.trim(Escala.cooperado_nome)).in_(variants))

    rows = (
        Escala.query.filter(or_(*conditions))
        .order_by(Escala.id.desc())
        .limit(260)
        .all()
    )

    # Fallback só para cadastros antigos com acento, espaço ou sublinhado diferente.
    if not rows:
        target = backend._norm(coop.nome)
        candidates = (
            Escala.query.filter(Escala.cooperado_nome.isnot(None))
            .order_by(Escala.id.desc())
            .limit(420)
            .all()
        )
        rows = [s for s in candidates if backend._norm(s.cooperado_nome) == target]

    seen = set()
    result = []
    for scale in reversed(rows):
        if scale.id not in seen:
            seen.add(scale.id)
            result.append(scale)
    return result


def _rest_scales_fast(rest):
    variants = _raw_variants(rest.nome)
    conditions = [Escala.restaurante_id == rest.id]
    for variant in variants:
        conditions.append(func.lower(func.trim(Escala.contrato)) == variant)
        conditions.append(func.lower(Escala.contrato).like(f"%{variant}%"))

    rows = (
        Escala.query.filter(or_(*conditions))
        .order_by(Escala.id.desc())
        .limit(320)
        .all()
    )

    if not rows:
        target = backend._norm(rest.nome)
        candidates = (
            Escala.query.filter(Escala.contrato.isnot(None))
            .order_by(Escala.id.desc())
            .limit(500)
            .all()
        )
        rows = [
            s for s in candidates
            if backend._norm(s.contrato)
            and (
                backend._norm(s.contrato) == target
                or target in backend._norm(s.contrato)
                or backend._norm(s.contrato) in target
            )
        ]

    seen = set()
    result = []
    for scale in reversed(rows):
        if scale.id not in seen:
            seen.add(scale.id)
            result.append(scale)
    return result


def _restaurant_map_fast(scales):
    result = {}
    ids = {s.restaurante_id for s in scales if s.restaurante_id}
    by_id = {}
    if ids:
        by_id = {r.id: r for r in Restaurante.query.filter(Restaurante.id.in_(ids)).all()}

    unresolved = [s for s in scales if not by_id.get(s.restaurante_id)]
    restaurants = []
    if unresolved:
        restaurants = Restaurante.query.filter(Restaurante.ativo.is_(True)).order_by(Restaurante.nome.asc()).all()
    normalized = [(backend._norm(r.nome), r) for r in restaurants if backend._norm(r.nome)]

    for scale in scales:
        rest = by_id.get(scale.restaurante_id)
        if not rest:
            contract = backend._norm(scale.contrato)
            if contract:
                rest = next(
                    (r for name, r in normalized if name == contract or name in contract or contract in name),
                    None,
                )
        result[scale.id] = rest
    return result


def _pending_approvals_fast(rest):
    items = (
        ProducaoCooperado.query.options(
            joinedload(ProducaoCooperado.cooperado),
            joinedload(ProducaoCooperado.escala),
        )
        .filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.status == "pendente",
            ProducaoCooperado.valor_total > 0,
        )
        .order_by(ProducaoCooperado.criado_em.desc(), ProducaoCooperado.id.desc())
        .limit(40)
        .all()
    )
    result = []
    for item in items:
        scale = item.escala or SimpleNamespace(cooperado_nome="", horario="")
        result.append(SimpleNamespace(
            producao=item,
            cooperado=item.cooperado,
            escala=scale,
            data=item.data,
        ))
    return result


def _matches_day(scale, day: date) -> bool:
    exact = backend.upgrade._parse_date(scale.data)
    if exact:
        return exact == day
    weekday = flow._weekday_for_scale(scale)
    return weekday is not None and int(weekday) == day.weekday()


def _timeline_fast(coop, start: date, end: date):
    if end < start:
        start, end = end, start
    if (end - start).days > 31:
        end = start + timedelta(days=31)

    now = datetime.now(TZ)
    all_scales = _coop_scales_fast(coop)
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    scales = [s for s in all_scales if any(_matches_day(s, d) for d in days)]
    restaurants = _restaurant_map_fast(scales)

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

    prod_scale_day = {}
    prod_slot = {}
    for item in productions:
        if item.escala_id:
            prod_scale_day[(item.escala_id, item.data)] = item
        prod_slot[(
            item.restaurante_id,
            item.data,
            backend.upgrade._norm_time(item.hora_inicio),
            backend.upgrade._norm_time(item.hora_fim),
        )] = item

    launches_day = {}
    for launch in launches:
        launches_day.setdefault((launch.restaurante_id, launch.data), []).append(launch)

    evaluation_map = {}
    Evaluation = getattr(backend.legacy, "AvaliacaoRestaurante", None)
    launch_ids = [l.id for l in launches if l.id]
    if Evaluation is not None and launch_ids:
        rows = (
            db.session.query(Evaluation.lancamento_id, Evaluation.estrelas_geral)
            .filter(
                Evaluation.lancamento_id.in_(launch_ids),
                Evaluation.cooperado_id == coop.id,
            )
            .all()
        )
        evaluation_map = {launch_id: note for launch_id, note in rows}

    result = []
    for day in days:
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
                color, label, can_submit = "green", "Produção aprovada", False
            elif production and production.status == "pendente" and total > 0:
                color, label, can_submit = "yellow", "Enviada · aguardando aprovação", False
            elif production and production.status == "recusada":
                color, label, can_submit = "red", "Recusada · bloqueada", False
            elif not finished:
                color, label, can_submit = "muted", "Libera após o fim do horário", False
            else:
                color, label, can_submit = "red", "Pendente de lançamento", bool(rest)

            note = evaluation_map.get(launch.id) if launch else None
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
                avaliada=note is not None,
                avaliacao_nota=note,
            ))

    result.sort(key=lambda x: (x.data or date.max, x.inicio or "", x.escala.id))
    return result


# Substitui os coletores pesados usados pelas rotas complementares.
ui._coop_scales = _coop_scales_fast
ui._rest_scales = _rest_scales_fast
ui._timeline = _timeline_fast
backend._coop_scales = _coop_scales_fast
backend._rest_scales = _rest_scales_fast
backend._restaurant_map = _restaurant_map_fast


# Remove o processador anterior, que montava escalas e aprovações em todas as abas.
processors = app.template_context_processors.get(None, [])
app.template_context_processors[None] = [
    fn for fn in processors if getattr(fn, "__name__", "") != "_coopex_ui_context"
]


@app.context_processor
def _coopex_fast_context():
    context = {
        "coopex_rest_display_name": "ESTABELECIMENTO",
        "coopex_rest_pending_rows": [],
        "coopex_coop_timeline": [],
        "coopex_filter_start": None,
        "coopex_filter_end": None,
    }
    role = (session.get("user_tipo") or "").strip().lower()
    try:
        if role == "restaurante" and request.endpoint == "portal_restaurante":
            rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first()
            if rest:
                context["coopex_rest_display_name"] = ui._norm_name(rest.nome)
                view = (request.args.get("view") or "lancar").strip().lower()
                if view == "lancar":
                    context["coopex_rest_pending_rows"] = _pending_approvals_fast(rest)

        elif role == "cooperado" and request.endpoint == "portal_cooperado":
            coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first()
            if coop:
                today = datetime.now(TZ).date()
                start = backend.upgrade._parse_date(request.args.get("data_inicio")) or today
                end = backend.upgrade._parse_date(request.args.get("data_fim")) or start
                if end < start:
                    start, end = end, start
                context.update(
                    coopex_coop_timeline=_timeline_fast(coop, start, end),
                    coopex_filter_start=start,
                    coopex_filter_end=end,
                )
    except Exception:
        app.logger.exception("Falha ao carregar contexto otimizado dos painéis")
    return context


def _install_template_repairs():
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_performance_repairs", False):
        return
    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "restaurante_dashboard.html":
            old_photo = "{{ coop.foto_url or url_for('static', filename='img/default.png') }}"
            new_photo = "{{ url_for('media_coop', coop_id=coop.id) }}"
            source = source.replace(old_photo, new_photo)
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_performance_repairs = True
    app.jinja_env.cache.clear()


_install_template_repairs()
