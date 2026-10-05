from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from functools import wraps
from types import SimpleNamespace

from flask import redirect, request, session, url_for
from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload

import production_scale_backend as backend

app = backend.app
db = backend.db
flow = backend.flow
legacy = flow.legacy
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


# ============================================================
# Performance consolidada — antigas camadas performance_ui/queries
# ============================================================

# Compatibilidade interna consolidada: antes vivia em performance hotfix.
if not hasattr(backend, "legacy"):
    backend.legacy = flow.legacy

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
_coop_scales = _coop_scales_fast
_rest_scales = _rest_scales_fast
_timeline = _timeline_fast
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
            rest = legacy.request_restaurante()
            if rest:
                context["coopex_rest_display_name"] = _norm_name(rest.nome)
                view = (request.args.get("view") or "lancar").strip().lower()
                if view == "lancar":
                    context["coopex_rest_pending_rows"] = _pending_approvals_fast(rest)

        elif role == "cooperado" and request.endpoint == "portal_cooperado":
            coop = legacy.request_cooperado()
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


def _dedupe(rows):
    seen = set()
    result = []
    for row in sorted(rows, key=lambda item: item.id):
        if row.id not in seen:
            seen.add(row.id)
            result.append(row)
    return result


def _coop_scales_indexed(coop):
    rows = (
        Escala.query.filter(Escala.cooperado_id == coop.id)
        .order_by(Escala.id.desc())
        .limit(260)
        .all()
    )

    # Só procura registros legados por nome quando os vínculos por ID não
    # formam uma semana completa. A consulta principal continua indexada.
    if len(rows) < 7:
        variants = _raw_variants(coop.nome)
        if variants:
            rows.extend(
                Escala.query.filter(
                    Escala.cooperado_id.is_(None),
                    func.lower(func.trim(Escala.cooperado_nome)).in_(variants),
                )
                .order_by(Escala.id.desc())
                .limit(80)
                .all()
            )

        # Último fallback para acentos e grafias históricas. É limitado e só
        # executa quando ainda faltam registros.
        if len(rows) < 7:
            target = backend._norm(coop.nome)
            candidates = (
                Escala.query.filter(
                    Escala.cooperado_id.is_(None),
                    Escala.cooperado_nome.isnot(None),
                )
                .order_by(Escala.id.desc())
                .limit(160)
                .all()
            )
            rows.extend(
                scale for scale in candidates
                if backend._norm(scale.cooperado_nome) == target
            )

    return _dedupe(rows)


def _rest_scales_indexed(rest):
    rows = (
        Escala.query.filter(Escala.restaurante_id == rest.id)
        .order_by(Escala.id.desc())
        .limit(320)
        .all()
    )

    if len(rows) < 7:
        variants = _raw_variants(rest.nome)
        if variants:
            rows.extend(
                Escala.query.filter(
                    Escala.restaurante_id.is_(None),
                    func.lower(func.trim(Escala.contrato)).in_(variants),
                )
                .order_by(Escala.id.desc())
                .limit(80)
                .all()
            )

        if len(rows) < 7:
            target = backend._norm(rest.nome)
            candidates = (
                Escala.query.filter(
                    Escala.restaurante_id.is_(None),
                    Escala.contrato.isnot(None),
                )
                .order_by(Escala.id.desc())
                .limit(180)
                .all()
            )
            rows.extend(
                scale for scale in candidates
                if backend._norm(scale.contrato)
                and (
                    backend._norm(scale.contrato) == target
                    or target in backend._norm(scale.contrato)
                    or backend._norm(scale.contrato) in target
                )
            )

    return _dedupe(rows)


# As funções de timeline resolvem esses nomes no módulo em tempo de execução.
_coop_scales_fast = _coop_scales_indexed
_rest_scales_fast = _rest_scales_indexed
_coop_scales = _coop_scales_indexed
_rest_scales = _rest_scales_indexed
backend._coop_scales = _coop_scales_indexed
backend._rest_scales = _rest_scales_indexed

