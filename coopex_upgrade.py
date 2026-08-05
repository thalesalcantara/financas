from __future__ import annotations

import gzip
import hashlib
import re
from datetime import date, datetime, timedelta
from functools import wraps
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import app as legacy
from flask import abort, flash, jsonify, make_response, redirect, render_template, request, session, url_for
from sqlalchemy import func, or_, text as sa_text
from sqlalchemy.exc import IntegrityError

app = legacy.app
db = legacy.db
Usuario = legacy.Usuario
Cooperado = legacy.Cooperado
Restaurante = legacy.Restaurante
Lancamento = legacy.Lancamento
Escala = legacy.Escala
TrocaSolicitacao = legacy.TrocaSolicitacao

TZ = ZoneInfo("America/Fortaleza")


class ProducaoCooperado(db.Model):
    __tablename__ = "producoes_cooperado"

    id = db.Column(db.Integer, primary_key=True)
    cooperado_id = db.Column(
        db.Integer,
        db.ForeignKey("cooperados.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    restaurante_id = db.Column(
        db.Integer,
        db.ForeignKey("restaurantes.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    escala_id = db.Column(
        db.Integer,
        db.ForeignKey("escalas.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    lancamento_id = db.Column(
        db.Integer,
        db.ForeignKey("lancamentos.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    data = db.Column(db.Date, nullable=False, index=True)
    hora_inicio = db.Column(db.String(10), nullable=False, default="")
    hora_fim = db.Column(db.String(10), nullable=False, default="")
    qtd_entregas = db.Column(db.Integer, nullable=False, default=0)
    valor_unitario = db.Column(db.Float, nullable=False, default=0.0)
    valor_total = db.Column(db.Float, nullable=False, default=0.0)
    descricao = db.Column(db.String(255))

    status = db.Column(db.String(20), nullable=False, default="pendente", index=True)
    motivo_recusa = db.Column(db.Text)
    revisao = db.Column(db.Integer, nullable=False, default=1)

    criado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    atualizado_em = db.Column(
        db.DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )
    decidido_em = db.Column(db.DateTime)

    cooperado = db.relationship("Cooperado")
    restaurante = db.relationship("Restaurante")
    escala = db.relationship("Escala")
    lancamento = db.relationship("Lancamento")

    __table_args__ = (
        db.UniqueConstraint(
            "cooperado_id",
            "restaurante_id",
            "data",
            "hora_inicio",
            "hora_fim",
            name="uq_producao_coop_slot",
        ),
    )


class ProducaoCooperadoHistorico(db.Model):
    __tablename__ = "producoes_cooperado_historico"

    id = db.Column(db.Integer, primary_key=True)
    producao_id = db.Column(
        db.Integer,
        db.ForeignKey("producoes_cooperado.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status_anterior = db.Column(db.String(20))
    status_novo = db.Column(db.String(20))
    qtd_entregas = db.Column(db.Integer)
    valor_unitario = db.Column(db.Float)
    valor_total = db.Column(db.Float)
    descricao = db.Column(db.String(255))
    motivo = db.Column(db.Text)
    alterado_por_tipo = db.Column(db.String(20))
    alterado_por_id = db.Column(db.Integer)
    criado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)

    producao = db.relationship(
        "ProducaoCooperado",
        backref=db.backref("historico", cascade="all, delete-orphan", lazy=True),
    )


def _install_schema() -> None:
    with app.app_context():
        db.create_all()
        schema = "" if legacy._is_sqlite() else "public."
        statements = (
            f"CREATE INDEX IF NOT EXISTS ix_lanc_rest_data_fast ON {schema}lancamentos (restaurante_id, data)",
            f"CREATE INDEX IF NOT EXISTS ix_lanc_coop_data_fast ON {schema}lancamentos (cooperado_id, data)",
            f"CREATE INDEX IF NOT EXISTS ix_lanc_slot_fast ON {schema}lancamentos (restaurante_id, cooperado_id, data, hora_inicio, hora_fim)",
            f"CREATE INDEX IF NOT EXISTS ix_escala_coop_fast ON {schema}escalas (cooperado_id)",
            f"CREATE INDEX IF NOT EXISTS ix_escala_rest_fast ON {schema}escalas (restaurante_id)",
            f"CREATE INDEX IF NOT EXISTS ix_troca_dest_status_fast ON {schema}trocas (destino_id, status)",
            f"CREATE INDEX IF NOT EXISTS ix_troca_solic_status_fast ON {schema}trocas (solicitante_id, status)",
            f"CREATE INDEX IF NOT EXISTS ix_prod_rest_status_fast ON {schema}producoes_cooperado (restaurante_id, status, data)",
            f"CREATE INDEX IF NOT EXISTS ix_prod_coop_status_fast ON {schema}producoes_cooperado (cooperado_id, status, data)",
        )
        try:
            for statement in statements:
                db.session.execute(sa_text(statement))
            db.session.commit()
        except Exception:
            db.session.rollback()
            app.logger.exception("Falha ao instalar índices da produção do cooperado")


_install_schema()


def _require_role(role: str):
    if (session.get("user_tipo") or "").strip().lower() != role:
        return redirect(url_for("login"))
    return None


def _money(raw, default: float = 0.0) -> float:
    parser = getattr(legacy, "parse_valor_monetario", None)
    if parser:
        try:
            return round(float(parser(raw, default)), 2)
        except Exception:
            pass
    try:
        text = str(raw if raw is not None else default).strip().replace("R$", "").replace(" ", "")
        if "," in text and "." in text:
            text = text.replace(".", "").replace(",", ".")
        elif "," in text:
            text = text.replace(",", ".")
        return round(float(text or default), 2)
    except Exception:
        return round(float(default or 0.0), 2)


def _parse_date(raw) -> date | None:
    if isinstance(raw, date):
        return raw
    value = str(raw or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _norm_time(raw) -> str:
    value = str(raw or "").strip().lower().replace("h", ":")
    match = re.search(r"(?<!\d)([01]?\d|2[0-3])(?::([0-5]\d))?", value)
    if not match:
        return ""
    return f"{int(match.group(1)):02d}:{int(match.group(2) or 0):02d}"


def _times_from_text(raw) -> tuple[str, str]:
    matches = re.findall(r"(?<!\d)([01]?\d|2[0-3])(?::([0-5]\d))?", str(raw or ""))
    if not matches:
        return "", ""
    start = f"{int(matches[0][0]):02d}:{int(matches[0][1] or 0):02d}"
    if len(matches) == 1:
        return start, ""
    end = f"{int(matches[-1][0]):02d}:{int(matches[-1][1] or 0):02d}"
    return start, end


def _minutes(raw) -> int | None:
    value = _norm_time(raw)
    if not value:
        return None
    hour, minute = map(int, value.split(":"))
    return hour * 60 + minute


def _overlap(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    if not any((a_start, a_end, b_start, b_end)):
        return True
    a0, a1 = _minutes(a_start), _minutes(a_end)
    b0, b1 = _minutes(b_start), _minutes(b_end)
    if a0 is None or b0 is None:
        return _norm_time(a_start) == _norm_time(b_start)
    if a1 is None:
        a1 = a0 + 1
    if b1 is None:
        b1 = b0 + 1
    if a1 <= a0:
        a1 += 1440
    if b1 <= b0:
        b1 += 1440
    return max(a0, b0) < min(a1, b1)


def _find_existing_launch(
    restaurante_id: int,
    cooperado_id: int,
    data_ref: date,
    hora_inicio: str,
    hora_fim: str,
    *,
    exclude_id: int | None = None,
) -> Lancamento | None:
    query = Lancamento.query.filter(
        Lancamento.restaurante_id == restaurante_id,
        Lancamento.cooperado_id == cooperado_id,
        Lancamento.data == data_ref,
    )
    if exclude_id:
        query = query.filter(Lancamento.id != exclude_id)
    for launch in query.order_by(Lancamento.id.asc()).all():
        if _overlap(
            _norm_time(launch.hora_inicio),
            _norm_time(launch.hora_fim),
            hora_inicio,
            hora_fim,
        ):
            return launch
    return None


def _history(
    item: ProducaoCooperado,
    *,
    old_status: str | None,
    new_status: str | None,
    reason: str | None = None,
) -> None:
    db.session.add(
        ProducaoCooperadoHistorico(
            producao_id=item.id,
            status_anterior=old_status,
            status_novo=new_status,
            qtd_entregas=item.qtd_entregas,
            valor_unitario=item.valor_unitario,
            valor_total=item.valor_total,
            descricao=item.descricao,
            motivo=reason,
            alterado_por_tipo=(session.get("user_tipo") or "")[:20],
            alterado_por_id=session.get("user_id"),
        )
    )


def _rest_for_scale(scale: Escala | None) -> Restaurante | None:
    if not scale:
        return None
    if scale.restaurante_id:
        return db.session.get(Restaurante, scale.restaurante_id)
    contract = re.sub(r"\s+", " ", str(scale.contrato or "")).strip().lower()
    if not contract:
        return None
    restaurants = Restaurante.query.filter(Restaurante.ativo.is_(True)).all()
    exact = next((r for r in restaurants if (r.nome or "").strip().lower() == contract), None)
    if exact:
        return exact
    return next(
        (
            r
            for r in restaurants
            if contract in (r.nome or "").strip().lower()
            or (r.nome or "").strip().lower() in contract
        ),
        None,
    )


def _next_date_for_scale(scale: Escala, today: date | None = None) -> date | None:
    today = today or datetime.now(TZ).date()
    parsed = _parse_date(scale.data)
    if parsed:
        return parsed
    helper = getattr(legacy, "_weekday_from_data_str", None)
    weekday = None
    if helper:
        try:
            weekday = helper(scale.data)
        except Exception:
            weekday = None
    if weekday is None:
        names = {
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
        text = str(scale.data or "").strip().lower()
        weekday = next((value for key, value in names.items() if key in text), None)
    if weekday is None:
        return None
    return today + timedelta(days=(int(weekday) - today.weekday()) % 7)


def _cooperado_current() -> Cooperado:
    return Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()


def _restaurante_current() -> Restaurante:
    return Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()


def _lock_production(item_id: int) -> ProducaoCooperado | None:
    query = ProducaoCooperado.query.filter_by(id=item_id)
    try:
        if not legacy._is_sqlite():
            query = query.with_for_update()
    except Exception:
        pass
    return query.first()


def _approve_item(
    item: ProducaoCooperado,
    *,
    quantity: int | None = None,
    unit_value: float | None = None,
) -> tuple[bool, str]:
    if item.status != "pendente":
        return False, "Esta produção já foi tratada."

    quantity = int(quantity if quantity is not None else item.qtd_entregas or 0)
    unit_value = round(
        float(unit_value if unit_value is not None else item.valor_unitario or 0.0),
        2,
    )
    if quantity <= 0 or unit_value < 0:
        return False, "Quantidade ou valor unitário inválido."

    existing = _find_existing_launch(
        item.restaurante_id,
        item.cooperado_id,
        item.data,
        item.hora_inicio,
        item.hora_fim,
    )
    old_status = item.status
    total = round(quantity * unit_value, 2)

    if existing:
        item.lancamento_id = existing.id
    else:
        launch = Lancamento(
            restaurante_id=item.restaurante_id,
            cooperado_id=item.cooperado_id,
            descricao=item.descricao or "Produção confirmada pelo estabelecimento",
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
    item.valor_unitario = unit_value
    item.valor_total = total
    item.status = "aprovada"
    item.motivo_recusa = None
    item.decidido_em = datetime.utcnow()
    _history(item, old_status=old_status, new_status="aprovada", reason="Confirmação do estabelecimento")
    return True, "Produção confirmada e lançada no financeiro."


@app.get("/healthz/upgrade")
def coopex_upgrade_health():
    return jsonify({"ok": True, "upgrade": "producao-cooperado-admin-rapido"})


@app.route("/coop/producao", methods=["GET", "POST"], endpoint="coop_producao")
@app.route("/coop/producao/nova", methods=["GET", "POST"], endpoint="coop_producao_nova")
def coop_producao_form():
    denied = _require_role("cooperado")
    if denied:
        return denied

    coop = _cooperado_current()
    today = datetime.now(TZ).date()
    restaurants = (
        Restaurante.query.filter(Restaurante.ativo.is_(True))
        .order_by(Restaurante.nome.asc())
        .all()
    )
    scales = (
        Escala.query.filter_by(cooperado_id=coop.id)
        .order_by(Escala.id.desc())
        .all()
    )

    if request.method == "POST":
        scale_id = request.form.get("escala_id", type=int)
        scale = None
        if scale_id:
            scale = Escala.query.filter_by(id=scale_id, cooperado_id=coop.id).first()
            if not scale:
                flash("A escala escolhida não pertence ao seu cadastro.", "danger")
                return redirect(url_for("coop_producao"))

        restaurante_id = request.form.get("restaurante_id", type=int)
        scale_rest = _rest_for_scale(scale)
        if scale_rest:
            restaurante_id = scale_rest.id
        restaurant = Restaurante.query.filter_by(id=restaurante_id, ativo=True).first()

        data_ref = _parse_date(request.form.get("data"))
        quantity = request.form.get("qtd_entregas", type=int)
        unit_value = _money(request.form.get("valor_unitario"))
        description = (
            request.form.get("descricao") or ""
        ).strip() or "Produção informada pelo cooperado"

        posted_start = _norm_time(request.form.get("hora_inicio"))
        posted_end = _norm_time(request.form.get("hora_fim"))
        scale_start, scale_end = _times_from_text(scale.horario if scale else "")
        start = posted_start or scale_start
        end = posted_end or scale_end

        if not restaurant:
            flash("Selecione um estabelecimento válido.", "warning")
            return redirect(url_for("coop_producao"))
        if not data_ref:
            flash("Informe uma data válida.", "warning")
            return redirect(url_for("coop_producao"))
        if data_ref > today:
            flash("A produção só pode ser enviada depois da realização do serviço.", "warning")
            return redirect(url_for("coop_producao"))
        if data_ref < today - timedelta(days=14):
            flash("Somente produções dos últimos 14 dias podem ser enviadas.", "warning")
            return redirect(url_for("coop_producao"))
        if not quantity or quantity <= 0:
            flash("Informe a quantidade realizada.", "warning")
            return redirect(url_for("coop_producao"))
        if unit_value < 0:
            flash("Informe um valor unitário válido.", "warning")
            return redirect(url_for("coop_producao"))

        if data_ref == today and end:
            end_minutes = _minutes(end)
            now = datetime.now(TZ)
            if end_minutes is not None and end_minutes > now.hour * 60 + now.minute:
                flash(
                    "Este horário ainda não terminou. O lançamento será liberado após o fim da escala.",
                    "warning",
                )
                return redirect(url_for("coop_producao"))

        existing_launch = _find_existing_launch(
            restaurant.id,
            coop.id,
            data_ref,
            start,
            end,
        )
        if existing_launch:
            flash(
                "Produção já lançada para este estabelecimento, data e horário.",
                "warning",
            )
            return redirect(url_for("coop_producao"))

        existing = ProducaoCooperado.query.filter_by(
            cooperado_id=coop.id,
            restaurante_id=restaurant.id,
            data=data_ref,
            hora_inicio=start,
            hora_fim=end,
        ).first()
        if existing:
            if existing.status == "recusada":
                flash(
                    "Essa produção foi recusada. Corrija o mesmo registro e reenvie, sem criar outro.",
                    "info",
                )
                return redirect(
                    url_for("coop_producao_editar", item_id=existing.id)
                )
            flash("Produção já enviada. Acompanhe o status no histórico.", "warning")
            return redirect(url_for("coop_producao"))

        total = round(quantity * unit_value, 2)
        item = ProducaoCooperado(
            cooperado_id=coop.id,
            restaurante_id=restaurant.id,
            escala_id=scale.id if scale else None,
            data=data_ref,
            hora_inicio=start,
            hora_fim=end,
            qtd_entregas=quantity,
            valor_unitario=unit_value,
            valor_total=total,
            descricao=description,
            status="pendente",
        )
        db.session.add(item)
        try:
            db.session.flush()
            _history(
                item,
                old_status=None,
                new_status="pendente",
                reason="Envio inicial",
            )
            db.session.commit()
            flash("Produção enviada ao estabelecimento para confirmação.", "success")
        except IntegrityError:
            db.session.rollback()
            flash("Produção já lançada. Nenhum registro duplicado foi criado.", "warning")
        return redirect(url_for("coop_producao"))

    status_filter = (request.args.get("status") or "").strip().lower()
    query = ProducaoCooperado.query.filter_by(cooperado_id=coop.id)
    if status_filter in {"pendente", "aprovada", "recusada"}:
        query = query.filter(ProducaoCooperado.status == status_filter)
    history = (
        query.order_by(
            ProducaoCooperado.data.desc(),
            ProducaoCooperado.id.desc(),
        )
        .limit(100)
        .all()
    )
    return render_template(
        "coop_producao.html",
        cooperado=coop,
        restaurantes=restaurants,
        escalas=scales,
        historico=history,
        status_filter=status_filter,
        today=today,
        min_date=today - timedelta(days=14),
    )


@app.route(
    "/coop/producao/<int:item_id>/editar",
    methods=["GET", "POST"],
    endpoint="coop_producao_editar",
)
def coop_producao_editar(item_id: int):
    denied = _require_role("cooperado")
    if denied:
        return denied
    coop = _cooperado_current()
    item = ProducaoCooperado.query.filter_by(
        id=item_id,
        cooperado_id=coop.id,
    ).first_or_404()

    if item.status == "aprovada":
        flash("Uma produção aprovada não pode ser alterada.", "warning")
        return redirect(url_for("coop_producao"))

    if request.method == "POST":
        old_status = item.status
        quantity = request.form.get("qtd_entregas", type=int)
        unit_value = _money(request.form.get("valor_unitario"), item.valor_unitario)
        description = (
            request.form.get("descricao") or ""
        ).strip() or item.descricao
        if not quantity or quantity <= 0 or unit_value < 0:
            flash("Informe quantidade e valor unitário válidos.", "warning")
            return redirect(url_for("coop_producao_editar", item_id=item.id))

        item.qtd_entregas = quantity
        item.valor_unitario = unit_value
        item.valor_total = round(quantity * unit_value, 2)
        item.descricao = description
        item.status = "pendente"
        item.motivo_recusa = None
        item.decidido_em = None
        item.revisao = int(item.revisao or 1) + 1
        _history(
            item,
            old_status=old_status,
            new_status="pendente",
            reason="Correção e reenvio pelo cooperado",
        )
        db.session.commit()
        flash("Produção corrigida e reenviada ao estabelecimento.", "success")
        return redirect(url_for("coop_producao"))

    return render_template(
        "coop_producao_editar.html",
        item=item,
        cooperado=coop,
    )


@app.get("/coop/agenda", endpoint="coop_agenda")
def coop_agenda():
    denied = _require_role("cooperado")
    if denied:
        return denied
    coop = _cooperado_current()
    selected_day = (request.args.get("dia") or "").strip().lower()
    scales = (
        Escala.query.filter_by(cooperado_id=coop.id)
        .order_by(Escala.id.asc())
        .all()
    )
    today = datetime.now(TZ).date()

    items = []
    days = []
    seen_days = set()
    for scale in scales:
        label = str(scale.data or "Sem dia").strip() or "Sem dia"
        key = label.lower()
        if key not in seen_days:
            seen_days.add(key)
            days.append(label)
        if selected_day and selected_day != key:
            continue
        restaurant = _rest_for_scale(scale)
        start, end = _times_from_text(scale.horario)
        items.append(
            SimpleNamespace(
                escala=scale,
                restaurante=restaurant,
                proxima_data=_next_date_for_scale(scale, today),
                inicio=start,
                fim=end,
            )
        )

    other_coops = (
        Cooperado.query.join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Usuario.ativo.is_(True), Cooperado.id != coop.id)
        .order_by(Cooperado.nome.asc())
        .all()
    )
    sent = (
        TrocaSolicitacao.query.filter_by(solicitante_id=coop.id)
        .order_by(TrocaSolicitacao.id.desc())
        .limit(30)
        .all()
    )
    received_count = TrocaSolicitacao.query.filter_by(
        destino_id=coop.id,
        status="pendente",
    ).count()

    return render_template(
        "coop_agenda.html",
        cooperado=coop,
        itens=items,
        dias=days,
        selected_day=selected_day,
        outros_cooperados=other_coops,
        trocas_enviadas=sent,
        trocas_recebidas_count=received_count,
        today=today,
    )


@app.get("/rest/producoes/pendentes", endpoint="rest_producoes_pendentes")
def rest_producoes_pendentes():
    denied = _require_role("restaurante")
    if denied:
        return denied
    rest = _restaurante_current()
    pending = (
        ProducaoCooperado.query.filter_by(
            restaurante_id=rest.id,
            status="pendente",
        )
        .order_by(ProducaoCooperado.criado_em.asc())
        .all()
    )
    recent = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.status.in_(["aprovada", "recusada"]),
        )
        .order_by(ProducaoCooperado.decidido_em.desc(), ProducaoCooperado.id.desc())
        .limit(80)
        .all()
    )
    return render_template(
        "rest_producoes_pendentes.html",
        restaurante=rest,
        pendentes=pending,
        recentes=recent,
    )


@app.get(
    "/api/rest/producoes/pendentes/status",
    endpoint="rest_producoes_status",
)
def rest_producoes_status():
    denied = _require_role("restaurante")
    if denied:
        return jsonify({"ok": False, "count": 0}), 403
    rest = _restaurante_current()
    count = ProducaoCooperado.query.filter_by(
        restaurante_id=rest.id,
        status="pendente",
    ).count()
    latest = (
        db.session.query(func.max(ProducaoCooperado.id))
        .filter_by(restaurante_id=rest.id, status="pendente")
        .scalar()
        or 0
    )
    return jsonify({"ok": True, "count": int(count), "latest": int(latest)})


@app.post(
    "/rest/producoes/<int:item_id>/aprovar",
    endpoint="rest_producao_aprovar",
)
def rest_producao_aprovar(item_id: int):
    denied = _require_role("restaurante")
    if denied:
        return denied
    rest = _restaurante_current()
    try:
        item = _lock_production(item_id)
        if not item or item.restaurante_id != rest.id:
            abort(404)
        quantity = request.form.get("qtd_entregas", type=int)
        unit_value = _money(
            request.form.get("valor_unitario"),
            item.valor_unitario,
        )
        ok, message = _approve_item(
            item,
            quantity=quantity,
            unit_value=unit_value,
        )
        if ok:
            db.session.commit()
            flash(message, "success")
        else:
            db.session.rollback()
            flash(message, "warning")
    except IntegrityError:
        db.session.rollback()
        flash("A produção já foi confirmada. Nenhum lançamento duplicado foi criado.", "warning")
    return redirect(url_for("rest_producoes_pendentes"))


@app.post(
    "/rest/producoes/aprovar-selecionadas",
    endpoint="rest_producoes_aprovar_selecionadas",
)
def rest_producoes_aprovar_selecionadas():
    denied = _require_role("restaurante")
    if denied:
        return denied
    rest = _restaurante_current()
    ids = request.form.getlist("ids")
    if not ids and request.form.get("aprovar_todas") == "1":
        ids = [
            str(item_id)
            for (item_id,) in db.session.query(ProducaoCooperado.id)
            .filter_by(restaurante_id=rest.id, status="pendente")
            .all()
        ]

    approved = 0
    try:
        for raw_id in ids:
            try:
                item_id = int(raw_id)
            except Exception:
                continue
            item = _lock_production(item_id)
            if not item or item.restaurante_id != rest.id:
                continue
            ok, _ = _approve_item(item)
            approved += int(ok)
        db.session.commit()
        flash(f"{approved} produção(ões) confirmada(s).", "success")
    except IntegrityError:
        db.session.rollback()
        flash("Uma produção já havia sido confirmada. A operação foi interrompida sem duplicar.", "warning")
    return redirect(url_for("rest_producoes_pendentes"))


@app.post(
    "/rest/producoes/<int:item_id>/recusar",
    endpoint="rest_producao_recusar",
)
def rest_producao_recusar(item_id: int):
    denied = _require_role("restaurante")
    if denied:
        return denied
    rest = _restaurante_current()
    item = _lock_production(item_id)
    if not item or item.restaurante_id != rest.id:
        abort(404)
    if item.status != "pendente":
        flash("Esta produção já foi tratada.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))
    reason = (request.form.get("motivo") or "").strip()
    if not reason:
        flash("Informe o motivo da recusa.", "warning")
        return redirect(url_for("rest_producoes_pendentes"))
    old_status = item.status
    item.status = "recusada"
    item.motivo_recusa = reason
    item.decidido_em = datetime.utcnow()
    _history(item, old_status=old_status, new_status="recusada", reason=reason)
    db.session.commit()
    flash("Produção devolvida ao cooperado para correção.", "info")
    return redirect(url_for("rest_producoes_pendentes"))


@app.get("/admin/producoes/pendentes", endpoint="admin_producoes_pendentes")
def admin_producoes_pendentes():
    denied = _require_role("admin")
    if denied:
        return denied
    status_filter = (request.args.get("status") or "pendente").strip().lower()
    query = ProducaoCooperado.query
    if status_filter in {"pendente", "aprovada", "recusada"}:
        query = query.filter(ProducaoCooperado.status == status_filter)
    items = (
        query.order_by(
            ProducaoCooperado.criado_em.desc(),
            ProducaoCooperado.id.desc(),
        )
        .limit(300)
        .all()
    )
    counts = dict(
        db.session.query(
            ProducaoCooperado.status,
            func.count(ProducaoCooperado.id),
        )
        .group_by(ProducaoCooperado.status)
        .all()
    )
    return render_template(
        "admin_producoes_pendentes.html",
        itens=items,
        counts=counts,
        status_filter=status_filter,
    )


@app.get("/admin/rapido", endpoint="admin_rapido")
def admin_rapido():
    denied = _require_role("admin")
    if denied:
        return denied
    view = (request.args.get("view") or "lancamentos").strip().lower()
    page = max(1, request.args.get("page", type=int) or 1)
    per_page = 60 if view == "lancamentos" else 100
    search = (request.args.get("q") or "").strip()
    data_start = _parse_date(request.args.get("data_inicio"))
    data_end = _parse_date(request.args.get("data_fim"))

    if view == "escalas":
        query = Escala.query
        if search:
            pattern = f"%{search}%"
            query = query.filter(
                or_(
                    Escala.cooperado_nome.ilike(pattern),
                    Escala.contrato.ilike(pattern),
                    Escala.data.ilike(pattern),
                    Escala.turno.ilike(pattern),
                    Escala.horario.ilike(pattern),
                )
            )
        pagination = query.order_by(Escala.id.desc()).paginate(
            page=page,
            per_page=per_page,
            error_out=False,
        )
        items = pagination.items
    else:
        view = "lancamentos"
        query = Lancamento.query
        if data_start:
            query = query.filter(Lancamento.data >= data_start)
        if data_end:
            query = query.filter(Lancamento.data <= data_end)
        if search:
            pattern = f"%{search}%"
            query = (
                query.outerjoin(Cooperado, Lancamento.cooperado_id == Cooperado.id)
                .outerjoin(Restaurante, Lancamento.restaurante_id == Restaurante.id)
                .filter(
                    or_(
                        Cooperado.nome.ilike(pattern),
                        Restaurante.nome.ilike(pattern),
                        Lancamento.descricao.ilike(pattern),
                    )
                )
            )
        pagination = query.order_by(
            Lancamento.data.desc(),
            Lancamento.id.desc(),
        ).paginate(
            page=page,
            per_page=per_page,
            error_out=False,
        )
        items = pagination.items

    cooperados = Cooperado.query.order_by(Cooperado.nome.asc()).all()
    restaurantes = Restaurante.query.order_by(Restaurante.nome.asc()).all()
    return render_template(
        "admin_rapido.html",
        view=view,
        itens=items,
        pagination=pagination,
        cooperados=cooperados,
        restaurantes=restaurantes,
        q=search,
        data_inicio=data_start,
        data_fim=data_end,
    )


@app.patch("/api/admin/lancamentos/<int:item_id>")
@app.delete("/api/admin/lancamentos/<int:item_id>")
def api_admin_lancamento(item_id: int):
    if _require_role("admin"):
        return jsonify({"ok": False, "message": "Não autorizado."}), 403
    item = Lancamento.query.get_or_404(item_id)
    if request.method == "DELETE":
        try:
            legacy.AvaliacaoCooperado.query.filter_by(lancamento_id=item.id).delete(
                synchronize_session=False
            )
        except Exception:
            pass
        try:
            legacy.AvaliacaoRestaurante.query.filter_by(lancamento_id=item.id).delete(
                synchronize_session=False
            )
        except Exception:
            pass
        ProducaoCooperado.query.filter_by(lancamento_id=item.id).update(
            {ProducaoCooperado.lancamento_id: None},
            synchronize_session=False,
        )
        db.session.delete(item)
        db.session.commit()
        return jsonify({"ok": True, "message": "Lançamento excluído."})

    payload = request.get_json(silent=True) or {}
    data_ref = _parse_date(payload.get("data"))
    restaurante_id = payload.get("restaurante_id")
    cooperado_id = payload.get("cooperado_id")
    if data_ref:
        item.data = data_ref
    if restaurante_id is not None:
        item.restaurante_id = int(restaurante_id)
    if cooperado_id is not None:
        item.cooperado_id = int(cooperado_id)
    item.descricao = str(payload.get("descricao", item.descricao or "")).strip()
    item.valor = _money(payload.get("valor"), item.valor or 0.0)
    item.qtd_entregas = int(payload.get("qtd_entregas", item.qtd_entregas or 0) or 0)
    item.hora_inicio = _norm_time(payload.get("hora_inicio")) or item.hora_inicio
    item.hora_fim = _norm_time(payload.get("hora_fim")) or item.hora_fim
    db.session.commit()
    return jsonify({"ok": True, "message": "Lançamento atualizado."})


@app.patch("/api/admin/escalas/<int:item_id>")
@app.delete("/api/admin/escalas/<int:item_id>")
def api_admin_escala(item_id: int):
    if _require_role("admin"):
        return jsonify({"ok": False, "message": "Não autorizado."}), 403
    item = Escala.query.get_or_404(item_id)
    if request.method == "DELETE":
        TrocaSolicitacao.query.filter_by(origem_escala_id=item.id).delete(
            synchronize_session=False
        )
        ProducaoCooperado.query.filter_by(escala_id=item.id).update(
            {ProducaoCooperado.escala_id: None},
            synchronize_session=False,
        )
        db.session.delete(item)
        db.session.commit()
        return jsonify({"ok": True, "message": "Escala excluída."})

    payload = request.get_json(silent=True) or {}
    for field in ("data", "turno", "horario", "contrato", "cooperado_nome"):
        if field in payload:
            setattr(item, field, str(payload.get(field) or "").strip() or None)
    if "cooperado_id" in payload:
        raw = payload.get("cooperado_id")
        item.cooperado_id = int(raw) if raw not in (None, "") else None
        if item.cooperado_id:
            item.cooperado_nome = None
    if "restaurante_id" in payload:
        raw = payload.get("restaurante_id")
        item.restaurante_id = int(raw) if raw not in (None, "") else None
    db.session.commit()
    return jsonify({"ok": True, "message": "Escala atualizada."})


@app.delete("/api/admin/escalas")
def api_admin_escalas_bulk():
    if _require_role("admin"):
        return jsonify({"ok": False, "message": "Não autorizado."}), 403
    payload = request.get_json(silent=True) or {}
    ids = []
    for raw in payload.get("ids") or []:
        try:
            ids.append(int(raw))
        except Exception:
            continue
    if not ids:
        return jsonify({"ok": False, "message": "Nenhuma escala selecionada."}), 400
    TrocaSolicitacao.query.filter(TrocaSolicitacao.origem_escala_id.in_(ids)).delete(
        synchronize_session=False
    )
    ProducaoCooperado.query.filter(ProducaoCooperado.escala_id.in_(ids)).update(
        {ProducaoCooperado.escala_id: None},
        synchronize_session=False,
    )
    deleted = Escala.query.filter(Escala.id.in_(ids)).delete(synchronize_session=False)
    db.session.commit()
    return jsonify({"ok": True, "message": f"{deleted} escala(s) excluída(s)."})


def _wrap_legacy_launch() -> None:
    original = app.view_functions.get("lancar_producao")
    if not original or getattr(original, "_coopex_upgrade_wrapped", False):
        return

    @wraps(original)
    def wrapped(*args, **kwargs):
        if request.method == "POST" and (session.get("user_tipo") or "").lower() == "restaurante":
            rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first()
            form = request.form
            coop_id = form.get("cooperado_id", type=int)
            data_ref = _parse_date(form.get("data")) or datetime.now(TZ).date()
            start = _norm_time(form.get("hora_inicio"))
            end = _norm_time(form.get("hora_fim"))
            if rest and coop_id:
                pending_query = ProducaoCooperado.query.filter_by(
                    restaurante_id=rest.id,
                    cooperado_id=coop_id,
                    data=data_ref,
                    status="pendente",
                )
                pending = next(
                    (
                        item
                        for item in pending_query.order_by(ProducaoCooperado.id.asc()).all()
                        if _overlap(item.hora_inicio, item.hora_fim, start, end)
                    ),
                    None,
                )
                if pending:
                    quantity = form.get("qtd_entregas", type=int) or pending.qtd_entregas
                    total = _money(form.get("valor"), pending.valor_total)
                    unit = round(total / quantity, 2) if quantity else pending.valor_unitario
                    ok, message = _approve_item(
                        pending,
                        quantity=quantity,
                        unit_value=unit,
                    )
                    if ok:
                        db.session.commit()
                        flash(message, "success")
                    else:
                        db.session.rollback()
                        flash(message, "warning")
                    return redirect(url_for("portal_restaurante", view="lancamentos"))
        return original(*args, **kwargs)

    wrapped._coopex_upgrade_wrapped = True
    app.view_functions["lancar_producao"] = wrapped


def _wrap_admin_dashboard() -> None:
    original = app.view_functions.get("admin_dashboard")
    if not original or getattr(original, "_coopex_upgrade_wrapped", False):
        return

    @wraps(original)
    def wrapped(*args, **kwargs):
        if request.method == "GET" and not request.args.get("tab"):
            return redirect(url_for("admin_rapido", view="lancamentos"))
        return original(*args, **kwargs)

    wrapped._coopex_upgrade_wrapped = True
    app.view_functions["admin_dashboard"] = wrapped


_wrap_legacy_launch()
_wrap_admin_dashboard()


def _inject_html(response, marker: str, payload: str):
    if response.direct_passthrough:
        return response
    content_type = response.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        return response
    try:
        html = response.get_data(as_text=True)
    except Exception:
        return response
    if marker in html:
        return response
    if "</body>" not in html:
        return response
    response.set_data(html.replace("</body>", payload + "\n</body>"))
    response.headers["Content-Length"] = str(len(response.get_data()))
    return response


COOP_FLOATING = r'''
<!-- coopex-upgrade-coop -->
<style>
#coopexProdDock{position:fixed;right:14px;bottom:16px;z-index:9999;display:flex;gap:9px;align-items:center}
#coopexProdDock a{display:flex;align-items:center;gap:8px;text-decoration:none;border-radius:999px;padding:12px 16px;font:800 14px/1 Inter,Arial,sans-serif;box-shadow:0 12px 30px rgba(15,23,42,.25)}
#coopexProdDock .prod{background:linear-gradient(135deg,#ff7a18,#ff9a3d);color:#fff}
#coopexProdDock .agenda{background:#fff;color:#2747d9;border:1px solid rgba(39,71,217,.18)}
@media(max-width:520px){#coopexProdDock{left:10px;right:10px;bottom:10px}#coopexProdDock a{flex:1;justify-content:center;padding:13px 10px}}
</style>
<div id="coopexProdDock">
<a class="agenda" href="/coop/agenda"><i class="bi bi-calendar3"></i> Escala</a>
<a class="prod" href="/coop/producao"><i class="bi bi-plus-circle-fill"></i> Informar produção</a>
</div>
'''


REST_FLOATING = r'''
<!-- coopex-upgrade-rest -->
<style>
#coopexPendingDock{position:fixed;right:14px;bottom:16px;z-index:9999;display:flex;gap:8px;align-items:center}
#coopexPendingDock a,#coopexPendingDock button{border:0;border-radius:999px;padding:12px 15px;font:800 14px/1 Inter,Arial,sans-serif;box-shadow:0 12px 30px rgba(15,23,42,.25)}
#coopexPendingDock a{background:linear-gradient(135deg,#f59e0b,#fb923c);color:#fff;text-decoration:none}
#coopexPendingDock button{background:#fff;color:#2747d9}
#coopexPendingCount{display:inline-flex;min-width:22px;height:22px;padding:0 6px;border-radius:999px;background:#fff;color:#b45309;align-items:center;justify-content:center;margin-left:6px}
</style>
<div id="coopexPendingDock">
<button type="button" id="coopexEnableSound">Ativar alerta</button>
<a href="/rest/producoes/pendentes">Produções pendentes <span id="coopexPendingCount">0</span></a>
</div>
<script>
(function(){
 let enabled=false,last=0,audio=null;
 function beep(){
   if(!enabled)return;
   try{
     audio=audio||new (window.AudioContext||window.webkitAudioContext)();
     const o=audio.createOscillator(),g=audio.createGain();
     o.frequency.value=880;g.gain.value=.18;o.connect(g);g.connect(audio.destination);o.start();o.stop(audio.currentTime+.35);
   }catch(e){}
 }
 document.getElementById('coopexEnableSound')?.addEventListener('click',function(){enabled=true;this.textContent='Alerta ativo';beep()});
 async function poll(){
   try{
     const r=await fetch('/api/rest/producoes/pendentes/status',{cache:'no-store'});
     const j=await r.json();
     const count=Number(j.count||0),latest=Number(j.latest||0);
     document.getElementById('coopexPendingCount').textContent=count;
     if(last && latest>last)beep();
     last=Math.max(last,latest);
   }catch(e){}
 }
 poll();setInterval(poll,5000);
})();
</script>
'''


ADMIN_BANK = r'''
<!-- coopex-upgrade-admin -->
<style>
:root{--coopex-nav-h:72px}
body{padding-top:var(--coopex-nav-h)!important}
.sidebar{display:none!important}
.main{margin-left:0!important;width:100%!important;padding-top:20px!important}
#coopexBankNav{position:fixed;top:0;left:0;right:0;height:var(--coopex-nav-h);z-index:10050;background:rgba(255,255,255,.96);backdrop-filter:blur(14px);border-bottom:1px solid #e6ebf5;box-shadow:0 8px 28px rgba(15,23,42,.10);display:flex;align-items:center;gap:8px;padding:9px 14px;overflow-x:auto}
#coopexBankNav .brand{background:linear-gradient(135deg,#2747d9,#3157ff);color:#fff;border-radius:15px;padding:11px 14px;font:900 14px/1 Inter,Arial,sans-serif;white-space:nowrap;margin:0 5px 0 0}
#coopexBankNav a{display:flex;align-items:center;gap:7px;color:#334155;text-decoration:none;border-radius:13px;padding:10px 12px;font:800 13px/1 Inter,Arial,sans-serif;white-space:nowrap;background:#f8faff;border:1px solid #e9eefb}
#coopexBankNav a:hover{background:#eef3ff;color:#2747d9}
#coopexBankNav a.primary{background:#2747d9;color:#fff;border-color:#2747d9}
@media(max-width:700px){:root{--coopex-nav-h:64px}#coopexBankNav{padding:7px 8px}#coopexBankNav .brand{display:none}#coopexBankNav a{padding:9px 10px}}
</style>
<nav id="coopexBankNav">
<span class="brand">COOPEX FINANÇAS</span>
<a class="primary" href="/admin/rapido?view=lancamentos"><i class="bi bi-lightning-charge-fill"></i> Lançamentos rápidos</a>
<a href="/admin/rapido?view=escalas"><i class="bi bi-calendar-week"></i> Escala rápida</a>
<a href="/admin/producoes/pendentes"><i class="bi bi-check2-square"></i> Produções pendentes</a>
<a href="/admin?tab=receitas"><i class="bi bi-cash-stack"></i> Receitas</a>
<a href="/admin?tab=despesas"><i class="bi bi-receipt"></i> Despesas</a>
<a href="/admin?tab=cooperados"><i class="bi bi-people"></i> Cooperados</a>
<a href="/admin?tab=restaurantes"><i class="bi bi-shop"></i> Estabelecimentos</a>
<a href="/admin?tab=avaliacoes"><i class="bi bi-star"></i> Avaliações</a>
<a href="/admin?tab=avisos"><i class="bi bi-megaphone"></i> Avisos</a>
<a href="/admin?tab=documentos"><i class="bi bi-folder2-open"></i> Documentos</a>
<a href="/admin?tab=tabelas"><i class="bi bi-table"></i> Tabelas</a>
<a href="/admin?tab=config"><i class="bi bi-gear"></i> Configurações</a>
</nav>
'''


@app.after_request
def coopex_upgrade_after_request(response):
    path = request.path
    if path.startswith("/portal/cooperado") or path == "/painel/cooperado":
        response = _inject_html(
            response,
            "coopex-upgrade-coop",
            COOP_FLOATING,
        )
    elif path.startswith("/portal/restaurante"):
        response = _inject_html(
            response,
            "coopex-upgrade-rest",
            REST_FLOATING,
        )
    if path.startswith("/admin"):
        response = _inject_html(
            response,
            "coopex-upgrade-admin",
            ADMIN_BANK,
        )

    if path.startswith("/static/"):
        response.headers.setdefault(
            "Cache-Control",
            "public, max-age=86400, immutable",
        )

    content_type = response.headers.get("Content-Type", "")
    if request.method == "GET" and response.status_code == 200 and (
        "text/html" in content_type or "application/json" in content_type
    ):
        raw = response.get_data()
        if raw:
            etag = hashlib.sha256(raw).hexdigest()
            response.set_etag(etag)
            if request.if_none_match and request.if_none_match.contains(etag):
                response = make_response("", 304)
                response.set_etag(etag)
                return response

    accepted = request.headers.get("Accept-Encoding", "")
    if (
        "gzip" in accepted.lower()
        and response.status_code == 200
        and "Content-Encoding" not in response.headers
        and ("text/html" in content_type or "application/json" in content_type)
    ):
        raw = response.get_data()
        if len(raw) >= 1000:
            compressed = gzip.compress(raw, compresslevel=5)
            response.set_data(compressed)
            response.headers["Content-Encoding"] = "gzip"
            response.headers["Content-Length"] = str(len(compressed))
            response.headers["Vary"] = "Accept-Encoding"
    return response
