"""Calendário comercial, lembretes privados e Caixa Postal COOPEX.

Leve por desenho:
- feriados calculados localmente, sem API externa;
- lembretes persistidos em tabelas próprias;
- scheduler dorme até o próximo disparo (sem polling de banco);
- Caixa Postal usa SSE apenas para o painel administrativo.
"""
from __future__ import annotations

import json
import queue
import threading
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import app as legacy
from flask import Response, abort, jsonify, render_template, request, session, stream_with_context, url_for
from sqlalchemy import UniqueConstraint

app = legacy.app
db = legacy.db
Restaurante = legacy.Restaurante

TZ = ZoneInfo("America/Fortaleza")


class CalendarioLembrete(db.Model):
    __tablename__ = "calendario_lembretes"
    id = db.Column(db.Integer, primary_key=True)
    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=False, index=True)
    titulo = db.Column(db.String(140), nullable=False)
    descricao = db.Column(db.Text, nullable=True)
    data_evento = db.Column(db.Date, nullable=False, index=True)
    hora_evento = db.Column(db.String(5), nullable=True)
    destino = db.Column(db.String(20), nullable=False, default="pessoal")  # pessoal | coopex
    antecedencia_dias = db.Column(db.Integer, nullable=False, default=0)
    enviar_agora = db.Column(db.Boolean, nullable=False, default=False)
    disparar_em = db.Column(db.DateTime, nullable=False, index=True)  # UTC naive
    enviado_em = db.Column(db.DateTime, nullable=True, index=True)
    concluido_em = db.Column(db.DateTime, nullable=True, index=True)
    criado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class CaixaPostalMensagem(db.Model):
    __tablename__ = "caixa_postal_mensagens"
    id = db.Column(db.Integer, primary_key=True)
    lembrete_id = db.Column(db.Integer, db.ForeignKey("calendario_lembretes.id"), nullable=True, index=True)
    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=False, index=True)
    assunto = db.Column(db.String(160), nullable=False)
    mensagem = db.Column(db.Text, nullable=False)
    data_evento = db.Column(db.Date, nullable=True)
    criado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    lido_em = db.Column(db.DateTime, nullable=True, index=True)
    __table_args__ = (UniqueConstraint("lembrete_id", name="uq_caixa_postal_lembrete"),)


_schema_lock = threading.Lock()
_schema_ready = False


def ensure_calendar_schema():
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        CalendarioLembrete.__table__.create(bind=db.engine, checkfirst=True)
        CaixaPostalMensagem.__table__.create(bind=db.engine, checkfirst=True)
        _schema_ready = True


def _rest_atual():
    if (session.get("user_tipo") or "").lower() != "restaurante":
        return None
    return Restaurante.query.filter_by(usuario_id=session.get("user_id")).first()


def _is_admin():
    return (session.get("user_tipo") or "").lower() == "admin" and bool(session.get("user_id"))


def _utc_naive_from_local(d: date, hhmm: str | None) -> datetime:
    raw = (hhmm or "09:00").strip()
    try:
        hh, mm = [int(x) for x in raw.split(":", 1)]
        lt = time(max(0, min(23, hh)), max(0, min(59, mm)))
    except Exception:
        lt = time(9, 0)
    aware = datetime.combine(d, lt).replace(tzinfo=TZ)
    return aware.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def _local_iso_from_utc(dt: datetime | None):
    if not dt:
        return None
    return dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(TZ).isoformat()


def _easter(year: int) -> date:
    # Meeus/Jones/Butcher
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def holidays_for_year(year: int):
    year = max(2024, min(2100, int(year)))
    easter = _easter(year)
    rows = [
        (date(year,1,1), "Confraternização Universal", "nacional"),
        (date(year,1,6), "Santos Reis", "municipal"),
        (easter - timedelta(days=2), "Paixão de Cristo", "municipal"),
        (date(year,4,21), "Tiradentes", "nacional"),
        (date(year,5,1), "Dia do Trabalho", "nacional"),
        (easter + timedelta(days=60), "Corpus Christi", "municipal"),
        (date(year,9,7), "Independência do Brasil", "nacional"),
        (date(year,10,3), "Mártires de Cunhaú e Uruaçu", "estadual"),
        (date(year,10,12), "Nossa Senhora Aparecida", "nacional"),
        (date(year,11,2), "Finados", "nacional"),
        (date(year,11,15), "Proclamação da República", "nacional"),
        (date(year,11,20), "Dia Nacional de Zumbi e da Consciência Negra", "nacional"),
        (date(year,11,21), "Nossa Senhora da Apresentação — Padroeira de Natal", "municipal"),
        (date(year,12,25), "Natal", "nacional"),
    ]
    labels={"municipal":"Municipal — Natal","estadual":"Estadual — RN","nacional":"Nacional"}
    return [{"date":d.isoformat(),"title":t,"scope":s,"scope_label":labels[s]} for d,t,s in sorted(rows)]


# -------- Eventos em tempo real somente para Admin (sem polling) --------
_admin_subscribers: list[queue.Queue] = []
_sub_lock = threading.Lock()


def _emit_admin(payload: dict):
    with _sub_lock:
        subscribers = list(_admin_subscribers)
    for q in subscribers:
        try:
            q.put_nowait(payload)
        except Exception:
            pass


def _mailbox_unread():
    ensure_calendar_schema()
    return int(CaixaPostalMensagem.query.filter(CaixaPostalMensagem.lido_em.is_(None)).count())


@app.get("/api/admin/caixa-postal/eventos", endpoint="admin_caixa_postal_eventos")
def admin_caixa_postal_eventos():
    if not _is_admin():
        abort(403)
    q = queue.Queue(maxsize=50)
    with _sub_lock:
        _admin_subscribers.append(q)

    @stream_with_context
    def generate():
        try:
            yield "event: ready\ndata: {}\n\n"
            while True:
                try:
                    payload = q.get(timeout=25)
                    yield "event: mailbox\ndata: " + json.dumps(payload, ensure_ascii=False) + "\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with _sub_lock:
                if q in _admin_subscribers:
                    _admin_subscribers.remove(q)

    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control":"no-cache, no-transform",
        "X-Accel-Buffering":"no",
    })


def _deliver(rem: CalendarioLembrete):
    if rem.enviado_em:
        return
    now = datetime.utcnow()
    if rem.destino == "coopex":
        exists = CaixaPostalMensagem.query.filter_by(lembrete_id=rem.id).first()
        if not exists:
            rest = Restaurante.query.get(rem.restaurante_id)
            rest_name = (rest.nome if rest else "Estabelecimento") or "Estabelecimento"
            msg = CaixaPostalMensagem(
                lembrete_id=rem.id,
                restaurante_id=rem.restaurante_id,
                assunto=rem.titulo,
                mensagem=(rem.descricao or "Lembrete enviado pelo estabelecimento."),
                data_evento=rem.data_evento,
                criado_em=now,
            )
            db.session.add(msg)
            db.session.flush()
            payload = {
                "id": msg.id,
                "restaurante": rest_name,
                "assunto": msg.assunto,
                "data_evento": rem.data_evento.isoformat() if rem.data_evento else None,
            }
        else:
            payload = {"id":exists.id,"assunto":exists.assunto}
        rem.enviado_em = now
        db.session.commit()
        payload["unread"] = _mailbox_unread()
        _emit_admin(payload)
    else:
        # Lembrete pessoal: fica marcado como disparado. O navegador agenda o
        # som localmente a partir da carga única dos lembretes (sem polling).
        rem.enviado_em = now
        db.session.commit()


_sched_condition = threading.Condition()
_sched_started = False
_sched_lock = threading.Lock()


def _scheduler_loop():
    while True:
        sleep_for = 3600.0
        try:
            with app.app_context():
                ensure_calendar_schema()
                now = datetime.utcnow()
                due = (
                    CalendarioLembrete.query
                    .filter(CalendarioLembrete.enviado_em.is_(None), CalendarioLembrete.disparar_em <= now)
                    .order_by(CalendarioLembrete.disparar_em.asc())
                    .limit(100)
                    .all()
                )
                for rem in due:
                    _deliver(rem)

                nxt = (
                    CalendarioLembrete.query
                    .filter(CalendarioLembrete.enviado_em.is_(None))
                    .order_by(CalendarioLembrete.disparar_em.asc())
                    .first()
                )
                if nxt:
                    sleep_for = max(1.0, min(86400.0, (nxt.disparar_em - datetime.utcnow()).total_seconds()))
        except Exception:
            try:
                with app.app_context():
                    db.session.rollback()
            except Exception:
                pass
            sleep_for = 60.0

        with _sched_condition:
            _sched_condition.wait(timeout=sleep_for)


def start_scheduler():
    global _sched_started
    with _sched_lock:
        if _sched_started:
            return
        _sched_started = True
        threading.Thread(target=_scheduler_loop, name="coopex-calendar-scheduler", daemon=True).start()


def wake_scheduler():
    start_scheduler()
    with _sched_condition:
        _sched_condition.notify_all()


@app.get("/api/rest/calendario", endpoint="rest_calendario_api")
def rest_calendario_api():
    rest = _rest_atual()
    if not rest:
        abort(403)
    ensure_calendar_schema()
    year = request.args.get("year", type=int) or datetime.now(TZ).year
    status = (request.args.get("status") or "todos").lower()
    q = CalendarioLembrete.query.filter(
        CalendarioLembrete.restaurante_id == rest.id,
        db.extract("year", CalendarioLembrete.data_evento) == year,
    )
    if status == "pendentes":
        q = q.filter(CalendarioLembrete.concluido_em.is_(None))
    elif status == "concluidos":
        q = q.filter(CalendarioLembrete.concluido_em.isnot(None))
    elif status == "coopex":
        q = q.filter(CalendarioLembrete.destino == "coopex")

    reminders=[]
    for x in q.order_by(CalendarioLembrete.data_evento.asc(), CalendarioLembrete.id.asc()).all():
        reminders.append({
            "id":x.id,"title":x.titulo,"description":x.descricao or "",
            "date":x.data_evento.isoformat(),"time":x.hora_evento or "",
            "target":x.destino,"advance_days":x.antecedencia_dias,
            "send_at":_local_iso_from_utc(x.disparar_em),
            "sent_at":_local_iso_from_utc(x.enviado_em),
            "completed":bool(x.concluido_em),
        })
    return jsonify(ok=True, year=year, holidays=holidays_for_year(year), reminders=reminders)


@app.get("/api/rest/calendario/proximos", endpoint="rest_calendario_proximos")
def rest_calendario_proximos():
    """Uma única carga para o navegador agendar sons pessoais; não faz polling."""
    rest = _rest_atual()
    if not rest:
        abort(403)
    ensure_calendar_schema()
    now=datetime.utcnow()
    end=now+timedelta(days=7)
    rows=CalendarioLembrete.query.filter(
        CalendarioLembrete.restaurante_id==rest.id,
        CalendarioLembrete.destino=="pessoal",
        CalendarioLembrete.enviado_em.is_(None),
        CalendarioLembrete.disparar_em>=now,
        CalendarioLembrete.disparar_em<=end,
    ).order_by(CalendarioLembrete.disparar_em.asc()).limit(30).all()
    return jsonify(ok=True, reminders=[{
        "id":x.id,"title":x.titulo,"description":x.descricao or "",
        "fire_at":_local_iso_from_utc(x.disparar_em)
    } for x in rows])


@app.post("/api/rest/calendario", endpoint="rest_calendario_criar")
def rest_calendario_criar():
    rest=_rest_atual()
    if not rest: abort(403)
    ensure_calendar_schema()
    data=request.get_json(silent=True) or request.form
    titulo=(data.get("title") or data.get("titulo") or "").strip()
    descricao=(data.get("description") or data.get("descricao") or "").strip()
    destino=(data.get("target") or data.get("destino") or "pessoal").strip().lower()
    try: event_date=date.fromisoformat(str(data.get("date") or data.get("data_evento")))
    except Exception: return jsonify(ok=False,message="Informe uma data válida."),400
    if not titulo: return jsonify(ok=False,message="Informe o título do lembrete."),400
    if destino not in {"pessoal","coopex"}: destino="pessoal"
    hora=(data.get("time") or data.get("hora_evento") or "09:00").strip()[:5]
    send_now=str(data.get("send_now") or "").lower() in {"1","true","on","sim"}
    try: advance=max(0,min(60,int(data.get("advance_days") or 0)))
    except Exception: advance=0

    now_local=datetime.now(TZ)
    if destino=="coopex":
        if send_now:
            fire=datetime.utcnow()
        else:
            send_date=event_date-timedelta(days=advance)
            # tempo hábil: precisa programar até o dia anterior ao envio à COOPEX
            if now_local.date() >= send_date:
                return jsonify(ok=False,message=f"Prazo insuficiente. Para chegar à COOPEX em {send_date.strftime('%d/%m')}, programe até o dia anterior ou use “Enviar agora”."),400
            fire=_utc_naive_from_local(send_date,"08:00")
    else:
        fire=_utc_naive_from_local(event_date,hora)
        if fire <= datetime.utcnow():
            return jsonify(ok=False,message="O lembrete pessoal precisa estar em uma data/horário futuro."),400

    rem=CalendarioLembrete(
        restaurante_id=rest.id,titulo=titulo,descricao=descricao or None,
        data_evento=event_date,hora_evento=hora,destino=destino,
        antecedencia_dias=advance,enviar_agora=send_now,disparar_em=fire,
    )
    db.session.add(rem);db.session.commit()
    if destino=="coopex" and send_now:
        _deliver(rem)
    wake_scheduler()
    return jsonify(ok=True,id=rem.id,message="Lembrete salvo.")


@app.post("/api/rest/calendario/<int:item_id>/concluir", endpoint="rest_calendario_concluir")
def rest_calendario_concluir(item_id):
    rest=_rest_atual()
    if not rest: abort(403)
    ensure_calendar_schema()
    row=CalendarioLembrete.query.filter_by(id=item_id,restaurante_id=rest.id).first_or_404()
    row.concluido_em=None if row.concluido_em else datetime.utcnow()
    db.session.commit()
    return jsonify(ok=True,completed=bool(row.concluido_em))


@app.delete("/api/rest/calendario/<int:item_id>", endpoint="rest_calendario_excluir")
def rest_calendario_excluir(item_id):
    rest=_rest_atual()
    if not rest: abort(403)
    ensure_calendar_schema()
    row=CalendarioLembrete.query.filter_by(id=item_id,restaurante_id=rest.id).first_or_404()
    if row.destino=="coopex" and row.enviado_em:
        return jsonify(ok=False,message="Este aviso já foi enviado à COOPEX e permanece no histórico."),409
    db.session.delete(row);db.session.commit();wake_scheduler()
    return jsonify(ok=True)


def admin_caixa_postal_calendar():
    """Amplia a Caixa Postal existente sem perder as respostas do rastreamento."""
    if not _is_admin():
        abort(403)
    ensure_calendar_schema()
    try:
        legacy._ensure_tracking_schema()
    except Exception:
        pass

    filtro=(request.args.get("status") or "todos").lower()
    q=CaixaPostalMensagem.query.order_by(CaixaPostalMensagem.criado_em.desc())
    if filtro=="nao_lidos":
        q=q.filter(CaixaPostalMensagem.lido_em.is_(None))
    elif filtro=="lidos":
        q=q.filter(CaixaPostalMensagem.lido_em.isnot(None))
    mail_rows=q.limit(500).all()
    rest_ids={x.restaurante_id for x in mail_rows}
    rest_map={r.id:r for r in Restaurante.query.filter(Restaurante.id.in_(rest_ids)).all()} if rest_ids else {}

    tracking_rows=[]
    try:
        tracking_rows=(
            db.session.query(legacy.RastreamentoPesquisa, Restaurante)
            .join(Restaurante, Restaurante.id==legacy.RastreamentoPesquisa.restaurante_id)
            .order_by(legacy.RastreamentoPesquisa.atualizado_em.desc())
            .all()
        )
    except Exception:
        db.session.rollback()

    return render_template(
        "admin_caixa_postal.html",
        mail_rows=mail_rows,
        tracking_rows=tracking_rows,
        rest_map=rest_map,
        unread=_mailbox_unread(),
        active_tab="caixa_postal",
    )


# A URL /admin/caixa-postal já existe no sistema para respostas do rastreamento.
# Troca somente a função da rota, preservando o mesmo endpoint usado pelo menu.
legacy.app.view_functions["admin_caixa_postal"] = admin_caixa_postal_calendar


@app.post("/admin/caixa-postal/<int:item_id>/lido", endpoint="admin_caixa_postal_lido")
def admin_caixa_postal_lido(item_id):
    if not _is_admin(): abort(403)
    ensure_calendar_schema()
    row=CaixaPostalMensagem.query.get_or_404(item_id)
    row.lido_em=row.lido_em or datetime.utcnow();db.session.commit()
    return jsonify(ok=True,unread=_mailbox_unread())


@app.context_processor
def _calendar_mailbox_context():
    if not _is_admin():
        return {}
    try:
        return {"admin_mailbox_unread_count":_mailbox_unread()}
    except Exception:
        return {"admin_mailbox_unread_count":0}


start_scheduler()
