from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from flask import abort, flash, redirect, render_template, request, send_file, session, url_for
from sqlalchemy import and_, case, func, or_

import app as legacy
import admin_runtime_v7 as runtime
import operational_rules_v5 as operational

app = legacy.app
db = legacy.db
Usuario = legacy.Usuario
Cooperado = legacy.Cooperado
Restaurante = legacy.Restaurante
Lancamento = legacy.Lancamento
Escala = legacy.Escala
TrocaSolicitacao = legacy.TrocaSolicitacao
EscalaHistorico = legacy.EscalaHistorico
TrocaHistorico = legacy.TrocaHistorico
Documento = legacy.Documento
Tabela = legacy.Tabela
Aviso = legacy.Aviso
AvaliacaoCooperado = legacy.AvaliacaoCooperado
ReceitaCooperado = legacy.ReceitaCooperado
DespesaCooperado = legacy.DespesaCooperado
ReceitaCooperativa = legacy.ReceitaCooperativa
DespesaCooperativa = legacy.DespesaCooperativa
TZ = ZoneInfo("America/Fortaleza")
BUILD = "20260807-1215"


class CooperadoArquivadoV8(db.Model):
    __tablename__ = "cooperados_arquivados_v8"
    cooperado_id = db.Column(db.Integer, primary_key=True)
    nome_original = db.Column(db.String(120))
    telefone_original = db.Column(db.String(30))
    excluido_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)


with app.app_context():
    CooperadoArquivadoV8.__table__.create(bind=db.engine, checkfirst=True)


# O filtro operacional V5 continua valendo no sistema. Nas páginas administrativas
# V8 precisamos enxergar também inativos/históricos e aplicar o filtro de forma
# explícita em cada consulta.
_original_operational_filter = operational._operational_filter_enabled


def _operational_filter_v8() -> bool:
    try:
        if request.path.startswith("/admin/leve/") or request.path.startswith("/media/cooperado/"):
            return False
    except Exception:
        pass
    return _original_operational_filter()


operational._operational_filter_enabled = _operational_filter_v8


def _guard(permission: str | None = None):
    if (session.get("user_tipo") or "").strip().lower() != "admin":
        return redirect(url_for("login"))
    if permission:
        try:
            if not legacy.is_admin_master() and not legacy.admin_has_perm(permission, "ver"):
                abort(403)
        except AttributeError:
            pass
    return None


def _parse_date(raw, default: date | None = None) -> date | None:
    try:
        parsed = legacy._parse_date(raw)
        return parsed or default
    except Exception:
        value = str(raw or "").strip()
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except Exception:
            return default


def _fmt_phone(value: str | None) -> str:
    raw = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(raw) == 11:
        return f"({raw[:2]}) {raw[2:7]}-{raw[7:]}"
    if len(raw) == 10:
        return f"({raw[:2]}) {raw[2:6]}-{raw[6:]}"
    return str(value or "").strip()


def _active_coops():
    return (
        Cooperado.query.join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(or_(Usuario.ativo.is_(True), Usuario.ativo.is_(None)))
        .order_by(Cooperado.nome.asc())
        .all()
    )


def _active_coop_ids_names():
    rows = _active_coops()
    return {c.id for c in rows}, {(c.nome or "").strip().casefold() for c in rows}, rows


def _archived_ids() -> set[int]:
    return {int(x[0]) for x in db.session.query(CooperadoArquivadoV8.cooperado_id).all()}


def _render(view: str, title: str, subtitle: str = "", **ctx):
    base = dict(
        view=view,
        page_title=title,
        page_subtitle=subtitle,
        build=BUILD,
        today=date.today(),
    )
    base.update(ctx)
    return render_template("admin_light_v8.html", **base)


def _period_from_request(default_today: bool = True):
    today = date.today()
    di = _parse_date(request.args.get("data_inicio"))
    df = _parse_date(request.args.get("data_fim"))
    if default_today and not di and not df:
        di = df = today
    elif di and not df:
        df = di
    elif df and not di:
        di = df
    if di and df and df < di:
        di, df = df, di
    return di, df


@app.get("/admin/leve/resumo", endpoint="admin_light_summary")
def admin_light_summary():
    denied = _guard("lancamentos")
    if denied:
        return denied
    di, df = _period_from_request(True)

    launch_filter = [Lancamento.data >= di, Lancamento.data <= df]
    prod_total, qtd_lanc = db.session.query(
        func.coalesce(func.sum(Lancamento.valor), 0.0), func.count(Lancamento.id)
    ).filter(*launch_filter).one()
    prod_total = float(prod_total or 0.0)

    prod_rows = (
        db.session.query(
            Cooperado.id,
            Cooperado.nome,
            func.coalesce(func.sum(Lancamento.valor), 0.0).label("prod"),
            func.count(Lancamento.id).label("qtd"),
        )
        .join(Lancamento, Lancamento.cooperado_id == Cooperado.id)
        .filter(*launch_filter)
        .group_by(Cooperado.id, Cooperado.nome)
        .order_by(Cooperado.nome.asc())
        .all()
    )

    rec_map = dict(
        db.session.query(ReceitaCooperado.cooperado_id, func.coalesce(func.sum(ReceitaCooperado.valor), 0.0))
        .filter(ReceitaCooperado.data >= di, ReceitaCooperado.data <= df)
        .group_by(ReceitaCooperado.cooperado_id).all()
    )

    desp_query = db.session.query(
        DespesaCooperado.cooperado_id,
        func.coalesce(func.sum(case((DespesaCooperado.eh_adiantamento.is_(True), 0.0), else_=DespesaCooperado.valor)), 0.0),
        func.coalesce(func.sum(case((DespesaCooperado.eh_adiantamento.is_(True), DespesaCooperado.valor), else_=0.0)), 0.0),
    ).filter(
        DespesaCooperado.cooperado_id.isnot(None),
        or_(
            and_(DespesaCooperado.data_inicio.isnot(None), DespesaCooperado.data_fim.isnot(None), DespesaCooperado.data_inicio <= df, DespesaCooperado.data_fim >= di),
            and_(DespesaCooperado.data_inicio.is_(None), DespesaCooperado.data >= di, DespesaCooperado.data <= df),
        ),
    ).group_by(DespesaCooperado.cooperado_id)
    desp_map = {int(cid): (float(d or 0), float(a or 0)) for cid, d, a in desp_query.all()}

    rows = []
    total_receitas_coop = 0.0
    total_desp_coop = 0.0
    total_adiant = 0.0
    for cid, nome, prod, qtd in prod_rows:
        prod = float(prod or 0.0)
        rec = float(rec_map.get(cid, 0.0) or 0.0)
        desp, adiant = desp_map.get(cid, (0.0, 0.0))
        inss = round(prod * 0.04, 2)
        sest = round(prod * 0.005, 2)
        receber = round(prod + rec - inss - sest - desp - adiant, 2)
        rows.append(SimpleNamespace(id=cid, nome=nome, prod=prod, qtd=int(qtd or 0), inss=inss, sest=sest, rec=rec, desp=desp, adiant=adiant, receber=receber))
        total_receitas_coop += rec
        total_desp_coop += desp
        total_adiant += adiant

    rec_coop_total = float(db.session.query(func.coalesce(func.sum(ReceitaCooperativa.valor_total), 0.0)).filter(ReceitaCooperativa.data >= di, ReceitaCooperativa.data <= df).scalar() or 0.0)
    desp_coop_total = float(db.session.query(func.coalesce(func.sum(DespesaCooperativa.valor), 0.0)).filter(DespesaCooperativa.data >= di, DespesaCooperativa.data <= df).scalar() or 0.0)

    totals = SimpleNamespace(
        prod=prod_total,
        qtd=int(qtd_lanc or 0),
        inss=round(prod_total * 0.04, 2),
        sest=round(prod_total * 0.005, 2),
        rec_coop=round(total_receitas_coop, 2),
        desp_coop=round(total_desp_coop, 2),
        adiant=round(total_adiant, 2),
        receita_central=round(rec_coop_total, 2),
        despesa_central=round(desp_coop_total, 2),
    )
    return _render("resumo", "Resumo", "Por padrão mostra somente o dia. Use o filtro para semana ou outro período.", data_inicio=di, data_fim=df, totals=totals, rows=rows)


@app.get("/admin/leve/lancamentos", endpoint="admin_light_launches")
def admin_light_launches():
    denied = _guard("lancamentos")
    if denied:
        return denied
    di, df = _period_from_request(True)
    q = (request.args.get("q") or "").strip()
    query = Lancamento.query
    if di:
        query = query.filter(Lancamento.data >= di)
    if df:
        query = query.filter(Lancamento.data <= df)
    if q:
        pat = f"%{q}%"
        query = query.join(Cooperado, Lancamento.cooperado_id == Cooperado.id).join(Restaurante, Lancamento.restaurante_id == Restaurante.id).filter(or_(Cooperado.nome.ilike(pat), Restaurante.nome.ilike(pat), Lancamento.descricao.ilike(pat)))
    launches = query.order_by(Lancamento.data.desc(), Lancamento.id.desc()).limit(220).all()
    coop_ids = {l.cooperado_id for l in launches}
    rest_ids = {l.restaurante_id for l in launches}
    coops = {c.id: c for c in Cooperado.query.filter(Cooperado.id.in_(coop_ids)).all()} if coop_ids else {}
    rests = {r.id: r for r in Restaurante.query.filter(Restaurante.id.in_(rest_ids)).all()} if rest_ids else {}
    total = sum(float(l.valor or 0.0) for l in launches)
    return _render("lancamentos", "Lançamentos", "Consulta leve. Até 220 registros por abertura.", data_inicio=di, data_fim=df, q=q, launches=launches, coop_map=coops, rest_map=rests, launch_total=total)


@app.get("/admin/leve/escala", endpoint="admin_light_scale")
def admin_light_scale():
    denied = _guard("escalas")
    if denied:
        return denied
    q = (request.args.get("q") or "").strip().casefold()
    active_ids, active_names, active_coops = _active_coop_ids_names()
    candidates = Escala.query.order_by(Escala.id.desc()).limit(420).all()
    scales = []
    for s in candidates:
        linked_active = s.cooperado_id in active_ids if s.cooperado_id else ((s.cooperado_nome or "").strip().casefold() in active_names)
        if not linked_active:
            continue
        hay = " ".join(str(x or "") for x in (s.data, s.turno, s.horario, s.contrato, s.cooperado_nome)).casefold()
        if q and q not in hay:
            continue
        scales.append(s)
        if len(scales) >= 180:
            break
    rest_ids = {s.restaurante_id for s in scales if s.restaurante_id}
    rest_map = {r.id: r for r in Restaurante.query.filter(Restaurante.id.in_(rest_ids)).all()} if rest_ids else {}
    coop_map = {c.id: c for c in active_coops}
    restaurants = Restaurante.query.filter(or_(Restaurante.ativo.is_(True), Restaurante.ativo.is_(None))).order_by(Restaurante.nome.asc()).all()
    return _render("escala", "Escala", "Somente a escala fica nesta página. Trocas, histórico e documentos estão separados.", q=q, scales=scales, coop_map=coop_map, rest_map=rest_map, cooperados=active_coops, restaurantes=restaurants)


@app.post("/admin/leve/escala/criar", endpoint="admin_light_scale_create")
def admin_light_scale_create():
    denied = _guard("escalas")
    if denied:
        return denied
    cid = request.form.get("cooperado_id", type=int)
    rid = request.form.get("restaurante_id", type=int)
    coop = Cooperado.query.get(cid) if cid else None
    rest = Restaurante.query.get(rid) if rid else None
    if not coop:
        flash("Selecione um cooperado ativo.", "warning")
        return redirect(url_for("admin_light_scale"))
    scale = Escala(
        cooperado_id=coop.id,
        restaurante_id=rest.id if rest else None,
        cooperado_nome=None,
        data=(request.form.get("data") or "").strip() or None,
        turno=(request.form.get("turno") or "").strip() or None,
        horario=(request.form.get("horario") or "").strip() or None,
        contrato=((request.form.get("contrato") or "").strip() or (rest.nome if rest else None)),
    )
    db.session.add(scale)
    db.session.commit()
    flash("Escala adicionada.", "success")
    return redirect(url_for("admin_light_scale"))


@app.get("/admin/leve/trocas", endpoint="admin_light_swaps")
def admin_light_swaps():
    denied = _guard("escalas")
    if denied:
        return denied
    active_ids, _, active_coops = _active_coop_ids_names()
    rows = TrocaSolicitacao.query.filter(TrocaSolicitacao.solicitante_id.in_(active_ids), TrocaSolicitacao.destino_id.in_(active_ids)).order_by(TrocaSolicitacao.id.desc()).limit(120).all() if active_ids else []
    coop_map = {c.id: c for c in active_coops}
    scale_ids = {t.origem_escala_id for t in rows if t.origem_escala_id}
    scale_map = {s.id: s for s in Escala.query.filter(Escala.id.in_(scale_ids)).all()} if scale_ids else {}
    return _render("trocas", "Trocas de escala", "Página separada para não pesar a escala.", trocas=rows, coop_map=coop_map, scale_map=scale_map)


@app.get("/admin/leve/historico", endpoint="admin_light_history")
def admin_light_history():
    denied = _guard("escalas")
    if denied:
        return denied
    tipo = (request.args.get("tipo") or "escala").strip().lower()
    if tipo == "trocas":
        rows = TrocaHistorico.query.order_by(TrocaHistorico.aplicada_em.desc(), TrocaHistorico.id.desc()).limit(180).all()
    else:
        tipo = "escala"
        rows = EscalaHistorico.query.order_by(EscalaHistorico.snapshot_em.desc(), EscalaHistorico.id.desc()).limit(180).all()
    return _render("historico", "Histórico", "Histórico separado e limitado na abertura. Use a busca do navegador ou filtros específicos quando necessário.", history_type=tipo, history_rows=rows)


@app.get("/admin/leve/documentos", endpoint="admin_light_documents")
def admin_light_documents():
    denied = _guard("documentos")
    if denied:
        return denied
    q = (request.args.get("q") or "").strip().casefold()
    coops = _active_coops()
    if q:
        coops = [c for c in coops if q in (c.nome or "").casefold() or q in (c.cnh_numero or "").casefold() or q in (c.placa or "").casefold()]
    today = date.today()
    return _render("documentos", "Documentos", "Documentos não são mais montados dentro da Escala.", cooperados=coops[:220], q=q, status_today=today)


@app.get("/admin/leve/cooperados", endpoint="admin_light_cooperatives")
def admin_light_cooperatives():
    denied = _guard("cooperados")
    if denied:
        return denied
    status = (request.args.get("status") or "ativos").strip().lower()
    q = (request.args.get("q") or "").strip().casefold()
    archived = _archived_ids()
    rows = Cooperado.query.join(Usuario, Cooperado.usuario_id == Usuario.id).add_entity(Usuario).order_by(Cooperado.nome.asc()).all()
    result = []
    for coop, user in rows:
        is_archived = coop.id in archived
        is_active = bool(user.ativo is not False)
        if status == "ativos" and (not is_active or is_archived):
            continue
        if status == "inativos" and (is_active or is_archived):
            continue
        if status == "excluidos" and not is_archived:
            continue
        hay = f"{coop.nome or ''} {coop.telefone or ''} {user.usuario or ''}".casefold()
        if q and q not in hay:
            continue
        result.append(SimpleNamespace(coop=coop, user=user, active=is_active, archived=is_archived, phone=_fmt_phone(coop.telefone)))
    return _render("cooperados", "Cooperados", "Desativado sai de toda operação. Excluído fica somente arquivado para preservar histórico.", cooperados=result[:250], q=q, status=status)


@app.post("/admin/leve/cooperados/<int:coop_id>/status", endpoint="admin_light_coop_status")
def admin_light_coop_status(coop_id: int):
    denied = _guard("cooperados")
    if denied:
        return denied
    coop = Cooperado.query.get_or_404(coop_id)
    user = Usuario.query.get_or_404(coop.usuario_id)
    action = (request.form.get("acao") or "").strip().lower()
    archive = CooperadoArquivadoV8.query.get(coop.id)
    if action == "ativar":
        user.ativo = True
        if archive:
            db.session.delete(archive)
        msg = f"{coop.nome} foi ativado e voltou a participar das operações."
    elif action == "desativar":
        user.ativo = False
        msg = f"{coop.nome} foi desativado. Escala, contratos e trocas deixam de considerá-lo."
    elif action == "excluir":
        user.ativo = False
        if not archive:
            archive = CooperadoArquivadoV8(cooperado_id=coop.id)
            db.session.add(archive)
        archive.nome_original = coop.nome
        archive.telefone_original = coop.telefone
        archive.excluido_em = datetime.utcnow()
        msg = f"{coop.nome} foi excluído do cadastro operacional. O histórico financeiro e de escalas foi preservado."
    else:
        flash("Ação inválida.", "warning")
        return redirect(url_for("admin_light_cooperatives"))
    db.session.commit()
    flash(msg, "success")
    return redirect(url_for("admin_light_cooperatives", status="ativos" if action == "ativar" else ("excluidos" if action == "excluir" else "inativos")))


@app.post("/admin/leve/cooperados/<int:coop_id>/salvar", endpoint="admin_light_coop_save")
def admin_light_coop_save(coop_id: int):
    denied = _guard("cooperados")
    if denied:
        return denied
    coop = Cooperado.query.get_or_404(coop_id)
    coop.nome = (request.form.get("nome") or coop.nome or "").strip()
    coop.telefone = (request.form.get("telefone") or "").strip() or None
    photo = request.files.get("foto")
    if photo and photo.filename:
        payload = photo.read(6 * 1024 * 1024 + 1)
        if len(payload) > 6 * 1024 * 1024:
            flash("A foto deve ter no máximo 6 MB.", "warning")
            return redirect(url_for("admin_light_cooperatives"))
        coop.foto_bytes = payload
        coop.foto_mime = photo.mimetype or "image/jpeg"
        coop.foto_filename = photo.filename[:255]
        coop.foto_url = None
    coop.ultima_atualizacao = datetime.utcnow()
    db.session.commit()
    flash("Cadastro atualizado. Telefone e foto foram gravados no banco.", "success")
    return redirect(url_for("admin_light_cooperatives", status=request.form.get("status") or "ativos"))


@app.get("/media/cooperado/<int:coop_id>", endpoint="admin_light_media_coop")
def admin_light_media_coop(coop_id: int):
    coop = Cooperado.query.get_or_404(coop_id)
    if coop.foto_bytes:
        response = send_file(BytesIO(bytes(coop.foto_bytes)), mimetype=coop.foto_mime or "image/jpeg", max_age=300)
        response.headers["Cache-Control"] = "private, max-age=300"
        return response
    raw = (coop.foto_url or "").strip()
    if raw:
        if raw.startswith("http://") or raw.startswith("https://") or raw.startswith("/"):
            return redirect(raw)
        return redirect(url_for("static", filename=raw.lstrip("/")))
    return redirect(url_for("static", filename="img/default.png"))


@app.get("/admin/leve/avaliacoes", endpoint="admin_light_ratings")
def admin_light_ratings():
    denied = _guard("avaliacoes")
    if denied:
        return denied
    coop_id = request.args.get("cooperado_id", type=int)
    # A classificação administrativa é vitalícia: sem filtro semanal implícito.
    ranking_rows = (
        db.session.query(
            Cooperado.id,
            Cooperado.nome,
            func.count(AvaliacaoCooperado.id),
            func.avg(AvaliacaoCooperado.estrelas_geral),
            func.avg(AvaliacaoCooperado.estrelas_pontualidade),
            func.avg(AvaliacaoCooperado.estrelas_educacao),
            func.avg(AvaliacaoCooperado.estrelas_eficiencia),
            func.avg(AvaliacaoCooperado.estrelas_apresentacao),
        )
        .join(AvaliacaoCooperado, AvaliacaoCooperado.cooperado_id == Cooperado.id)
        .group_by(Cooperado.id, Cooperado.nome)
        .order_by(func.avg(AvaliacaoCooperado.estrelas_geral).desc(), Cooperado.nome.asc())
        .all()
    )
    ranking = []
    labels = [("Pontualidade", 4), ("Educação", 5), ("Eficiência", 6), ("Apresentação", 7)]
    for row in ranking_rows:
        criteria = [(label, float(row[idx] or 0.0)) for label, idx in labels]
        valid = [x for x in criteria if x[1] > 0]
        lowest = min((v for _, v in valid), default=0.0)
        improve = [name for name, value in valid if abs(value - lowest) < 0.001][:2]
        ranking.append(SimpleNamespace(id=row[0], nome=row[1], qtd=int(row[2] or 0), geral=float(row[3] or 0.0), pont=float(row[4] or 0.0), educ=float(row[5] or 0.0), efic=float(row[6] or 0.0), apres=float(row[7] or 0.0), melhorar=", ".join(improve) or "Sem dados suficientes"))

    detail = []
    selected = Cooperado.query.get(coop_id) if coop_id else None
    if coop_id:
        detail = AvaliacaoCooperado.query.filter_by(cooperado_id=coop_id).order_by(AvaliacaoCooperado.criado_em.desc(), AvaliacaoCooperado.id.desc()).limit(120).all()
    return _render("avaliacoes", "Avaliações vitalícias", "A nota não reinicia por semana. Toda avaliação recebida entra na média da vida do cooperado.", ranking=ranking, selected_coop=selected, detail=detail)


@app.get("/admin/leve/tabelas", endpoint="admin_light_tables")
def admin_light_tables():
    denied = _guard("tabelas")
    if denied:
        return denied
    rows = Tabela.query.order_by(Tabela.enviado_em.desc(), Tabela.id.desc()).limit(160).all()
    return _render("tabelas", "Tabelas", "Tela leve no mesmo padrão administrativo.", tabelas=rows)


@app.route("/admin/leve/avisos", methods=["GET", "POST"], endpoint="admin_light_notices")
def admin_light_notices():
    denied = _guard("avisos")
    if denied:
        return denied
    if request.method == "POST":
        title = (request.form.get("titulo") or "").strip()
        body = (request.form.get("corpo") or "").strip()
        if not title or not body:
            flash("Informe título e mensagem.", "warning")
            return redirect(url_for("admin_light_notices"))
        item = Aviso(
            titulo=title[:140],
            corpo=body,
            tipo=(request.form.get("tipo") or "global")[:20],
            prioridade=(request.form.get("prioridade") or "normal")[:10],
            fixado=bool(request.form.get("fixado")),
            ativo=True,
            criado_por_id=session.get("user_id"),
        )
        db.session.add(item)
        db.session.commit()
        flash("Aviso publicado.", "success")
        return redirect(url_for("admin_light_notices"))
    rows = Aviso.query.order_by(Aviso.criado_em.desc(), Aviso.id.desc()).limit(120).all()
    return _render("avisos", "Avisos", "Publicação e consulta sem carregar o painel inteiro.", avisos=rows)


@app.post("/admin/leve/avisos/<int:item_id>/alternar", endpoint="admin_light_notice_toggle")
def admin_light_notice_toggle(item_id: int):
    denied = _guard("avisos")
    if denied:
        return denied
    item = Aviso.query.get_or_404(item_id)
    item.ativo = not bool(item.ativo)
    db.session.commit()
    flash("Status do aviso atualizado.", "success")
    return redirect(url_for("admin_light_notices"))


@app.context_processor
def _coopex_lifetime_rating_context_v8():
    if (session.get("user_tipo") or "").strip().lower() != "cooperado" or request.endpoint != "portal_cooperado":
        return {}
    coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first()
    if not coop:
        return {}
    row = db.session.query(
        func.count(AvaliacaoCooperado.id),
        func.avg(AvaliacaoCooperado.estrelas_geral),
        func.avg(AvaliacaoCooperado.estrelas_pontualidade),
        func.avg(AvaliacaoCooperado.estrelas_educacao),
        func.avg(AvaliacaoCooperado.estrelas_eficiencia),
        func.avg(AvaliacaoCooperado.estrelas_apresentacao),
    ).filter(AvaliacaoCooperado.cooperado_id == coop.id).one()
    return {"coopex_lifetime_rating_v8": SimpleNamespace(qtd=int(row[0] or 0), geral=float(row[1] or 5.0), pont=float(row[2] or 0.0), educ=float(row[3] or 0.0), efic=float(row[4] or 0.0), apres=float(row[5] or 0.0))}


def _install_template_repairs_v8():
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_admin_light_v8", False):
        return
    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "painel_cooperado.html":
            source = source.replace("{% set _score_title = 'Sua pontuação geral' %}", "{% set _score_title = 'Sua pontuação vitalícia' %}")
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_admin_light_v8 = True
    app.jinja_env.cache.clear()


_install_template_repairs_v8()
app.logger.info("Admin leve V8 carregado: páginas separadas, avaliações vitalícias e mídia persistente.")
