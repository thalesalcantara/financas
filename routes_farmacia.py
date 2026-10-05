"""Rotas e regras do módulo Farmácia.

Extraído do app.py sem alterar endpoints ou regras de negócio.
O módulo usa o namespace já inicializado do app principal para manter
compatibilidade enquanto a aplicação é modularizada.
"""
from __future__ import annotations

import app as legacy

# Compatibilidade controlada com o núcleo legado: inclui modelos, helpers e
# dependências já inicializados antes do registro destas rotas.
for _name, _value in vars(legacy).items():
    if not _name.startswith("__"):
        globals().setdefault(_name, _value)

# =========================
# FARMÁCIA
# =========================
def _farmacia_rest_or_403() -> Restaurante:
    if session.get("user_tipo") != "restaurante":
        abort(403)
    rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    if not getattr(rest, "eh_farmacia", False):
        abort(403)
    return rest


def _farmacia_status_label(status: str | None) -> str:
    mp = {
        "preparacao": "Em preparação",
        "aguardando": "Aguardando montagem de lote",
        "aguardando_motoboy": "Aguardando motoboy",
        "em_rota": "Em rota",
        "indo_ate_voce": "Indo até você",
        "entregue": "Entregue",
        "nao_entregue": "Não entregue",
    }
    return mp.get((status or "").lower(), status or "-")


def _farmacia_status_badge(status: str | None) -> str:
    s = (status or "").lower()
    if s == "entregue":
        return "success"
    if s == "nao_entregue":
        return "danger"
    if s in {"em_rota", "indo_ate_voce"}:
        return "primary"
    if s in {"aguardando", "aguardando_motoboy"}:
        return "secondary"
    return "warning"


def _save_farmacia_foto(entrega: FarmaciaEntrega, file_storage) -> None:
    if not file_storage or not getattr(file_storage, "filename", None):
        return
    entrega.foto_bytes = file_storage.read()
    entrega.foto_mime = getattr(file_storage, "mimetype", None) or "application/octet-stream"
    entrega.foto_filename = secure_filename(file_storage.filename or f"entrega_{entrega.id}.jpg")


def _farmacia_media_display_url(entrega: FarmaciaEntrega) -> str | None:
    if entrega and entrega.id and entrega.foto_bytes:
        try:
            return url_for("farmacia_download_foto", entrega_id=entrega.id)
        except Exception:
            return None
    return getattr(entrega, "foto_comprovante_url", None)


def _sync_farmacia_lancamento(entrega: FarmaciaEntrega) -> None:
    valor = round(float(entrega.valor_entrega or 0.0), 2)
    nome_cliente = getattr(entrega, "cliente_nome", None) or getattr(entrega, "nome_cliente", None) or "Cliente"
    extra_text = f"Farmácia - {nome_cliente}"

    def _apply_extra_fields(lanc):
        lanc.descricao = extra_text
        lanc.qtd_entregas = 1
        if hasattr(lanc, "observacao"):
            base_obs = getattr(entrega, "observacao", None) or ""
            obs_final = f"{extra_text}" if not base_obs else f"{extra_text} | {base_obs}"
            setattr(lanc, "observacao", obs_final)

    if valor > 0 and entrega.cooperado_id and entrega.status == "entregue":
        if entrega.lancamento_id:
            lanc = Lancamento.query.get(entrega.lancamento_id)
            if lanc:
                lanc.cooperado_id = entrega.cooperado_id
                lanc.valor = valor
                lanc.data = (entrega.entregue_em.date() if entrega.entregue_em else date.today())
                lanc.hora_inicio = ((entrega.saiu_em or entrega.saida_em).strftime("%H:%M") if (entrega.saiu_em or entrega.saida_em) else None)
                lanc.hora_fim = (entrega.entregue_em.strftime("%H:%M") if entrega.entregue_em else None)
                _apply_extra_fields(lanc)
                return
        lanc = Lancamento(
            restaurante_id=entrega.restaurante_id,
            cooperado_id=entrega.cooperado_id,
            descricao=extra_text,
            valor=valor,
            data=(entrega.entregue_em.date() if entrega.entregue_em else date.today()),
            hora_inicio=((entrega.saiu_em or entrega.saida_em).strftime("%H:%M") if (entrega.saiu_em or entrega.saida_em) else None),
            hora_fim=(entrega.entregue_em.strftime("%H:%M") if entrega.entregue_em else None),
            qtd_entregas=1,
        )
        db.session.add(lanc)
        db.session.flush()
        _apply_extra_fields(lanc)
        entrega.lancamento_id = lanc.id
    else:
        if entrega.lancamento_id:
            lanc = Lancamento.query.get(entrega.lancamento_id)
            if lanc:
                db.session.delete(lanc)
            entrega.lancamento_id = None


def _farmacia_items_from_form(prefix='item_'):
    nomes = request.form.getlist(f"{prefix}nome[]") or request.form.getlist(f"{prefix}nome")
    qtds = request.form.getlist(f"{prefix}qtd[]") or request.form.getlist(f"{prefix}qtd")
    vals = request.form.getlist(f"{prefix}valor[]") or request.form.getlist(f"{prefix}valor")
    itens = []
    maxlen = max(len(nomes), len(qtds), len(vals), 0)
    for i in range(maxlen):
        nome = (nomes[i] if i < len(nomes) else "").strip()
        qtd = (qtds[i] if i < len(qtds) else "").strip() or "1"
        raw_val = (vals[i] if i < len(vals) else "").strip().replace('.', '').replace(',', '.') if False else (vals[i] if i < len(vals) else '').strip().replace(',', '.')
        try:
            val = round(float(raw_val), 2) if raw_val else 0.0
        except Exception:
            val = 0.0
        if nome:
            itens.append({"descricao": nome, "quantidade": qtd, "valor_item": val})
    return itens


def _farmacia_replace_itens(entrega: FarmaciaEntrega, itens: list[dict]):
    FarmaciaEntregaItem.query.filter_by(entrega_id=entrega.id).delete(synchronize_session=False)
    for item in itens:
        db.session.add(FarmaciaEntregaItem(
            entrega_id=entrega.id,
            descricao=item["descricao"],
            quantidade=item.get("quantidade") or "1",
            valor_item=round(float(item.get("valor_item") or 0.0), 2),
        ))


def _farmacia_total_pedido_por_itens(itens: list[dict], fallback: float = 0.0) -> float:
    total = round(sum(float(i.get("valor_item") or 0.0) for i in itens), 2)
    return total if total > 0 else round(float(fallback or 0.0), 2)


def _farmacia_build_item_payload(e: FarmaciaEntrega, coop_map: dict[int, str]):
    status = (e.status or "preparacao").strip()
    itens = []
    try:
        itens = [SimpleNamespace(descricao=i.descricao, quantidade=i.quantidade, valor_item=float(i.valor_item or 0.0)) for i in (e.itens or [])]
    except Exception:
        itens = []
    cliente_nome = getattr(e, "cliente_nome", None) or getattr(e, "nome_cliente", None) or ""
    return SimpleNamespace(
        id=e.id,
        data_entrega=(e.criado_em.date() if e.criado_em else None),
        lote=getattr(e, "lote", None),
        ordem_rota=int(getattr(e, "ordem_rota", 0) or 0),
        cliente_nome=cliente_nome,
        telefone=(getattr(e, "cliente_telefone", None) or getattr(e, "telefone", None) or ""),
        endereco=(getattr(e, "cliente_endereco", None) or getattr(e, "endereco", None) or ""),
        cpf=(getattr(e, "cliente_cpf", None) or getattr(e, "cpf", None) or ""),
        observacao=e.observacao or "",
        valor_pedido=round(float(getattr(e, 'valor_pedido', 0.0) or 0.0), 2),
        valor_entrega=round(float(e.valor_entrega or 0.0), 2),
        pago=bool(e.pago),
        forma_pagamento=e.forma_pagamento or "",
        parcelas=(getattr(e, "parcelas", None) or getattr(e, "parcelas_credito", None)),
        status=status,
        status_label=_farmacia_status_label(status),
        tracking_url=(url_for("farmacia_rastreio", codigo=e.codigo_rastreio, _external=True) if e.codigo_rastreio else ""),
        cooperado_id=e.cooperado_id,
        cooperado_nome=coop_map.get(e.cooperado_id, "-"),
        entregue_a=e.entregue_a,
        motivo_nao_entrega=(getattr(e, "nao_entregue_motivo", None) or ""),
        foto_url=_farmacia_media_display_url(e),
        itens=itens,
        itens_resumo="; ".join([f"{i.descricao} ({i.quantidade})" for i in itens]) or "-",
    )


@app.get("/farmacia")
@role_required("restaurante")
def farmacia_dashboard():
    rest = _farmacia_rest_or_403()
    view = (request.args.get("view") or "clientes").strip().lower()
    if view not in {"clientes", "pedidos", "lotes", "historico"}:
        view = "clientes"

    q = (request.args.get("q") or "").strip()
    data_inicio = _parse_date(request.args.get("data_inicio"))
    data_fim = _parse_date(request.args.get("data_fim"))
    filtro_status = (request.args.get("status") or "").strip().lower()
    filtro_cooperado_id = request.args.get("cooperado_id", type=int)

    clientes_q = FarmaciaCliente.query.filter_by(restaurante_id=rest.id)
    if q:
        like = f"%{q}%"
        clientes_q = clientes_q.filter(or_(
            FarmaciaCliente.nome.ilike(like),
            FarmaciaCliente.telefone.ilike(like),
            FarmaciaCliente.endereco.ilike(like),
            FarmaciaCliente.cpf.ilike(like),
        ))
    clientes = clientes_q.order_by(FarmaciaCliente.nome.asc()).all()
    cooperados = Cooperado.query.order_by(Cooperado.nome.asc()).all()
    coop_map = {c.id: c.nome for c in cooperados}

    pedidos_q = FarmaciaEntrega.query.filter_by(restaurante_id=rest.id)
    if data_inicio:
        pedidos_q = pedidos_q.filter(func.date(FarmaciaEntrega.criado_em) >= data_inicio)
    if data_fim:
        pedidos_q = pedidos_q.filter(func.date(FarmaciaEntrega.criado_em) <= data_fim)
    if filtro_status:
        if filtro_status == "pendentes":
            pedidos_q = pedidos_q.filter(FarmaciaEntrega.status.notin_(["entregue", "nao_entregue"]))
        else:
            pedidos_q = pedidos_q.filter(FarmaciaEntrega.status == filtro_status)
    if filtro_cooperado_id:
        pedidos_q = pedidos_q.filter(FarmaciaEntrega.cooperado_id == filtro_cooperado_id)

    entregas = pedidos_q.order_by(FarmaciaEntrega.criado_em.desc(), FarmaciaEntrega.ordem_rota.asc(), FarmaciaEntrega.id.desc()).all()
    pedidos = [_farmacia_build_item_payload(e, coop_map) for e in entregas]

    total_gasto_periodo = round(sum(float(p.valor_entrega or 0.0) + (0 if p.pago else float(p.valor_pedido or 0.0)) for p in pedidos), 2)
    total_producao_periodo = round(sum(float(p.valor_entrega or 0.0) for p in pedidos if float(p.valor_entrega or 0.0) > 0), 2)
    total_pendentes = sum(1 for p in pedidos if p.status not in {"entregue", "nao_entregue"})

    pedidos_sem_lote = [p for p in pedidos if not p.cooperado_id]
    pedidos_em_lote = [p for p in pedidos if p.cooperado_id]

    return render_template(
        "farmacia_dashboard.html",
        rest=rest,
        restaurante=rest,
        view=view,
        q=q,
        clientes=clientes or [],
        pedidos=pedidos or [],
        pedidos_sem_lote=pedidos_sem_lote,
        pedidos_em_lote=pedidos_em_lote,
        cooperados=cooperados or [],
        total_gasto_periodo=round(total_gasto_periodo, 2),
        total_producao_periodo=round(total_producao_periodo, 2),
        total_pendentes=int(total_pendentes),
        data_inicio=data_inicio,
        data_fim=data_fim,
        filtro_status=filtro_status or "",
        filtro_cooperado_id=filtro_cooperado_id,
        farmacia_status_label=_farmacia_status_label,
        farmacia_status_badge=_farmacia_status_badge,
        now=datetime.utcnow(),
        current_year=datetime.utcnow().year,
    )


@app.post("/farmacia/clientes/add")
@role_required("restaurante")
def farmacia_add_cliente():
    rest = _farmacia_rest_or_403()
    f = request.form
    nome = (f.get("nome") or "").strip()
    endereco = (f.get("endereco") or "").strip()
    if not nome or not endereco:
        flash("Informe nome e endereço do cliente.", "warning")
        return redirect(url_for("farmacia_dashboard", view="clientes"))

    c = FarmaciaCliente(
        restaurante_id=rest.id,
        nome=nome,
        telefone=(f.get("telefone") or "").strip() or None,
        endereco=endereco,
        cpf=(f.get("cpf") or "").strip() or None,
        criado_em=datetime.utcnow(),
        atualizado_em=datetime.utcnow(),
    )
    db.session.add(c)
    db.session.commit()
    flash("Cliente cadastrado.", "success")
    return redirect(url_for("farmacia_dashboard", view="clientes"))


@app.post("/farmacia/clientes/<int:cliente_id>/edit")
@role_required("restaurante")
def farmacia_edit_cliente(cliente_id):
    rest = _farmacia_rest_or_403()
    cliente = FarmaciaCliente.query.filter_by(id=cliente_id, restaurante_id=rest.id).first_or_404()
    cliente.nome = (request.form.get("nome") or cliente.nome or "").strip()
    cliente.telefone = (request.form.get("telefone") or "").strip() or None
    cliente.endereco = (request.form.get("endereco") or cliente.endereco or "").strip()
    cliente.cpf = (request.form.get("cpf") or "").strip() or None
    cliente.atualizado_em = datetime.utcnow()
    db.session.commit()
    flash("Cliente atualizado.", "success")
    return redirect(url_for("farmacia_dashboard", view="clientes"))


@app.post("/farmacia/clientes/<int:cliente_id>/delete")
@role_required("restaurante")
def farmacia_delete_cliente(cliente_id):
    rest = _farmacia_rest_or_403()
    cliente = FarmaciaCliente.query.filter_by(id=cliente_id, restaurante_id=rest.id).first()
    if not cliente:
        flash("Cliente não encontrado.", "warning")
        return redirect(url_for("farmacia_dashboard", view="clientes"))

    try:
        FarmaciaEntrega.query.filter_by(restaurante_id=rest.id, cliente_id=cliente.id).update({FarmaciaEntrega.cliente_id: None}, synchronize_session=False)
        db.session.delete(cliente)
        db.session.commit()
        flash("Cliente excluído com sucesso.", "success")
    except Exception:
        db.session.rollback()
        flash("Não foi possível excluir o cliente.", "danger")

    return redirect(url_for("farmacia_dashboard", view="clientes"))


@app.post("/farmacia/pedidos/add")
@role_required("restaurante")
def farmacia_add_pedido():
    rest = _farmacia_rest_or_403()
    f = request.form
    cliente_id = f.get("cliente_id", type=int)
    cliente = FarmaciaCliente.query.filter_by(id=cliente_id, restaurante_id=rest.id).first() if cliente_id else None

    nome = (f.get("cliente_nome") or (cliente.nome if cliente else "") or "").strip()
    endereco = (f.get("endereco") or (cliente.endereco if cliente else "") or "").strip()
    telefone = (f.get("telefone") or (cliente.telefone if cliente else "") or "").strip()
    cpf = (f.get("cpf") or (cliente.cpf if cliente else "") or "").strip()

    if not nome or not endereco:
        flash("Informe nome e endereço do pedido.", "warning")
        return redirect(url_for("farmacia_dashboard", view="pedidos"))

    pago = (f.get("pago") or "").strip().lower() in {"1", "true", "on", "sim", "pago"}
    forma = (f.get("forma_pagamento") or "").strip() or None
    parcelas = f.get("parcelas", type=int)

    def _to_money(v):
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return round(float(v), 2)
        s = str(v).strip()
        if not s:
            return 0.0
        s = s.replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        return round(float(s), 2)

    valor_entrega = _to_money(f.get("valor_entrega"))
    valor_pedido = _to_money(f.get("valor_pedido"))
    itens = _farmacia_items_from_form()
    valor_pedido = _farmacia_total_pedido_por_itens(itens, valor_pedido)

    ent = FarmaciaEntrega(
        restaurante_id=rest.id,
        cliente_id=(cliente.id if cliente else None),
        cooperado_id=None,
        lote=None,
        ordem_rota=0,
        codigo_rastreio=uuid.uuid4().hex[:12],
        # compatibilidade
        nome_cliente=nome,
        telefone=telefone or None,
        endereco=endereco,
        cpf=cpf or None,
        # novos
        cliente_nome=nome,
        cliente_endereco=endereco,
        cliente_telefone=telefone or None,
        cliente_cpf=cpf or None,
        valor_pedido=valor_pedido,
        valor_entrega=valor_entrega,
        observacao=(f.get("observacao") or "").strip() or None,
        pago=pago,
        forma_pagamento=(None if pago else forma),
        parcelas=(parcelas if (forma or "").lower() == "credito" else None),
        parcelas_credito=(parcelas if (forma or "").lower() == "credito" else None),
        status="aguardando",
        criado_em=datetime.utcnow(),
        atualizado_em=datetime.utcnow(),
    )
    db.session.add(ent)
    db.session.flush()
    _farmacia_replace_itens(ent, itens)
    db.session.commit()
    flash("Pedido lançado na farmácia.", "success")
    return redirect(url_for("farmacia_dashboard", view="pedidos"))


@app.post("/farmacia/pedidos/<int:pedido_id>/edit")
@role_required("restaurante")
def farmacia_edit_pedido(pedido_id):
    rest = _farmacia_rest_or_403()
    ent = FarmaciaEntrega.query.filter_by(id=pedido_id, restaurante_id=rest.id).first_or_404()
    f = request.form
    ent.nome_cliente = ent.cliente_nome = (f.get("cliente_nome") or ent.cliente_nome or ent.nome_cliente or "").strip()
    ent.endereco = ent.cliente_endereco = (f.get("endereco") or ent.cliente_endereco or ent.endereco or "").strip()
    ent.telefone = ent.cliente_telefone = (f.get("telefone") or ent.cliente_telefone or ent.telefone or "").strip() or None
    ent.cpf = ent.cliente_cpf = (f.get("cpf") or ent.cliente_cpf or ent.cpf or "").strip() or None
    def _to_money(v, default=0.0):
        if v is None:
            return round(float(default or 0.0), 2)
        if isinstance(v, (int, float)):
            return round(float(v), 2)
        s = str(v).strip()
        if not s:
            return round(float(default or 0.0), 2)
        s = s.replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        return round(float(s), 2)

    ent.valor_pedido = _to_money(request.form.get('valor_pedido'), ent.valor_pedido or 0)
    ent.valor_entrega = _to_money(request.form.get('valor_entrega'), ent.valor_entrega or 0)
    ent.observacao = (f.get("observacao") or "").strip() or None
    ent.pago = (f.get("pago") or "").strip().lower() in {"1","true","on","sim","pago"}
    ent.forma_pagamento = None if ent.pago else ((f.get("forma_pagamento") or "").strip() or None)
    parcelas = f.get("parcelas", type=int)
    ent.parcelas = parcelas if (ent.forma_pagamento or '').lower() == 'credito' else None
    ent.parcelas_credito = ent.parcelas
    itens = _farmacia_items_from_form()
    ent.valor_pedido = _farmacia_total_pedido_por_itens(itens, ent.valor_pedido)
    _farmacia_replace_itens(ent, itens)
    ent.atualizado_em = datetime.utcnow()
    db.session.commit()
    flash("Pedido atualizado.", "success")
    return redirect(url_for("farmacia_dashboard", view="pedidos"))


@app.post("/farmacia/pedidos/<int:pedido_id>/delete")
@role_required("restaurante")
def farmacia_delete_pedido(pedido_id):
    rest = _farmacia_rest_or_403()
    ent = FarmaciaEntrega.query.filter_by(id=pedido_id, restaurante_id=rest.id).first_or_404()
    try:
        if ent.lancamento_id:
            lanc = Lancamento.query.get(ent.lancamento_id)
            if lanc:
                db.session.delete(lanc)
        db.session.delete(ent)
        db.session.commit()
        flash("Entrega excluída.", "success")
    except Exception:
        db.session.rollback()
        flash("Não foi possível excluir a entrega.", "danger")
    return redirect(url_for("farmacia_dashboard", view="pedidos"))


@app.post("/farmacia/lotes/salvar")
@role_required("restaurante")
def farmacia_salvar_lote():
    rest = _farmacia_rest_or_403()
    cooperado_id = request.form.get("cooperado_id", type=int)
    entrega_ids = [int(x) for x in request.form.getlist("entrega_ids") if str(x).isdigit()]
    if not cooperado_id or not entrega_ids:
        flash("Selecione o motoboy e pelo menos um pedido.", "warning")
        return redirect(url_for("farmacia_dashboard", view="lotes"))

    cooperado = Cooperado.query.get_or_404(cooperado_id)
    lote_nome = (request.form.get("lote") or "").strip() or f"LOTE-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    entregas = FarmaciaEntrega.query.filter(FarmaciaEntrega.restaurante_id == rest.id, FarmaciaEntrega.id.in_(entrega_ids)).order_by(FarmaciaEntrega.id.asc()).all()
    for idx, ent in enumerate(entregas, start=1):
        ent.cooperado_id = cooperado.id
        ent.lote = lote_nome
        ent.ordem_rota = idx
        if ent.status in {"preparacao", "aguardando"}:
            ent.status = "aguardando_motoboy"
        ent.atualizado_em = datetime.utcnow()
    db.session.commit()
    flash(f"Lote {lote_nome} enviado para {cooperado.nome}.", "success")
    return redirect(url_for("farmacia_dashboard", view="historico"))


@app.post("/farmacia/pedidos/<int:entrega_id>/retirar-lote")
@role_required("restaurante")
def farmacia_retirar_lote(entrega_id):
    rest = _farmacia_rest_or_403()
    ent = FarmaciaEntrega.query.filter_by(id=entrega_id, restaurante_id=rest.id).first_or_404()
    ent.cooperado_id = None
    ent.lote = None
    ent.ordem_rota = 0
    if ent.status in {"aguardando_motoboy", "em_rota", "indo_ate_voce"}:
        ent.status = "aguardando"
    ent.atualizado_em = datetime.utcnow()
    db.session.commit()
    flash("Entrega retirada do lote com sucesso.", "success")
    return redirect(url_for("farmacia_dashboard", view="historico"))


@app.get("/farmacia/pedidos/<int:entrega_id>/foto")
@role_required("restaurante")
def farmacia_download_foto(entrega_id):
    rest = _farmacia_rest_or_403()
    ent = FarmaciaEntrega.query.filter_by(id=entrega_id, restaurante_id=rest.id).first_or_404()
    if not ent.foto_bytes:
        abort(404)
    return send_file(
        io.BytesIO(ent.foto_bytes),
        mimetype=ent.foto_mime or "application/octet-stream",
        as_attachment=True,
        download_name=ent.foto_filename or f"entrega_{ent.id}.jpg",
    )


@app.post("/farmacia/pedidos/<int:entrega_id>/reordenar")
@role_required("restaurante")
def farmacia_reordenar_pedido(entrega_id):
    rest = _farmacia_rest_or_403()
    ent = FarmaciaEntrega.query.filter_by(id=entrega_id, restaurante_id=rest.id).first_or_404()
    acao = (request.form.get("acao") or "").strip().lower()
    view = (request.form.get("view") or "historico").strip().lower()
    data_inicio = (request.form.get("data_inicio") or "").strip()
    data_fim = (request.form.get("data_fim") or "").strip()
    status_filtro = (request.form.get("status_filtro") or "").strip()
    cooperado_id_filtro = (request.form.get("cooperado_id_filtro") or "").strip()

    if not ent.cooperado_id:
        flash("Defina o lote antes de reorganizar.", "warning")
        return redirect(url_for("farmacia_dashboard", view=view, data_inicio=data_inicio, data_fim=data_fim, status=status_filtro, cooperado_id=cooperado_id_filtro))

    if acao in {"subir", "descer"}:
        viz_q = FarmaciaEntrega.query.filter_by(restaurante_id=rest.id, cooperado_id=ent.cooperado_id, lote=ent.lote)
        if acao == "subir":
            viz = viz_q.filter(FarmaciaEntrega.ordem_rota < (ent.ordem_rota or 0)).order_by(FarmaciaEntrega.ordem_rota.desc(), FarmaciaEntrega.id.desc()).first()
        else:
            viz = viz_q.filter(FarmaciaEntrega.ordem_rota > (ent.ordem_rota or 0)).order_by(FarmaciaEntrega.ordem_rota.asc(), FarmaciaEntrega.id.asc()).first()
        if viz:
            ent_ord = ent.ordem_rota or 0
            ent.ordem_rota = viz.ordem_rota or ent_ord
            viz.ordem_rota = ent_ord
    else:
        ent.ordem_rota = max(1, int(request.form.get("ordem_rota") or ent.ordem_rota or 1))

    db.session.commit()
    flash("Ordem da rota atualizada.", "success")
    return redirect(url_for("farmacia_dashboard", view=view, data_inicio=data_inicio, data_fim=data_fim, status=status_filtro, cooperado_id=cooperado_id_filtro))


@app.get("/farmacia/rastreio/<codigo>")
def farmacia_rastreio(codigo):
    ent = FarmaciaEntrega.query.filter_by(codigo_rastreio=codigo).first_or_404()
    ahead = 0
    if ent.cooperado_id and ent.status not in {"entregue", "nao_entregue"}:
        ahead = (FarmaciaEntrega.query
                 .filter(FarmaciaEntrega.cooperado_id == ent.cooperado_id,
                         FarmaciaEntrega.status.in_(["preparacao", "aguardando", "aguardando_motoboy", "em_rota", "indo_ate_voce"]),
                         FarmaciaEntrega.ordem_rota < ent.ordem_rota)
                 .count())

    cooperado_username = "Aguardando definição"
    if ent.cooperado_id:
        coop = Cooperado.query.get(ent.cooperado_id)
        if coop:
            # nome público no rastreio: prioriza usuário/apelido; se não houver, usa o primeiro nome
            for attr in ("usuario", "username", "login", "apelido", "nome_usuario"):
                val = getattr(coop, attr, None)
                if val:
                    cooperado_username = str(val).strip()
                    break
            else:
                nome_full = getattr(coop, "nome", None)
                if nome_full:
                    cooperado_username = str(nome_full).strip().split()[0]

    label = "indo_ate_voce" if ahead == 0 and ent.status == "em_rota" else ent.status
    return render_template(
        "farmacia_rastreio.html",
        pedido=ent,
        entrega=ent,
        ahead=ahead,
        status_publico=label,
        farmacia_status_label=_farmacia_status_label,
        cooperado_username=cooperado_username,
    )


@app.post("/farmacia/pedidos/<int:pedido_id>/status")
@role_required("restaurante")
def farmacia_update_pedido_status(pedido_id):
    rest = _farmacia_rest_or_403()
    p = FarmaciaEntrega.query.filter_by(id=pedido_id, restaurante_id=rest.id).first_or_404()
    status = (request.form.get("status") or "preparacao").strip()
    p.status = status
    if status == "em_rota" and not (p.saiu_em or p.saida_em):
        p.saiu_em = p.saida_em = datetime.utcnow()
    if status == "entregue":
        p.entregue_em = datetime.utcnow()
        p.entregue_a = (request.form.get("entregue_a") or "").strip() or None
        foto = request.files.get("foto_comprovante")
        if foto and foto.filename:
            _save_farmacia_foto(p, foto)
    elif status == "nao_entregue":
        p.nao_entregue_motivo = (request.form.get("motivo_nao_entrega") or "").strip() or None
        p.entregue_a = None
    _sync_farmacia_lancamento(p)
    db.session.commit()
    flash("Pedido atualizado.", "success")
    return redirect(url_for("farmacia_dashboard", view="historico", data_inicio=(request.form.get("data_inicio") or ""), data_fim=(request.form.get("data_fim") or ""), status=(request.form.get("status_filtro") or ""), cooperado_id=(request.form.get("cooperado_id_filtro") or "")))


@app.post("/cooperado/farmacia/ordem")
@role_required("cooperado")
def cooperado_farmacia_ordem():
    coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    ids_raw = (request.form.get("ids") or "").strip()
    ids = [int(x) for x in ids_raw.split(",") if x.strip().isdigit()]
    if not ids:
        flash("Nenhuma entrega informada para reorganizar.", "warning")
        return redirect(url_for("portal_cooperado", active_tab="farmacia"))

    entregas = FarmaciaEntrega.query.filter(FarmaciaEntrega.id.in_(ids), FarmaciaEntrega.cooperado_id == coop.id).all()
    ordem_map = {eid: i+1 for i, eid in enumerate(ids)}
    for ent in entregas:
        ent.ordem_rota = ordem_map.get(ent.id, ent.ordem_rota or 1)
    db.session.commit()
    flash("Rota reorganizada.", "success")
    return redirect(url_for("portal_cooperado", active_tab="farmacia"))


@app.post("/cooperado/farmacia/<int:entrega_id>/saida")
@role_required("cooperado")
def cooperado_farmacia_saida(entrega_id):
    coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    ent = FarmaciaEntrega.query.filter_by(id=entrega_id, cooperado_id=coop.id).first_or_404()
    ent.status = "em_rota"
    ent.saiu_em = ent.saida_em = (ent.saiu_em or ent.saida_em or datetime.utcnow())
    db.session.commit()
    flash("Saída registrada.", "success")
    return redirect(url_for("portal_cooperado", active_tab="farmacia"))


@app.post("/cooperado/farmacia/<int:entrega_id>/entregue")
@role_required("cooperado")
def cooperado_farmacia_entregue(entrega_id):
    coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    ent = FarmaciaEntrega.query.filter_by(id=entrega_id, cooperado_id=coop.id).first_or_404()
    ent.status = "entregue"
    ent.entregue_em = datetime.utcnow()
    ent.entregue_a = (request.form.get("entregue_a") or "").strip() or None
    foto = request.files.get("foto")
    if foto and foto.filename:
        _save_farmacia_foto(ent, foto)
    _sync_farmacia_lancamento(ent)
    db.session.commit()
    flash("Entrega concluída.", "success")
    return redirect(url_for("portal_cooperado", active_tab="farmacia"))


@app.post("/cooperado/farmacia/<int:entrega_id>/nao_entregue")
@role_required("cooperado")
def cooperado_farmacia_nao_entregue(entrega_id):
    coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    ent = FarmaciaEntrega.query.filter_by(id=entrega_id, cooperado_id=coop.id).first_or_404()
    ent.status = "nao_entregue"
    ent.nao_entregue_motivo = (request.form.get("motivo") or "").strip() or None
    _sync_farmacia_lancamento(ent)
    db.session.commit()
    flash("Entrega marcada como não entregue.", "warning")
    return redirect(url_for("portal_cooperado", active_tab="farmacia"))



@app.post("/farmacia/cooperado/reordenar/<int:entrega_id>")
@role_required("cooperado")
def farmacia_cooperado_reordenar(entrega_id):
    coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    ent = FarmaciaEntrega.query.filter_by(id=entrega_id, cooperado_id=coop.id).first_or_404()
    acao = (request.form.get("acao") or "").strip().lower()

    vizinhas = (FarmaciaEntrega.query
                .filter(FarmaciaEntrega.cooperado_id == coop.id,
                        FarmaciaEntrega.status.in_(["aguardando", "aguardando_motoboy", "em_rota", "indo_ate_voce", "entregue", "nao_entregue"]))
                .order_by(FarmaciaEntrega.ordem_rota.asc(), FarmaciaEntrega.id.asc())
                .all())

    ids = [x.id for x in vizinhas]
    if ent.id not in ids:
        return redirect(url_for("portal_cooperado", active_tab="farmacia"))

    idx = ids.index(ent.id)
    if acao == "subir" and idx > 0:
        ids[idx-1], ids[idx] = ids[idx], ids[idx-1]
    elif acao == "descer" and idx < len(ids)-1:
        ids[idx+1], ids[idx] = ids[idx], ids[idx+1]

    ordem_map = {eid: i+1 for i, eid in enumerate(ids)}
    for x in vizinhas:
        x.ordem_rota = ordem_map.get(x.id, x.ordem_rota or 1)

    db.session.commit()
    flash("Sequência das entregas atualizada.", "success")
    return redirect(url_for("portal_cooperado", active_tab="farmacia"))


@app.post("/farmacia/cooperado/status/<int:entrega_id>")
@role_required("cooperado")
def farmacia_cooperado_status(entrega_id):
    coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    ent = FarmaciaEntrega.query.filter_by(id=entrega_id, cooperado_id=coop.id).first_or_404()

    status = (request.form.get("status") or "").strip().lower()
    if status not in {"aguardando_motoboy", "em_rota", "indo_ate_voce", "entregue", "nao_entregue"}:
        flash("Status inválido.", "warning")
        return redirect(url_for("portal_cooperado", active_tab="farmacia"))

    ent.status = status

    if status == "em_rota" and not (ent.saiu_em or ent.saida_em):
        ent.saiu_em = ent.saida_em = datetime.utcnow()

    if status == "indo_ate_voce" and not (ent.saiu_em or ent.saida_em):
        ent.saiu_em = ent.saida_em = datetime.utcnow()

    if status == "entregue":
        ent.entregue_em = datetime.utcnow()
        ent.entregue_a = (request.form.get("entregue_a") or "").strip() or None
        ent.nao_entregue_motivo = None
        foto = request.files.get("foto_comprovante")
        if foto and foto.filename:
            _save_farmacia_foto(ent, foto)

    elif status == "nao_entregue":
        ent.nao_entregue_motivo = (request.form.get("motivo_nao_entrega") or "").strip() or None
        ent.entregue_a = None
        ent.entregue_em = None

    _sync_farmacia_lancamento(ent)
    db.session.commit()
    flash("Entrega da farmácia atualizada.", "success")
    return redirect(url_for("portal_cooperado", active_tab="farmacia"))


import click

@app.cli.command("init-db")
def init_db_command():
    """Roda init_db() manualmente."""
    click.echo("Rodando init_db() ...")
    with app.app_context():
        init_db()
    click.echo("init_db() concluído.")

