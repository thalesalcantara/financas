"""Rotas do Portal do Cooperado.

Extraídas do app.py sem alterar endpoints ou regras de negócio.
"""
from __future__ import annotations

import app as legacy

for _name, _value in vars(legacy).items():
    if not _name.startswith("__"):
        globals().setdefault(_name, _value)

# =========================
# PORTAL COOPERADO
# =========================
@app.route("/portal/cooperado")
@role_required("cooperado")
def portal_cooperado():
    u_id = session.get("user_id")
    coop = request_cooperado()
    if not coop:
        return "<p style='font-family:Arial;margin:40px'>Seu usuário não está vinculado a um cooperado. Avise o administrador.</p>"

    try:
        coop.usuario = coop.usuario_ref.usuario
    except Exception:
        coop.usuario = ""

    active_tab = (request.args.get("active_tab") or "resumo").strip().lower()
    if active_tab not in {"resumo", "producoes", "ajustes", "escalas", "trocas"}:
        active_tab = "resumo"

    # ---------- FILTRO POR DATA (padrão = semana atual seg-dom) ----------
    di = _parse_date(request.args.get("data_inicio"))
    df = _parse_date(request.args.get("data_fim"))

    if di and not df:
        df = di
    if df and not di:
        di = df
    elif not di and not df:
        hoje = date.today()
        di = hoje - timedelta(days=hoje.weekday())
        df = di + timedelta(days=6)

    def in_range(qs, col):
        return qs.filter(col >= di, col <= df)

    # =========================
    # Produções (Lançamentos)
    # =========================
    producoes = (
        in_range(
            Lancamento.query.options(selectinload(Lancamento.restaurante)).filter_by(cooperado_id=coop.id),
            Lancamento.data,
        )
        .order_by(Lancamento.data.desc(), Lancamento.id.desc())
        .limit(120)
        .all()
    )
    ids = [l.id for l in producoes]
    minhas = {}
    if ids:
        rows = (
            db.session.query(
                AvaliacaoRestaurante.lancamento_id,
                AvaliacaoRestaurante.estrelas_geral,
            )
            .filter(
                AvaliacaoRestaurante.lancamento_id.in_(ids),
                AvaliacaoRestaurante.cooperado_id == coop.id,
            )
            .all()
        )
        minhas = {lid: nota for lid, nota in rows}
    for l in producoes:
        l.minha_avaliacao = minhas.get(l.id)

    q_prod_total = db.session.query(func.coalesce(func.sum(Lancamento.valor), 0.0)).filter(
        Lancamento.cooperado_id == coop.id,
        Lancamento.data >= di,
        Lancamento.data <= df,
    )
    producao_total_periodo = float(q_prod_total.scalar() or 0.0)

    # =========================
    # Receitas / Despesas
    # =========================
    receitas_coop = (
        in_range(
            ReceitaCooperado.query.filter_by(cooperado_id=coop.id),
            ReceitaCooperado.data,
        )
        .order_by(ReceitaCooperado.data.desc(), ReceitaCooperado.id.desc())
        .limit(120)
        .all()
    )

    qd = DespesaCooperado.query.filter_by(cooperado_id=coop.id)
    if di and df:
        qd = qd.filter(
            DespesaCooperado.data_inicio <= df,
            DespesaCooperado.data_fim >= di,
        )
    elif di:
        qd = qd.filter(DespesaCooperado.data_fim >= di)
    elif df:
        qd = qd.filter(DespesaCooperado.data_inicio <= df)
    despesas_coop = qd.order_by(
        DespesaCooperado.data_fim.desc().nullslast(),
        DespesaCooperado.id.desc(),
    ).limit(120).all()

    solicitacoes_adiantamento = (
        SolicitacaoAdiantamento.query
        .filter_by(cooperado_id=coop.id)
        .order_by(SolicitacaoAdiantamento.pedido_em.desc(), SolicitacaoAdiantamento.id.desc())
        .limit(60)
        .all()
    )

    receita_total_periodo = float(
        db.session.query(func.coalesce(func.sum(ReceitaCooperado.valor), 0.0))
        .filter(
            ReceitaCooperado.cooperado_id == coop.id,
            ReceitaCooperado.data >= di,
            ReceitaCooperado.data <= df,
        )
        .scalar() or 0.0
    )

    # =========================
    # Totais
    # =========================
    total_bruto = producao_total_periodo + receita_total_periodo
    inss_valor = producao_total_periodo * 0.04
    sest_valor = producao_total_periodo * 0.005
    encargos_valor = inss_valor + sest_valor

    debt_snapshot = _compute_coop_debt_snapshot(coop.id, di, df)
    total_descontos = (
        float(debt_snapshot.get('descontado_periodo_despesa', 0.0) or 0.0)
        + float(debt_snapshot.get('descontado_periodo_adiant', 0.0) or 0.0)
    )
    total_liquido = max(0.0, total_bruto - encargos_valor - total_descontos)
    saldo_devedor = float(debt_snapshot.get('saldo_devedor', 0.0) or 0.0)
    total_a_descontar = float(debt_snapshot.get('a_descontar', 0.0) or 0.0)
    despesas_detalhadas = debt_snapshot.get('itens', [])
    adiantamento_disponivel = _adiantamento_disponivel_cooperado(coop.id, di, df)

    # =========================
    # Métricas da vida
    # =========================
    total_entregas_vida = (
        db.session.query(func.count(Lancamento.id))
        .filter(Lancamento.cooperado_id == coop.id)
        .scalar()
        or 0
    )
    nota_vida = (
        db.session.query(func.avg(AvaliacaoCooperado.estrelas_geral))
        .filter(AvaliacaoCooperado.cooperado_id == coop.id)
        .scalar()
    )
    nota_vida = float(nota_vida or 5.0)

    # =========================
    # Config / Complemento
    # =========================
    cfg = get_config()
    salario_minimo = cfg.salario_minimo or 0.0
    inss_complemento = salario_minimo * 0.20

    today = date.today()

    def dias_para_3112():
        alvo = date(today.year, 12, 31)
        if today > alvo:
            alvo = date(today.year + 1, 12, 31)
        return (alvo - today).days

    doc_cnh = {
        "numero": coop.cnh_numero,
        "vencimento": coop.cnh_validade,
        "ok": (coop.cnh_validade is not None and coop.cnh_validade >= today),
        "dias_para_prazo": dias_para_3112(),
    }
    doc_placa = {
        "numero": coop.placa,
        "vencimento": coop.placa_validade,
        "ok": (coop.placa_validade is not None and coop.placa_validade >= today),
        "dias_para_prazo": dias_para_3112(),
    }

    # ---------- ESCALA (dedupe + ordenação cronológica robusta) ----------
    raw_escala = (
        Escala.query.filter(
            or_(
                Escala.cooperado_id == coop.id,
                func.lower(func.trim(Escala.cooperado_nome)) == (coop.nome or "").strip().lower(),
            )
        )
        .order_by(Escala.id.desc())
        .limit(120)
        .all()
    )

    import unicodedata as _u, re as _re

    def _norm_c(s: str) -> str:
        s = _u.normalize("NFD", str(s or "").lower())
        s = "".join(ch for ch in s if _u.category(ch) != "Mn")
        return _re.sub(r"[^a-z0-9]+", " ", s).strip()

    def _score(e):
        h = (e.horario or "").strip()
        return (1 if h else 0, len(h), e.id)

    def _to_date_from_str(s: str):
        m = _re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', str(s or ''))
        if not m:
            return None
        d_, mth, y = map(int, m.groups())
        if y < 100:
            y += 2000
        try:
            return date(y, mth, d_)
        except Exception:
            return None

    def _mins(h):
        m = _re.search(r'(\d{1,2}):(\d{2})', str(h or ''))
        if not m:
            return 24 * 60 + 59
        hh, mm = map(int, m.groups())
        return hh * 60 + mm

    def _bucket_idx(turno, horario):
        b = (_turno_bucket(turno, horario) or "").lower()
        if "dia" in b:
            return 1
        if "noite" in b:
            return 2
        mins = _mins(horario)
        return 2 if (mins >= 17 * 60 or mins <= 6 * 60) else 1

    best = {}
    for e in raw_escala:
        key = (_norm_c(e.data), _norm_c(e.turno), _norm_c(e.contrato))
        cur = best.get(key)
        if not cur or _score(e) > _score(cur):
            best[key] = e

    cand = list(best.values())
    for e in cand:
        d = _to_date_from_str(e.data) or date.min
        mins = _mins(e.horario or "")
        bidx = _bucket_idx(e.turno, e.horario)
        e._ord = (d.toordinal(), bidx, mins, (e.contrato or ""), e.id)

    minha_escala = sorted(cand, key=lambda x: x._ord)

    for e in minha_escala:
        dt = _to_date_from_str(e.data)
        if dt is None:
            status = 'unknown'
        elif dt < today:
            status = 'past'
        elif dt == today:
            status = 'today'
        elif dt == today + timedelta(days=1):
            status = 'tomorrow'
        else:
            status = 'future'
        e.status = status
        e.status_color = (
            '#ef4444' if status == 'past' else
            '#22c55e' if status == 'today' else
            '#3b82f6' if status in ('tomorrow', 'future') else
            'transparent'
        )

    minha_escala_json = [
        {
            "id": e.id,
            "data": e.data or "",
            "turno": e.turno or "",
            "horario": e.horario or "",
            "contrato": e.contrato or "",
            "weekday": _weekday_from_data_str(e.data),
            "turno_bucket": _turno_bucket(e.turno, e.horario),
        }
        for e in minha_escala
    ]

    # ---------- Trocas / Cooperados / Escalas (sem N+1) ----------
    coops = (
        Cooperado.query
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Cooperado.id != coop.id, Usuario.ativo.is_(True))
        .order_by(Cooperado.nome.asc())
        .limit(180)
        .all()
    )
    cooperados_json = [
        {"id": c.id, "nome": c.nome, "foto_url": (c.foto_url or "")}
        for c in coops
    ]

    coop_ids = [c.id for c in coops]
    escalas_outros = []
    if coop_ids:
        escalas_outros = (
            Escala.query
            .filter(Escala.cooperado_id.in_(coop_ids))
            .order_by(Escala.cooperado_id.asc(), Escala.data.asc(), Escala.id.asc())
            .all()
        )

    cooperados_escalas_map = {str(c.id): [] for c in coops}
    for e in escalas_outros:
        cooperados_escalas_map.setdefault(str(e.cooperado_id), []).append({
            "id": e.id,
            "data": e.data or "",
            "turno": e.turno or "",
            "horario": e.horario or "",
            "contrato": e.contrato or "",
            "weekday": _weekday_from_data_str(e.data),
            "turno_bucket": _turno_bucket(e.turno, e.horario),
        })

    def _escala_desc(e: Escala | None) -> str:
        return _escala_label(e)

    rx = (
        TrocaSolicitacao.query
        .filter(TrocaSolicitacao.destino_id == coop.id)
        .order_by(TrocaSolicitacao.id.desc())
        .limit(60)
        .all()
    )
    ex = (
        TrocaSolicitacao.query
        .filter(TrocaSolicitacao.solicitante_id == coop.id)
        .order_by(TrocaSolicitacao.id.desc())
        .limit(60)
        .all()
    )

    troca_coop_ids = set()
    troca_escala_ids = set()
    for t in rx:
        if t.solicitante_id:
            troca_coop_ids.add(t.solicitante_id)
        if t.origem_escala_id:
            troca_escala_ids.add(t.origem_escala_id)
    for t in ex:
        if t.destino_id:
            troca_coop_ids.add(t.destino_id)
        if t.origem_escala_id:
            troca_escala_ids.add(t.origem_escala_id)

    troca_coops_map = {}
    if troca_coop_ids:
        troca_coops_map = {
            c.id: c
            for c in Cooperado.query.filter(Cooperado.id.in_(list(troca_coop_ids))).all()
        }

    troca_escalas_map = {}
    if troca_escala_ids:
        troca_escalas_map = {
            e.id: e
            for e in Escala.query.filter(Escala.id.in_(list(troca_escala_ids))).all()
        }

    trocas_recebidas_pendentes = []
    trocas_recebidas_historico = []
    for t in rx:
        solicitante = troca_coops_map.get(t.solicitante_id)
        orig = troca_escalas_map.get(t.origem_escala_id)
        mensagem_limpa = _strip_afetacao_blob(t.mensagem)
        linhas_afetadas = _parse_linhas_from_msg(t.mensagem) if t.status == "aprovada" else []
        item = {
            "id": t.id,
            "status": t.status,
            "mensagem": mensagem_limpa,
            "criada_em": t.criada_em,
            "aplicada_em": t.aplicada_em,
            "solicitante": solicitante,
            "origem": orig,
            "origem_desc": _escala_desc(orig),
            "linhas_afetadas": linhas_afetadas,
            "origem_weekday": _weekday_from_data_str(orig.data) if orig else None,
            "origem_turno": (orig.turno if orig else None),
            "origem_turno_bucket": _turno_bucket(orig.turno if orig else None, orig.horario if orig else None),
        }
        (trocas_recebidas_pendentes if t.status == "pendente" else trocas_recebidas_historico).append(item)

    trocas_enviadas = []
    for t in ex:
        destino = troca_coops_map.get(t.destino_id)
        orig = troca_escalas_map.get(t.origem_escala_id)
        mensagem_limpa = _strip_afetacao_blob(t.mensagem)
        linhas_afetadas = _parse_linhas_from_msg(t.mensagem) if t.status == "aprovada" else []
        trocas_enviadas.append({
            "id": t.id,
            "status": t.status,
            "mensagem": mensagem_limpa,
            "criada_em": t.criada_em,
            "aplicada_em": t.aplicada_em,
            "destino": destino,
            "origem": orig,
            "origem_desc": _escala_desc(orig),
            "linhas_afetadas": linhas_afetadas,
        })


    return render_template(
        "painel_cooperado.html",
        cooperado=coop,
        producoes=producoes,
        receitas_coop=receitas_coop,
        despesas_coop=despesas_coop,
        total_bruto=total_bruto,
        inss_valor=inss_valor,
        sest_senat_valor=sest_valor,
        total_descontos=total_descontos,
        total_liquido=total_liquido,
        inss_complemento=inss_complemento,
        salario_minimo=salario_minimo,
        current_year=today.year,
        hoje=today,
        doc_cnh=doc_cnh,
        doc_placa=doc_placa,
        minha_escala=minha_escala,
        minha_escala_json=minha_escala_json,
        cooperados_json=cooperados_json,
        cooperados_escalas_map=cooperados_escalas_map,
        trocas_recebidas_pendentes=trocas_recebidas_pendentes,
        trocas_recebidas_historico=trocas_recebidas_historico,
        trocas_enviadas=trocas_enviadas,
        nota_vida=nota_vida,
        total_entregas_vida=total_entregas_vida,
        data_inicio=di,
        data_fim=df,
        saldo_devedor=saldo_devedor,
        total_a_descontar=total_a_descontar,
        despesas_detalhadas=despesas_detalhadas,
        solicitacoes_adiantamento=solicitacoes_adiantamento,
        adiantamento_disponivel=adiantamento_disponivel,
        bloquear_adiantamento=bool(getattr(cfg, "bloquear_adiantamento", False)),
        status_adiantamento_label=_status_adiantamento_label,
        status_adiantamento_badge=_status_adiantamento_badge,
        competencia_humana=_competencia_humana,
    )

@app.post("/portal/cooperado/adiantamento/solicitar")
@role_required("cooperado")
def solicitar_adiantamento_cooperado():
    u_id = session.get("user_id")
    coop = request_cooperado() or abort(404)

    cfg = get_config()
    if bool(getattr(cfg, "bloquear_adiantamento", False)):
        msg = "O pedido de adiantamento está temporariamente bloqueado pelo administrador."
        if _wants_json_response():
            return jsonify({"ok": False, "message": msg, "blocked": True}), 403
        flash(msg, "warning")
        return _portal_cooperado_redirect_tab("ajustes")

    di = _parse_date(request.form.get("data_inicio"))
    df = _parse_date(request.form.get("data_fim"))
    if di and not df:
        df = di
    if df and not di:
        di = df
    elif not di and not df:
        hoje = date.today()
        di = hoje - timedelta(days=hoje.weekday())
        df = di + timedelta(days=6)

    valor = round(float(request.form.get("valor") or 0), 2)
    disponivel = _adiantamento_disponivel_cooperado(coop.id, di, df)

    if valor <= 0:
        if _wants_json_response():
            return jsonify({"ok": False, "message": "Informe um valor válido."}), 400
        flash("Informe um valor válido.", "warning")
        return _portal_cooperado_redirect_tab("ajustes")

    if valor > disponivel:
        msg = f"O valor solicitado ultrapassa o líquido disponível após descontos. Máximo disponível: R$ {disponivel:.2f}"
        if _wants_json_response():
            return jsonify({"ok": False, "message": msg, "disponivel": disponivel}), 400
        flash(msg, "warning")
        return _portal_cooperado_redirect_tab("ajustes")

    s = SolicitacaoAdiantamento(
        cooperado_id=coop.id,
        valor_solicitado=valor,
        status="em_analise",
        data_pedido=date.today(),
    )
    db.session.add(s)
    db.session.commit()

    msg = "Solicitação de adiantamento enviada para análise."
    if _wants_json_response():
        return jsonify({
            "ok": True,
            "message": msg,
            "item": {
                "id": s.id,
                "status": s.status,
                "status_label": _status_adiantamento_label(s.status),
                "status_badge": _status_adiantamento_badge(s.status),
                "valor_solicitado": round(s.valor_solicitado or 0, 2),
                "valor_aprovado": round(s.valor_aprovado or 0, 2),
                "pedido_em": s.pedido_em.strftime("%d/%m/%Y %H:%M") if s.pedido_em else "—",
                "data_desconto": "—",
                "competencia_label": _competencia_humana(s.competencia_desconto),
                "motivo_recusa": s.motivo_recusa or "",
            },
            "disponivel": round(max(0.0, disponivel - valor), 2)
        })
    flash(msg, "success")
    return _portal_cooperado_redirect_tab("ajustes")




@app.post("/portal/cooperado/adiantamento/<int:id>/cancelar")
@role_required("cooperado")
def cancelar_adiantamento_cooperado(id):
    u_id = session.get("user_id")
    coop = request_cooperado() or abort(404)
    sol = SolicitacaoAdiantamento.query.filter_by(id=id, cooperado_id=coop.id).first_or_404()

    if (sol.status or "").strip().lower() != "em_analise":
        msg = "Só é possível cancelar solicitações em análise."
        if _wants_json_response():
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "warning")
        return _portal_cooperado_redirect_tab("ajustes")

    if sol.despesa_cooperado_id:
        dc = DespesaCooperado.query.get(sol.despesa_cooperado_id)
        if dc:
            if getattr(dc, 'abatimentos', None):
                msg = "Esta solicitação já teve descontos aplicados e não pode ser cancelada."
                if _wants_json_response():
                    return jsonify({"ok": False, "message": msg}), 400
                flash(msg, "warning")
                return _portal_cooperado_redirect_tab("ajustes")
            db.session.delete(dc)
        sol.despesa_cooperado_id = None

    db.session.delete(sol)
    db.session.commit()

    msg = "Solicitação cancelada com sucesso."
    if _wants_json_response():
        return jsonify({"ok": True, "message": msg})
    flash(msg, "success")
    return _portal_cooperado_redirect_tab("ajustes")


@app.post("/admin/adiantamentos/<int:id>/analisar")
@admin_perm_required("coop_despesas", "editar")
def analisar_adiantamento_admin(id):
    sol = SolicitacaoAdiantamento.query.get_or_404(id)
    acao = (request.form.get("acao") or "").strip().lower()
    motivo = (request.form.get("motivo_recusa") or "").strip()
    observacao_admin = (request.form.get("observacao_admin") or "").strip()
    valor_aprovado = round(float(request.form.get("valor_aprovado") or sol.valor_solicitado or 0), 2)
    data_escolhida = _parse_date(request.form.get("data_desconto")) or date.today()
    competencia_semana = (request.form.get("competencia_desconto") or "esta_semana").strip().lower()

    if acao not in {"aprovar", "recusar"}:
        return jsonify({"ok": False, "message": "Ação inválida."}), 400

    if acao == "recusar" and not motivo:
        return jsonify({"ok": False, "message": "Informe o motivo da recusa."}), 400

    if acao == "aprovar" and valor_aprovado <= 0:
        return jsonify({"ok": False, "message": "Informe um valor aprovado válido."}), 400

    sol.analisado_em = datetime.utcnow()
    sol.observacao_admin = observacao_admin or None

    if acao == "recusar":
        if sol.despesa_cooperado_id:
            dc = DespesaCooperado.query.get(sol.despesa_cooperado_id)
            if dc:
                if getattr(dc, 'abatimentos', None):
                    return jsonify({"ok": False, "message": "Esse adiantamento já teve descontos aplicados e não pode ser recusado."}), 400
                db.session.delete(dc)
            sol.despesa_cooperado_id = None
        sol.status = "recusado"
        sol.motivo_recusa = motivo
        sol.valor_aprovado = None
        sol.data_desconto = data_escolhida
        sol.competencia_desconto = competencia_semana
        db.session.commit()
        return jsonify({
            "ok": True,
            "message": "Solicitação recusada.",
            "item": {
                "id": sol.id,
                "status": sol.status,
                "status_label": _status_adiantamento_label(sol.status),
                "status_badge": _status_adiantamento_badge(sol.status),
                "motivo_recusa": sol.motivo_recusa or "",
                "valor_aprovado": 0.0,
                "data_desconto": sol.data_desconto.strftime("%d/%m/%Y") if sol.data_desconto else "—",
                "competencia_label": _competencia_humana(sol.competencia_desconto),
                "despesa_id": None,
            }
        })

    data_comp = _competencia_ref(data_escolhida, competencia_semana)
    di_comp, df_comp = semana_bounds(data_comp)

    dc = None
    if sol.despesa_cooperado_id:
        dc = DespesaCooperado.query.get(sol.despesa_cooperado_id)
    if not dc:
        dc = DespesaCooperado(cooperado_id=sol.cooperado_id)
        db.session.add(dc)

    dc.cooperado_id = sol.cooperado_id
    dc.descricao = f"Adiantamento de produção solicitado em {sol.data_pedido.strftime('%d/%m/%Y')}"
    dc.valor = valor_aprovado
    dc.data = df_comp
    dc.data_inicio = di_comp
    dc.data_fim = df_comp
    dc.eh_adiantamento = True
    dc.competencia_desconto = competencia_semana

    sol.status = "aprovado"
    sol.motivo_recusa = None
    sol.valor_aprovado = valor_aprovado
    sol.data_desconto = data_escolhida
    sol.competencia_desconto = competencia_semana

    db.session.flush()
    sol.despesa_cooperado_id = dc.id
    db.session.commit()
    return jsonify({
        "ok": True,
        "message": "Solicitação aprovada e lançada como adiantamento.",
        "item": {
            "id": sol.id,
            "status": sol.status,
            "status_label": _status_adiantamento_label(sol.status),
            "status_badge": _status_adiantamento_badge(sol.status),
            "motivo_recusa": "",
            "valor_aprovado": round(sol.valor_aprovado or 0, 2),
            "data_desconto": sol.data_desconto.strftime("%d/%m/%Y") if sol.data_desconto else "—",
            "competencia_label": _competencia_humana(sol.competencia_desconto),
            "despesa_id": sol.despesa_cooperado_id,
        }
    })


# === AVALIAR RESTAURANTE (cooperado -> restaurante)
# Duas rotas para a MESMA função e MESMO endpoint (o do template):
@app.post("/coop/avaliar/restaurante/<int:lanc_id>")
@app.post("/producoes/<int:lanc_id>/avaliar")
@role_required("cooperado")
def producoes_avaliar(lanc_id):
    # 1) Cooperado logado
    u_id = session.get("user_id")
    coop = request_cooperado() or abort(404)

    # 2) Lançamento existe e é dele
    lanc = Lancamento.query.get_or_404(lanc_id)
    if lanc.cooperado_id != coop.id:
        abort(403)

    # 3) Já existe avaliação DESTE cooperado para ESTE lançamento?
    ja = (AvaliacaoRestaurante.query
          .filter_by(lancamento_id=lanc.id, cooperado_id=coop.id)
          .first())
    if ja:
        flash("Você já avaliou esta produção.", "info")
        return _portal_cooperado_redirect_tab("producoes")

    # -------- Helpers locais --------
    def _clamp_star_local(v):
        try:
            n = int(float(v))
        except Exception:
            return None
        return n if 1 <= n <= 5 else None

    def _get(v):
        return request.form.get(v)

    # 4) Campos do form — SOMENTE 3 dimensões, com retrocompat:
    amb  = _clamp_star_local(_get("av_ambiente")    or _get("av_apresentacao"))  # retro
    trat = _clamp_star_local(_get("av_tratamento")  or _get("av_educacao"))      # retro
    sup  = _clamp_star_local(_get("av_suporte")     or _get("av_eficiencia"))    # retro

    # Se vier 'nota' / 'av_geral', usa como fallback nas faltantes
    nota = _clamp_star_local(_get("nota") or _get("av_geral"))
    if nota is not None:
        if amb  is None: amb  = nota
        if trat is None: trat = nota
        if sup  is None: sup  = nota

    # Validação
    if not (amb and trat and sup):
        flash("Selecione notas (1..5) para Ambiente, Tratamento e Suporte.", "warning")
        return _portal_cooperado_redirect_tab("producoes")

    # Média (geral e ponderada iguais, com arredondamentos diferentes)
    media = (amb + trat + sup) / 3.0
    estrelas_geral  = round(media, 1)
    media_ponderada = round(media, 2)

    comentario = (_get("av_comentario") or "").strip() or None

    # Derivados opcionais — mantém compat se suas funções existirem
    try:
        senti = _analise_sentimento(comentario) if comentario else None
    except Exception:
        senti = None
    try:
        temas = "; ".join(_identifica_temas(comentario)) if comentario else None
    except Exception:
        temas = None
    try:
        crise = _sinaliza_crise(estrelas_geral, comentario)
    except Exception:
        crise = False

    a = AvaliacaoRestaurante(
        restaurante_id=lanc.restaurante_id,
        cooperado_id=coop.id,
        lancamento_id=lanc.id,

        estrelas_ambiente=amb,
        estrelas_tratamento=trat,
        estrelas_suporte=sup,
        estrelas_geral=estrelas_geral,
        media_ponderada=media_ponderada,

        comentario=comentario,
        sentimento=senti,
        temas=temas,
        alerta_crise=crise,
        # criado_em => default no modelo
    )
    db.session.add(a)

    try:
        db.session.commit()
        flash("Avaliação do restaurante registrada.", "success")
    except IntegrityError:
        db.session.rollback()
        flash("Avaliação já registrada para este lançamento.", "info")

    return _portal_cooperado_redirect_tab("producoes")


# === ALIAS DO PAINEL: /painel/cooperado  -> redireciona para o endpoint oficial
@app.get("/painel/cooperado")
@role_required("cooperado")
def coop_dashboard_alias():
    return redirect(url_for("coop_dashboard"))
    

def _portal_cooperado_redirect_tab(tab: str = "resumo", **extra):
    params = {"active_tab": tab}
    for k, v in (extra or {}).items():
        if v is not None and v != "":
            params[k] = v
    return redirect(url_for("portal_cooperado", **params))


@app.route("/escala/solicitar_troca", methods=["POST"])
@role_required("cooperado")
def solicitar_troca():
    u_id = session.get("user_id")
    me = request_cooperado()
    if not me:
        abort(403)

    from_escala_id = request.form.get("from_escala_id", type=int)
    to_cooperado_id = request.form.get("to_cooperado_id", type=int)
    destino_escala_id = request.form.get("destino_escala_id", type=int)
    mensagem = (request.form.get("mensagem") or "").strip()

    if not from_escala_id or not to_cooperado_id:
        flash("Selecione o seu turno e o cooperado de destino.", "warning")
        return _portal_cooperado_redirect_tab("trocas")

    origem = Escala.query.get(from_escala_id)
    if not origem or origem.cooperado_id != me.id:
        flash("Turno inválido para solicitação.", "danger")
        return _portal_cooperado_redirect_tab("trocas")

    destino = Cooperado.query.get(to_cooperado_id)
    if not destino or destino.id == me.id:
        flash("Cooperado de destino inválido.", "danger")
        return _portal_cooperado_redirect_tab("trocas")

    def _safe_str(v):
        return str(v or "").strip()

    def _norm_horario(v):
        txt = _safe_str(v)
        m = re.findall(r"(\d{1,2}):(\d{2})", txt)
        if m:
            return " | ".join(f"{int(h):02d}:{int(mm):02d}" for h, mm in m)
        return _norm(txt)

    def _escala_signature(e: Escala | None):
        if not e:
            return None
        return {
            "weekday": _weekday_from_data_str(getattr(e, "data", None)),
            "bucket": _turno_bucket(getattr(e, "turno", None), getattr(e, "horario", None)),
            "horario": _norm_horario(getattr(e, "horario", None)),
            "contrato": _norm(getattr(e, "contrato", "") or ""),
        }

    def _same_turno(sig_a, sig_b) -> bool:
        return bool(
            sig_a and sig_b
            and sig_a["weekday"] == sig_b["weekday"]
            and sig_a["bucket"] == sig_b["bucket"]
        )

    nova_sig = _escala_signature(origem)
    if not nova_sig:
        flash("Não foi possível identificar esse turno para a troca.", "danger")
        return _portal_cooperado_redirect_tab("trocas")

    escalas_destino_compativeis = []
    for e_dest in Escala.query.filter_by(cooperado_id=destino.id).order_by(Escala.id.asc()).all():
        sig_dest = _escala_signature(e_dest)
        if sig_dest and _same_turno(sig_dest, nova_sig):
            escalas_destino_compativeis.append(e_dest)

    escala_destino_escolhida = None
    modo_passagem = False
    if escalas_destino_compativeis:
        if destino_escala_id:
            escala_destino_escolhida = next((e for e in escalas_destino_compativeis if int(e.id) == int(destino_escala_id)), None)
            if not escala_destino_escolhida:
                flash("O turno escolhido do cooperado não é compatível com essa troca.", "warning")
                return _portal_cooperado_redirect_tab("trocas")
        elif len(escalas_destino_compativeis) == 1:
            escala_destino_escolhida = escalas_destino_compativeis[0]
        else:
            flash("Esse cooperado possui mais de um turno compatível. Escolha qual turno dele será usado na troca.", "info")
            return _portal_cooperado_redirect_tab("trocas")
    else:
        modo_passagem = True

    contratos_destino = {
        _norm(getattr(e, "contrato", "") or "")
        for e in escalas_destino_compativeis
        if (getattr(e, "contrato", "") or "").strip()
    }
    ids_destino_compativeis = {int(e.id) for e in escalas_destino_compativeis if e.id}

    pendentes_mesma_dupla = (
        TrocaSolicitacao.query
        .filter(TrocaSolicitacao.status == "pendente")
        .filter(
            or_(
                and_(TrocaSolicitacao.solicitante_id == me.id, TrocaSolicitacao.destino_id == destino.id),
                and_(TrocaSolicitacao.solicitante_id == destino.id, TrocaSolicitacao.destino_id == me.id),
            )
        )
        .order_by(TrocaSolicitacao.id.desc())
        .all()
    )

    for t_exist in pendentes_mesma_dupla:
        esc_exist = Escala.query.get(t_exist.origem_escala_id)
        if not esc_exist:
            continue
        exist_sig = _escala_signature(esc_exist)
        if not exist_sig or not _same_turno(nova_sig, exist_sig):
            continue

        contrato_exist = _norm(getattr(esc_exist, "contrato", "") or "")
        mesma_origem = int(t_exist.origem_escala_id or 0) == int(origem.id)

        troca_reversa_equivalente = (
            t_exist.solicitante_id == destino.id and (
                mesma_origem
                or int(getattr(esc_exist, "id", 0) or 0) == int(getattr(escala_destino_escolhida, "id", 0) or 0)
                or int(getattr(esc_exist, "id", 0) or 0) in ids_destino_compativeis
                or (contrato_exist and contrato_exist in contratos_destino)
                or modo_passagem
            )
        )
        troca_mesmo_sentido = (
            t_exist.solicitante_id == me.id and (
                mesma_origem
                or int(getattr(esc_exist, "id", 0) or 0) == int(getattr(escala_destino_escolhida, "id", 0) or 0)
                or (contrato_exist and contrato_exist == _norm(getattr(origem, "contrato", "") or ""))
                or modo_passagem
            )
        )

        if troca_reversa_equivalente:
            flash("Já existe uma solicitação dessa mesma troca. Vá na aba Trocas e aceite a solicitação que já foi enviada.", "info")
            return _portal_cooperado_redirect_tab("trocas")

        if troca_mesmo_sentido:
            flash("Já existe uma solicitação pendente para esse mesmo turno com esse cooperado. Abra a aba Trocas para acompanhar.", "warning")
            return _portal_cooperado_redirect_tab("trocas")

    t = TrocaSolicitacao(
        solicitante_id=me.id,
        destino_id=destino.id,
        origem_escala_id=origem.id,
        mensagem=mensagem or None,
        status="pendente",
    )
    db.session.add(t)
    db.session.commit()

    if modo_passagem:
        flash("Solicitação de passagem de turno enviada com sucesso. Agora o cooperado precisa aceitar.", "success")
    else:
        flash("Solicitação de troca enviada com sucesso.", "success")
    return _portal_cooperado_redirect_tab("trocas")

@app.post("/trocas/<int:troca_id>/aceitar")
@role_required("cooperado")
def aceitar_troca(troca_id):
    u_id = session.get("user_id")
    me = request_cooperado()
    t = TrocaSolicitacao.query.get_or_404(troca_id)

    if not me or t.destino_id != me.id:
        abort(403)

    if t.status != "pendente":
        flash("Esta solicitação já foi tratada.", "warning")
        return _portal_cooperado_redirect_tab("trocas")

    destino_escala_id = request.form.get("destino_escala_id", type=int)
    orig_e = Escala.query.get(t.origem_escala_id)

    if not orig_e:
        flash("Turno de origem inválido.", "danger")
        return _portal_cooperado_redirect_tab("trocas")

    minhas = Escala.query.filter_by(cooperado_id=me.id).order_by(Escala.id.asc()).all()
    wd_o = _weekday_from_data_str(orig_e.data)
    buck_o = _turno_bucket(orig_e.turno, orig_e.horario)
    candidatas = [
        e for e in minhas
        if _weekday_from_data_str(e.data) == wd_o
        and _turno_bucket(e.turno, e.horario) == buck_o
    ]

    dest_e = None
    if destino_escala_id:
        dest_e = Escala.query.get(destino_escala_id)
        if not dest_e or dest_e.cooperado_id != me.id:
            flash("Seleção de turno inválida.", "danger")
            return _portal_cooperado_redirect_tab("trocas")
    elif len(candidatas) == 1:
        dest_e = candidatas[0]
    elif len(candidatas) > 1:
        flash("Escolha na aba Trocas qual dos seus turnos compatíveis deseja usar.", "warning")
        return _portal_cooperado_redirect_tab("trocas")

    solicitante = Cooperado.query.get(t.solicitante_id)
    destinatario = me

    if dest_e is None:
        linhas = [{
            "dia": _escala_label(orig_e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(orig_e.turno or "").strip(), (orig_e.horario or "").strip()] if x]),
            "contrato": (orig_e.contrato or "").strip(),
            "saiu": solicitante.nome if solicitante else "",
            "entrou": destinatario.nome,
        }]
        afetacao_json = {"linhas": linhas}
        orig_e.cooperado_id = destinatario.id
        orig_e.cooperado_nome = None
        t.status = "aprovada"
        t.aplicada_em = datetime.utcnow()
        prefix = "" if not (t.mensagem and t.mensagem.strip()) else (t.mensagem.rstrip() + "\n")
        t.mensagem = prefix + "__AFETACAO_JSON__:" + json.dumps(afetacao_json, ensure_ascii=False)
        when = datetime.utcnow()
        _prune_histories()
        _log_troca_historico_rows(t.id, linhas, solicitante=solicitante, destinatario=destinatario, tipo="passagem", when=when)
        for linha in linhas:
            turno_txt = linha.get("turno_horario", "")
            turno_part = turno_txt.split("•")[0].strip() if turno_txt else ""
            horario_part = turno_txt.split("•", 1)[1].strip() if "•" in turno_txt else ""
            _log_escala_historico(
                origem="passagem_aprovada",
                acao="passagem",
                escala_ref_id=orig_e.id,
                troca_ref_id=t.id,
                grupo_ref=str(uuid.uuid4()),
                data=linha.get("dia", ""),
                turno=turno_part,
                horario=horario_part,
                contrato=linha.get("contrato", ""),
                cooperado_id=destinatario.id,
                cooperado_nome=linha.get("entrou", ""),
                saiu_nome=linha.get("saiu", ""),
                entrou_nome=linha.get("entrou", ""),
                snapshot_em=when,
            )
        db.session.commit()
        flash("Turno passado com sucesso!", "success")
        return _portal_cooperado_redirect_tab("trocas")

    wd_dest = _weekday_from_data_str(dest_e.data)
    buck_dest = _turno_bucket(dest_e.turno, dest_e.horario)
    if wd_o is None or wd_dest is None or wd_o != wd_dest or buck_o != buck_dest:
        flash("Troca incompatível: precisa ser no mesmo dia da semana e no mesmo turno.", "danger")
        return _portal_cooperado_redirect_tab("trocas")

    linhas = [
        {
            "dia": _escala_label(orig_e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(orig_e.turno or "").strip(), (orig_e.horario or "").strip()] if x]),
            "contrato": (orig_e.contrato or "").strip(),
            "saiu": solicitante.nome if solicitante else "",
            "entrou": destinatario.nome,
        },
        {
            "dia": _escala_label(dest_e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(dest_e.turno or "").strip(), (dest_e.horario or "").strip()] if x]),
            "contrato": (dest_e.contrato or "").strip(),
            "saiu": destinatario.nome,
            "entrou": solicitante.nome if solicitante else "",
        }
    ]
    afetacao_json = {"linhas": linhas}

    solicitante_id = orig_e.cooperado_id
    destino_id = dest_e.cooperado_id
    orig_e.cooperado_id = destino_id
    dest_e.cooperado_id = solicitante_id
    if orig_e.cooperado_id:
        orig_e.cooperado_nome = None
    if dest_e.cooperado_id:
        dest_e.cooperado_nome = None

    t.status = "aprovada"
    t.aplicada_em = datetime.utcnow()
    prefix = "" if not (t.mensagem and t.mensagem.strip()) else (t.mensagem.rstrip() + "\n")
    t.mensagem = prefix + "__AFETACAO_JSON__:" + json.dumps(afetacao_json, ensure_ascii=False)

    when = datetime.utcnow()
    _prune_histories()
    _log_troca_historico_rows(t.id, linhas, solicitante=solicitante, destinatario=destinatario, tipo="troca", when=when)
    for linha in linhas:
        turno_txt = linha.get("turno_horario", "")
        turno_part = turno_txt.split("•")[0].strip() if turno_txt else ""
        horario_part = turno_txt.split("•", 1)[1].strip() if "•" in turno_txt else ""
        _log_escala_historico(
            origem="troca_aprovada",
            acao="troca",
            troca_ref_id=t.id,
            grupo_ref=str(uuid.uuid4()),
            data=linha.get("dia", ""),
            turno=turno_part,
            horario=horario_part,
            contrato=linha.get("contrato", ""),
            cooperado_nome=linha.get("entrou", ""),
            saiu_nome=linha.get("saiu", ""),
            entrou_nome=linha.get("entrou", ""),
            snapshot_em=when,
        )
    db.session.commit()
    flash("Troca aplicada com sucesso!", "success")
    return _portal_cooperado_redirect_tab("trocas")

@app.post("/trocas/<int:troca_id>/recusar")
@role_required("cooperado")
def recusar_troca(troca_id):
    u_id = session.get("user_id")
    me = request_cooperado()
    t = TrocaSolicitacao.query.get_or_404(troca_id)

    if not me or t.destino_id != me.id:
        abort(403)

    if t.status != "pendente":
        flash("Esta solicitação já foi tratada.", "warning")
        return _portal_cooperado_redirect_tab("trocas")

    t.status = "recusada"
    db.session.commit()

    flash("Solicitação recusada.", "info")
    return _portal_cooperado_redirect_tab("trocas")

