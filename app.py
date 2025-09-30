@app.post("/trocas/<int:troca_id>/aceitar")
@role_required("cooperado")
def aceitar_troca(troca_id):
    u_id = session.get("user_id")
    me = Cooperado.query.filter_by(usuario_id=u_id).first()
    t = TrocaSolicitacao.query.get_or_404(troca_id)
    if not me or t.destino_id != me.id:
        abort(403)
    if t.status != "pendente":
        flash("Esta solicitação já foi tratada.", "warning")
        return redirect(url_for("portal_cooperado"))

    destino_escala_id = request.form.get("destino_escala_id", type=int)
    orig_e = Escala.query.get(t.origem_escala_id)
    if not orig_e:
        flash("Plantão de origem inválido.", "danger")
        return redirect(url_for("portal_cooperado"))

    if not destino_escala_id:
        minhas = Escala.query.filter_by(cooperado_id=me.id).order_by(Escala.id.asc()).all()
        wd_o = _weekday_from_data_str(orig_e.data)
        buck_o = _turno_bucket(orig_e.turno, orig_e.horario)
        candidatas = [e for e in minhas
                      if _weekday_from_data_str(e.data) == wd_o and _turno_bucket(e.turno, e.horario) == buck_o]
        if len(candidatas) == 1:
            destino_escala_id = candidatas[0].id
        elif len(candidatas) == 0:
            flash("Você não tem plantões compatíveis (mesmo dia da semana e turno).", "danger")
            return redirect(url_for("portal_cooperado"))
        else:
            flash("Selecione no modal qual dos seus plantões compatíveis deseja usar.", "warning")
            return redirect(url_for("portal_cooperado"))

    dest_e = Escala.query.get(destino_escala_id)
    if not dest_e or dest_e.cooperado_id != me.id:
        flash("Seleção de escala inválida.", "danger")
        return redirect(url_for("portal_cooperado"))

    wd_orig = _weekday_from_data_str(orig_e.data)
    wd_dest = _weekday_from_data_str(dest_e.data)
    buck_orig = _turno_bucket(orig_e.turno, orig_e.horario)
    buck_dest = _turno_bucket(dest_e.turno, dest_e.horario)
    if wd_orig is None or wd_dest is None or wd_orig != wd_dest or buck_orig != buck_dest:
        flash("Troca incompatível: precisa ser mesmo dia da semana e mesmo turno (dia/noite).", "danger")
        return redirect(url_for("portal_cooperado"))

    solicitante = Cooperado.query.get(t.solicitante_id)
    destinatario = me
    linhas = [
        {
            "dia": _escala_label(orig_e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(orig_e.turno or "").strip(), (orig_e.horario or "").strip()] if x]),
            "contrato": (orig_e.contrato or "").strip(),
            "saiu": solicitante.nome,
            "entrou": destinatario.nome,
        },
        {
            "dia": _escala_label(dest_e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(dest_e.turno or "").strip(), (dest_e.horario or "").strip()] if x]),
            "contrato": (dest_e.contrato or "").strip(),
            "saiu": destinatario.nome,
            "entrou": solicitante.nome,
        }
    ]
    afetacao_json = {"linhas": linhas}

    # estas duas linhas DEVEM ficar alinhadas aqui (sem indent extra)
    solicitante_id = orig_e.cooperado_id
    destino_id = dest_e.cooperado_id

    # Faz a troca de fato
    orig_e.cooperado_id = destino_id
    dest_e.cooperado_id = solicitante_id

    # Marca como aprovada e anexa o JSON de afetação na mensagem
    t.status = "aprovada"
    t.aplicada_em = datetime.utcnow()
    prefix = "" if not (t.mensagem and t.mensagem.strip()) else (t.mensagem.rstrip() + "\n")
    t.mensagem = prefix + "__AFETACAO_JSON__:" + json.dumps({"linhas": linhas}, ensure_ascii=False)

    db.session.commit()
    flash("Troca realizada com sucesso!", "success")
    return redirect(url_for("portal_cooperado"))
