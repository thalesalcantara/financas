"""Rotas do Portal do Estabelecimento.

Extraídas do app.py sem alterar endpoints ou regras de negócio.
"""
from __future__ import annotations

import app as legacy

for _name, _value in vars(legacy).items():
    if not _name.startswith("__"):
        globals().setdefault(_name, _value)

# =========================
# PORTAL RESTAURANTE
# =========================
@app.route("/portal/restaurante")
@role_required("restaurante")
def portal_restaurante():
    from datetime import date, timedelta, datetime
    import re
    from werkzeug.routing import BuildError

    u_id = session.get("user_id")
    rest = request_restaurante()
    if not rest:
        return (
            "<p style='font-family:Arial;margin:40px'>"
            "Seu usuário não está vinculado a um estabelecimento. Avise o administrador."
            "</p>"
        )
    if getattr(rest, "eh_farmacia", False):
        return redirect(url_for("farmacia_dashboard"))

    # Abas/visões
    view = (request.args.get("view", "lancar") or "lancar").strip().lower()

    # ---- helper mês YYYY-MM
    def _parse_yyyy_mm_local(s: str):
        if not s:
            return None, None
        m = re.fullmatch(r"(\d{4})-(\d{2})", s.strip())
        if not m:
            return None, None
        y = int(m.group(1))
        mth = int(m.group(2))
        try:
            di_ = date(y, mth, 1)
            if mth == 12:
                df_ = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                df_ = date(y, mth + 1, 1) - timedelta(days=1)
            return di_, df_
        except Exception:
            return None, None

    # -------------------- FILTRO DE PERÍODO --------------------
    di = _parse_date(request.args.get("data_inicio"))
    df = _parse_date(request.args.get("data_fim"))

    mes = (request.args.get("mes") or "").strip()
    periodo_desc = None

    if mes:
        di_mes, df_mes = _parse_yyyy_mm_local(mes)
        if di_mes and df_mes:
            di, df = di_mes, df_mes
            periodo_desc = "mês"

    if not di or not df:
        wd_map = {"seg-dom": 0, "sab-sex": 5, "sex-qui": 4}
        start_wd = wd_map.get(getattr(rest, "periodo", None), 0)
        hoje = date.today()
        delta = (hoje.weekday() - start_wd) % 7
        di_auto = hoje - timedelta(days=delta)
        df_auto = di_auto + timedelta(days=6)
        di = di or di_auto
        df = df or df_auto
        periodo_desc = periodo_desc or getattr(rest, "periodo", "seg-dom")
    else:
        periodo_desc = periodo_desc or "personalizado"

    # -------------------- HELPER: contrato do restaurante --------------------
    def contrato_bate_restaurante(contrato: str, rest_nome: str) -> bool:
        a = " ".join(_normalize_name(contrato or ""))
        b = " ".join(_normalize_name(rest_nome or ""))
        if not a or not b:
            return False
        return a == b or a in b or b in a

    # -------------------- ESCALA (Quem trabalha) --------------------
    ref = _parse_date(request.args.get("ref")) or date.today()
    modo = request.args.get("modo", "semana")

    if modo == "dia":
        dias_list = [ref]
    else:
        semana_inicio = ref - timedelta(days=ref.weekday())
        dias_list = [semana_inicio + timedelta(days=i) for i in range(7)]

    # Escalas do estabelecimento: vínculo por ID + compatibilidade somente
    # para linhas legadas sem restaurante_id. Evita varrer toda a tabela.
    direct_scales = (
        db.session.query(Escala)
        .outerjoin(Cooperado, Escala.cooperado_id == Cooperado.id)
        .outerjoin(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(
            Escala.restaurante_id == rest.id,
            or_(Escala.cooperado_id.is_(None), Usuario.ativo.is_(True)),
        )
        .order_by(Escala.id.asc())
        .all()
    )
    legacy_scales = (
        db.session.query(Escala)
        .outerjoin(Cooperado, Escala.cooperado_id == Cooperado.id)
        .outerjoin(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(
            Escala.restaurante_id.is_(None),
            Escala.contrato.isnot(None),
            or_(Escala.cooperado_id.is_(None), Usuario.ativo.is_(True)),
        )
        .order_by(Escala.id.asc())
        .all()
    )
    escalas_all = sorted(direct_scales + legacy_scales, key=lambda e: e.id)
    eff_map = _carry_forward_contrato(escalas_all)

    direct_ids = {e.id for e in direct_scales}
    escalas_rest = []
    seen_scale_ids = set()
    for e in escalas_all:
        belongs = e.id in direct_ids
        if not belongs:
            belongs = contrato_bate_restaurante(
                eff_map.get(e.id, e.contrato or ""),
                rest.nome,
            )
        if belongs and e.id not in seen_scale_ids:
            seen_scale_ids.add(e.id)
            escalas_rest.append(e)

    agenda = {d: [] for d in dias_list}
    seen = {d: set() for d in dias_list}

    coop_ids_escala = {e.cooperado_id for e in escalas_rest if e.cooperado_id}
    coops_escala_map = {}
    if coop_ids_escala:
        coops_escala_map = {
            c.id: c for c in (
                Cooperado.query
                .join(Usuario, Cooperado.usuario_id == Usuario.id)
                .filter(Cooperado.id.in_(coop_ids_escala), Usuario.ativo.is_(True))
                .all()
            )
        }

    for e in escalas_rest:
        dt = _parse_data_escala_str(e.data)
        wd = _weekday_from_data_str(e.data)

        for d in dias_list:
            hit = (dt and dt == d) or (wd and wd == ((d.weekday() % 7) + 1))
            if not hit:
                continue

            coop = coops_escala_map.get(e.cooperado_id) if e.cooperado_id else None

            nome_fallback = (e.cooperado_nome or "").strip()
            nome_show = (coop.nome if coop else nome_fallback) or "—"
            contrato_eff = (eff_map.get(e.id, e.contrato or "") or "").strip()

            key = (
                (coop.id if coop else _norm(nome_show)),
                _norm(e.turno),
                _norm(e.horario),
                _norm(contrato_eff),
            )

            if key in seen[d]:
                break

            seen[d].add(key)

            agenda[d].append({
                "coop": coop,
                "cooperado_nome": nome_fallback or None,
                "nome_planilha": nome_show,
                "turno": (e.turno or "").strip(),
                "horario": (e.horario or "").strip(),
                "contrato": contrato_eff,
                "cor": (e.cor or "").strip(),
            })

            break

    for d in dias_list:
        agenda[d].sort(
            key=lambda x: (
                (x["contrato"] or "").lower(),
                (x.get("nome_planilha") or (x["coop"].nome if x["coop"] else "")).lower(),
            )
        )

    # -------------------- COOPERADOS ESCALADOS NO PERÍODO / HOJE --------------------
    hoje = date.today()

    ids_escalados_periodo = set()
    ids_escalados_hoje = set()
    nomes_escalados_sem_cadastro = set()

    for d in dias_list:
        for item in agenda.get(d, []):
            coop_item = item.get("coop")

            if coop_item and coop_item.id:
                ids_escalados_periodo.add(coop_item.id)

                if d == hoje:
                    ids_escalados_hoje.add(coop_item.id)
            else:
                nome_pl = (item.get("nome_planilha") or "").strip()
                if nome_pl:
                    nomes_escalados_sem_cadastro.add(nome_pl)

    cooperados_escalados = (
        Cooperado.query
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(
            Usuario.ativo.is_(True),
            Cooperado.id.in_(ids_escalados_periodo) if ids_escalados_periodo else literal(False)
        )
        .order_by(Cooperado.nome)
        .all()
    )

    # todos ativos, para busca manual no lançamento
    cooperados_ativos = (
        Cooperado.query
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Usuario.ativo.is_(True))
        .order_by(Cooperado.nome)
        .all()
    )

    # marca quem está escalado no período e quem está escalado hoje
    for c in cooperados_ativos:
        c.escalado = c.id in ids_escalados_periodo
        c.escalado_hoje = c.id in ids_escalados_hoje

    # lista exibida no painel "lancar":
    # primeiro os escalados de hoje, depois os demais
    cooperados = sorted(
        cooperados_ativos,
        key=lambda c: (
            0 if getattr(c, "escalado_hoje", False) else 1,
            (c.nome or "").lower()
        )
    )
    # -------------------- LANÇAMENTOS / TOTAIS POR PERÍODO --------------------
    total_bruto = 0.0
    total_qtd = 0
    total_entregas = 0
    total_inss = 0.0
    total_sest = 0.0

    # Totais por cooperado em uma única consulta (evita 1 consulta para cada cooperado).
    totais_rows = (
        db.session.query(
            Lancamento.cooperado_id,
            func.coalesce(func.sum(Lancamento.valor), 0.0),
            func.count(Lancamento.id),
            func.coalesce(func.sum(Lancamento.qtd_entregas), 0),
        )
        .filter(
            Lancamento.restaurante_id == rest.id,
            Lancamento.data >= di,
            Lancamento.data <= df,
        )
        .group_by(Lancamento.cooperado_id)
        .all()
    )
    totais_map = {cid: {"valor": float(valor or 0.0), "qtd": int(qtd or 0), "entregas": int(entregas or 0)} for cid, valor, qtd, entregas in totais_rows}

    # Para o painel de lançamento, mantém a lista de produções por cooperado
    # para aparecer a tabela antiga com Editar/Excluir, sem fazer 1 consulta por cooperado.
    lancs_por_coop_periodo = defaultdict(list)
    if view == "lancar":
        lancamentos_periodo_all = (
            Lancamento.query
            .filter(
                Lancamento.restaurante_id == rest.id,
                Lancamento.data >= di,
                Lancamento.data <= df,
            )
            .order_by(Lancamento.data.desc(), Lancamento.id.desc())
            .all()
        )
        for _l in lancamentos_periodo_all:
            lancs_por_coop_periodo[_l.cooperado_id].append(_l)

    for c in cooperados:
        t = totais_map.get(c.id, {"valor": 0.0, "qtd": 0, "entregas": 0})
        c.lancamentos = lancs_por_coop_periodo.get(c.id, [])
        c.total_periodo = t["valor"]
        c.inss_periodo = c.total_periodo * 0.04
        c.sest_periodo = c.total_periodo * 0.005
        c.encargos_periodo = c.inss_periodo + c.sest_periodo
        c.liquido_periodo = c.total_periodo - c.encargos_periodo

        total_bruto += c.total_periodo
        total_qtd += t["qtd"]
        total_entregas += t["entregas"]
        total_inss += c.inss_periodo
        total_sest += c.sest_periodo

    total_encargos = total_inss + total_sest
    total_liquido = total_bruto - total_encargos

    # -------------------- LISTA DE LANÇAMENTOS --------------------
    lancamentos_periodo = []
    total_lanc_valor = 0.0
    total_lanc_entregas = 0

    if view == "lancamentos":
        q = (
            db.session.query(Lancamento, Cooperado)
            .join(Cooperado, Cooperado.id == Lancamento.cooperado_id)
            .filter(
                Lancamento.restaurante_id == rest.id,
                Lancamento.data >= di,
                Lancamento.data <= df,
            )
            .order_by(Lancamento.data.asc(), Lancamento.id.asc())
        )

        for lanc, coop in q.all():
            item = {
                "id": lanc.id,
                "data": lanc.data.strftime("%d/%m/%Y") if lanc.data else "",
                "hora_inicio": (
                    lanc.hora_inicio if isinstance(lanc.hora_inicio, str)
                    else (lanc.hora_inicio.strftime("%H:%M") if lanc.hora_inicio else "")
                ),
                "hora_fim": (
                    lanc.hora_fim if isinstance(lanc.hora_fim, str)
                    else (lanc.hora_fim.strftime("%H:%M") if lanc.hora_fim else "")
                ),
                "qtd_entregas": lanc.qtd_entregas or 0,
                "valor": float(lanc.valor or 0.0),
                "descricao": (lanc.descricao or ""),
                "cooperado_id": coop.id,
                "cooperado_nome": coop.nome,
                "contrato_nome": rest.nome,
            }
            lancamentos_periodo.append(item)

        total_lanc_valor = sum(x["valor"] for x in lancamentos_periodo)
        total_lanc_entregas = sum(x["qtd_entregas"] for x in lancamentos_periodo)

    # -------------------- PENDÊNCIAS DE LANÇAMENTO DO DIA --------------------
    pendencias_lancamento = []
    hoje = date.today()
    agora = datetime.now()

    def _hora_inicial_min(horario_txt: str) -> int | None:
        m = re.search(r"(\d{1,2}):(\d{2})", str(horario_txt or ""))
        if not m:
            return None
        return int(m.group(1)) * 60 + int(m.group(2))

    def _hora_final_min(horario_txt: str) -> int | None:
        txt = str(horario_txt or "")
        pares = re.findall(r"(\d{1,2}):(\d{2})", txt)
        if len(pares) >= 2:
            hh, mm = pares[-1]
            return int(hh) * 60 + int(mm)

        m = re.search(r"\b(?:as|às|a)\s*(\d{1,2}):(\d{2})", txt.lower())
        if m:
            return int(m.group(1)) * 60 + int(m.group(2))

        return None

    minutos_agora = agora.hour * 60 + agora.minute

    escalas_hoje = agenda.get(hoje, [])
    lancs_hoje_por_coop = defaultdict(list)
    for _l in (
        Lancamento.query
        .filter(Lancamento.restaurante_id == rest.id, Lancamento.data == hoje)
        .order_by(Lancamento.cooperado_id.asc(), Lancamento.id.asc())
        .all()
    ):
        lancs_hoje_por_coop[_l.cooperado_id].append(_l)
    for item in escalas_hoje:
        coop = item.get("coop")
        if not coop:
            continue

        horario_txt = (item.get("horario") or "").strip()
        turno_txt = (item.get("turno") or "").strip()
        contrato_txt = (item.get("contrato") or rest.nome).strip()

        hora_ini = _hora_inicial_min(horario_txt)
        hora_fim = _hora_final_min(horario_txt)

        if hora_fim is None and hora_ini is not None:
            if "noite" in turno_txt.lower():
                hora_fim = 23 * 60 + 59
            else:
                hora_fim = hora_ini + 240

        if hora_fim is None:
            continue

        if minutos_agora < hora_fim:
            continue

        lanc_do_dia = lancs_hoje_por_coop.get(coop.id, [])

        existe_mesmo_horario = False
        for lanc in lanc_do_dia:
            hi = (lanc.hora_inicio or "").strip() if isinstance(lanc.hora_inicio, str) else (
                lanc.hora_inicio.strftime("%H:%M") if lanc.hora_inicio else ""
            )
            hf = (lanc.hora_fim or "").strip() if isinstance(lanc.hora_fim, str) else (
                lanc.hora_fim.strftime("%H:%M") if lanc.hora_fim else ""
            )

            if hi and hf and horario_txt:
                if hi in horario_txt and hf in horario_txt:
                    existe_mesmo_horario = True
                    break

            if hi and not hf and horario_txt and hi in horario_txt:
                existe_mesmo_horario = True
                break

        if not existe_mesmo_horario:
            pendencias_lancamento.append({
                "cooperado_id": coop.id,
                "cooperado_nome": coop.nome,
                "turno": turno_txt or "—",
                "horario": horario_txt or "—",
                "contrato": contrato_txt or "—",
                "data": hoje.strftime("%d/%m/%Y"),
            })

    pendencias_lancamento.sort(
        key=lambda x: (
            x["cooperado_nome"].lower(),
            x["horario"].lower(),
            x["turno"].lower(),
        )
    )

    # -------------------- URLs auxiliares --------------------
    try:
        url_lancar_producao = url_for("lancar_producao")
    except BuildError:
        url_lancar_producao = "/restaurante/lancar_producao"

    has_editar_lanc = ("editar_lancamento" in app.view_functions)

    # -------------------- Render --------------------
    return render_template(
        "restaurante_dashboard.html",
        rest=rest,
        cooperados=cooperados,
        cooperados_escalados=cooperados_escalados,
        pendencias_lancamento=pendencias_lancamento,
        filtro_inicio=di,
        filtro_fim=df,
        filtro_mes=(mes or ""),
        periodo_desc=periodo_desc,
        total_bruto=total_bruto,
        total_inss=total_inss,
        total_sest=total_sest,
        total_encargos=total_encargos,
        total_liquido=total_liquido,
        total_qtd=total_qtd,
        total_entregas=total_entregas,
        view=view,
        agenda=agenda,
        dias_list=dias_list,
        ref_data=ref,
        modo=modo,
        lancamentos_periodo=(lancamentos_periodo if view == "lancamentos" else []),
        total_lanc_valor=total_lanc_valor,
        total_lanc_entregas=total_lanc_entregas,
        url_lancar_producao=url_lancar_producao,
        has_editar_lanc=has_editar_lanc,
    )
    # =====================================================
    # COOPERADOS ESCALADOS HOJE PARA ESTE RESTAURANTE
    # =====================================================
    hoje = date.today()

    escalados_hoje = []
    escalados_ids = set()

    escalas_hoje_rest = agenda.get(hoje, []) if "agenda" in locals() else []

    for item in escalas_hoje_rest:
        coop_obj = item.get("coop")
        if coop_obj and coop_obj.id not in escalados_ids:
            escalados_ids.add(coop_obj.id)
            escalados_hoje.append(coop_obj)

    escalados_hoje = sorted(escalados_hoje, key=lambda c: (c.nome or "").lower())

    # lista completa apenas para busca manual
    cooperados_busca_manual = (
        Cooperado.query
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Usuario.ativo.is_(True))
        .order_by(Cooperado.nome)
        .all()
    )

    # =====================================================
    # HELPERS DE HORÁRIO / PENDÊNCIAS
    # =====================================================
    def _parse_hora_min(hs: str | None):
        s = (hs or "").strip().lower()
        if not s:
            return None

        s = s.replace("h", ":")

        m = re.search(r"(\d{1,2})(?::(\d{2}))?", s)
        if not m:
            return None

        hh = int(m.group(1))
        mm = int(m.group(2) or 0)

        if hh < 0 or hh > 23 or mm < 0 or mm > 59:
            return None

        return hh * 60 + mm

    def _extrair_inicio_fim_intervalo(horario_txt: str | None):
        s = (horario_txt or "").strip().lower()
        if not s:
            return (None, None)

        s = s.replace("às", "as")
        s = s.replace("á", "a")

        partes = re.split(r"\s+as\s+|\s*-\s*|\s*a\s*", s)
        partes = [p.strip() for p in partes if p.strip()]

        if len(partes) >= 2:
            ini = _parse_hora_min(partes[0])
            fim = _parse_hora_min(partes[1])
            return ini, fim

        unico = _parse_hora_min(s)
        return unico, None

    # =====================================================
    # LANÇAMENTOS PENDENTES DO DIA
    # =====================================================
    agora = datetime.now()
    agora_min = agora.hour * 60 + agora.minute

    lancamentos_hoje_rest = (
        Lancamento.query
        .filter(
            Lancamento.restaurante_id == rest.id,
            Lancamento.data == hoje
        )
        .order_by(Lancamento.cooperado_id.asc(), Lancamento.id.asc())
        .all()
    )

    lancs_por_coop_hoje = defaultdict(list)
    for l in lancamentos_hoje_rest:
        lancs_por_coop_hoje[l.cooperado_id].append(l)

    lancamentos_pendentes = []
    pendentes_chaves = set()

    for item in escalas_hoje_rest:
        coop_obj = item.get("coop")
        if not coop_obj:
            continue

        horario_txt = (item.get("horario") or "").strip()
        turno_txt = (item.get("turno") or "").strip()
        contrato_txt = (item.get("contrato") or "").strip()

        ini_min, fim_min = _extrair_inicio_fim_intervalo(horario_txt)
        referencia_fim = fim_min if fim_min is not None else ini_min

        if referencia_fim is None:
            continue

        if agora_min < referencia_fim:
            continue

        escalas_vencidas_do_coop = []
        for x in escalas_hoje_rest:
            x_coop = x.get("coop")
            if not x_coop or x_coop.id != coop_obj.id:
                continue

            x_ini, x_fim = _extrair_inicio_fim_intervalo(x.get("horario"))
            x_ref = x_fim if x_fim is not None else x_ini

            if x_ref is not None and agora_min >= x_ref:
                escalas_vencidas_do_coop.append(x)

        qtd_vencidas = len(escalas_vencidas_do_coop)
        qtd_lancadas = len(lancs_por_coop_hoje.get(coop_obj.id, []))

        if qtd_lancadas < qtd_vencidas:
            chave_pend = (
                coop_obj.id,
                turno_txt.lower(),
                horario_txt.lower(),
                contrato_txt.lower(),
            )

            if chave_pend not in pendentes_chaves:
                pendentes_chaves.add(chave_pend)
                lancamentos_pendentes.append({
                    "chave": chave_pend,
                    "cooperado_id": coop_obj.id,
                    "cooperado_nome": coop_obj.nome,
                    "turno": turno_txt,
                    "horario": horario_txt,
                    "contrato": contrato_txt,
                    "fim_min": referencia_fim,
                })

    lancamentos_pendentes.sort(
        key=lambda x: (x["fim_min"], (x["cooperado_nome"] or "").lower())
    )

    # -------------------- Lista de lançamentos (aba "lancamentos") --------------------
    lancamentos_periodo = []
    total_lanc_valor = 0.0
    total_lanc_entregas = 0

    if view == "lancamentos":
        q = (
            db.session.query(Lancamento, Cooperado)
            .join(Cooperado, Cooperado.id == Lancamento.cooperado_id)
            .filter(
                Lancamento.restaurante_id == rest.id,
                Lancamento.data >= di,
                Lancamento.data <= df,
            )
            .order_by(Lancamento.data.asc(), Lancamento.id.asc())
        )
        for lanc, coop in q.all():
            item = {
                "id": lanc.id,
                "data": lanc.data.strftime("%d/%m/%Y") if lanc.data else "",
                "hora_inicio": (
                    lanc.hora_inicio if isinstance(lanc.hora_inicio, str)
                    else (lanc.hora_inicio.strftime("%H:%M") if lanc.hora_inicio else "")
                ),
                "hora_fim": (
                    lanc.hora_fim if isinstance(lanc.hora_fim, str)
                    else (lanc.hora_fim.strftime("%H:%M") if lanc.hora_fim else "")
                ),
                "qtd_entregas": lanc.qtd_entregas or 0,
                "valor": float(lanc.valor or 0.0),
                "descricao": (lanc.descricao or ""),
                "cooperado_id": coop.id,
                "cooperado_nome": coop.nome,
                "contrato_nome": rest.nome,
            }
            lancamentos_periodo.append(item)

        total_lanc_valor = sum(x["valor"] for x in lancamentos_periodo)
        total_lanc_entregas = sum(x["qtd_entregas"] for x in lancamentos_periodo)

    # ---- URLs/flags para template
    from werkzeug.routing import BuildError
    try:
        url_lancar_producao = url_for("lancar_producao")
    except BuildError:
        url_lancar_producao = "/restaurante/lancar_producao"

    has_editar_lanc = ("editar_lancamento" in app.view_functions)

    # -------------------- Render --------------------
    return render_template(
        "restaurante_dashboard.html",
        rest=rest,
        cooperados=cooperados,
        filtro_inicio=di,
        filtro_fim=df,
        filtro_mes=(mes or ""),
        periodo_desc=periodo_desc,
        total_bruto=total_bruto,
        total_inss=total_inss,
        total_sest=total_sest,
        total_encargos=total_encargos,
        total_liquido=total_liquido,
        total_qtd=total_qtd,
        total_entregas=total_entregas,
        view=view,
        agenda=agenda,
        dias_list=dias_list,
        ref_data=ref,
        modo=modo,
        lancamentos_periodo=(lancamentos_periodo if view == "lancamentos" else []),
        total_lanc_valor=total_lanc_valor,
        total_lanc_entregas=total_lanc_entregas,
        url_lancar_producao=url_lancar_producao,
        has_editar_lanc=has_editar_lanc,
        escalados_hoje=escalados_hoje,
        cooperados_busca_manual=cooperados_busca_manual,
        lancamentos_pendentes=lancamentos_pendentes,
        hoje=hoje,
    )

