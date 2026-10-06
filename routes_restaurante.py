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
    import time as _time
    _portal_perf_started = _time.perf_counter()
    from datetime import date, timedelta, datetime
    import re
    from werkzeug.routing import BuildError
    from sqlalchemy.orm import defer

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
    if view == "producoes":
        return redirect(url_for("portal_restaurante", view="lancar"))

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

    if view == "lancar":
        ref = date.today()
        modo = "dia"
        dias_list = [ref]
    elif modo == "dia":
        dias_list = [ref]
    else:
        semana_inicio = ref - timedelta(days=ref.weekday())
        dias_list = [semana_inicio + timedelta(days=i) for i in range(7)]

    # Nomes de cooperados desativados/excluídos também são bloqueados quando
    # existirem em linhas antigas de escala sem cooperado_id.
    _inactive_name_rows = (
        db.session.query(Cooperado.nome)
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Usuario.ativo.is_(False))
        .all()
    )
    _inactive_coop_names = {
        _norm(nome)
        for (nome,) in _inactive_name_rows
        if (nome or "").strip()
    }

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
        .order_by(Escala.id.desc())
        .limit(180 if view == "lancar" else 350)
        .all()
    )
    # Compatibilidade legada sem varrer centenas de escalas de outros contratos.
    # Filtra no próprio banco apenas nomes compatíveis com este estabelecimento.
    _rest_name_sql = (rest.nome or "").strip().lower()
    legacy_scales = (
        db.session.query(Escala)
        .outerjoin(Cooperado, Escala.cooperado_id == Cooperado.id)
        .outerjoin(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(
            Escala.restaurante_id.is_(None),
            Escala.contrato.isnot(None),
            or_(Escala.cooperado_id.is_(None), Usuario.ativo.is_(True)),
            or_(
                func.lower(func.trim(Escala.contrato)) == _rest_name_sql,
                func.lower(func.replace(func.trim(Escala.contrato), "_", " ")) == _rest_name_sql,
                func.lower(func.trim(Escala.contrato)).contains(_rest_name_sql),
            ),
        )
        .order_by(Escala.id.desc())
        .limit(80 if view == "lancar" else 180)
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
                .options(defer(Cooperado.foto_bytes))
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

            # Se a linha antiga não tem ID, não deixa reaparecer no painel do
            # estabelecimento um nome pertencente a cooperado inativo/excluído.
            if not coop and nome_fallback and _norm(nome_fallback) in _inactive_coop_names:
                continue

            # Se existe cooperado_id mas ele não foi encontrado no mapa de ativos,
            # trata como inativo/excluído e não exibe operacionalmente.
            if e.cooperado_id and not coop:
                continue

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
                "escala_id": e.id,
                "escala": e,
            })

            break

    for d in dias_list:
        agenda[d].sort(
            key=lambda x: (
                (x["contrato"] or "").lower(),
                (x.get("nome_planilha") or (x["coop"].nome if x["coop"] else "")).lower(),
            )
        )

    try:
        current_app.logger.info("REST_PORTAL_SCALES %.3fs view=%s rest_id=%s", _time.perf_counter()-_portal_perf_started, view, rest.id)
    except Exception:
        pass

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

    # todos ativos, para busca manual no lançamento
    cooperados_ativos = (
        Cooperado.query
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Usuario.ativo.is_(True))
        .options(defer(Cooperado.foto_bytes))
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
    try:
        current_app.logger.info("REST_PORTAL_COOPS %.3fs view=%s rest_id=%s", _time.perf_counter()-_portal_perf_started, view, rest.id)
    except Exception:
        pass

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
                Lancamento.data == date.today(),
            )
            .order_by(Lancamento.data.desc(), Lancamento.id.desc())
            .limit(80)
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

    # Reaproveita a lista ativa já carregada acima com foto_bytes adiada.
    # Antes esta segunda consulta materializava novamente todos os cooperados,
    # inclusive fotos em bytea, deixando o login do estabelecimento muito pesado.
    cooperados_busca_manual = cooperados_ativos

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

    if view == "lancar" and "lancamentos_periodo_all" in locals():
        lancamentos_hoje_rest = lancamentos_periodo_all
    else:
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


    # -------------------- FALTAS DE LANÇAMENTO NO HISTÓRICO --------------------
    # Se havia escala no período filtrado e nenhum lançamento compatível com
    # aquele horário, mantém a pessoa visível no histórico como "FALTOU LANÇAR".
    if view == "lancamentos":
        def _hm_to_min(v):
            m = re.search(r"(\d{1,2}):(\d{2})", str(v or ""))
            if not m:
                return None
            return int(m.group(1)) * 60 + int(m.group(2))

        def _interval_overlap(a_ini, a_fim, b_ini, b_fim):
            ai, af = _hm_to_min(a_ini), _hm_to_min(a_fim)
            bi, bf = _hm_to_min(b_ini), _hm_to_min(b_fim)
            if None in (ai, af, bi, bf):
                return False
            return max(ai, bi) < min(af, bf) or (ai == bi and af == bf)

        hist_launches = {}
        for x in lancamentos_periodo:
            d = x.get("data")
            if isinstance(d, str):
                try:
                    d = datetime.strptime(d, "%d/%m/%Y").date()
                except Exception:
                    d = None
            hist_launches.setdefault((int(x.get("cooperado_id") or 0), d), []).append(x)

        faltas = []
        faltas_seen = set()
        for scale in escalas_rest:
            scale_day = _parse_data_escala_str(scale.data)
            if not scale_day or scale_day < di or scale_day > df:
                continue

            coop = coops_escala_map.get(scale.cooperado_id) if scale.cooperado_id else None
            if not coop:
                continue

            s_ini, s_fim = _extrair_inicio_fim_intervalo(scale.horario)
            # converte minutos para HH:MM para comparação com lançamento
            s_ini_txt = f"{s_ini//60:02d}:{s_ini%60:02d}" if s_ini is not None else ""
            s_fim_txt = f"{s_fim//60:02d}:{s_fim%60:02d}" if s_fim is not None else ""

            launches = hist_launches.get((coop.id, scale_day), [])
            matched = False
            for x in launches:
                l_ini = x.get("hora_inicio") or ""
                l_fim = x.get("hora_fim") or ""
                if s_ini_txt and s_fim_txt and l_ini and l_fim:
                    if _interval_overlap(s_ini_txt, s_fim_txt, l_ini, l_fim):
                        matched = True
                        break
                elif len(launches) == 1:
                    matched = True
                    break

            k = (coop.id, scale_day, s_ini_txt, s_fim_txt)
            if matched or k in faltas_seen:
                continue
            faltas_seen.add(k)
            faltas.append({
                "id": None,
                "data": scale_day.strftime("%d/%m/%Y"),
                "hora_inicio": s_ini_txt,
                "hora_fim": s_fim_txt,
                "qtd_entregas": 0,
                "valor": 0.0,
                "descricao": "FALTOU LANÇAR",
                "cooperado_id": coop.id,
                "cooperado_nome": coop.nome,
                "contrato_nome": rest.nome,
                "faltou_lancar": True,
            })

        if faltas:
            lancamentos_periodo.extend(faltas)
            def _hist_sort_key(x):
                raw = x.get("data")
                if isinstance(raw, date):
                    d = raw
                else:
                    try:
                        d = datetime.strptime(str(raw or ""), "%d/%m/%Y").date()
                    except Exception:
                        d = date.min
                return (d, x.get("hora_inicio") or "", (x.get("cooperado_nome") or "").lower())
            lancamentos_periodo.sort(key=_hist_sort_key)

    # -------------------- PRODUÇÕES DA SEMANA --------------------
    # Usa a MESMA fonte de escala do painel "Escalados" e das pendências.
    # Isso evita divergência: se existe escala válida no dia, ela aparece aqui.
    producoes_semana_previstas = []
    producoes_semana_pendentes = []
    producoes_semana_recentes = []

    if view == "producoes":
        from types import SimpleNamespace
        import production_scale_backend as production_backend

        today_local = datetime.now(TZ).date() if "TZ" in globals() else date.today()
        week_start = today_local - timedelta(days=today_local.weekday())
        week_end = week_start + timedelta(days=6)

        # FONTE ÚNICA: usa exatamente "escalas_rest", já resolvida acima
        # pelo mesmo portal (restaurante_id + compatibilidade de contrato).
        # Se a aba Escala enxerga a linha, Produções da Semana também enxerga.
        week_scales = []
        seen_scale_ids = set()
        for scale in escalas_rest:
            if scale.id in seen_scale_ids:
                continue

            raw_day = _parse_data_escala_str(scale.data)
            weekday_num = _weekday_from_data_str(scale.data)

            # Escalas da semana atual usam a data real.
            if raw_day and week_start <= raw_day <= week_end:
                scale_day = raw_day
            # Compatibilidade com escalas recorrentes/antigas: projeta o dia
            # da semana para a semana atual.
            elif weekday_num in (1, 2, 3, 4, 5, 6, 7):
                scale_day = week_start + timedelta(days=int(weekday_num) - 1)
            else:
                scale_day = None

            if not scale_day:
                continue

            seen_scale_ids.add(scale.id)
            week_scales.append((scale, scale_day))

        try:
            current_app.logger.info(
                "REST_WEEK_PRODUCTIONS rest_id=%s rest=%s escalas_rest=%s week_scales=%s week=%s..%s",
                rest.id, rest.nome, len(escalas_rest), len(week_scales), week_start, week_end
            )
        except Exception:
            pass

        coop_ids = {s.cooperado_id for s, _ in week_scales if s.cooperado_id}
        week_coops = {}
        if coop_ids:
            week_coops = {
                x.id: x
                for x in (
                    Cooperado.query
                    .filter(Cooperado.id.in_(coop_ids))
                    .options(defer(Cooperado.foto_bytes))
                    .all()
                )
            }

        # fallback para escalas antigas que ainda guardam somente o nome
        coop_name_map = {
            _norm(x.nome): x
            for x in cooperados_ativos
            if _norm(x.nome)
        }

        week_launches = (
            Lancamento.query
            .filter(
                Lancamento.restaurante_id == rest.id,
                Lancamento.data >= week_start,
                Lancamento.data <= week_end,
            )
            .order_by(Lancamento.id.desc())
            .all()
        )
        launches_by_day = {}
        for lanc in week_launches:
            launches_by_day.setdefault((lanc.cooperado_id, lanc.data), []).append(lanc)

        week_productions = (
            production_backend.production_backend.ProducaoCooperado.query
            .filter(
                production_backend.ProducaoCooperado.restaurante_id == rest.id,
                production_backend.ProducaoCooperado.data >= week_start,
                production_backend.ProducaoCooperado.data <= week_end,
            )
            .order_by(production_backend.ProducaoCooperado.id.desc())
            .all()
        )
        prod_by_scale = {p.escala_id: p for p in week_productions if p.escala_id}

        now_local = datetime.now(TZ) if "TZ" in globals() else datetime.now()

        for scale, scale_day in week_scales:
            coop = week_coops.get(scale.cooperado_id) if scale.cooperado_id else None
            if not coop and (scale.cooperado_nome or "").strip():
                coop = coop_name_map.get(_norm(scale.cooperado_nome))
            if not coop:
                # Mesmo sem cadastro resolvido, mantém a escala visível.
                coop = SimpleNamespace(
                    id=0,
                    nome=(scale.cooperado_nome or "Cooperado não vinculado").strip()
                )

            start_time, end_time = production_backend.upgrade._times_from_text(scale.horario)
            start_time = production_backend.upgrade._norm_time(start_time) or ""
            end_time = production_backend.upgrade._norm_time(end_time) or ""
            end_at = production_backend.flow._end_at(scale_day, start_time, end_time)
            finished = bool(end_at and now_local >= end_at)

            production = prod_by_scale.get(scale.id)
            launch = None
            if getattr(coop, "id", 0):
                for cand in launches_by_day.get((coop.id, scale_day), []):
                    if production_backend.upgrade._overlap(
                        cand.hora_inicio,
                        cand.hora_fim,
                        start_time,
                        end_time,
                    ):
                        launch = cand
                        break

            total = float(
                (launch.valor if launch else None)
                or (production.valor_total if production else 0)
                or 0
            )

            if launch or (production and production.status == "aprovada"):
                color, label = "green", "Lançada"
            elif production and production.status == "pendente" and total > 0:
                color, label = "green", "Informada pelo cooperado · confira"
            elif production and production.status == "recusada":
                color, label = "red", "Recusada · lançar pelo estabelecimento"
            elif finished:
                color, label = "red", "Pendente"
            else:
                color, label = "blue", "Prevista"

            can_launch = bool(
                getattr(coop, "id", 0)
                and not launch
                and not (production and production.status == "aprovada")
                and not (production and production.status == "pendente" and total > 0)
            )

            row = SimpleNamespace(
                escala=scale,
                cooperado=coop,
                data=scale_day,
                inicio=start_time,
                fim=end_time,
                fim_em=end_at,
                finalizada=finished,
                producao=production,
                lancamento=launch,
                valor_total=total,
                color=color,
                status_label=label,
                pode_lancar=can_launch,
                turno=scale.turno or "",
                horario=scale.horario or "",
                contrato=(eff_map.get(scale.id, scale.contrato or "") or "").strip(),
            )
            producoes_semana_previstas.append(row)

            if production and production.status == "pendente" and float(production.valor_total or 0) > 0:
                producoes_semana_pendentes.append(production)

        producoes_semana_previstas.sort(
            key=lambda row: (
                row.data or date.max,
                row.inicio or "",
                (row.cooperado.nome or "").lower(),
            )
        )

        producoes_semana_recentes = (
            production_backend.production_backend.ProducaoCooperado.query
            .filter(
                production_backend.ProducaoCooperado.restaurante_id == rest.id,
                production_backend.ProducaoCooperado.status.in_(["aprovada", "recusada"]),
            )
            .order_by(production_backend.ProducaoCooperado.decidido_em.desc(), production_backend.ProducaoCooperado.id.desc())
            .limit(30)
            .all()
        )

    # ---- URLs/flags para template
    from werkzeug.routing import BuildError
    try:
        url_lancar_producao = url_for("lancar_producao")
    except BuildError:
        url_lancar_producao = "/restaurante/lancar_producao"

    has_editar_lanc = ("editar_lancamento" in app.view_functions)

    # -------------------- Render --------------------
    try:
        current_app.logger.info("REST_PORTAL_BUILD %.3fs view=%s rest_id=%s", _time.perf_counter()-_portal_perf_started, view, rest.id)
    except Exception:
        pass
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
        producoes_semana_previstas=producoes_semana_previstas,
        producoes_semana_pendentes=producoes_semana_pendentes,
        producoes_semana_recentes=producoes_semana_recentes,
        hoje=hoje,
    )

