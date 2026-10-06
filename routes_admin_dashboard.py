"""Rotas principais do Admin Dashboard.

Extraídas do app.py sem alterar endpoints, permissões ou regras.
"""
from __future__ import annotations

import app as legacy

for _name, _value in vars(legacy).items():
    if not _name.startswith("__"):
        globals().setdefault(_name, _value)

# =========================
# Admin Dashboard
# =========================

from flask import jsonify, request, render_template, session, flash, redirect, url_for
from sqlalchemy import func, inspect, or_
from sqlalchemy.exc import SQLAlchemyError, OperationalError, ProgrammingError
from datetime import date, timedelta
from collections import defaultdict, namedtuple
import re


@app.post("/admin/cooperados/<int:id>/toggle-status")
@admin_perm_required("cooperados", "editar")
def toggle_status_cooperado(id):
    """
    Alterna o status 'ativo' do usuário vinculado ao cooperado.

    Observação crítica:
    - Se o campo/coluna 'ativo' ainda não existir no MODEL/DB, retorna erro orientando migração.
    """
    try:
        coop = db.session.get(Cooperado, id)
        if not coop or not getattr(coop, "usuario_ref", None):
            return jsonify(ok=False, error="Cooperado não encontrado"), 404

        user = coop.usuario_ref

        if not hasattr(user, "ativo"):
            return jsonify(
                ok=False,
                error="Campo 'ativo' ausente no modelo. Atualize o models.py (Usuario.ativo) e faça deploy."
            ), 500

        try:
            insp = inspect(db.engine)
            table = getattr(Usuario, "__tablename__", None)

            if not table:
                return jsonify(
                    ok=False,
                    error="Não foi possível identificar a tabela do modelo Usuario."
                ), 500

            cols = {c["name"] for c in insp.get_columns(table)}
            if "ativo" not in cols:
                return jsonify(
                    ok=False,
                    error=(
                        "Coluna 'ativo' ausente no banco. Faça a migração/ALTER TABLE em produção "
                        f"(tabela: {table})."
                    ),
                    table=table
                ), 409
        except Exception:
            pass

        atual = bool(getattr(user, "ativo", True))
        user.ativo = not atual

        db.session.commit()
        return jsonify(ok=True, ativo=bool(user.ativo))

    except (OperationalError, ProgrammingError):
        db.session.rollback()
        return jsonify(
            ok=False,
            error="Falha ao salvar: provável falta da coluna 'ativo' no banco. Faça migração/ALTER TABLE."
        ), 409
    except SQLAlchemyError:
        db.session.rollback()
        return jsonify(ok=False, error="Falha ao salvar no banco"), 500

@app.route("/admin/admins/<int:usuario_id>/toggle-status", methods=["POST"])
@admin_required
def admin_toggle_admin_status(usuario_id):
    if not is_admin_master():
        flash("Apenas o administrador master pode alterar o status de administradores.", "danger")
        return redirect(url_for("admin_dashboard", tab="config"))

    admin = Usuario.query.filter_by(id=usuario_id, tipo="admin").first_or_404()

    if admin.is_master:
        flash("O administrador master não pode ser desativado por esta tela.", "warning")
        return redirect(url_for("admin_dashboard", tab="config"))

    admin.ativo = not bool(admin.ativo)
    db.session.commit()

    if admin.ativo:
        flash("Administrador ativado com sucesso.", "success")
    else:
        flash("Administrador desativado com sucesso.", "success")

    return redirect(url_for("admin_dashboard", tab="config"))

@app.route("/admin/admins/<int:usuario_id>/delete", methods=["POST"])
@admin_perm_required("config", "editar")
def admin_delete_admin(usuario_id):
    if not is_admin_master():
        flash("Apenas o administrador master pode excluir administradores.", "danger")
        return redirect(url_for("admin_dashboard", tab="config"))

    admin = Usuario.query.filter_by(id=usuario_id, tipo="admin").first_or_404()

    if admin.is_master:
        flash("O administrador master não pode ser excluído.", "warning")
        return redirect(url_for("admin_dashboard", tab="config"))

    AdminPermissao.query.filter_by(usuario_id=admin.id).delete()
    db.session.delete(admin)
    db.session.commit()

    flash("Administrador excluído com sucesso.", "success")
    return redirect(url_for("admin_dashboard", tab="config"))
    

@app.get("/admin/sistemas/abrir/<sistema>")
@admin_perm_required("sistemas", "ver")
def admin_sistemas_abrir(sistema):
    sistema = (sistema or "").strip().lower()
    if sistema == "sistema1":
        return redirect(_build_remote_sso_url(PORTAL_SISTEMA1_URL, aud="sistema1", role="master", next_path="/admin"))
    if sistema == "sistema2":
        return redirect(_build_remote_sso_url(PORTAL_SISTEMA2_URL, aud="sistema2", role="admin", next_path="/dashboard"))
    flash("Sistema inválido.", "warning")
    return redirect(url_for("admin_dashboard", tab="sistemas"))

@app.route("/admin", methods=["GET"])
@admin_required
def admin_dashboard():
    args = request.args
    active_tab = (args.get("tab") or "lancamentos").strip().lower()

    admin_logado = _usuario_logado()
    if not admin_logado:
        session.clear()
        flash("Sessão inválida. Faça login novamente.", "danger")
        return redirect(url_for("login"))

    if (admin_logado.tipo or "").strip().lower() != "admin":
        session.clear()
        flash("Acesso restrito ao administrador.", "danger")
        return redirect(url_for("login"))

    if active_tab not in ADMIN_ABAS:
        active_tab = "lancamentos" if "lancamentos" in ADMIN_ABAS else "resumo"

    # monta o mapa de permissões logo no início
    if getattr(admin_logado, "is_master", False):
        admin_perms = {
            aba: {
                "ver": True,
                "criar": True,
                "editar": True,
                "excluir": True,
            }
            for aba in ADMIN_ABAS.keys()
        }
    else:
        admin_perms = get_admin_permissions_map(admin_logado.id)

        abas_liberadas = [
            aba
            for aba in ADMIN_ABAS.keys()
            if admin_perms.get(aba, {}).get("ver", False)
        ]
        aba_preferida = "lancamentos" if "lancamentos" in abas_liberadas else ("resumo" if "resumo" in abas_liberadas else abas_liberadas[0] if abas_liberadas else "resumo")

        if not abas_liberadas:
            session.clear()
            flash("Seu usuário admin está sem permissões liberadas. Fale com o administrador master.", "danger")
            return redirect(url_for("login"))

        # config sempre restrita ao master
        if active_tab == "config":
            flash("A aba de configurações é restrita ao administrador master.", "danger")
            return redirect(url_for("admin_dashboard", tab=aba_preferida))

        # se tentar abrir aba sem permissão, redireciona
        if active_tab not in abas_liberadas:
            flash("Você não tem permissão para acessar essa aba.", "warning")
            return redirect(url_for("admin_dashboard", tab=aba_preferida))

    def _pick_date(*keys):
        for k in keys:
            v = args.get(k)
            if v:
                d = _parse_date(v)
                if d:
                    return d
        return None

    data_inicio = _pick_date("resumo_inicio", "data_inicio")
    data_fim = _pick_date("resumo_fim", "data_fim")

    filtro_periodo_aplicado = bool(data_inicio or data_fim)

    if data_inicio and not data_fim:
        data_fim = data_inicio
    elif data_fim and not data_inicio:
        data_inicio = data_fim
    elif not data_inicio and not data_fim:
        hoje_ref = date.today()
        if active_tab == 'receitas':
            data_inicio = date(hoje_ref.year, hoje_ref.month, 1)
            data_fim = (data_inicio + relativedelta(months=1)) - timedelta(days=1)
        else:
            data_inicio = hoje_ref - timedelta(days=hoje_ref.weekday())
            data_fim = data_inicio + timedelta(days=6)

    def _active_coop_ids_finance():
        q = (
            db.session.query(Cooperado.id)
            .join(Usuario, Cooperado.usuario_id == Usuario.id)
            .filter(
                Usuario.tipo == "cooperado",
                or_(Usuario.ativo.is_(True), Usuario.ativo.is_(None)),
            )
        )
        ids = {int(x[0]) for x in q.all()}
        archived_ids = set()
        try:
            rows_arch = db.session.execute(text("SELECT cooperado_id FROM cooperados_arquivados_v8")).all()
            archived_ids = {int(x[0]) for x in rows_arch if x and x[0] is not None}
        except Exception:
            db.session.rollback()
        return ids - archived_ids

    active_finance_ids = _active_coop_ids_finance()

    restaurante_id = args.get("restaurante_id", type=int)
    cooperado_id = args.get("cooperado_id", type=int)
    considerar_periodo = bool(args.get("considerar_periodo"))
    dows = set(args.getlist("dow"))

    # ==========================================================
    # AJAX RÁPIDO DE FILTROS
    # ==========================================================
    # Aqui a tela não monta o admin inteiro. Cada filtro retorna somente
    # o bloco solicitado. As regras continuam as mesmas; só evita carregar
    # escala, histórico, configuração e outras abas sem necessidade.
    ajax_partial_fast = (request.args.get("ajax_partial") or "").strip().lower()
    if ajax_partial_fast:
        partial_permission = {
            "lancamentos": "lancamentos",
            "receitas": "receitas",
            "despesas": "despesas",
            "coop_receitas": "coop_receitas",
            "coop_despesas": "coop_despesas",
            "beneficios": "beneficios",
        }.get(ajax_partial_fast)
        resumo_liberado = any(
            admin_has_perm(aba, "ver")
            for aba in (
                "lancamentos", "receitas", "despesas",
                "coop_receitas", "coop_despesas", "beneficios",
            )
        )
        if (
            (ajax_partial_fast == "resumo" and not resumo_liberado)
            or (partial_permission and not admin_has_perm(partial_permission, "ver"))
        ):
            return jsonify({"ok": False, "message": "Você não tem permissão para acessar esta área."}), 403

    if ajax_partial_fast in {"resumo", "lancamentos", "receitas", "despesas", "coop_receitas", "coop_despesas"}:
        cfg_fast = get_config()
        restaurantes_fast = Restaurante.query.order_by(Restaurante.nome).all()
        cooperados_fast = (
            Cooperado.query
            .join(Usuario, Cooperado.usuario_id == Usuario.id)
            .filter(Usuario.tipo == "cooperado", or_(Usuario.ativo.is_(True), Usuario.ativo.is_(None)))
            .order_by(Cooperado.nome.asc())
            .all()
        )

        ctx_fast = dict(
            tab=ajax_partial_fast,
            fast_mode=True,
            restaurantes=restaurantes_fast,
            cooperados=cooperados_fast,
            admin_perms=admin_perms,
            admin_is_master=is_admin_master(),
            filtro_periodo_aplicado=filtro_periodo_aplicado,
            current_date=date.today(),
            data_limite=date(date.today().year, 12, 31),
            salario_minimo=(cfg_fast.salario_minimo or 0.0) if cfg_fast else 0.0,
            bloquear_adiantamento=bool(getattr(cfg_fast, "bloquear_adiantamento", False)) if cfg_fast else False,
            status_adiantamento_label=_status_adiantamento_label,
            status_adiantamento_badge=_status_adiantamento_badge,
            competencia_humana=_competencia_humana,
        )

        if ajax_partial_fast == "resumo":
            # O resumo precisa de totais, não dos objetos Lancamento completos.
            # Agrupa no banco por cooperado/dia e reduz milhares de linhas a poucas dezenas.
            qprod = db.session.query(
                Lancamento.cooperado_id,
                Lancamento.data,
                func.coalesce(func.sum(Lancamento.valor), 0.0).label("valor_total"),
            )
            if restaurante_id:
                qprod = qprod.filter(Lancamento.restaurante_id == restaurante_id)
            if cooperado_id:
                qprod = qprod.filter(Lancamento.cooperado_id == cooperado_id)
            if data_inicio:
                qprod = qprod.filter(Lancamento.data >= data_inicio)
            if data_fim:
                qprod = qprod.filter(Lancamento.data <= data_fim)
            prod_rows = (
                qprod.group_by(Lancamento.cooperado_id, Lancamento.data)
                .order_by(Lancamento.data.asc())
                .all()
            )

            permitidos = None
            if considerar_periodo and restaurante_id:
                rest_fast = db.session.get(Restaurante, restaurante_id)
                if rest_fast:
                    mapa = {
                        "seg-dom": {"1", "2", "3", "4", "5", "6", "7"},
                        "sab-sex": {"6", "7", "1", "2", "3", "4", "5"},
                        "sex-qui": {"5", "6", "7", "1", "2", "3", "4"},
                    }
                    permitidos = mapa.get(rest_fast.periodo, {"1", "2", "3", "4", "5", "6", "7"})

            prod_by_coop = defaultdict(float)
            chart_sums = defaultdict(float)
            for cid, data_ref, valor_total in prod_rows:
                if not data_ref:
                    continue
                dia = _dow(data_ref)
                if dows and dia not in dows:
                    continue
                if permitidos and dia not in permitidos:
                    continue
                valor = float(valor_total or 0.0)
                prod_by_coop[cid] += valor
                chart_sums[data_ref.strftime("%Y-%m")] += valor

            rqcf = db.session.query(
                ReceitaCooperado.cooperado_id,
                func.coalesce(func.sum(ReceitaCooperado.valor), 0.0).label("valor_total"),
            ).filter(ReceitaCooperado.cooperado_id.in_(active_finance_ids))
            if data_inicio:
                rqcf = rqcf.filter(ReceitaCooperado.data >= data_inicio)
            if data_fim:
                rqcf = rqcf.filter(ReceitaCooperado.data <= data_fim)
            if cooperado_id:
                rqcf = rqcf.filter(ReceitaCooperado.cooperado_id == cooperado_id)
            receitas_coop_resumo = rqcf.group_by(ReceitaCooperado.cooperado_id).all()
            rec_by_coop = defaultdict(float)
            for cid, valor_total in receitas_coop_resumo:
                rec_by_coop[cid] += float(valor_total or 0.0)

            # Limita os snapshots aos cooperados realmente envolvidos no período/dívida.
            ids_relevantes = set(k for k in prod_by_coop.keys() if k) | set(k for k in rec_by_coop.keys() if k)
            dq_ids = DespesaCooperado.query.with_entities(DespesaCooperado.cooperado_id).filter(DespesaCooperado.cooperado_id.in_(active_finance_ids))
            if cooperado_id:
                dq_ids = dq_ids.filter(DespesaCooperado.cooperado_id == cooperado_id)
            elif data_fim:
                dq_ids = dq_ids.filter(DespesaCooperado.data_inicio <= data_fim)
            for (_cid,) in dq_ids.distinct().all():
                if _cid:
                    ids_relevantes.add(_cid)
            if cooperado_id:
                ids_relevantes = {cooperado_id}

            resumo_coop_rows_fast = []
            resumo_totais_fast = {
                "prod": 0.0, "inss4": 0.0, "sest05": 0.0, "rec": 0.0,
                "des": 0.0, "adiant": 0.0, "a_receber": 0.0, "saldo_pendente": 0.0,
                "pend_programado": 0.0
            }
            coop_map_fast = {c.id: c for c in cooperados_fast}
            for _cid in sorted(ids_relevantes, key=lambda x: (coop_map_fast.get(x).nome if coop_map_fast.get(x) else str(x)).lower()):
                coop = coop_map_fast.get(_cid)
                if not coop:
                    continue
                snap = _compute_coop_debt_snapshot(_cid, data_inicio, data_fim)
                prod = round(prod_by_coop.get(_cid, 0.0), 2)
                rec = round(rec_by_coop.get(_cid, 0.0), 2)
                inss4 = round(prod * INSS_ALIQ, 2)
                sest05 = round(prod * SEST_ALIQ, 2)
                des = round(snap.get("descontado_periodo_despesa", 0.0), 2)
                adiant = round(snap.get("descontado_periodo_adiant", 0.0), 2)
                a_receber = round(max(0.0, snap.get("disponivel_auto_restante", 0.0)), 2)
                saldo_pendente = round(snap.get("saldo_devedor", 0.0), 2)
                pend_programado = round(snap.get("a_descontar", 0.0), 2)
                if prod or rec or des or adiant or a_receber or saldo_pendente or pend_programado:
                    resumo_coop_rows_fast.append({
                        "id": coop.id,
                        "nome": coop.nome,
                        "prod": prod,
                        "inss4": inss4,
                        "sest05": sest05,
                        "rec": rec,
                        "des": des,
                        "adiant": adiant,
                        "a_receber": a_receber,
                        "aReceber": a_receber,
                        "saldo_pendente": saldo_pendente,
                        "saldoPendente": saldo_pendente,
                        "pend_programado": pend_programado,
                        "pendProgramado": pend_programado,
                    })
                    resumo_totais_fast["prod"] += prod
                    resumo_totais_fast["inss4"] += inss4
                    resumo_totais_fast["sest05"] += sest05
                    resumo_totais_fast["rec"] += rec
                    resumo_totais_fast["des"] += des
                    resumo_totais_fast["adiant"] += adiant
                    resumo_totais_fast["a_receber"] += a_receber
                    resumo_totais_fast["saldo_pendente"] += saldo_pendente
                    resumo_totais_fast["pend_programado"] += pend_programado

            labels_ord = sorted(chart_sums.keys())
            chart_fast = {
                "labels": [f"{k.split('-')[1]}/{k.split('-')[0][-2:]}" for k in labels_ord],
                "values": [round(chart_sums[k], 2) for k in labels_ord],
            }
            total_prod_fast = resumo_totais_fast["prod"]
            ctx_fast.update(
                lancamentos=[],
                receitas=[],
                despesas=[],
                receitas_coop=[],
                despesas_coop=[],
                resumo_coop_rows=resumo_coop_rows_fast,
                resumo_totais=resumo_totais_fast,
                total_producoes=round(total_prod_fast, 2),
                total_inss=round(resumo_totais_fast["inss4"], 2),
                total_sest=round(resumo_totais_fast["sest05"], 2),
                total_encargos=round(resumo_totais_fast["inss4"] + resumo_totais_fast["sest05"], 2),
                total_receitas=0.0,
                total_despesas=0.0,
                total_receitas_coop=round(sum(rec_by_coop.values()), 2),
                total_despesas_coop=round(resumo_totais_fast["des"], 2),
                total_adiantamentos_coop=round(resumo_totais_fast["adiant"], 2),
                chart_data_lancamentos_coop=chart_fast,
                chart_data_lancamentos_cooperados=chart_fast,
                taxa_admin_rows=[],
                taxa_admin_totais={},
                juros_arrecadados_total=0.0,
                despesa_snapshot_map={},
                solicitacoes_adiantamento=[],
                adiantamento_status_map={},
                beneficios_view=[],
                historico_beneficios=[],
            )
            return _render_admin_dashboard_partial("resumo", **ctx_fast)

        if ajax_partial_fast == "lancamentos":
            qf = Lancamento.query
            if restaurante_id:
                qf = qf.filter(Lancamento.restaurante_id == restaurante_id)
            if cooperado_id:
                qf = qf.filter(Lancamento.cooperado_id == cooperado_id)
            if data_inicio:
                qf = qf.filter(Lancamento.data >= data_inicio)
            if data_fim:
                qf = qf.filter(Lancamento.data <= data_fim)
            lancamentos_fast = qf.order_by(Lancamento.data.desc(), Lancamento.id.desc()).all()
            if dows:
                lancamentos_fast = [l for l in lancamentos_fast if l.data and _dow(l.data) in dows]
            if considerar_periodo and restaurante_id:
                rest_fast = Restaurante.query.get(restaurante_id)
                if rest_fast:
                    mapa = {
                        "seg-dom": {"1", "2", "3", "4", "5", "6", "7"},
                        "sab-sex": {"6", "7", "1", "2", "3", "4", "5"},
                        "sex-qui": {"5", "6", "7", "1", "2", "3", "4"},
                    }
                    permitidos = mapa.get(rest_fast.periodo, {"1", "2", "3", "4", "5", "6", "7"})
                    lancamentos_fast = [l for l in lancamentos_fast if l.data and _dow(l.data) in permitidos]
            total_prod_fast = sum((l.valor or 0.0) for l in lancamentos_fast)
            ctx_fast.update(
                lancamentos=lancamentos_fast,
                total_producoes=total_prod_fast,
                total_inss=round(total_prod_fast * INSS_ALIQ, 2),
                total_sest=round(total_prod_fast * SEST_ALIQ, 2),
                total_encargos=round(total_prod_fast * (INSS_ALIQ + SEST_ALIQ), 2),
            )
            return _render_admin_dashboard_partial("lancamentos", **ctx_fast)

        if ajax_partial_fast == "receitas":
            rqf = ReceitaCooperativa.query
            if data_inicio:
                rqf = rqf.filter(ReceitaCooperativa.data >= data_inicio)
            if data_fim:
                rqf = rqf.filter(ReceitaCooperativa.data <= data_fim)
            receitas_fast = rqf.order_by(ReceitaCooperativa.data.desc().nullslast(), ReceitaCooperativa.id.desc()).all()
            taxa_rows_fast, taxa_totais_fast = _build_taxa_admin_rows(receitas_fast)
            ctx_fast.update(
                receitas=receitas_fast,
                taxa_admin_rows=taxa_rows_fast,
                taxa_admin_totais=taxa_totais_fast,
                juros_arrecadados_total=round(sum((r['valor_multa'] + r['valor_juros']) for r in taxa_rows_fast if r['status'] == 'pago'), 2),
                total_receitas=sum(_receita_total_real(r) for r in receitas_fast),
            )
            return _render_admin_dashboard_partial("receitas", **ctx_fast)

        if ajax_partial_fast == "despesas":
            dqf = DespesaCooperativa.query
            if data_inicio:
                dqf = dqf.filter(DespesaCooperativa.data >= data_inicio)
            if data_fim:
                dqf = dqf.filter(DespesaCooperativa.data <= data_fim)
            despesas_fast = dqf.order_by(DespesaCooperativa.data.desc(), DespesaCooperativa.id.desc()).all()
            ctx_fast.update(despesas=despesas_fast, total_despesas=sum((d.valor or 0.0) for d in despesas_fast))
            return _render_admin_dashboard_partial("despesas", **ctx_fast)

        if ajax_partial_fast == "coop_receitas":
            rqcf = ReceitaCooperado.query.filter(ReceitaCooperado.cooperado_id.in_(active_finance_ids))
            if data_inicio:
                rqcf = rqcf.filter(ReceitaCooperado.data >= data_inicio)
            if data_fim:
                rqcf = rqcf.filter(ReceitaCooperado.data <= data_fim)
            if cooperado_id:
                rqcf = rqcf.filter(ReceitaCooperado.cooperado_id == cooperado_id)
            receitas_coop_fast = rqcf.order_by(ReceitaCooperado.data.desc(), ReceitaCooperado.id.desc()).all()
            ctx_fast.update(receitas_coop=receitas_coop_fast, total_receitas_coop=sum((r.valor or 0.0) for r in receitas_coop_fast))
            return _render_admin_dashboard_partial("coop_receitas", **ctx_fast)

        if ajax_partial_fast == "coop_despesas":
            dqcf = DespesaCooperado.query.filter(DespesaCooperado.cooperado_id.in_(active_finance_ids))
            if data_inicio and data_fim:
                dqcf = dqcf.filter(DespesaCooperado.data_inicio <= data_fim, DespesaCooperado.data_fim >= data_inicio)
            elif data_inicio:
                dqcf = dqcf.filter(DespesaCooperado.data_fim >= data_inicio)
            elif data_fim:
                dqcf = dqcf.filter(DespesaCooperado.data_inicio <= data_fim)
            if cooperado_id:
                dqcf = dqcf.filter(DespesaCooperado.cooperado_id == cooperado_id)
            despesas_coop_fast = dqcf.order_by(DespesaCooperado.data_fim.desc().nullslast(), DespesaCooperado.id.desc()).all()
            despesa_snapshot_map_fast = {}
            for _cid in {getattr(d, "cooperado_id", None) for d in despesas_coop_fast if getattr(d, "cooperado_id", None)}:
                _snap = _compute_coop_debt_snapshot(_cid, data_inicio, data_fim)
                for _it in _snap.get("itens", []):
                    despesa_snapshot_map_fast[_it["id"]] = _it
            adiantamentos_q_fast = SolicitacaoAdiantamento.query.join(Cooperado, SolicitacaoAdiantamento.cooperado_id == Cooperado.id).filter(SolicitacaoAdiantamento.cooperado_id.in_(active_finance_ids))
            if cooperado_id:
                adiantamentos_q_fast = adiantamentos_q_fast.filter(SolicitacaoAdiantamento.cooperado_id == cooperado_id)
            solicitacoes_fast = adiantamentos_q_fast.order_by(SolicitacaoAdiantamento.pedido_em.desc(), SolicitacaoAdiantamento.id.desc()).all()
            ctx_fast.update(
                despesas_coop=despesas_coop_fast,
                despesa_snapshot_map=despesa_snapshot_map_fast,
                solicitacoes_adiantamento=solicitacoes_fast,
                adiantamento_status_map={s.despesa_cooperado_id: s for s in solicitacoes_fast if s.despesa_cooperado_id},
                total_despesas_coop=sum((d.valor or 0.0) for d in despesas_coop_fast if not getattr(d, "eh_adiantamento", False)),
                total_adiantamentos_coop=sum((d.valor or 0.0) for d in despesas_coop_fast if getattr(d, "eh_adiantamento", False)),
            )
            return _render_admin_dashboard_partial("coop_despesas", **ctx_fast)

    # ==========================================================
    # PRIMEIRO PAINT LEVE DO ADMIN
    # ==========================================================
    # Com o menu usando pré-carregamento em segundo plano, a primeira tela
    # não deve montar resumo, escala, documentos, histórico e benefícios.
    # Isso reduz login/troca de abas. As outras abas são buscadas via AJAX
    # e ficam em cache no navegador.
    if not ajax_partial_fast and active_tab == "lancamentos":
        cfg_light = get_config()
        restaurantes_light = Restaurante.query.order_by(Restaurante.nome).all()
        cooperados_light = (
            Cooperado.query
            .join(Usuario, Cooperado.usuario_id == Usuario.id)
            .filter(Usuario.tipo == "cooperado", or_(Usuario.ativo.is_(True), Usuario.ativo.is_(None)))
            .order_by(Cooperado.nome.asc())
            .all()
        )
        q_light = Lancamento.query
        if restaurante_id:
            q_light = q_light.filter(Lancamento.restaurante_id == restaurante_id)
        if cooperado_id:
            q_light = q_light.filter(Lancamento.cooperado_id == cooperado_id)
        if data_inicio:
            q_light = q_light.filter(Lancamento.data >= data_inicio)
        if data_fim:
            q_light = q_light.filter(Lancamento.data <= data_fim)
        lancamentos_light = q_light.order_by(Lancamento.data.desc(), Lancamento.id.desc()).all()
        if dows:
            lancamentos_light = [l for l in lancamentos_light if l.data and _dow(l.data) in dows]
        if considerar_periodo and restaurante_id:
            rest_light = Restaurante.query.get(restaurante_id)
            if rest_light:
                mapa = {
                    "seg-dom": {"1", "2", "3", "4", "5", "6", "7"},
                    "sab-sex": {"6", "7", "1", "2", "3", "4", "5"},
                    "sex-qui": {"5", "6", "7", "1", "2", "3", "4"},
                }
                permitidos = mapa.get(rest_light.periodo, {"1", "2", "3", "4", "5", "6", "7"})
                lancamentos_light = [l for l in lancamentos_light if l.data and _dow(l.data) in permitidos]
        total_prod_light = sum((l.valor or 0.0) for l in lancamentos_light)
        chart_empty = {"labels": [], "values": []}
        resumo_totais_empty = {
            "prod": 0.0, "inss4": 0.0, "sest05": 0.0, "rec": 0.0,
            "des": 0.0, "adiant": 0.0, "a_receber": 0.0,
            "saldo_pendente": 0.0, "pend_programado": 0.0
        }
        return render_template(
            "admin_dashboard.html",
            fast_mode=True,
            tab=active_tab,
            total_producoes=total_prod_light,
            total_inss=round(total_prod_light * INSS_ALIQ, 2),
            total_sest=round(total_prod_light * SEST_ALIQ, 2),
            total_encargos=round(total_prod_light * (INSS_ALIQ + SEST_ALIQ), 2),
            total_receitas=0.0,
            total_despesas=0.0,
            total_receitas_coop=0.0,
            total_despesas_coop=0.0,
            total_adiantamentos_coop=0.0,
            salario_minimo=(cfg_light.salario_minimo or 0.0) if cfg_light else 0.0,
            bloquear_adiantamento=bool(getattr(cfg_light, "bloquear_adiantamento", False)) if cfg_light else False,
            lancamentos=lancamentos_light,
            receitas=[],
            despesas=[],
            receitas_coop=[],
            despesas_coop=[],
            cooperados=cooperados_light,
            restaurantes=restaurantes_light,
            beneficios_view=[],
            historico_beneficios=[],
            current_date=date.today(),
            data_limite=date(date.today().year, 12, 31),
            admin=admin_logado,
            admin_user=admin_logado,
            docinfo_map={},
            escalas_por_coop=defaultdict(list),
            escalas_por_coop_json={},
            qtd_escalas_map={},
            qtd_escalas_sem_cadastro=0,
            status_doc_por_coop={},
            chart_data_lancamentos_coop=chart_empty,
            chart_data_lancamentos_cooperados=chart_empty,
            folha_inicio=None,
            folha_fim=None,
            folha_por_coop=[],
            trocas_pendentes=[],
            trocas_historico=[],
            trocas_historico_flat=[],
            admin_perms=admin_perms,
            admin_is_master=is_admin_master(),
            ADMIN_ABAS=ADMIN_ABAS,
            admins=[],
            admin_permissions_map={},
            admins_secundarios=[],
            admins_permissoes={},
            filtro_periodo_aplicado=filtro_periodo_aplicado,
            escala_editor_rows=[],
            escala_editor_rows_export=[],
            contratos_escala_opcoes=[],
            escala_hist_inicio=None,
            escala_hist_fim=None,
            trocas_hist_inicio=None,
            trocas_hist_fim=None,
            escala_historico_rows=[],
            trocas_historico_export=[],
            contagem_contrato_turno={},
            resumo_coop_rows=[],
            resumo_totais=resumo_totais_empty,
            despesa_snapshot_map={},
            taxa_admin_rows=[],
            taxa_admin_totais={},
            juros_arrecadados_total=0.0,
            solicitacoes_adiantamento=[],
            adiantamento_status_map={},
            status_adiantamento_label=_status_adiantamento_label,
            status_adiantamento_badge=_status_adiantamento_badge,
            competencia_humana=_competencia_humana,
            farmacia_entregas=[],
        )

    # =========================
    # Lançamentos
    # =========================
    lancamentos = []
    total_producoes = 0.0
    total_inss = 0.0
    total_sest = 0.0
    total_encargos = 0.0

    q = Lancamento.query

    if restaurante_id:
        q = q.filter(Lancamento.restaurante_id == restaurante_id)
    if cooperado_id:
        q = q.filter(Lancamento.cooperado_id == cooperado_id)
    if data_inicio:
        q = q.filter(Lancamento.data >= data_inicio)
    if data_fim:
        q = q.filter(Lancamento.data <= data_fim)

    lanc_base = q.order_by(Lancamento.data.desc(), Lancamento.id.desc()).all()

    if dows:
        lancamentos = [l for l in lanc_base if l.data and _dow(l.data) in dows]
    else:
        lancamentos = lanc_base

    if considerar_periodo and restaurante_id:
        rest = Restaurante.query.get(restaurante_id)
        if rest:
            mapa = {
                "seg-dom": {"1", "2", "3", "4", "5", "6", "7"},
                "sab-sex": {"6", "7", "1", "2", "3", "4", "5"},
                "sex-qui": {"5", "6", "7", "1", "2", "3", "4"},
            }
            permitidos = mapa.get(rest.periodo, {"1", "2", "3", "4", "5", "6", "7"})
            lancamentos = [l for l in lancamentos if l.data and _dow(l.data) in permitidos]

    total_producoes = sum((l.valor or 0.0) for l in lancamentos)
    total_inss = round(total_producoes * INSS_ALIQ, 2)
    total_sest = round(total_producoes * SEST_ALIQ, 2)
    total_encargos = round(total_inss + total_sest, 2)

    # =========================
    # Receitas / Despesas Coop
    # =========================
    receitas = []
    despesas = []
    total_receitas = 0.0
    total_despesas = 0.0

    if True:
        rq = ReceitaCooperativa.query
        dq = DespesaCooperativa.query

        if data_inicio:
            rq = rq.filter(ReceitaCooperativa.data >= data_inicio)
            dq = dq.filter(DespesaCooperativa.data >= data_inicio)
        if data_fim:
            rq = rq.filter(ReceitaCooperativa.data <= data_fim)
            dq = dq.filter(DespesaCooperativa.data <= data_fim)

        receitas = rq.order_by(
            ReceitaCooperativa.data.desc().nullslast(),
            ReceitaCooperativa.id.desc()
        ).all()

        despesas = dq.order_by(
            DespesaCooperativa.data.desc(),
            DespesaCooperativa.id.desc()
        ).all()

        total_receitas = sum(_receita_total_real(r) for r in receitas)
        total_despesas = sum((d.valor or 0.0) for d in despesas)

    # =========================
    # Receitas / Despesas Cooperado
    # =========================
    receitas_coop = []
    despesas_coop = []
    total_receitas_coop = 0.0
    total_despesas_coop = 0.0
    total_adiantamentos_coop = 0.0

    if True:
        rq2 = ReceitaCooperado.query.filter(ReceitaCooperado.cooperado_id.in_(active_finance_ids))
        dq2 = DespesaCooperado.query.filter(DespesaCooperado.cooperado_id.in_(active_finance_ids))

        if data_inicio:
            rq2 = rq2.filter(ReceitaCooperado.data >= data_inicio)
        if data_fim:
            rq2 = rq2.filter(ReceitaCooperado.data <= data_fim)

        if data_inicio and data_fim:
            dq2 = dq2.filter(
                DespesaCooperado.data_inicio <= data_fim,
                DespesaCooperado.data_fim >= data_inicio,
            )
        elif data_inicio:
            dq2 = dq2.filter(DespesaCooperado.data_fim >= data_inicio)
        elif data_fim:
            dq2 = dq2.filter(DespesaCooperado.data_inicio <= data_fim)

        if cooperado_id:
            rq2 = rq2.filter(ReceitaCooperado.cooperado_id == cooperado_id)
            dq2 = dq2.filter(DespesaCooperado.cooperado_id == cooperado_id)

        somente_pendentes = bool((request.args.get('somente_pendentes') or '').strip())

        receitas_coop = rq2.order_by(
            ReceitaCooperado.data.desc(),
            ReceitaCooperado.id.desc()
        ).all()

        despesas_coop = dq2.order_by(
            DespesaCooperado.data_fim.desc().nullslast(),
            DespesaCooperado.id.desc()
        ).all()

        if somente_pendentes and cooperado_id:
            snap_pend = _compute_coop_debt_snapshot(cooperado_id, data_inicio, data_fim)
            pend_ids = {item['id'] for item in snap_pend['itens'] if item['status'] in ('pendente', 'parcial', 'a_descontar') and item['restante'] > 0}
            despesas_coop = [d for d in despesas_coop if d.id in pend_ids]

        total_receitas_coop = sum((r.valor or 0.0) for r in receitas_coop)
        total_despesas_coop = sum(
            (d.valor or 0.0) for d in despesas_coop
            if not getattr(d, "eh_adiantamento", False)
        )
        total_adiantamentos_coop = sum(
            (d.valor or 0.0) for d in despesas_coop
            if getattr(d, "eh_adiantamento", False)
        )

        despesa_snapshot_map = {}
        for _cid in {getattr(d, "cooperado_id", None) for d in despesas_coop if getattr(d, "cooperado_id", None)}:
            _snap = _compute_coop_debt_snapshot(_cid, data_inicio, data_fim)
            for _it in _snap["itens"]:
                despesa_snapshot_map[_it["id"]] = _it

    adiantamentos_q = SolicitacaoAdiantamento.query.join(Cooperado, SolicitacaoAdiantamento.cooperado_id == Cooperado.id).filter(SolicitacaoAdiantamento.cooperado_id.in_(active_finance_ids))
    if cooperado_id:
        adiantamentos_q = adiantamentos_q.filter(SolicitacaoAdiantamento.cooperado_id == cooperado_id)
    solicitacoes_adiantamento = adiantamentos_q.order_by(SolicitacaoAdiantamento.pedido_em.desc(), SolicitacaoAdiantamento.id.desc()).all()
    adiantamento_status_map = {s.despesa_cooperado_id: s for s in solicitacoes_adiantamento if s.despesa_cooperado_id}

    cfg = get_config()

    # Cooperados inativos/excluídos permanecem fora do financeiro e dos benefícios.
    cooperados = (
        Cooperado.query
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(
            Usuario.tipo == "cooperado",
            or_(Usuario.ativo.is_(True), Usuario.ativo.is_(None))
        )
        .order_by(Cooperado.nome.asc())
        .all()
    )

    restaurantes = Restaurante.query.order_by(Restaurante.nome).all()
    _ensure_taxas_admin_receitas(restaurantes, months_back=0)

    # Recarrega SEMPRE as receitas/despesas após gerar taxas automáticas.
    # Assim, sem filtro manual, a aba de receitas já abre mostrando o mês atual,
    # e com filtro continua respeitando o período informado.
    rq = ReceitaCooperativa.query
    dq = DespesaCooperativa.query
    if data_inicio:
        rq = rq.filter(ReceitaCooperativa.data >= data_inicio)
        dq = dq.filter(DespesaCooperativa.data >= data_inicio)
    if data_fim:
        rq = rq.filter(ReceitaCooperativa.data <= data_fim)
        dq = dq.filter(DespesaCooperativa.data <= data_fim)

    receitas = rq.order_by(
        ReceitaCooperativa.data.desc().nullslast(),
        ReceitaCooperativa.id.desc(),
    ).all()
    despesas = dq.order_by(
        DespesaCooperativa.data.desc(),
        DespesaCooperativa.id.desc(),
    ).all()
    total_receitas = sum(_receita_total_real(r) for r in receitas)
    total_despesas = sum((d.valor or 0.0) for d in despesas)
    taxa_admin_rows, taxa_admin_totais = _build_taxa_admin_rows(receitas)
    juros_arrecadados_total = round(sum((r['valor_multa'] + r['valor_juros']) for r in taxa_admin_rows if r['status'] == 'pago'), 2)
    cooperados_map = {c.id: c for c in cooperados}

    # =========================
    # Documentos / status
    # =========================
    docinfo_map = {c.id: _build_docinfo(c) for c in cooperados}
    status_doc_por_coop = {
        c.id: {
            "cnh_ok": docinfo_map[c.id]["cnh"]["ok"],
            "placa_ok": docinfo_map[c.id]["placa"]["ok"],
        }
        for c in cooperados
    }

    # =========================
    # Escalas
    # =========================
    escalas_all = (
        db.session.query(Escala)
        .outerjoin(Cooperado, Escala.cooperado_id == Cooperado.id)
        .outerjoin(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(
            or_(
                Escala.cooperado_id.is_(None),
                Usuario.ativo.is_(True)
            )
        )
        .order_by(Escala.id.asc())
        .all()
    )

    esc_by_int = defaultdict(list)
    esc_by_str = defaultdict(list)

    for e in escalas_all:
        k_int = e.cooperado_id if e.cooperado_id is not None else 0
        esc_item = {
            "data": e.data,
            "turno": e.turno,
            "horario": e.horario,
            "contrato": e.contrato,
            "cor": getattr(e, "cor", None),
            "nome_planilha": getattr(e, "cooperado_nome", None),
        }
        esc_by_int[k_int].append(esc_item)
        esc_by_str[str(k_int)].append(esc_item)

    cont_rows = dict(
        db.session.query(Escala.cooperado_id, func.count(Escala.id))
        .outerjoin(Cooperado, Escala.cooperado_id == Cooperado.id)
        .outerjoin(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(
            or_(
                Escala.cooperado_id.is_(None),
                Usuario.ativo.is_(True)
            )
        )
        .group_by(Escala.cooperado_id)
        .all()
    )

    qtd_escalas_map = {c.id: int(cont_rows.get(c.id, 0)) for c in cooperados}
    qtd_sem_cadastro = int(cont_rows.get(None, 0))

    contratos_set = {((e.contrato or "").strip()) for e in escalas_all if (e.contrato or "").strip()}
    contratos_set.update({((r.nome or "").strip()) for r in restaurantes if (r.nome or "").strip()})
    contratos_escala_opcoes = sorted(contratos_set, key=lambda s: s.lower())

    escala_editor_rows = []
    for e in sorted(escalas_all, key=_escala_sort_key):
        coop_obj = None
        if e.cooperado_id:
            coop_obj = cooperados_map.get(e.cooperado_id)

        nome_atual = (coop_obj.nome if coop_obj else (e.cooperado_nome or "").strip())
        escala_editor_rows.append({
            "id": e.id,
            "data": e.data or "",
            "weekday_num": _escala_weekday_num(e.data),
            "weekday_label": _escala_weekday_label(e.data),
            "turno": e.turno or "",
            "horario": e.horario or "",
            "contrato": e.contrato or "",
            "cooperado_id": e.cooperado_id,
            "cooperado_nome": nome_atual or "",
            "cooperado_nome_livre": (e.cooperado_nome or "") if not coop_obj else "",
            "restaurante_id": e.restaurante_id,
            "cor": getattr(e, "cor", None),
        })

    escala_alertas_1h = _build_escala_alertas_1h(escalas_all, cooperados_map)

    # =========================
    # Gráficos
    # =========================
    sums = {}
    for l in lancamentos:
        if not l.data:
            continue
        key = l.data.strftime("%Y-%m")
        sums[key] = sums.get(key, 0.0) + (l.valor or 0.0)

    labels_ord = sorted(sums.keys())

    def _fmt_label(k: str) -> str:
        parts = k.split("-")
        if len(parts) == 2 and parts[0] and parts[1]:
            year, month = parts[0], parts[1]
            return f"{month}/{year[-2:]}"
        return k

    labels_fmt = [_fmt_label(k) for k in labels_ord]
    values = [round(sums[k], 2) for k in labels_ord]
    chart_data_lancamentos_coop = {"labels": labels_fmt, "values": values}
    chart_data_lancamentos_cooperados = {"labels": labels_fmt, "values": values}

    # =========================
    # Admin master / principal
    # =========================
    admin_user = (
        Usuario.query
        .filter_by(tipo="admin", is_master=True)
        .order_by(Usuario.id.asc())
        .first()
    )

    if not admin_user:
        admin_user = (
            Usuario.query
            .filter_by(tipo="admin")
            .order_by(Usuario.id.asc())
            .first()
        )

    # =========================
    # Folha
    # =========================
    folha_por_coop = []
    folha_inicio = None
    folha_fim = None

    if active_tab == "folha":
        folha_inicio = _parse_date(args.get("folha_inicio"))
        folha_fim = _parse_date(args.get("folha_fim"))

        if folha_inicio and not folha_fim:
            folha_fim = folha_inicio
        elif folha_fim and not folha_inicio:
            folha_inicio = folha_fim
        elif not folha_inicio and not folha_fim:
            hoje_ref = date.today()
            folha_inicio = hoje_ref - timedelta(days=hoje_ref.weekday())
            folha_fim = folha_inicio + timedelta(days=6)

        INSS_ALIQ_FOLHA = 0.04
        SEST_ALIQ_FOLHA = 0.005

        FolhaItem = namedtuple(
            "FolhaItem",
            "cooperado lancamentos receitas despesas bruto inss sest encargos outras_desp liquido"
        )

        for c in cooperados:
            l = (
                Lancamento.query.filter(
                    Lancamento.cooperado_id == c.id,
                    Lancamento.data >= folha_inicio,
                    Lancamento.data <= folha_fim,
                )
                .order_by(Lancamento.data.asc(), Lancamento.id.asc())
                .all()
            )

            r = (
                ReceitaCooperado.query.filter(
                    ReceitaCooperado.cooperado_id == c.id,
                    ReceitaCooperado.data >= folha_inicio,
                    ReceitaCooperado.data <= folha_fim,
                )
                .order_by(ReceitaCooperado.data.asc(), ReceitaCooperado.id.asc())
                .all()
            )

            d = (
                DespesaCooperado.query.filter(
                    (DespesaCooperado.cooperado_id == c.id) | (DespesaCooperado.cooperado_id.is_(None)),
                    DespesaCooperado.data_inicio <= folha_fim,
                    DespesaCooperado.data_fim >= folha_inicio,
                )
                .order_by(DespesaCooperado.data_inicio.asc(), DespesaCooperado.id.asc())
                .all()
            )

            bruto_lanc = sum((x.valor or 0) for x in l)
            inss = round(bruto_lanc * INSS_ALIQ_FOLHA, 2)
            sest = round(bruto_lanc * SEST_ALIQ_FOLHA, 2)
            encargos = round(inss + sest, 2)
            outras_desp = sum((x.valor or 0) for x in d)
            bruto_total = bruto_lanc + sum((x.valor or 0) for x in r)
            liquido = round(bruto_total - encargos - outras_desp, 2)

            for x in l:
                x.conta_inss = True
                x.isento_benef = False
                x.inss = round((x.valor or 0) * INSS_ALIQ_FOLHA, 2)
                x.sest = round((x.valor or 0) * SEST_ALIQ_FOLHA, 2)
                x.encargos = round((x.inss or 0) + (x.sest or 0), 2)

            folha_por_coop.append(
                FolhaItem(
                    cooperado=c,
                    lancamentos=l,
                    receitas=r,
                    despesas=d,
                    bruto=round(bruto_total, 2),
                    inss=inss,
                    sest=sest,
                    encargos=encargos,
                    outras_desp=round(outras_desp, 2),
                    liquido=liquido,
                )
            )

    # =========================
    # Benefícios
    # =========================
    def _tokenize(s: str):
        return [x.strip() for x in re.split(r"[;,]", s or "") if x.strip()]

    def _d(s):
        if not s:
            return None
        s = s.strip()
        try:
            if "/" in s:
                d_, m_, y_ = s.split("/")
                return date(int(y_), int(m_), int(d_))
            y_, m_, d_ = s.split("-")
            return date(int(y_), int(m_), int(d_))
        except Exception:
            return None

    b_ini = _d(request.args.get("b_ini"))
    b_fim = _d(request.args.get("b_fim"))
    coop_filter = request.args.get("coop_benef_id", type=int)

    historico_beneficios = []
    beneficios_view = []

    if True:
        if b_ini and not b_fim:
            b_fim = b_ini
        elif b_fim and not b_ini:
            b_ini = b_fim
        elif not b_ini and not b_fim:
            hoje_ref = date.today()
            b_ini = hoje_ref - timedelta(days=hoje_ref.weekday())
            b_fim = b_ini + timedelta(days=6)

        q_benef = BeneficioRegistro.query.filter(
            BeneficioRegistro.data_inicial <= b_fim,
            BeneficioRegistro.data_final >= b_ini,
        )

        historico_beneficios = q_benef.order_by(BeneficioRegistro.id.desc()).all()

        for b in historico_beneficios:
            nomes = _tokenize(b.recebedores_nomes or "")
            ids = _tokenize(b.recebedores_ids or "")

            recs = []
            for i, nome in enumerate(nomes):
                rid = None

                if i < len(ids) and str(ids[i]).isdigit():
                    try:
                        rid = int(ids[i])
                    except Exception:
                        rid = None

                # Benefícios e prévia consideram somente cooperados ativos e não excluídos.
                if rid is None or rid not in active_finance_ids:
                    continue
                if coop_filter and rid != coop_filter:
                    continue

                recs.append({
                    "id": rid,
                    "nome": nome,
                })

            if not recs:
                continue

            valor_total = float(b.valor_total or 0.0)
            valor_por_recebedor = round(valor_total / len(recs), 2) if recs else 0.0

            beneficios_view.append({
                "id": b.id,
                "data_inicial": b.data_inicial,
                "data_final": b.data_final,
                "data_lancamento": b.data_lancamento,
                "tipo": b.tipo,
                "valor_total": valor_total,
                "valor_por_recebedor": valor_por_recebedor,
                "qtd_recebedores": len(recs),
                "recebedores": recs,
            })

    ajax_partial = (request.args.get("ajax_partial") or "").strip().lower()
    ajax_financeiros = {"resumo", "lancamentos", "receitas", "despesas", "coop_receitas", "coop_despesas", "beneficios"}
    if ajax_partial in ajax_financeiros:
        resumo_coop_rows = []
        resumo_totais = {
            "prod": 0.0, "inss4": 0.0, "sest05": 0.0, "rec": 0.0,
            "des": 0.0, "adiant": 0.0, "a_receber": 0.0, "saldo_pendente": 0.0,
            "pend_programado": 0.0
        }
        chart_data_lancamentos_coop = {"labels": [], "values": []}
        chart_data_lancamentos_cooperados = {"labels": [], "values": []}

        if ajax_partial == "resumo":
            sums = {}
            for l in lancamentos:
                if not l.data:
                    continue
                key = l.data.strftime("%Y-%m")
                sums[key] = sums.get(key, 0.0) + (l.valor or 0.0)

            labels_ord = sorted(sums.keys())
            labels_fmt = []
            for k in labels_ord:
                parts = k.split("-")
                if len(parts) == 2 and parts[0] and parts[1]:
                    year, month = parts[0], parts[1]
                    labels_fmt.append(f"{month}/{year[-2:]}")
                else:
                    labels_fmt.append(k)
            values = [round(sums[k], 2) for k in labels_ord]
            chart_data_lancamentos_coop = {"labels": labels_fmt, "values": values}
            chart_data_lancamentos_cooperados = {"labels": labels_fmt, "values": values}

            for coop in cooperados:
                snap = _compute_coop_debt_snapshot(coop.id, data_inicio, data_fim)
                prod = sum((l.valor or 0.0) for l in lancamentos if getattr(l, "cooperado_id", None) == coop.id)
                rec = sum((r.valor or 0.0) for r in receitas_coop if getattr(r, "cooperado_id", None) == coop.id)
                inss4 = sum((l.valor or 0.0) * INSS_ALIQ for l in lancamentos if getattr(l, "cooperado_id", None) == coop.id)
                sest05 = sum((l.valor or 0.0) * SEST_ALIQ for l in lancamentos if getattr(l, "cooperado_id", None) == coop.id)
                des = round(snap.get("descontado_periodo_despesa", 0.0), 2)
                adiant = round(snap.get("descontado_periodo_adiant", 0.0), 2)
                if prod or rec or des or adiant or snap["saldo_devedor"] or snap["a_descontar"]:
                    a_receber = round(max(0.0, snap["disponivel_auto_restante"]), 2)
                    saldo_pendente = round(snap["saldo_devedor"], 2)
                    pend_programado = round(snap["a_descontar"], 2)
                    resumo_coop_rows.append({
                        "id": coop.id,
                        "nome": coop.nome,
                        "prod": round(prod,2),
                        "inss4": round(inss4,2),
                        "sest05": round(sest05,2),
                        "rec": round(rec,2),
                        "des": round(des,2),
                        "adiant": round(adiant,2),
                        "a_receber": a_receber,
                        "aReceber": a_receber,
                        "saldo_pendente": saldo_pendente,
                        "saldoPendente": saldo_pendente,
                        "pend_programado": pend_programado,
                        "pendProgramado": pend_programado,
                    })
                    resumo_totais["prod"] += prod
                    resumo_totais["inss4"] += inss4
                    resumo_totais["sest05"] += sest05
                    resumo_totais["rec"] += rec
                    resumo_totais["des"] += des
                    resumo_totais["adiant"] += adiant
                    resumo_totais["a_receber"] += max(0.0, snap["disponivel_auto_restante"])
                    resumo_totais["saldo_pendente"] += snap["saldo_devedor"]
                    resumo_totais["pend_programado"] += snap["a_descontar"]

        partial_context = dict(
            tab=ajax_partial,
            fast_mode=False,
            total_producoes=total_producoes,
            total_inss=total_inss,
            total_sest=total_sest,
            total_encargos=total_encargos,
            total_receitas=total_receitas,
            total_despesas=total_despesas,
            total_receitas_coop=total_receitas_coop,
            total_despesas_coop=total_despesas_coop,
            total_adiantamentos_coop=total_adiantamentos_coop,
            salario_minimo=(cfg.salario_minimo or 0.0) if cfg else 0.0,
        bloquear_adiantamento=bool(getattr(cfg, "bloquear_adiantamento", False)) if cfg else False,
            lancamentos=lancamentos,
            receitas=receitas,
            despesas=despesas,
            receitas_coop=receitas_coop,
            despesas_coop=despesas_coop,
            cooperados=cooperados,
            restaurantes=restaurantes,
            beneficios_view=beneficios_view,
            historico_beneficios=historico_beneficios,
            admin_perms=admin_perms,
            admin_is_master=is_admin_master(),
            taxa_admin_rows=taxa_admin_rows,
            taxa_admin_totais=taxa_admin_totais,
            juros_arrecadados_total=juros_arrecadados_total,
            resumo_coop_rows=resumo_coop_rows,
            resumo_totais=resumo_totais,
            chart_data_lancamentos_coop=chart_data_lancamentos_coop,
            chart_data_lancamentos_cooperados=chart_data_lancamentos_cooperados,
            despesa_snapshot_map=despesa_snapshot_map if 'despesa_snapshot_map' in locals() else {},
            solicitacoes_adiantamento=solicitacoes_adiantamento if 'solicitacoes_adiantamento' in locals() else [],
            adiantamento_status_map=adiantamento_status_map if 'adiantamento_status_map' in locals() else {},
            status_adiantamento_label=_status_adiantamento_label,
            status_adiantamento_badge=_status_adiantamento_badge,
            competencia_humana=_competencia_humana,
            current_date=date.today(),
            data_limite=date(date.today().year, 12, 31),
            filtro_periodo_aplicado=filtro_periodo_aplicado,
        )
        return _render_admin_dashboard_partial(ajax_partial, **partial_context)

    # =========================
    # Trocas
    # =========================
    def _escala_desc(e):
        return _escala_label(e) if e else ""

    def _split_turno_horario(s: str) -> tuple[str, str]:
        if not s:
            return "", ""
        parts = [p.strip() for p in s.split("•")]
        if len(parts) == 2:
            return parts[0], parts[1]
        return s.strip(), ""

    def _linha_from_escala(e: Escala, saiu: str, entrou: str) -> dict:
        return {
            "dia": _escala_label(e).split(" • ")[0],
            "turno_horario": " • ".join(
                [x for x in [(e.turno or "").strip(), (e.horario or "").strip()] if x]
            ),
            "contrato": (e.contrato or "").strip(),
            "saiu": saiu,
            "entrou": entrou,
        }

    trocas_all = TrocaSolicitacao.query.order_by(TrocaSolicitacao.id.desc()).all()
    trocas_pendentes = []
    trocas_historico = []
    trocas_historico_flat = []

    for t in trocas_all:
        solicitante = Cooperado.query.get(t.solicitante_id)
        destinatario = Cooperado.query.get(t.destino_id)
        orig = Escala.query.get(t.origem_escala_id)

        linhas_afetadas = _parse_linhas_from_msg(t.mensagem) if t.status == "aprovada" else []

        if t.status == "aprovada" and not linhas_afetadas and orig and solicitante and destinatario:
            linhas_afetadas.append(_linha_from_escala(orig, saiu=solicitante.nome, entrou=destinatario.nome))

            wd_o = _weekday_from_data_str(orig.data)
            buck_o = _turno_bucket(orig.turno, orig.horario)
            candidatas = Escala.query.filter_by(cooperado_id=destinatario.id).all()
            best = None

            for e in candidatas:
                if _weekday_from_data_str(e.data) == wd_o and _turno_bucket(e.turno, e.horario) == buck_o:
                    if (orig.contrato or "").strip().lower() == (e.contrato or "").strip().lower():
                        best = e
                        break
                    if best is None:
                        best = e

            if best:
                linhas_afetadas.append(_linha_from_escala(best, saiu=destinatario.nome, entrou=solicitante.nome))

        destino_data = ""
        destino_turno = ""
        destino_horario = ""
        destino_contrato = ""

        if t.status == "aprovada" and linhas_afetadas and solicitante and destinatario:
            linha_dest = None
            for r_ in linhas_afetadas:
                if r_.get("saiu") == destinatario.nome and r_.get("entrou") == solicitante.nome:
                    linha_dest = r_
                    break

            if linha_dest:
                destino_data = linha_dest.get("dia", "")
                turno_txt, horario_txt = _split_turno_horario(linha_dest.get("turno_horario", ""))
                destino_turno = turno_txt
                destino_horario = horario_txt
                destino_contrato = linha_dest.get("contrato", "")

        if not destino_data and orig and destinatario:
            wd_o = _weekday_from_data_str(orig.data)
            buck_o = _turno_bucket(orig.turno, orig.horario)
            candidatas = Escala.query.filter_by(cooperado_id=destinatario.id).all()
            best = None

            for e in candidatas:
                if _weekday_from_data_str(e.data) == wd_o and _turno_bucket(e.turno, e.horario) == buck_o:
                    if (orig.contrato or "").strip().lower() == (e.contrato or "").strip().lower():
                        best = e
                        break
                    if best is None:
                        best = e

            if best:
                destino_data = best.data
                destino_turno = (best.turno or "").strip()
                destino_horario = (best.horario or "").strip()
                destino_contrato = (best.contrato or "").strip()

        item = {
            "id": t.id,
            "status": t.status,
            "mensagem": t.mensagem,
            "criada_em": t.criada_em,
            "aplicada_em": t.aplicada_em,
            "solicitante": solicitante,
            "destinatario": destinatario,
            "origem": orig,
            "destino": destinatario,
            "origem_desc": _escala_desc(orig),
            "origem_weekday": _weekday_from_data_str(orig.data) if orig else None,
            "origem_turno_bucket": _turno_bucket(orig.turno if orig else None, orig.horario if orig else None),
            "destino_data": destino_data,
            "destino_turno": destino_turno,
            "destino_horario": destino_horario,
            "destino_contrato": destino_contrato,
            "linhas_afetadas": linhas_afetadas,
        }

        if t.status == "aprovada" and linhas_afetadas:
            itens = []
            for r_ in linhas_afetadas:
                turno_txt, horario_txt = _split_turno_horario(r_.get("turno_horario", ""))
                itens.append(
                    {
                        "data": r_.get("dia", ""),
                        "turno": turno_txt,
                        "horario": horario_txt,
                        "contrato": r_.get("contrato", ""),
                        "saiu_nome": r_.get("saiu", ""),
                        "entrou_nome": r_.get("entrou", ""),
                    }
                )

                trocas_historico_flat.append(
                    {
                        "data": r_.get("dia", ""),
                        "turno": turno_txt,
                        "horario": horario_txt,
                        "contrato": r_.get("contrato", ""),
                        "saiu_nome": r_.get("saiu", ""),
                        "entrou_nome": r_.get("entrou", ""),
                        "aplicada_em": t.aplicada_em,
                    }
                )

            item["itens"] = itens

        if t.status == "pendente":
            trocas_pendentes.append(item)
        else:
            trocas_historico.append(item)

    admins = (
        Usuario.query
        .filter_by(tipo="admin", is_master=False)
        .order_by(Usuario.id.asc())
        .all()
    )

    admin_permissions_map = {}
    for adm in admins:
        admin_permissions_map[adm.id] = get_admin_permissions_map(adm.id)

    admins_secundarios = admins
    admins_permissoes = admin_permissions_map

    current_date = date.today()
    data_limite = date(current_date.year, 12, 31)

    escala_hist_inicio = _parse_ymd_date(args.get("escala_hist_inicio"))
    escala_hist_fim = _parse_ymd_date(args.get("escala_hist_fim"))
    trocas_hist_inicio = _parse_ymd_date(args.get("trocas_hist_inicio"))
    trocas_hist_fim = _parse_ymd_date(args.get("trocas_hist_fim"))

    escala_historico_rows = []
    escala_editor_rows_export = []
    trocas_historico_export = []
    contagem_contrato_turno = []

    if active_tab == "escalas":
        if not escala_hist_inicio and not escala_hist_fim:
            escala_hist_fim = current_date
            escala_hist_inicio = current_date - timedelta(days=30)
        elif escala_hist_inicio and not escala_hist_fim:
            escala_hist_fim = escala_hist_inicio
        elif escala_hist_fim and not escala_hist_inicio:
            escala_hist_inicio = escala_hist_fim

        escala_hist_q = _history_rows_between(EscalaHistorico.query, EscalaHistorico.snapshot_em, escala_hist_inicio, escala_hist_fim)
        escala_hist_rows_db = escala_hist_q.order_by(EscalaHistorico.snapshot_em.desc(), EscalaHistorico.id.desc()).all()
        for h in escala_hist_rows_db:
            escala_historico_rows.append({
                "snapshot_em": h.snapshot_em,
                "data": h.data or "",
                "turno": h.turno or "",
                "horario": h.horario or "",
                "contrato": h.contrato or "",
                "cooperado_nome": h.cooperado_nome or "",
                "saiu_nome": h.saiu_nome or "",
                "entrou_nome": h.entrou_nome or "",
                "origem": h.origem or "",
                "acao": h.acao or "",
            })

        hist_rows_for_current = _history_rows_between(EscalaHistorico.query.filter(EscalaHistorico.saiu_nome.isnot(None)), EscalaHistorico.snapshot_em, escala_hist_inicio, escala_hist_fim).all()
        escala_editor_rows_export = _resolve_change_columns(escala_editor_rows, hist_rows_for_current)
    else:
        escala_hist_fim = escala_hist_fim or current_date
        escala_hist_inicio = escala_hist_inicio or (current_date - timedelta(days=30))

    if active_tab == "trocas":
        if not trocas_hist_inicio and not trocas_hist_fim:
            trocas_hist_fim = current_date
            trocas_hist_inicio = current_date - timedelta(days=30)
        elif trocas_hist_inicio and not trocas_hist_fim:
            trocas_hist_fim = trocas_hist_inicio
        elif trocas_hist_fim and not trocas_hist_inicio:
            trocas_hist_inicio = trocas_hist_fim

        trocas_hist_q = _history_rows_between(TrocaHistorico.query, TrocaHistorico.aplicada_em, trocas_hist_inicio, trocas_hist_fim)
        trocas_historico_export = trocas_hist_q.order_by(TrocaHistorico.aplicada_em.desc(), TrocaHistorico.id.desc()).all()
    else:
        trocas_hist_fim = trocas_hist_fim or current_date
        trocas_hist_inicio = trocas_hist_inicio or (current_date - timedelta(days=30))


    # resumo por cooperado calculado no backend para evitar travar no JS
    resumo_coop_rows = []
    resumo_totais = {
        "prod": 0.0, "inss4": 0.0, "sest05": 0.0, "rec": 0.0,
        "des": 0.0, "adiant": 0.0, "a_receber": 0.0, "saldo_pendente": 0.0,
        "pend_programado": 0.0
    }
    for coop in cooperados:
        snap = _compute_coop_debt_snapshot(coop.id, data_inicio, data_fim)
        prod = sum((l.valor or 0.0) for l in lancamentos if getattr(l, "cooperado_id", None) == coop.id)
        rec = sum((r.valor or 0.0) for r in receitas_coop if getattr(r, "cooperado_id", None) == coop.id)
        inss4 = sum((l.valor or 0.0) * INSS_ALIQ for l in lancamentos if getattr(l, "cooperado_id", None) == coop.id)
        sest05 = sum((l.valor or 0.0) * SEST_ALIQ for l in lancamentos if getattr(l, "cooperado_id", None) == coop.id)
        des = round(snap.get("descontado_periodo_despesa", 0.0), 2)
        adiant = round(snap.get("descontado_periodo_adiant", 0.0), 2)
        if prod or rec or des or adiant or snap["saldo_devedor"] or snap["a_descontar"]:
            _a_receber = round(max(0.0, snap["disponivel_auto_restante"]), 2)
            _saldo_pendente = round(snap["saldo_devedor"], 2)
            _pend_programado = round(snap["a_descontar"], 2)
            resumo_coop_rows.append({
                "id": coop.id,
                "nome": coop.nome,
                "prod": round(prod,2),
                "inss4": round(inss4,2),
                "sest05": round(sest05,2),
                "rec": round(rec,2),
                "des": round(des,2),
                "adiant": round(adiant,2),
                "a_receber": _a_receber,
                "aReceber": _a_receber,
                "saldo_pendente": _saldo_pendente,
                "saldoPendente": _saldo_pendente,
                "pend_programado": _pend_programado,
                "pendProgramado": _pend_programado,
            })
            resumo_totais["prod"] += prod
            resumo_totais["inss4"] += inss4
            resumo_totais["sest05"] += sest05
            resumo_totais["rec"] += rec
            resumo_totais["des"] += des
            resumo_totais["adiant"] += adiant
            resumo_totais["a_receber"] += max(0.0, snap["disponivel_auto_restante"])
            resumo_totais["saldo_pendente"] += snap["saldo_devedor"]
            resumo_totais["pend_programado"] += snap["a_descontar"]

    _rendered_html = render_template(
        "admin_dashboard.html",
        fast_mode=True,
        tab=active_tab,
        total_producoes=total_producoes,
        total_inss=total_inss,
        total_sest=total_sest,
        total_encargos=total_encargos,
        total_receitas=total_receitas,
        total_despesas=total_despesas,
        total_receitas_coop=total_receitas_coop,
        total_despesas_coop=total_despesas_coop,
        total_adiantamentos_coop=total_adiantamentos_coop,
        salario_minimo=(cfg.salario_minimo or 0.0) if cfg else 0.0,
        bloquear_adiantamento=bool(getattr(cfg, "bloquear_adiantamento", False)) if cfg else False,
        lancamentos=lancamentos,
        receitas=receitas,
        despesas=despesas,
        receitas_coop=receitas_coop,
        despesas_coop=despesas_coop,
        cooperados=cooperados,
        restaurantes=restaurantes,
        beneficios_view=beneficios_view,
        historico_beneficios=historico_beneficios,
        current_date=current_date,
        data_limite=data_limite,
        admin=admin_user,
        admin_user=admin_user,
        docinfo_map=docinfo_map,
        escalas_por_coop=esc_by_int,
        escalas_por_coop_json=esc_by_str,
        qtd_escalas_map=qtd_escalas_map,
        qtd_escalas_sem_cadastro=qtd_sem_cadastro,
        status_doc_por_coop=status_doc_por_coop,
        chart_data_lancamentos_coop=chart_data_lancamentos_coop,
        chart_data_lancamentos_cooperados=chart_data_lancamentos_cooperados,
        folha_inicio=folha_inicio,
        folha_fim=folha_fim,
        folha_por_coop=folha_por_coop,
        trocas_pendentes=trocas_pendentes,
        trocas_historico=trocas_historico,
        trocas_historico_flat=trocas_historico_flat,
        admin_perms=admin_perms,
        admin_is_master=is_admin_master(),
        ADMIN_ABAS=ADMIN_ABAS,
        admins=admins,
        admin_permissions_map=admin_permissions_map,
        admins_secundarios=admins_secundarios,
        admins_permissoes=admins_permissoes,
        filtro_periodo_aplicado=filtro_periodo_aplicado,
        escala_editor_rows=escala_editor_rows,
        escala_editor_rows_export=escala_editor_rows_export,
        contratos_escala_opcoes=contratos_escala_opcoes,
        escala_hist_inicio=escala_hist_inicio,
        escala_hist_fim=escala_hist_fim,
        trocas_hist_inicio=trocas_hist_inicio,
        trocas_hist_fim=trocas_hist_fim,
        escala_historico_rows=escala_historico_rows,
        trocas_historico_export=trocas_historico_export,
        contagem_contrato_turno=contagem_contrato_turno,
        resumo_coop_rows=resumo_coop_rows,
        resumo_totais=resumo_totais,
        despesa_snapshot_map=despesa_snapshot_map,
        taxa_admin_rows=taxa_admin_rows,
        taxa_admin_totais=taxa_admin_totais,
        juros_arrecadados_total=juros_arrecadados_total,
        solicitacoes_adiantamento=solicitacoes_adiantamento if 'solicitacoes_adiantamento' in locals() else [],
        adiantamento_status_map=adiantamento_status_map if 'adiantamento_status_map' in locals() else {},
        status_adiantamento_label=_status_adiantamento_label,
        status_adiantamento_badge=_status_adiantamento_badge,
        competencia_humana=_competencia_humana,
        farmacia_entregas=(farmacia_entregas if 'farmacia_entregas' in locals() else []),
    )

    ajax_partial = (request.args.get("ajax_partial") or "").strip().lower()
    if ajax_partial:
        marker_map = {
            "resumo": "RESUMO",
            "lancamentos": "LANC",
            "receitas": "RECEITAS",
            "despesas": "DESPESAS",
            "coop_receitas": "COOP_RECEITAS",
            "coop_despesas": "COOP_DESPESAS",
            "beneficios": "BENEFICIOS",
        }
        marker = marker_map.get(ajax_partial)
        if marker:
            m = re.search(rf"<!--AJAX_{marker}_START-->(.*?)<!--AJAX_{marker}_END-->", _rendered_html, flags=re.DOTALL)
            if m:
                return m.group(1)
    return _rendered_html

