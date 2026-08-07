from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from flask import flash, redirect, request, url_for
from sqlalchemy import or_

import admin_light_v8 as light

app = light.app
db = light.db
Usuario = light.Usuario
Cooperado = light.Cooperado
Restaurante = light.Restaurante
Lancamento = light.Lancamento
Escala = light.Escala
BUILD = "20260807-1318"


def _weekday_num(value) -> int | None:
    try:
        if hasattr(value, "isoweekday"):
            return int(value.isoweekday())
    except Exception:
        pass
    text = str(value or "").strip().lower()
    names = {
        "segunda": 1, "seg": 1,
        "terça": 2, "terca": 2, "ter": 2,
        "quarta": 3, "qua": 3,
        "quinta": 4, "qui": 4,
        "sexta": 5, "sex": 5,
        "sábado": 6, "sabado": 6, "sáb": 6, "sab": 6,
        "domingo": 7, "dom": 7,
    }
    for key, num in names.items():
        if key in text:
            return num
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(text, fmt).date().isoweekday()
        except Exception:
            continue
    return None


def _weekday_label(value) -> str:
    labels = {1: "Segunda", 2: "Terça", 3: "Quarta", 4: "Quinta", 5: "Sexta", 6: "Sábado", 7: "Domingo"}
    return labels.get(_weekday_num(value), str(value or "—"))


def _admin_light_launches_full():
    denied = light._guard("lancamentos")
    if denied:
        return denied

    di, df = light._period_from_request(True)
    q = (request.args.get("q") or "").strip()
    restaurante_id = request.args.get("restaurante_id", type=int)
    cooperado_id = request.args.get("cooperado_id", type=int)
    considerar_periodo = bool(request.args.get("considerar_periodo"))
    dows = {str(x) for x in request.args.getlist("dow") if str(x).strip()}

    query = Lancamento.query
    if di:
        query = query.filter(Lancamento.data >= di)
    if df:
        query = query.filter(Lancamento.data <= df)
    if restaurante_id:
        query = query.filter(Lancamento.restaurante_id == restaurante_id)
    if cooperado_id:
        query = query.filter(Lancamento.cooperado_id == cooperado_id)
    if q:
        pat = f"%{q}%"
        query = (
            query.outerjoin(Cooperado, Lancamento.cooperado_id == Cooperado.id)
            .outerjoin(Restaurante, Lancamento.restaurante_id == Restaurante.id)
            .filter(or_(Cooperado.nome.ilike(pat), Restaurante.nome.ilike(pat), Lancamento.descricao.ilike(pat)))
        )

    launches = query.order_by(Lancamento.data.desc(), Lancamento.id.desc()).limit(500).all()
    if dows:
        launches = [row for row in launches if row.data and str(row.data.isoweekday()) in dows]

    # Mantém a opção histórica "considerar período do restaurante".
    if considerar_periodo and restaurante_id:
        rest_selected = db.session.get(Restaurante, restaurante_id)
        if rest_selected:
            allowed = {
                "seg-dom": {"1", "2", "3", "4", "5", "6", "7"},
                "sab-sex": {"6", "7", "1", "2", "3", "4", "5"},
                "sex-qui": {"5", "6", "7", "1", "2", "3", "4"},
            }.get(rest_selected.periodo, {"1", "2", "3", "4", "5", "6", "7"})
            launches = [row for row in launches if row.data and str(row.data.isoweekday()) in allowed]

    cooperados = light._active_coops()
    restaurantes = (
        Restaurante.query.filter(or_(Restaurante.ativo.is_(True), Restaurante.ativo.is_(None)))
        .order_by(Restaurante.nome.asc()).all()
    )
    coop_map = {c.id: c for c in Cooperado.query.filter(Cooperado.id.in_({x.cooperado_id for x in launches})).all()} if launches else {}
    rest_map = {r.id: r for r in Restaurante.query.filter(Restaurante.id.in_({x.restaurante_id for x in launches})).all()} if launches else {}

    total = sum(float(x.valor or 0.0) for x in launches)
    total_inss = round(total * 0.04, 2)
    total_sest = round(total * 0.005, 2)
    total_liquido = round(total - total_inss - total_sest, 2)

    return light._render(
        "lancamentos",
        "Lançamentos",
        "Modo completo preservado, com os mesmos filtros, descontos e ações. O padrão continua sendo o dia atual.",
        data_inicio=di,
        data_fim=df,
        q=q,
        launches=launches,
        coop_map=coop_map,
        rest_map=rest_map,
        launch_total=total,
        launch_total_inss=total_inss,
        launch_total_sest=total_sest,
        launch_total_liquido=total_liquido,
        cooperados=cooperados,
        restaurantes=restaurantes,
        restaurante_id=restaurante_id,
        cooperado_id=cooperado_id,
        considerar_periodo=considerar_periodo,
        dows=dows,
    )


def _admin_light_scale_full():
    denied = light._guard("escalas")
    if denied:
        return denied

    q = (request.args.get("q") or "").strip().casefold()
    active_ids, active_names, active_coops = light._active_coop_ids_names()
    restaurants = (
        Restaurante.query.filter(or_(Restaurante.ativo.is_(True), Restaurante.ativo.is_(None)))
        .order_by(Restaurante.nome.asc()).all()
    )

    # A grade antiga depende também das vagas sem cooperado. Por isso elas são
    # mantidas. Linhas vinculadas a cooperados desativados ficam fora da operação.
    candidates = Escala.query.order_by(Escala.id.asc()).limit(1400).all()
    scales = []
    scale_rows = []
    contracts = set()
    for s in candidates:
        if s.cooperado_id and s.cooperado_id not in active_ids:
            continue
        if not s.cooperado_id and (s.cooperado_nome or "").strip():
            # Nome livre antigo continua visível; só ignora se corresponder
            # explicitamente a alguém desativado e não houver vínculo ativo.
            pass
        current_name = ""
        if s.cooperado_id:
            coop = next((c for c in active_coops if c.id == s.cooperado_id), None)
            current_name = coop.nome if coop else ""
        else:
            current_name = (s.cooperado_nome or "").strip()

        hay = " ".join(str(x or "") for x in (s.data, s.turno, s.horario, s.contrato, current_name)).casefold()
        if q and q not in hay:
            continue
        scales.append(s)
        if (s.contrato or "").strip():
            contracts.add((s.contrato or "").strip())
        scale_rows.append({
            "id": s.id,
            "data": s.data or "",
            "weekday_num": _weekday_num(s.data),
            "weekday_label": _weekday_label(s.data),
            "turno": s.turno or "",
            "horario": s.horario or "",
            "contrato": s.contrato or "",
            "restaurante_id": s.restaurante_id,
            "cooperado_id": s.cooperado_id,
            "cooperado_nome": current_name,
            "cooperado_nome_livre": "" if s.cooperado_id else (s.cooperado_nome or ""),
        })

    contracts.update((r.nome or "").strip() for r in restaurants if (r.nome or "").strip())
    contract_options = sorted(contracts, key=lambda x: x.casefold())
    coop_map = {c.id: c for c in active_coops}
    rest_map = {r.id: r for r in restaurants}

    return light._render(
        "escala",
        "Escala",
        "O modo de montar a escala foi preservado. A grade semanal abre sob demanda para manter a velocidade.",
        q=q,
        scales=scales,
        scale_rows=scale_rows,
        contract_options=contract_options,
        coop_map=coop_map,
        rest_map=rest_map,
        cooperados=active_coops,
        restaurantes=restaurants,
    )


def _admin_light_coop_save_full(coop_id: int):
    denied = light._guard("cooperados")
    if denied:
        return denied
    coop = Cooperado.query.get_or_404(coop_id)
    user = Usuario.query.get_or_404(coop.usuario_id)

    nome = (request.form.get("nome") or coop.nome or "").strip()
    telefone = (request.form.get("telefone") or "").strip() or None
    usuario = (request.form.get("usuario") or user.usuario or "").strip()
    nova_senha = request.form.get("senha") or ""

    if not nome:
        flash("Informe o nome do cooperado.", "warning")
        return redirect(url_for("admin_light_cooperatives", status=request.form.get("status") or "ativos"))
    if usuario:
        duplicate = Usuario.query.filter(Usuario.usuario == usuario, Usuario.id != user.id).first()
        if duplicate:
            flash("Este usuário de acesso já está em uso.", "warning")
            return redirect(url_for("admin_light_cooperatives", status=request.form.get("status") or "ativos"))
        user.usuario = usuario

    coop.nome = nome
    user.nome = nome
    coop.telefone = telefone
    if nova_senha.strip():
        user.set_password(nova_senha)

    photo = request.files.get("foto")
    if photo and photo.filename:
        payload = photo.read(6 * 1024 * 1024 + 1)
        if len(payload) > 6 * 1024 * 1024:
            flash("A foto deve ter no máximo 6 MB.", "warning")
            return redirect(url_for("admin_light_cooperatives", status=request.form.get("status") or "ativos"))
        coop.foto_bytes = payload
        coop.foto_mime = photo.mimetype or "image/jpeg"
        coop.foto_filename = photo.filename[:255]
        coop.foto_url = None

    coop.ultima_atualizacao = datetime.utcnow()
    db.session.commit()
    flash("Cadastro atualizado. Nome, telefone, usuário, foto e senha foram preservados no mesmo cadastro.", "success")
    return redirect(url_for("admin_light_cooperatives", status=request.form.get("status") or "ativos"))


# Substitui apenas a implementação das páginas V8; as rotas e modos antigos
# continuam registradas no Flask e podem ser acessadas com ?legacy=1.
app.view_functions["admin_light_launches"] = _admin_light_launches_full
app.view_functions["admin_light_scale"] = _admin_light_scale_full
app.view_functions["admin_light_coop_save"] = _admin_light_coop_save_full

app.logger.info("Admin Preserve V9 carregado: nenhum modo antigo removido; V8 ganhou filtros e edição completos.")
