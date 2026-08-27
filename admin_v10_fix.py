from __future__ import annotations

import os
import unicodedata
from datetime import date, datetime
from io import BytesIO
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from flask import flash, redirect, render_template, render_template_string, request, send_file, session, url_for
from sqlalchemy import event, or_

import app as legacy
import admin_light_v8 as light
import admin_preserve_v9 as preserve

app = legacy.app
db = legacy.db
Usuario = legacy.Usuario
Cooperado = legacy.Cooperado
Restaurante = legacy.Restaurante
Escala = legacy.Escala
TrocaSolicitacao = legacy.TrocaSolicitacao
AdminPermissao = getattr(legacy, "AdminPermissao", None)
BUILD = "20260807-1408-v11"


@app.context_processor
def _admin_v11_permission_context():
    """Disponibiliza as permissões reais para todo o menu/painel V11."""
    user = legacy._usuario_logado()
    is_master = bool(user and getattr(user, "is_master", False))
    permissions = {}
    if user and (user.tipo or "").strip().lower() == "admin":
        if is_master:
            permissions = {
                aba: {"ver": True, "criar": True, "editar": True, "excluir": True}
                for aba in legacy.ADMIN_ABAS
            }
        else:
            permissions = legacy.get_admin_permissions_map(user.id)
    home_url = url_for("admin_light_summary")
    if not is_master:
        destinations = (
            ("lancamentos", "admin_light_summary", {}),
            ("receitas", "admin_v10_finance", {"tab": "receitas"}),
            ("despesas", "admin_v10_finance", {"tab": "despesas"}),
            ("coop_receitas", "admin_v10_finance", {"tab": "coop_receitas"}),
            ("coop_despesas", "admin_v10_finance", {"tab": "coop_despesas"}),
            ("beneficios", "admin_v10_finance", {"tab": "beneficios"}),
            ("cooperados", "admin_light_cooperatives", {}),
            ("restaurantes", "admin_v10_establishments", {}),
            ("escalas", "admin_light_scale", {}),
            ("avaliacoes", "admin_light_ratings", {}),
            ("avisos", "admin_light_notices", {}),
            ("documentos", "admin_light_documents", {}),
            ("tabelas", "admin_light_tables", {}),
        )
        for permission, endpoint, values in destinations:
            if permissions.get(permission, {}).get("ver") and endpoint in app.view_functions:
                home_url = url_for(endpoint, **values)
                break
    return {
        "admin_nav_master": is_master,
        "admin_nav_perms": permissions,
        "admin_can_edit_escalas": is_master or bool(permissions.get("escalas", {}).get("editar")),
        "admin_nav_home_url": home_url,
    }


def brl(value) -> str:
    try:
        number = float(value or 0.0)
    except Exception:
        number = 0.0
    raw = f"{number:,.2f}"
    return raw.replace(",", "§").replace(".", ",").replace("§", ".")


app.jinja_env.filters["brl"] = brl
app.jinja_env.globals["brl"] = brl


def _norm(value) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("_", " ").casefold()
    return " ".join(text.split())


def _parse_date(value):
    if not value:
        return None
    if isinstance(value, date):
        return value
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(value), fmt).date()
        except Exception:
            pass
    return None


# Todos os estabelecimentos usam segunda a domingo.
@event.listens_for(Restaurante, "before_insert", propagate=True)
def _rest_period_insert(mapper, connection, target):
    target.periodo = "seg-dom"


@event.listens_for(Restaurante, "before_update", propagate=True)
def _rest_period_update(mapper, connection, target):
    target.periodo = "seg-dom"


try:
    with app.app_context():
        Restaurante.query.filter(
            or_(Restaurante.periodo != "seg-dom", Restaurante.periodo.is_(None))
        ).update({Restaurante.periodo: "seg-dom"}, synchronize_session=False)
        db.session.commit()
except Exception:
    db.session.rollback()
    app.logger.exception("Falha ao normalizar período dos estabelecimentos")


# Protege cooperados desativados contra a rotina legada que os reativava.
_PROTECTED_INACTIVE_USER_IDS: set[int] = set()


def _reload_protected_users():
    global _PROTECTED_INACTIVE_USER_IDS
    with app.app_context():
        rows = (
            db.session.query(Usuario.id)
            .join(Cooperado, Cooperado.usuario_id == Usuario.id)
            .filter(Usuario.ativo.is_(False))
            .all()
        )
        _PROTECTED_INACTIVE_USER_IDS = {int(uid) for uid, in rows if uid}


try:
    _reload_protected_users()
except Exception:
    app.logger.exception("Falha ao carregar cooperados desativados")


@event.listens_for(Usuario.ativo, "set", retval=True)
def _protect_inactive_coop(target, value, oldvalue, initiator):
    try:
        if value is True and target.id and int(target.id) in _PROTECTED_INACTIVE_USER_IDS:
            return False
    except Exception:
        pass
    return value


def _active_coops_v11():
    try:
        archived = light._archived_ids()
    except Exception:
        archived = set()
    return (
        Cooperado.query.join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(or_(Usuario.ativo.is_(True), Usuario.ativo.is_(None)))
        .filter(~Cooperado.id.in_(archived) if archived else True)
        .order_by(Cooperado.nome.asc())
        .all()
    )


def _active_coop_ids_names_v11():
    rows = _active_coops_v11()
    return {c.id for c in rows}, {_norm(c.nome) for c in rows}, rows


light._active_coops = _active_coops_v11
light._active_coop_ids_names = _active_coop_ids_names_v11


def _scale_assignment_counts(active_coops=None):
    active_coops = active_coops if active_coops is not None else _active_coops_v11()
    active_ids = {c.id for c in active_coops}
    name_to_id = {_norm(c.nome): c.id for c in active_coops if _norm(c.nome)}
    assigned = set()
    if active_ids:
        for cid, in db.session.query(Escala.cooperado_id).filter(Escala.cooperado_id.in_(active_ids)).distinct().all():
            if cid:
                assigned.add(int(cid))
    for raw_name, in db.session.query(Escala.cooperado_nome).filter(
        Escala.cooperado_id.is_(None), Escala.cooperado_nome.isnot(None)
    ).distinct().all():
        cid = name_to_id.get(_norm(raw_name))
        if cid:
            assigned.add(cid)
    return len(active_ids), len(assigned)


# Fotos persistentes.
def _safe_coop_media(coop_id: int):
    coop = Cooperado.query.get_or_404(coop_id)
    if getattr(coop, "foto_bytes", None):
        response = send_file(
            BytesIO(bytes(coop.foto_bytes)),
            mimetype=getattr(coop, "foto_mime", None) or "image/jpeg",
            max_age=300,
        )
        response.headers["Cache-Control"] = "private, max-age=300"
        return response
    raw = (getattr(coop, "foto_url", None) or "").strip()
    if raw.startswith(("http://", "https://")):
        return redirect(raw)
    if raw:
        rel = raw.lstrip("/")
        if rel.startswith("static/"):
            rel = rel[7:]
        path = os.path.join(legacy.BASE_DIR, "static", rel)
        if os.path.isfile(path):
            return send_file(path, max_age=300)
    return redirect(url_for("static", filename="img/default.png"))


app.view_functions["admin_light_media_coop"] = _safe_coop_media


@app.get("/media/restaurante/<int:rest_id>", endpoint="admin_v10_media_rest")
def admin_v10_media_rest(rest_id: int):
    rest = Restaurante.query.get_or_404(rest_id)
    if getattr(rest, "foto_bytes", None):
        response = send_file(
            BytesIO(bytes(rest.foto_bytes)),
            mimetype=getattr(rest, "foto_mime", None) or "image/jpeg",
            max_age=300,
        )
        response.headers["Cache-Control"] = "private, max-age=300"
        return response
    raw = (getattr(rest, "foto_url", None) or "").strip()
    if raw.startswith(("http://", "https://")):
        return redirect(raw)
    if raw:
        rel = raw.lstrip("/")
        if rel.startswith("static/"):
            rel = rel[7:]
        path = os.path.join(legacy.BASE_DIR, "static", rel)
        if os.path.isfile(path):
            return send_file(path, max_age=300)
    return redirect(url_for("static", filename="img/default.png"))


_FINANCE = {
    "receitas": ("receitas", "Receitas Coop"),
    "despesas": ("despesas", "Despesas Coop"),
    "coop_receitas": ("coop_receitas", "Receitas Cooperados"),
    "coop_despesas": ("coop_despesas", "Despesas Cooperados"),
    "beneficios": ("beneficios", "Benefícios"),
}


@app.get("/admin/leve/financeiro/<tab>", endpoint="admin_v10_finance")
def admin_v10_finance(tab: str):
    item = _FINANCE.get(tab)
    if not item:
        return redirect(url_for("admin_light_summary"))
    denied = light._guard(item[0])
    if denied:
        return denied
    params = request.args.to_dict(flat=True)
    params.update({"tab": tab, "legacy": "1", "ajax_partial": tab})
    return render_template(
        "admin_partial_shell_v10.html",
        page_title=item[1],
        active_tab=tab,
        partial_url=url_for("admin_dashboard", **params),
        build=BUILD,
    )


_CONFIG_TEMPLATE = r'''
<!doctype html><html lang="pt-BR"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Configurações — COOPEX</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css">
<link rel="stylesheet" href="{{ url_for('static', filename='css/admin_light_v8.css', v=build) }}">
<style>
.v11-config-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.v11-config-card{background:#fff;border:1px solid #dfe5f1;border-radius:12px;padding:14px;box-shadow:0 2px 7px rgba(15,23,42,.04)}
.v11-config-card h2{margin:0 0 5px;font-size:18px;font-weight:900;color:#17213c}.v11-config-card p{margin:0 0 12px;font-size:14px;font-weight:700;color:#667085}
.v11-config-wide{grid-column:1/-1}.v11-admin-row{display:grid;grid-template-columns:1.2fr 1fr .7fr 2.3fr;gap:8px;align-items:center;padding:9px 0;border-bottom:1px solid #edf0f5}
.v11-perms{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:10px}.v11-perm{border:1px solid #e3e7ef;border-radius:9px;padding:9px;background:#fafbff}
.v11-perm strong{display:block;margin-bottom:6px}.v11-perm label{display:block;margin:4px 0;font-weight:700}
@media(max-width:1250px){.v11-config-grid{grid-template-columns:1fr}.v11-perms{grid-template-columns:repeat(2,minmax(0,1fr))}}
</style></head><body>
{% include '_admin_nav_v10.html' %}
<main class="alv8-page">
<div class="alv8-head"><div><h1 class="alv8-title">Configurações</h1><p class="alv8-sub">Organizado por grupos e sem carregar o painel antigo inteiro.</p></div></div>
{% with msgs=get_flashed_messages(with_categories=true) %}{% if msgs %}<div class="alv8-card">{% for cat,msg in msgs %}<div class="alv8-note">{{ msg }}</div>{% endfor %}</div>{% endif %}{% endwith %}
<div class="v11-config-grid">

<section class="v11-config-card"><h2><i class="bi bi-person-gear"></i> Administrador principal</h2><p>Usuário de acesso e alteração de senha.</p>
<form method="post" action="{{ url_for('alterar_admin') }}" class="alv8-filter">
<div class="alv8-field grow"><label>Usuário</label><input class="alv8-input" name="usuario" value="{{ admin_user.usuario if admin_user else '' }}" required></div>
<div class="alv8-field"><label>Nova senha</label><input class="alv8-input" type="password" name="nova_senha"></div>
<div class="alv8-field"><label>Confirmar senha</label><input class="alv8-input" type="password" name="confirmar_senha"></div>
<button class="alv8-btn primary"><i class="bi bi-save"></i> Salvar</button></form></section>

<section class="v11-config-card"><h2><i class="bi bi-calculator"></i> Parâmetros financeiros</h2><p>Salário mínimo e liberação de adiantamentos.</p>
<form method="post" action="{{ url_for('update_config') }}" class="alv8-filter"><div class="alv8-field grow"><label>Salário mínimo (R$)</label><input class="alv8-input" type="number" step="0.01" name="salario_minimo" value="{{ salario_minimo }}" required></div><button class="alv8-btn primary">Salvar</button></form>
<form method="post" action="{{ url_for('admin_config_adiantamentos') }}" class="alv8-filter" style="margin-top:12px"><label class="alv8-check"><input type="checkbox" name="bloquear_adiantamento" value="1" {{ 'checked' if bloquear_adiantamento else '' }}> Bloquear novas solicitações de adiantamento</label><button class="alv8-btn primary">Aplicar</button></form></section>

<section class="v11-config-card"><h2><i class="bi bi-download"></i> Backup</h2><p>Exportação completa dos cadastros e configurações.</p><a class="alv8-btn primary" href="{{ url_for('exportar_backup_admin_xlsx') }}"><i class="bi bi-file-earmark-excel"></i> Baixar backup XLSX</a></section>
<section class="v11-config-card"><h2><i class="bi bi-upload"></i> Restaurar backup</h2><p>Importe somente XLSX gerado pelo sistema.</p><form method="post" action="{{ url_for('importar_backup_admin_xlsx') }}" enctype="multipart/form-data" class="alv8-filter"><div class="alv8-field grow"><label>Arquivo XLSX</label><input class="alv8-input" type="file" name="arquivo_backup" accept=".xlsx" required></div><button class="alv8-btn primary">Importar</button></form></section>

<section class="v11-config-card v11-config-wide"><h2><i class="bi bi-person-plus"></i> Criar administrador</h2><p>Cadastre administradores secundários.</p>
<form method="post" action="{{ url_for('add_admin_secundario') }}" class="alv8-filter">
<div class="alv8-field grow"><label>Nome</label><input class="alv8-input" name="nome" required></div><div class="alv8-field"><label>Usuário</label><input class="alv8-input" name="usuario" required></div><div class="alv8-field"><label>Senha</label><input class="alv8-input" type="password" name="senha" required></div><div class="alv8-field"><label>Confirmar</label><input class="alv8-input" type="password" name="confirmar_senha" required></div><div class="alv8-field"><label>Status</label><select class="alv8-select" name="ativo"><option value="1">Ativo</option><option value="0">Inativo</option></select></div><button class="alv8-btn primary">Criar</button></form></section>

<section class="v11-config-card v11-config-wide"><h2><i class="bi bi-shield-lock"></i> Acessos e permissões</h2><p>O master mantém acesso total. Secundários podem ser limitados por aba.</p>
{% for adm in admins %}
<div class="v11-admin-row"><strong>{{ adm.nome or '—' }}</strong><span>{{ adm.usuario }}</span><span class="alv8-badge {{ 'ok' if adm.ativo else 'warn' }}">{{ 'Ativo' if adm.ativo else 'Inativo' }}</span>
<div class="alv8-inline">{% if not adm.is_master %}
<details><summary class="alv8-btn"><i class="bi bi-key"></i> Senha</summary><form method="post" action="{{ url_for('admin_reset_admin_password', usuario_id=adm.id) }}" class="alv8-edit-coop"><label>Nova senha</label><input class="alv8-input" type="password" name="nova_senha" required><label>Confirmar</label><input class="alv8-input" type="password" name="confirmar_senha" required><button class="alv8-btn primary">Salvar senha</button></form></details>
<form method="post" action="{{ url_for('admin_toggle_admin_status', usuario_id=adm.id) }}"><button class="alv8-btn">{{ 'Desativar' if adm.ativo else 'Ativar' }}</button></form>
<form method="post" action="{{ url_for('admin_delete_admin', usuario_id=adm.id) }}" onsubmit="return confirm('Excluir este administrador?');"><button class="alv8-btn danger">Excluir</button></form>
{% else %}<span class="alv8-badge">Master</span>{% endif %}</div></div>
{% if not adm.is_master %}<details class="alv8-card" style="margin:8px 0 12px"><summary><strong>Permissões de {{ adm.nome }}</strong></summary>
<form method="post" action="{{ url_for('admin_salvar_permissoes', usuario_id=adm.id) }}"><div class="v11-perms">
{% for aba in admin_abas %}{% set perm=admin_permissions_map.get(adm.id,{}).get(aba,{}) %}
<div class="v11-perm"><strong>{{ aba.replace('_',' ')|title }}</strong>
<label><input type="checkbox" name="perm_{{ aba }}_ver" {{ 'checked' if perm.get('ver') else '' }}> Ver</label>
<label><input type="checkbox" name="perm_{{ aba }}_criar" {{ 'checked' if perm.get('criar') else '' }}> Criar</label>
<label><input type="checkbox" name="perm_{{ aba }}_editar" {{ 'checked' if perm.get('editar') else '' }}> Editar</label>
<label><input type="checkbox" name="perm_{{ aba }}_excluir" {{ 'checked' if perm.get('excluir') else '' }}> Excluir</label></div>{% endfor %}
</div><button class="alv8-btn primary" style="margin-top:10px">Salvar permissões</button></form></details>{% endif %}
{% else %}<div class="alv8-empty">Nenhum administrador.</div>{% endfor %}
</section>
</div></main>
<script>(function(){document.querySelectorAll('.alv8-group>button').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();const g=btn.parentElement;document.querySelectorAll('.alv8-group').forEach(x=>{if(x!==g)x.classList.remove('open')});g.classList.toggle('open')}));document.addEventListener('click',()=>document.querySelectorAll('.alv8-group').forEach(x=>x.classList.remove('open')));})();</script>
</body></html>
'''


@app.get("/admin/leve/configuracoes", endpoint="admin_v11_config")
def admin_v11_config():
    denied = light._guard("config")
    if denied:
        return denied
    admin_user = Usuario.query.filter_by(tipo="admin", is_master=True).first()
    admins = Usuario.query.filter_by(tipo="admin").order_by(Usuario.is_master.desc(), Usuario.nome.asc(), Usuario.id.asc()).all()
    permission_map = {}
    if AdminPermissao is not None and admins:
        for p in AdminPermissao.query.filter(AdminPermissao.usuario_id.in_([a.id for a in admins])).all():
            permission_map.setdefault(p.usuario_id, {})[p.aba] = {
                "ver": bool(p.pode_ver), "criar": bool(p.pode_criar),
                "editar": bool(p.pode_editar), "excluir": bool(p.pode_excluir),
            }
    cfg = legacy.get_config()
    admin_abas = ["resumo","lancamentos","receitas","despesas","coop_receitas","coop_despesas","beneficios","cooperados","restaurantes","escalas","documentos","tabelas","avisos","avaliacoes","config"]
    return render_template_string(
        _CONFIG_TEMPLATE, active_tab="config", build=BUILD, admin_user=admin_user,
        admins=admins, admin_permissions_map=permission_map, admin_abas=admin_abas,
        salario_minimo=float(getattr(cfg, "salario_minimo", 0.0) or 0.0),
        bloquear_adiantamento=bool(getattr(cfg, "bloquear_adiantamento", False)),
    )


@app.get("/admin/leve/estabelecimentos", endpoint="admin_v10_establishments")
def admin_v10_establishments():
    denied = light._guard("restaurantes")
    if denied:
        return denied
    q_raw = request.args.get("q") or ""
    q = _norm(q_raw)
    status = (request.args.get("status") or "todos").strip().lower()
    result = []
    for r in Restaurante.query.order_by(Restaurante.nome.asc()).all():
        user = getattr(r, "usuario_ref", None)
        active = bool(getattr(r, "ativo", True) is not False and getattr(user, "ativo", True) is not False)
        if status == "ativos" and not active:
            continue
        if status == "inativos" and active:
            continue
        if q and q not in _norm(f"{r.nome} {getattr(user, 'usuario', '')}"):
            continue
        result.append(SimpleNamespace(rest=r, active=active))
    return render_template("admin_establishments_v10.html", page_title="Estabelecimentos", active_tab="estabelecimentos", rows=result, q=q_raw, status=status, build=BUILD)


@app.get("/admin/leve/blitz", endpoint="admin_v10_blitz")
def admin_v10_blitz():
    denied = light._guard("documentos")
    if denied:
        return denied
    q_raw = request.args.get("q") or ""
    q = _norm(q_raw)
    coops = _active_coops_v11()
    if q:
        coops = [c for c in coops if q in _norm(f"{c.nome} {getattr(c,'cnh_numero','')} {getattr(c,'placa','')}")]
    return render_template("admin_blitz_v10.html", page_title="Blitz — CNH e CRLV", active_tab="blitz", cooperados=coops[:300], q=q_raw, today=date.today(), build=BUILD)


@app.post("/admin/leve/blitz/<int:coop_id>", endpoint="admin_v10_blitz_save")
def admin_v10_blitz_save(coop_id: int):
    denied = light._guard("documentos")
    if denied:
        return denied
    coop = Cooperado.query.get_or_404(coop_id)
    coop.cnh_numero = (request.form.get("cnh_numero") or "").strip() or None
    coop.cnh_validade = _parse_date(request.form.get("cnh_validade"))
    coop.placa = (request.form.get("placa") or "").strip().upper() or None
    coop.placa_validade = _parse_date(request.form.get("placa_validade"))
    coop.ultima_atualizacao = datetime.utcnow()
    db.session.commit()
    flash(f"Blitz de {coop.nome} atualizada.", "success")
    return redirect(url_for("admin_v10_blitz", q=request.form.get("q") or ""))


def _documents_redirect_v10():
    return redirect(url_for("admin_v10_blitz"))


app.view_functions["admin_light_documents"] = _documents_redirect_v10


def _admin_light_scale_v11():
    denied = light._guard("escalas")
    if denied:
        return denied
    q_raw = (request.args.get("q") or "").strip()
    q = _norm(q_raw)
    active_ids, _, active_coops = _active_coop_ids_names_v11()
    coop_by_id = {c.id: c for c in active_coops}
    active_count, assigned_count = _scale_assignment_counts(active_coops)
    restaurants = Restaurante.query.filter(or_(Restaurante.ativo.is_(True), Restaurante.ativo.is_(None))).order_by(Restaurante.nome.asc()).all()
    scales, scale_rows, contracts = [], [], set()
    for s in Escala.query.order_by(Escala.id.asc()).limit(1400).all():
        if s.cooperado_id and s.cooperado_id not in active_ids:
            continue
        if not s.cooperado_id and (s.cooperado_nome or "").strip():
            known = Cooperado.query.filter(Cooperado.nome.ilike((s.cooperado_nome or "").strip())).first()
            if known and known.id not in active_ids:
                continue
        coop = coop_by_id.get(s.cooperado_id) if s.cooperado_id else None
        current_name = coop.nome if coop else (s.cooperado_nome or "").strip()
        searchable = _norm(" ".join(str(x or "") for x in (s.data, s.turno, s.horario, s.contrato, current_name)))
        if q and q not in searchable:
            continue
        scales.append(s)
        if (s.contrato or "").strip():
            contracts.add((s.contrato or "").strip())
        scale_rows.append({
            "id": s.id, "data": s.data or "", "weekday_num": preserve._weekday_num(s.data),
            "weekday_label": preserve._weekday_label(s.data), "turno": s.turno or "",
            "horario": s.horario or "", "contrato": s.contrato or "",
            "restaurante_id": s.restaurante_id, "cooperado_id": s.cooperado_id,
            "cooperado_nome": current_name,
            "cooperado_nome_livre": "" if s.cooperado_id else (s.cooperado_nome or ""),
        })
    contracts.update((r.nome or "").strip() for r in restaurants if (r.nome or "").strip())
    return light._render(
        "escala","Escala","Upload XLSX e escala semanal. A busca encontra qualquer parte do nome do cooperado.",
        q=q_raw, scales=scales, scale_rows=scale_rows, contract_options=sorted(contracts,key=_norm),
        coop_map=coop_by_id, rest_map={r.id:r for r in restaurants}, cooperados=active_coops,
        restaurantes=restaurants, scale_active_count=active_count, scale_assigned_count=assigned_count,
        scale_line_count=len(scale_rows),
    )


app.view_functions["admin_light_scale"] = _admin_light_scale_v11


def _scale_text(scale):
    if not scale:
        return "escala não localizada"
    return " · ".join(x for x in [str(scale.data or "").strip(), str(scale.horario or "").strip(), str(scale.contrato or "").strip()] if x) or f"escala #{scale.id}"


def _line_text(line):
    return " · ".join(x for x in [str(line.get("dia") or "").strip(),str(line.get("turno_horario") or "").strip(),str(line.get("contrato") or "").strip()] if x)


def _admin_light_swaps_v11():
    denied = light._guard("escalas")
    if denied:
        return denied
    active_ids, _, active_coops = _active_coop_ids_names_v11()
    rows = TrocaSolicitacao.query.filter(TrocaSolicitacao.solicitante_id.in_(active_ids),TrocaSolicitacao.destino_id.in_(active_ids)).order_by(TrocaSolicitacao.id.desc()).limit(160).all() if active_ids else []
    coop_map = {c.id:c for c in active_coops}
    scale_ids = {t.origem_escala_id for t in rows if t.origem_escala_id}
    scale_map = {s.id:s for s in Escala.query.filter(Escala.id.in_(scale_ids)).all()} if scale_ids else {}
    result, parser = [], getattr(legacy,"_parse_linhas_from_msg",None)
    for t in rows:
        sn = coop_map.get(t.solicitante_id).nome if coop_map.get(t.solicitante_id) else "Cooperado"
        dn = coop_map.get(t.destino_id).nome if coop_map.get(t.destino_id) else "Cooperado"
        origem = scale_map.get(t.origem_escala_id)
        linhas = []
        if parser and (t.status or "").lower() in {"aprovada","aceita"}:
            try: linhas = parser(t.mensagem) or []
            except Exception: linhas = []
        if len(linhas)>=2: resumo=f"{sn} — {_line_text(linhas[0])} trocou com {dn} — {_line_text(linhas[1])}"
        elif len(linhas)==1: resumo=f"{sn} — {_line_text(linhas[0])} passou a escala para {dn}"
        elif (t.status or "").lower()=="pendente": resumo=f"{sn} — {_scale_text(origem)} solicitou troca com {dn}"
        elif (t.status or "").lower() in {"recusada","rejeitada"}: resumo=f"{sn} — {_scale_text(origem)} teve a troca com {dn} recusada"
        else: resumo=f"{sn} — {_scale_text(origem)} trocou com {dn}"
        result.append(SimpleNamespace(id=t.id,status=t.status or "—",criada_em=getattr(t,"criada_em",None),aplicada_em=getattr(t,"aplicada_em",None),resumo=resumo))
    return light._render("trocas","Trocas de escala","Uma linha por troca, sem redundância.",swap_rows=result,trocas=rows,coop_map=coop_map,scale_map=scale_map)


app.view_functions["admin_light_swaps"] = _admin_light_swaps_v11


def _coop_counts_v11():
    archived = light._archived_ids()
    rows = Cooperado.query.join(Usuario,Cooperado.usuario_id==Usuario.id).add_entity(Usuario).all()
    active_count=inactive_count=archived_count=0
    for coop,user in rows:
        if coop.id in archived: archived_count+=1
        elif user.ativo is False: inactive_count+=1
        else: active_count+=1
    _, assigned_count = _scale_assignment_counts(_active_coops_v11())
    return active_count,inactive_count,archived_count,assigned_count


def _admin_light_cooperatives_v11():
    denied=light._guard("cooperados")
    if denied:return denied
    status=(request.args.get("status") or "ativos").strip().lower()
    if status not in {"ativos","inativos","excluidos"}:status="ativos"
    q_raw=(request.args.get("q") or "").strip();q=_norm(q_raw)
    archived=light._archived_ids()
    rows=Cooperado.query.join(Usuario,Cooperado.usuario_id==Usuario.id).add_entity(Usuario).order_by(Cooperado.nome.asc()).all()
    result=[]
    for coop,user in rows:
        is_archived=coop.id in archived
        is_active=(user.ativo is not False) and not is_archived
        if status=="ativos" and not is_active:continue
        if status=="inativos" and (is_active or is_archived):continue
        if status=="excluidos" and not is_archived:continue
        if q and q not in _norm(f"{coop.nome or ''} {coop.telefone or ''} {user.usuario or ''}"):continue
        result.append(SimpleNamespace(coop=coop,user=user,active=is_active,archived=is_archived,phone=light._fmt_phone(coop.telefone)))
    ca,ci,ce,cs=_coop_counts_v11()
    return light._render("cooperados","Cooperados","Desativado sai da operação sem perder histórico.",cooperados=result[:350],q=q_raw,status=status,count_ativos=ca,count_inativos=ci,count_excluidos=ce,count_com_escala=cs)


app.view_functions["admin_light_cooperatives"]=_admin_light_cooperatives_v11


def _set_coop_status(coop,user,action):
    archive=db.session.get(light.CooperadoArquivadoV8,coop.id)
    if action=="ativar":
        _PROTECTED_INACTIVE_USER_IDS.discard(int(user.id))
        user.ativo=True
        if archive:db.session.delete(archive)
        return f"{coop.nome} foi ativado."
    if action=="desativar":
        _PROTECTED_INACTIVE_USER_IDS.add(int(user.id));user.ativo=False
        return f"{coop.nome} foi desativado e saiu de escala, contratos e trocas."
    if action=="excluir":
        _PROTECTED_INACTIVE_USER_IDS.add(int(user.id));user.ativo=False
        if not archive:
            archive=light.CooperadoArquivadoV8(cooperado_id=coop.id);db.session.add(archive)
        archive.nome_original=coop.nome;archive.telefone_original=coop.telefone;archive.excluido_em=datetime.utcnow()
        return f"{coop.nome} foi excluído da operação. O histórico foi preservado."
    raise ValueError


def _admin_light_coop_status_v11(coop_id):
    denied=light._guard("cooperados")
    if denied:return denied
    coop=Cooperado.query.get_or_404(coop_id);user=Usuario.query.get_or_404(coop.usuario_id)
    action=(request.form.get("acao") or "").strip().lower();current=(request.form.get("status") or "ativos").strip().lower()
    try:
        flash(_set_coop_status(coop,user,action),"success");db.session.commit()
    except ValueError:
        db.session.rollback();flash("Ação inválida.","warning")
    return redirect(url_for("admin_light_cooperatives",status=current))


app.view_functions["admin_light_coop_status"]=_admin_light_coop_status_v11


@app.post("/admin/leve/cooperados/status-lote",endpoint="admin_v11_coop_status_batch")
def admin_v11_coop_status_batch():
    denied=light._guard("cooperados")
    if denied:return denied
    ids=[int(x) for x in request.form.getlist("coop_ids") if str(x).isdigit()]
    action=(request.form.get("acao") or "").strip().lower();current=(request.form.get("status") or "ativos").strip().lower()
    if not ids:
        flash("Selecione pelo menos um cooperado.","warning");return redirect(url_for("admin_light_cooperatives",status=current))
    try:
        changed=0
        for coop in Cooperado.query.filter(Cooperado.id.in_(ids)).all():
            user=Usuario.query.get(coop.usuario_id)
            if user:_set_coop_status(coop,user,action);changed+=1
        db.session.commit();flash(f"{changed} cooperado(s) atualizado(s).","success")
    except Exception:
        db.session.rollback();app.logger.exception("Falha na alteração em lote");flash("Não foi possível concluir a alteração em lote.","danger")
    return redirect(url_for("admin_light_cooperatives",status=current))


@app.after_request
def _admin_v11_redirect_light(response):
    try:
        if (session.get("user_tipo") or "").strip().lower()!="admin":return response
        if response.status_code not in (301,302,303,307,308):return response
        parsed=urlparse(response.headers.get("Location") or "")
        tab=(parse_qs(parsed.query).get("tab") or [""])[0]
        if tab in _FINANCE:response.headers["Location"]=url_for("admin_v10_finance",tab=tab)
        elif tab=="restaurantes":response.headers["Location"]=url_for("admin_v10_establishments")
        elif tab=="cooperados":response.headers["Location"]=url_for("admin_light_cooperatives")
        elif tab=="config":response.headers["Location"]=url_for("admin_v11_config")
    except Exception:
        app.logger.exception("Falha ao redirecionar retorno do Admin V11")
    return response


_READABILITY_CSS='''<style id="coopex-v11-readability">
body{font-weight:700!important}.alv8-brand-copy small{font-size:12px!important}.alv8-brand-copy strong{font-size:17px!important}
.alv8-group>button,.alv8-direct{font-size:16px!important;font-weight:800!important}.alv8-menu a{font-size:15px!important;font-weight:800!important}
.alv8-title{font-size:25px!important;font-weight:900!important}.alv8-sub{font-size:15px!important;font-weight:700!important}
.alv8-card h3{font-size:16px!important;font-weight:900!important}.alv8-card p{font-size:14px!important;font-weight:700!important}
.alv8-kpi span{font-size:12px!important;font-weight:900!important}.alv8-kpi strong{font-size:20px!important;font-weight:900!important}
.alv8-field label,.alv8-edit-coop label{font-size:13px!important;font-weight:900!important}.alv8-input,.alv8-select,.alv8-textarea{font-size:15px!important;font-weight:700!important}
.alv8-check{font-size:14px!important;font-weight:800!important}.alv8-btn{font-size:14px!important;font-weight:900!important}
.alv8-table{font-size:14px!important;font-weight:700!important}.alv8-table th{font-size:12px!important;font-weight:900!important}
.alv8-badge{font-size:12px!important;font-weight:900!important}.alv8-person strong{font-size:14px!important}.alv8-person small{font-size:11px!important;font-weight:700!important}
.alv8-note{font-size:14px!important;font-weight:800!important}.alv8-tabs a,.alv8-tabs button{font-size:13px!important;font-weight:900!important}
.alv8-compact .alv8-input,.alv8-compact .alv8-select{font-size:13px!important}.alv8-compact .alv8-btn{font-size:12px!important}
.alv8-table tfoot th,.alv8-table tfoot td{background:#eef3ff!important;color:#10295f!important;font-weight:900!important;border-top:2px solid #cbd8f3!important}
</style>'''

_FINANCE_PRO_CSS='''<style id="coopex-v11-finance">
.v10-partial-page{background:#f5f7ff!important}#v10PartialMount{font-weight:700!important}
#v10PartialMount .page-header,#v10PartialMount .panel-head{display:none!important}
#v10PartialMount .card,#v10PartialMount .card-modern{border:1px solid #dfe5f1!important;border-radius:12px!important;box-shadow:0 2px 7px rgba(15,23,42,.04)!important;background:#fff!important}
#v10PartialMount .card-body{padding:14px!important}#v10PartialMount h4,#v10PartialMount h5,#v10PartialMount h6{color:#17213c!important;font-weight:900!important}
#v10PartialMount .form-label,#v10PartialMount label{font-size:14px!important;font-weight:900!important;color:#344054!important}
#v10PartialMount .form-control,#v10PartialMount .form-select,#v10PartialMount .input-group-text{font-size:15px!important;font-weight:700!important;border-radius:8px!important;border-color:#d8deea!important;min-height:38px!important}
#v10PartialMount .btn{font-size:14px!important;font-weight:900!important;border-radius:8px!important}#v10PartialMount .table{font-size:14px!important;font-weight:700!important}
#v10PartialMount .table th{font-size:12px!important;font-weight:900!important;background:#f3f6fc!important;color:#344054!important}#v10PartialMount .table td{font-weight:700!important}
#v10PartialMount .badge{font-size:12px!important;font-weight:900!important}#v10PartialMount .text-muted,#v10PartialMount .small,#v10PartialMount small{font-size:13px!important;font-weight:700!important;color:#667085!important}
</style>'''

_NAV_SCRIPT='''<script id="coopex-v11-nav">(function(){document.querySelectorAll('.alv8-group>button').forEach(btn=>{if(btn.dataset.v11bound)return;btn.dataset.v11bound='1';btn.addEventListener('click',e=>{e.preventDefault();e.stopPropagation();const g=btn.parentElement;document.querySelectorAll('.alv8-group').forEach(x=>{if(x!==g)x.classList.remove('open')});g.classList.toggle('open')})});document.addEventListener('click',()=>document.querySelectorAll('.alv8-group').forEach(x=>x.classList.remove('open')));})();</script>'''

_COOP_BLOCK=r'''  {% elif view=='cooperados' %}
    <div class="alv8-grid alv8-grid-5">
      <div class="alv8-kpi"><span>Ativos</span><strong id="v11CountAtivos">{{ count_ativos|default(0) }}</strong></div>
      <div class="alv8-kpi"><span>Desativados</span><strong id="v11CountInativos">{{ count_inativos|default(0) }}</strong></div>
      <div class="alv8-kpi"><span>Receberam escala</span><strong>{{ count_com_escala|default(0) }}</strong></div>
      <div class="alv8-kpi"><span>Excluídos / arquivo</span><strong id="v11CountExcluidos">{{ count_excluidos|default(0) }}</strong></div>
    </div>
    <div class="alv8-tabs">
      <a class="{{ 'active' if status=='ativos' else '' }}" href="{{ url_for('admin_light_cooperatives',status='ativos') }}">Ativos ({{ count_ativos|default(0) }})</a>
      <a class="{{ 'active' if status=='inativos' else '' }}" href="{{ url_for('admin_light_cooperatives',status='inativos') }}">Desativados ({{ count_inativos|default(0) }})</a>
      <a class="{{ 'active' if status=='excluidos' else '' }}" href="{{ url_for('admin_light_cooperatives',status='excluidos') }}">Excluídos/arquivo ({{ count_excluidos|default(0) }})</a>
    </div>
    <details class="alv8-card"><summary><strong><i class="bi bi-person-plus"></i> Novo cooperado</strong></summary>
      <form method="post" action="{{ url_for('add_cooperado') }}" enctype="multipart/form-data" class="alv8-filter" style="margin-top:10px">
        <div class="alv8-field grow"><label>Nome</label><input class="alv8-input" name="nome" required></div><div class="alv8-field"><label>Telefone</label><input class="alv8-input" name="telefone"></div><div class="alv8-field"><label>Usuário</label><input class="alv8-input" name="usuario" required></div><div class="alv8-field"><label>Senha</label><input class="alv8-input" type="password" name="senha" required></div><div class="alv8-field"><label>Foto</label><input class="alv8-input" type="file" name="foto" accept="image/*"></div><button class="alv8-btn primary">Cadastrar</button>
      </form>
    </details>
    <form class="alv8-card alv8-filter" method="get"><input type="hidden" name="status" value="{{ status }}"><div class="alv8-field grow"><label>Buscar</label><input class="alv8-input" name="q" value="{{ q }}" placeholder="Nome, telefone ou usuário"></div><button class="alv8-btn primary">Buscar</button></form>
    <form id="v11BatchForm" method="post" action="{{ url_for('admin_v11_coop_status_batch') }}" class="alv8-card alv8-filter"><input type="hidden" name="status" value="{{ status }}"><strong>Ação em lote:</strong><button class="alv8-btn" name="acao" value="desativar">Desativar selecionados</button><button class="alv8-btn ok" name="acao" value="ativar">Ativar selecionados</button><button class="alv8-btn danger" name="acao" value="excluir" onclick="return confirm('Excluir os selecionados da operação? O histórico será preservado.');">Excluir selecionados</button></form>
    <div class="alv8-card"><div class="alv8-table-wrap"><table class="alv8-table"><thead><tr><th><input id="v11SelectAll" type="checkbox"></th><th>Cooperado</th><th>Telefone</th><th>Usuário</th><th>Status</th><th>Editar cadastro</th><th>Ações</th></tr></thead><tbody>
    {% for row in cooperados %}<tr data-v11-coop-row="{{ row.coop.id }}" data-v11-status="{{ 'excluidos' if row.archived else ('ativos' if row.active else 'inativos') }}">
      <td><input class="v11-coop-check" type="checkbox" name="coop_ids" value="{{ row.coop.id }}" form="v11BatchForm"></td>
      <td><div class="alv8-person"><img loading="lazy" class="alv8-photo" src="{{ url_for('admin_light_media_coop',coop_id=row.coop.id) }}?v={{ row.coop.ultima_atualizacao.timestamp() if row.coop.ultima_atualizacao else row.coop.id }}" alt=""><div><strong>{{ row.coop.nome }}</strong><small>#{{ row.coop.id }}</small></div></div></td><td>{{ row.phone or '—' }}</td><td>{{ row.user.usuario }}</td>
      <td class="v11-status-cell">{% if row.archived %}<span class="alv8-badge bad">Excluído</span>{% elif row.active %}<span class="alv8-badge ok">Ativo</span>{% else %}<span class="alv8-badge warn">Desativado</span>{% endif %}</td>
      <td><details><summary class="alv8-btn edit"><i class="bi bi-pencil"></i> Editar</summary><form enctype="multipart/form-data" method="post" action="{{ url_for('admin_light_coop_save',coop_id=row.coop.id) }}" class="alv8-edit-coop"><input type="hidden" name="status" value="{{ status }}"><label>Nome</label><input class="alv8-input" name="nome" value="{{ row.coop.nome }}"><label>Telefone</label><input class="alv8-input" name="telefone" value="{{ row.coop.telefone or '' }}"><label>Usuário de acesso</label><input class="alv8-input" name="usuario" value="{{ row.user.usuario }}"><label>Nova senha</label><input class="alv8-input" type="password" name="senha" placeholder="Deixe em branco para manter a atual"><label>Foto</label><input class="alv8-input" type="file" name="foto" accept="image/*"><button class="alv8-btn primary">Salvar cadastro</button></form></details></td>
      <td><div class="alv8-inline">{% if row.archived or not row.active %}<form class="v11-status-form" method="post" action="{{ url_for('admin_light_coop_status',coop_id=row.coop.id) }}"><input type="hidden" name="status" value="{{ status }}"><button class="alv8-btn ok" name="acao" value="ativar">Ativar</button></form>{% else %}<form class="v11-status-form" method="post" action="{{ url_for('admin_light_coop_status',coop_id=row.coop.id) }}"><input type="hidden" name="status" value="{{ status }}"><button class="alv8-btn" name="acao" value="desativar">Desativar</button></form><form class="v11-status-form" method="post" action="{{ url_for('admin_light_coop_status',coop_id=row.coop.id) }}"><input type="hidden" name="status" value="{{ status }}"><button class="alv8-btn danger" name="acao" value="excluir" onclick="return confirm('Excluir do cadastro operacional? O histórico será preservado.');">Excluir</button></form>{% endif %}</div></td>
    </tr>{% else %}<tr><td colspan="7" class="alv8-empty">Nenhum cooperado neste status.</td></tr>{% endfor %}</tbody></table></div></div>
    <script>(function(){const all=document.getElementById('v11SelectAll');all?.addEventListener('change',()=>document.querySelectorAll('.v11-coop-check').forEach(x=>x.checked=all.checked));function n(id){return Number(document.getElementById(id)?.textContent||0)}function setn(id,v){const e=document.getElementById(id);if(e)e.textContent=Math.max(0,v)}document.querySelectorAll('.v11-status-form').forEach(form=>form.addEventListener('submit',async ev=>{ev.preventDefault();const btn=ev.submitter,action=btn?.value||'',row=form.closest('[data-v11-coop-row]'),old=row?.dataset.v11Status||'';btn.disabled=true;try{const fd=new FormData(form);fd.set('acao',action);const r=await fetch(form.action,{method:'POST',body:fd,credentials:'same-origin',cache:'no-store'});if(!r.ok)throw new Error();if(action==='desativar'){row.dataset.v11Status='inativos';row.querySelector('.v11-status-cell').innerHTML='<span class="alv8-badge warn">Desativado</span>';if(old==='ativos'){setn('v11CountAtivos',n('v11CountAtivos')-1);setn('v11CountInativos',n('v11CountInativos')+1)}}else if(action==='ativar'){row.dataset.v11Status='ativos';row.querySelector('.v11-status-cell').innerHTML='<span class="alv8-badge ok">Ativo</span>';if(old==='inativos'){setn('v11CountInativos',n('v11CountInativos')-1);setn('v11CountAtivos',n('v11CountAtivos')+1)}if(old==='excluidos'){setn('v11CountExcluidos',n('v11CountExcluidos')-1);setn('v11CountAtivos',n('v11CountAtivos')+1)}}else if(action==='excluir'){row.dataset.v11Status='excluidos';row.querySelector('.v11-status-cell').innerHTML='<span class="alv8-badge bad">Excluído</span>';if(old==='ativos')setn('v11CountAtivos',n('v11CountAtivos')-1);if(old==='inativos')setn('v11CountInativos',n('v11CountInativos')-1);setn('v11CountExcluidos',n('v11CountExcluidos')+1)}}catch(e){alert('Não foi possível atualizar o cooperado.');btn.disabled=false}}));})();</script>
'''


def _same_nav(source):
    start=source.find('<header class="alv8-top">');end=source.find("</header>",start)
    if start>=0 and end>=0:return source[:start]+"{% set active_tab=view %}{% include '_admin_nav_v10.html' %}"+source[end+9:]
    return source


def _summary_footer(source):
    start=source.find("{% if view=='resumo' %}");end=source.find("{% elif view=='lancamentos' %}",start)
    if start<0 or end<0:return source
    block=source[start:end];target="</tbody></table>";pos=block.find(target)
    if pos<0 or "<tfoot" in block:return source
    footer=r'''{% set ns_total_receber=namespace(v=0) %}{% for rr in rows %}{% set ns_total_receber.v=ns_total_receber.v+(rr.receber or 0) %}{% endfor %}</tbody><tfoot><tr><th>TOTAL</th><th>R$ {{ totals.prod|brl }}</th><th>R$ {{ totals.inss|brl }}</th><th>R$ {{ totals.sest|brl }}</th><th>R$ {{ totals.rec_coop|brl }}</th><th>R$ {{ totals.desp_coop|brl }}</th><th>R$ {{ totals.adiant|brl }}</th><th>R$ {{ ns_total_receber.v|brl }}</th></tr></tfoot></table>'''
    block=block[:pos]+footer+block[pos+len(target):]
    return source[:start]+block+source[end:]


def _launch_footer(source):
    start=source.find("{% elif view=='lancamentos' %}");end=source.find("{% elif view=='escala' %}",start)
    if start<0 or end<0:return source
    block=source[start:end]
    p=block.find('<label class="alv8-check"><input type="checkbox" name="considerar_periodo"')
    if p>=0:
        e=block.find("</label>",p)
        if e>=0:block=block[:p]+block[e+8:]
    p=block.find('<div class="alv8-weekdays">')
    if p>=0:
        e=block.find("</div>",p)
        if e>=0:block=block[:p]+block[e+6:]
    target="</tbody></table>";pos=block.find(target)
    if pos>=0 and "<tfoot" not in block:
        footer=r'''{% set ns_ent=namespace(v=0) %}{% for ll in launches %}{% set ns_ent.v=ns_ent.v+(ll.qtd_entregas or 0) %}{% endfor %}</tbody><tfoot><tr><th colspan="3">TOTAL</th><th>R$ {{ launch_total|brl }}</th><th>{{ ns_ent.v }}</th><th></th><th></th><th>R$ {{ launch_total_inss|brl }}</th><th>R$ {{ launch_total_sest|brl }}</th><th>R$ {{ launch_total_liquido|brl }}</th><th></th></tr></tfoot></table>'''
        block=block[:pos]+footer+block[pos+len(target):]
    return source[:start]+block+source[end:]


def _scale_counts(source):
    marker="{% elif view=='escala' %}"
    if marker not in source or "scale_active_count" in source:return source
    cards=r'''<div class="alv8-grid alv8-grid-5"><div class="alv8-kpi"><span>Cooperados ativos</span><strong>{{ scale_active_count|default(0) }}</strong></div><div class="alv8-kpi"><span>Receberam escala</span><strong>{{ scale_assigned_count|default(0) }}</strong></div><div class="alv8-kpi"><span>Linhas da escala</span><strong>{{ scale_line_count|default(0) }}</strong></div></div>'''
    return source.replace(marker,marker+cards,1)


def _replace_coop_block(source):
    start=source.find("  {% elif view=='cooperados' %}");end=source.find("  {% elif view=='avaliacoes' %}",start)
    return source[:start]+_COOP_BLOCK+source[end:] if start>=0 and end>start else source


def _install_template_v11():
    loader=app.jinja_loader
    if not loader or getattr(loader,"_coopex_admin_v11",False):return
    original=loader.get_source
    def get_source(environment,template):
        source,filename,uptodate=original(environment,template)
        if template in {"admin_light_v8.html","admin_dashboard.html","admin_lancamentos.html"}:
            source=source.replace("'%.2f'|format(","brl(").replace(" ({{ r.periodo }})","").replace("({{ r.periodo }})","")
        if template=="_admin_nav_v10.html":
            source=source.replace('href="/admin?tab=config&legacy=1"','href="{{ url_for(\'admin_v11_config\') }}"')
        if template=="admin_light_v8.html":
            source=_same_nav(source)
            for old,new in {
                '/admin?tab=receitas&legacy=1':"{{ url_for('admin_v10_finance',tab='receitas') }}",
                '/admin?tab=despesas&legacy=1':"{{ url_for('admin_v10_finance',tab='despesas') }}",
                '/admin?tab=coop_receitas&legacy=1':"{{ url_for('admin_v10_finance',tab='coop_receitas') }}",
                '/admin?tab=coop_despesas&legacy=1':"{{ url_for('admin_v10_finance',tab='coop_despesas') }}",
                '/admin?tab=beneficios&legacy=1':"{{ url_for('admin_v10_finance',tab='beneficios') }}",
                '/admin?tab=restaurantes&legacy=1':"{{ url_for('admin_v10_establishments') }}",
                '/admin/avaliacoes?legacy=1':"{{ url_for('admin_light_ratings') }}",
                '/admin/tabelas?legacy=1':"{{ url_for('admin_light_tables') }}",
                '/admin/avisos?legacy=1':"{{ url_for('admin_light_notices') }}",
                '/admin?tab=config&legacy=1':"{{ url_for('admin_v11_config') }}",
            }.items():source=source.replace(old,new)
            source=source.replace('<a class="alv8-btn" href="/admin/documentos?legacy=1"><i class="bi bi-folder2"></i> Documentos</a>','<a class="alv8-btn" href="{{ url_for(\'admin_v10_blitz\') }}"><i class="bi bi-shield-check"></i> Blitz</a>',1)
            marker='<details class="alv8-card"><summary><strong><i class="bi bi-person-plus"></i> Acrescentar alguém / nova linha na escala</strong></summary>'
            if "Upload da Escala (.xlsx)" not in source:
                upload=r'''<div class="alv8-card"><div class="alv8-section-head"><div><h3>Upload da Escala (.xlsx)</h3><p>Envie a planilha oficial. O sistema separa a escala de cada cooperado.</p></div></div><form method="post" action="{{ url_for('upload_escala') }}" enctype="multipart/form-data" class="alv8-filter"><div class="alv8-field grow"><label>Planilha XLSX</label><input class="alv8-input" type="file" name="arquivo" accept=".xlsx" required></div><button class="alv8-btn primary" type="submit"><i class="bi bi-file-earmark-spreadsheet"></i> Enviar escala</button></form></div>'''
                source=source.replace(marker,upload+marker,1)
            old="const coopOptions=[{% for c in cooperados|default([]) %}{id:{{ c.id }},nome:{{ c.nome|tojson }}}{% if not loop.last %},{% endif %}{% endfor %}];"
            new="const coopOptions={% if view=='escala' %}[{% for c in cooperados|default([]) %}{id:{{ c.id }},nome:{{ c.nome|tojson }}}{% if not loop.last %},{% endif %}{% endfor %}]{% else %}[]{% endif %};"
            source=source.replace(old,new,1)
            oldm="function matches(r){const n=(nameF?.value||'').trim().toLowerCase(),ct=(contractF?.value||'').trim().toLowerCase(),free=!!freeF?.checked;const rn=String(r.cooperado_nome||r.cooperado_nome_livre||'').toLowerCase(),rc=String(r.contrato||'').toLowerCase();if(currentDay!=='all'&&String(r.weekday_num)!==String(currentDay))return false;if(n&&!rn.includes(n))return false;if(ct&&rc!==ct)return false;if(free&&(r.cooperado_id||String(r.cooperado_nome_livre||'').trim()))return false;return true}"
            newm="function normV11(v){return String(v??'').normalize('NFD').replace(/[\\u0300-\\u036f]/g,'').replace(/_/g,' ').toLowerCase().replace(/\\s+/g,' ').trim()} function matches(r){const n=normV11(nameF?.value||''),ct=normV11(contractF?.value||''),free=!!freeF?.checked;const rn=normV11(r.cooperado_nome||r.cooperado_nome_livre||''),rc=normV11(r.contrato||'');if(currentDay!=='all'&&String(r.weekday_num)!==String(currentDay))return false;if(n&&!rn.includes(n))return false;if(ct&&rc!==ct)return false;if(free&&(r.cooperado_id||String(r.cooperado_nome_livre||'').trim()))return false;return true}"
            source=source.replace(oldm,newm,1)
            source=_summary_footer(source);source=_launch_footer(source);source=_scale_counts(source);source=_replace_coop_block(source)
            start=source.find("  {% elif view=='trocas' %}");end=source.find("  {% elif view=='historico' %}",start)
            if start>=0 and end>start:
                simple=r'''  {% elif view=='trocas' %}<div class="alv8-card"><div class="alv8-table-wrap"><table class="alv8-table"><thead><tr><th>Troca</th><th>Status</th><th>Data</th>{% if admin_can_edit_escalas %}<th>Ações</th>{% endif %}</tr></thead><tbody>{% for t in swap_rows|default([]) %}<tr><td><strong>{{ t.resumo }}</strong></td><td><span class="alv8-badge {{ 'warn' if t.status=='pendente' else 'ok' if t.status in ['aprovada','aceita'] else 'bad' }}">{{ t.status }}</span></td><td>{{ t.aplicada_em.strftime('%d/%m/%Y %H:%M') if t.aplicada_em else (t.criada_em.strftime('%d/%m/%Y %H:%M') if t.criada_em else '—') }}</td>{% if admin_can_edit_escalas %}<td>{% if t.status=='pendente' %}<div class="alv8-inline"><form method="post" action="{{ url_for('admin_aprovar_troca',id=t.id) }}" onsubmit="return confirm('Confirmar aprovação desta troca?');"><button class="alv8-btn ok" type="submit"><i class="bi bi-check-circle"></i> Aprovar</button></form><form method="post" action="{{ url_for('admin_recusar_troca',id=t.id) }}" onsubmit="return confirm('Recusar esta solicitação?');"><button class="alv8-btn danger" type="submit"><i class="bi bi-x-circle"></i> Recusar</button></form></div>{% else %}—{% endif %}</td>{% endif %}</tr>{% else %}<tr><td colspan="{{ 4 if admin_can_edit_escalas else 3 }}" class="alv8-empty">Nenhuma troca.</td></tr>{% endfor %}</tbody></table></div></div>'''
                source=source[:start]+simple+source[end:]
            source=source.replace("</head>",_READABILITY_CSS+"</head>",1)
        if template in {"admin_partial_shell_v10.html","admin_establishments_v10.html","admin_blitz_v10.html"}:
            extra=_READABILITY_CSS+(_FINANCE_PRO_CSS if template=="admin_partial_shell_v10.html" else "")
            source=source.replace("</head>",extra+"</head>",1)
            source=source.replace("</body>",_NAV_SCRIPT+"</body>",1)
        return source,filename,uptodate
    loader.get_source=get_source;loader._coopex_admin_v11=True;app.jinja_env.cache.clear()


_install_template_v11()
app.logger.info("Admin V11 carregado: menu único, financeiro/config leve, fonte ampliada, totais e status persistente.")
