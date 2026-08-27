from __future__ import annotations

from datetime import date, datetime
from functools import wraps

from flask import abort, flash, redirect, render_template_string, request, session, url_for

import app as legacy

app = legacy.app
db = legacy.db
DespesaCooperativa = legacy.DespesaCooperativa


class DespesaCoopControleV16(db.Model):
    __tablename__ = "despesas_coop_controle_v16"

    id = db.Column(db.Integer, primary_key=True)
    despesa_id = db.Column(
        db.Integer,
        db.ForeignKey("despesas_coop.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    tipo = db.Column(db.String(20), nullable=True, index=True)  # fixa | variavel
    pago = db.Column(db.Boolean, nullable=False, default=False, index=True)
    data_pagamento = db.Column(db.Date, nullable=True, index=True)
    criado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    atualizado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


with app.app_context():
    DespesaCoopControleV16.__table__.create(bind=db.engine, checkfirst=True)


def _allowed(action: str) -> bool:
    if (session.get("user_tipo") or "").strip().lower() != "admin":
        return False
    try:
        return bool(legacy.is_admin_master() or legacy.admin_has_perm("despesas", action))
    except Exception:
        return False


def _require(action: str):
    if not _allowed(action):
        abort(403)


def _parse_date(raw):
    value = str(raw or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def _money(raw, default=0.0):
    try:
        return legacy.parse_valor_monetario(raw, default)
    except Exception:
        try:
            return round(float(raw or default), 2)
        except Exception:
            return round(float(default or 0.0), 2)


def _control(despesa_id: int, create=False):
    item = DespesaCoopControleV16.query.filter_by(despesa_id=despesa_id).first()
    if not item and create:
        item = DespesaCoopControleV16(despesa_id=despesa_id, pago=False)
        db.session.add(item)
        db.session.flush()
    return item


@app.post("/admin/leve/despesas-coop/nova", endpoint="admin_v16_despesa_create")
def admin_v16_despesa_create():
    _require("criar")
    descricao = (request.form.get("descricao") or "").strip()
    valor = _money(request.form.get("valor"))
    data_ref = _parse_date(request.form.get("data"))
    tipo = (request.form.get("tipo") or "").strip().lower()
    if not descricao or valor < 0 or not data_ref:
        flash("Preencha descrição, valor e data corretamente.", "warning")
        return redirect(url_for("admin_v10_finance", tab="despesas"))
    if tipo not in {"fixa", "variavel"}:
        flash("Informe se a despesa é Fixa ou Variável.", "warning")
        return redirect(url_for("admin_v10_finance", tab="despesas"))
    try:
        desp = DespesaCooperativa(descricao=descricao, valor=valor, data=data_ref)
        db.session.add(desp)
        db.session.flush()
        db.session.add(DespesaCoopControleV16(despesa_id=desp.id, tipo=tipo, pago=False))
        db.session.commit()
        flash("Despesa cadastrada.", "success")
    except Exception:
        db.session.rollback()
        app.logger.exception("Falha ao criar despesa da cooperativa V16")
        flash("Não foi possível cadastrar a despesa.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


@app.post("/admin/leve/despesas-coop/<int:item_id>/editar", endpoint="admin_v16_despesa_edit")
def admin_v16_despesa_edit(item_id: int):
    _require("editar")
    desp = DespesaCooperativa.query.get_or_404(item_id)
    descricao = (request.form.get("descricao") or "").strip()
    valor = _money(request.form.get("valor"), desp.valor or 0.0)
    data_ref = _parse_date(request.form.get("data"))
    tipo = (request.form.get("tipo") or "").strip().lower()
    if not descricao or valor < 0 or not data_ref or tipo not in {"fixa", "variavel"}:
        flash("Preencha descrição, valor, data e tipo corretamente.", "warning")
        return redirect(url_for("admin_v10_finance", tab="despesas"))
    try:
        desp.descricao = descricao
        desp.valor = valor
        desp.data = data_ref
        ctl = _control(desp.id, create=True)
        ctl.tipo = tipo
        db.session.commit()
        flash("Despesa atualizada.", "success")
    except Exception:
        db.session.rollback()
        app.logger.exception("Falha ao editar despesa da cooperativa V16")
        flash("Não foi possível atualizar a despesa.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


@app.post("/admin/leve/despesas-coop/<int:item_id>/pagamento", endpoint="admin_v16_despesa_payment")
def admin_v16_despesa_payment(item_id: int):
    _require("editar")
    DespesaCooperativa.query.get_or_404(item_id)
    try:
        ctl = _control(item_id, create=True)
        if ctl.pago:
            ctl.pago = False
            ctl.data_pagamento = None
            flash("Pagamento desmarcado.", "info")
        else:
            ctl.pago = True
            ctl.data_pagamento = date.today()
            flash("Despesa marcada como paga com a data de hoje.", "success")
        db.session.commit()
    except Exception:
        db.session.rollback()
        app.logger.exception("Falha ao alterar pagamento da despesa V16")
        flash("Não foi possível alterar o pagamento.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


@app.post("/admin/leve/despesas-coop/<int:item_id>/excluir", endpoint="admin_v16_despesa_delete")
def admin_v16_despesa_delete(item_id: int):
    _require("excluir")
    desp = DespesaCooperativa.query.get_or_404(item_id)
    try:
        DespesaCoopControleV16.query.filter_by(despesa_id=item_id).delete(synchronize_session=False)
        db.session.delete(desp)
        db.session.commit()
        flash("Despesa excluída.", "success")
    except Exception:
        db.session.rollback()
        app.logger.exception("Falha ao excluir despesa da cooperativa V16")
        flash("Não foi possível excluir a despesa.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


_TEMPLATE = r'''
<!doctype html><html lang="pt-BR"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Despesas Coop — COOPEX</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css">
<link rel="stylesheet" href="{{ url_for('static', filename='css/admin_light_v8.css', v=build) }}">
<style>
.v16-summary{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-bottom:9px}.v16-pay{font-weight:900}.v16-paid{color:#067647}.v16-open{color:#b42318}.v16-type{font-weight:900}.v16-actions{display:flex;gap:5px;flex-wrap:wrap}.v16-edit{min-width:min(620px,86vw);padding:10px 0}.v16-edit-grid{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:8px;align-items:end}
@media(max-width:800px){.v16-summary{grid-template-columns:repeat(2,minmax(0,1fr))}.v16-edit{min-width:0;width:100%}.v16-edit-grid{grid-template-columns:1fr}.v16-actions{min-width:210px}}
@media(max-width:380px){.v16-summary{grid-template-columns:1fr}}
</style></head><body>
{% include '_admin_nav_v10.html' %}
<main class="alv8-page">
<div class="alv8-head"><div><h1 class="alv8-title">Despesas Coop</h1><p class="alv8-sub">Controle das despesas da cooperativa por tipo e situação de pagamento.</p></div></div>
{% with msgs=get_flashed_messages(with_categories=true) %}{% if msgs %}<div class="alv8-card">{% for cat,msg in msgs %}<div class="alv8-note">{{ msg }}</div>{% endfor %}</div>{% endif %}{% endwith %}

<div class="v16-summary">
<div class="alv8-kpi"><span>Total lançado</span><strong>R$ {{ total|brl }}</strong></div>
<div class="alv8-kpi"><span>Pago</span><strong>R$ {{ total_pago|brl }}</strong></div>
<div class="alv8-kpi"><span>Não pago</span><strong>R$ {{ total_aberto|brl }}</strong></div>
<div class="alv8-kpi"><span>Registros</span><strong>{{ rows|length }}</strong></div>
</div>

{% if can_create %}<details class="alv8-card" open><summary><strong><i class="bi bi-plus-circle"></i> Lançar despesa</strong></summary>
<form method="post" action="{{ url_for('admin_v16_despesa_create') }}" class="alv8-filter" style="margin-top:10px">
<div class="alv8-field grow"><label>Descrição</label><input class="alv8-input" name="descricao" required placeholder="Ex.: Aluguel, COSERN, Contabilidade"></div>
<div class="alv8-field"><label>Valor (R$)</label><input class="alv8-input" type="number" name="valor" min="0" step="0.01" required></div>
<div class="alv8-field"><label>Data</label><input class="alv8-input" type="date" name="data" value="{{ today.isoformat() }}" required></div>
<div class="alv8-field"><label>Tipo da despesa</label><select class="alv8-select" name="tipo" required><option value="">Selecione</option><option value="fixa">Fixa</option><option value="variavel">Variável</option></select></div>
<button class="alv8-btn primary" type="submit"><i class="bi bi-plus-circle"></i> Lançar</button>
</form></details>{% endif %}

<div class="alv8-card"><div class="alv8-table-wrap"><table class="alv8-table"><thead><tr><th>Data</th><th>Descrição</th><th>Valor</th><th>Fixa</th><th>Variável</th><th>Situação</th><th>Data pagamento</th><th>Ações</th></tr></thead><tbody>
{% for r in rows %}<tr>
<td>{{ r.desp.data.strftime('%d/%m/%Y') if r.desp.data else '—' }}</td><td><strong>{{ r.desp.descricao }}</strong></td><td><strong>R$ {{ r.desp.valor|brl }}</strong></td>
<td>{% if r.tipo=='fixa' %}<span class="alv8-badge ok"><i class="bi bi-check-lg"></i> Sim</span>{% else %}<span class="alv8-badge">—</span>{% endif %}</td>
<td>{% if r.tipo=='variavel' %}<span class="alv8-badge ok"><i class="bi bi-check-lg"></i> Sim</span>{% else %}<span class="alv8-badge">—</span>{% endif %}</td>
<td>{% if r.pago %}<span class="alv8-badge ok">Pago</span>{% else %}<span class="alv8-badge warn">Não pago</span>{% endif %}{% if not r.tipo %}<br><span class="alv8-badge warn">Não classificada</span>{% endif %}</td>
<td>{{ r.data_pagamento.strftime('%d/%m/%Y') if r.data_pagamento else '—' }}</td>
<td><div class="v16-actions">
{% if can_edit %}<form method="post" action="{{ url_for('admin_v16_despesa_payment',item_id=r.desp.id) }}"><button class="alv8-btn {{ '' if r.pago else 'ok' }}" type="submit"><i class="bi {{ 'bi-arrow-counterclockwise' if r.pago else 'bi-check-circle' }}"></i> {{ 'Desmarcar pago' if r.pago else 'Pago' }}</button></form>
<details><summary class="alv8-btn edit"><i class="bi bi-pencil"></i> Editar</summary><form class="v16-edit" method="post" action="{{ url_for('admin_v16_despesa_edit',item_id=r.desp.id) }}"><div class="v16-edit-grid"><div class="alv8-field"><label>Descrição</label><input class="alv8-input" name="descricao" value="{{ r.desp.descricao }}" required></div><div class="alv8-field"><label>Valor</label><input class="alv8-input" type="number" step="0.01" min="0" name="valor" value="{{ '%.2f'|format(r.desp.valor or 0) }}" required></div><div class="alv8-field"><label>Data</label><input class="alv8-input" type="date" name="data" value="{{ r.desp.data.isoformat() if r.desp.data else '' }}" required></div><div class="alv8-field"><label>Tipo</label><select class="alv8-select" name="tipo" required><option value="">Selecione</option><option value="fixa" {{ 'selected' if r.tipo=='fixa' else '' }}>Fixa</option><option value="variavel" {{ 'selected' if r.tipo=='variavel' else '' }}>Variável</option></select></div></div><button class="alv8-btn primary" style="margin-top:8px" type="submit">Salvar alterações</button></form></details>{% endif %}
{% if can_delete %}<form method="post" action="{{ url_for('admin_v16_despesa_delete',item_id=r.desp.id) }}" onsubmit="return confirm('Excluir esta despesa?');"><button class="alv8-btn danger" type="submit"><i class="bi bi-trash"></i> Excluir</button></form>{% endif %}
{% if not can_edit and not can_delete %}<span class="alv8-badge">Somente leitura</span>{% endif %}
</div></td></tr>{% else %}<tr><td colspan="8" class="alv8-empty">Nenhuma despesa cadastrada.</td></tr>{% endfor %}
</tbody></table></div></div>
<div class="alv8-footer">COOPEX Admin · {{ build }}</div></main>
<script>(function(){document.querySelectorAll('.alv8-group>button').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();const g=btn.parentElement;document.querySelectorAll('.alv8-group').forEach(x=>{if(x!==g)x.classList.remove('open')});g.classList.toggle('open')}));document.addEventListener('click',()=>document.querySelectorAll('.alv8-group').forEach(x=>x.classList.remove('open')));})();</script>
</body></html>
'''


def _render_expenses_v16():
    _require("ver")
    expenses = DespesaCooperativa.query.order_by(DespesaCooperativa.data.desc(), DespesaCooperativa.id.desc()).all()
    controls = {c.despesa_id: c for c in DespesaCoopControleV16.query.all()}
    rows = []
    total = total_pago = 0.0
    for desp in expenses:
        ctl = controls.get(desp.id)
        pago = bool(ctl and ctl.pago)
        valor = float(desp.valor or 0.0)
        total += valor
        if pago:
            total_pago += valor
        rows.append(type("ExpenseRow", (), {
            "desp": desp,
            "tipo": (ctl.tipo if ctl else None),
            "pago": pago,
            "data_pagamento": (ctl.data_pagamento if ctl else None),
        })())
    user = legacy._usuario_logado()
    is_master = bool(user and getattr(user, "is_master", False))
    perms = {}
    if user and (user.tipo or "").strip().lower() == "admin":
        perms = {aba: {acao: True for acao in ("ver","criar","editar","excluir")} for aba in legacy.ADMIN_ABAS} if is_master else legacy.get_admin_permissions_map(user.id)
    return render_template_string(
        _TEMPLATE,
        rows=rows,
        total=round(total,2),
        total_pago=round(total_pago,2),
        total_aberto=round(total-total_pago,2),
        today=date.today(),
        can_create=_allowed("criar"),
        can_edit=_allowed("editar"),
        can_delete=_allowed("excluir"),
        build="20260827-v16",
        active_tab="despesas",
        admin_nav_master=is_master,
        admin_nav_perms=perms,
        admin_nav_home_url=url_for("admin_light_summary"),
    )


_original_finance = app.view_functions.get("admin_v10_finance")
if _original_finance and not getattr(_original_finance, "_expense_v16", False):
    @wraps(_original_finance)
    def _finance_v16(tab: str, *args, **kwargs):
        if (tab or "").strip().lower() == "despesas":
            return _render_expenses_v16()
        return _original_finance(tab, *args, **kwargs)
    _finance_v16._expense_v16 = True
    app.view_functions["admin_v10_finance"] = _finance_v16


app.logger.info("V16: despesas Coop com tipo fixa/variável, pagamento e ações restauradas.")
