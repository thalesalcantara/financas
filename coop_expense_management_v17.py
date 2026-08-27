from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

from flask import flash, redirect, render_template_string, request, url_for

import app as legacy
import coop_expense_control_v16 as v16

app = legacy.app
db = legacy.db
DespesaCooperativa = legacy.DespesaCooperativa


class DespesaCoopRecorrenciaV17(db.Model):
    __tablename__ = "despesas_coop_recorrencia_v17"
    id = db.Column(db.Integer, primary_key=True)
    despesa_id = db.Column(db.Integer, db.ForeignKey("despesas_coop.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    repete_mensalmente = db.Column(db.Boolean, nullable=False, default=False, index=True)
    atualizado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


with app.app_context():
    DespesaCoopRecorrenciaV17.__table__.create(bind=db.engine, checkfirst=True)


def _rec(despesa_id: int, create=False):
    item = DespesaCoopRecorrenciaV17.query.filter_by(despesa_id=despesa_id).first()
    if not item and create:
        item = DespesaCoopRecorrenciaV17(despesa_id=despesa_id, repete_mensalmente=False)
        db.session.add(item)
        db.session.flush()
    return item


def _bool_form(name: str) -> bool:
    return (request.form.get(name) or "").strip().lower() in {"1", "true", "on", "sim", "yes"}


@app.post("/admin/leve/despesas-coop-v17/nova", endpoint="admin_v17_despesa_create")
def admin_v17_despesa_create():
    v16._require("criar")
    descricao = (request.form.get("descricao") or "").strip()
    valor = v16._money(request.form.get("valor"))
    data_ref = v16._parse_date(request.form.get("data"))
    tipo = (request.form.get("tipo") or "").strip().lower()
    mensal = _bool_form("repete_mensalmente")
    if not descricao or valor < 0 or not data_ref or tipo not in {"fixa", "variavel"}:
        flash("Preencha descrição, valor, data e escolha Fixa ou Variável.", "warning")
        return redirect(url_for("admin_v10_finance", tab="despesas"))
    try:
        desp = DespesaCooperativa(descricao=descricao, valor=valor, data=data_ref)
        db.session.add(desp); db.session.flush()
        db.session.add(v16.DespesaCoopControleV16(despesa_id=desp.id, tipo=tipo, pago=False))
        db.session.add(DespesaCoopRecorrenciaV17(despesa_id=desp.id, repete_mensalmente=mensal))
        db.session.commit()
        flash("Despesa cadastrada.", "success")
    except Exception:
        db.session.rollback(); app.logger.exception("Falha V17 ao criar despesa")
        flash("Não foi possível cadastrar a despesa.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


@app.post("/admin/leve/despesas-coop-v17/<int:item_id>/editar", endpoint="admin_v17_despesa_edit")
def admin_v17_despesa_edit(item_id: int):
    v16._require("editar")
    desp = DespesaCooperativa.query.get_or_404(item_id)
    descricao = (request.form.get("descricao") or "").strip()
    valor = v16._money(request.form.get("valor"), desp.valor or 0.0)
    data_ref = v16._parse_date(request.form.get("data"))
    tipo = (request.form.get("tipo") or "").strip().lower()
    mensal = _bool_form("repete_mensalmente")
    if not descricao or valor < 0 or not data_ref or tipo not in {"fixa", "variavel"}:
        flash("Preencha descrição, valor, data e tipo corretamente.", "warning")
        return redirect(url_for("admin_v10_finance", tab="despesas"))
    try:
        desp.descricao = descricao; desp.valor = valor; desp.data = data_ref
        ctl = v16._control(desp.id, create=True); ctl.tipo = tipo
        rec = _rec(desp.id, create=True); rec.repete_mensalmente = mensal
        db.session.commit(); flash("Despesa atualizada.", "success")
    except Exception:
        db.session.rollback(); app.logger.exception("Falha V17 ao editar despesa")
        flash("Não foi possível atualizar a despesa.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


@app.post("/admin/leve/despesas-coop-v17/<int:item_id>/pagamento", endpoint="admin_v17_despesa_payment")
def admin_v17_despesa_payment(item_id: int):
    v16._require("editar")
    DespesaCooperativa.query.get_or_404(item_id)
    try:
        ctl = v16._control(item_id, create=True)
        ctl.pago = not bool(ctl.pago)
        ctl.data_pagamento = date.today() if ctl.pago else None
        db.session.commit()
        flash("Despesa marcada como paga." if ctl.pago else "Pagamento desmarcado.", "success" if ctl.pago else "info")
    except Exception:
        db.session.rollback(); app.logger.exception("Falha V17 pagamento")
        flash("Não foi possível alterar o pagamento.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


@app.post("/admin/leve/despesas-coop-v17/<int:item_id>/excluir", endpoint="admin_v17_despesa_delete")
def admin_v17_despesa_delete(item_id: int):
    v16._require("excluir")
    desp = DespesaCooperativa.query.get_or_404(item_id)
    try:
        DespesaCoopRecorrenciaV17.query.filter_by(despesa_id=item_id).delete(synchronize_session=False)
        v16.DespesaCoopControleV16.query.filter_by(despesa_id=item_id).delete(synchronize_session=False)
        db.session.delete(desp); db.session.commit(); flash("Despesa excluída.", "success")
    except Exception:
        db.session.rollback(); app.logger.exception("Falha V17 exclusão")
        flash("Não foi possível excluir a despesa.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


_TEMPLATE = r'''
<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Despesas Coop — COOPEX</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css"><link rel="stylesheet" href="{{ url_for('static',filename='css/admin_light_v8.css',v=build) }}">
<style>.v17-kpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-bottom:9px}.v17-actions{display:flex;gap:5px;flex-wrap:wrap}.v17-edit{min-width:min(760px,88vw);padding:10px 0}.v17-edit-grid{display:grid;grid-template-columns:2fr 1fr 1fr 1fr 1fr;gap:8px;align-items:end}.v17-monthly{display:inline-flex;align-items:center;gap:6px;font-weight:800}.v17-monthly input{width:18px;height:18px}@media(max-width:800px){.v17-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.v17-edit{min-width:0;width:100%}.v17-edit-grid{grid-template-columns:1fr}.v17-actions{min-width:220px}}@media(max-width:380px){.v17-kpis{grid-template-columns:1fr}}</style></head><body>
{% include '_admin_nav_v10.html' %}<main class="alv8-page"><div class="alv8-head"><div><h1 class="alv8-title">Despesas Coop</h1><p class="alv8-sub">Fixa/Variável, recorrência mensal e controle de pagamento.</p></div></div>
{% with msgs=get_flashed_messages(with_categories=true) %}{% if msgs %}<div class="alv8-card">{% for cat,msg in msgs %}<div class="alv8-note">{{ msg }}</div>{% endfor %}</div>{% endif %}{% endwith %}
<div class="v17-kpis"><div class="alv8-kpi"><span>Total geral</span><strong>R$ {{ total|brl }}</strong></div><div class="alv8-kpi"><span>Total fixo</span><strong>R$ {{ total_fixo|brl }}</strong></div><div class="alv8-kpi"><span>Total variável</span><strong>R$ {{ total_variavel|brl }}</strong></div><div class="alv8-kpi"><span>Não classificado</span><strong>R$ {{ total_nc|brl }}</strong></div><div class="alv8-kpi"><span>Fixo pago</span><strong>R$ {{ fixo_pago|brl }}</strong></div><div class="alv8-kpi"><span>Fixo não pago</span><strong>R$ {{ fixo_aberto|brl }}</strong></div><div class="alv8-kpi"><span>Variável pago</span><strong>R$ {{ var_pago|brl }}</strong></div><div class="alv8-kpi"><span>Variável não pago</span><strong>R$ {{ var_aberto|brl }}</strong></div></div>
{% if can_create %}<details class="alv8-card" open><summary><strong><i class="bi bi-plus-circle"></i> Lançar despesa</strong></summary><form method="post" action="{{ url_for('admin_v17_despesa_create') }}" class="alv8-filter" style="margin-top:10px"><div class="alv8-field grow"><label>Descrição</label><input class="alv8-input" name="descricao" required></div><div class="alv8-field"><label>Valor (R$)</label><input class="alv8-input" type="number" name="valor" min="0" step="0.01" required></div><div class="alv8-field"><label>Data</label><input class="alv8-input" type="date" name="data" value="{{ today.isoformat() }}" required></div><div class="alv8-field"><label>Tipo</label><select class="alv8-select" name="tipo" required><option value="">Selecione</option><option value="fixa">Fixa</option><option value="variavel">Variável</option></select></div><label class="v17-monthly"><input type="checkbox" name="repete_mensalmente" value="1"> Repete todo mês</label><button class="alv8-btn primary" type="submit">Lançar</button></form></details>{% endif %}
<div class="alv8-card"><div class="alv8-table-wrap"><table class="alv8-table"><thead><tr><th>Data</th><th>Descrição</th><th>Valor</th><th>Fixa</th><th>Variável</th><th>Repete mensal</th><th>Situação</th><th>Data pagamento</th><th>Ações</th></tr></thead><tbody>{% for r in rows %}<tr><td>{{ r.desp.data.strftime('%d/%m/%Y') if r.desp.data else '—' }}</td><td><strong>{{ r.desp.descricao }}</strong></td><td><strong>R$ {{ r.desp.valor|brl }}</strong></td><td>{{ 'Sim' if r.tipo=='fixa' else '—' }}</td><td>{{ 'Sim' if r.tipo=='variavel' else '—' }}</td><td>{% if r.mensal %}<span class="alv8-badge ok">Sim</span>{% else %}<span class="alv8-badge">Não</span>{% endif %}</td><td>{% if r.pago %}<span class="alv8-badge ok">Pago</span>{% else %}<span class="alv8-badge warn">Não pago</span>{% endif %}{% if not r.tipo %}<br><span class="alv8-badge warn">Não classificada</span>{% endif %}</td><td>{{ r.data_pagamento.strftime('%d/%m/%Y') if r.data_pagamento else '—' }}</td><td><div class="v17-actions">{% if can_edit %}<form method="post" action="{{ url_for('admin_v17_despesa_payment',item_id=r.desp.id) }}"><button class="alv8-btn {{ '' if r.pago else 'ok' }}" type="submit">{{ 'Desmarcar pago' if r.pago else 'Pago' }}</button></form><details><summary class="alv8-btn edit">Editar</summary><form class="v17-edit" method="post" action="{{ url_for('admin_v17_despesa_edit',item_id=r.desp.id) }}"><div class="v17-edit-grid"><div class="alv8-field"><label>Descrição</label><input class="alv8-input" name="descricao" value="{{ r.desp.descricao }}" required></div><div class="alv8-field"><label>Valor</label><input class="alv8-input" type="number" step="0.01" min="0" name="valor" value="{{ '%.2f'|format(r.desp.valor or 0) }}" required></div><div class="alv8-field"><label>Data</label><input class="alv8-input" type="date" name="data" value="{{ r.desp.data.isoformat() if r.desp.data else '' }}" required></div><div class="alv8-field"><label>Tipo</label><select class="alv8-select" name="tipo" required><option value="">Selecione</option><option value="fixa" {{ 'selected' if r.tipo=='fixa' else '' }}>Fixa</option><option value="variavel" {{ 'selected' if r.tipo=='variavel' else '' }}>Variável</option></select></div><label class="v17-monthly"><input type="checkbox" name="repete_mensalmente" value="1" {{ 'checked' if r.mensal else '' }}> Repete todo mês</label></div><button class="alv8-btn primary" style="margin-top:8px" type="submit">Salvar alterações</button></form></details>{% endif %}{% if can_delete %}<form method="post" action="{{ url_for('admin_v17_despesa_delete',item_id=r.desp.id) }}" onsubmit="return confirm('Excluir esta despesa?');"><button class="alv8-btn danger" type="submit">Excluir</button></form>{% endif %}{% if not can_edit and not can_delete %}<span class="alv8-badge">Somente leitura</span>{% endif %}</div></td></tr>{% else %}<tr><td colspan="9" class="alv8-empty">Nenhuma despesa cadastrada.</td></tr>{% endfor %}</tbody></table></div></div><div class="alv8-footer">COOPEX Admin · {{ build }}</div></main>
<script>(function(){document.querySelectorAll('.alv8-group>button').forEach(btn=>btn.addEventListener('click',e=>{e.stopPropagation();const g=btn.parentElement;document.querySelectorAll('.alv8-group').forEach(x=>{if(x!==g)x.classList.remove('open')});g.classList.toggle('open')}));document.addEventListener('click',()=>document.querySelectorAll('.alv8-group').forEach(x=>x.classList.remove('open')));})();</script></body></html>
'''


def _render_v17():
    v16._require("ver")
    expenses = DespesaCooperativa.query.order_by(DespesaCooperativa.data.desc(), DespesaCooperativa.id.desc()).all()
    controls = {x.despesa_id: x for x in v16.DespesaCoopControleV16.query.all()}
    recs = {x.despesa_id: x for x in DespesaCoopRecorrenciaV17.query.all()}
    rows=[]; total=total_fixo=total_variavel=total_nc=fixo_pago=fixo_aberto=var_pago=var_aberto=0.0
    for desp in expenses:
        ctl=controls.get(desp.id); rec=recs.get(desp.id); tipo=ctl.tipo if ctl else None; pago=bool(ctl and ctl.pago); val=float(desp.valor or 0)
        total += val
        if tipo=="fixa": total_fixo+=val; fixo_pago += val if pago else 0; fixo_aberto += 0 if pago else val
        elif tipo=="variavel": total_variavel+=val; var_pago += val if pago else 0; var_aberto += 0 if pago else val
        else: total_nc += val
        rows.append(SimpleNamespace(desp=desp,tipo=tipo,pago=pago,data_pagamento=(ctl.data_pagamento if ctl else None),mensal=bool(rec and rec.repete_mensalmente)))
    user=legacy._usuario_logado(); is_master=bool(user and getattr(user,"is_master",False)); perms={}
    if user and (user.tipo or "").strip().lower()=="admin": perms=({aba:{a:True for a in ("ver","criar","editar","excluir")} for aba in legacy.ADMIN_ABAS} if is_master else legacy.get_admin_permissions_map(user.id))
    return render_template_string(_TEMPLATE,rows=rows,total=round(total,2),total_fixo=round(total_fixo,2),total_variavel=round(total_variavel,2),total_nc=round(total_nc,2),fixo_pago=round(fixo_pago,2),fixo_aberto=round(fixo_aberto,2),var_pago=round(var_pago,2),var_aberto=round(var_aberto,2),today=date.today(),can_create=v16._allowed("criar"),can_edit=v16._allowed("editar"),can_delete=v16._allowed("excluir"),build="20260827-v17",active_tab="despesas",admin_nav_master=is_master,admin_nav_perms=perms,admin_nav_home_url=url_for("admin_light_summary"))


v16._render_expenses_v16 = _render_v17
app.logger.info("V17: despesas fixas/variáveis, recorrência mensal e totais separados carregados.")
