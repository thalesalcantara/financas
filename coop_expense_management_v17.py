from __future__ import annotations

import calendar
import uuid
from datetime import date, datetime
from types import SimpleNamespace

from flask import flash, redirect, render_template_string, request, url_for

import app as legacy
import coop_expense_control_v16 as v16

app = legacy.app
db = legacy.db
DespesaCooperativa = legacy.DespesaCooperativa
TIPOS = {"fixa", "variavel", "nao_rateio"}


class DespesaCoopRecorrenciaV17(db.Model):
    __tablename__ = "despesas_coop_recorrencia_v17"
    id = db.Column(db.Integer, primary_key=True)
    despesa_id = db.Column(db.Integer, db.ForeignKey("despesas_coop.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    repete_mensalmente = db.Column(db.Boolean, nullable=False, default=False, index=True)
    atualizado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class DespesaCoopSerieV18(db.Model):
    __tablename__ = "despesas_coop_serie_v18"
    id = db.Column(db.Integer, primary_key=True)
    despesa_id = db.Column(db.Integer, db.ForeignKey("despesas_coop.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    serie_key = db.Column(db.String(64), nullable=False, index=True)


with app.app_context():
    DespesaCoopRecorrenciaV17.__table__.create(bind=db.engine, checkfirst=True)
    DespesaCoopSerieV18.__table__.create(bind=db.engine, checkfirst=True)


def _rec(despesa_id, create=False):
    x = DespesaCoopRecorrenciaV17.query.filter_by(despesa_id=despesa_id).first()
    if not x and create:
        x = DespesaCoopRecorrenciaV17(despesa_id=despesa_id, repete_mensalmente=False)
        db.session.add(x); db.session.flush()
    return x


def _serie(despesa_id, create=False, key=None):
    x = DespesaCoopSerieV18.query.filter_by(despesa_id=despesa_id).first()
    if not x and create:
        x = DespesaCoopSerieV18(despesa_id=despesa_id, serie_key=key or uuid.uuid4().hex)
        db.session.add(x); db.session.flush()
    return x


def _bool(name):
    return (request.form.get(name) or "").lower() in {"1","on","true","sim"}


def _next_month(d):
    y, m = (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def _month_end(d):
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


def _ensure_recurring():
    today = date.today()
    recs = DespesaCoopRecorrenciaV17.query.filter_by(repete_mensalmente=True).all()
    for r in recs:
        if not _serie(r.despesa_id):
            _serie(r.despesa_id, True)
    db.session.flush()
    keys = {}
    for s in DespesaCoopSerieV18.query.all():
        keys.setdefault(s.serie_key, []).append(s.despesa_id)
    for key, ids in keys.items():
        active = DespesaCoopRecorrenciaV17.query.filter(DespesaCoopRecorrenciaV17.despesa_id.in_(ids), DespesaCoopRecorrenciaV17.repete_mensalmente.is_(True)).first()
        if not active:
            continue
        current = DespesaCooperativa.query.filter(DespesaCooperativa.id.in_(ids)).order_by(DespesaCooperativa.data.desc(), DespesaCooperativa.id.desc()).first()
        if not current or not current.data or today < _month_end(current.data):
            continue
        nxt = _next_month(current.data)
        exists = (DespesaCooperativa.query.join(DespesaCoopSerieV18, DespesaCoopSerieV18.despesa_id==DespesaCooperativa.id)
                  .filter(DespesaCoopSerieV18.serie_key==key,
                          db.extract("year", DespesaCooperativa.data)==nxt.year,
                          db.extract("month", DespesaCooperativa.data)==nxt.month).first())
        if exists:
            continue
        ctl = v16._control(current.id, True)
        new = DespesaCooperativa(descricao=current.descricao, valor=current.valor, data=nxt)
        db.session.add(new); db.session.flush()
        db.session.add(v16.DespesaCoopControleV16(despesa_id=new.id, tipo=ctl.tipo, pago=False, data_pagamento=None))
        db.session.add(DespesaCoopRecorrenciaV17(despesa_id=new.id, repete_mensalmente=True))
        db.session.add(DespesaCoopSerieV18(despesa_id=new.id, serie_key=key))
    db.session.commit()


@app.post("/admin/leve/despesas-coop-v17/nova", endpoint="admin_v17_despesa_create")
def create_expense():
    v16._require("criar")
    desc=(request.form.get("descricao") or "").strip(); val=v16._money(request.form.get("valor")); d=v16._parse_date(request.form.get("data")); tipo=(request.form.get("tipo") or "").strip().lower(); mensal=_bool("repete_mensalmente")
    if not desc or val < 0 or not d or tipo not in TIPOS:
        flash("Preencha descrição, valor, data e modalidade corretamente.", "warning"); return redirect(url_for("admin_v10_finance", tab="despesas"))
    try:
        x=DespesaCooperativa(descricao=desc, valor=val, data=d); db.session.add(x); db.session.flush()
        db.session.add(v16.DespesaCoopControleV16(despesa_id=x.id,tipo=tipo,pago=False)); db.session.add(DespesaCoopRecorrenciaV17(despesa_id=x.id,repete_mensalmente=mensal))
        if mensal: _serie(x.id, True)
        db.session.commit(); flash("Despesa cadastrada.", "success")
    except Exception:
        db.session.rollback(); app.logger.exception("V18 criar despesa"); flash("Não foi possível cadastrar.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


@app.post("/admin/leve/despesas-coop-v17/<int:item_id>/editar", endpoint="admin_v17_despesa_edit")
def edit_expense(item_id):
    v16._require("editar"); x=DespesaCooperativa.query.get_or_404(item_id)
    desc=(request.form.get("descricao") or "").strip(); val=v16._money(request.form.get("valor"),x.valor); d=v16._parse_date(request.form.get("data")); tipo=(request.form.get("tipo") or "").strip().lower(); mensal=_bool("repete_mensalmente")
    if not desc or val < 0 or not d or tipo not in TIPOS:
        flash("Preencha os campos corretamente.", "warning"); return redirect(url_for("admin_v10_finance", tab="despesas"))
    try:
        x.descricao=desc; x.valor=val; x.data=d; v16._control(x.id,True).tipo=tipo; _rec(x.id,True).repete_mensalmente=mensal
        if mensal and not _serie(x.id): _serie(x.id,True)
        db.session.commit(); flash("Despesa atualizada.", "success")
    except Exception:
        db.session.rollback(); app.logger.exception("V18 editar despesa"); flash("Não foi possível atualizar.", "danger")
    return redirect(url_for("admin_v10_finance", tab="despesas"))


@app.post("/admin/leve/despesas-coop-v17/<int:item_id>/pagamento", endpoint="admin_v17_despesa_payment")
def payment(item_id):
    v16._require("editar"); DespesaCooperativa.query.get_or_404(item_id)
    ctl=v16._control(item_id,True); ctl.pago=not bool(ctl.pago); ctl.data_pagamento=date.today() if ctl.pago else None; db.session.commit()
    flash("Despesa marcada como paga." if ctl.pago else "Pagamento desmarcado.", "success"); return redirect(url_for("admin_v10_finance", tab="despesas"))


@app.post("/admin/leve/despesas-coop-v17/<int:item_id>/excluir", endpoint="admin_v17_despesa_delete")
def delete_expense(item_id):
    v16._require("excluir"); x=DespesaCooperativa.query.get_or_404(item_id)
    DespesaCoopSerieV18.query.filter_by(despesa_id=item_id).delete(); DespesaCoopRecorrenciaV17.query.filter_by(despesa_id=item_id).delete(); v16.DespesaCoopControleV16.query.filter_by(despesa_id=item_id).delete(); db.session.delete(x); db.session.commit(); flash("Despesa excluída.", "success"); return redirect(url_for("admin_v10_finance", tab="despesas"))


_TEMPLATE=r'''<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Despesas Coop</title><link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css"><link rel="stylesheet" href="{{url_for('static',filename='css/admin_light_v8.css',v=build)}}"><style>
.v18-kpis{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:7px;margin-bottom:9px}.v18-actions{display:inline-flex;gap:3px;white-space:nowrap}.v18-actions .alv8-btn{padding:5px 7px;min-height:28px;font-size:9px}.alv8-table th:last-child,.alv8-table td:last-child{width:1%;white-space:nowrap}.v18-editbox{position:fixed;z-index:6200;left:50%;top:50%;transform:translate(-50%,-50%);width:min(760px,calc(100vw - 20px));background:#fff;border:1px solid #dfe5ef;border-radius:12px;padding:14px;box-shadow:0 20px 60px rgba(0,0,0,.3)}.v18-edit-grid{display:grid;grid-template-columns:2fr 1fr 1fr 1.4fr 1fr;gap:8px;align-items:end}.v18-month{display:inline-flex;align-items:center;gap:6px;font-weight:800}.v18-month input{width:18px;height:18px}@media(max-width:800px){.v18-kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.v18-edit-grid{grid-template-columns:1fr}}@media(max-width:380px){.v18-kpis{grid-template-columns:1fr}}</style></head><body>{% include '_admin_nav_v10.html' %}<main class="alv8-page"><div class="alv8-head"><div><h1 class="alv8-title">Despesas Coop</h1><p class="alv8-sub">O filtro limita a lista e os totais ao período selecionado.</p></div></div>
<form class="alv8-card alv8-filter" method="get" action="{{url_for('admin_v10_finance',tab='despesas')}}"><input type="hidden" name="tab" value="despesas"><div class="alv8-field"><label>Data inicial</label><input class="alv8-input" type="date" name="inicio" value="{{inicio.isoformat()}}"></div><div class="alv8-field"><label>Data final</label><input class="alv8-input" type="date" name="fim" value="{{fim.isoformat()}}"></div><button class="alv8-btn primary" type="submit"><i class="bi bi-funnel"></i> Filtrar</button><a class="alv8-btn" href="{{url_for('admin_v10_finance',tab='despesas')}}">Mês atual</a></form>
{% with msgs=get_flashed_messages(with_categories=true)%}{%if msgs%}<div class="alv8-card">{%for c,m in msgs%}<div class="alv8-note">{{m}}</div>{%endfor%}</div>{%endif%}{%endwith%}
<div class="v18-kpis"><div class="alv8-kpi"><span>Total geral</span><strong>R$ {{total|brl}}</strong></div><div class="alv8-kpi"><span>Total para rateio</span><strong>R$ {{total_rateio|brl}}</strong></div><div class="alv8-kpi"><span>Total fixo</span><strong>R$ {{fixo|brl}}</strong></div><div class="alv8-kpi"><span>Total variável</span><strong>R$ {{variavel|brl}}</strong></div><div class="alv8-kpi"><span>Não vai para rateio</span><strong>R$ {{nao_rateio|brl}}</strong></div></div>
{%if can_create%}<details class="alv8-card" open><summary><strong>Lançar despesa</strong></summary><form method="post" action="{{url_for('admin_v17_despesa_create')}}" class="alv8-filter" style="margin-top:9px"><div class="alv8-field grow"><label>Descrição</label><input class="alv8-input" name="descricao" required></div><div class="alv8-field"><label>Valor</label><input class="alv8-input" type="number" step="0.01" min="0" name="valor" required></div><div class="alv8-field"><label>Data</label><input class="alv8-input" type="date" name="data" value="{{today.isoformat()}}" required></div><div class="alv8-field"><label>Modalidade</label><select class="alv8-select" name="tipo" required><option value="">Selecione</option><option value="fixa">Fixa</option><option value="variavel">Variável</option><option value="nao_rateio">Não vai para o rateio</option></select></div><label class="v18-month"><input type="checkbox" name="repete_mensalmente" value="1"> Repete todo mês</label><button class="alv8-btn primary">Lançar</button></form></details>{%endif%}
<div class="alv8-card"><div class="alv8-table-wrap"><table class="alv8-table"><thead><tr><th>Data</th><th>Descrição</th><th>Valor</th><th>Fixa</th><th>Variável</th><th>Não rateio</th><th>Mensal</th><th>Situação</th><th>Data pagamento</th><th>Ações</th></tr></thead><tbody>{%for r in rows%}<tr><td>{{r.desp.data.strftime('%d/%m/%Y') if r.desp.data else '—'}}</td><td><strong>{{r.desp.descricao}}</strong></td><td>R$ {{r.desp.valor|brl}}</td><td>{{'Sim' if r.tipo=='fixa' else '—'}}</td><td>{{'Sim' if r.tipo=='variavel' else '—'}}</td><td>{{'Sim' if r.tipo=='nao_rateio' else '—'}}</td><td>{{'Sim' if r.mensal else 'Não'}}</td><td><span class="alv8-badge {{'ok' if r.pago else 'warn'}}">{{'Pago' if r.pago else 'Não pago'}}</span>{%if not r.tipo%}<br><span class="alv8-badge warn">Não classificada</span>{%endif%}</td><td>{{r.data_pagamento.strftime('%d/%m/%Y') if r.data_pagamento else '—'}}</td><td><div class="v18-actions">{%if can_edit%}<form method="post" action="{{url_for('admin_v17_despesa_payment',item_id=r.desp.id)}}"><button class="alv8-btn {{'' if r.pago else 'ok'}}" title="Pago"><i class="bi bi-check-circle"></i></button></form><details><summary class="alv8-btn edit" title="Editar"><i class="bi bi-pencil"></i></summary><div class="v18-editbox"><form method="post" action="{{url_for('admin_v17_despesa_edit',item_id=r.desp.id)}}"><h3>Editar despesa</h3><div class="v18-edit-grid"><div><label>Descrição</label><input class="alv8-input" name="descricao" value="{{r.desp.descricao}}" required></div><div><label>Valor</label><input class="alv8-input" type="number" step="0.01" name="valor" value="{{'%.2f'|format(r.desp.valor or 0)}}" required></div><div><label>Data</label><input class="alv8-input" type="date" name="data" value="{{r.desp.data.isoformat() if r.desp.data else ''}}" required></div><div><label>Modalidade</label><select class="alv8-select" name="tipo" required><option value="fixa" {{'selected' if r.tipo=='fixa' else ''}}>Fixa</option><option value="variavel" {{'selected' if r.tipo=='variavel' else ''}}>Variável</option><option value="nao_rateio" {{'selected' if r.tipo=='nao_rateio' else ''}}>Não vai para o rateio</option></select></div><label class="v18-month"><input type="checkbox" name="repete_mensalmente" value="1" {{'checked' if r.mensal else ''}}> Repete todo mês</label></div><button class="alv8-btn primary" style="margin-top:9px">Salvar</button></form></div></details>{%endif%}{%if can_delete%}<form method="post" action="{{url_for('admin_v17_despesa_delete',item_id=r.desp.id)}}" onsubmit="return confirm('Excluir esta despesa?')"><button class="alv8-btn danger" title="Excluir"><i class="bi bi-trash"></i></button></form>{%endif%}{%if not can_edit and not can_delete%}<span class="alv8-badge">Somente leitura</span>{%endif%}</div></td></tr>{%else%}<tr><td colspan="10" class="alv8-empty">Nenhuma despesa neste período.</td></tr>{%endfor%}</tbody></table></div></div></main></body></html>'''


def _render_v18():
    v16._require("ver")
    try: _ensure_recurring()
    except Exception: db.session.rollback(); app.logger.exception("V18 recorrência")
    today=date.today(); first=date(today.year,today.month,1); last=date(today.year,today.month,calendar.monthrange(today.year,today.month)[1])
    inicio=v16._parse_date(request.args.get("inicio")) or first; fim=v16._parse_date(request.args.get("fim")) or last
    if fim < inicio: inicio,fim=fim,inicio
    expenses=DespesaCooperativa.query.filter(DespesaCooperativa.data>=inicio,DespesaCooperativa.data<=fim).order_by(DespesaCooperativa.data.desc(),DespesaCooperativa.id.desc()).all()
    ids=[d.id for d in expenses]
    ctls={x.despesa_id:x for x in v16.DespesaCoopControleV16.query.filter(v16.DespesaCoopControleV16.despesa_id.in_(ids)).all()} if ids else {}
    recs={x.despesa_id:x for x in DespesaCoopRecorrenciaV17.query.filter(DespesaCoopRecorrenciaV17.despesa_id.in_(ids)).all()} if ids else {}
    rows=[]; total=fixo=variavel=nao_rateio=0.0
    for d in expenses:
        ctl=ctls.get(d.id); tipo=ctl.tipo if ctl else None; val=float(d.valor or 0); total+=val
        if tipo=='fixa': fixo+=val
        elif tipo=='variavel': variavel+=val
        elif tipo=='nao_rateio': nao_rateio+=val
        rows.append(SimpleNamespace(desp=d,tipo=tipo,pago=bool(ctl and ctl.pago),data_pagamento=ctl.data_pagamento if ctl else None,mensal=bool(recs.get(d.id) and recs[d.id].repete_mensalmente)))
    user=legacy._usuario_logado(); master=bool(user and getattr(user,'is_master',False)); perms={}
    if user and (user.tipo or '').lower()=='admin': perms=({a:{x:True for x in ('ver','criar','editar','excluir')} for a in legacy.ADMIN_ABAS} if master else legacy.get_admin_permissions_map(user.id))
    return render_template_string(_TEMPLATE,rows=rows,total=round(total,2),total_rateio=round(fixo+variavel,2),fixo=round(fixo,2),variavel=round(variavel,2),nao_rateio=round(nao_rateio,2),inicio=inicio,fim=fim,today=today,can_create=v16._allowed('criar'),can_edit=v16._allowed('editar'),can_delete=v16._allowed('excluir'),build='20260827-v18',active_tab='despesas',admin_nav_master=master,admin_nav_perms=perms,admin_nav_home_url=url_for('admin_light_summary'))

v16._render_expenses_v16=_render_v18
app.logger.info('V18: filtro restaurado, modalidades e recorrencia mensal carregados.')
