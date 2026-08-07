from __future__ import annotations

import os
import unicodedata
from datetime import date, datetime
from io import BytesIO
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from flask import flash, redirect, render_template, request, send_file, session, url_for
from sqlalchemy import event, or_

import app as legacy
import admin_light_v8 as light
import admin_preserve_v9 as preserve

app = legacy.app
db = legacy.db
Cooperado = legacy.Cooperado
Restaurante = legacy.Restaurante
Escala = legacy.Escala
TrocaSolicitacao = legacy.TrocaSolicitacao
BUILD = "20260807-1408"


def brl(value) -> str:
    try:
        number = float(value or 0.0)
    except Exception:
        number = 0.0
    raw = f"{number:,.2f}"
    return raw.replace(",", "§").replace(".", ",").replace("§", ".")


app.jinja_env.filters["brl"] = brl


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


# Todos os estabelecimentos trabalham de segunda a domingo.
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


# Fotos: prefere bytes persistidos no banco e usa fallback quando arquivo antigo sumiu.
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


# Financeiro sob demanda: não carrega o Admin inteiro de ~2,5 MB.
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
    return render_template(
        "admin_establishments_v10.html",
        page_title="Estabelecimentos",
        active_tab="estabelecimentos",
        rows=result,
        q=q_raw,
        status=status,
        build=BUILD,
    )


# Blitz = documentos operacionais de CNH e CRLV dentro do grupo Escala.
@app.get("/admin/leve/blitz", endpoint="admin_v10_blitz")
def admin_v10_blitz():
    denied = light._guard("documentos")
    if denied:
        return denied
    q_raw = request.args.get("q") or ""
    q = _norm(q_raw)
    coops = light._active_coops()
    if q:
        coops = [
            c for c in coops
            if q in _norm(f"{c.nome} {getattr(c, 'cnh_numero', '')} {getattr(c, 'placa', '')}")
        ]
    return render_template(
        "admin_blitz_v10.html",
        page_title="Blitz — CNH e CRLV",
        active_tab="blitz",
        cooperados=coops[:300],
        q=q_raw,
        today=date.today(),
        build=BUILD,
    )


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


# Escala leve, mantendo o mesmo banco/linhas geradas pelo upload XLSX.
def _admin_light_scale_v10():
    denied = light._guard("escalas")
    if denied:
        return denied
    q_raw = (request.args.get("q") or "").strip()
    q = _norm(q_raw)
    active_ids, _, active_coops = light._active_coop_ids_names()
    coop_by_id = {c.id: c for c in active_coops}
    restaurants = (
        Restaurante.query
        .filter(or_(Restaurante.ativo.is_(True), Restaurante.ativo.is_(None)))
        .order_by(Restaurante.nome.asc())
        .all()
    )
    scales = []
    scale_rows = []
    contracts = set()
    for s in Escala.query.order_by(Escala.id.asc()).limit(1400).all():
        if s.cooperado_id and s.cooperado_id not in active_ids:
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
            "id": s.id,
            "data": s.data or "",
            "weekday_num": preserve._weekday_num(s.data),
            "weekday_label": preserve._weekday_label(s.data),
            "turno": s.turno or "",
            "horario": s.horario or "",
            "contrato": s.contrato or "",
            "restaurante_id": s.restaurante_id,
            "cooperado_id": s.cooperado_id,
            "cooperado_nome": current_name,
            "cooperado_nome_livre": "" if s.cooperado_id else (s.cooperado_nome or ""),
        })
    contracts.update((r.nome or "").strip() for r in restaurants if (r.nome or "").strip())
    return light._render(
        "escala",
        "Escala",
        "Upload XLSX e escala semanal. A busca encontra qualquer parte do nome do cooperado.",
        q=q_raw,
        scales=scales,
        scale_rows=scale_rows,
        contract_options=sorted(contracts, key=_norm),
        coop_map=coop_by_id,
        rest_map={r.id: r for r in restaurants},
        cooperados=active_coops,
        restaurantes=restaurants,
    )


app.view_functions["admin_light_scale"] = _admin_light_scale_v10


def _scale_text(scale) -> str:
    if not scale:
        return "escala não localizada"
    parts = [str(scale.data or "").strip(), str(scale.horario or "").strip(), str(scale.contrato or "").strip()]
    return " · ".join(x for x in parts if x) or f"escala #{scale.id}"


def _line_text(line: dict) -> str:
    parts = [str(line.get("dia") or "").strip(), str(line.get("turno_horario") or "").strip(), str(line.get("contrato") or "").strip()]
    return " · ".join(x for x in parts if x)


def _admin_light_swaps_v10():
    denied = light._guard("escalas")
    if denied:
        return denied
    active_ids, _, active_coops = light._active_coop_ids_names()
    rows = (
        TrocaSolicitacao.query
        .filter(TrocaSolicitacao.solicitante_id.in_(active_ids), TrocaSolicitacao.destino_id.in_(active_ids))
        .order_by(TrocaSolicitacao.id.desc()).limit(160).all()
        if active_ids else []
    )
    coop_map = {c.id: c for c in active_coops}
    scale_ids = {t.origem_escala_id for t in rows if t.origem_escala_id}
    scale_map = {s.id: s for s in Escala.query.filter(Escala.id.in_(scale_ids)).all()} if scale_ids else {}
    result = []
    parser = getattr(legacy, "_parse_linhas_from_msg", None)
    for t in rows:
        sn = coop_map.get(t.solicitante_id).nome if coop_map.get(t.solicitante_id) else "Cooperado"
        dn = coop_map.get(t.destino_id).nome if coop_map.get(t.destino_id) else "Cooperado"
        origem = scale_map.get(t.origem_escala_id)
        linhas = []
        if parser and (t.status or "").lower() in {"aprovada", "aceita"}:
            try:
                linhas = parser(t.mensagem) or []
            except Exception:
                linhas = []
        if len(linhas) >= 2:
            resumo = f"{sn} — {_line_text(linhas[0])} trocou com {dn} — {_line_text(linhas[1])}"
        elif len(linhas) == 1:
            resumo = f"{sn} — {_line_text(linhas[0])} passou a escala para {dn}"
        elif (t.status or "").lower() == "pendente":
            resumo = f"{sn} — {_scale_text(origem)} solicitou troca com {dn}"
        elif (t.status or "").lower() in {"recusada", "rejeitada"}:
            resumo = f"{sn} — {_scale_text(origem)} teve a troca com {dn} recusada"
        else:
            resumo = f"{sn} — {_scale_text(origem)} trocou com {dn}"
        result.append(SimpleNamespace(
            id=t.id,
            status=t.status or "—",
            criada_em=getattr(t, "criada_em", None),
            aplicada_em=getattr(t, "aplicada_em", None),
            resumo=resumo,
        ))
    return light._render(
        "trocas",
        "Trocas de escala",
        "Uma linha por troca, sem redundância.",
        swap_rows=result,
        trocas=rows,
        coop_map=coop_map,
        scale_map=scale_map,
    )


app.view_functions["admin_light_swaps"] = _admin_light_swaps_v10


@app.after_request
def _admin_v10_redirect_light(response):
    try:
        if (session.get("user_tipo") or "").strip().lower() != "admin":
            return response
        if response.status_code not in (301, 302, 303, 307, 308):
            return response
        location = response.headers.get("Location") or ""
        parsed = urlparse(location)
        tab = (parse_qs(parsed.query).get("tab") or [""])[0]
        if tab in _FINANCE:
            response.headers["Location"] = url_for("admin_v10_finance", tab=tab)
        elif tab == "restaurantes":
            response.headers["Location"] = url_for("admin_v10_establishments")
        elif tab == "cooperados":
            response.headers["Location"] = url_for("admin_light_cooperatives")
    except Exception:
        app.logger.exception("Falha ao redirecionar retorno do Admin V10")
    return response


def _install_template_v10() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_admin_v10", False):
        return
    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)

        if template in {"admin_light_v8.html", "admin_dashboard.html", "admin_lancamentos.html"}:
            source = source.replace("'%.2f'|format(", "brl(")
            source = source.replace(" ({{ r.periodo }})", "")
            source = source.replace("({{ r.periodo }})", "")

        if template == "admin_light_v8.html":
            # Não existe mais filtro de período especial do estabelecimento.
            old_period = '<label class="alv8-check"><input type="checkbox" name="considerar_periodo" value="1" {{ \'checked\' if considerar_periodo else \'\' }}> Considerar período do estabelecimento</label>'
            source = source.replace(old_period, "")

            replacements = {
                '/admin?tab=receitas&legacy=1': "{{ url_for('admin_v10_finance', tab='receitas') }}",
                '/admin?tab=despesas&legacy=1': "{{ url_for('admin_v10_finance', tab='despesas') }}",
                '/admin?tab=coop_receitas&legacy=1': "{{ url_for('admin_v10_finance', tab='coop_receitas') }}",
                '/admin?tab=coop_despesas&legacy=1': "{{ url_for('admin_v10_finance', tab='coop_despesas') }}",
                '/admin?tab=beneficios&legacy=1': "{{ url_for('admin_v10_finance', tab='beneficios') }}",
                '/admin?tab=restaurantes&legacy=1': "{{ url_for('admin_v10_establishments') }}",
                '/admin/avaliacoes?legacy=1': "{{ url_for('admin_light_ratings') }}",
                '/admin/tabelas?legacy=1': "{{ url_for('admin_light_tables') }}",
                '/admin/avisos?legacy=1': "{{ url_for('admin_light_notices') }}",
            }
            for old, new in replacements.items():
                source = source.replace(old, new)

            # Menu Operação: Escala + Blitz + Trocas + Histórico.
            escala_link = '<a class="{{ \'active\' if view==\'escala\' else \'\' }}" href="{{ url_for(\'admin_light_scale\') }}"><i class="bi bi-calendar-week"></i> Escala</a>'
            blitz_link = '<a class="{{ \'active\' if view==\'blitz\' else \'\' }}" href="{{ url_for(\'admin_v10_blitz\') }}"><i class="bi bi-shield-check"></i> Blitz</a>'
            if blitz_link not in source:
                source = source.replace(escala_link, escala_link + "\n        " + blitz_link, 1)
            source = source.replace(
                '<a class="alv8-btn" href="/admin/documentos?legacy=1"><i class="bi bi-folder2"></i> Documentos</a>',
                '<a class="alv8-btn" href="{{ url_for(\'admin_v10_blitz\') }}"><i class="bi bi-shield-check"></i> Blitz</a>',
                1,
            )

            # Upload XLSX continua sendo o modo principal de alimentar/separar a escala.
            marker = '<details class="alv8-card"><summary><strong><i class="bi bi-person-plus"></i> Acrescentar alguém / nova linha na escala</strong></summary>'
            if "Upload da Escala (.xlsx)" not in source:
                upload = '''<div class="alv8-card"><div class="alv8-section-head"><div><h3>Upload da Escala (.xlsx)</h3><p>Envie a planilha oficial. O sistema mantém a separação da escala de cada cooperado.</p></div></div><form method="post" action="{{ url_for('upload_escala') }}" enctype="multipart/form-data" class="alv8-filter"><div class="alv8-field grow"><label>Planilha XLSX</label><input class="alv8-input" type="file" name="arquivo" accept=".xlsx" required></div><button class="alv8-btn primary" type="submit"><i class="bi bi-file-earmark-spreadsheet"></i> Enviar escala</button></form></div>\n\n'''
                source = source.replace(marker, upload + marker, 1)

            # Em páginas que não são Escala, a lista "cooperados" tem outro formato.
            old_coop_options = "const coopOptions=[{% for c in cooperados|default([]) %}{id:{{ c.id }},nome:{{ c.nome|tojson }}}{% if not loop.last %},{% endif %}{% endfor %}];"
            new_coop_options = "const coopOptions={% if view=='escala' %}[{% for c in cooperados|default([]) %}{id:{{ c.id }},nome:{{ c.nome|tojson }}}{% if not loop.last %},{% endif %}{% endfor %}]{% else %}[]{% endif %};"
            source = source.replace(old_coop_options, new_coop_options, 1)

            # Filtro instantâneo da escala por qualquer parte do nome, ignorando acentos e _. 
            old_matches = "function matches(r){const n=(nameF?.value||'').trim().toLowerCase(),ct=(contractF?.value||'').trim().toLowerCase(),free=!!freeF?.checked;const rn=String(r.cooperado_nome||r.cooperado_nome_livre||'').toLowerCase(),rc=String(r.contrato||'').toLowerCase();if(currentDay!=='all'&&String(r.weekday_num)!==String(currentDay))return false;if(n&&!rn.includes(n))return false;if(ct&&rc!==ct)return false;if(free&&(r.cooperado_id||String(r.cooperado_nome_livre||'').trim()))return false;return true}"
            new_matches = "function normV10(v){return String(v??'').normalize('NFD').replace(/[\\u0300-\\u036f]/g,'').replace(/_/g,' ').toLowerCase().replace(/\\s+/g,' ').trim()} function matches(r){const n=normV10(nameF?.value||''),ct=normV10(contractF?.value||''),free=!!freeF?.checked;const rn=normV10(r.cooperado_nome||r.cooperado_nome_livre||''),rc=normV10(r.contrato||'');if(currentDay!=='all'&&String(r.weekday_num)!==String(currentDay))return false;if(n&&!rn.includes(n))return false;if(ct&&rc!==ct)return false;if(free&&(r.cooperado_id||String(r.cooperado_nome_livre||'').trim()))return false;return true}"
            source = source.replace(old_matches, new_matches, 1)

            # Trocas sem colunas/mensagem redundantes.
            start = source.find("  {% elif view=='trocas' %}")
            end = source.find("  {% elif view=='historico' %}", start)
            if start >= 0 and end > start:
                simple_swap = '''  {% elif view=='trocas' %}\n    <div class="alv8-card"><div class="alv8-table-wrap"><table class="alv8-table"><thead><tr><th>Troca</th><th>Status</th><th>Data</th></tr></thead><tbody>{% for t in swap_rows|default([]) %}<tr><td><strong>{{ t.resumo }}</strong></td><td><span class="alv8-badge {{ 'warn' if t.status=='pendente' else 'ok' if t.status in ['aprovada','aceita'] else 'bad' }}">{{ t.status }}</span></td><td>{{ t.aplicada_em.strftime('%d/%m/%Y %H:%M') if t.aplicada_em else (t.criada_em.strftime('%d/%m/%Y %H:%M') if t.criada_em else '—') }}</td></tr>{% else %}<tr><td colspan="3" class="alv8-empty">Nenhuma troca.</td></tr>{% endfor %}</tbody></table></div></div>\n\n'''
                source = source[:start] + simple_swap + source[end:]

        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_admin_v10 = True
    app.jinja_env.cache.clear()


_install_template_v10()
app.logger.info("Admin V10 corrigido: sem regex insegura, financeiro leve, XLSX da escala preservado e Blitz ativa.")
