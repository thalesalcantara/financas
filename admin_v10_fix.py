from __future__ import annotations

import os
import re
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
Usuario = legacy.Usuario
Cooperado = legacy.Cooperado
Restaurante = legacy.Restaurante
Escala = legacy.Escala
TrocaSolicitacao = legacy.TrocaSolicitacao
BUILD = "20260807-1305"


def brl(value) -> str:
    """Formata números no padrão monetário brasileiro, sem alterar o valor salvo."""
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
            continue
    return None


# ---------------------------------------------------------------------------
# Regra única dos estabelecimentos: segunda a domingo.
# ---------------------------------------------------------------------------
@event.listens_for(Restaurante, "before_insert", propagate=True)
def _rest_period_insert(mapper, connection, target):  # pragma: no cover - SQLAlchemy hook
    target.periodo = "seg-dom"


@event.listens_for(Restaurante, "before_update", propagate=True)
def _rest_period_update(mapper, connection, target):  # pragma: no cover - SQLAlchemy hook
    target.periodo = "seg-dom"


try:
    with app.app_context():
        Restaurante.query.filter(or_(Restaurante.periodo != "seg-dom", Restaurante.periodo.is_(None))).update(
            {Restaurante.periodo: "seg-dom"}, synchronize_session=False
        )
        db.session.commit()
except Exception:
    db.session.rollback()
    app.logger.exception("Não foi possível normalizar os estabelecimentos para seg-dom")


# ---------------------------------------------------------------------------
# Mídia persistente: nunca devolve ícone quebrado quando o arquivo antigo sumiu.
# ---------------------------------------------------------------------------
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
    if raw.startswith("http://") or raw.startswith("https://"):
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
    if raw.startswith("http://") or raw.startswith("https://"):
        return redirect(raw)
    if raw:
        rel = raw.lstrip("/")
        if rel.startswith("static/"):
            rel = rel[7:]
        path = os.path.join(legacy.BASE_DIR, "static", rel)
        if os.path.isfile(path):
            return send_file(path, max_age=300)
    return redirect(url_for("static", filename="img/default.png"))


# ---------------------------------------------------------------------------
# Financeiro: mesmo conteúdo/funções do painel antigo, mas apenas uma aba por vez.
# ---------------------------------------------------------------------------
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
    partial_url = url_for("admin_dashboard", **params)
    return render_template(
        "admin_partial_shell_v10.html",
        page_title=item[1],
        active_tab=tab,
        partial_url=partial_url,
        build=BUILD,
    )


# ---------------------------------------------------------------------------
# Estabelecimentos: página própria leve, com as mesmas ações administrativas.
# ---------------------------------------------------------------------------
@app.get("/admin/leve/estabelecimentos", endpoint="admin_v10_establishments")
def admin_v10_establishments():
    denied = light._guard("restaurantes")
    if denied:
        return denied

    q = _norm(request.args.get("q"))
    status = (request.args.get("status") or "todos").strip().lower()
    rows = Restaurante.query.order_by(Restaurante.nome.asc()).all()
    result = []
    for r in rows:
        active = bool(getattr(r, "ativo", True) is not False and getattr(r.usuario_ref, "ativo", True) is not False)
        if status == "ativos" and not active:
            continue
        if status == "inativos" and active:
            continue
        hay = _norm(f"{r.nome} {getattr(r.usuario_ref, 'usuario', '')}")
        if q and q not in hay:
            continue
        result.append(SimpleNamespace(rest=r, active=active))

    return render_template(
        "admin_establishments_v10.html",
        page_title="Estabelecimentos",
        active_tab="estabelecimentos",
        rows=result,
        q=request.args.get("q") or "",
        status=status,
        build=BUILD,
    )


# ---------------------------------------------------------------------------
# Blitz: CNH e CRLV ficam dentro da operação de Escala, sem misturar Documentos.
# ---------------------------------------------------------------------------
@app.get("/admin/leve/blitz", endpoint="admin_v10_blitz")
def admin_v10_blitz():
    denied = light._guard("documentos")
    if denied:
        return denied
    q = _norm(request.args.get("q"))
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
        q=request.args.get("q") or "",
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


# A rota antiga leve de "documentos" representava exatamente CNH/CRLV.
# Mantemos compatibilidade, mas o nome operacional agora é Blitz.
def _documents_redirect_v10():
    return redirect(url_for("admin_v10_blitz"))


app.view_functions["admin_light_documents"] = _documents_redirect_v10


# ---------------------------------------------------------------------------
# Escala: busca parcial por qualquer parte do nome, ignorando acento, _ e caixa.
# ---------------------------------------------------------------------------
def _admin_light_scale_v10():
    denied = light._guard("escalas")
    if denied:
        return denied

    q_raw = (request.args.get("q") or "").strip()
    q = _norm(q_raw)
    active_ids, _active_names, active_coops = light._active_coop_ids_names()
    coop_by_id = {c.id: c for c in active_coops}
    restaurants = (
        Restaurante.query.filter(or_(Restaurante.ativo.is_(True), Restaurante.ativo.is_(None)))
        .order_by(Restaurante.nome.asc()).all()
    )

    candidates = Escala.query.order_by(Escala.id.asc()).limit(1400).all()
    scales = []
    scale_rows = []
    contracts = set()
    for s in candidates:
        if s.cooperado_id and s.cooperado_id not in active_ids:
            continue

        coop = coop_by_id.get(s.cooperado_id) if s.cooperado_id else None
        current_name = coop.nome if coop else (s.cooperado_nome or "").strip()
        hay = _norm(" ".join(str(x or "") for x in (s.data, s.turno, s.horario, s.contrato, current_name)))
        if q and q not in hay:
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
    contract_options = sorted(contracts, key=lambda x: _norm(x))
    return light._render(
        "escala",
        "Escala",
        "Escala semanal completa. A busca encontra qualquer parte do nome do cooperado.",
        q=q_raw,
        scales=scales,
        scale_rows=scale_rows,
        contract_options=contract_options,
        coop_map=coop_by_id,
        rest_map={r.id: r for r in restaurants},
        cooperados=active_coops,
        restaurantes=restaurants,
    )


app.view_functions["admin_light_scale"] = _admin_light_scale_v10


# ---------------------------------------------------------------------------
# Trocas: uma linha clara por solicitação; sem mensagem técnica/redundante.
# ---------------------------------------------------------------------------
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
        solicitante = coop_map.get(t.solicitante_id)
        destino = coop_map.get(t.destino_id)
        sn = solicitante.nome if solicitante else "Cooperado"
        dn = destino.nome if destino else "Cooperado"
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
        "Uma informação por troca, sem redundância. A escala aprovada é a mesma fonte usada em todo o sistema.",
        swap_rows=result,
        trocas=rows,
        coop_map=coop_map,
        scale_map=scale_map,
    )


app.view_functions["admin_light_swaps"] = _admin_light_swaps_v10


# ---------------------------------------------------------------------------
# Redireciona CRUDs antigos para a tela leve correspondente depois de salvar.
# ---------------------------------------------------------------------------
@app.after_request
def _admin_v10_redirect_light(response):
    try:
        if (session.get("user_tipo") or "").strip().lower() != "admin":
            return response
        if response.status_code not in (301, 302, 303, 307, 308):
            return response
        location = response.headers.get("Location") or ""
        if not location:
            return response
        parsed = urlparse(location)
        qs = parse_qs(parsed.query)
        tab = (qs.get("tab") or [""])[0]
        if tab in _FINANCE:
            response.headers["Location"] = url_for("admin_v10_finance", tab=tab)
        elif tab == "restaurantes":
            response.headers["Location"] = url_for("admin_v10_establishments")
        elif tab == "cooperados":
            response.headers["Location"] = url_for("admin_light_cooperatives")
    except Exception:
        app.logger.exception("Falha ao reescrever retorno do Admin V10")
    return response


# ---------------------------------------------------------------------------
# Reparos de template do V8 sem duplicar a lógica das páginas que já estão boas.
# ---------------------------------------------------------------------------
def _install_template_v10() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_admin_v10", False):
        return
    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)

        if template in {"admin_light_v8.html", "admin_dashboard.html", "admin_lancamentos.html"}:
            source = source.replace("'%.2f'|format(", "brl(")
            source = re.sub(
                r'<label[^>]*>\s*<input[^>]*name=["\']considerar_periodo["\'][^>]*>.*?</label>',
                "",
                source,
                flags=re.I | re.S,
            )
            source = source.replace(" ({{ r.periodo }})", "")
            source = source.replace("({{ r.periodo }})", "")

        if template == "admin_light_v8.html":
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

            # Blitz fica dentro da Operação/Escala; Documentos gerais continuam em Cadastros.
            escala_link = "<a class=\"{{ 'active' if view=='escala' else '' }}\" href=\"{{ url_for('admin_light_scale') }}\"><i class=\"bi bi-calendar-week\"></i> Escala</a>"
            blitz_link = "<a class=\"{{ 'active' if view=='blitz' else '' }}\" href=\"{{ url_for('admin_v10_blitz') }}\"><i class=\"bi bi-shield-check\"></i> Blitz</a>"
            if blitz_link not in source:
                source = source.replace(escala_link, escala_link + "\n        " + blitz_link, 1)
            source = source.replace(
                '<a class="alv8-btn" href="/admin/documentos?legacy=1"><i class="bi bi-folder2"></i> Documentos</a>',
                '<a class="alv8-btn" href="{{ url_for(\'admin_v10_blitz\') }}"><i class="bi bi-shield-check"></i> Blitz</a>',
                1,
            )

            # Corrige o 500 de Cooperados: o JS da Escala não pode serializar linhas de outro tipo de tela.
            source = re.sub(
                r"const coopOptions=\[\{% for c in cooperados\|default\(\[\]\) %\}.*?\{% endfor %\}\];",
                "const coopOptions={% if view=='escala' %}[{% for c in cooperados|default([]) %}{id:{{ c.id }},nome:{{ c.nome|tojson }}}{% if not loop.last %},{% endif %}{% endfor %}]{% else %}[]{% endif %};",
                source,
                count=1,
                flags=re.S,
            )

            # Busca instantânea da grade: nome parcial, sem acentos e sem diferença de maiúsculas.
            source = re.sub(
                r"function matches\(r\)\{.*?return true\}",
                r"function normV10(v){return String(v??'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/_/g,' ').toLowerCase().replace(/\s+/g,' ').trim()} function matches(r){const n=normV10(nameF?.value||''),ct=normV10(contractF?.value||''),free=!!freeF?.checked;const rn=normV10(r.cooperado_nome||r.cooperado_nome_livre||''),rc=normV10(r.contrato||'');if(currentDay!=='all'&&String(r.weekday_num)!==String(currentDay))return false;if(n&&!rn.includes(n))return false;if(ct&&rc!==ct)return false;if(free&&(r.cooperado_id||String(r.cooperado_nome_livre||'').trim()))return false;return true}",
                source,
                count=1,
                flags=re.S,
            )

            # Trocas: remove colunas redundantes e mensagem técnica.
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
app.logger.info("Admin V10 carregado: financeiro sob demanda, estabelecimentos leves, Blitz e correções de escala/cooperados.")
