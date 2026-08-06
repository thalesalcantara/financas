from __future__ import annotations

import re
from functools import wraps

from flask import redirect, request, session, url_for
from sqlalchemy import or_

import production_scale_patch as production_patch

app = production_patch.app
flow = production_patch.flow
Escala = production_patch.Escala
Cooperado = production_patch.Cooperado
Restaurante = production_patch.Restaurante
ProducaoCooperado = production_patch.ProducaoCooperado


# ---------------------------------------------------------------------------
# Vínculos tolerantes a dados antigos/importados
# ---------------------------------------------------------------------------
def _coop_scales_robust(coop):
    target = production_patch._norm(coop.nome)
    candidates = (
        Escala.query.filter(
            or_(
                Escala.cooperado_id == coop.id,
                Escala.cooperado_nome.isnot(None),
            )
        )
        .order_by(Escala.id.asc())
        .limit(2000)
        .all()
    )
    result = []
    seen = set()
    for scale in candidates:
        name_match = bool(target and production_patch._norm(scale.cooperado_nome) == target)
        if (scale.cooperado_id == coop.id or name_match) and scale.id not in seen:
            seen.add(scale.id)
            result.append(scale)
    return result


def _scale_belongs_to_coop_robust(scale, coop) -> bool:
    return bool(
        scale.cooperado_id == coop.id
        or (
            production_patch._norm(scale.cooperado_nome)
            and production_patch._norm(scale.cooperado_nome)
            == production_patch._norm(coop.nome)
        )
    )


def _rest_scales_robust(rest):
    target = production_patch._norm(rest.nome)
    candidates = (
        Escala.query.filter(
            or_(
                Escala.restaurante_id == rest.id,
                Escala.contrato.isnot(None),
            )
        )
        .order_by(Escala.id.asc())
        .limit(2400)
        .all()
    )
    result = []
    seen = set()
    for scale in candidates:
        contract = production_patch._norm(scale.contrato)
        name_match = bool(
            target
            and contract
            and (target == contract or target in contract or contract in target)
        )
        if (scale.restaurante_id == rest.id or name_match) and scale.id not in seen:
            seen.add(scale.id)
            result.append(scale)
    return result


production_patch._coop_scales = _coop_scales_robust
production_patch._rest_scales = _rest_scales_robust
flow._scale_belongs_to_coop = _scale_belongs_to_coop_robust


# ---------------------------------------------------------------------------
# Dados das produções dentro dos painéis já existentes
# ---------------------------------------------------------------------------
if not app.extensions.get("coopex_inline_production_context"):
    @app.context_processor
    def _coopex_inline_production_context():
        try:
            role = (session.get("user_tipo") or "").strip().lower()
            if role == "cooperado" and request.endpoint == "portal_cooperado":
                coop = Cooperado.query.filter_by(
                    usuario_id=session.get("user_id")
                ).first()
                rows = flow._coop_scale_rows(coop) if coop else []
                return {"coopex_coop_week_rows": rows}

            if role == "restaurante" and request.endpoint == "portal_restaurante":
                rest = Restaurante.query.filter_by(
                    usuario_id=session.get("user_id")
                ).first()
                rows = production_patch._rest_scale_rows(rest) if rest else []
                pending = [
                    row
                    for row in rows
                    if row.producao
                    and row.producao.status == "pendente"
                    and float(row.producao.valor_total or 0) > 0
                ]
                return {
                    "coopex_rest_week_rows": rows,
                    "coopex_rest_pending_rows": pending,
                }
        except Exception:
            app.logger.exception("Falha ao carregar produções dentro do painel")
        return {
            "coopex_coop_week_rows": [],
            "coopex_rest_week_rows": [],
            "coopex_rest_pending_rows": [],
        }

    app.extensions["coopex_inline_production_context"] = True


# Ao lançar dentro da aba Produções, retorna para a mesma aba do painel.
if not app.extensions.get("coopex_coop_inline_redirect"):
    _original_coop_endpoint = app.view_functions.get("coop_producao")

    if _original_coop_endpoint:
        @wraps(_original_coop_endpoint)
        def _coop_producao_inline_redirect(*args, **kwargs):
            response = _original_coop_endpoint(*args, **kwargs)
            if (
                request.method == "POST"
                and request.form.get("return_to") == "painel"
            ):
                return redirect(url_for("portal_cooperado", active_tab="producoes"))
            return response

        app.view_functions["coop_producao"] = _coop_producao_inline_redirect
        app.view_functions["coop_producao_nova"] = _coop_producao_inline_redirect

    app.extensions["coopex_coop_inline_redirect"] = True


_RESTAURANT_DESIGN_CSS = r"""
/* COOPEX — painel horizontal azul royal, desktop e celular */
:root{
  --royal:#064fc8;
  --royal-2:#07348f;
  --royal-soft:#edf4ff;
  --bg:#f5f7fb;
  --text:#07183e;
  --muted:#53627e;
  --border:#dce4f1;
}
body{background:#f5f7fb!important;color:var(--text)!important}
.shell{display:block!important;min-height:100vh!important}
.aside{
  position:sticky!important;top:0!important;z-index:1050!important;
  width:100%!important;height:74px!important;min-height:74px!important;
  padding:0 24px!important;display:flex!important;flex-direction:row!important;
  align-items:stretch!important;gap:18px!important;color:#fff!important;
  background:linear-gradient(90deg,#062b80 0%,#0545b7 52%,#063897 100%)!important;
  box-shadow:0 5px 18px rgba(4,45,126,.18)!important;
  overflow:visible!important;
}
.brand{
  width:260px!important;min-width:260px!important;margin:0!important;padding:0!important;
  border:0!important;border-radius:0!important;background:transparent!important;
  box-shadow:none!important;display:flex!important;align-items:center!important;
  font-size:1.03rem!important;line-height:1.15!important;
}
.brand i{width:44px!important;height:44px!important;border-radius:10px!important;border:2px solid rgba(255,255,255,.75)!important;background:rgba(255,255,255,.08)!important;font-size:1.15rem!important}
.brand span{display:inline!important}
.theme-toggle{display:none!important}
.navmenu{
  margin:0!important;display:flex!important;flex:1!important;min-width:0!important;
  flex-direction:row!important;align-items:stretch!important;gap:0!important;
  overflow-x:auto!important;overflow-y:hidden!important;scrollbar-width:none!important;
}
.navmenu::-webkit-scrollbar{display:none!important}
.navmenu a{
  min-height:74px!important;margin:0!important;padding:0 18px!important;
  border:0!important;border-radius:0!important;display:flex!important;
  flex-direction:row!important;align-items:center!important;justify-content:center!important;
  gap:8px!important;white-space:nowrap!important;font-size:.88rem!important;
  font-weight:750!important;color:#fff!important;transform:none!important;
}
.navmenu a span{display:inline!important}
.navmenu a:hover,.navmenu a.active{background:rgba(255,255,255,.10)!important;box-shadow:none!important}
.navmenu a.active::before{left:14px!important;right:14px!important;top:auto!important;bottom:0!important;width:auto!important;height:4px!important;background:#fff!important}
.badge-dot{margin-left:2px!important}
.content{width:100%!important;max-width:1920px!important;margin:0 auto!important;padding:18px 24px 32px!important}
.coopex-welcome-bar{display:none!important}
.hero-panel{
  min-height:120px!important;padding:22px 24px!important;margin-bottom:14px!important;
  border-radius:14px!important;background:#fff!important;border:1px solid var(--border)!important;
  box-shadow:0 4px 15px rgba(15,38,85,.05)!important;vertical-align:top!important;
}
.hero-title{font-size:1.55rem!important;color:#07183e!important}
.hero-subtitle{font-size:.94rem!important;color:#3e4f70!important}
section[data-view="lancar"]>.hero-panel{display:inline-flex!important;width:65.5%!important;flex-direction:column!important;justify-content:center!important;margin-right:1%!important}
section[data-view="lancar"]>.pending-wrap{display:inline-grid!important;width:33%!important;vertical-align:top!important;margin-bottom:14px!important}
section[data-view="lancar"]>.pending-wrap .pending-col{grid-column:span 12!important}
.pending-card{min-height:120px!important;border-radius:14px!important;background:#fff!important;display:flex!important;flex-direction:column!important;justify-content:center!important;padding:18px 20px!important}
.card,.card-soft,.lancar-layout-card,.table-box,.coop-list-box{border-radius:14px!important;border-color:var(--border)!important;box-shadow:0 4px 15px rgba(15,38,85,.045)!important}
.btn-royal,.btn-outline-royal{border-radius:9px!important}
.btn-royal{background:#064fc8!important}
.form-control,.form-select{border-radius:8px!important;min-height:42px!important}
.form-label{text-transform:none!important;letter-spacing:0!important;font-size:.82rem!important;color:#18315f!important}
.cooperado-item{border-radius:10px!important}
.selected-header{text-align:left!important;background:#fff!important;border:0!important;border-bottom:1px solid var(--border)!important;border-radius:0!important}
.selected-header img.big{width:56px!important;height:56px!important}
.table thead th{background:#f8faff!important;color:#1c315a!important;text-transform:none!important;font-size:.78rem!important}
.coopex-inline-board{clear:both;margin:0 0 14px;background:#fff;border:1px solid var(--border);border-radius:14px;padding:16px 18px;box-shadow:0 4px 15px rgba(15,38,85,.045)}
.coopex-inline-title{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px}
.coopex-inline-title strong{font-size:1rem;color:#07183e}
.coopex-inline-count{background:#eaf2ff;color:#064fc8;border-radius:999px;padding:5px 9px;font-size:.76rem;font-weight:800}
.coopex-proposal-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.coopex-proposal{border:1px solid #cfe0fa;border-left:4px solid #064fc8;border-radius:11px;padding:12px;background:#fbfdff}
.coopex-proposal-head{display:flex;justify-content:space-between;gap:10px;margin-bottom:8px}
.coopex-proposal-name{font-weight:800;font-size:.9rem}
.coopex-proposal-meta{font-size:.75rem;color:#60708c;margin-top:3px}
.coopex-proposal-value{font-weight:850;color:#064fc8;white-space:nowrap}
.coopex-proposal-form{display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:end}
.coopex-proposal-form label{display:block;font-size:.7rem;font-weight:800;color:#53627e;margin-bottom:4px}
.coopex-proposal-form input{width:100%;height:38px;border:1px solid var(--border);border-radius:8px;padding:0 9px}
.coopex-proposal-form button{height:38px;border:0;border-radius:8px;background:#064fc8;color:#fff;padding:0 13px;font-weight:800}
@media(max-width:999px){
  .aside{height:auto!important;min-height:0!important;padding:10px 12px 0!important;flex-wrap:wrap!important;gap:6px!important}
  .brand{width:100%!important;min-width:0!important;height:46px!important;justify-content:flex-start!important}
  .brand i{width:36px!important;height:36px!important}
  .navmenu{width:100%!important;flex:0 0 100%!important;height:53px!important}
  .navmenu a{min-height:53px!important;padding:0 12px!important;font-size:.78rem!important}
  .navmenu a i{font-size:.92rem!important}
  .content{padding:12px 10px 24px!important}
  section[data-view="lancar"]>.hero-panel,section[data-view="lancar"]>.pending-wrap{display:flex!important;width:100%!important;margin-right:0!important}
  .hero-panel{min-height:0!important;padding:16px!important}
  .hero-title{font-size:1.22rem!important}
  .pending-card{min-height:0!important}
  .coopex-proposal-grid{grid-template-columns:1fr!important}
  .coopex-proposal-form{grid-template-columns:1fr 1fr!important}
  .coopex-proposal-form button{grid-column:1/-1!important}
}
@media(max-width:575px){
  .navmenu a span{display:inline!important}
  .navmenu a{padding:0 10px!important}
  .coopex-proposal-form{grid-template-columns:1fr!important}
  .coopex-proposal-form button{grid-column:auto!important;width:100%!important}
}
"""


_COOP_DESIGN_CSS = r"""
.coopex-week-board{margin-bottom:14px}
.coopex-week-intro{border:1px solid rgba(39,71,217,.13);background:linear-gradient(135deg,#f6f9ff,#fff);border-radius:17px;padding:13px 14px;margin-bottom:10px}
.coopex-week-intro strong{display:block;color:#1838bb;font-size:.94rem}
.coopex-week-intro span{display:block;color:#64748b;font-size:.78rem;margin-top:3px}
.coopex-week-list{display:flex;flex-direction:column;gap:10px}
.coopex-week-card{border:1px solid rgba(15,23,42,.08);border-left:5px solid #dc2626;border-radius:16px;background:#fff;padding:12px;box-shadow:0 7px 18px rgba(15,23,42,.045)}
.coopex-week-card.green{border-left-color:#16a34a}
.coopex-week-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}
.coopex-week-place{font-weight:800;color:#0f172a;font-size:.92rem}
.coopex-week-meta{font-size:.76rem;color:#64748b;margin-top:3px}
.coopex-week-status{border-radius:999px;padding:5px 8px;background:#fee2e2;color:#991b1b;font-size:.65rem;font-weight:850;text-align:center;max-width:165px}
.coopex-week-card.green .coopex-week-status{background:#dcfce7;color:#166534}
.coopex-week-value{font-size:1.15rem;font-weight:850;color:#0f172a;margin-top:9px}
.coopex-week-lock{margin-top:9px;border-radius:10px;padding:9px 10px;background:#f1f5f9;color:#64748b;font-size:.75rem;font-weight:700}
.coopex-week-form{margin-top:10px;display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:end}
.coopex-week-form label{display:block;font-size:.68rem;font-weight:800;color:#64748b;margin-bottom:4px}
.coopex-week-form input{width:100%;height:40px;border:1px solid rgba(15,23,42,.12);border-radius:10px;padding:0 10px}
.coopex-week-form button{height:40px;border:0;border-radius:10px;background:linear-gradient(135deg,#2747d9,#3157ff);color:#fff;padding:0 13px;font-weight:800}
@media(max-width:480px){.coopex-week-head{flex-direction:column}.coopex-week-status{max-width:none}.coopex-week-form{grid-template-columns:1fr}.coopex-week-form button{width:100%}}
"""


_RESTAURANT_PENDING_BLOCK = r"""
        {% set _coopex_pending = coopex_rest_pending_rows|default([]) %}
        {% if _coopex_pending %}
        <div class="coopex-inline-board">
          <div class="coopex-inline-title">
            <strong><i class="bi bi-clipboard-check me-1"></i> Valores enviados pelos cooperados</strong>
            <span class="coopex-inline-count">{{ _coopex_pending|length }} para conferir</span>
          </div>
          <div class="coopex-proposal-grid">
            {% for x in _coopex_pending %}
            <div class="coopex-proposal">
              <div class="coopex-proposal-head">
                <div>
                  <div class="coopex-proposal-name">{{ x.cooperado.nome if x.cooperado else (x.escala.cooperado_nome or 'Cooperado') }}</div>
                  <div class="coopex-proposal-meta">{{ x.data.strftime('%d/%m/%Y') if x.data else x.escala.data }} · {{ x.escala.horario or 'Sem horário' }}</div>
                </div>
                <div class="coopex-proposal-value">R$ {{ '%.2f'|format(x.producao.valor_total or 0)|replace('.', ',') }}</div>
              </div>
              <form class="coopex-proposal-form" method="post" action="{{ url_for('rest_producao_aprovar', item_id=x.producao.id) }}">
                <div><label>Entregas finais</label><input type="number" min="1" name="qtd_entregas" value="{{ x.producao.qtd_entregas or '' }}" required></div>
                <div><label>Valor total final</label><input type="number" min="0.01" step="0.01" name="valor_total" value="{{ '%.2f'|format(x.producao.valor_total or 0) }}" required></div>
                <button type="submit"><i class="bi bi-check-circle me-1"></i>Aprovar</button>
              </form>
            </div>
            {% endfor %}
          </div>
        </div>
        {% endif %}

"""


_COOP_WEEK_BLOCK = r"""
      {% set _coopex_week = coopex_coop_week_rows|default([]) %}
      <div class="coopex-week-board">
        <div class="coopex-week-intro">
          <strong><i class="bi bi-calendar2-check me-1"></i> Produções previstas nesta semana</strong>
          <span>Os plantões aparecem com R$ 0,00. O preenchimento é liberado somente depois do término do seu horário.</span>
        </div>
        <div class="coopex-week-list">
          {% for x in _coopex_week %}
          <div class="coopex-week-card {{ x.color }}">
            <div class="coopex-week-head">
              <div>
                <div class="coopex-week-place">{{ x.restaurante.nome|replace('_',' ') if x.restaurante else (x.escala.contrato|replace('_',' ') or 'Estabelecimento não vinculado') }}</div>
                <div class="coopex-week-meta">{{ x.data.strftime('%d/%m/%Y') if x.data else (x.escala.data or 'Sem data') }} · {{ x.escala.horario or 'Sem horário' }}</div>
              </div>
              <span class="coopex-week-status">{{ x.status_label }}</span>
            </div>
            <div class="coopex-week-value">R$ {{ '%.2f'|format(x.valor_total or 0)|replace('.', ',') }}</div>

            {% if x.pode_lancar %}
            <form class="coopex-week-form" method="post" action="{{ url_for('coop_producao') }}">
              <input type="hidden" name="return_to" value="painel">
              <input type="hidden" name="escala_id" value="{{ x.escala.id }}">
              <div><label>Quantidade de entregas</label><input type="number" min="1" name="qtd_entregas" required></div>
              <div><label>Valor total da produção</label><input type="number" min="0.01" step="0.01" name="valor_total" required></div>
              <button type="submit"><i class="bi bi-send-check me-1"></i>Lançar</button>
            </form>
            {% elif not x.finalizada and not x.lancamento and not x.producao %}
            <div class="coopex-week-lock"><i class="bi bi-lock-fill me-1"></i>Bloqueado até o término do horário.</div>
            {% elif not x.restaurante %}
            <div class="coopex-week-lock"><i class="bi bi-exclamation-triangle me-1"></i>Escala sem estabelecimento vinculado. A administração precisa corrigir o vínculo.</div>
            {% endif %}
          </div>
          {% else %}
          <div class="item-card text-center muted">Nenhuma escala da semana foi vinculada ao seu cadastro.</div>
          {% endfor %}
        </div>
      </div>

"""


def _install_panel_design_and_inline_productions() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_final_panel_patch", False):
        return

    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)

        if template == "restaurante_dashboard.html":
            # Retira a aba extra: tudo fica dentro do Lançar Produção.
            source = re.sub(
                r'<a\s+data-coopex-producoes="1".*?</a>\s*',
                "",
                source,
                count=1,
                flags=re.S,
            )
            if "COOPEX — painel horizontal azul royal" not in source:
                source = source.replace("</style>", _RESTAURANT_DESIGN_CSS + "\n</style>", 1)
            marker = "        {% set _pendencias ="
            if "coopex-inline-board" not in source and marker in source:
                source = source.replace(marker, _RESTAURANT_PENDING_BLOCK + marker, 1)

        elif template == "painel_cooperado.html":
            if "coopex-week-board" not in source:
                source = source.replace("</style>", _COOP_DESIGN_CSS + "\n</style>", 1)
                marker = '<section class="tab-pane-custom" id="tab-producoes">'
                if marker in source:
                    source = source.replace(marker, marker + "\n" + _COOP_WEEK_BLOCK, 1)

        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_final_panel_patch = True
    app.jinja_env.cache.clear()


_install_panel_design_and_inline_productions()
