from __future__ import annotations

from datetime import datetime, time, timedelta

from flask import has_request_context, jsonify, render_template, request, session
from sqlalchemy import and_, event, func, or_, select
from sqlalchemy.orm import Session, with_loader_criteria

import cooperative_notifications_ui as notifications
import performance_ui_fix as perf
import production_shift_time_fix as shifts

app = notifications.app
flow = shifts.flow
upgrade = shifts.upgrade
TZ = shifts.TZ

Usuario = shifts.patch.flow.Usuario
Cooperado = shifts.patch.Cooperado
Restaurante = shifts.patch.Restaurante
Escala = shifts.patch.Escala
TrocaSolicitacao = shifts.patch.flow.TrocaSolicitacao

BUILD = "20260807-1042"


def _operational_filter_enabled() -> bool:
    """Inativos só podem aparecer na gestão de cooperados do administrador."""
    if not has_request_context():
        return False

    path = request.path or ""
    endpoint = request.endpoint or ""

    # A aba de cooperados precisa enxergar ativos e inativos para permitir reativação.
    if endpoint == "admin_dashboard":
        tab = (request.args.get("tab") or "lancamentos").strip().lower()
        if tab == "cooperados":
            return False

    # CRUD/status do cadastro também precisa localizar o registro inativo.
    if path.startswith("/admin/cooperados"):
        return False
    if endpoint == "toggle_status_cooperado":
        return False

    return True


# Subconsultas reutilizáveis. Escalas e trocas de usuários inativos deixam de
# participar de contagens, contratos, trocas e telas operacionais.
_active_user_ids = select(Usuario.id).where(Usuario.ativo.is_(True))
_active_coop_ids = (
    select(Cooperado.id)
    .join(Usuario, Cooperado.usuario_id == Usuario.id)
    .where(Usuario.ativo.is_(True))
)
_inactive_names = (
    select(func.lower(func.replace(func.trim(Cooperado.nome), "_", " ")))
    .join(Usuario, Cooperado.usuario_id == Usuario.id)
    .where(Usuario.ativo.is_(False))
)


if not app.extensions.get("coopex_active_only_orm_v5"):
    @event.listens_for(Session, "do_orm_execute")
    def _coopex_active_only_orm(execute_state):
        if not execute_state.is_select or not _operational_filter_enabled():
            return

        normalized_scale_name = func.lower(func.replace(func.trim(Escala.cooperado_nome), "_", " "))
        scale_active = or_(
            Escala.cooperado_id.in_(_active_coop_ids),
            and_(
                Escala.cooperado_id.is_(None),
                Escala.cooperado_nome.isnot(None),
                ~normalized_scale_name.in_(_inactive_names),
            ),
        )
        troca_active = and_(
            TrocaSolicitacao.solicitante_id.in_(_active_coop_ids),
            TrocaSolicitacao.destino_id.in_(_active_coop_ids),
        )

        execute_state.statement = execute_state.statement.options(
            with_loader_criteria(
                Cooperado,
                Cooperado.usuario_id.in_(_active_user_ids),
                include_aliases=True,
            ),
            with_loader_criteria(Escala, scale_active, include_aliases=True),
            with_loader_criteria(TrocaSolicitacao, troca_active, include_aliases=True),
        )

    app.extensions["coopex_active_only_orm_v5"] = True


def _active_coops_by_id_and_name():
    rows = (
        Cooperado.query
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Usuario.ativo.is_(True))
        .with_entities(Cooperado.id, Cooperado.nome)
        .order_by(Cooperado.nome.asc())
        .all()
    )
    by_id = {int(cid): name for cid, name in rows}
    by_name = {shifts.patch._norm(name): int(cid) for cid, name in rows if shifts.patch._norm(name)}
    return by_id, by_name


def _rest_current_shift_coop_ids(rest: Restaurante) -> list[int]:
    """Cooperados que estão dentro do horário do turno neste exato momento."""
    now = datetime.now(TZ)
    today = now.date()
    yesterday = today - timedelta(days=1)
    by_id, by_name = _active_coops_by_id_and_name()

    result: set[int] = set()
    for scale in shifts.query_override._rest_scales_indexed(rest):
        data_ref = shifts.exact_scale_date(scale, today)
        if data_ref not in {today, yesterday}:
            continue

        start_text, end_text = upgrade._times_from_text(scale.horario)
        start_minutes = upgrade._minutes(start_text)
        end_at = flow._end_at(data_ref, start_text, end_text)
        if start_minutes is None or end_at is None:
            continue

        start_at = datetime.combine(
            data_ref,
            time(start_minutes // 60, start_minutes % 60),
            tzinfo=TZ,
        )
        if not (start_at <= now < end_at):
            continue

        coop_id = int(scale.cooperado_id) if scale.cooperado_id in by_id else None
        if coop_id is None and scale.cooperado_nome:
            coop_id = by_name.get(shifts.patch._norm(scale.cooperado_nome))
        if coop_id:
            result.add(coop_id)

    return sorted(result)


# O contexto anterior calculava a timeline do cooperado até quando a aba aberta
# era Resumo. Removemos esse custo; a timeline passa a ser carregada via AJAX
# somente ao abrir Produções.
_processors = app.template_context_processors.get(None, [])
app.template_context_processors[None] = [
    fn for fn in _processors if getattr(fn, "__name__", "") != "_coopex_fast_context"
]


@app.context_processor
def _coopex_v5_context():
    context = {
        "coopex_rest_display_name": "ESTABELECIMENTO",
        "coopex_rest_pending_rows": [],
        "coopex_rest_current_coop_ids": [],
        "coopex_coop_timeline": [],
        "coopex_filter_start": None,
        "coopex_filter_end": None,
        "coopex_timeline_deferred": True,
    }
    role = (session.get("user_tipo") or "").strip().lower()

    try:
        if role == "restaurante" and request.endpoint == "portal_restaurante":
            rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first()
            if rest:
                context["coopex_rest_display_name"] = notifications.menu.final_ui.perf.ui._norm_name(rest.nome)
                view = (request.args.get("view") or "lancar").strip().lower()
                if view == "lancar":
                    context["coopex_rest_pending_rows"] = perf._pending_approvals_fast(rest)
                    context["coopex_rest_current_coop_ids"] = _rest_current_shift_coop_ids(rest)
    except Exception:
        app.logger.exception("Falha ao montar contexto operacional V5")

    return context


if "coop_timeline_v5" not in app.view_functions:
    @app.get("/api/coop/timeline", endpoint="coop_timeline_v5")
    def coop_timeline_v5():
        if (session.get("user_tipo") or "").strip().lower() != "cooperado":
            return jsonify(ok=False), 403

        coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first()
        if not coop:
            return jsonify(ok=False), 404

        today = datetime.now(TZ).date()
        start = upgrade._parse_date(request.args.get("data_inicio")) or today
        end = upgrade._parse_date(request.args.get("data_fim")) or start
        if end < start:
            start, end = end, start
        if (end - start).days > 31:
            end = start + timedelta(days=31)

        rows = perf._timeline_fast(coop, start, end)
        return render_template(
            "_coop_timeline.html",
            coopex_coop_timeline=rows,
            coopex_filter_start=start,
            coopex_filter_end=end,
        )


def _install_v5_templates() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_operational_v5", False):
        return

    original_get_source = loader.get_source
    css_tag = """<link rel="stylesheet" href="{{ url_for('static', filename='css/operational_v5.css', v='__BUILD__') }}">""".replace("__BUILD__", BUILD)

    timeline_loader_js = r'''
<script id="coopexTimelineDeferredV5">
(function(){
  let loaded=false, loading=false;
  const targetId='coopexTimelineDeferred';
  async function loadTimeline(){
    const target=document.getElementById(targetId);
    if(!target||loaded||loading)return;
    loading=true;
    target.innerHTML='<div class="coopex-v5-loading">Carregando escala e produção…</div>';
    try{
      const url=new URL('/api/coop/timeline',window.location.origin);
      const form=document.getElementById('periodoForm');
      const di=form?.querySelector('[name="data_inicio"]')?.value||'';
      const df=form?.querySelector('[name="data_fim"]')?.value||'';
      if(di)url.searchParams.set('data_inicio',di);
      if(df)url.searchParams.set('data_fim',df);
      const r=await fetch(url.toString(),{credentials:'same-origin',cache:'no-store'});
      if(!r.ok)throw new Error('timeline');
      target.innerHTML=await r.text();
      loaded=true;
    }catch(e){
      target.innerHTML='<div class="coopex-v5-loading">Não foi possível carregar a escala agora. Toque novamente em Produções.</div>';
    }finally{loading=false;}
  }
  document.addEventListener('DOMContentLoaded',function(){
    document.querySelectorAll('.tab-btn[data-tab="producoes"]').forEach(btn=>btn.addEventListener('click',loadTimeline));
    const p=new URLSearchParams(location.search);
    if((p.get('active_tab')||'')==='producoes')loadTimeline();
  });
})();
</script>
'''

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)

        if template == "restaurante_dashboard.html":
            # A lista inicial passa de "escalados hoje" para "no turno agora".
            source = source.replace(
                "{{ '1' if (coop.escalado_hoje is defined and coop.escalado_hoje) else '0' }}",
                "{{ '1' if coop.id in coopex_rest_current_coop_ids else '0' }}",
            )
            source = source.replace(
                "{% if not (coop.escalado_hoje is defined and coop.escalado_hoje) %}hidden{% endif %}",
                "{% if coop.id not in coopex_rest_current_coop_ids %}hidden{% endif %}",
            )
            source = source.replace(
                "{{ '1' if (coop.escalado_hoje if coop.escalado_hoje is defined else false) else '0' }}",
                "{{ '1' if coop.id in coopex_rest_current_coop_ids else '0' }}",
            )
            source = source.replace(
                "{% if not (coop.escalado_hoje if coop.escalado_hoje is defined else false) %}display:none;{% endif %}",
                "{% if coop.id not in coopex_rest_current_coop_ids %}display:none;{% endif %}",
            )
            source = source.replace(
                "{% if coop.escalado_hoje %}\n                                Escalado hoje",
                "{% if coop.id in coopex_rest_current_coop_ids %}\n                                No turno agora",
            )

            if "operational_v5.css" not in source:
                source = source.replace("</head>", "  " + css_tag + "\n</head>", 1)

        elif template == "painel_cooperado.html":
            include = "{% include '_coop_timeline.html' %}"
            if include in source:
                source = source.replace(
                    include,
                    '<div id="coopexTimelineDeferred"><div class="coopex-v5-loading">Abra Produções para carregar a escala do período.</div></div>',
                    1,
                )
            if "coopexTimelineDeferredV5" not in source:
                source = source.replace("</body>", timeline_loader_js + "\n</body>", 1)
            if "operational_v5.css" not in source:
                source = source.replace("</head>", "  " + css_tag + "\n</head>", 1)

        elif template == "admin_dashboard.html":
            # Não pré-carrega todas as abas. Cada aba passa a carregar somente ao clicar.
            source = source.replace("    warmQueue();", "    /* V5: abas pesadas carregam apenas sob demanda */")
            if "operational_v5.css" not in source:
                source = source.replace("</head>", "  " + css_tag + "\n</head>", 1)

        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_operational_v5 = True
    app.jinja_env.cache.clear()


_install_v5_templates()
