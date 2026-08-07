from __future__ import annotations

import re
from datetime import datetime, timedelta

from flask import flash, jsonify, redirect, request, session, url_for
from sqlalchemy import text as sa_text

import menu_horizontal_enforcer as menu
import production_shift_time_fix as shifts

app = menu.app
db = shifts.patch.db
Usuario = shifts.patch.flow.Usuario
Cooperado = shifts.patch.Cooperado
Restaurante = shifts.patch.Restaurante
Escala = shifts.patch.Escala
Lancamento = shifts.patch.Lancamento
ProducaoCooperado = shifts.patch.ProducaoCooperado
TZ = shifts.patch.TZ
BUILD = "20260807-1458"

_NO_SHOW_PREFIX = "NAO_COMPARECEU|"
_SUBSTITUTE_PREFIX = "SUBSTITUIDO|"


if "coop_latest_incoming_evaluation" not in app.view_functions:
    @app.get("/api/coop/avaliacoes/latest", endpoint="coop_latest_incoming_evaluation")
    def coop_latest_incoming_evaluation():
        if (session.get("user_tipo") or "").strip().lower() != "cooperado":
            return jsonify(ok=False, latest_id=0), 403

        coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first()
        if not coop:
            return jsonify(ok=False, latest_id=0), 404

        try:
            row = db.session.execute(
                sa_text(
                    "SELECT id, estrelas_geral, criado_em "
                    "FROM avaliacoes WHERE cooperado_id=:coop_id "
                    "ORDER BY id DESC LIMIT 1"
                ),
                {"coop_id": coop.id},
            ).mappings().first()
        except Exception:
            db.session.rollback()
            app.logger.exception("Falha ao consultar a última avaliação do cooperado")
            return jsonify(ok=False, cooperado_id=coop.id, latest_id=0), 500

        return jsonify(
            ok=True,
            cooperado_id=coop.id,
            latest_id=int(row["id"] if row else 0),
            estrelas=float(row["estrelas_geral"] or 0) if row else 0,
            criado_em=(row["criado_em"].isoformat() if row and row["criado_em"] else None),
        )


def _rest_current():
    return Restaurante.query.filter_by(usuario_id=session.get("user_id")).first()


def _active_substitute_rows():
    return (
        db.session.query(Cooperado.id, Cooperado.nome)
        .join(Usuario, Cooperado.usuario_id == Usuario.id)
        .filter(Usuario.ativo.is_(True))
        .order_by(Cooperado.nome.asc())
        .all()
    )


def _scale_belongs_to_rest(scale, rest) -> bool:
    if not scale or not rest:
        return False
    if scale.restaurante_id == rest.id:
        return True
    contract = shifts.patch._norm(scale.contrato)
    rest_name = shifts.patch._norm(rest.nome)
    return bool(contract and rest_name and (contract == rest_name or rest_name in contract or contract in rest_name))


def _scale_coop(scale):
    if scale.cooperado_id:
        coop = Cooperado.query.get(scale.cooperado_id)
        if coop:
            return coop
    target = shifts.patch._norm(scale.cooperado_nome)
    if not target:
        return None
    for coop in Cooperado.query.order_by(Cooperado.nome.asc()).all():
        if shifts.patch._norm(coop.nome) == target:
            return coop
    return None


def _block_scale_for_coop(rest, scale, coop, reason: str):
    today = datetime.now(TZ).date()
    data_ref = shifts.exact_scale_date(scale, today)
    if not data_ref:
        raise ValueError("Não foi possível identificar a data desta escala.")

    start_time, end_time = shifts.patch.upgrade._times_from_text(scale.horario)
    start_time = shifts.patch.upgrade._norm_time(start_time) or ""
    end_time = shifts.patch.upgrade._norm_time(end_time) or ""

    item = (
        ProducaoCooperado.query.filter_by(escala_id=scale.id, cooperado_id=coop.id)
        .order_by(ProducaoCooperado.id.desc())
        .first()
    )
    if not item:
        item = ProducaoCooperado.query.filter_by(
            cooperado_id=coop.id,
            restaurante_id=rest.id,
            data=data_ref,
            hora_inicio=start_time,
            hora_fim=end_time,
        ).order_by(ProducaoCooperado.id.desc()).first()

    if item and (item.status == "aprovada" or item.lancamento_id):
        raise ValueError("Esta produção já foi lançada e não pode ser marcada como ausência.")

    old_status = item.status if item else None
    if not item:
        item = ProducaoCooperado(
            cooperado_id=coop.id,
            restaurante_id=rest.id,
            escala_id=scale.id,
            data=data_ref,
            hora_inicio=start_time,
            hora_fim=end_time,
            qtd_entregas=0,
            valor_unitario=0,
            valor_total=0,
        )
        db.session.add(item)
        db.session.flush()

    item.escala_id = scale.id
    item.status = "recusada"
    item.motivo_recusa = reason
    item.decidido_em = datetime.utcnow()
    item.atualizado_em = datetime.utcnow()
    try:
        shifts.patch.upgrade._history(
            item,
            old_status=old_status,
            new_status="recusada",
            reason=reason,
        )
    except Exception:
        app.logger.exception("Falha ao registrar histórico da pendência da escala %s", scale.id)
    return item


if "rest_pendencia_nao_compareceu" not in app.view_functions:
    @app.post(
        "/portal/restaurante/pendencia/<int:scale_id>/nao-compareceu",
        endpoint="rest_pendencia_nao_compareceu",
    )
    def rest_pendencia_nao_compareceu(scale_id: int):
        if (session.get("user_tipo") or "").strip().lower() != "restaurante":
            return redirect(url_for("login"))

        rest = _rest_current()
        scale = Escala.query.get(scale_id)
        if not rest or not scale or not _scale_belongs_to_rest(scale, rest):
            flash("Pendência não localizada para este estabelecimento.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))

        coop = _scale_coop(scale)
        if not coop:
            flash("Não foi possível identificar o cooperado desta escala.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))

        try:
            reason = f"{_NO_SHOW_PREFIX}{coop.nome}|Marcado pelo estabelecimento"
            _block_scale_for_coop(rest, scale, coop, reason)
            db.session.commit()
            flash(
                f"{coop.nome} foi marcado como não compareceu nesta escala. A produção ficou bloqueada para o cooperado.",
                "info",
            )
        except ValueError as exc:
            db.session.rollback()
            flash(str(exc), "warning")
        except Exception:
            db.session.rollback()
            app.logger.exception("Falha ao marcar não comparecimento da escala %s", scale_id)
            flash("Não foi possível marcar o não comparecimento.", "danger")

        return redirect(url_for("portal_restaurante", view="lancar"))


if "rest_pendencia_trocar_cooperado" not in app.view_functions:
    @app.post(
        "/portal/restaurante/pendencia/<int:scale_id>/trocar-cooperado",
        endpoint="rest_pendencia_trocar_cooperado",
    )
    def rest_pendencia_trocar_cooperado(scale_id: int):
        if (session.get("user_tipo") or "").strip().lower() != "restaurante":
            return redirect(url_for("login"))

        rest = _rest_current()
        scale = Escala.query.get(scale_id)
        if not rest or not scale or not _scale_belongs_to_rest(scale, rest):
            flash("Pendência não localizada para este estabelecimento.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))

        original = _scale_coop(scale)
        new_id = request.form.get("cooperado_id", type=int)
        substitute = (
            Cooperado.query.join(Usuario, Cooperado.usuario_id == Usuario.id)
            .filter(Cooperado.id == new_id, Usuario.ativo.is_(True))
            .first()
            if new_id else None
        )
        if not original or not substitute:
            flash("Selecione um cooperado ativo para substituir.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))
        if substitute.id == original.id:
            flash("Escolha um cooperado diferente do cooperado original.", "warning")
            return redirect(url_for("portal_restaurante", view="lancar"))

        try:
            reason = f"{_SUBSTITUTE_PREFIX}{original.nome}|{substitute.nome}|Substituição informada pelo estabelecimento"
            _block_scale_for_coop(rest, scale, original, reason)
            scale.cooperado_id = substitute.id
            scale.cooperado_nome = substitute.nome
            db.session.commit()
            flash(
                f"A escala de {original.nome} foi transferida para {substitute.nome}. O lançamento desta pendência agora deve ser feito para o substituto.",
                "success",
            )
        except ValueError as exc:
            db.session.rollback()
            flash(str(exc), "warning")
        except Exception:
            db.session.rollback()
            app.logger.exception("Falha ao substituir cooperado da escala %s", scale_id)
            flash("Não foi possível trocar o cooperado desta pendência.", "danger")

        return redirect(url_for("portal_restaurante", view="lancar"))


def _week_pending_rows(rest):
    """Pendências vencidas da semana, separadas por escala/turno."""
    now = datetime.now(TZ)
    today = now.date()
    monday = today - timedelta(days=today.weekday())

    scales = [
        scale
        for scale in shifts.query_override._rest_scales_indexed(rest)
        if (lambda d: bool(d and monday <= d <= today))(shifts.exact_scale_date(scale, today))
    ]
    if not scales:
        return []

    coop_ids = {s.cooperado_id for s in scales if s.cooperado_id}
    coops_by_id = {
        c.id: c for c in Cooperado.query.filter(Cooperado.id.in_(coop_ids)).all()
    } if coop_ids else {}

    need_name = any(not s.cooperado_id and s.cooperado_nome for s in scales)
    coops_by_name = {}
    if need_name:
        for coop in Cooperado.query.order_by(Cooperado.nome.asc()).all():
            key = shifts.patch._norm(coop.nome)
            if key:
                coops_by_name[key] = coop

    productions = (
        ProducaoCooperado.query.filter(
            ProducaoCooperado.restaurante_id == rest.id,
            ProducaoCooperado.data >= monday,
            ProducaoCooperado.data <= today,
        )
        .order_by(ProducaoCooperado.id.desc())
        .all()
    )
    by_scale_coop = {}
    by_slot = {}
    for production in productions:
        if production.escala_id:
            by_scale_coop.setdefault((production.escala_id, production.cooperado_id), production)
        slot = (
            production.cooperado_id,
            production.data,
            shifts.patch.upgrade._norm_time(production.hora_inicio),
            shifts.patch.upgrade._norm_time(production.hora_fim),
        )
        by_slot.setdefault(slot, production)

    launches = (
        Lancamento.query.filter(
            Lancamento.restaurante_id == rest.id,
            Lancamento.data >= monday,
            Lancamento.data <= today,
        )
        .order_by(Lancamento.id.desc())
        .all()
    )
    launches_by_day = {}
    for launch in launches:
        launches_by_day.setdefault((launch.cooperado_id, launch.data), []).append(launch)

    result = []
    for scale in scales:
        data_ref = shifts.exact_scale_date(scale, today)
        if not data_ref:
            continue

        coop = coops_by_id.get(scale.cooperado_id) if scale.cooperado_id else coops_by_name.get(shifts.patch._norm(scale.cooperado_nome))
        if not coop:
            continue

        start_time, end_time = shifts.patch.upgrade._times_from_text(scale.horario)
        end_at = shifts.flow._end_at(data_ref, start_time, end_time)

        if data_ref == today:
            if not end_at or now < end_at:
                continue

        launch = None
        for candidate in launches_by_day.get((coop.id, data_ref), []):
            if shifts.patch.upgrade._overlap(
                candidate.hora_inicio,
                candidate.hora_fim,
                start_time,
                end_time,
            ):
                launch = candidate
                break
        if launch:
            continue

        production = by_scale_coop.get((scale.id, coop.id)) or by_slot.get(
            (
                coop.id,
                data_ref,
                shifts.patch.upgrade._norm_time(start_time),
                shifts.patch.upgrade._norm_time(end_time),
            )
        )
        if production and production.status == "aprovada":
            continue
        if (
            production
            and production.status == "recusada"
            and str(production.motivo_recusa or "").startswith(_NO_SHOW_PREFIX)
        ):
            continue

        sent = bool(production and production.status == "pendente" and float(production.valor_total or 0) > 0)
        result.append({
            "cooperado_id": coop.id,
            "cooperado_nome": coop.nome,
            "turno": scale.turno or "—",
            "horario": scale.horario or (f"{start_time} às {end_time}" if start_time and end_time else "—"),
            "contrato": scale.contrato or rest.nome,
            "data": data_ref.strftime("%d/%m/%Y"),
            "escala_id": scale.id,
            "aguardando_aprovacao": sent,
        })

    result.sort(key=lambda item: (item["data"], item["horario"], item["cooperado_nome"].lower()))
    return result


@app.context_processor
def _coopex_week_pending_context():
    context = {
        "coopex_rest_week_pending_rows": [],
        "coopex_rest_substitute_coops": [],
    }
    if (session.get("user_tipo") or "").strip().lower() != "restaurante":
        return context
    if request.endpoint != "portal_restaurante":
        return context
    if (request.args.get("view") or "lancar").strip().lower() != "lancar":
        return context

    try:
        rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first()
        if rest:
            context["coopex_rest_week_pending_rows"] = _week_pending_rows(rest)
            context["coopex_rest_substitute_coops"] = [
                {"id": int(coop_id), "nome": nome}
                for coop_id, nome in _active_substitute_rows()
            ]
    except Exception:
        db.session.rollback()
        app.logger.exception("Falha ao montar pendências semanais do estabelecimento")
    return context


def _install_coop_ui() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_coop_v4_ui", False):
        return

    original_get_source = loader.get_source

    css_tag = """<link rel="stylesheet" href="{{ url_for('static', filename='css/cooperado_v4.css', v='__BUILD__') }}">""".replace("__BUILD__", BUILD)
    js_tag = """<script src="{{ url_for('static', filename='js/cooperado_v4.js', v='__BUILD__') }}"></script>""".replace("__BUILD__", BUILD)

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)

        if template == "painel_cooperado.html":
            if "cooperado_v4.css" not in source:
                source = source.replace("</head>", "  " + css_tag + "\n</head>", 1)
            if "cooperado_v4.js" not in source:
                source = source.replace("</body>", js_tag + "\n</body>", 1)

            production_card_pattern = re.compile(
                r'(<section class="tab-pane-custom" id="tab-producoes">.*?'
                r'{%\s*for\s+lanc\s+in\s+producoes\s*%}\s*)'
                r'<div class="item-card">',
                re.S,
            )
            source = production_card_pattern.sub(
                r'\1<div class="item-card coopex-production-card">',
                source,
                count=1,
            )

            title = '<div class="item-title">{{ lanc.descricao }}</div>'
            state = title + """
                <div class="d-flex flex-wrap gap-1 align-items-center">
                  <span class="coopex-prod-state"><i class="bi bi-check-circle-fill"></i> Produção aprovada</span>
                  {% if lanc.minha_avaliacao is not none %}
                    <span class="coopex-prod-state"><i class="bi bi-star-fill"></i> Avaliada · {{ lanc.minha_avaliacao }}/5</span>
                  {% else %}
                    <span class="coopex-prod-state pending-rating"><i class="bi bi-star"></i> Avaliação pendente</span>
                  {% endif %}
                </div>"""
            source = source.replace(title, state, 1)

        elif template == "coop_producao.html":
            source = source.replace(
                '<form method="post" class="form">',
                '<form method="post" class="form" data-coopex-production-submit="1">',
                1,
            )
            if "cooperado_v4.js" not in source:
                source = source.replace("</body>", js_tag + "\n</body>", 1)

        elif template == "restaurante_dashboard.html":
            source = source.replace(
                "{% set _pendencias = pendencias_lancamento if pendencias_lancamento is defined and pendencias_lancamento is not none else [] %}",
                "{% set _pendencias = coopex_rest_week_pending_rows|default([]) %}",
                1,
            )
            source = source.replace(
                "Estes cooperados estão com horário encerrado e ainda não tiveram produção lançada no dia.",
                "Pendências vencidas da semana permanecem aqui até o estabelecimento lançar, marcar não comparecimento ou trocar o cooperado.",
                1,
            )
            source = source.replace(
                "Nenhum cooperado com horário encerrado está pendente de lançamento no momento.",
                "Nenhuma escala vencida da semana está pendente de lançamento ou aprovação.",
                1,
            )

            old_actions = '''<div class="d-flex align-items-center gap-2 flex-wrap">
                        <div class="pending-badge">
                          <i class="bi bi-clock-history"></i>
                          Pendente
                        </div>
                        <button type="button" class="btn btn-outline-royal btn-sm" onclick="selecionarCooperadoPendente('{{ p.cooperado_id }}', '{{ p.horario or '' }}')">
                          <i class="bi bi-cursor-fill"></i> Selecionar
                        </button>
                      </div>'''
            new_actions = '''<div class="d-flex align-items-center gap-1 flex-wrap coopex-pending-actions">
                        <div class="pending-badge">
                          <i class="bi bi-clock-history"></i>
                          Pendente
                        </div>
                        <button type="button" class="btn btn-outline-royal btn-sm" onclick="selecionarCooperadoPendente('{{ p.cooperado_id }}', '{{ p.horario or '' }}')">
                          <i class="bi bi-cursor-fill"></i> Selecionar
                        </button>
                        <form method="post" action="{{ url_for('rest_pendencia_nao_compareceu', scale_id=p.escala_id) }}" class="m-0" onsubmit="return confirm('Confirmar que {{ p.cooperado_nome|e }} não compareceu nesta escala?');">
                          <button type="submit" class="btn btn-outline-danger btn-sm">
                            <i class="bi bi-person-x"></i> Não compareceu
                          </button>
                        </form>
                        <button type="button" class="btn btn-outline-warning btn-sm coopex-btn-substituir"
                                data-action="{{ url_for('rest_pendencia_trocar_cooperado', scale_id=p.escala_id) }}"
                                data-original-id="{{ p.cooperado_id }}"
                                data-original-nome="{{ p.cooperado_nome|e }}">
                          <i class="bi bi-arrow-left-right"></i> Trocar cooperado
                        </button>
                      </div>'''
            if "coopex-btn-substituir" not in source:
                source = source.replace(old_actions, new_actions, 1)

            if "coopex-pending-actions" in source and "coopex-pending-actions .btn" not in source:
                source = source.replace(
                    "</style>",
                    """
    .coopex-pending-actions .btn{font-size:.72rem;padding:.25rem .48rem;white-space:nowrap;font-weight:700}
    .coopex-pending-actions form{display:inline-flex;margin:0}
    #coopexSubModal .modal-dialog{max-width:560px}
    #coopexSubModal .modal-title{font-weight:800;color:var(--royal)}
  </style>""",
                    1,
                )

            if "id=\"coopexSubModal\"" not in source:
                modal = '''
  <div class="modal fade" id="coopexSubModal" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog modal-dialog-centered">
      <div class="modal-content">
        <form id="coopexSubForm" method="post">
          <div class="modal-header">
            <h5 class="modal-title"><i class="bi bi-arrow-left-right"></i> Trocar cooperado</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Fechar"></button>
          </div>
          <div class="modal-body">
            <div class="mb-2 text-muted">Escala de <strong id="coopexSubOriginal"></strong></div>
            <label class="form-label" for="coopexSubSelect">Cooperado que trabalhou no lugar</label>
            <select class="form-select" id="coopexSubSelect" name="cooperado_id" required>
              <option value="">Selecione o cooperado ativo...</option>
              {% for c in coopex_rest_substitute_coops|default([]) %}
                <option value="{{ c.id }}">{{ c.nome }}</option>
              {% endfor %}
            </select>
            <div class="form-text mt-2">A data, o horário e o estabelecimento permanecem os mesmos. A produção passa a ser lançada para o substituto.</div>
          </div>
          <div class="modal-footer">
            <button type="button" class="btn btn-outline-secondary" data-bs-dismiss="modal">Cancelar</button>
            <button type="submit" class="btn btn-royal"><i class="bi bi-check2-circle"></i> Confirmar troca</button>
          </div>
        </form>
      </div>
    </div>
  </div>
  <script>
  document.addEventListener('DOMContentLoaded', function(){
    const modalEl=document.getElementById('coopexSubModal');
    const form=document.getElementById('coopexSubForm');
    const select=document.getElementById('coopexSubSelect');
    const original=document.getElementById('coopexSubOriginal');
    if(!modalEl||!form||!select)return;
    document.querySelectorAll('.coopex-btn-substituir').forEach(function(btn){
      btn.addEventListener('click',function(){
        form.action=btn.dataset.action||'';
        if(original)original.textContent=btn.dataset.originalNome||'';
        select.value='';
        Array.from(select.options).forEach(function(opt){
          opt.disabled=!!opt.value && String(opt.value)===String(btn.dataset.originalId||'');
        });
        bootstrap.Modal.getOrCreateInstance(modalEl).show();
      });
    });
  });
  </script>
'''
                source = source.replace("</body>", modal + "\n</body>", 1)

        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_coop_v4_ui = True
    app.jinja_env.cache.clear()


_install_coop_ui()
