from __future__ import annotations

from datetime import datetime
from functools import wraps

from flask import abort, flash, jsonify, redirect, request, session, url_for

import app as legacy
import admin_light_v8 as light

app = legacy.app
db = legacy.db
Usuario = legacy.Usuario
Cooperado = legacy.Cooperado
Escala = legacy.Escala
TrocaSolicitacao = legacy.TrocaSolicitacao
TrocaHistorico = getattr(legacy, "TrocaHistorico", None)
EscalaHistorico = getattr(legacy, "EscalaHistorico", None)


def _admin_allowed(aba: str, acao: str) -> bool:
    if (session.get("user_tipo") or "").strip().lower() != "admin":
        return False
    try:
        return bool(legacy.is_admin_master() or legacy.admin_has_perm(aba, acao))
    except Exception:
        return False


def _deny_admin(aba: str, acao: str):
    if not _admin_allowed(aba, acao):
        if request.path.startswith("/api/") or request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok": False, "message": f"Sem permissão para {acao} em {aba}."}), 403
        abort(403)
    return None


def _wrap_endpoint(endpoint: str, aba: str, acao):
    original = app.view_functions.get(endpoint)
    if not original or getattr(original, "_coopex_permission_v12", False):
        return

    @wraps(original)
    def wrapped(*args, **kwargs):
        action = acao(request) if callable(acao) else acao
        denied = _deny_admin(aba, action)
        if denied:
            return denied
        return original(*args, **kwargs)

    wrapped._coopex_permission_v12 = True
    app.view_functions[endpoint] = wrapped


def _method_action(_request):
    return "excluir" if request.method == "DELETE" else "editar"


def _coop_status_action(_request):
    return "excluir" if (request.form.get("acao") or "").strip().lower() == "excluir" else "editar"


# As APIs rápidas V11 antes verificavam somente se o usuário era admin.
_wrap_endpoint("api_admin_lancamento", "lancamentos", _method_action)
_wrap_endpoint("api_admin_escala", "escalas", _method_action)
_wrap_endpoint("api_admin_escalas_bulk", "escalas", "excluir")

# Rotas administrativas leves que antes conferiam apenas a permissão VER.
_wrap_endpoint("admin_light_scale_create", "escalas", "criar")
_wrap_endpoint("admin_light_coop_save", "cooperados", "editar")
_wrap_endpoint("admin_light_coop_status", "cooperados", _coop_status_action)
_wrap_endpoint("admin_v11_coop_status_batch", "cooperados", _coop_status_action)
_wrap_endpoint("admin_v10_blitz_save", "documentos", "editar")
_wrap_endpoint("admin_light_notice_toggle", "avisos", "editar")


@app.context_processor
def _permission_flags_v12():
    user = legacy._usuario_logado()
    master = bool(user and getattr(user, "is_master", False))
    perms = {}
    if user and (user.tipo or "").strip().lower() == "admin":
        perms = {aba: {acao: True for acao in ("ver", "criar", "editar", "excluir")} for aba in legacy.ADMIN_ABAS} if master else legacy.get_admin_permissions_map(user.id)

    def allowed(aba, acao):
        return master or bool(perms.get(aba, {}).get(acao))

    return {
        "admin_can_create_escalas": allowed("escalas", "criar"),
        "admin_can_edit_escalas": allowed("escalas", "editar"),
        "admin_can_delete_escalas": allowed("escalas", "excluir"),
        "admin_can_create_lancamentos": allowed("lancamentos", "criar"),
        "admin_can_edit_lancamentos": allowed("lancamentos", "editar"),
        "admin_can_delete_lancamentos": allowed("lancamentos", "excluir"),
        "admin_can_create_cooperados": allowed("cooperados", "criar"),
        "admin_can_edit_cooperados": allowed("cooperados", "editar"),
        "admin_can_delete_cooperados": allowed("cooperados", "excluir"),
    }


def _belongs_to(scale: Escala, coop: Cooperado) -> bool:
    if scale.cooperado_id:
        return int(scale.cooperado_id) == int(coop.id)
    return (scale.cooperado_nome or "").strip().casefold() == (coop.nome or "").strip().casefold()


def _write_scale_owner(scale: Escala, coop: Cooperado):
    scale.cooperado_id = coop.id
    scale.cooperado_nome = None


def _history_swap(troca, scale, saiu, entrou, tipo="troca"):
    if TrocaHistorico is not None:
        try:
            db.session.add(TrocaHistorico(
                troca_ref_id=troca.id,
                tipo=tipo,
                solicitante_id=troca.solicitante_id,
                solicitante_nome=getattr(saiu, "nome", None),
                destino_id=troca.destino_id,
                destino_nome=getattr(entrou, "nome", None),
                data=getattr(scale, "data", None),
                turno=getattr(scale, "turno", None),
                horario=getattr(scale, "horario", None),
                contrato=getattr(scale, "contrato", None),
                saiu_nome=getattr(saiu, "nome", None),
                entrou_nome=getattr(entrou, "nome", None),
                aplicada_em=datetime.utcnow(),
            ))
        except Exception:
            pass
    if EscalaHistorico is not None:
        try:
            db.session.add(EscalaHistorico(
                origem="troca_admin",
                acao="aprovar_troca",
                escala_ref_id=scale.id,
                troca_ref_id=troca.id,
                admin_usuario_id=session.get("user_id"),
                data=getattr(scale, "data", None),
                turno=getattr(scale, "turno", None),
                horario=getattr(scale, "horario", None),
                contrato=getattr(scale, "contrato", None),
                cooperado_id=getattr(entrou, "id", None),
                cooperado_nome=getattr(entrou, "nome", None),
                saiu_nome=getattr(saiu, "nome", None),
                entrou_nome=getattr(entrou, "nome", None),
                snapshot_em=datetime.utcnow(),
            ))
        except Exception:
            pass


def _redirect_coop_agenda():
    endpoint = "coop_agenda" if "coop_agenda" in app.view_functions else "portal_cooperado"
    return redirect(url_for(endpoint))


# A tela de agenda já envia para esta URL, mas a rota havia desaparecido.
@app.post("/escala/solicitar_troca", endpoint="coopex_solicitar_troca_v12")
def solicitar_troca_v12():
    if (session.get("user_tipo") or "").strip().lower() != "cooperado":
        return redirect(url_for("login"))
    coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    scale_id = request.form.get("from_escala_id", type=int)
    destino_id = request.form.get("to_cooperado_id", type=int)
    scale = db.session.get(Escala, scale_id) if scale_id else None
    destino = db.session.get(Cooperado, destino_id) if destino_id else None
    if not scale or not _belongs_to(scale, coop):
        flash("A escala selecionada não pertence ao seu cadastro.", "warning")
        return _redirect_coop_agenda()
    if not destino or destino.id == coop.id:
        flash("Selecione outro cooperado para receber a escala.", "warning")
        return _redirect_coop_agenda()
    destino_user = db.session.get(Usuario, destino.usuario_id) if destino.usuario_id else None
    if destino_user and destino_user.ativo is False:
        flash("O cooperado escolhido está inativo.", "warning")
        return _redirect_coop_agenda()
    duplicate = TrocaSolicitacao.query.filter_by(
        solicitante_id=coop.id,
        destino_id=destino.id,
        origem_escala_id=scale.id,
        status="pendente",
    ).first()
    if duplicate:
        flash("Essa solicitação de troca já está pendente.", "info")
        return _redirect_coop_agenda()
    item = TrocaSolicitacao(
        solicitante_id=coop.id,
        destino_id=destino.id,
        origem_escala_id=scale.id,
        mensagem=f"{coop.nome} solicitou passar a escala #{scale.id} para {destino.nome}.",
        status="pendente",
        criada_em=datetime.utcnow(),
    )
    db.session.add(item)
    db.session.commit()
    flash("Solicitação enviada. A administração deve aprovar ou recusar.", "success")
    return _redirect_coop_agenda()


def _load_pending_swap(troca_id: int):
    query = TrocaSolicitacao.query.filter_by(id=troca_id)
    try:
        if not legacy._is_sqlite():
            query = query.with_for_update()
    except Exception:
        pass
    return query.first()


@app.post("/admin/leve/trocas/<int:troca_id>/aprovar", endpoint="admin_aprovar_troca")
def admin_aprovar_troca_v12(troca_id: int):
    denied = _deny_admin("escalas", "editar")
    if denied:
        return denied
    troca = _load_pending_swap(troca_id)
    if not troca:
        abort(404)
    if (troca.status or "").strip().lower() != "pendente":
        flash("Esta solicitação já foi tratada.", "warning")
        return redirect(url_for("admin_light_swaps"))
    scale = db.session.get(Escala, troca.origem_escala_id)
    saiu = db.session.get(Cooperado, troca.solicitante_id)
    entrou = db.session.get(Cooperado, troca.destino_id)
    if not scale or not saiu or not entrou:
        flash("A troca não pode ser aplicada porque a escala ou um cooperado não foi localizado.", "danger")
        return redirect(url_for("admin_light_swaps"))
    if not _belongs_to(scale, saiu):
        flash("A escala já foi alterada desde a solicitação. Nada foi modificado.", "warning")
        return redirect(url_for("admin_light_swaps"))
    try:
        _write_scale_owner(scale, entrou)
        troca.status = "aprovada"
        troca.aplicada_em = datetime.utcnow()
        _history_swap(troca, scale, saiu, entrou)
        db.session.commit()
        flash(f"Troca aprovada: {saiu.nome} → {entrou.nome}.", "success")
    except Exception:
        db.session.rollback()
        app.logger.exception("Falha ao aprovar troca de escala")
        flash("Não foi possível aprovar a troca. Nenhuma alteração parcial foi mantida.", "danger")
    return redirect(url_for("admin_light_swaps"))


@app.post("/admin/leve/trocas/<int:troca_id>/recusar", endpoint="admin_recusar_troca")
def admin_recusar_troca_v12(troca_id: int):
    denied = _deny_admin("escalas", "editar")
    if denied:
        return denied
    troca = _load_pending_swap(troca_id)
    if not troca:
        abort(404)
    if (troca.status or "").strip().lower() != "pendente":
        flash("Esta solicitação já foi tratada.", "warning")
        return redirect(url_for("admin_light_swaps"))
    troca.status = "recusada"
    troca.aplicada_em = datetime.utcnow()
    db.session.commit()
    flash("Solicitação de troca recusada. A escala não foi alterada.", "info")
    return redirect(url_for("admin_light_swaps"))


# Esconde/neutraliza controles incompatíveis com as permissões no painel V11.
def _install_readonly_template_v12():
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_readonly_v12", False):
        return
    original = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original(environment, template)
        if template == "admin_light_v8.html":
            source = source.replace(
                '<details class="alv8-card" open><summary><strong><i class="bi bi-plus-circle"></i> Lançar Produção</strong></summary>',
                '{% if admin_can_create_lancamentos %}<details class="alv8-card" open><summary><strong><i class="bi bi-plus-circle"></i> Lançar Produção</strong></summary>',
                1,
            ).replace(
                '</form>\n    </details>\n\n    <div class="alv8-grid alv8-grid-5">',
                '</form>\n    </details>{% endif %}\n\n    <div class="alv8-grid alv8-grid-5">',
                1,
            )
            source = source.replace(
                '<td><div class="alv8-inline"><button class="alv8-btn edit js-edit-launch" type="button"><i class="bi bi-pencil"></i> Editar</button><button class="alv8-btn danger js-del-launch" type="button"><i class="bi bi-trash"></i> Excluir</button></div></td>',
                '<td><div class="alv8-inline">{% if admin_can_edit_lancamentos %}<button class="alv8-btn edit js-edit-launch" type="button"><i class="bi bi-pencil"></i> Editar</button>{% endif %}{% if admin_can_delete_lancamentos %}<button class="alv8-btn danger js-del-launch" type="button"><i class="bi bi-trash"></i> Excluir</button>{% endif %}{% if not admin_can_edit_lancamentos and not admin_can_delete_lancamentos %}<span class="alv8-badge">Somente leitura</span>{% endif %}</div></td>',
                1,
            )
            source = source.replace(
                '<details class="alv8-card"><summary><strong><i class="bi bi-person-plus"></i> Acrescentar alguém / nova linha na escala</strong></summary>',
                '{% if admin_can_create_escalas %}<details class="alv8-card"><summary><strong><i class="bi bi-person-plus"></i> Acrescentar alguém / nova linha na escala</strong></summary>',
                1,
            ).replace(
                '</form></details>\n\n    <div class="alv8-card"><div class="alv8-section-head"><div><h3>Escala semanal individual</h3>',
                '</form></details>{% endif %}\n\n    <div class="alv8-card"><div class="alv8-section-head"><div><h3>Escala semanal individual</h3>',
                1,
            )
            source = source.replace(
                '<td><div class="alv8-inline"><button type="button" class="alv8-btn primary js-save-scale">Salvar</button><button type="button" class="alv8-btn js-remove-scale">Retirar</button><button type="button" class="alv8-btn danger js-delete-scale">Excluir linha</button></div></td>',
                '<td><div class="alv8-inline">{% if admin_can_edit_escalas %}<button type="button" class="alv8-btn primary js-save-scale">Salvar</button><button type="button" class="alv8-btn js-remove-scale">Retirar</button>{% endif %}{% if admin_can_delete_escalas %}<button type="button" class="alv8-btn danger js-delete-scale">Excluir linha</button>{% endif %}{% if not admin_can_edit_escalas and not admin_can_delete_escalas %}<span class="alv8-badge">Somente leitura</span>{% endif %}</div></td>',
                1,
            )
            # Esses controles são montados por JavaScript; em somente leitura ficam visualmente bloqueados.
            source = source.replace(
                "const esc=s=>String(s??'').replace",
                "const canScaleEdit={{ admin_can_edit_escalas|tojson }},canScaleDelete={{ admin_can_delete_escalas|tojson }};const esc=s=>String(s??'').replace",
                1,
            )
            source = source.replace(
                "<select class=\"alv8-select js-contract\">",
                "<select class=\"alv8-select js-contract\" ${canScaleEdit?'':'disabled'}>",
                1,
            ).replace(
                "<select class=\"alv8-select js-coop\">",
                "<select class=\"alv8-select js-coop\" ${canScaleEdit?'':'disabled'}>",
                1,
            ).replace(
                "class=\"alv8-input js-free-name\" value=\"${esc(r.cooperado_nome_livre||'')}\" ${r.cooperado_id?'disabled':''}",
                "class=\"alv8-input js-free-name\" value=\"${esc(r.cooperado_nome_livre||'')}\" ${(!canScaleEdit||r.cooperado_id)?'disabled':''}",
                1,
            )
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_readonly_v12 = True
    app.jinja_env.cache.clear()


_install_readonly_template_v12()
app.logger.info("Correção V12 carregada: permissões por ação e aprovação/recusa de trocas.")
