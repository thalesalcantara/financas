from __future__ import annotations

from datetime import datetime
from functools import wraps

from flask import abort, flash, jsonify, redirect, request, session, url_for

import app as legacy

app = legacy.app
db = legacy.db
Usuario = legacy.Usuario
Cooperado = legacy.Cooperado
Escala = legacy.Escala
TrocaSolicitacao = legacy.TrocaSolicitacao


def _allowed(aba: str, acao: str) -> bool:
    if (session.get("user_tipo") or "").strip().lower() != "admin": return False
    try: return bool(legacy.is_admin_master() or legacy.admin_has_perm(aba, acao))
    except Exception: return False


def _deny(aba: str, acao: str):
    if _allowed(aba, acao): return None
    if request.path.startswith("/api/") or request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": False, "message": f"Sem permissão para {acao} em {aba}."}), 403
    abort(403)


def _wrap(endpoint: str, aba: str, action):
    original = app.view_functions.get(endpoint)
    if not original or getattr(original, "_perm_v12", False): return
    @wraps(original)
    def secured(*args, **kwargs):
        acao = action() if callable(action) else action
        denied = _deny(aba, acao)
        return denied if denied else original(*args, **kwargs)
    secured._perm_v12 = True
    app.view_functions[endpoint] = secured


def _edit_or_delete(): return "excluir" if request.method == "DELETE" else "editar"
def _coop_action(): return "excluir" if (request.form.get("acao") or "").strip().lower() == "excluir" else "editar"

# Segurança no servidor: não basta esconder os botões.
_wrap("api_admin_lancamento", "lancamentos", _edit_or_delete)
_wrap("api_admin_escala", "escalas", _edit_or_delete)
_wrap("api_admin_escalas_bulk", "escalas", "excluir")
_wrap("admin_light_scale_create", "escalas", "criar")
_wrap("admin_light_coop_save", "cooperados", "editar")
_wrap("admin_light_coop_status", "cooperados", _coop_action)
_wrap("admin_v11_coop_status_batch", "cooperados", _coop_action)
_wrap("admin_v10_blitz_save", "documentos", "editar")
_wrap("admin_light_notice_toggle", "avisos", "editar")


@app.context_processor
def permission_flags_v12():
    return {
        "admin_can_create_escalas": _allowed("escalas", "criar"),
        "admin_can_edit_escalas": _allowed("escalas", "editar"),
        "admin_can_delete_escalas": _allowed("escalas", "excluir"),
        "admin_can_create_lancamentos": _allowed("lancamentos", "criar"),
        "admin_can_edit_lancamentos": _allowed("lancamentos", "editar"),
        "admin_can_delete_lancamentos": _allowed("lancamentos", "excluir"),
    }


def _belongs(scale, coop):
    if scale.cooperado_id: return int(scale.cooperado_id) == int(coop.id)
    return (scale.cooperado_nome or "").strip().casefold() == (coop.nome or "").strip().casefold()


def _coop_redirect():
    return redirect(url_for("coop_agenda" if "coop_agenda" in app.view_functions else "portal_cooperado"))


# A agenda já envia para esta URL; a rota havia desaparecido.
@app.post("/escala/solicitar_troca", endpoint="solicitar_troca_v12")
def solicitar_troca_v12():
    if (session.get("user_tipo") or "").strip().lower() != "cooperado": return redirect(url_for("login"))
    coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    scale_id = request.form.get("from_escala_id", type=int)
    destino_id = request.form.get("to_cooperado_id", type=int)
    scale = db.session.get(Escala, scale_id) if scale_id else None
    destino = db.session.get(Cooperado, destino_id) if destino_id else None
    if not scale or not _belongs(scale, coop):
        flash("A escala selecionada não pertence ao seu cadastro.", "warning"); return _coop_redirect()
    if not destino or destino.id == coop.id:
        flash("Selecione outro cooperado para a troca.", "warning"); return _coop_redirect()
    destino_user = db.session.get(Usuario, destino.usuario_id) if destino.usuario_id else None
    if destino_user and destino_user.ativo is False:
        flash("O cooperado escolhido está inativo.", "warning"); return _coop_redirect()
    existing = TrocaSolicitacao.query.filter_by(solicitante_id=coop.id, destino_id=destino.id, origem_escala_id=scale.id, status="pendente").first()
    if existing:
        flash("Essa solicitação já está pendente.", "info"); return _coop_redirect()
    db.session.add(TrocaSolicitacao(
        solicitante_id=coop.id, destino_id=destino.id, origem_escala_id=scale.id,
        status="pendente", criada_em=datetime.utcnow(),
        mensagem=f"{coop.nome} solicitou troca da escala #{scale.id} com {destino.nome}."
    ))
    db.session.commit()
    flash("Solicitação enviada. Ela aparecerá em Operação → Trocas para aprovação ou recusa.", "success")
    return _coop_redirect()


# Os endpoints de aprovar/recusar já existem no app.py e executam a troca completa.
# Mantemos a lógica original e só retornamos para a tela leve de Trocas.
for _ep in ("admin_aprovar_troca", "admin_recusar_troca"):
    _original = app.view_functions.get(_ep)
    if _original and not getattr(_original, "_return_v12", False):
        def _make(original):
            @wraps(original)
            def wrapped(*args, **kwargs):
                result = original(*args, **kwargs)
                try:
                    if getattr(result, "status_code", None) in (301,302,303,307,308): result.headers["Location"] = url_for("admin_light_swaps")
                except Exception: pass
                return result
            wrapped._return_v12 = True
            return wrapped
        app.view_functions[_ep] = _make(_original)


def _install_template_v12():
    loader = app.jinja_loader
    if not loader or getattr(loader, "_readonly_v12", False): return
    original = loader.get_source
    def get_source(environment, template):
        source, filename, uptodate = original(environment, template)
        if template == "admin_light_v8.html":
            # Lançamentos: botões respeitam Editar e Excluir.
            source = source.replace(
                '<button class="alv8-btn edit js-edit-launch" type="button"><i class="bi bi-pencil"></i> Editar</button><button class="alv8-btn danger js-del-launch" type="button"><i class="bi bi-trash"></i> Excluir</button>',
                '{% if admin_can_edit_lancamentos %}<button class="alv8-btn edit js-edit-launch" type="button"><i class="bi bi-pencil"></i> Editar</button>{% endif %}{% if admin_can_delete_lancamentos %}<button class="alv8-btn danger js-del-launch" type="button"><i class="bi bi-trash"></i> Excluir</button>{% endif %}{% if not admin_can_edit_lancamentos and not admin_can_delete_lancamentos %}<span class="alv8-badge">Somente leitura</span>{% endif %}', 1)
            # Escala semanal é montada em JavaScript: desabilita edição e remove ações.
            source = source.replace("const esc=s=>String(s??'').replace", "const canScaleEdit={{ admin_can_edit_escalas|tojson }},canScaleDelete={{ admin_can_delete_escalas|tojson }};const esc=s=>String(s??'').replace", 1)
            source = source.replace('<select class="alv8-select js-contract">${optsContract(r.contrato||\'\')}</select>', '<select class="alv8-select js-contract" ${canScaleEdit?\'\':\'disabled\'}>${optsContract(r.contrato||\'\')}</select>', 1)
            source = source.replace('<select class="alv8-select js-coop">${optsCoop(r.cooperado_id)}</select>', '<select class="alv8-select js-coop" ${canScaleEdit?\'\':\'disabled\'}>${optsCoop(r.cooperado_id)}</select>', 1)
            source = source.replace(
                '<div class="alv8-inline"><button type="button" class="alv8-btn primary js-save-scale">Salvar</button><button type="button" class="alv8-btn js-remove-scale">Retirar</button><button type="button" class="alv8-btn danger js-delete-scale">Excluir linha</button></div>',
                '<div class="alv8-inline">${canScaleEdit?\'<button type="button" class="alv8-btn primary js-save-scale">Salvar</button><button type="button" class="alv8-btn js-remove-scale">Retirar</button>\':\'\'}${canScaleDelete?\'<button type="button" class="alv8-btn danger js-delete-scale">Excluir linha</button>\':\'\'}${(!canScaleEdit&&!canScaleDelete)?\'<span class="alv8-badge">Somente leitura</span>\':\'\'}</div>', 1)
        return source, filename, uptodate
    loader.get_source = get_source
    loader._readonly_v12 = True
    app.jinja_env.cache.clear()

_install_template_v12()
app.logger.info("V12: permissões por ação e solicitação de trocas restauradas.")
