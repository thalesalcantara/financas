from __future__ import annotations

from flask import abort, jsonify, request, session

import app as legacy

app = legacy.app
AdminPermissao = legacy.AdminPermissao
Usuario = legacy.Usuario

# Endpoints que alteram Despesas Cooperados / Adiantamentos.
# A verificação é feita diretamente no banco para reforçar o decorator legado.
_EXACT = {
    "add_despesa_coop": ("coop_despesas", "criar"),
    "edit_despesa_coop": ("coop_despesas", "editar"),
    "delete_despesa_coop": ("coop_despesas", "excluir"),
    "bulk_delete_despesa_coop": ("coop_despesas", "excluir"),
    "delete_despesa_coop_bulk": ("coop_despesas", "excluir"),
    "add_abatimento_despesa_coop": ("coop_despesas", "editar"),
    "analisar_adiantamento_admin": ("coop_despesas", "editar"),
    "admin_config_adiantamentos": ("coop_despesas", "editar"),
}


def _admin_user():
    if (session.get("user_tipo") or "").strip().lower() != "admin":
        return None
    uid = session.get("user_id")
    if uid:
        try:
            return legacy.db.session.get(Usuario, int(uid))
        except Exception:
            pass
    try:
        return legacy._usuario_logado()
    except Exception:
        return None


def _allowed(aba: str, acao: str) -> bool:
    user = _admin_user()
    if not user:
        return False
    if bool(getattr(user, "is_master", False)):
        return True
    row = AdminPermissao.query.filter_by(usuario_id=user.id, aba=aba).first()
    if not row:
        return False
    attr = {
        "ver": "pode_ver",
        "criar": "pode_criar",
        "editar": "pode_editar",
        "excluir": "pode_excluir",
    }.get(acao)
    return bool(attr and getattr(row, attr, False))


def _deny(aba: str, acao: str):
    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.is_json or request.path.startswith("/api/"):
        return jsonify({
            "ok": False,
            "message": f"Acesso somente leitura. Sem permissão para {acao} em {aba}."
        }), 403
    abort(403)


def _infer_from_tab():
    """Reforço genérico: para POST/PUT/PATCH/DELETE em uma aba conhecida, exige a ação correta."""
    tab = (
        request.form.get("tab")
        or request.args.get("tab")
        or request.form.get("ajax_partial")
        or request.args.get("ajax_partial")
        or ""
    ).strip().lower()
    if tab not in getattr(legacy, "ADMIN_ABAS", {}):
        return None

    text = f"{request.endpoint or ''} {request.path}".lower()
    if any(k in text for k in ("delete", "excluir", "remove", "remover", "bulk-delete")):
        return tab, "excluir"
    if any(k in text for k in ("add", "create", "criar", "novo", "nova")):
        return tab, "criar"
    return tab, "editar"


@app.before_request
def permission_readonly_guard_v24():
    user = _admin_user()
    if not user or bool(getattr(user, "is_master", False)):
        return None

    endpoint = request.endpoint or ""
    rule = _EXACT.get(endpoint)
    if rule:
        aba, acao = rule
        if not _allowed(aba, acao):
            return _deny(aba, acao)
        return None

    # GET normalmente é leitura. Exceção: algumas rotas legadas de exclusão aceitam GET;
    # elas já estão cobertas na tabela _EXACT acima.
    if request.method in {"GET", "HEAD", "OPTIONS"}:
        return None

    inferred = _infer_from_tab()
    if inferred:
        aba, acao = inferred
        if not _allowed(aba, acao):
            return _deny(aba, acao)
    return None


@app.after_request
def permission_readonly_ui_v24(response):
    """Remove controles de mutação da parcial de Despesas Cooperados conforme as permissões."""
    try:
        user = _admin_user()
        if not user or bool(getattr(user, "is_master", False)):
            return response
        if "text/html" not in (response.content_type or ""):
            return response

        partial = (request.args.get("ajax_partial") or request.args.get("tab") or "").strip().lower()
        if partial != "coop_despesas":
            return response

        can_create = _allowed("coop_despesas", "criar")
        can_edit = _allowed("coop_despesas", "editar")
        can_delete = _allowed("coop_despesas", "excluir")
        if can_create and can_edit and can_delete:
            return response

        css = ["<style id='readonly-v24'>"]
        if not can_create:
            css.append("form[action*='/coop/despesas/add']{display:none!important}")
        if not can_edit:
            css.append("form[action*='/admin/adiantamentos/'][action*='/analisar'],form[action*='/admin/adiantamentos/config'],form[action*='/abatimentos/add'],button[data-url*='/coop/despesas/'][data-url*='/edit']{display:none!important}")
        if not can_delete:
            css.append("form[action*='/coop/despesas/'][action*='/delete'],form[action*='/coop/despesas/delete'],form[action*='bulk-delete']{display:none!important}")
        if not can_create and not can_edit and not can_delete:
            css.append("#coopDespesasAjaxWrap .js-adiantamento-admin-form,#coopDespesasAjaxWrap [data-perm-form]{display:none!important}")
        css.append("</style>")
        body = response.get_data(as_text=True)
        body = "".join(css) + body
        response.set_data(body)
        response.headers["Content-Length"] = str(len(response.get_data()))
    except Exception:
        app.logger.exception("V24: falha ao aplicar somente leitura na interface")
    return response


app.logger.info("V24: somente leitura reforçada no backend e interface, inclusive adiantamentos.")
