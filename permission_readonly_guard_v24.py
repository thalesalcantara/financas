from __future__ import annotations

from flask import abort, jsonify, request, session

import app as legacy

app = legacy.app
AdminPermissao = legacy.AdminPermissao
Usuario = legacy.Usuario

# Reforço explícito para endpoints sensíveis já conhecidos.
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

# Todas as abas administrativas que usam a matriz Ver/Criar/Editar/Excluir.
_AREAS = set(getattr(legacy, "ADMIN_ABAS", {}).keys())


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
            "message": f"Sem permissão para {acao}. Esta área está em modo somente leitura."
        }), 403
    abort(403)


def _explicit_area():
    tab = (
        request.form.get("tab")
        or request.args.get("tab")
        or request.form.get("ajax_partial")
        or request.args.get("ajax_partial")
        or ""
    ).strip().lower()
    return tab if tab in _AREAS else None


def _area_from_route():
    """Identifica a aba mesmo quando a rota antiga não envia o campo tab."""
    explicit = _explicit_area()
    if explicit:
        return explicit

    ep = (request.endpoint or "").lower()
    path = (request.path or "").lower()
    text = f"{ep} {path}"

    # Ordem importa: áreas de cooperados vêm antes de receitas/despesas gerais.
    if "/coop/despesas" in path or "/admin/adiantamentos" in path or "despesa_coop" in ep or "adiantamento_admin" in ep:
        return "coop_despesas"
    if "/coop/receitas" in path or "receita_coop" in ep:
        return "coop_receitas"
    if "benefic" in text:
        return "beneficios"
    if "lancamento" in text or "lancamentos" in text:
        return "lancamentos"
    if "/admin/leve/despesas-coop" in path or "admin_v16_despesa" in ep or "admin_v17_despesa" in ep:
        return "despesas"
    if "receita" in text:
        return "receitas"
    if "despesa" in text:
        return "despesas"
    if "cooperado" in text or "cooperados" in text:
        return "cooperados"
    if "restaurante" in text or "estabelecimento" in text:
        return "restaurantes"
    if "escala" in text or "troca" in text:
        return "escalas"
    if "document" in text or "blitz" in text:
        return "documentos"
    if "aviso" in text or "notice" in text:
        return "avisos"
    if "avali" in text or "rating" in text:
        return "avaliacoes"
    if "tabela" in text:
        return "tabelas"
    if "config" in text or "admin_perm" in text:
        return "config"
    return None


def _action_from_request():
    text = f"{request.endpoint or ''} {request.path or ''} {(request.form.get('acao') or '')}".lower()
    if request.method == "DELETE" or any(k in text for k in ("delete", "excluir", "remove", "remover", "bulk-delete", "bulk_delete")):
        return "excluir"
    if any(k in text for k in ("add", "create", "criar", "novo", "nova", "cadastrar", "importar")):
        return "criar"
    return "editar"


@app.before_request
def permission_readonly_guard_v24():
    """Backend: Ver nunca autoriza mutação em nenhuma aba administrativa."""
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

    if request.method in {"GET", "HEAD", "OPTIONS"}:
        return None

    aba = _area_from_route()
    if not aba:
        return None
    acao = _action_from_request()
    if not _allowed(aba, acao):
        return _deny(aba, acao)
    return None


def _readonly_areas():
    """Abas em que o usuário tem Ver, mas nenhuma permissão de mutação."""
    return [
        aba for aba in _AREAS
        if _allowed(aba, "ver")
        and not _allowed(aba, "criar")
        and not _allowed(aba, "editar")
        and not _allowed(aba, "excluir")
    ]


@app.after_request
def permission_readonly_ui_v24(response):
    """Interface: esconde controles de alteração em qualquer aba somente leitura."""
    try:
        user = _admin_user()
        if not user or bool(getattr(user, "is_master", False)):
            return response
        if "text/html" not in (response.content_type or ""):
            return response

        readonly = _readonly_areas()
        if not readonly:
            return response

        current = _area_from_route()
        body = response.get_data(as_text=True)

        # Para parciais AJAX de uma aba somente leitura, toda operação POST é ocultada.
        # Formulários GET (filtros/pesquisas) continuam funcionando normalmente.
        if current in readonly and (request.args.get("ajax_partial") or request.headers.get("X-Requested-With") == "XMLHttpRequest"):
            body = (
                "<style id='readonly-global-v25'>"
                "form[method='POST'],form[method='post'],"
                "button[data-url*='/edit'],button[data-url*='/delete'],"
                ".js-edit,.js-delete,.js-del,.js-save,.js-adiantamento-admin-form,"
                "[data-perm-form]{display:none!important}"
                "</style>"
                + body
            )
        else:
            # Página completa: aplica o bloqueio apenas dentro das seções das abas somente leitura.
            selectors = []
            for aba in readonly:
                selectors.extend([
                    f"#{aba} form[method='POST']",
                    f"#{aba} form[method='post']",
                    f"#{aba} button[data-url*='/edit']",
                    f"#{aba} button[data-url*='/delete']",
                    f"#{aba} .js-edit",
                    f"#{aba} .js-delete",
                    f"#{aba} .js-del",
                    f"#{aba} .js-save",
                    f"#{aba} .js-adiantamento-admin-form",
                    f"#{aba} [data-perm-form]",
                ])
            if selectors:
                body = "<style id='readonly-global-v25'>" + ",".join(selectors) + "{display:none!important}</style>" + body

        # Despesas Cooperados possui controles antigos fora do padrão; reforça especificamente.
        if "coop_despesas" in readonly:
            body = (
                "<style id='readonly-coop-desp-v25'>"
                "#coopDespesasAjaxWrap form[method='POST'],"
                "#coopDespesasAjaxWrap form[method='post'],"
                "#coopDespesasAjaxWrap button[data-url*='/edit'],"
                "#coopDespesasAjaxWrap button[data-url*='/delete'],"
                "#coopDespesasAjaxWrap .js-adiantamento-admin-form,"
                "#coopDespesasAjaxWrap [data-perm-form]{display:none!important}"
                "</style>"
                + body
            )

        response.set_data(body)
        response.headers["Content-Length"] = str(len(response.get_data()))
    except Exception:
        app.logger.exception("V25: falha ao aplicar somente leitura global na interface")
    return response


app.logger.info("V25: permissões Ver/Criar/Editar/Excluir reforçadas em todas as abas administrativas.")
