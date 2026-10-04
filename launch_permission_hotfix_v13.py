from __future__ import annotations

from functools import wraps

from flask import abort, request, session

import app as legacy

app = legacy.app


def _can_create_launch() -> bool:
    if (session.get("user_tipo") or "").strip().lower() != "admin":
        return False
    try:
        return bool(legacy.is_admin_master() or legacy.admin_has_perm("lancamentos", "criar"))
    except Exception:
        return False


# Defesa no servidor: mesmo chamando a URL manualmente, perfil somente Ver não lança.
_original = app.view_functions.get("admin_add_lancamento")
if _original and not getattr(_original, "_launch_create_perm_v13", False):
    @wraps(_original)
    def _secured_admin_add_lancamento(*args, **kwargs):
        if not _can_create_launch():
            abort(403)
        return _original(*args, **kwargs)

    _secured_admin_add_lancamento._launch_create_perm_v13 = True
    app.view_functions["admin_add_lancamento"] = _secured_admin_add_lancamento



# A visibilidade do formulário foi incorporada diretamente ao template.

app.logger.info("V13: lançamento de produção bloqueado para perfil somente leitura.")
