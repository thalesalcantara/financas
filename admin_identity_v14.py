from __future__ import annotations

from flask import session

import app as legacy

app = legacy.app
Usuario = legacy.Usuario


@app.context_processor
def admin_identity_v14():
    """Expõe a identidade real do administrador autenticado ao cabeçalho."""
    if (session.get("user_tipo") or "").strip().lower() != "admin":
        return {}

    user = None
    user_id = session.get("user_id")
    if user_id:
        try:
            user = Usuario.query.get(int(user_id))
        except Exception:
            user = None

    if not user:
        try:
            user = legacy._usuario_logado()
        except Exception:
            user = None

    nome = "Administrador"
    usuario = ""
    is_master = False
    if user:
        nome = (getattr(user, "nome", None) or getattr(user, "usuario", None) or "Administrador").strip()
        usuario = (getattr(user, "usuario", None) or "").strip()
        is_master = bool(getattr(user, "is_master", False))

    return {
        "admin_logged_name": nome,
        "admin_logged_username": usuario,
        "admin_logged_is_master": is_master,
    }


app.logger.info("V14: identidade do administrador disponível no cabeçalho.")
