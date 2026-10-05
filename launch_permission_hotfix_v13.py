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


# Corrige a interface V11: o formulário inteiro de "Lançar Produção" só é renderizado
# para quem possui a permissão Criar em Lançamentos (ou para o master).
def _install_launch_ui_v13():
    loader = app.jinja_loader
    if not loader or getattr(loader, "_launch_ui_v13", False):
        return
    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "admin_light_v8.html":
            opening = '<details class="alv8-card" open><summary><strong><i class="bi bi-plus-circle"></i> Lançar Produção</strong></summary>'
            if opening in source and "v13-launch-create" not in source:
                source = source.replace(
                    opening,
                    '{% if admin_can_create_lancamentos %}<!-- v13-launch-create -->' + opening,
                    1,
                )
                anchor = '<div class="alv8-grid alv8-grid-5">'
                start = source.find('<!-- v13-launch-create -->')
                end = source.find(anchor, start)
                if start >= 0 and end > start:
                    source = source[:end] + '{% endif %}\n    ' + source[end:]
        return source, filename, uptodate

    loader.get_source = get_source
    loader._launch_ui_v13 = True
    app.jinja_env.cache.clear()


_install_launch_ui_v13()
app.logger.info("V13: lançamento de produção bloqueado para perfil somente leitura.")
