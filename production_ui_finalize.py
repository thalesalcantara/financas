from __future__ import annotations

import production_ui_patch as ui

app = ui.app


def _install_pending_board_finalizer() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_pending_board_finalized", False):
        return

    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "restaurante_dashboard.html":
            marker = "        {% set _pendencias ="
            if "Valores enviados pelos cooperados" not in source and marker in source:
                source = source.replace(
                    marker,
                    ui._RESTAURANT_PENDING_BLOCK + marker,
                    1,
                )
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_pending_board_finalized = True
    app.jinja_env.cache.clear()


_install_pending_board_finalizer()
