from __future__ import annotations

import re

import production_ui_patch as ui

app = ui.app


def _install_final_cleanup() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_ui_v3_final_cleanup", False):
        return

    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "restaurante_dashboard.html":
            source = re.sub(
                r'<a\s+data-coopex-producoes="1".*?</a>\s*',
                "",
                source,
                count=1,
                flags=re.S,
            )
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_ui_v3_final_cleanup = True
    app.jinja_env.cache.clear()


_install_final_cleanup()
