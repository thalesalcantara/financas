from __future__ import annotations

import admin_launch_sync_v5 as sync

app = sync.app
BUILD = "20260807-1120"

# Todas as páginas administrativas que ainda eram páginas completas separadas.
ADMIN_FULL_TEMPLATES = {
    "admin_dashboard.html",
    "admin_avaliacoes.html",
    "admin_tabelas.html",
    "admin_documentos.html",
    "admin_avisos.html",
    "editar_tabelas.html",
    "editar_documentos.html",
    "admin_lancamentos.html",
    "admin_escalas (3).html",
}


def _install_admin_v6() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_admin_v7", False):
        return

    original_get_source = loader.get_source
    css_tag = "<link rel=\"stylesheet\" href=\"{{ url_for('static', filename='css/admin_v6.css', v='" + BUILD + "') }}\">"
    js_tag = "<script src=\"{{ url_for('static', filename='js/admin_v6.js', v='" + BUILD + "') }}\"></script>"

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template in ADMIN_FULL_TEMPLATES:
            if "css/admin_v6.css" not in source:
                source = source.replace("</head>", "  " + css_tag + "\n</head>", 1)
            if "js/admin_v6.js" not in source:
                source = source.replace("</body>", "  " + js_tag + "\n</body>", 1)
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_admin_v6 = True
    loader._coopex_admin_v7 = True
    app.jinja_env.cache.clear()


_install_admin_v6()
