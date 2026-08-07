from __future__ import annotations

from flask import redirect, request, url_for

import admin_light_v8 as light

app = light.app
BUILD = light.BUILD


@app.before_request
def _admin_light_v8_redirects():
    if request.method != "GET" or (request.headers.get("X-Requested-With") or "").lower() == "xmlhttprequest":
        return None

    path = request.path or ""
    endpoint = request.endpoint or ""

    # Toda entrada principal do Admin deve cair no visual leve V8.
    if endpoint == "admin_dashboard" or path == "/admin":
        tab = (request.args.get("tab") or "").strip().lower()
        target = {
            "": "admin_light_summary",
            "resumo": "admin_light_summary",
            "lancamentos": "admin_light_launches",
            "escalas": "admin_light_scale",
            "cooperados": "admin_light_cooperatives",
            "avaliacoes": "admin_light_ratings",
            "documentos": "admin_light_documents",
            "tabelas": "admin_light_tables",
            "avisos": "admin_light_notices",
            "config": "admin_light_summary",
        }.get(tab)
        if target:
            values = {}
            for key in ("data_inicio", "data_fim", "q", "cooperado_id", "status"):
                value = request.args.get(key)
                if value not in (None, ""):
                    values[key] = value
            return redirect(url_for(target, **values))

    path_map = {
        "/admin/avaliacoes": "admin_light_ratings",
        "/admin/documentos": "admin_light_documents",
        "/admin/tabelas": "admin_light_tables",
        "/admin/avisos": "admin_light_notices",
        "/admin/rapido": "admin_light_launches",
    }
    target = path_map.get(path)
    if target:
        values = {}
        for key in ("data_inicio", "data_fim", "q", "cooperado_id", "status"):
            value = request.args.get(key)
            if value not in (None, ""):
                values[key] = value
        return redirect(url_for(target, **values))
    return None


def _install_bridge_templates():
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_admin_light_v8_bridge", False):
        return
    original_get_source = loader.get_source
    old_admin_templates = {
        "admin_dashboard.html",
        "admin_avaliacoes.html",
        "admin_documentos.html",
        "admin_tabelas.html",
        "admin_avisos.html",
        "admin_escalas (3).html",
        "admin_rapido.html",
        "admin_lancamentos.html",
    }
    js_tag = "<script src=\"{{ url_for('static', filename='js/admin_light_v8_bridge.js', v='" + BUILD + "') }}\"></script>"
    flat_css = """
<style id="adminLightV8FlatOverride">
.sidebar .brand,.surface-card,.admin-card,.card{backdrop-filter:none!important;-webkit-backdrop-filter:none!important}
.main .card,.main .surface-card{box-shadow:0 2px 8px rgba(16,24,40,.05)!important;border-color:#e5e9f2!important}
</style>
"""

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template in old_admin_templates:
            if "adminLightV8FlatOverride" not in source:
                source = source.replace("</head>", flat_css + "\n</head>", 1)
            if "admin_light_v8_bridge.js" not in source:
                source = source.replace("</body>", js_tag + "\n</body>", 1)
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_admin_light_v8_bridge = True
    app.jinja_env.cache.clear()


_install_bridge_templates()
app.logger.info("Bridge Admin V8 carregado: rotas antigas bloqueadas e visual leve obrigatório.")
