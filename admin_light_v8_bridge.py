from __future__ import annotations

from flask import redirect, request, url_for

import admin_light_v8 as light

app = light.app
BUILD = "20260807-1318"


@app.before_request
def _admin_light_v8_redirects():
    if request.method != "GET" or (request.headers.get("X-Requested-With") or "").lower() == "xmlhttprequest":
        return None

    # legacy=1 significa: manter exatamente a função/modo antigo, alterando
    # somente o visual/menu pelo bridge. Nunca redireciona esse acesso.
    if request.args.get("legacy") == "1":
        return None

    path = request.path or ""
    endpoint = request.endpoint or ""
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
        }.get(tab)
        if target:
            values = {}
            for key in ("data_inicio", "data_fim", "q", "cooperado_id", "restaurante_id", "status"):
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
        return redirect(url_for(target))
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
        "editar_tabelas.html",
        "editar_documentos.html",
    }
    css_tag = "<link rel=\"stylesheet\" href=\"{{ url_for('static', filename='css/admin_light_v8.css', v='" + BUILD + "') }}\">"
    js_tag = "<script src=\"{{ url_for('static', filename='js/admin_light_v8_bridge.js', v='" + BUILD + "') }}\"></script>"
    flat_css = """
<style id="adminLightV8FlatOverride">
html,body{min-width:1180px!important}body{padding-top:68px!important;background:#f5f7ff!important}
.sidebar,.admin-v6-topbar,.admin-topbar{display:none!important}
.layout,.shell{display:block!important;min-height:0!important}
.main,.content{margin-left:0!important;width:100%!important;max-width:none!important;padding:10px 14px 18px!important}
.content-shell,.container-fluid,.main>.tab-content{max-width:none!important;width:100%!important}
.sidebar .brand,.surface-card,.admin-card,.card{backdrop-filter:none!important;-webkit-backdrop-filter:none!important}
.main .card,.main .surface-card,.content .card{box-shadow:0 2px 6px rgba(15,23,42,.035)!important;border-color:#e1e5f2!important}
</style>
"""

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template in old_admin_templates:
            if "admin_light_v8.css" not in source:
                source = source.replace("</head>", css_tag + "\n" + flat_css + "\n</head>", 1)
            elif "adminLightV8FlatOverride" not in source:
                source = source.replace("</head>", flat_css + "\n</head>", 1)
            if "admin_light_v8_bridge.js" not in source:
                source = source.replace("</body>", js_tag + "\n</body>", 1)
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_admin_light_v8_bridge = True
    app.jinja_env.cache.clear()


_install_bridge_templates()
app.logger.info("Bridge Admin V8 completo: funções antigas preservadas com o mesmo menu horizontal.")
