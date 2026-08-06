from __future__ import annotations

import performance_ui_finalize as final_ui

app = final_ui.app
BUILD = "20260806-1336"


def _install() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_horizontal_menu", False):
        return

    original_get_source = loader.get_source
    critical_css = r'''
<style id="coopex-horizontal-menu-critical">
.shell{display:block!important;grid-template-columns:none!important;min-height:100vh!important}
.aside{position:sticky!important;top:0!important;left:auto!important;z-index:3000!important;width:100%!important;height:68px!important;min-height:68px!important;padding:0 14px!important;margin:0!important;display:flex!important;flex-direction:row!important;align-items:stretch!important;gap:10px!important;overflow:visible!important;background:linear-gradient(90deg,#062b80,#064fc8 58%,#063b9e)!important;color:#fff!important;box-shadow:0 4px 18px rgba(4,45,126,.20)!important}
.aside .brand.coopex-brand-welcome{width:238px!important;min-width:238px!important;height:68px!important;margin:0!important;padding:0!important;border:0!important;border-radius:0!important;background:transparent!important;box-shadow:none!important;display:flex!important;flex-direction:row!important;align-items:center!important;gap:10px!important}
.aside .coopex-brand-copy{display:flex!important;flex-direction:column!important;line-height:1.08!important;min-width:0!important}.aside .coopex-brand-copy small{display:block!important;font-size:.70rem!important;letter-spacing:.07em!important;font-weight:800!important;color:rgba(255,255,255,.80)!important}.aside .coopex-brand-copy strong{display:block!important;font-size:1rem!important;color:#fff!important;white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important;max-width:180px!important}
.aside .theme-toggle{display:none!important}.aside .navmenu{margin:0!important;padding:0!important;display:flex!important;flex:1 1 auto!important;min-width:0!important;height:68px!important;flex-direction:row!important;align-items:stretch!important;gap:0!important;overflow-x:auto!important;overflow-y:hidden!important;scrollbar-width:none!important}.aside .navmenu::-webkit-scrollbar{display:none!important}
.aside .navmenu a{position:relative!important;min-height:68px!important;height:68px!important;margin:0!important;padding:0 13px!important;border:0!important;border-radius:0!important;display:flex!important;flex-direction:row!important;align-items:center!important;justify-content:center!important;gap:7px!important;white-space:nowrap!important;font-size:.92rem!important;font-weight:750!important;color:#fff!important;opacity:1!important;transform:none!important;background:transparent!important}.aside .navmenu a span{display:inline!important}.aside .navmenu a:hover,.aside .navmenu a.active{background:rgba(255,255,255,.12)!important}.aside .navmenu a.active::before{content:""!important;position:absolute!important;left:11px!important;right:11px!important;top:auto!important;bottom:0!important;width:auto!important;height:4px!important;border-radius:4px 4px 0 0!important;background:#fff!important}
.content{margin:0 auto!important;width:100%!important;max-width:1720px!important;padding:8px 14px 14px!important}
@media(max-width:980px){.aside{height:auto!important;min-height:0!important;padding:7px 9px 0!important;flex-wrap:wrap!important;gap:3px!important}.aside .brand.coopex-brand-welcome{width:100%!important;min-width:0!important;height:44px!important}.aside .navmenu{width:100%!important;flex:0 0 100%!important;height:50px!important}.aside .navmenu a{height:50px!important;min-height:50px!important;padding:0 10px!important;font-size:.84rem!important}}
</style>
'''

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "restaurante_dashboard.html":
            source = source.replace(
                '<div class="brand"><i class="bi bi-building"></i><span>Portal do Estabelecimento</span></div>',
                '''<div class="brand coopex-brand-welcome"><i class="bi bi-shop"></i><span class="coopex-brand-copy"><small>SEJA BEM-VINDO</small><strong>{{ coopex_rest_display_name|default('ESTABELECIMENTO') }}</strong></span></div>''',
                1,
            )
            source = source.replace(
                '<link rel="stylesheet" href="{{ url_for(\'static\', filename=\'css/restaurante_v3.css\') }}">',
                '<link rel="stylesheet" href="{{ url_for(\'static\', filename=\'css/restaurante_v3.css\', v=\'' + BUILD + '\') }}">',
                1,
            )
            source = source.replace(
                '<script src="{{ url_for(\'static\', filename=\'js/restaurante_v3.js\') }}"></script>',
                '<script src="{{ url_for(\'static\', filename=\'js/restaurante_v3.js\', v=\'' + BUILD + '\') }}"></script>',
                1,
            )
            if "coopex-horizontal-menu-critical" not in source:
                source = source.replace("</head>", critical_css + "\n</head>", 1)
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_horizontal_menu = True
    app.jinja_env.cache.clear()


_install()
