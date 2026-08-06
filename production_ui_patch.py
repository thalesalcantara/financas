from __future__ import annotations

import production_scale_patch as production_patch

app = production_patch.app


def _install_coop_weekly_production_access() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_coop_production_link", False):
        return

    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "painel_cooperado.html":
            menu_marker = '<a href="{{ url_for(\'documentos_publicos\') }}" class="menu-link">'
            if 'data-week-production-menu="1"' not in source and menu_marker in source:
                menu_link = '''<a data-week-production-menu="1" href="{{ url_for('coop_producao') }}" class="menu-link"><i class="bi bi-clipboard2-check"></i><span>Produções da Semana</span></a>\n    '''
                source = source.replace(menu_marker, menu_link + menu_marker, 1)

            tabs_marker = '<section class="tab-shell no-print">'
            if 'data-week-production-card="1"' not in source and tabs_marker in source:
                quick_access = '''
  <section class="surface no-print" data-week-production-card="1">
    <a href="{{ url_for('coop_producao') }}" style="display:flex;align-items:center;justify-content:space-between;gap:12px;text-decoration:none;background:linear-gradient(135deg,#2747d9 0%,#3157ff 100%);color:#fff;border-radius:18px;padding:14px 15px;box-shadow:0 10px 24px rgba(39,71,217,.20)">
      <div style="display:flex;align-items:center;gap:12px;min-width:0">
        <span style="width:42px;height:42px;border-radius:14px;background:rgba(255,255,255,.16);display:grid;place-items:center;flex:0 0 42px"><i class="bi bi-clipboard2-check" style="font-size:1.2rem"></i></span>
        <span style="min-width:0"><strong style="display:block;font-size:.94rem">Produções da Semana</strong><small style="display:block;color:rgba(255,255,255,.82);margin-top:2px">Veja os plantões em R$ 0,00 e lance após o fim do horário</small></span>
      </div>
      <i class="bi bi-chevron-right" style="font-size:1.1rem"></i>
    </a>
  </section>

  '''
                source = source.replace(tabs_marker, quick_access + tabs_marker, 1)
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_coop_production_link = True
    app.jinja_env.cache.clear()


_install_coop_weekly_production_access()
