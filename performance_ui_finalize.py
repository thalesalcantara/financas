from __future__ import annotations

import re

import performance_ui_fix as perf

app = perf.app


def _install_final_ui_order() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_performance_final_ui", False):
        return

    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)

        if template == "painel_cooperado.html":
            include = "{% include '_coop_timeline.html' %}"
            source = source.replace(include, "")
            pattern = re.compile(
                r'(<section class="tab-pane-custom" id="tab-producoes">)(.*?)(</section>\s*<section class="tab-pane-custom")',
                re.S,
            )

            def place_after_history(match):
                return match.group(1) + match.group(2) + "\n      " + include + "\n    " + match.group(3)

            source, count = pattern.subn(place_after_history, source, count=1)
            if count == 0 and include not in source:
                marker = '<section class="tab-pane-custom" id="tab-producoes">'
                source = source.replace(marker, marker + "\n      " + include, 1)

        elif template == "restaurante_dashboard.html":
            source = source.replace(
                '<img id="selFoto" src="" class="big mb-2" alt="Foto do cooperado selecionado">',
                '<img id="selFoto" src="" class="big mb-2" alt="Foto do cooperado selecionado" onerror="if(!this.dataset.fallback){this.dataset.fallback=\'1\';this.src=\'{{ url_for(\'static\', filename=\'img/default.png\') }}\';}">',
                1,
            )
            modal = """
  <div class="modal fade coopex-photo-modal" id="coopexPhotoModal" tabindex="-1" aria-hidden="true">
    <div class="modal-dialog modal-dialog-centered modal-xl">
      <div class="modal-content">
        <div class="modal-body p-2 text-center position-relative">
          <button type="button" class="btn-close btn-close-white position-absolute top-0 end-0 m-3" data-bs-dismiss="modal" aria-label="Fechar"></button>
          <img id="coopexExpandedPhoto" src="" alt="Foto ampliada do cooperado" onerror="if(!this.dataset.fallback){this.dataset.fallback='1';this.src='{{ url_for('static', filename='img/default.png') }}';}">
        </div>
      </div>
    </div>
  </div>
"""
            if "coopexPhotoModal" not in source:
                source = source.replace("</body>", modal + "\n</body>", 1)

        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_performance_final_ui = True
    app.jinja_env.cache.clear()


_install_final_ui_order()
