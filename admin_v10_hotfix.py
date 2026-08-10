from __future__ import annotations

import admin_v10_fix as v10

app = v10.app

# Mantém apenas o hotfix necessário para o envio XLSX da escala.
# A proteção de cooperados desativados já existe no admin_v10_fix.py;
# não repetimos consultas, listeners nem varreduras aqui.
app.jinja_env.filters["brl"] = v10.brl
app.jinja_env.globals["brl"] = v10.brl


def _install_scale_xlsx_hotfix() -> None:
    """Faz o formulário enviar o XLSX no campo legado `file`."""
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_scale_xlsx_hotfix", False):
        return

    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "admin_light_v8.html":
            source = source.replace(
                'action="{{ url_for(\'upload_escala\') }}" enctype="multipart/form-data" class="alv8-filter"><div class="alv8-field grow"><label>Planilha XLSX</label><input class="alv8-input" type="file" name="arquivo" accept=".xlsx" required>',
                'action="{{ url_for(\'upload_escala\') }}" enctype="multipart/form-data" class="alv8-filter"><div class="alv8-field grow"><label>Planilha XLSX</label><input class="alv8-input" type="file" name="file" accept=".xlsx" required>',
                1,
            )
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_scale_xlsx_hotfix = True


_install_scale_xlsx_hotfix()
app.jinja_env.cache.clear()

if "upload_escala" not in app.view_functions:
    app.logger.error("Hotfix XLSX: endpoint upload_escala não está registrado.")
else:
    app.logger.info("Hotfix XLSX mínimo ativo: sem consultas ou varreduras adicionais.")
