from __future__ import annotations

import admin_v10_fix as v10

app = v10.app

# O V10 converteu expressões monetárias para chamadas brl(...).
# Jinja precisa da função registrada como global além do filtro |brl.
app.jinja_env.filters["brl"] = v10.brl
app.jinja_env.globals["brl"] = v10.brl


def _install_scale_xlsx_hotfix() -> None:
    """Restaura o upload XLSX da escala com o nome de campo legado.

    O fluxo original de importação/distribuição da escala está registrado no
    endpoint ``upload_escala`` e espera o arquivo no campo ``file``. O Admin
    V10/V11 injeta o formulário leve em tempo de execução, mas estava usando
    ``name=\"arquivo\"``; por isso o POST chegava ao endpoint sem o arquivo.

    Mantemos o endpoint e toda a regra antiga de leitura/distribuição intactos
    e corrigimos somente o formulário para voltar a enviar ``file`` como a
    tela antiga fazia.
    """
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_scale_xlsx_hotfix", False):
        return

    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "admin_light_v8.html":
            # Corrige somente o upload da escala injetado pelo Admin V10/V11.
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
    app.logger.info(
        "Hotfix XLSX ativo: formulário da escala voltou a enviar o campo 'file' para o fluxo legado de distribuição."
    )

app.logger.info("Admin V10 hotfix carregado: brl disponível como filtro e função Jinja.")
