"""Inicialização segura dos complementos do COOPEX.

O serviço principal continua sendo app:app. Cada complemento é carregado
isoladamente: falha em um patch não impede os módulos seguintes de subir.
Isso evita 404/502 em cascata sem alterar os fluxos dos cooperados e dos
estabelecimentos.
"""
from __future__ import annotations

import importlib
import logging

log = logging.getLogger("gunicorn.error")
BUILD_VERSION = "2026-10-05-consolidacao-definitiva"


# Ordem preservada para não alterar regras de produção já em uso.
MODULES = (
    "production_scale_flow",
    "production_scale_backend",
    "production_ui",
    "performance_ui",
    "performance_queries",
    "production_shift_time",
    "cooperative_notifications_ui",
    "operational_rules",
    "historical_inactive",
    "approval_rejection",
    "approval_return_dashboard",
    # Admin consolidado: substitui V5/V6/V7/V8/V9/V10/V12/V13/V14.
    "admin_final",
    "finance_expense_control",
    "finance_expense_management",
    "finance_expense_totals",
    "finance_expense_recurring",
    "finance_expense_ui",
    "permission_readonly_guard",
    "finance_navigation",
    "finance_expense_delete",
)


def _load_module(name: str) -> bool:
    try:
        importlib.import_module(name)
        return True
    except Exception:
        log.exception("Complemento %s falhou; os demais continuarão carregando.", name)
        return False


def post_worker_init(worker):
    flask_app = None

    try:
        import coopex_upgrade as upgrade

        flask_app = upgrade.app
        callbacks = flask_app.after_request_funcs.get(None, [])
        flask_app.after_request_funcs[None] = [
            callback
            for callback in callbacks
            if getattr(callback, "__name__", "") != "coopex_upgrade_after_request"
        ]
    except Exception:
        # app:app já foi carregado pelo Gunicorn. Este fallback evita que uma
        # falha no upgrade impeça a inicialização dos demais complementos.
        log.exception("coopex_upgrade falhou; usando o app principal.")
        try:
            import app as legacy
            flask_app = legacy.app
        except Exception:
            log.exception("Não foi possível obter o app Flask principal.")
            return

    loaded = []
    failed = []
    for module_name in MODULES:
        if _load_module(module_name):
            loaded.append(module_name)
        else:
            failed.append(module_name)

    if "coopex_build_probe" not in flask_app.view_functions:
        from flask import jsonify

        @flask_app.get("/__coopex_build", endpoint="coopex_build_probe")
        def coopex_build_probe():
            return jsonify(
                ok=True,
                build=BUILD_VERSION,
                modules_loaded=len(loaded),
                modules_failed=failed,
            )

    if not getattr(flask_app, "_coopex_build_header_v30", False):
        @flask_app.after_request
        def coopex_build_header(response):
            response.headers["X-COOPEX-Build"] = BUILD_VERSION
            return response

        flask_app._coopex_build_header_v30 = True

    if failed:
        log.warning(
            "COOPEX iniciado com %s módulos; falharam: %s",
            len(loaded),
            ", ".join(failed),
        )
    else:
        log.info("COOPEX iniciado com %s módulos. Build %s", len(loaded), BUILD_VERSION)
