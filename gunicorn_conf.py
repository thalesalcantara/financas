"""Inicialização segura e leve do Gunicorn.

O serviço continua usando app:app. Os módulos complementares são carregados
antes da primeira requisição. Se uma melhoria falhar, o aplicativo principal
permanece disponível para evitar ciclo de 502.
"""
from __future__ import annotations

import logging

log = logging.getLogger("gunicorn.error")
BUILD_VERSION = "2026-08-06.1155"


def post_worker_init(worker):
    try:
        import coopex_upgrade as upgrade

        flask_app = upgrade.app

        # Remove o processamento antigo que relia, alterava e compactava todo
        # HTML/JSON depois de cada resposta. Isso pesava inclusive após o login.
        callbacks = flask_app.after_request_funcs.get(None, [])
        flask_app.after_request_funcs[None] = [
            callback
            for callback in callbacks
            if getattr(callback, "__name__", "") != "coopex_upgrade_after_request"
        ]

        # Fluxo por escala, regras de aprovação e atalhos de interface.
        import production_scale_flow  # noqa: F401
        import production_scale_patch  # noqa: F401
        import production_ui_patch  # noqa: F401

        if "coopex_build_probe" not in flask_app.view_functions:
            from flask import jsonify

            @flask_app.get("/__coopex_build", endpoint="coopex_build_probe")
            def coopex_build_probe():
                return jsonify(
                    ok=True,
                    build=BUILD_VERSION,
                    production_scale=True,
                    original_restaurant_dashboard=True,
                    cooperative_weekly_production=True,
                )

        @flask_app.after_request
        def coopex_build_header(response):
            response.headers["X-COOPEX-Build"] = BUILD_VERSION
            return response

        log.info("Fluxo semanal de produção carregado. Build %s", BUILD_VERSION)
    except Exception:
        log.exception(
            "Melhorias complementares não carregaram; mantendo o aplicativo principal disponível."
        )
