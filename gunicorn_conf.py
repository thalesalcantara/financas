"""Inicialização segura e leve do Gunicorn.

O serviço continua usando app:app. Os módulos complementares são carregados
antes da primeira requisição. Se uma melhoria falhar, o aplicativo principal
permanece disponível para evitar ciclo de 502.
"""
from __future__ import annotations

import logging

log = logging.getLogger("gunicorn.error")
BUILD_VERSION = "2026-08-06.1305"


def post_worker_init(worker):
    try:
        import coopex_upgrade as upgrade

        flask_app = upgrade.app

        callbacks = flask_app.after_request_funcs.get(None, [])
        flask_app.after_request_funcs[None] = [
            callback
            for callback in callbacks
            if getattr(callback, "__name__", "") != "coopex_upgrade_after_request"
        ]

        import production_scale_flow  # noqa: F401
        import production_scale_patch  # noqa: F401
        import production_ui_patch  # noqa: F401
        import production_ui_finalize  # noqa: F401
        import performance_ui_fix  # noqa: F401
        import performance_ui_finalize  # noqa: F401

        if "coopex_build_probe" not in flask_app.view_functions:
            from flask import jsonify

            @flask_app.get("/__coopex_build", endpoint="coopex_build_probe")
            def coopex_build_probe():
                return jsonify(
                    ok=True,
                    build=BUILD_VERSION,
                    production_scale=True,
                    restaurant_horizontal_dashboard=True,
                    restaurant_welcome_name=True,
                    restaurant_approval_tab=True,
                    strong_approval_sound=True,
                    tables_horizontal_dashboard=True,
                    cooperative_daily_timeline=True,
                    cooperative_default_today=True,
                    cooperative_history_preserved=True,
                    cooperative_rating_preserved=True,
                    optimized_panel_queries=True,
                    media_route_photos=True,
                    enlarged_cooperative_photo=True,
                    full_history_table=True,
                    launch_preview_limit=2,
                    extra_production_tab=False,
                )

        @flask_app.after_request
        def coopex_build_header(response):
            response.headers["X-COOPEX-Build"] = BUILD_VERSION
            return response

        log.info("Painéis otimizados e conteúdo preservado. Build %s", BUILD_VERSION)
    except Exception:
        log.exception(
            "Melhorias complementares não carregaram; mantendo o aplicativo principal disponível."
        )
