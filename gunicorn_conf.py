"""Inicialização segura e leve do Gunicorn.

O serviço continua usando app:app. Os módulos complementares são carregados
antes da primeira requisição. Se uma melhoria falhar, o aplicativo principal
permanece disponível para evitar ciclo de 502.
"""
from __future__ import annotations

import logging

log = logging.getLogger("gunicorn.error")
BUILD_VERSION = "2026-08-07.1104"


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
        import performance_ui_hotfix  # noqa: F401
        import performance_query_override  # noqa: F401
        import production_shift_time_fix  # noqa: F401
        import performance_ui_finalize  # noqa: F401
        import menu_horizontal_enforcer  # noqa: F401
        import cooperative_notifications_ui  # noqa: F401
        import operational_rules_v5  # noqa: F401
        import approval_rejection_v5  # noqa: F401
        import approval_return_dashboard_v5  # noqa: F401
        import admin_launch_sync_v5  # noqa: F401
        import admin_ui_v6  # noqa: F401

        if "coopex_build_probe" not in flask_app.view_functions:
            from flask import jsonify

            @flask_app.get("/__coopex_build", endpoint="coopex_build_probe")
            def coopex_build_probe():
                return jsonify(
                    ok=True,
                    build=BUILD_VERSION,
                    production_scale=True,
                    exact_scale_date=True,
                    exact_shift_end=True,
                    multiple_daily_shifts=True,
                    previous_day_submission=True,
                    restaurant_week_pending=True,
                    restaurant_pending_without_inner_scroll=True,
                    restaurant_current_shift_only=True,
                    restaurant_approval_tab=True,
                    restaurant_reject_and_lock=True,
                    rejected_coop_resubmit_blocked=True,
                    rejected_rest_manual_launch_allowed=True,
                    approved_value_creates_admin_launch=True,
                    approval_returns_main_dashboard=True,
                    approval_no_second_launch_required=True,
                    approval_named_confirmation=True,
                    admin_launch_live_sync=True,
                    admin_launch_partial_replacement_disabled=True,
                    admin_actions_persistent=True,
                    admin_horizontal_v6=True,
                    admin_grouped_dropdowns=True,
                    admin_lightweight_design=True,
                    normal_financial_deductions_for_approved_launch=True,
                    restaurant_alarm_every_5_minutes=True,
                    cooperative_submission_sound=True,
                    cooperative_new_rating_sound_once=True,
                    cooperative_light_production_cards=True,
                    cooperative_timeline_on_demand=True,
                    cooperative_rating_preserved=True,
                    inactive_cooperatives_operationally_hidden=True,
                    inactive_visible_only_in_admin_cooperatives=True,
                    admin_tab_prefetch_disabled=True,
                    restaurant_horizontal_dashboard=True,
                    horizontal_menu_enforced=True,
                    old_sidebar_disabled=True,
                    optimized_panel_queries=True,
                    indexed_scale_queries=True,
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

        log.info("Admin V6 horizontal e ações persistentes carregados. Build %s", BUILD_VERSION)
    except Exception:
        log.exception(
            "Melhorias complementares não carregaram; mantendo o aplicativo principal disponível."
        )
