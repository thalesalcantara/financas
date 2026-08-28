"""Inicialização segura e leve do Gunicorn.

O serviço continua usando app:app. Os módulos complementares são carregados
antes da primeira requisição. Se uma melhoria falhar, o aplicativo principal
permanece disponível para evitar ciclo de 502.
"""
from __future__ import annotations

import logging

log = logging.getLogger("gunicorn.error")
BUILD_VERSION = "2026-08-27.2200-v26"


def post_worker_init(worker):
    try:
        import coopex_upgrade as upgrade
        flask_app = upgrade.app
        callbacks = flask_app.after_request_funcs.get(None, [])
        flask_app.after_request_funcs[None] = [callback for callback in callbacks if getattr(callback, "__name__", "") != "coopex_upgrade_after_request"]

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
        import admin_runtime_v7  # noqa: F401
        import admin_light_v8  # noqa: F401
        import admin_light_v8_bridge  # noqa: F401
        import admin_preserve_v9  # noqa: F401
        import admin_v10_fix  # noqa: F401
        import admin_v10_hotfix  # noqa: F401
        import permission_swap_fix_v12  # noqa: F401
        import launch_permission_hotfix_v13  # noqa: F401
        import admin_identity_v14  # noqa: F401
        import coop_expense_control_v16  # noqa: F401
        import coop_expense_management_v17  # noqa: F401
        import coop_expense_totals_v19  # noqa: F401
        import coop_expense_recurring_fixed_v20  # noqa: F401
        import coop_expense_responsive_v23  # noqa: F401
        import permission_readonly_guard_v24  # noqa: F401
        import inactive_contract_tax_guard_v26  # noqa: F401

        if "coopex_build_probe" not in flask_app.view_functions:
            from flask import jsonify
            @flask_app.get("/__coopex_build", endpoint="coopex_build_probe")
            def coopex_build_probe():
                return jsonify(ok=True, build=BUILD_VERSION, granular_permissions=True, swap_request_restored=True, swap_admin_actions=True, launch_create_permission=True, admin_identity=True, responsive_admin=True, coop_expense_control=True, expense_filter=True, expense_modes=True, expense_recurring_all_modes=True, expense_totals_exclude_no_rateio=True, expense_checkbox_layout=True, expense_table_responsive=True, readonly_guard_global=True, readonly_all_admin_tabs=True, coop_advance_permissions=True, inactive_contract_tax_guard=True)

        @flask_app.after_request
        def coopex_build_header(response):
            response.headers["X-COOPEX-Build"] = BUILD_VERSION
            return response

        log.info("Admin V26 carregado. Build %s", BUILD_VERSION)
    except Exception:
        log.exception("Melhorias complementares não carregaram; mantendo o aplicativo principal disponível.")
