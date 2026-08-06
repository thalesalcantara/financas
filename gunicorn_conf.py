"""Inicialização segura e leve do Gunicorn.

O serviço continua usando app:app. As melhorias de produção são carregadas
após o worker iniciar. Se houver qualquer falha nesse módulo, o aplicativo
principal permanece disponível e não entra em ciclo de 502.
"""
from __future__ import annotations

import logging

log = logging.getLogger("gunicorn.error")


def post_worker_init(worker):
    try:
        import coopex_upgrade as upgrade

        flask_app = upgrade.app

        # A versão anterior recalculava ETag, lia o corpo inteiro e compactava
        # todas as respostas HTML/JSON. Isso consumia CPU em cada navegação.
        callbacks = flask_app.after_request_funcs.get(None, [])
        flask_app.after_request_funcs[None] = [
            callback
            for callback in callbacks
            if getattr(callback, "__name__", "") != "coopex_upgrade_after_request"
        ]

        from flask import request

        def _inject(response, marker: str, payload: str):
            if response.direct_passthrough or response.status_code != 200:
                return response
            if "text/html" not in response.headers.get("Content-Type", ""):
                return response
            try:
                html = response.get_data(as_text=True)
            except Exception:
                return response
            if marker in html or "</body>" not in html:
                return response
            response.set_data(html.replace("</body>", payload + "\n</body>", 1))
            response.headers["Content-Length"] = str(len(response.get_data()))
            return response

        @flask_app.after_request
        def production_shortcuts(response):
            path = request.path
            if path.startswith("/portal/cooperado") or path == "/painel/cooperado":
                return _inject(response, "coopex-upgrade-coop", upgrade.COOP_FLOATING)
            if path.startswith("/portal/restaurante"):
                return _inject(response, "coopex-upgrade-rest", upgrade.REST_FLOATING)
            return response

        log.info("Fluxo de produção carregado com inicialização segura.")
    except Exception:
        log.exception(
            "Fluxo de produção não carregou; mantendo o aplicativo principal disponível."
        )
