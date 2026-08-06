"""Inicialização segura e leve do Gunicorn.

O serviço continua usando app:app. Os módulos complementares são carregados
antes da primeira requisição. Se uma melhoria falhar, o aplicativo principal
permanece disponível para evitar ciclo de 502.
"""
from __future__ import annotations

import logging

log = logging.getLogger("gunicorn.error")


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

        # Fluxo por escala e correções finais de valor/painel.
        import production_scale_flow  # noqa: F401
        import production_scale_patch  # noqa: F401

        log.info("Fluxo de produção por escala e painel leve carregados.")
    except Exception:
        log.exception(
            "Melhorias complementares não carregaram; mantendo o aplicativo principal disponível."
        )
