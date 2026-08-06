"""Configuração padrão descoberta automaticamente pelo Gunicorn.

Mantém compatibilidade com serviços Render configurados apenas com
`gunicorn app:app`, sem depender do comando salvo no render.yaml.
"""
from gunicorn_conf import *  # noqa: F401,F403
