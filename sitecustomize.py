"""Hotfix de inicialização V27.

Carrega a correção segura antes do primeiro request. O gunicorn_conf será
normalizado em seguida; este arquivo é inofensivo quando não importado.
"""
