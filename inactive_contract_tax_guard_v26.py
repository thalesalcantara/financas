from __future__ import annotations

import app as legacy

app = legacy.app


def _rest_is_active(rest) -> bool:
    """Considera o status efetivo do contrato/estabelecimento.

    O painel de estabelecimentos já usa usuario_ref.ativo quando disponível;
    portanto a taxa administrativa deve seguir exatamente a mesma regra.
    """
    if rest is None:
        return False
    try:
        user = getattr(rest, "usuario_ref", None)
        if user is not None and getattr(user, "ativo", None) is not None:
            return bool(user.ativo)
    except Exception:
        pass
    try:
        return bool(getattr(rest, "ativo", True))
    except Exception:
        return True


def _receita_taxa_ativa(receita) -> bool:
    if not getattr(receita, "auto_taxa_adm", False):
        return True
    return _rest_is_active(getattr(receita, "restaurante", None))


# 1) Não cria taxa nova para contrato inativo.
_original_ensure = getattr(legacy, "_ensure_taxas_admin_receitas", None)
if _original_ensure and not getattr(_original_ensure, "_inactive_guard_v26", False):
    def _ensure_taxas_admin_receitas_v26(restaurantes, months_back=0):
        ativos = [r for r in (restaurantes or []) if _rest_is_active(r)]
        return _original_ensure(ativos, months_back=months_back)
    _ensure_taxas_admin_receitas_v26._inactive_guard_v26 = True
    legacy._ensure_taxas_admin_receitas = _ensure_taxas_admin_receitas_v26


# 2) Taxas automáticas já existentes de contrato inativo não entram nos cards/totais.
_original_build = getattr(legacy, "_build_taxa_admin_rows", None)
if _original_build and not getattr(_original_build, "_inactive_guard_v26", False):
    def _build_taxa_admin_rows_v26(receitas):
        filtradas = [r for r in (receitas or []) if _receita_taxa_ativa(r)]
        return _original_build(filtradas)
    _build_taxa_admin_rows_v26._inactive_guard_v26 = True
    legacy._build_taxa_admin_rows = _build_taxa_admin_rows_v26


# 3) Também impede taxa de contrato inativo de contaminar o Total de Receitas Coop.
_original_total = getattr(legacy, "_receita_total_real", None)
if _original_total and not getattr(_original_total, "_inactive_guard_v26", False):
    def _receita_total_real_v26(receita):
        if getattr(receita, "auto_taxa_adm", False) and not _receita_taxa_ativa(receita):
            return 0.0
        return _original_total(receita)
    _receita_total_real_v26._inactive_guard_v26 = True
    legacy._receita_total_real = _receita_total_real_v26


# 4) Na tabela de Receitas Coop, não mostra a linha automática ligada a contrato inativo.
# Mantemos o registro no banco para histórico; apenas deixa de ser exibido/contabilizado enquanto inativo.
loader = app.jinja_loader
if loader and not getattr(loader, "_inactive_contract_tax_v26", False):
    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "admin_dashboard.html":
            # Loop da tabela principal de Receitas Coop.
            needle = "{% for r in receitas %}\n              {% set v_row = r.valor|default(r.valor_total, true)|default(0, true) %}"
            replacement = "{% for r in receitas %}\n              {% set _taxa_rest_ativa = (not r.auto_taxa_adm) or (r.restaurante and ((r.restaurante.usuario_ref.ativo if (r.restaurante.usuario_ref is defined and r.restaurante.usuario_ref and r.restaurante.usuario_ref.ativo is not none) else r.restaurante.ativo)|default(true))) %}\n              {% if _taxa_rest_ativa %}\n              {% set v_row = r.valor|default(r.valor_total, true)|default(0, true) %}"
            if needle in source:
                source = source.replace(needle, replacement, 1)
                # Fecha o IF imediatamente antes do endfor correspondente dessa tabela.
                row_end = "                </td>\n              </tr>\n            {% endfor %}"
                source = source.replace(row_end, "                </td>\n              </tr>\n              {% endif %}\n            {% endfor %}", 1)

            # Total defensivo do topo: ignora taxa automática de contrato inativo.
            total_needle = "{% for r in receitas or [] %}\n    {% set v_item = (r.valor_pago|default(0,true) + r.valor_multa|default(0,true) + r.valor_juros|default(0,true)) if r.auto_taxa_adm else r.valor|default(r.valor_total, true)|default(0, true) %}\n    {% set ns_total.v = ns_total.v + (v_item or 0) %}\n  {% endfor %}"
            total_repl = "{% for r in receitas or [] %}\n    {% set _taxa_rest_ativa = (not r.auto_taxa_adm) or (r.restaurante and ((r.restaurante.usuario_ref.ativo if (r.restaurante.usuario_ref is defined and r.restaurante.usuario_ref and r.restaurante.usuario_ref.ativo is not none) else r.restaurante.ativo)|default(true))) %}\n    {% if _taxa_rest_ativa %}\n    {% set v_item = (r.valor_pago|default(0,true) + r.valor_multa|default(0,true) + r.valor_juros|default(0,true)) if r.auto_taxa_adm else r.valor|default(r.valor_total, true)|default(0, true) %}\n    {% set ns_total.v = ns_total.v + (v_item or 0) %}\n    {% endif %}\n  {% endfor %}"
            if total_needle in source:
                source = source.replace(total_needle, total_repl, 1)
        return source, filename, uptodate

    loader.get_source = get_source
    loader._inactive_contract_tax_v26 = True
    app.jinja_env.cache.clear()


app.logger.info("V26: contratos inativos não geram, exibem nem somam taxa administrativa em Receitas Coop.")
