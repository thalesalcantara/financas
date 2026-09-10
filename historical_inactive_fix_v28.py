"""Preserva cooperados inativos/excluídos no histórico do estabelecimento.

A regra operacional continua ocultando inativos em lançamentos novos, escalas e
outras telas correntes. Somente a aba Histórico de Produção do estabelecimento
(view=lancamentos) consulta o passado sem o filtro global de usuários ativos.
"""
from __future__ import annotations

from flask import has_request_context, request
from sqlalchemy import and_, event, func, or_
from sqlalchemy.orm import Session, with_loader_criteria

import operational_rules_v5 as rules


BUILD = "20260910-v28-historico-inativos"


def _is_restaurant_history() -> bool:
    if not has_request_context():
        return False
    if request.endpoint != "portal_restaurante":
        return False
    view = (request.args.get("view") or "lancar").strip().lower()
    return view == "lancamentos"


# O listener V5 aplica Usuario.ativo=True globalmente nas consultas operacionais.
# Isso é correto para operação atual, mas fazia lançamentos históricos sumirem
# quando o cooperado era posteriormente desativado/arquivado.
try:
    event.remove(Session, "do_orm_execute", rules._coopex_active_only_orm)
except Exception:
    # Se outro worker/import já tiver removido, a instalação abaixo continua
    # sendo idempotente pelo marcador em app.extensions.
    pass


if not rules.app.extensions.get("coopex_active_only_orm_v28"):
    @event.listens_for(Session, "do_orm_execute")
    def _coopex_active_only_orm_v28(execute_state):
        if not execute_state.is_select:
            return

        # Histórico do estabelecimento precisa representar o que aconteceu na
        # época do lançamento, independentemente do status atual do cooperado.
        if _is_restaurant_history():
            return

        if not rules._operational_filter_enabled():
            return

        normalized_scale_name = func.lower(
            func.replace(func.trim(rules.Escala.cooperado_nome), "_", " ")
        )
        scale_active = or_(
            rules.Escala.cooperado_id.in_(rules._active_coop_ids),
            and_(
                rules.Escala.cooperado_id.is_(None),
                rules.Escala.cooperado_nome.isnot(None),
                ~normalized_scale_name.in_(rules._inactive_names),
            ),
        )
        troca_active = and_(
            rules.TrocaSolicitacao.solicitante_id.in_(rules._active_coop_ids),
            rules.TrocaSolicitacao.destino_id.in_(rules._active_coop_ids),
        )

        execute_state.statement = execute_state.statement.options(
            with_loader_criteria(
                rules.Cooperado,
                rules.Cooperado.usuario_id.in_(rules._active_user_ids),
                include_aliases=True,
            ),
            with_loader_criteria(rules.Escala, scale_active, include_aliases=True),
            with_loader_criteria(
                rules.TrocaSolicitacao,
                troca_active,
                include_aliases=True,
            ),
        )

    rules.app.extensions["coopex_active_only_orm_v28"] = True

# Mantém o marcador antigo coerente: o filtro continua instalado, agora pela V28.
rules.app.extensions["coopex_active_only_orm_v5"] = True
rules.app.logger.info(
    "V28 carregada: histórico de produção do estabelecimento inclui cooperados inativos/excluídos."
)
