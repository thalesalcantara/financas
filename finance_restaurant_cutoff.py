"""Exclusão financeira por competência, sem perda do histórico."""
from __future__ import annotations

from datetime import datetime, date
from zoneinfo import ZoneInfo


def _today_local():
    return datetime.now(ZoneInfo('America/Fortaleza')).date()
from functools import wraps
from flask import g
from sqlalchemy import text
import app as legacy
import routes_admin_dashboard as dashboard

app = legacy.app
db = legacy.db

# Tabela auxiliar: não altera relacionamentos nem registros financeiros existentes.
with app.app_context():
    with db.engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS finance_restaurant_pauses (
                restaurante_id INTEGER PRIMARY KEY,
                inicio DATE NOT NULL,
                retomada DATE
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS finance_restaurant_cutoffs (
                restaurante_id INTEGER PRIMARY KEY,
                competencia VARCHAR(7) NOT NULL
            )
        """))


def _cutoffs():
    if not hasattr(g, "_finance_cutoffs_v31"):
        rows = db.session.execute(text(
            "SELECT restaurante_id, competencia FROM finance_restaurant_cutoffs"
        )).all()
        g._finance_cutoffs_v31 = {int(r[0]): str(r[1]) for r in rows}
    return g._finance_cutoffs_v31


def _pauses():
    if not hasattr(g, "_finance_pauses_v32"):
        rows = db.session.execute(text(
            "SELECT restaurante_id, inicio, retomada FROM finance_restaurant_pauses"
        )).all()
        def parse(v):
            return v if isinstance(v, date) else date.fromisoformat(str(v)[:10]) if v else None
        g._finance_pauses_v32 = {int(r[0]): (parse(r[1]), parse(r[2])) for r in rows}
    return g._finance_pauses_v32


def _paid_before_pause(receita, inicio):
    venc = getattr(receita, "data_vencimento", None) or getattr(receita, "data", None)
    pago_em = getattr(receita, "data_pagamento", None)
    return (
        (getattr(receita, "status_pagamento", "") or "").lower() == "pago"
        and venc is not None and venc < inicio
        and pago_em is not None and pago_em <= inicio
    )


def _month_of_receipt(receita):
    competencia = (getattr(receita, "competencia", "") or "").strip()
    if len(competencia) == 7 and competencia[4] == "-":
        return competencia
    data = getattr(receita, "data_vencimento", None) or getattr(receita, "data", None)
    return data.strftime("%Y-%m") if data else None


def _visible(receita):
    # Receitas automáticas sem contrato real e identificável são registros
    # órfãos; não devem aparecer como "Estabelecimento -" nem somar valores.
    if getattr(receita, "auto_taxa_adm", False):
        rest = getattr(receita, "restaurante", None)
        nome = (getattr(rest, "nome", "") or "").strip()
        descricao = (getattr(receita, "descricao", "") or "").strip()
        if not nome:
            # Histórico com nome recuperável da descrição permanece visível.
            prefixo = "Taxa administrativa - "
            extraido = descricao[len(prefixo):].rsplit(" - ", 1)[0].strip() if descricao.startswith(prefixo) else ""
            if not extraido or extraido in ("-", "Estabelecimento", "Sem estabelecimento"):
                return False
    rest_id = getattr(receita, "restaurante_id", None)
    if rest_id is None:
        # Receita manual sem contrato identificado: não eliminar receita legítima.
        return True
    pause = _pauses().get(int(rest_id))
    if pause:
        inicio, retomada = pause
        venc = getattr(receita, "data_vencimento", None) or getattr(receita, "data", None)
        if venc is not None and venc >= inicio and (retomada is None or venc < retomada):
            return False
        if venc is not None and venc < inicio and _month_of_receipt(receita) == inicio.strftime("%Y-%m"):
            return _paid_before_pause(receita, inicio)
        # Reativação não recupera cobranças do período suspenso.
    cutoff = _cutoffs().get(int(rest_id))
    if cutoff:
        competencia = _month_of_receipt(receita)
        return bool(competencia and competencia < cutoff)
    # Contratos desativados antes desta correção, ainda sem corte gravado:
    # preservar competências históricas, ocultar apenas o mês corrente e os próximos.
    rest = getattr(receita, "restaurante", None)
    if rest is not None and getattr(rest, "ativo", True) is False:
        competencia = _month_of_receipt(receita)
        mes_atual = _today_local().strftime("%Y-%m")
        if competencia == mes_atual:
            return _paid_before_pause(receita, _today_local())
        return bool(competencia and competencia < mes_atual)
    return True


def _filter(receitas):
    return [r for r in (receitas or []) if _visible(r)]


def _wrap_totals(original):
    @wraps(original)
    def wrapper(receita):
        return original(receita) if _visible(receita) else 0.0
    return wrapper


def _wrap_taxas(original):
    @wraps(original)
    def wrapper(receitas):
        return original(_filter(receitas))
    return wrapper


# Rotas extraídas para outro módulo têm referências próprias às funções financeiras.
# Atualizar ambos os módulos impede divergência entre lista, KPI e exportações.
for mod in (legacy, dashboard):
    fn = getattr(mod, "_receita_total_real", None)
    if fn and not getattr(fn, "_cutoff_v31", False):
        wrapped = _wrap_totals(fn)
        wrapped._cutoff_v31 = True
        setattr(mod, "_receita_total_real", wrapped)
    fn = getattr(mod, "_build_taxa_admin_rows", None)
    if fn and not getattr(fn, "_cutoff_v31", False):
        wrapped = _wrap_taxas(fn)
        wrapped._cutoff_v31 = True
        setattr(mod, "_build_taxa_admin_rows", wrapped)


def _wrap_partial(original):
    @wraps(original)
    def wrapper(partial_name, **context):
        if (partial_name or "").strip().lower() == "receitas":
            context["receitas"] = _filter(context.get("receitas"))
            context["total_receitas"] = sum(legacy._receita_total_real(r) for r in context["receitas"])
        return original(partial_name, **context)
    return wrapper


for mod in (legacy, dashboard):
    fn = getattr(mod, "_render_admin_dashboard_partial", None)
    if fn and not getattr(fn, "_cutoff_v31", False):
        wrapped = _wrap_partial(fn)
        wrapped._cutoff_v31 = True
        setattr(mod, "_render_admin_dashboard_partial", wrapped)


_original_render = dashboard.render_template


@wraps(_original_render)
def _render_template_with_cutoff(template_name, **context):
    if template_name == "admin_dashboard.html" and "receitas" in context:
        context["receitas"] = _filter(context["receitas"])
        context["total_receitas"] = sum(legacy._receita_total_real(r) for r in context["receitas"])
    return _original_render(template_name, **context)


dashboard.render_template = _render_template_with_cutoff

# Guardar a data da desativação sem alterar o cadastro e o histórico original.
_original_edit = app.view_functions.get("edit_restaurante")
if _original_edit:
    @legacy.admin_perm_required("restaurantes", "editar")
    @wraps(_original_edit)
    def _edit_with_pause(id):
        from flask import request
        restaurante = db.session.get(legacy.Restaurante, id)
        if restaurante is None:
            return _original_edit(id)
        old_active = getattr(restaurante, "ativo", True) is not False
        raw = (request.form.get("ativo") or "1").strip().lower()
        new_active = raw in ("1", "true", "ativo", "on", "sim")
        if old_active != new_active:
            hoje = _today_local()
            if not new_active:
                db.session.execute(text("""
                    INSERT INTO finance_restaurant_pauses (restaurante_id, inicio, retomada)
                    VALUES (:id, :hoje, NULL)
                    ON CONFLICT (restaurante_id) DO UPDATE SET
                    inicio = excluded.inicio, retomada = NULL
                """), {"id": id, "hoje": hoje})
            else:
                db.session.execute(text("""
                    UPDATE finance_restaurant_pauses
                    SET retomada = :hoje
                    WHERE restaurante_id = :id AND retomada IS NULL
                """), {"id": id, "hoje": hoje})
        return _original_edit(id)
    app.view_functions["edit_restaurante"] = _edit_with_pause


# A permissão original permanece intacta: chamamos o endpoint original,
# que aplica @admin_perm_required e confirma a transação.
_original_delete = app.view_functions.get("delete_restaurante")
if _original_delete:
    @legacy.admin_perm_required("restaurantes", "excluir")
    @wraps(_original_delete)
    def _delete_with_cutoff(id):
        # Confirma a competência no mesmo commit que desativa o estabelecimento.
        # A rotina original ainda faz a verificação de permissão e o commit.
        cutoff = _today_local().strftime("%Y-%m")
        db.session.execute(text("""
            INSERT INTO finance_restaurant_cutoffs (restaurante_id, competencia)
            VALUES (:id, :competencia)
            ON CONFLICT (restaurante_id) DO NOTHING
        """), {"id": id, "competencia": cutoff})
        return _original_delete(id)

    app.view_functions["delete_restaurante"] = _delete_with_cutoff

app.logger.info("V31: receitas de estabelecimentos excluidos filtradas por competencia.")
