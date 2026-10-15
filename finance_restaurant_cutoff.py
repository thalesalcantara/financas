"""Exclusão financeira por competência, sem perda do histórico."""
from __future__ import annotations

from datetime import date
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


def _month_of_receipt(receita):
    competencia = (getattr(receita, "competencia", "") or "").strip()
    if len(competencia) == 7 and competencia[4] == "-":
        return competencia
    data = getattr(receita, "data_vencimento", None) or getattr(receita, "data", None)
    return data.strftime("%Y-%m") if data else None


def _visible(receita):
    rest_id = getattr(receita, "restaurante_id", None)
    if rest_id is None:
        # Receita manual sem contrato identificado: não eliminar receita legítima.
        return True
    cutoff = _cutoffs().get(int(rest_id))
    if cutoff:
        competencia = _month_of_receipt(receita)
        return bool(competencia and competencia < cutoff)
    # Contratos desativados antes desta correção, ainda sem corte gravado:
    # preservar competências históricas, ocultar apenas o mês corrente e os próximos.
    rest = getattr(receita, "restaurante", None)
    if rest is not None and getattr(rest, "ativo", True) is False:
        competencia = _month_of_receipt(receita)
        return bool(competencia and competencia < date.today().strftime("%Y-%m"))
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

# A permissão original permanece intacta: chamamos o endpoint original,
# que aplica @admin_perm_required e confirma a transação.
_original_delete = app.view_functions.get("delete_restaurante")
if _original_delete:
    @wraps(_original_delete)
    def _delete_with_cutoff(id):
        # O decorator original valida a autorização quando acionado.
        # Inserir antes da chamada garante uma única transação com a inativação.
        from flask import session
        user_id = session.get("user_id")
        usuario = db.session.get(legacy.Usuario, user_id) if user_id else None
        # Não persistir alterações antes da autenticação/autorização do endpoint.
        # A autorização é revalidada pelo handler original; se negada, rollback.
        if usuario is not None:
            cutoff = date.today().strftime("%Y-%m")
            db.session.execute(text("""
                INSERT INTO finance_restaurant_cutoffs (restaurante_id, competencia)
                VALUES (:id, :competencia)
                ON CONFLICT (restaurante_id) DO NOTHING
            """), {"id": id, "competencia": cutoff})
        response = _original_delete(id)
        if usuario is None:
            db.session.rollback()
        return response

    app.view_functions["delete_restaurante"] = _delete_with_cutoff

app.logger.info("V31: receitas de estabelecimentos excluidos filtradas por competencia.")
