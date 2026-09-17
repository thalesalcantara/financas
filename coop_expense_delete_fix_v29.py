from __future__ import annotations

from flask import flash, redirect, url_for

import app as legacy
import coop_expense_control_v16 as v16
import coop_expense_management_v17 as v18

app = legacy.app
db = legacy.db
DespesaCooperativa = legacy.DespesaCooperativa


def delete_expense_fixed(item_id: int):
    """Exclui a despesa e impede que uma recorrência mensal a recrie.

    O V18 gera automaticamente o próximo lançamento enquanto existir qualquer
    item ativo da mesma série com ``repete_mensalmente=True``. Portanto, ao
    excluir um item recorrente, encerramos a recorrência da série preservando
    os lançamentos históricos e só então removemos o item selecionado.
    """
    v16._require("excluir")
    expense = DespesaCooperativa.query.get_or_404(item_id)

    try:
        serie = v18.DespesaCoopSerieV18.query.filter_by(despesa_id=item_id).first()
        recurring_series = bool(serie)

        if serie:
            related = v18.DespesaCoopSerieV18.query.filter_by(serie_key=serie.serie_key).all()
            related_ids = [row.despesa_id for row in related if row.despesa_id != item_id]

            if related_ids:
                recurrences = v18.DespesaCoopRecorrenciaV17.query.filter(
                    v18.DespesaCoopRecorrenciaV17.despesa_id.in_(related_ids)
                ).all()
                for recurrence in recurrences:
                    recurrence.repete_mensalmente = False

        v18.DespesaCoopSerieV18.query.filter_by(despesa_id=item_id).delete()
        v18.DespesaCoopRecorrenciaV17.query.filter_by(despesa_id=item_id).delete()
        v16.DespesaCoopControleV16.query.filter_by(despesa_id=item_id).delete()
        db.session.delete(expense)
        db.session.commit()

        if recurring_series:
            flash("Despesa excluída e recorrência mensal encerrada.", "success")
        else:
            flash("Despesa excluída.", "success")
    except Exception:
        db.session.rollback()
        app.logger.exception("V29 excluir despesa cooperativa")
        flash("Não foi possível excluir a despesa. Tente novamente.", "danger")

    return redirect(url_for("admin_v10_finance", tab="despesas"))


# Mantém a URL já usada pela tela e substitui somente a função responsável
# pela exclusão. Isso evita alterar o template e preserva compatibilidade.
app.view_functions["admin_v17_despesa_delete"] = delete_expense_fixed

app.logger.info("V29: exclusão de Despesas Coop corrigida para séries recorrentes.")
