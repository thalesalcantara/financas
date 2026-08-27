from __future__ import annotations

import app as legacy
import coop_expense_control_v16 as v16
import coop_expense_management_v17 as v18
import coop_expense_totals_v19 as v19

app = legacy.app
db = legacy.db

_original_ensure_recurring = v18._ensure_recurring


def _ensure_recurring_fixed_only():
    """Somente despesa FIXA explicitamente marcada como mensal pode gerar o próximo mês."""
    changed = False
    marked = v18.DespesaCoopRecorrenciaV17.query.filter_by(repete_mensalmente=True).all()
    for rec in marked:
        ctl = v16.DespesaCoopControleV16.query.filter_by(despesa_id=rec.despesa_id).first()
        if not ctl or (ctl.tipo or "").strip().lower() != "fixa":
            rec.repete_mensalmente = False
            changed = True
    if changed:
        db.session.commit()
    return _original_ensure_recurring()


v18._ensure_recurring = _ensure_recurring_fixed_only

# Interface: deixa explícito que a recorrência pertence somente às despesas fixas
v19._TEMPLATE = v19._TEMPLATE.replace(
    'Repete todo mês</label>',
    'Repete todo mês <small>(somente despesa fixa)</small></label>'
)

# Desabilita/desmarca recorrência quando a modalidade não é Fixa.
_SCRIPT = r'''
<script>
(function(){
  function bind(scope){
    (scope || document).querySelectorAll('select[name="tipo"]').forEach(function(sel){
      var form = sel.closest('form');
      if(!form) return;
      var chk = form.querySelector('input[name="repete_mensalmente"]');
      if(!chk) return;
      function sync(){
        var fixa = sel.value === 'fixa';
        chk.disabled = !fixa;
        if(!fixa) chk.checked = false;
        var label = chk.closest('label');
        if(label) label.style.opacity = fixa ? '1' : '.55';
      }
      sel.addEventListener('change', sync);
      sync();
    });
  }
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function(){bind(document)});
  else bind(document);
})();
</script>
'''

v19._TEMPLATE = v19._TEMPLATE.replace('</body></html>', _SCRIPT + '</body></html>')

app.logger.info('V20: recorrência mensal restrita a despesas fixas explicitamente marcadas.')
