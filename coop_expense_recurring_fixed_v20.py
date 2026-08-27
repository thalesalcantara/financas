from __future__ import annotations

import app as legacy
import coop_expense_control_v16 as v16
import coop_expense_management_v17 as v18
import coop_expense_totals_v19 as v19

app = legacy.app
db = legacy.db

_original_ensure_recurring = v18._ensure_recurring


def _ensure_recurring_fixed_or_variable():
    """Somente despesas FIXAS ou VARIÁVEIS explicitamente marcadas como mensais podem gerar o próximo mês."""
    changed = False
    marked = v18.DespesaCoopRecorrenciaV17.query.filter_by(repete_mensalmente=True).all()
    for rec in marked:
        ctl = v16.DespesaCoopControleV16.query.filter_by(despesa_id=rec.despesa_id).first()
        tipo = (getattr(ctl, "tipo", None) or "").strip().lower() if ctl else ""
        if tipo not in {"fixa", "variavel"}:
            rec.repete_mensalmente = False
            changed = True
    if changed:
        db.session.commit()
    return _original_ensure_recurring()


v18._ensure_recurring = _ensure_recurring_fixed_or_variable

# Interface: deixa explícito que Fixa e Variável podem repetir; Não rateio não pode.
v19._TEMPLATE = v19._TEMPLATE.replace(
    'Repete todo mês</label>',
    'Repete todo mês <small>(fixa ou variável)</small></label>'
)

_STYLE = r'''
<style>
/* V21 - recorrência sem sobreposição no formulário */
.v18-month{
  display:flex !important;
  align-items:center !important;
  justify-content:flex-start !important;
  gap:8px !important;
  min-width:190px !important;
  width:auto !important;
  white-space:normal !important;
  line-height:1.25 !important;
  padding:7px 8px !important;
  margin:0 !important;
  box-sizing:border-box !important;
  position:relative !important;
  z-index:2 !important;
}
.v18-month input[type="checkbox"]{
  flex:0 0 20px !important;
  width:20px !important;
  height:20px !important;
  min-width:20px !important;
  margin:0 !important;
  position:static !important;
  transform:none !important;
}
.v18-month small{
  display:inline !important;
  font-size:10px !important;
  opacity:.72 !important;
  white-space:nowrap !important;
}
.v18-edit-grid .v18-month{
  grid-column:1 / -1 !important;
  width:100% !important;
  min-height:38px !important;
  border:1px solid #dfe5ef !important;
  border-radius:8px !important;
  background:#f8fafc !important;
}
.v18-edit-grid{
  align-items:end !important;
}
@media(max-width:800px){
  .v18-month{width:100% !important;min-width:0 !important;}
  .v18-month small{white-space:normal !important;}
}
</style>
'''

# Desabilita/desmarca recorrência apenas quando a modalidade é "Não vai para o rateio".
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
        var podeRepetir = sel.value === 'fixa' || sel.value === 'variavel';
        chk.disabled = !podeRepetir;
        if(!podeRepetir) chk.checked = false;
        var label = chk.closest('label');
        if(label){
          label.style.opacity = podeRepetir ? '1' : '.55';
          label.title = podeRepetir ? 'Esta despesa pode ser repetida automaticamente todo mês.' : 'Despesas que não vão para o rateio não repetem automaticamente.';
        }
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

v19._TEMPLATE = v19._TEMPLATE.replace('</head>', _STYLE + '</head>')
v19._TEMPLATE = v19._TEMPLATE.replace('</body></html>', _SCRIPT + '</body></html>')

app.logger.info('V21: recorrência mensal permitida para despesas fixas e variáveis; checkbox reorganizado.')
