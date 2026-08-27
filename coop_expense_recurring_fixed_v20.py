from __future__ import annotations

import app as legacy
import coop_expense_management_v17 as v18
import coop_expense_totals_v19 as v19

app = legacy.app

# Mantém a rotina original: qualquer modalidade explicitamente marcada como mensal
# pode gerar o lançamento do mês seguinte. A modalidade só interfere nos totais/rateio.

_STYLE = r'''
<style>
/* V22 - recorrência visível e clicável sem sobreposição */
.v18-month{
  display:flex !important;
  align-items:center !important;
  justify-content:flex-start !important;
  gap:10px !important;
  min-width:210px !important;
  width:auto !important;
  white-space:normal !important;
  line-height:1.25 !important;
  padding:8px 10px !important;
  margin:0 !important;
  box-sizing:border-box !important;
  position:relative !important;
  z-index:2 !important;
  cursor:pointer !important;
}
.v18-month input[type="checkbox"]{
  flex:0 0 22px !important;
  width:22px !important;
  height:22px !important;
  min-width:22px !important;
  margin:0 !important;
  position:static !important;
  transform:none !important;
  cursor:pointer !important;
}
.v18-edit-grid .v18-month{
  grid-column:1 / -1 !important;
  width:100% !important;
  min-height:42px !important;
  border:1px solid #dfe5ef !important;
  border-radius:8px !important;
  background:#f8fafc !important;
}
.v18-edit-grid{align-items:end !important;}
@media(max-width:800px){
  .v18-month{width:100% !important;min-width:0 !important;}
}
</style>
'''

# Remove textos antigos que restringiam a recorrência por modalidade.
v19._TEMPLATE = v19._TEMPLATE.replace(
    'Repete todo mês <small>(fixa ou variável)</small></label>',
    'Repete todo mês</label>'
)
v19._TEMPLATE = v19._TEMPLATE.replace(
    'Repete todo mês <small>(somente despesa fixa)</small></label>',
    'Repete todo mês</label>'
)

v19._TEMPLATE = v19._TEMPLATE.replace('</head>', _STYLE + '</head>')

app.logger.info('V22: recorrência mensal liberada para fixa, variável e não rateio; checkbox corrigido.')
