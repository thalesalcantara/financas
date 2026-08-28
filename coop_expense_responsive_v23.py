from __future__ import annotations

import app as legacy
import coop_expense_totals_v19 as v19

app = legacy.app

_STYLE = r'''
<style>
/* V23 - tabela de despesas compacta no PC e legível no celular */
.v18-table-card{overflow:hidden !important;}
.v18-table-wrap{overflow-x:auto !important;-webkit-overflow-scrolling:touch !important;}
.v18-expense-table{width:100% !important;table-layout:auto !important;min-width:1040px !important;}
.v18-expense-table th,.v18-expense-table td{padding:8px 9px !important;vertical-align:middle !important;}
.v18-expense-table th{font-size:10px !important;white-space:nowrap !important;}
.v18-expense-table td{font-size:12px !important;}
.v18-expense-table th:nth-child(1),.v18-expense-table td:nth-child(1){width:92px !important;white-space:nowrap !important;}
.v18-expense-table th:nth-child(2),.v18-expense-table td:nth-child(2){width:auto !important;min-width:220px !important;}
.v18-expense-table th:nth-child(3),.v18-expense-table td:nth-child(3){width:110px !important;white-space:nowrap !important;}
.v18-expense-table th:nth-child(4),.v18-expense-table td:nth-child(4),
.v18-expense-table th:nth-child(5),.v18-expense-table td:nth-child(5){width:72px !important;text-align:center !important;white-space:nowrap !important;}
.v18-expense-table th:nth-child(6),.v18-expense-table td:nth-child(6){width:92px !important;text-align:center !important;white-space:nowrap !important;}
.v18-expense-table th:nth-child(7),.v18-expense-table td:nth-child(7){width:76px !important;text-align:center !important;white-space:nowrap !important;}
.v18-expense-table th:nth-child(8),.v18-expense-table td:nth-child(8){width:108px !important;white-space:nowrap !important;}
.v18-expense-table th:nth-child(9),.v18-expense-table td:nth-child(9){width:118px !important;white-space:nowrap !important;}
.v18-expense-table th:nth-child(10),.v18-expense-table td:nth-child(10){width:104px !important;min-width:104px !important;max-width:104px !important;white-space:nowrap !important;}
.v18-expense-table .v18-actions{display:flex !important;justify-content:flex-start !important;gap:4px !important;width:max-content !important;}
.v18-expense-table .v18-actions .alv8-btn{width:30px !important;height:30px !important;min-width:30px !important;min-height:30px !important;padding:0 !important;display:inline-flex !important;align-items:center !important;justify-content:center !important;}

@media(max-width:800px){
  .v18-table-card{padding:8px !important;}
  .v18-table-wrap{width:100% !important;max-width:100% !important;border:1px solid #e4e9f2 !important;border-radius:10px !important;}
  .v18-expense-table{min-width:900px !important;}
  .v18-expense-table th,.v18-expense-table td{padding:8px 7px !important;}
  .v18-expense-table th:nth-child(2),.v18-expense-table td:nth-child(2){min-width:175px !important;}
  .v18-expense-table th:nth-child(10),.v18-expense-table td:nth-child(10){position:sticky !important;right:0 !important;background:#fff !important;z-index:3 !important;box-shadow:-5px 0 8px rgba(15,23,42,.06) !important;}
  .v18-expense-table thead th:nth-child(10){background:#f7f9fc !important;z-index:4 !important;}
}
@media(max-width:480px){
  .v18-expense-table{min-width:850px !important;}
  .v18-expense-table th,.v18-expense-table td{font-size:11px !important;}
  .v18-expense-table th{font-size:9px !important;}
  .v18-expense-table .v18-actions .alv8-btn{width:32px !important;height:32px !important;min-width:32px !important;}
}
</style>
'''

# Marca apenas a tabela de Despesas Coop para não alterar as demais telas.
v19._TEMPLATE = v19._TEMPLATE.replace(
    '<div class="alv8-card"><div class="alv8-table-wrap"><table class="alv8-table">',
    '<div class="alv8-card v18-table-card"><div class="alv8-table-wrap v18-table-wrap"><table class="alv8-table v18-expense-table">'
)
v19._TEMPLATE = v19._TEMPLATE.replace('</head>', _STYLE + '</head>')

app.logger.info('V23: tabela Despesas Coop compactada no desktop e responsiva no celular.')
