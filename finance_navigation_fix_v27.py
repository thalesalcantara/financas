from __future__ import annotations

import app as legacy
import coop_expense_totals_v19 as expense_v19

app = legacy.app


def _rest_is_active(rest) -> bool:
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


def _receita_visivel(receita) -> bool:
    if not getattr(receita, "auto_taxa_adm", False):
        return True
    return _rest_is_active(getattr(receita, "restaurante", None))


# Regra financeira sem tocar no HTML/Jinja: contratos inativos não geram taxa.
_original_ensure = getattr(legacy, "_ensure_taxas_admin_receitas", None)
if _original_ensure and not getattr(_original_ensure, "_inactive_guard_v27", False):
    def _ensure_v27(restaurantes, months_back=0):
        ativos = [r for r in (restaurantes or []) if _rest_is_active(r)]
        return _original_ensure(ativos, months_back=months_back)
    _ensure_v27._inactive_guard_v27 = True
    legacy._ensure_taxas_admin_receitas = _ensure_v27

_original_build = getattr(legacy, "_build_taxa_admin_rows", None)
if _original_build and not getattr(_original_build, "_inactive_guard_v27", False):
    def _build_v27(receitas):
        return _original_build([r for r in (receitas or []) if _receita_visivel(r)])
    _build_v27._inactive_guard_v27 = True
    legacy._build_taxa_admin_rows = _build_v27

_original_total = getattr(legacy, "_receita_total_real", None)
if _original_total and not getattr(_original_total, "_inactive_guard_v27", False):
    def _total_v27(receita):
        if not _receita_visivel(receita):
            return 0.0
        return _original_total(receita)
    _total_v27._inactive_guard_v27 = True
    legacy._receita_total_real = _total_v27

# A parcial AJAX de Receitas recebe a lista já filtrada. Evita alterar o template
# dinamicamente, que foi a causa do HTTP 500 da V26.
_original_partial = getattr(legacy, "_render_admin_dashboard_partial", None)
if _original_partial and not getattr(_original_partial, "_inactive_guard_v27", False):
    def _partial_v27(partial_name: str, **context):
        if (partial_name or "").strip().lower() == "receitas" and "receitas" in context:
            context["receitas"] = [r for r in (context.get("receitas") or []) if _receita_visivel(r)]
        return _original_partial(partial_name, **context)
    _partial_v27._inactive_guard_v27 = True
    legacy._render_admin_dashboard_partial = _partial_v27

# Despesas Coop usa um HTML próprio. As versões V18/V19 retiraram o JavaScript
# que abre os menus Financeiro/Cadastros/Operação/Gestão. Recolocamos somente
# nessa tela, sem interferir nas demais páginas.
_MENU_JS = r'''<script id="expense-nav-v27">(function(){
if(window.__expenseNavV27)return;window.__expenseNavV27=true;
document.querySelectorAll('.alv8-group>button').forEach(function(btn){
  btn.addEventListener('click',function(e){
    e.stopPropagation();
    var g=btn.parentElement;
    document.querySelectorAll('.alv8-group').forEach(function(x){if(x!==g)x.classList.remove('open');});
    g.classList.toggle('open');
  });
});
document.addEventListener('click',function(){document.querySelectorAll('.alv8-group').forEach(function(x){x.classList.remove('open');});});
})();</script>'''
if 'expense-nav-v27' not in expense_v19._TEMPLATE:
    expense_v19._TEMPLATE = expense_v19._TEMPLATE.replace('</body>', _MENU_JS + '</body>')

app.logger.info('V27: Receitas Coop recuperada e navegação de Despesas Coop restaurada.')
