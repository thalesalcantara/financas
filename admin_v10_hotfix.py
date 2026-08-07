from __future__ import annotations

import admin_v10_fix as v10

app = v10.app

# O V10 converteu expressões monetárias para chamadas brl(...).
# Jinja precisa da função registrada como global além do filtro |brl.
app.jinja_env.filters["brl"] = v10.brl
app.jinja_env.globals["brl"] = v10.brl
app.jinja_env.cache.clear()

app.logger.info("Admin V10 hotfix carregado: brl disponível como filtro e função Jinja.")
