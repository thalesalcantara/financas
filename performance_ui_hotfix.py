from __future__ import annotations

import performance_ui_fix as perf

app = perf.app

# Compatibilidade entre os módulos novos e o modelo legado de avaliações.
# A função otimizada usa backend.legacy; o objeto correto fica em flow.legacy.
if not hasattr(perf.backend, "legacy"):
    perf.backend.legacy = perf.flow.legacy
