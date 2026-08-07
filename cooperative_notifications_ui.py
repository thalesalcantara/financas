from __future__ import annotations

import re

from flask import jsonify, session
from sqlalchemy import text as sa_text

import menu_horizontal_enforcer as menu
import production_shift_time_fix as shifts

app = menu.app
db = shifts.patch.db
Cooperado = shifts.patch.Cooperado
BUILD = "20260807-0948"


if "coop_latest_incoming_evaluation" not in app.view_functions:
    @app.get("/api/coop/avaliacoes/latest", endpoint="coop_latest_incoming_evaluation")
    def coop_latest_incoming_evaluation():
        if (session.get("user_tipo") or "").strip().lower() != "cooperado":
            return jsonify(ok=False, latest_id=0), 403

        coop = Cooperado.query.filter_by(usuario_id=session.get("user_id")).first()
        if not coop:
            return jsonify(ok=False, latest_id=0), 404

        # `avaliacoes` é a avaliação recebida pelo cooperado (estabelecimento -> cooperado).
        # Retornamos somente o maior ID; o navegador grava esse ID e toca uma única vez.
        try:
            row = db.session.execute(
                sa_text(
                    "SELECT id, estrelas_geral, criado_em "
                    "FROM avaliacoes WHERE cooperado_id=:coop_id "
                    "ORDER BY id DESC LIMIT 1"
                ),
                {"coop_id": coop.id},
            ).mappings().first()
        except Exception:
            db.session.rollback()
            app.logger.exception("Falha ao consultar a última avaliação do cooperado")
            return jsonify(ok=False, cooperado_id=coop.id, latest_id=0), 500

        return jsonify(
            ok=True,
            cooperado_id=coop.id,
            latest_id=int(row["id"] if row else 0),
            estrelas=float(row["estrelas_geral"] or 0) if row else 0,
            criado_em=(row["criado_em"].isoformat() if row and row["criado_em"] else None),
        )


def _install_coop_ui() -> None:
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_coop_v4_ui", False):
        return

    original_get_source = loader.get_source

    css_tag = (
        '<link rel="stylesheet" href="{{ url_for(\'static\', '
        "filename='css/cooperado_v4.css', v='" + BUILD + "') }}">"
    )
    js_tag = (
        '<script src="{{ url_for(\'static\', '
        "filename='js/cooperado_v4.js', v='" + BUILD + "') }}"></script>"
    )

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)

        if template == "painel_cooperado.html":
            if "cooperado_v4.css" not in source:
                source = source.replace("</head>", "  " + css_tag + "\n</head>", 1)
            if "cooperado_v4.js" not in source:
                source = source.replace("</body>", js_tag + "\n</body>", 1)

            # Marca somente os cards do histórico de produções; os demais cards
            # do painel permanecem intactos.
            production_card_pattern = re.compile(
                r'(<section class="tab-pane-custom" id="tab-producoes">.*?'
                r'{%\s*for\s+lanc\s+in\s+producoes\s*%}\s*)'
                r'<div class="item-card">',
                re.S,
            )
            source = production_card_pattern.sub(
                r'\1<div class="item-card coopex-production-card">',
                source,
                count=1,
            )

            title = '<div class="item-title">{{ lanc.descricao }}</div>'
            state = title + """
                <div class="d-flex flex-wrap gap-1 align-items-center">
                  <span class="coopex-prod-state"><i class="bi bi-check-circle-fill"></i> Produção aprovada</span>
                  {% if lanc.minha_avaliacao is not none %}
                    <span class="coopex-prod-state"><i class="bi bi-star-fill"></i> Avaliada · {{ lanc.minha_avaliacao }}/5</span>
                  {% else %}
                    <span class="coopex-prod-state pending-rating"><i class="bi bi-star"></i> Avaliação pendente</span>
                  {% endif %}
                </div>"""
            source = source.replace(title, state, 1)

        elif template == "coop_producao.html":
            source = source.replace(
                '<form method="post" class="form">',
                '<form method="post" class="form" data-coopex-production-submit="1">',
                1,
            )
            if "cooperado_v4.js" not in source:
                source = source.replace("</body>", js_tag + "\n</body>", 1)

        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_coop_v4_ui = True
    app.jinja_env.cache.clear()


_install_coop_ui()
