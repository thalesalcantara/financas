from __future__ import annotations

from sqlalchemy import func

import performance_ui_hotfix as hotfix

perf = hotfix.perf
app = perf.app
Escala = perf.Escala


def _dedupe(rows):
    seen = set()
    result = []
    for row in sorted(rows, key=lambda item: item.id):
        if row.id not in seen:
            seen.add(row.id)
            result.append(row)
    return result


def _coop_scales_indexed(coop):
    rows = (
        Escala.query.filter(Escala.cooperado_id == coop.id)
        .order_by(Escala.id.desc())
        .limit(260)
        .all()
    )

    # Só procura registros legados por nome quando os vínculos por ID não
    # formam uma semana completa. A consulta principal continua indexada.
    if len(rows) < 7:
        variants = perf._raw_variants(coop.nome)
        if variants:
            rows.extend(
                Escala.query.filter(
                    Escala.cooperado_id.is_(None),
                    func.lower(func.trim(Escala.cooperado_nome)).in_(variants),
                )
                .order_by(Escala.id.desc())
                .limit(80)
                .all()
            )

        # Último fallback para acentos e grafias históricas. É limitado e só
        # executa quando ainda faltam registros.
        if len(rows) < 7:
            target = perf.backend._norm(coop.nome)
            candidates = (
                Escala.query.filter(
                    Escala.cooperado_id.is_(None),
                    Escala.cooperado_nome.isnot(None),
                )
                .order_by(Escala.id.desc())
                .limit(160)
                .all()
            )
            rows.extend(
                scale for scale in candidates
                if perf.backend._norm(scale.cooperado_nome) == target
            )

    return _dedupe(rows)


def _rest_scales_indexed(rest):
    rows = (
        Escala.query.filter(Escala.restaurante_id == rest.id)
        .order_by(Escala.id.desc())
        .limit(320)
        .all()
    )

    if len(rows) < 7:
        variants = perf._raw_variants(rest.nome)
        if variants:
            rows.extend(
                Escala.query.filter(
                    Escala.restaurante_id.is_(None),
                    func.lower(func.trim(Escala.contrato)).in_(variants),
                )
                .order_by(Escala.id.desc())
                .limit(80)
                .all()
            )

        if len(rows) < 7:
            target = perf.backend._norm(rest.nome)
            candidates = (
                Escala.query.filter(
                    Escala.restaurante_id.is_(None),
                    Escala.contrato.isnot(None),
                )
                .order_by(Escala.id.desc())
                .limit(180)
                .all()
            )
            rows.extend(
                scale for scale in candidates
                if perf.backend._norm(scale.contrato)
                and (
                    perf.backend._norm(scale.contrato) == target
                    or target in perf.backend._norm(scale.contrato)
                    or perf.backend._norm(scale.contrato) in target
                )
            )

    return _dedupe(rows)


# As funções de timeline resolvem esses nomes no módulo em tempo de execução.
perf._coop_scales_fast = _coop_scales_indexed
perf._rest_scales_fast = _rest_scales_indexed
perf.ui._coop_scales = _coop_scales_indexed
perf.ui._rest_scales = _rest_scales_indexed
perf.backend._coop_scales = _coop_scales_indexed
perf.backend._rest_scales = _rest_scales_indexed
