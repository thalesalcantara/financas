from __future__ import annotations

from datetime import datetime

from flask import flash, redirect, request, url_for

import admin_light_v8 as light

app = light.app
db = light.db
Usuario = light.Usuario
Cooperado = light.Cooperado
BUILD = "20260807-1318"


def _weekday_num(value) -> int | None:
    try:
        if hasattr(value, "isoweekday"):
            return int(value.isoweekday())
    except Exception:
        pass
    text = str(value or "").strip().lower()
    names = {
        "segunda": 1, "seg": 1,
        "terça": 2, "terca": 2, "ter": 2,
        "quarta": 3, "qua": 3,
        "quinta": 4, "qui": 4,
        "sexta": 5, "sex": 5,
        "sábado": 6, "sabado": 6, "sáb": 6, "sab": 6,
        "domingo": 7, "dom": 7,
    }
    for key, num in names.items():
        if key in text:
            return num
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(text, fmt).date().isoweekday()
        except Exception:
            continue
    return None


def _weekday_label(value) -> str:
    labels = {
        1: "Segunda", 2: "Terça", 3: "Quarta", 4: "Quinta",
        5: "Sexta", 6: "Sábado", 7: "Domingo",
    }
    return labels.get(_weekday_num(value), str(value or "—"))


def _admin_light_coop_save_full(coop_id: int):
    """Salva o cadastro completo do cooperado sem perder campos de acesso."""
    denied = light._guard("cooperados")
    if denied:
        return denied

    coop = Cooperado.query.get_or_404(coop_id)
    user = Usuario.query.get_or_404(coop.usuario_id)

    nome = (request.form.get("nome") or coop.nome or "").strip()
    telefone = (request.form.get("telefone") or "").strip() or None
    usuario = (request.form.get("usuario") or user.usuario or "").strip()
    nova_senha = request.form.get("senha") or ""

    if not nome:
        flash("Informe o nome do cooperado.", "warning")
        return redirect(
            url_for(
                "admin_light_cooperatives",
                status=request.form.get("status") or "ativos",
            )
        )

    if usuario:
        duplicate = Usuario.query.filter(
            Usuario.usuario == usuario,
            Usuario.id != user.id,
        ).first()
        if duplicate:
            flash("Este usuário de acesso já está em uso.", "warning")
            return redirect(
                url_for(
                    "admin_light_cooperatives",
                    status=request.form.get("status") or "ativos",
                )
            )
        user.usuario = usuario

    coop.nome = nome
    user.nome = nome
    coop.telefone = telefone

    if nova_senha.strip():
        user.set_password(nova_senha)

    photo = request.files.get("foto")
    if photo and photo.filename:
        payload = photo.read(6 * 1024 * 1024 + 1)
        if len(payload) > 6 * 1024 * 1024:
            flash("A foto deve ter no máximo 6 MB.", "warning")
            return redirect(
                url_for(
                    "admin_light_cooperatives",
                    status=request.form.get("status") or "ativos",
                )
            )
        coop.foto_bytes = payload
        coop.foto_mime = photo.mimetype or "image/jpeg"
        coop.foto_filename = photo.filename[:255]
        coop.foto_url = None

    coop.ultima_atualizacao = datetime.utcnow()
    db.session.commit()

    flash(
        "Cadastro atualizado. Nome, telefone, usuário, foto e senha foram preservados no mesmo cadastro.",
        "success",
    )
    return redirect(
        url_for(
            "admin_light_cooperatives",
            status=request.form.get("status") or "ativos",
        )
    )


# V10/V11 assumem Lançamentos e Escala. V9 permanece apenas com o salvamento
# completo do cooperado e helpers de dia da semana ainda usados pelo editor.
app.view_functions["admin_light_coop_save"] = _admin_light_coop_save_full

app.logger.info(
    "Admin Preserve V9 reduzido: somente cadastro completo do cooperado e helpers de escala."
)
