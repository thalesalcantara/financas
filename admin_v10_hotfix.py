from __future__ import annotations

from datetime import datetime

from sqlalchemy import event

import admin_v10_fix as v10

app = v10.app
db = v10.db
Usuario = v10.Usuario
legacy = v10.legacy

# O V10 converteu expressões monetárias para chamadas brl(...).
# Jinja precisa da função registrada como global além do filtro |brl.
app.jinja_env.filters["brl"] = v10.brl
app.jinja_env.globals["brl"] = v10.brl


class CooperadoInativoLockV11(db.Model):
    """Trava persistente para impedir sincronizações de reativarem cooperados."""

    __tablename__ = "cooperados_inativos_lock_v11"

    usuario_id = db.Column(
        db.Integer,
        db.ForeignKey("usuarios.id", ondelete="CASCADE"),
        primary_key=True,
    )
    criado_em = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    motivo = db.Column(db.String(120), nullable=False, default="desativado_admin")


_DURABLE_INACTIVE_USER_IDS: set[int] = set()


def _install_inactive_lock() -> None:
    global _DURABLE_INACTIVE_USER_IDS
    with app.app_context():
        CooperadoInativoLockV11.__table__.create(bind=db.engine, checkfirst=True)

        inactive_rows = (
            db.session.query(Usuario.id)
            .filter(Usuario.tipo == "cooperado", Usuario.ativo.is_(False))
            .all()
        )
        existing = {
            int(uid)
            for uid, in db.session.query(CooperadoInativoLockV11.usuario_id).all()
            if uid is not None
        }
        for uid, in inactive_rows:
            uid = int(uid)
            if uid not in existing:
                db.session.add(
                    CooperadoInativoLockV11(
                        usuario_id=uid,
                        motivo="desativado_antes_da_unificacao",
                    )
                )
                existing.add(uid)
        db.session.commit()
        _DURABLE_INACTIVE_USER_IDS = existing


try:
    _install_inactive_lock()
except Exception:
    app.logger.exception("Falha ao instalar trava persistente de cooperados inativos")


@event.listens_for(Usuario.ativo, "set", retval=True)
def _keep_durable_inactive(target, value, oldvalue, initiator):
    """Bloqueia reativação automática; ativação manual remove a trava antes."""
    try:
        if (
            value is True
            and target.id
            and str(getattr(target, "tipo", "") or "").strip().lower() == "cooperado"
            and int(target.id) in _DURABLE_INACTIVE_USER_IDS
        ):
            return False
    except Exception:
        pass
    return value


_original_set_coop_status = v10._set_coop_status


def _set_coop_status_persistent(coop, user, action):
    action = (action or "").strip().lower()
    uid = int(user.id)

    if action == "ativar":
        lock = db.session.get(CooperadoInativoLockV11, uid)
        if lock:
            db.session.delete(lock)
            db.session.flush()
        _DURABLE_INACTIVE_USER_IDS.discard(uid)
        v10._PROTECTED_INACTIVE_USER_IDS.discard(uid)
        user.ativo = True
        archive = db.session.get(v10.light.CooperadoArquivadoV8, coop.id)
        if archive:
            db.session.delete(archive)
        return f"{coop.nome} foi ativado manualmente."

    if action in {"desativar", "excluir"}:
        lock = db.session.get(CooperadoInativoLockV11, uid)
        if not lock:
            db.session.add(
                CooperadoInativoLockV11(
                    usuario_id=uid,
                    motivo="excluido_admin" if action == "excluir" else "desativado_admin",
                )
            )
        else:
            lock.motivo = "excluido_admin" if action == "excluir" else "desativado_admin"
        _DURABLE_INACTIVE_USER_IDS.add(uid)
        v10._PROTECTED_INACTIVE_USER_IDS.add(uid)
        user.ativo = False

        if action == "desativar":
            return f"{coop.nome} foi desativado e permanecerá desativado nas sincronizações."

        archive = db.session.get(v10.light.CooperadoArquivadoV8, coop.id)
        if not archive:
            archive = v10.light.CooperadoArquivadoV8(cooperado_id=coop.id)
            db.session.add(archive)
        archive.nome_original = coop.nome
        archive.telefone_original = coop.telefone
        archive.excluido_em = datetime.utcnow()
        return f"{coop.nome} foi excluído da operação. O histórico foi preservado."

    return _original_set_coop_status(coop, user, action)


v10._set_coop_status = _set_coop_status_persistent


# ---------------------------------------------------------------------------
# Distribuição da escala — caminho rápido
# ---------------------------------------------------------------------------
# O importador já carrega a lista de cooperados. Não fazemos nenhuma consulta
# adicional por linha da planilha. Apenas retiramos da lista os usuários que o
# administrador já desativou e deixamos o comparador legado resolver nomes
# abreviados dentro do nome completo (ex.: AZEVEDO -> FRANCISCO OLIVEIRA AZEVEDO).
_original_match_cooperado_by_name = getattr(legacy, "_match_cooperado_by_name", None)


def _match_cooperado_active_only(nome_planilha, cooperados):
    if _original_match_cooperado_by_name is None:
        return None

    ativos = [
        coop
        for coop in (cooperados or [])
        if getattr(coop, "usuario_id", None) is not None
        and int(coop.usuario_id) not in _DURABLE_INACTIVE_USER_IDS
    ]
    return _original_match_cooperado_by_name(nome_planilha, ativos)


if _original_match_cooperado_by_name is not None:
    legacy._match_cooperado_by_name = _match_cooperado_active_only


def _install_scale_xlsx_hotfix() -> None:
    """Restaura o upload XLSX da escala com o nome de campo legado."""
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_scale_xlsx_hotfix", False):
        return

    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "admin_light_v8.html":
            source = source.replace(
                'action="{{ url_for(\'upload_escala\') }}" enctype="multipart/form-data" class="alv8-filter"><div class="alv8-field grow"><label>Planilha XLSX</label><input class="alv8-input" type="file" name="arquivo" accept=".xlsx" required>',
                'action="{{ url_for(\'upload_escala\') }}" enctype="multipart/form-data" class="alv8-filter"><div class="alv8-field grow"><label>Planilha XLSX</label><input class="alv8-input" type="file" name="file" accept=".xlsx" required>',
                1,
            )
        return source, filename, uptodate

    loader.get_source = get_source
    loader._coopex_scale_xlsx_hotfix = True


_install_scale_xlsx_hotfix()
app.jinja_env.cache.clear()

if "upload_escala" not in app.view_functions:
    app.logger.error("Hotfix XLSX: endpoint upload_escala não está registrado.")
else:
    app.logger.info(
        "Hotfix XLSX rápido ativo: distribuição sem consultas adicionais por linha."
    )

app.logger.info(
    "Admin V10 hotfix leve carregado: escala distribuída somente entre cooperados ativos."
)
