from __future__ import annotations

from datetime import datetime

from sqlalchemy import event

import admin_v10_fix as v10

app = v10.app
db = v10.db
Usuario = v10.Usuario

# O V10 converteu expressões monetárias para chamadas brl(...).
# Jinja precisa da função registrada como global além do filtro |brl.
app.jinja_env.filters["brl"] = v10.brl
app.jinja_env.globals["brl"] = v10.brl


class CooperadoInativoLockV11(db.Model):
    """Trava persistente para impedir sincronizações de reativarem cooperados.

    A origem canônica continua sendo o Cadastro Principal. Quando um cooperado
    é desativado pelo administrador, gravamos a decisão nesta tabela. Rotinas de
    sincronização podem atualizar nome/login/senha, mas não podem mudar o status
    para ativo enquanto existir a trava. A trava só é removida por uma ativação
    manual no Cadastro Principal.
    """

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

        # Tudo que ainda estiver desativado no banco no momento do deploy passa
        # a ficar protegido de forma persistente para os próximos deploys/syncs.
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
    db.session.rollback()
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
        # Somente esta ação administrativa pode remover a trava.
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


# As rotas V11 consultam este nome global em tempo de execução; a substituição
# preserva as telas existentes e muda apenas a persistência do status.
v10._set_coop_status = _set_coop_status_persistent


def _install_scale_xlsx_hotfix() -> None:
    """Restaura o upload XLSX da escala com o nome de campo legado.

    O fluxo original de importação/distribuição da escala está registrado no
    endpoint ``upload_escala`` e espera o arquivo no campo ``file``. O Admin
    V10/V11 injeta o formulário leve em tempo de execução, mas estava usando
    ``name=\"arquivo\"``; por isso o POST chegava ao endpoint sem o arquivo.

    Mantemos o endpoint e toda a regra antiga de leitura/distribuição intactos
    e corrigimos somente o formulário para voltar a enviar ``file`` como a
    tela antiga fazia.
    """
    loader = app.jinja_loader
    if not loader or getattr(loader, "_coopex_scale_xlsx_hotfix", False):
        return

    original_get_source = loader.get_source

    def get_source(environment, template):
        source, filename, uptodate = original_get_source(environment, template)
        if template == "admin_light_v8.html":
            # Corrige somente o upload da escala injetado pelo Admin V10/V11.
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
        "Hotfix XLSX ativo: formulário da escala voltou a enviar o campo 'file' para o fluxo legado de distribuição."
    )

app.logger.info(
    "Admin V10 hotfix carregado: XLSX restaurado e cooperados inativos protegidos contra reativação automática."
)
