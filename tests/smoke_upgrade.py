from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

DB_PATH = Path("/tmp/coopex_upgrade_test.db")
try:
    DB_PATH.unlink()
except FileNotFoundError:
    pass

os.environ["DATABASE_URL"] = f"sqlite:///{DB_PATH}"
os.environ["FLASK_ENV"] = "development"
os.environ["FLASK_SECURE_COOKIES"] = "0"
os.environ["INIT_DB_ON_START"] = "0"
os.environ["SECRET_KEY"] = "upgrade-test"

from enhanced_app import app, db  # noqa: E402
import app as legacy  # noqa: E402
import coopex_upgrade as upgrade  # noqa: E402


def login(client, user_id: int, role: str) -> None:
    with client.session_transaction() as sess:
        sess["user_id"] = user_id
        sess["user_tipo"] = role


with app.app_context():
    db.drop_all()
    db.create_all()
    production_date = date.today() - timedelta(days=1)

    admin_user = legacy.Usuario(
        usuario="admin-upgrade",
        nome="Admin",
        tipo="admin",
        senha_hash="x",
        ativo=True,
        is_master=True,
    )
    coop_user = legacy.Usuario(
        usuario="coop-upgrade",
        nome="Cooperado",
        tipo="cooperado",
        senha_hash="x",
        ativo=True,
    )
    rest_user = legacy.Usuario(
        usuario="rest-upgrade",
        nome="Restaurante",
        tipo="restaurante",
        senha_hash="x",
        ativo=True,
    )
    db.session.add_all([admin_user, coop_user, rest_user])
    db.session.flush()

    coop = legacy.Cooperado(nome="Cooperado Teste", usuario_id=coop_user.id)
    rest = legacy.Restaurante(
        nome="Estabelecimento Teste",
        periodo="seg-dom",
        usuario_id=rest_user.id,
        ativo=True,
    )
    db.session.add_all([coop, rest])
    db.session.flush()

    escala = legacy.Escala(
        cooperado_id=coop.id,
        restaurante_id=rest.id,
        data=production_date.isoformat(),
        turno="Manhã",
        horario="08:00 às 09:00",
        contrato=rest.nome,
    )
    db.session.add(escala)
    db.session.commit()

    ids = {
        "admin_user": admin_user.id,
        "coop_user": coop_user.id,
        "rest_user": rest_user.id,
        "coop": coop.id,
        "rest": rest.id,
        "escala": escala.id,
    }

client = app.test_client()

health = client.get("/healthz/upgrade")
assert health.status_code == 200
assert health.get_json()["ok"] is True

login(client, ids["coop_user"], "cooperado")
agenda = client.get("/coop/agenda")
assert agenda.status_code == 200
assert b"Estabelecimento Teste" in agenda.data
payload = {
    "escala_id": str(ids["escala"]),
    "restaurante_id": str(ids["rest"]),
    "data": production_date.isoformat(),
    "hora_inicio": "08:00",
    "hora_fim": "09:00",
    "qtd_entregas": "10",
    "valor_unitario": "5.00",
    "descricao": "Produção teste",
}
response = client.post("/coop/producao", data=payload)
assert response.status_code in {302, 303}

with app.app_context():
    assert upgrade.ProducaoCooperado.query.count() == 1
    pending = upgrade.ProducaoCooperado.query.first()
    assert pending.status == "pendente"
    pending_id = pending.id

response = client.post("/coop/producao", data=payload)
assert response.status_code in {302, 303}
with app.app_context():
    assert upgrade.ProducaoCooperado.query.count() == 1
    assert legacy.Lancamento.query.count() == 0

login(client, ids["rest_user"], "restaurante")
status = client.get("/api/rest/producoes/pendentes/status")
assert status.status_code == 200
assert status.get_json()["count"] == 1

response = client.post(
    f"/rest/producoes/{pending_id}/aprovar",
    data={"qtd_entregas": "12", "valor_unitario": "5.50"},
)
assert response.status_code in {302, 303}

with app.app_context():
    item = db.session.get(upgrade.ProducaoCooperado, pending_id)
    assert item.status == "aprovada"
    assert item.qtd_entregas == 12
    assert round(item.valor_total, 2) == 66.00
    assert legacy.Lancamento.query.count() == 1
    launch = legacy.Lancamento.query.first()
    assert round(launch.valor, 2) == 66.00
    launch_id = launch.id

response = client.post(
    f"/rest/producoes/{pending_id}/aprovar",
    data={"qtd_entregas": "12", "valor_unitario": "5.50"},
)
assert response.status_code in {302, 303}
with app.app_context():
    assert legacy.Lancamento.query.count() == 1

login(client, ids["admin_user"], "admin")
response = client.patch(
    f"/api/admin/lancamentos/{launch_id}",
    json={"valor": 70.0, "qtd_entregas": 14, "descricao": "Ajuste admin"},
)
assert response.status_code == 200
assert response.get_json()["ok"] is True
with app.app_context():
    launch = db.session.get(legacy.Lancamento, launch_id)
    assert round(launch.valor, 2) == 70.0
    assert launch.qtd_entregas == 14

response = client.delete(f"/api/admin/lancamentos/{launch_id}")
assert response.status_code == 200
with app.app_context():
    assert legacy.Lancamento.query.count() == 0
    item = db.session.get(upgrade.ProducaoCooperado, pending_id)
    assert item.lancamento_id is None

required_endpoints = {
    "coop_producao",
    "coop_agenda",
    "rest_producoes_pendentes",
    "rest_producao_aprovar",
    "admin_producoes_pendentes",
    "admin_rapido",
}
assert required_endpoints.issubset(app.view_functions)

print("COOPEX upgrade smoke test: OK")
