from __future__ import annotations

import os
import csv
import io
import re
import json
import difflib
import unicodedata
import re
import mimetypes
from dateutil.relativedelta import relativedelta
from sqlalchemy.inspection import inspect as sa_inspect
from datetime import datetime, date, timedelta, time
from functools import wraps
from collections import defaultdict, namedtuple
from types import SimpleNamespace
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, send_file, abort
)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from sqlalchemy import delete as sa_delete, text as sa_text, func

# =========================
# App / DB
# =========================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "static", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

DOCS_DIR = os.path.join(UPLOAD_DIR, "docs")
os.makedirs(DOCS_DIR, exist_ok=True)
TABELAS_DIR = os.path.join(UPLOAD_DIR, "tabelas")
os.makedirs(TABELAS_DIR, exist_ok=True)

def _build_db_uri() -> str:
    """
    Usa SQLite local se não houver DATABASE_URL.
    Se estiver no Render/Heroku com postgres, converte para o dialeto psycopg3:
      postgres://...         -> postgresql+psycopg://...
      postgresql://...       -> postgresql+psycopg://...
    Garante sslmode=require quando for Postgres.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        return "sqlite:///" + os.path.join(BASE_DIR, "app.db")

    # normaliza esquema
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://") and "+psycopg" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)

    # ssl obrigatório no Render
    if url.startswith("postgresql+psycopg://") and "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

app = Flask(__name__, static_folder="static", template_folder="templates")

# chave da sessão
app.secret_key = os.environ.get("SECRET_KEY", "coopex-secret")

# Configs principais
app.config["SQLALCHEMY_DATABASE_URI"] = _build_db_uri()
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["JSON_SORT_KEYS"] = False  # evita comparar None com int ao serializar |tojson
app.config["MAX_CONTENT_LENGTH"] = 32 * 1024 * 1024  # 32MB

# Sessão/cookies (mais seguro; deixe SECURE=True em produção com HTTPS)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    REMEMBER_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",                 # "Strict" pode quebrar logins vindos de links
    SESSION_COOKIE_SECURE=os.environ.get("FLASK_SECURE_COOKIES", "1") == "1",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12) # ajuste conforme sua política
)

from sqlalchemy.pool import QueuePool

app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "poolclass": QueuePool,   # usa pool local quando não houver PgBouncer
    "pool_size": 5,
    "max_overflow": 10,
    "pool_pre_ping": True,
    "pool_recycle": 1800,
}

db = SQLAlchemy(app)

# --- habilita foreign_keys no SQLite (para ON DELETE CASCADE funcionar) ---
from sqlalchemy import event
from sqlalchemy.engine import Engine

@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_con, con_record):
    try:
        if app.config["SQLALCHEMY_DATABASE_URI"].startswith("sqlite"):
            cur = dbapi_con.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()
    except Exception:
        pass

# =========================
# Models
# =========================
class Usuario(db.Model):
    __tablename__ = "usuarios"
    id = db.Column(db.Integer, primary_key=True)
    usuario = db.Column(db.String(80), unique=True, nullable=False)
    senha_hash = db.Column(db.String(200), nullable=False)
    tipo = db.Column(db.String(20), nullable=False)  # admin | cooperado | restaurante

    def set_password(self, raw: str):
        self.senha_hash = generate_password_hash(raw)

    def check_password(self, raw: str) -> bool:
        return check_password_hash(self.senha_hash, raw)


class Cooperado(db.Model):
    __tablename__ = "cooperados"
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(120), nullable=False)
    usuario_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=False)
    usuario_ref = db.relationship("Usuario", backref="coop_account", uselist=False)
    # Foto: agora guardada no banco (bytea no Postgres / BLOB no SQLite)
    foto_bytes = db.Column(db.LargeBinary)
    foto_mime = db.Column(db.String(100))
    foto_filename = db.Column(db.String(255))
    # Para manter compatibilidade com os templates existentes que usam 'foto_url'
    foto_url = db.Column(db.String(255))
    cnh_numero = db.Column(db.String(50))
    cnh_validade = db.Column(db.Date)
    placa = db.Column(db.String(20))
    placa_validade = db.Column(db.Date)
    ultima_atualizacao = db.Column(db.DateTime)


class Restaurante(db.Model):
    __tablename__ = "restaurantes"
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(120), nullable=False)
    periodo = db.Column(db.String(20), nullable=False)  # seg-dom | sab-sex | sex-qui
    usuario_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=False)
    usuario_ref = db.relationship("Usuario", backref="rest_account", uselist=False)
    # Foto no banco (bytea)
    foto_bytes = db.Column(db.LargeBinary)
    foto_mime = db.Column(db.String(100))
    foto_filename = db.Column(db.String(255))
    # compatibilidade
    foto_url = db.Column(db.String(255))


class Lancamento(db.Model):
    __tablename__ = "lancamentos"
    id = db.Column(db.Integer, primary_key=True)
    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=False)
    cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=False)
    restaurante = db.relationship("Restaurante")
    cooperado = db.relationship("Cooperado")
    descricao = db.Column(db.String(200))
    valor = db.Column(db.Float, default=0.0)
    data = db.Column(db.Date)
    hora_inicio = db.Column(db.String(10))
    hora_fim = db.Column(db.String(10))
    # opcional: quantidade de entregas
    qtd_entregas = db.Column(db.Integer)

# === AVALIAÇÕES DE COOPERADO (NOVO) =========================================
class AvaliacaoCooperado(db.Model):
    __tablename__ = "avaliacoes"
    id = db.Column(db.Integer, primary_key=True)

    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=False)
    cooperado_id   = db.Column(db.Integer, db.ForeignKey("cooperados.id"),  nullable=False)

    # 🔴 IMPORTANTE: CASCADE na FK para o lançamento
    lancamento_id  = db.Column(
        db.Integer,
        db.ForeignKey("lancamentos.id", ondelete="CASCADE"),
        nullable=True  # deixe True para permitir trocar para SET NULL futuramente, se quiser
    )

    # notas 1..5
    estrelas_geral         = db.Column(db.Integer)
    estrelas_pontualidade  = db.Column(db.Integer)
    estrelas_educacao      = db.Column(db.Integer)
    estrelas_eficiencia    = db.Column(db.Integer)
    estrelas_apresentacao  = db.Column(db.Integer)  # "Bem apresentado"

    comentario       = db.Column(db.Text)

    # IA/heurísticas
    media_ponderada  = db.Column(db.Float)
    sentimento       = db.Column(db.String(12))     # positivo | neutro | negativo
    temas            = db.Column(db.String(255))    # palavras-chave resumidas
    alerta_crise     = db.Column(db.Boolean, default=False)
    feedback_motoboy = db.Column(db.Text)

    criado_em = db.Column(db.DateTime, default=datetime.utcnow)


class ReceitaCooperativa(db.Model):
    __tablename__ = "receitas_coop"
    id = db.Column(db.Integer, primary_key=True)
    descricao = db.Column(db.String(200), nullable=False)
    valor_total = db.Column(db.Float, default=0.0)
    data = db.Column(db.Date, nullable=True)


class DespesaCooperativa(db.Model):
    __tablename__ = "despesas_coop"
    id = db.Column(db.Integer, primary_key=True)
    descricao = db.Column(db.String(200), nullable=False)
    valor = db.Column(db.Float, default=0.0)
    data = db.Column(db.Date)


class ReceitaCooperado(db.Model):
    __tablename__ = "receitas_cooperado"
    id = db.Column(db.Integer, primary_key=True)
    cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=False)
    cooperado = db.relationship("Cooperado")
    descricao = db.Column(db.String(200), nullable=False)
    valor = db.Column(db.Float, default=0.0)
    data = db.Column(db.Date)


class DespesaCooperado(db.Model):
    __tablename__ = "despesas_cooperado"
    id = db.Column(db.Integer, primary_key=True)
    cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=True)  # None = Todos
    cooperado = db.relationship("Cooperado")
    descricao = db.Column(db.String(200), nullable=False)
    valor = db.Column(db.Float, default=0.0)
    data = db.Column(db.Date)


class BeneficioRegistro(db.Model):
    __tablename__ = "beneficios_registro"
    id = db.Column(db.Integer, primary_key=True)
    data_inicial = db.Column(db.Date, nullable=False)
    data_final = db.Column(db.Date, nullable=False)
    data_lancamento = db.Column(db.Date)
    tipo = db.Column(db.String(40), nullable=False)  # hospitalar | farmaceutico | alimentar
    valor_total = db.Column(db.Float, default=0.0)
    recebedores_nomes = db.Column(db.Text)  # nomes separados por ';'
    recebedores_ids = db.Column(db.Text)    # ids separados por ';'


class Escala(db.Model):
    __tablename__ = "escalas"
    id = db.Column(db.Integer, primary_key=True)
    cooperado_id = db.Column(
        db.Integer,
        db.ForeignKey("cooperados.id", ondelete="CASCADE"),
        nullable=True  # pode não ter cadastro
    )
    restaurante_id = db.Column(
        db.Integer,
        db.ForeignKey("restaurantes.id", ondelete="CASCADE"),
        nullable=True
    )

    data = db.Column(db.String(40))
    turno = db.Column(db.String(50))
    horario = db.Column(db.String(50))
    contrato = db.Column(db.String(80))
    cor = db.Column(db.String(200))
    cooperado_nome = db.Column(db.String(120))  # nome bruto da planilha quando não há cadastro


class TrocaSolicitacao(db.Model):
    __tablename__ = "trocas"
    id = db.Column(db.Integer, primary_key=True)
    solicitante_id = db.Column(
        db.Integer,
        db.ForeignKey("cooperados.id", ondelete="CASCADE"),
        nullable=False
    )
    destino_id = db.Column(
        db.Integer,
        db.ForeignKey("cooperados.id", ondelete="CASCADE"),
        nullable=False
    )
    origem_escala_id = db.Column(
        db.Integer,
        db.ForeignKey("escalas.id", ondelete="CASCADE"),
        nullable=False
    )
    mensagem = db.Column(db.Text)
    status = db.Column(db.String(20), default="pendente")
    criada_em = db.Column(db.DateTime, default=datetime.utcnow)
    aplicada_em = db.Column(db.DateTime)


class Config(db.Model):
    __tablename__ = "config"
    id = db.Column(db.Integer, primary_key=True)
    salario_minimo = db.Column(db.Float, default=0.0)


class Documento(db.Model):
    __tablename__ = "documentos"
    id = db.Column(db.Integer, primary_key=True)
    titulo = db.Column(db.String(200), nullable=False)
    categoria = db.Column(db.String(40))
    descricao = db.Column(db.String(255))
    arquivo_url = db.Column(db.String(255), nullable=False)
    arquivo_nome = db.Column(db.String(255))
    enviado_em = db.Column(db.DateTime, default=datetime.utcnow)


class Tabela(db.Model):

    id = db.Column(db.Integer, primary_key=True)
    titulo = db.Column(db.String(200), nullable=False)
    categoria = db.Column(db.String(40))
    descricao = db.Column(db.String(255))
    arquivo_url = db.Column(db.String(255), nullable=False)
    arquivo_nome = db.Column(db.String(255))
    enviado_em = db.Column(db.DateTime, default=datetime.utcnow)


# ---------- AVISOS (NOVO) ----------
aviso_restaurantes = db.Table(
    "aviso_restaurantes",
    db.Column("aviso_id", db.Integer, db.ForeignKey("avisos.id"), primary_key=True),
    db.Column("restaurante_id", db.Integer, db.ForeignKey("restaurantes.id"), primary_key=True),
)

class Aviso(db.Model):
    __tablename__ = "avisos"
    id = db.Column(db.Integer, primary_key=True)
    titulo = db.Column(db.String(140), nullable=False)
    corpo = db.Column(db.Text, nullable=False)
    # escopo: global | restaurante | cooperado
    tipo = db.Column(db.String(20), nullable=False, default="global")

    # destino individual (opcional)
    destino_cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"))
    destino_cooperado = db.relationship("Cooperado", foreign_keys=[destino_cooperado_id])

    # destino por restaurante (opcional, N:N)
    restaurantes = db.relationship("Restaurante", secondary=aviso_restaurantes, backref="avisos")

    prioridade = db.Column(db.String(10), default="normal")  # normal | alta
    fixado = db.Column(db.Boolean, default=False)
    ativo = db.Column(db.Boolean, default=True)
    inicio_em = db.Column(db.DateTime)  # janela de exibição opcional
    fim_em = db.Column(db.DateTime)

    criado_por_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=False)
    criado_em = db.Column(db.DateTime, default=datetime.utcnow)
    atualizado_em = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AvisoLeitura(db.Model):
    __tablename__ = "aviso_leituras"
    id = db.Column(db.Integer, primary_key=True)
    aviso_id = db.Column(db.Integer, db.ForeignKey("avisos.id"), nullable=False, index=True)
    cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=True, index=True)
    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=True, index=True)
    lido_em = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (db.UniqueConstraint("aviso_id", "cooperado_id", "restaurante_id", name="uq_aviso_dest"), )


# =========================
# Helpers
# =========================
def _is_sqlite() -> bool:
    try:
        return db.session.get_bind().dialect.name == "sqlite"
    except Exception:
        return "sqlite" in (app.config.get("SQLALCHEMY_DATABASE_URI") or "")

def init_db():
    # --- PERF (SQLite): liga WAL e ajusta synchronous ---
    try:
        if _is_sqlite():
            db.session.execute(sa_text("PRAGMA journal_mode=WAL;"))
            db.session.execute(sa_text("PRAGMA synchronous=NORMAL;"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    db.create_all()

def init_db():
    # --- PERF (SQLite): liga WAL e ajusta synchronous ---
    try:
        if _is_sqlite():
            db.session.execute(sa_text("PRAGMA journal_mode=WAL;"))
            db.session.execute(sa_text("PRAGMA synchronous=NORMAL;"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # Cria tabelas/mapeamentos
    db.create_all()

    # --- índices de performance p/ avaliações (restaurante_id, cooperado_id, criado_em) ---
    try:
        if _is_sqlite():
            db.session.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_avaliacoes_criado_em        ON avaliacoes (criado_em);
            CREATE INDEX IF NOT EXISTS ix_avaliacoes_rest_criado      ON avaliacoes (restaurante_id, criado_em);
            CREATE INDEX IF NOT EXISTS ix_avaliacoes_coop_criado      ON avaliacoes (cooperado_id,  criado_em);

            CREATE INDEX IF NOT EXISTS ix_av_rest_criado_em           ON avaliacoes_restaurante (criado_em);
            CREATE INDEX IF NOT EXISTS ix_av_rest_rest_criado         ON avaliacoes_restaurante (restaurante_id, criado_em);
            CREATE INDEX IF NOT EXISTS ix_av_rest_coop_criado         ON avaliacoes_restaurante (cooperado_id,  criado_em);
            """))
        else:
            db.session.execute(sa_text("""
            CREATE INDEX IF NOT EXISTS ix_avaliacoes_criado_em        ON public.avaliacoes (criado_em);
            CREATE INDEX IF NOT EXISTS ix_avaliacoes_rest_criado      ON public.avaliacoes (restaurante_id, criado_em);
            CREATE INDEX IF NOT EXISTS ix_avaliacoes_coop_criado      ON public.avaliacoes (cooperado_id,  criado_em);

            CREATE INDEX IF NOT EXISTS ix_av_rest_criado_em           ON public.avaliacoes_restaurante (criado_em);
            CREATE INDEX IF NOT EXISTS ix_av_rest_rest_criado         ON public.avaliacoes_restaurante (restaurante_id, criado_em);
            CREATE INDEX IF NOT EXISTS ix_av_rest_coop_criado         ON public.avaliacoes_restaurante (cooperado_id,  criado_em);
            """))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # --- qtd_entregas em lancamentos ---
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(lancamentos);")).fetchall()
            colnames = {row[1] for row in cols}
            if "qtd_entregas" not in colnames:
                db.session.execute(sa_text("ALTER TABLE lancamentos ADD COLUMN qtd_entregas INTEGER"))
                db.session.commit()
        else:
            db.session.execute(sa_text("ALTER TABLE IF EXISTS lancamentos ADD COLUMN IF NOT EXISTS qtd_entregas INTEGER"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # --- cooperado_nome em escalas ---
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(escalas);")).fetchall()
            colnames = {row[1] for row in cols}
            if "cooperado_nome" not in colnames:
                db.session.execute(sa_text("ALTER TABLE escalas ADD COLUMN cooperado_nome VARCHAR(120)"))
                db.session.commit()
        else:
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS escalas ADD COLUMN IF NOT EXISTS cooperado_nome VARCHAR(120)"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # --- restaurante_id em escalas ---
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(escalas);")).fetchall()
            colnames = {row[1] for row in cols}
            if "restaurante_id" not in colnames:
                db.session.execute(sa_text("ALTER TABLE escalas ADD COLUMN restaurante_id INTEGER"))
                db.session.commit()
        else:
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS escalas ADD COLUMN IF NOT EXISTS restaurante_id INTEGER"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # --- fotos no banco (cooperados) ---
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(cooperados);")).fetchall()
            colnames = {row[1] for row in cols}
            if "foto_bytes" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN foto_bytes BLOB"))
            if "foto_mime" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN foto_mime VARCHAR(100)"))
            if "foto_filename" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN foto_filename VARCHAR(255)"))
            if "foto_url" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN foto_url VARCHAR(255)"))
            db.session.commit()
        else:
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_bytes BYTEA"))
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_mime VARCHAR(100)"))
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_filename VARCHAR(255)"))
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_url VARCHAR(255)"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # --- tabela avaliacoes_restaurante (se não existir) ---
    try:
        if _is_sqlite():
            db.session.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS avaliacoes_restaurante (
                  id INTEGER PRIMARY KEY,
                  restaurante_id INTEGER NOT NULL,
                  cooperado_id   INTEGER NOT NULL,
                  lancamento_id  INTEGER UNIQUE,
                  estrelas_geral INTEGER,
                  estrelas_pontualidade INTEGER,
                  estrelas_educacao INTEGER,
                  estrelas_eficiencia INTEGER,
                  estrelas_apresentacao INTEGER,
                  comentario TEXT,
                  media_ponderada REAL,
                  sentimento TEXT,
                  temas TEXT,
                  alerta_crise INTEGER DEFAULT 0,
                  criado_em TIMESTAMP
                );
            """))
            db.session.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_av_rest_rest ON avaliacoes_restaurante(restaurante_id, criado_em)"))
            db.session.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_av_rest_coop ON avaliacoes_restaurante(cooperado_id)"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS avaliacoes_restaurante (
                  id SERIAL PRIMARY KEY,
                  restaurante_id INTEGER NOT NULL,
                  cooperado_id   INTEGER NOT NULL,
                  lancamento_id  INTEGER UNIQUE,
                  estrelas_geral INTEGER,
                  estrelas_pontualidade INTEGER,
                  estrelas_educacao INTEGER,
                  estrelas_eficiencia INTEGER,
                  estrelas_apresentacao INTEGER,
                  comentario TEXT,
                  media_ponderada DOUBLE PRECISION,
                  sentimento VARCHAR(12),
                  temas VARCHAR(255),
                  alerta_crise BOOLEAN DEFAULT FALSE,
                  criado_em TIMESTAMP
                );
            """))
            db.session.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_av_rest_rest ON avaliacoes_restaurante(restaurante_id, criado_em)"))
            db.session.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_av_rest_coop ON avaliacoes_restaurante(cooperado_id)"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # --- fotos no banco (restaurantes) ---
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(restaurantes);")).fetchall()
            colnames = {row[1] for row in cols}
            if "foto_bytes" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN foto_bytes BLOB"))
            if "foto_mime" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN foto_mime VARCHAR(100)"))
            if "foto_filename" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN foto_filename VARCHAR(255)"))
            if "foto_url" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN foto_url VARCHAR(255)"))
            db.session.commit()
        else:
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_bytes BYTEA"))
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_mime VARCHAR(100)"))
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_filename VARCHAR(255)"))
            db.session.execute(sa_text("ALTER TABLE IF NOT EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_url VARCHAR(255)"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # --- usuário admin + config padrão ---
    if not Usuario.query.filter_by(tipo="admin").first():
        admin_user = os.environ.get("ADMIN_USER", "admin")
        admin_pass = os.environ.get("ADMIN_PASS", os.urandom(8).hex())
        admin = Usuario(usuario=admin_user, tipo="admin", senha_hash="")
        admin.set_password(admin_pass)
        db.session.add(admin)
        db.session.commit()

    if not Config.query.get(1):
        db.session.add(Config(id=1, salario_minimo=0.0))
        db.session.commit()


# === Bootstrap do banco no start (Render/Gunicorn) ===
try:
    # Opcional: controle por variável de ambiente para evitar lentidão no boot
    if os.environ.get("INIT_DB_ON_START", "1") == "1":
        _t0 = datetime.utcnow()
        with app.app_context():
            init_db()
        try:
            app.logger.info(f"init_db concluído em {(datetime.utcnow() - _t0).total_seconds():.2f}s")
        except Exception:
            pass
    else:
        try:
            app.logger.info("INIT_DB_ON_START=0: pulando init_db no boot.")
        except Exception:
            pass
except Exception as e:
    # Evita derrubar o serviço se a inicialização falhar por motivo não crítico
    try:
        app.logger.warning(f"init_db pulado: {e}")
    except Exception:
        pass


def get_config() -> Config:
    cfg = Config.query.get(1)
    if not cfg:
        cfg = Config(id=1, salario_minimo=0.0)
        db.session.add(cfg)
        db.session.commit()
    return cfg


def role_required(role: str):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if session.get("user_tipo") != role:
                return redirect(url_for("login"))
            return fn(*args, **kwargs)
        return wrapper
    return deco


def admin_required(fn):
    return role_required("admin")(fn)


def _normalize_name(s: str) -> list[str]:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)
    parts = [p.lower() for p in s.split() if p.strip()]
    return parts


def _norm_login(s: str) -> str:
    # remove acento, minúsculo e sem espaços
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower().strip()
    s = re.sub(r"\s+", "", s)
    return s

def _match_cooperado_by_login(login_planilha: str, cooperados: list[Cooperado]) -> Cooperado | None:
    """Casa EXATAMENTE com Usuario.usuario após normalização."""
    key = _norm_login(login_planilha)
    if not key:
        return None
    for c in cooperados:
        # c.usuario_ref.usuario é o login usado no sistema
        login = getattr(c.usuario_ref, "usuario", "") or ""
        if _norm_login(login) == key:
            return c
    return None


def _match_restaurante_id(contrato_txt: str) -> int | None:
    alvo = " ".join(_normalize_name(contrato_txt or ""))
    if not alvo:
        return None
    restaurantes = Restaurante.query.order_by(Restaurante.nome.asc()).all()

    for r in restaurantes:
        rn = " ".join(_normalize_name(r.nome))
        if alvo == rn or alvo in rn or rn in alvo:
            return r.id

    try:
        nomes_norm = [" ".join(_normalize_name(r.nome)) for r in restaurantes]
        close = difflib.get_close_matches(alvo, nomes_norm, n=1, cutoff=0.85)
        if close:
            alvo_norm = close[0]
            for r in restaurantes:
                if " ".join(_normalize_name(r.nome)) == alvo_norm:
                    return r.id
    except Exception:
        pass
    return None


def _match_cooperado_by_name(nome_planilha: str, cooperados: list[Cooperado]) -> Cooperado | None:
    def norm_join(s: str) -> str:
        return " ".join(_normalize_name(s))

    sheet_tokens = _normalize_name(nome_planilha)
    sheet_norm = " ".join(sheet_tokens)
    if not sheet_norm:
        return None

    for c in cooperados:
        c_norm = norm_join(c.nome)
        if sheet_norm == c_norm or sheet_norm in c_norm or c_norm in sheet_norm:
            return c

    parts_sheet = set(sheet_tokens)
    best, best_count = None, 0
    for c in cooperados:
        parts_c = set(_normalize_name(c.nome))
        inter = parts_sheet & parts_c
        if len(inter) > best_count:
            best, best_count = c, len(inter)
    if best and best_count >= 2:
        return best

    if len(sheet_tokens) == 1 and len(sheet_tokens[0]) >= 3:
        token = sheet_tokens[0]
        hits = [c for c in cooperados if token in set(_normalize_name(c.nome))]
        if os.environ.get("INIT_DB_ON_START", "0") == "1":
            return hits[0]

    names_norm = [norm_join(c.nome) for c in cooperados]
    close = difflib.get_close_matches(sheet_norm, names_norm, n=1, cutoff=0.85)
    if close:
        target = close[0]
        for c in cooperados:
            if norm_join(c.nome) == target:
                return c
    return None


def _build_docinfo(c: Cooperado) -> dict:
    today = date.today()
    cnh_ok = (c.cnh_validade is not None and c.cnh_validade >= today)
    placa_ok = (c.placa_validade is not None and c.placa_validade >= today)
    return {"cnh": {"ok": cnh_ok}, "placa": {"ok": placa_ok}}


def _save_upload(file_storage) -> str | None:
    # Mantido para compatibilidade com outras partes do app (ex.: uploads de xlsx)
    if not file_storage:
        return None
    fname = secure_filename(file_storage.filename or "")
    if not fname:
        return None
    path = os.path.join(UPLOAD_DIR, fname)
    file_storage.save(path)
    return f"/static/uploads/{fname}"

def _save_foto_to_db(entidade, file_storage, *, is_cooperado: bool) -> str | None:
    """
    Salva o arquivo enviado diretamente no banco (bytea/Blob) e
    retorna uma URL interna (/media/coop/<id> ou /media/rest/<id>).
    """
    if not file_storage or not file_storage.filename:
        return getattr(entidade, "foto_url", None)
    data = file_storage.read()
    if not data:
        return getattr(entidade, "foto_url", None)
    entidade.foto_bytes = data
    entidade.foto_mime = (file_storage.mimetype or "application/octet-stream")
    entidade.foto_filename = secure_filename(file_storage.filename)
    # garante que temos ID
    db.session.flush()
    if is_cooperado:
        url = url_for("media_coop", coop_id=entidade.id)
    else:
        url = url_for("media_rest", rest_id=entidade.id)
    entidade.foto_url = f"{url}?v={int(datetime.utcnow().timestamp())}"
    return entidade.foto_url

def _abs_path_from_url(rel_url: str) -> str:
    """
    Converte '/static/uploads/arquivo.pdf' para o caminho absoluto no disco.
    """
    if not rel_url:
        return ""
    # caminho padrão: /static/uploads/...
    if rel_url.startswith("/"):
        rel_url = rel_url.lstrip("/")
    return os.path.join(BASE_DIR, rel_url.replace("/", os.sep))

def _serve_uploaded(rel_url: str, *, download_name: str | None = None, force_download: bool = False):
    """
    Entrega um arquivo salvo em /static/uploads com mimetype correto.
    - PDFs abrem inline (no navegador) por padrão.
    - Se quiser forçar download, passe force_download=True.
    """
    if not rel_url:
        abort(404)
    abs_path = _abs_path_from_url(rel_url)
    if not os.path.exists(abs_path):
        abort(404)

    mime, _ = mimetypes.guess_type(abs_path)
    is_pdf = (mime == "application/pdf") or abs_path.lower().endswith(".pdf")
    return send_file(
        abs_path,
        mimetype=mime or "application/octet-stream",
        as_attachment=(force_download or not is_pdf),
        download_name=(download_name or os.path.basename(abs_path)),
        conditional=True,     # ajuda visualização/retomar download
    )

def _prox_ocorrencia_anual(dt: date | None) -> date | None:
    if not dt:
        return None
    hoje = date.today()
    alvo = date(hoje.year, dt.month, dt.day)
    if alvo < hoje:
        alvo = date(hoje.year + 1, dt.month, dt.day)
    return alvo


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None
    

def _parse_ymd(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _bounds_mes(yyyy_mm: str):
    # "2025-06" -> [2025-06-01, 2025-07-01)
    y, m = map(int, yyyy_mm.split("-"))
    ini = date(y, m, 1)
    fim = (ini + relativedelta(months=1))
    return ini, fim
    

def _parse_data_ymd(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _fmt_br(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if d else ""


def _dow(dt: date) -> str:
    return str((dt.weekday() % 7) + 1)


# === Helpers de Avaliação (NLP leve + métricas) =============================
def _clamp_star(v):
    try:
        v = int(v)
    except Exception:
        return None
    return min(5, max(1, v))

def _media_ponderada(geral, pont, educ, efic, apres):
    """
    Ponderação (soma=1.0):
      Geral 0.40 + Pontualidade 0.15 + Educação 0.15 + Eficiência 0.15 + Bem Apresentado 0.15
    Calcula só com os campos presentes (ignora None) e renormaliza pesos.
    """
    pares = [
        (geral, 0.40),
        (pont,  0.15),
        (educ,  0.15),
        (efic,  0.15),
        (apres, 0.15),
    ]
    num = 0.0
    den = 0.0
    for nota, w in pares:
        if nota is not None:
            num += float(nota) * w
            den += w
    return round(num / den, 2) if den > 0 else None

_POS = set("""
bom ótima otimo excelente parabéns educado gentil atencioso cordial limpo cheiroso organizado rápido rapida rapido pontual
""".split())
_NEG = set("""
ruim péssimo pessimo horrível horrivel sujo atrasado grosseiro mal educado agressivo impaciente amassado quebrado frio derramou
""".split())

def _analise_sentimento(txt: str | None) -> str:
    if not txt:
        return "neutro"
    t = (txt or "").lower()
    # contagem bem simples
    pos = sum(1 for w in _POS if w in t)
    neg = sum(1 for w in _NEG if w in t)
    if neg > pos + 0: return "negativo"
    if pos > neg + 0: return "positivo"
    return "neutro"

# mapeia temas por palavras-chave simples
_TEMAS = {
    "Pontualidade":  ["pontual", "atras", "horario", "horário", "demor", "rápido", "rapido", "lent"],
    "Educação":      ["educad", "grosseir", "simpat", "antipatic", "mal trat", "sem paciencia", "sem paciência", "atencios"],
    "Eficiência":    ["amass", "vazou", "quebrad", "frio", "bagunça", "bagunca", "cuidado", "eficien", "desorgan"],
    "Bem apresentado": ["uniform", "higien", "apresenta", "limpo", "cheiroso", "aparencia", "aparência"],
}

def _identifica_temas(txt: str | None) -> list[str]:
    if not txt:
        return []
    t = (txt or "").lower()
    hits = []
    for tema, keys in _TEMAS.items():
        if any(k in t for k in keys):
            hits.append(tema)
    return hits[:4]

_RISCO = ["ameaça","ameaca","acidente","quebrado","agress","roubo","violên","violenc","lesão","lesao","sangue","caiu","bateu","droga","alcool","álcool"]

def _sinaliza_crise(nota_geral: int | None, txt: str | None) -> bool:
    if nota_geral == 1 and txt:
        low = txt.lower()
        return any(k in low for k in _RISCO)
    return False

def _gerar_feedback(pont, educ, efic, apres, comentario, sentimento):
    partes = []
    def badge(nome, nota):
        return f"{nome}: {nota} ★" if nota is not None else None

    for nome, nota in (("Pontualidade", pont), ("Educação", educ), ("Eficiência", efic), ("Apresentação", apres)):
        b = badge(nome, nota)
        if b: partes.append(b)

    dicas = []
    if educ is not None and educ <= 2: dicas.append("melhore a abordagem/educação ao falar com o cliente")
    if pont is not None and pont <= 2: dicas.append("tente chegar no horário combinado")
    if efic is not None and efic <= 2: dicas.append("redobre o cuidado com o pedido durante o transporte")
    if apres is not None and apres <= 2: dicas.append("capriche na apresentação pessoal (higiene/uniforme)")

    txt = f"Notas — " + " | ".join(partes) if partes else "Obrigado pelo trabalho!"
    if comentario:
        txt += f". Cliente comentou: \"{comentario.strip()}\""
    if dicas:
        txt += ". Dica: " + "; ".join(dicas) + "."
    if sentimento == "positivo":
        txt += " 👏"
    return txt[:1000]

from datetime import datetime, date
from sqlalchemy import or_, and_

# routes_avisos.py
from datetime import datetime
from flask import Blueprint, render_template, redirect, request, url_for, abort
from flask_login import current_user


def _cooperado_atual() -> Cooperado | None:
    """
    Retorna o Cooperado do usuário logado, usando a sessão da aplicação.
    """
    uid = session.get("user_id")
    if not uid:
        return None
    return Cooperado.query.filter_by(usuario_id=uid).first()

    # --- Blueprint Portal (topo do arquivo, depois de criar `app`) ---
portal_bp = Blueprint("portal", __name__, url_prefix="/portal")

@portal_bp.get("/avisos", endpoint="portal_cooperado_avisos")
@role_required("cooperado")
def avisos_list():
    coop = _cooperado_atual()
    if not coop:
        abort(403)

    # pega todos os avisos que se aplicam ao cooperado (seu helper)
    avisos = get_avisos_for_cooperado(coop)

    # busca leituras de uma vez (evita N+1)
    lidos_ids = {
        r.aviso_id
        for r in AvisoLeitura.query.filter_by(cooperado_id=coop.id).all()
    }

    # injeta flag lido para o template (sem tocar no banco)
    for a in avisos:
        a.lido = (a.id in lidos_ids)

    avisos_nao_lidos_count = sum(1 for a in avisos if not getattr(a, "lido", False))
    current_year = datetime.now().year

    return render_template(
        "portal_cooperado_avisos.html",
        avisos=avisos,
        avisos_nao_lidos_count=avisos_nao_lidos_count,
        current_year=current_year
    )

# === AVALIAÇÕES: Cooperado -> Restaurante (NOVO) =============================
class AvaliacaoRestaurante(db.Model):
    __tablename__ = "avaliacoes_restaurante"
    id = db.Column(db.Integer, primary_key=True)

    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=False, index=True)
    cooperado_id   = db.Column(db.Integer, db.ForeignKey("cooperados.id"),   nullable=False, index=True)

    # 🔴 IMPORTANTE: CASCADE ao apagar o lançamento
    lancamento_id  = db.Column(
        db.Integer,
        db.ForeignKey("lancamentos.id", ondelete="CASCADE"),
        unique=True,
        index=True,
        nullable=True
    )

    # mesmas métricas 1..5
    estrelas_geral        = db.Column(db.Integer)
    estrelas_pontualidade = db.Column(db.Integer)
    estrelas_educacao     = db.Column(db.Integer)
    estrelas_eficiencia   = db.Column(db.Integer)
    estrelas_apresentacao = db.Column(db.Integer)

    comentario      = db.Column(db.Text)
    media_ponderada = db.Column(db.Float)
    sentimento      = db.Column(db.String(12))
    temas           = db.Column(db.String(255))
    alerta_crise    = db.Column(db.Boolean, default=False)

    criado_em = db.Column(db.DateTime, default=datetime.utcnow, index=True)

@portal_bp.post("/avisos/<int:aviso_id>/lido", endpoint="marcar_aviso_lido")
@role_required("cooperado")
def avisos_marcar_lido(aviso_id: int):
    coop = _cooperado_atual()
    if not coop:
        abort(403)

    aviso = Aviso.query.get_or_404(aviso_id)

    # idempotente: só cria se ainda não houver leitura
    ja_leu = AvisoLeitura.query.filter_by(
        cooperado_id=coop.id,
        aviso_id=aviso.id
    ).first()

    if not ja_leu:
        db.session.add(AvisoLeitura(
            cooperado_id=coop.id,
            aviso_id=aviso.id,
            lido_em=datetime.utcnow()
        ))
        db.session.commit()

    next_url = request.form.get("next") or (url_for("portal.portal_cooperado_avisos") + f"#aviso-{aviso.id}")
    return redirect(next_url)

@portal_bp.post("/avisos/marcar-todos", endpoint="marcar_todos_avisos_lidos")
@role_required("cooperado")
def avisos_marcar_todos():
    coop = _cooperado_atual()
    if not coop:
        abort(403)

    avisos = get_avisos_for_cooperado(coop)
    if not avisos:
        return redirect(url_for("portal.portal_cooperado_avisos"))
    ids_todos = {a.id for a in avisos}
    ids_ja_lidos = {
        r.aviso_id
        for r in AvisoLeitura.query.filter_by(cooperado_id=coop.id).all()
    }
    ids_pendentes = list(ids_todos - ids_ja_lidos)

    if ids_pendentes:
        db.session.bulk_save_objects([
            AvisoLeitura(cooperado_id=coop.id, aviso_id=aid, lido_em=datetime.utcnow())
            for aid in ids_pendentes
        ])
        db.session.commit()

    return redirect(url_for("portal.portal_cooperado_avisos"))

  # --- Registro do blueprint 'portal' (uma única vez, após definir TODAS as rotas dele)
# --- Registro do blueprint 'portal' (depois de definir TODAS as rotas do blueprint)
def register_blueprints_once(app):
    if "portal" not in app.blueprints:
        app.register_blueprint(portal_bp)

register_blueprints_once(app)

# --- Alias para compatibilidade com o template (endpoint esperado: 'portal_cooperado_avisos')
from flask import redirect, url_for

def _portal_cooperado_avisos_alias():
    # redireciona para a rota real dentro do blueprint 'portal'
    return redirect(url_for("portal.portal_cooperado_avisos"))

# publica a URL "antiga" (ajuste o path se o seu antigo era outro)
app.add_url_rule(
    "/portal/cooperado/avisos",         # caminho acessado
    endpoint="portal_cooperado_avisos", # nome que o template usa no url_for(...)
    view_func=_portal_cooperado_avisos_alias,
    methods=["GET"],
)

# ======== Helpers p/ troca: data/weekday/turno ========
def _parse_data_escala_str(s: str) -> date | None:
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', str(s or ''))
    if not m:
        return None
    d_, mth, y = map(int, m.groups())
    if y < 100:
        y += 2000
    try:
        return date(y, mth, d_)
    except Exception:
        return None

def _weekday_from_data_str(s: str) -> int | None:
    dt = _parse_data_escala_str(s)
    if dt:
        return (dt.weekday() % 7) + 1
    txt = unicodedata.normalize("NFD", str(s or "").lower())
    txt = "".join(ch for ch in txt if unicodedata.category(ch) != "Mn")
    M = re.search(
        r"\b(seg|segunda|ter|terca|terça|qua|quarta|qui|quinta|sex|sexta|sab|sabado|sábado|dom|domingo)\b",
        txt
    )
    if not M:
        M = re.search(r"\b(seg|ter|qua|qui|sex|sab|dom)\b", txt)
        if not M:
            return None
    token = M.group(1)
    mapa = {
        "seg":1,"segunda":1,
        "ter":2,"terca":2,"terça":2,
        "qua":3,"quarta":3,
        "qui":4,"quinta":4,
        "sex":5,"sexta":5,
        "sab":6,"sabado":6,"sábado":6,
        "dom":7,"domingo":7,
    }
    return mapa.get(token)

def _weekday_abbr(num: int | None) -> str:
    return {1:"SEG",2:"TER",3:"QUA",4:"QUI",5:"SEX",6:"SÁB",7:"DOM"}.get(num or 0, "")

def _turno_bucket(turno: str | None, horario: str | None) -> str:
    t = (turno or "").lower()
    t = unicodedata.normalize("NFD", t)
    t = "".join(ch for ch in t if unicodedata.category(ch) != "Mn")
    if "noite" in t or "noturn" in t:
        return "noite"
    if any(x in t for x in ["dia", "diurn", "manha", "manhã", "tarde"]):
        return "dia"
    m = re.search(r'(\d{1,2}):(\d{2})', str(horario or ""))
    if m:
        h = int(m.group(1))
        return "noite" if (h >= 17 or h <= 6) else "dia"
    return "dia"

def _escala_label(e: Escala | None) -> str:
    if not e:
        return "—"
    wd = _weekday_from_data_str(e.data)
    wd_abbr = _weekday_abbr(wd)
    dt = _parse_data_escala_str(e.data)
    if dt:
        data_txt = dt.strftime("%d/%m/%y") + (f"-{wd_abbr}" if wd_abbr else "")
    else:
        data_txt = (str(e.data or "").strip() or wd_abbr)
    turno_txt = (e.turno or "").strip() or _turno_bucket(e.turno, e.horario).upper()
    horario_txt = (e.horario or "").strip()
    contrato_txt = (e.contrato or "").strip()
    parts = [x for x in [data_txt, turno_txt, horario_txt, contrato_txt] if x]
    return " • ".join(parts)

def _carry_forward_contrato(escalas: list[Escala]) -> dict[int, str]:
    eff = {}
    atual = ""
    for e in escalas:
        raw = (e.contrato or "").strip()
        if raw:
            atual = raw
        eff[e.id] = atual
    return eff

def _parse_linhas_from_msg(msg: str | None) -> list[dict]:
    if not msg:
        return []
    blobs = re.findall(r"__AFETACAO_JSON__\s*[:=]\s*(\{.*?\})\s*$", str(msg), flags=re.DOTALL)
    if not blobs:
        return []
    raw = blobs[-1]
    try:
        payload = json.loads(raw)
    except Exception:
        try:
            payload = json.loads(raw.replace("'", '\"'))
        except Exception:
            return []
    linhas = payload.get("linhas") or payload.get("rows") or []
    out = []
    for r in (linhas if isinstance(linhas, list) else []):
        turno = str(r.get("turno") or "").strip()
        horario = str(r.get("horario") or "").strip()
        turno_horario = (r.get("turno_horario") or " • ".join(x for x in [turno, horario] if x)).strip()
        out.append({
            "dia": str(r.get("dia") or ""),
            "turno_horario": turno_horario,
            "contrato": str(r.get("contrato") or ""),
            "saiu": str(r.get("saiu") or ""),
            "entrou": str(r.get("entrou") or ""),
        })
    return out

def _strip_afetacao_blob(msg: str | None) -> str:
    if not msg:
        return ""
    return re.sub(r"__AFETACAO_JSON__\s*[:=]\s*\{.*\}\s*$", "", str(msg), flags=re.DOTALL).strip()

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", str(s or "").strip().lower())
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s

def to_css_color(v: str) -> str:
    t = str(v or "").strip()
    if not t:
        return ""
    t_low = t.lower().strip()
    if re.fullmatch(r"[0-9a-fA-F]{8}", t):
        return f"#{t[2:8]}"
    if re.fullmatch(r"[0-9a-fA-F]{6}", t):
        return f"#{t}"
    if re.fullmatch(r"#?[0-9a-fA-F]{6,8}", t):
        if not t.startswith("#"):
            t = f"#{t}"
        if len(t) == 9:
            a = int(t[1:3], 16) / 255.0
            r = int(t[3:5], 16)
            g = int(t[5:7], 16)
            b = int(t[7:9], 16)
            return f"rgba({r},{g},{b},{a:.3f})"
        return t
    m = re.fullmatch(r"\s*(\d{1,3})\s*[,;]\s*(\d{1,3})\s*[,;]\s*(\d{1,3})\s*", t)
    if m:
        r, g, b = [max(0, min(255, int(x))) for x in m.groups()]
        return f"rgb({r},{g},{b})"
    mapa = {
        "azul": "blue", "vermelho": "red", "verde": "green",
        "amarelo": "yellow", "cinza": "gray", "preto": "black",
        "branco": "white", "laranja": "orange", "roxo": "purple",
    }
    return mapa.get(t_low, t)

# ---------- AVISOS: helpers ----------
from sqlalchemy import case, or_, and_, func
from sqlalchemy.orm import selectinload

def _avisos_base_query():
    # usa o relógio do banco; evita divergência de TZ/UTC da app
    now = func.now()
    return (
        Aviso.query
        .options(selectinload(Aviso.restaurantes))  # evita N+1 no template
        .filter(Aviso.ativo.is_(True))
        .filter(or_(Aviso.inicio_em.is_(None), Aviso.inicio_em <= now))
        .filter(or_(Aviso.fim_em.is_(None),    Aviso.fim_em    >= now))
    )

# PRIORIDADE: "alta" (0), "media"/"média" (1), outras/NULL (2)
_PRIORD = case(
    (func.lower(Aviso.prioridade) == "alta", 0),
    (func.lower(Aviso.prioridade).in_(("media", "média")), 1),
    else_=2,
)

def get_avisos_for_cooperado(coop: Cooperado):
    """
    COOPERADO vê:
      - global
      - cooperado (destino = mim OU broadcast)
      - restaurante (broadcast OU ligado a QUALQUER restaurante onde tenho escala)
    """
    # busca os restaurantes do cooperado sem trazer objetos inteiros
    rest_ids = [
        rid for (rid,) in (
            db.session.query(Escala.restaurante_id)
            .filter(Escala.cooperado_id == coop.id, Escala.restaurante_id.isnot(None))
            .distinct()
            .all()
        )
    ]

    cond_rest = or_(~Aviso.restaurantes.any())
    if rest_ids:  # só usa IN se houver IDs
        cond_rest = or_(cond_rest, Aviso.restaurantes.any(Restaurante.id.in_(rest_ids)))

    q = (
        _avisos_base_query()
        .filter(
            or_(
                (Aviso.tipo == "global"),
                and_(
                    Aviso.tipo == "cooperado",
                    or_(Aviso.destino_cooperado_id == coop.id,
                        Aviso.destino_cooperado_id.is_(None)),
                ),
                and_(
                    Aviso.tipo == "restaurante",
                    cond_rest,
                ),
            )
        )
        .order_by(
            Aviso.fixado.desc(),
            _PRIORD.asc(),
            Aviso.criado_em.desc(),
        )
    )

    return q.all()

def get_avisos_for_restaurante(rest: Restaurante):
    """
    RESTAURANTE vê:
      - global
      - restaurante (broadcast ou destinado a ESTE restaurante)
    """
    q = (
        _avisos_base_query()
        .filter(
            or_(
                (Aviso.tipo == "global"),
                and_(
                    Aviso.tipo == "restaurante",
                    or_(
                        ~Aviso.restaurantes.any(),                  # broadcast
                        Aviso.restaurantes.any(Restaurante.id == rest.id),  # específico
                    ),
                ),
            )
        )
        .order_by(
            Aviso.fixado.desc(),
            _PRIORD.asc(),
            Aviso.criado_em.desc(),
        )
    )

    return q.all()

# =========================
# Rotas de mídia (fotos armazenadas no banco)
# =========================
@app.get("/media/coop/<int:coop_id>")
def media_coop(coop_id: int):
    c = Cooperado.query.get_or_404(coop_id)
    if c.foto_bytes:
        return send_file(
            io.BytesIO(c.foto_bytes),
            mimetype=(c.foto_mime or "application/octet-stream"),
            as_attachment=False,
            download_name=(c.foto_filename or f"coop_{coop_id}.bin"),
        )
    abort(404)

@app.get("/media/rest/<int:rest_id>")
def media_rest(rest_id: int):
    r = Restaurante.query.get_or_404(rest_id)
    if r.foto_bytes:
        return send_file(
            io.BytesIO(r.foto_bytes),
            mimetype=(r.foto_mime or "application/octet-stream"),
            as_attachment=False,
            download_name=(r.foto_filename or f"rest_{rest_id}.bin"),
        )
    abort(404)

# =========================
# Rota raiz
# =========================
@app.route("/")
def index():
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login"))
    u = Usuario.query.get(uid)
    if not u:
        return redirect(url_for("login"))
    if u.tipo == "admin":
        return redirect(url_for("admin_dashboard"))
    if u.tipo == "cooperado":
        return redirect(url_for("portal_cooperado"))
    if u.tipo == "restaurante":
        return redirect(url_for("portal_restaurante"))
    return redirect(url_for("login"))

# =========================
# Auth
# =========================
@app.route("/login", methods=["GET", "POST"])
def login():
    erro_login = None
    if request.method == "POST":
        usuario = request.form.get("usuario", "").strip()
        senha = request.form.get("senha", "")

        u = Usuario.query.filter_by(usuario=usuario).first()

        # fallback: aceitar login pelo nome do restaurante
        if not u:
            r = Restaurante.query.filter(Restaurante.nome.ilike(usuario)).first()
            if not r:
                r = Restaurante.query.filter(Restaurante.nome.ilike(f"%{usuario}%")).first()
            if r and r.usuario_ref:
                u = r.usuario_ref

        if u and u.check_password(senha):
            session["user_id"] = u.id
            session["user_tipo"] = u.tipo
            if u.tipo == "admin":
                return redirect(url_for("admin_dashboard"))
            elif u.tipo == "cooperado":
                return redirect(url_for("portal_cooperado"))
            elif u.tipo == "restaurante":
                return redirect(url_for("portal_restaurante"))
        erro_login = "Usuário/senha inválidos."
        flash(erro_login, "danger")

    login_tpl = os.path.join("templates", "login.html")
    if os.path.exists(login_tpl):
        return render_template("login.html", erro_login=erro_login)
    return """
    <form method="POST" style="max-width:320px;margin:80px auto;font-family:Arial">
      <h3>Login</h3>
      <input name="usuario" placeholder="Usuário" style="width:100%;padding:10px;margin:6px 0">
      <input name="senha" type="password" placeholder="Senha" style="width:100%;padding:10px;margin:6px 0">
      <button style="padding:10px 16px">Entrar</button>
    </form>
    """

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# =========================
# Admin Dashboard
# =========================
@app.route("/admin", methods=["GET"])
@admin_required
def admin_dashboard():
    args = request.args
    active_tab = args.get("tab", "lancamentos")  # <- NOVO: controla a aba ativa
    restaurante_id = args.get("restaurante_id", type=int)
    cooperado_id = args.get("cooperado_id", type=int)
    data_inicio = _parse_date(args.get("data_inicio"))
    data_fim = _parse_date(args.get("data_fim"))
    considerar_periodo = bool(args.get("considerar_periodo"))
    dows = set(args.getlist("dow"))  # {"1","2",...}

    # ---- Lançamentos (com filtros + DOW)
    q = Lancamento.query
    if restaurante_id:
        q = q.filter(Lancamento.restaurante_id == restaurante_id)
    if cooperado_id:
        q = q.filter(Lancamento.cooperado_id == cooperado_id)
    if data_inicio:
        q = q.filter(Lancamento.data >= data_inicio)
    if data_fim:
        q = q.filter(Lancamento.data <= data_fim)
    lanc_base = q.order_by(Lancamento.data.desc(), Lancamento.id.desc()).all()

    if dows:
        lancamentos = [l for l in lanc_base if l.data and _dow(l.data) in dows]
    else:
        lancamentos = lanc_base

    # Se marcar "considerar_periodo", só mantemos dias do período do restaurante
    if considerar_periodo and restaurante_id:
        rest = Restaurante.query.get(restaurante_id)
        if rest:
            mapa = {
                "seg-dom": {"1","2","3","4","5","6","7"},
                "sab-sex": {"6","7","1","2","3","4","5"},
                "sex-qui": {"5","6","7","1","2","3","4"},
            }
            permitidos = mapa.get(rest.periodo, {"1","2","3","4","5","6","7"})
            lancamentos = [l for l in lancamentos if l.data and _dow(l.data) in permitidos]

    total_producoes = sum((l.valor or 0.0) for l in lancamentos)
    total_inss = total_producoes * 0.045

    # ---- Coop (institucional)
    rq = ReceitaCooperativa.query
    dq = DespesaCooperativa.query
    if data_inicio:
        rq = rq.filter(ReceitaCooperativa.data >= data_inicio)
        dq = dq.filter(DespesaCooperativa.data >= data_inicio)
    if data_fim:
        rq = rq.filter(ReceitaCooperativa.data <= data_fim)
        dq = dq.filter(DespesaCooperativa.data <= data_fim)

    receitas = rq.order_by(ReceitaCooperativa.data.desc().nullslast(), ReceitaCooperativa.id.desc()).all()
    despesas = dq.order_by(DespesaCooperativa.data.desc(), DespesaCooperativa.id.desc()).all()
    total_receitas = sum((r.valor_total or 0.0) for r in receitas)
    total_despesas = sum((d.valor or 0.0) for d in despesas)

    # ---- Cooperados (pessoa física)
    rq2 = ReceitaCooperado.query
    dq2 = DespesaCooperado.query
    if data_inicio:
        rq2 = rq2.filter(ReceitaCooperado.data >= data_inicio)
        dq2 = dq2.filter(DespesaCooperado.data >= data_inicio)
    if data_fim:
        rq2 = rq2.filter(ReceitaCooperado.data <= data_fim)
        dq2 = dq2.filter(DespesaCooperado.data <= data_fim)
    receitas_coop = rq2.order_by(ReceitaCooperado.data.desc(), ReceitaCooperado.id.desc()).all()
    despesas_coop = dq2.order_by(DespesaCooperado.data.desc(), DespesaCooperado.id.desc()).all()
    total_receitas_coop = sum((r.valor or 0.0) for r in receitas_coop)
    total_despesas_coop = sum((d.valor or 0.0) for d in despesas_coop)

    cfg = get_config()
    historico_beneficios = BeneficioRegistro.query.order_by(BeneficioRegistro.id.desc()).all()
    cooperados = Cooperado.query.order_by(Cooperado.nome).all()
    restaurantes = Restaurante.query.order_by(Restaurante.nome).all()

    # documentos OK?
    docinfo_map = {c.id: _build_docinfo(c) for c in cooperados}
    status_doc_por_coop = {
        c.id: {"cnh_ok": docinfo_map[c.id]["cnh"]["ok"], "placa_ok": docinfo_map[c.id]["placa"]["ok"]}
        for c in cooperados
    }

    # -------- Escalas agrupadas e contagem por cooperado ----------
    escalas_all = Escala.query.order_by(Escala.id.asc()).all()

    esc_by_int: dict[int, list] = defaultdict(list)
    esc_by_str: dict[str, list] = defaultdict(list)
    for e in escalas_all:
        k_int = e.cooperado_id if e.cooperado_id is not None else 0  # 0 = sem cadastro
        esc_item = {
            "data": e.data, "turno": e.turno, "horario": e.horario,
            "contrato": e.contrato, "cor": e.cor, "nome_planilha": e.cooperado_nome
        }
        esc_by_int[k_int].append(esc_item)
        esc_by_str[str(k_int)].append(esc_item)

    cont_rows = dict(db.session.query(Escala.cooperado_id, func.count(Escala.id)).group_by(Escala.cooperado_id).all())
    qtd_escalas_map = {c.id: int(cont_rows.get(c.id, 0)) for c in cooperados}
    qtd_sem_cadastro = int(cont_rows.get(None, 0))

    # gráficos (por mês)
    sums = {}
    for l in lancamentos:
        if not l.data:
            continue
        key = l.data.strftime("%Y-%m")
        sums[key] = sums.get(key, 0.0) + (l.valor or 0.0)
    labels_ord = sorted(sums.keys())
    labels_fmt = [datetime.strptime(k, "%Y-%m").strftime("%m/%Y") for k in labels_ord]
    values = [round(sums[k], 2) for k in labels_ord]
    chart_data_lancamentos_coop = {"labels": labels_fmt, "values": values}
    chart_data_lancamentos_cooperados = chart_data_lancamentos_coop

    admin_user = Usuario.query.filter_by(tipo="admin").first()

    # ---- Folha (últimos 30 dias padrão)
    folha_inicio = _parse_date(args.get("folha_inicio")) or (date.today() - timedelta(days=30))
    folha_fim = _parse_date(args.get("folha_fim")) or date.today()
    FolhaItem = namedtuple("FolhaItem", "cooperado lancamentos receitas despesas bruto inss outras_desp liquido")
    folha_por_coop = []
    for c in cooperados:
        l = (Lancamento.query
             .filter(Lancamento.cooperado_id == c.id,
                     Lancamento.data >= folha_inicio,
                     Lancamento.data <= folha_fim)
             .order_by(Lancamento.data.asc(), Lancamento.id.asc())
             .all())
        r = (ReceitaCooperado.query
             .filter(ReceitaCooperado.cooperado_id == c.id,
                     ReceitaCooperado.data >= folha_inicio,
                     ReceitaCooperado.data <= folha_fim)
             .order_by(ReceitaCooperado.data.asc(), ReceitaCooperado.id.asc())
             .all())
        d = (DespesaCooperado.query
             .filter((DespesaCooperado.cooperado_id == c.id) | (DespesaCooperado.cooperado_id.is_(None)),
                     DespesaCooperado.data >= folha_inicio,
                     DespesaCooperado.data <= folha_fim)
             .order_by(DespesaCooperado.data.asc(), DespesaCooperado.id.asc())
             .all())
        bruto_lanc = sum(x.valor or 0 for x in l)
        inss = round(bruto_lanc * 0.045, 2)
        outras_desp = sum(x.valor or 0 for x in d)
        bruto_total = bruto_lanc + sum(x.valor or 0 for x in r)
        liquido = bruto_total - inss - outras_desp
        for x in l:
            x.conta_inss = True
            x.isento_benef = False
            x.inss = round((x.valor or 0) * 0.045, 2)

        folha_por_coop.append(FolhaItem(
            cooperado=c, lancamentos=l, receitas=r, despesas=d,
            bruto=bruto_total, inss=inss, outras_desp=outras_desp, liquido=liquido
        ))

    # Benefícios para template
    def _tokenize(s: str):
        return [x.strip() for x in re.split(r"[;,]", s or "") if x.strip()]

    historico_beneficios = BeneficioRegistro.query.order_by(BeneficioRegistro.id.desc()).all()
    beneficios_view = []
    for b in historico_beneficios:
        nomes = _tokenize(b.recebedores_nomes or "")
        ids = _tokenize(b.recebedores_ids or "")
        recs = []
        for i, nome in enumerate(nomes):
            rid = ids[i] if i < len(ids) else None
            try:
                rid = int(rid) if rid and str(rid).isdigit() else None
            except Exception:
                rid = None
            recs.append({"id": rid, "nome": nome})
        beneficios_view.append({
            "data_inicial": b.data_inicial,
            "data_final": b.data_final,
            "data_lancamento": b.data_lancamento,
            "tipo": b.tipo,
            "valor_total": b.valor_total or 0.0,
            "recebedores": recs,
        })

    # ======== Trocas no admin ========
    def _escala_desc(e: Escala | None) -> str:
        return _escala_label(e)

    def _split_turno_horario(s: str) -> tuple[str, str]:
        if not s:
            return "", ""
        parts = [p.strip() for p in s.split("•")]
        if len(parts) == 2:
            return parts[0], parts[1]
        return s.strip(), ""

    def _linha_from_escala(e: Escala, saiu: str, entrou: str) -> dict:
        return {
            "dia": _escala_label(e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(e.turno or "").strip(), (e.horario or "").strip()] if x]),
            "contrato": (e.contrato or "").strip(),
            "saiu": saiu,
            "entrou": entrou,
        }

    trocas_all = TrocaSolicitacao.query.order_by(TrocaSolicitacao.id.desc()).all()
    trocas_pendentes, trocas_historico = [], []
    trocas_historico_flat = []

    for t in trocas_all:
        solicitante = Cooperado.query.get(t.solicitante_id)
        destinatario = Cooperado.query.get(t.destino_id)
        orig = Escala.query.get(t.origem_escala_id)

        linhas_afetadas = _parse_linhas_from_msg(t.mensagem) if t.status == "aprovada" else []

        if t.status == "aprovada" and not linhas_afetadas and orig and solicitante and destinatario:
            linhas_afetadas.append(_linha_from_escala(
                orig, saiu=solicitante.nome, entrou=destinatario.nome
            ))
            wd_o = _weekday_from_data_str(orig.data)
            buck_o = _turno_bucket(orig.turno, orig.horario)
            candidatas = (Escala.query
                          .filter_by(cooperado_id=solicitante.id)
                          .all())
            best = None
            for e in candidatas:
                if _weekday_from_data_str(e.data) == wd_o and _turno_bucket(e.turno, e.horario) == buck_o:
                    if (orig.contrato or "").strip().lower() == (e.contrato or "").strip().lower():
                        best = e
                        break
                    if best is None:
                        best = e
            if best:
                linhas_afetadas.append(_linha_from_escala(
                    best, saiu=destinatario.nome, entrou=solicitante.nome
                ))

        item = {
            "id": t.id,
            "status": t.status,
            "mensagem": t.mensagem,
            "criada_em": t.criada_em,
            "aplicada_em": t.aplicada_em,
            "solicitante": solicitante,
            "destinatario": destinatario,
            "origem": orig,
            "destino": destinatario,
            "origem_desc": _escala_desc(orig),
            "origem_weekday": _weekday_from_data_str(orig.data) if orig else None,
            "origem_turno_bucket": _turno_bucket(orig.turno if orig else None, orig.horario if orig else None),
            "linhas_afetadas": linhas_afetadas,
        }

        if t.status == "aprovada" and linhas_afetadas:
            itens = []
            for r in linhas_afetadas:
                turno_txt, horario_txt = _split_turno_horario(r.get("turno_horario", ""))
                itens.append({
                    "data": r.get("dia", ""),
                    "turno": turno_txt,
                    "horario": horario_txt,
                    "contrato": r.get("contrato", ""),
                    "saiu_nome": r.get("saiu", ""),
                    "entrou_nome": r.get("entrou", ""),
                })
                trocas_historico_flat.append({
                    "data": r.get("dia", ""),
                    "turno": turno_txt,
                    "horario": horario_txt,
                    "contrato": r.get("contrato", ""),
                    "saiu_nome": r.get("saiu", ""),
                    "entrou_nome": r.get("entrou", ""),
                    "aplicada_em": t.aplicada_em,
                })
            item["itens"] = itens

        (trocas_pendentes if t.status == "pendente" else trocas_historico).append(item)

    current_date = date.today()
    data_limite = date(current_date.year, 12, 31)

    return render_template(
        "admin_dashboard.html",
        total_producoes=total_producoes,
        total_inss=total_inss,
        total_receitas=total_receitas,
        total_despesas=total_despesas,
        total_receitas_coop=total_receitas_coop,
        total_despesas_coop=total_despesas_coop,
        salario_minimo=cfg.salario_minimo or 0.0,
        lancamentos=lancamentos,
        receitas=receitas,
        despesas=despesas,
        receitas_coop=receitas_coop,
        despesas_coop=despesas_coop,
        cooperados=cooperados,
        restaurantes=restaurantes,
        beneficios_view=beneficios_view,
        historico_beneficios=historico_beneficios,
        current_date=current_date,
        data_limite=data_limite,
        admin=admin_user,
        docinfo_map=docinfo_map,
        escalas_por_coop=esc_by_int,
        escalas_por_coop_json=esc_by_str,
        qtd_escalas_map=qtd_escalas_map,
        qtd_escalas_sem_cadastro=qtd_sem_cadastro,
        status_doc_por_coop=status_doc_por_coop,
        chart_data_lancamentos_coop=chart_data_lancamentos_coop,
        chart_data_lancamentos_cooperados=chart_data_lancamentos_cooperados,
        folha_inicio=folha_inicio,
        folha_fim=folha_fim,
        folha_por_coop=folha_por_coop,
        trocas_pendentes=trocas_pendentes,
        trocas_historico=trocas_historico,
        trocas_historico_flat=trocas_historico_flat,
    )

# =========================
# Navegação/Export util
# =========================
@app.route("/filtrar_lancamentos")
@admin_required
def filtrar_lancamentos():
    qs = request.query_string.decode("utf-8")
    base = url_for("admin_dashboard")
    joiner = "&" if qs else ""
    return redirect(f"{base}?tab=lancamentos{joiner}{qs}")

@app.route("/exportar_lancamentos")
@admin_required
def exportar_lancamentos():
    args = request.args
    restaurante_id = args.get("restaurante_id", type=int)
    cooperado_id = args.get("cooperado_id", type=int)
    data_inicio = _parse_date(args.get("data_inicio"))
    data_fim = _parse_date(args.get("data_fim"))
    dows = set(args.getlist("dow"))

    q = Lancamento.query
    if restaurante_id:
        q = q.filter(Lancamento.restaurante_id == restaurante_id)
    if cooperado_id:
        q = q.filter(Lancamento.cooperado_id == cooperado_id)
    if data_inicio:
        q = q.filter(Lancamento.data >= data_inicio)
    if data_fim:
        q = q.filter(Lancamento.data <= data_fim)

    lancs = q.order_by(Lancamento.data.desc(), Lancamento.id.desc()).all()
    if dows:
        lancs = [l for l in lancs if l.data and _dow(l.data) in dows]

    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")
    w.writerow(["Restaurante", "Periodo", "Cooperado", "Descricao", "Valor", "Data", "HoraInicio", "HoraFim", "INSS", "Liquido"])
    for l in lancs:
        v = l.valor or 0.0
        inss = v * 0.045
        liq = v - inss
        w.writerow([
            l.restaurante.nome, l.restaurante.periodo, l.cooperado.nome, l.descricao,
            f"{v:.2f}", l.data.strftime("%d/%m/%Y") if l.data else "",
            l.hora_inicio or "", l.hora_fim or "", f"{inss:.2f}", f"{liq:.2f}"
        ])
    mem = io.BytesIO(buf.getvalue().encode("utf-8-sig"))
    return send_file(mem, as_attachment=True, download_name="lancamentos.csv", mimetype="text/csv")

# =========================
# CRUD Lançamentos (Admin)
# =========================
@app.route("/admin/lancamentos/add", methods=["POST"])
@admin_required
def admin_add_lancamento():
    f = request.form
    l = Lancamento(
        restaurante_id=f.get("restaurante_id", type=int),
        cooperado_id=f.get("cooperado_id", type=int),
        descricao=f.get("descricao", "").strip(),
        valor=f.get("valor", type=float),
        data=_parse_date(f.get("data")),
        hora_inicio=f.get("hora_inicio"),
        hora_fim=f.get("hora_fim"),
        qtd_entregas=f.get("qtd_entregas", type=int),
    )
    db.session.add(l)
    db.session.commit()
    flash("Lançamento inserido.", "success")
    return redirect(url_for("admin_dashboard", tab="lancamentos"))

@app.route("/admin/lancamentos/<int:id>/edit", methods=["POST"])
@admin_required
def admin_edit_lancamento(id):
    l = Lancamento.query.get_or_404(id)
    f = request.form
    l.restaurante_id = f.get("restaurante_id", type=int)
    l.cooperado_id = f.get("cooperado_id", type=int)
    l.descricao = f.get("descricao", "").strip()
    l.valor = f.get("valor", type=float)
    l.data = _parse_date(f.get("data"))
    l.hora_inicio = f.get("hora_inicio")
    l.hora_fim = f.get("hora_fim")
    l.qtd_entregas = f.get("qtd_entregas", type=int)
    db.session.commit()
    flash("Lançamento atualizado.", "success")
    return redirect(url_for("admin_dashboard", tab="lancamentos"))

@app.route("/admin/lancamentos/<int:id>/delete")
@admin_required
def admin_delete_lancamento(id):
    l = Lancamento.query.get_or_404(id)

    # 👇 LIMPEZA MANUAL: apaga avaliações amarradas a este lançamento
    db.session.execute(sa_delete(AvaliacaoCooperado).where(AvaliacaoCooperado.lancamento_id == id))
    db.session.execute(sa_delete(AvaliacaoRestaurante).where(AvaliacaoRestaurante.lancamento_id == id))

    db.session.delete(l)
    db.session.commit()
    flash("Lançamento excluído.", "success")
    return redirect(url_for("admin_dashboard", tab="lancamentos"))

# ===== IMPORTS =====
from flask import request, render_template, send_file, url_for
from sqlalchemy import func, literal, and_
from types import SimpleNamespace
import io, csv

# =========================
# /admin/avaliacoes — Lista + KPIs + Ranking (sempre na aba "Avaliações")
# =========================
@app.route("/admin/avaliacoes", methods=["GET"])
@admin_required
def admin_avaliacoes():
    # Tipo padrão: 'cooperado' (Rest. -> Coop.). Use ?tipo=restaurante p/ Cooperado -> Restaurante.
    tipo = (request.args.get("tipo", "cooperado") or "cooperado").strip().lower()

    restaurante_id = request.args.get("restaurante_id", type=int)
    cooperado_id   = request.args.get("cooperado_id", type=int)
    data_inicio    = request.args.get("data_inicio")  # 'YYYY-MM-DD'
    data_fim       = request.args.get("data_fim")     # 'YYYY-MM-DD'

    Model = AvaliacaoRestaurante if (tipo == "restaurante") else AvaliacaoCooperado

    # Helper de coluna com fallback (útil durante migração de schema)
    def col(*names):
        for n in names:
            if hasattr(Model, n):
                return getattr(Model, n)
        return None

    f_geral = col("estrelas_geral")

    # Campos por tipo
    if tipo == "restaurante":  # Cooperado -> Restaurante
        f_trat = col("estrelas_tratamento", "estrelas_pontualidade")
        f_amb  = col("estrelas_ambiente",   "estrelas_educacao")
        f_sup  = col("estrelas_suporte",    "estrelas_eficiencia")
    else:                      # Restaurante -> Cooperado
        f_pont  = col("estrelas_pontualidade")
        f_educ  = col("estrelas_educacao")
        f_efic  = col("estrelas_eficiencia")
        f_apres = col("estrelas_apresentacao")

    # ===== Query com nomes (sem precisar de relationship)
    base = (
        db.session.query(
            Model,
            Restaurante.id.label("rest_id"),
            Restaurante.nome.label("rest_nome"),
            Cooperado.id.label("coop_id"),
            Cooperado.nome.label("coop_nome"),
        )
        .join(Restaurante, Model.restaurante_id == Restaurante.id)
        .join(Cooperado,   Model.cooperado_id   == Cooperado.id)
    )

    # ----- Filtros (otimizados para usar índice) -----
    filtros = []
    if restaurante_id:
        filtros.append(Model.restaurante_id == restaurante_id)
    if cooperado_id:
        filtros.append(Model.cooperado_id == cooperado_id)

    # Datas como intervalo [>= di, < df+1dia] para usar índice de datetime
    di = datetime.strptime(data_inicio, "%Y-%m-%d") if data_inicio else None
    df = (datetime.strptime(data_fim, "%Y-%m-%d") + timedelta(days=1)) if data_fim else None

    if di:
        filtros.append(Model.criado_em >= di)
    if df:
        filtros.append(Model.criado_em < df)

    base_filtered = base.filter(and_(*filtros)) if filtros else base

    # ----- Paginação eficiente -----
    page = max(1, request.args.get("page", type=int) or 1)
    per_page = min(200, max(1, request.args.get("per_page", type=int) or 50))
    offset = (page - 1) * per_page

    # Ordena e pagina
    q_ordered = base_filtered.order_by(Model.criado_em.desc())
    rows = q_ordered.limit(per_page).offset(offset).all()

    # Total com os mesmos filtros
    cnt_q = db.session.query(func.count(Model.id))
    if filtros:
        cnt_q = cnt_q.filter(and_(*filtros))
    total = int(cnt_q.scalar() or 0)
    pages = max(1, (total + per_page - 1) // per_page)

    pager = SimpleNamespace(
        page=page,
        per_page=per_page,
        total=total,
        pages=pages,
        has_prev=(page > 1),
        has_next=(page < pages),
    )

    # ===== Achatar para o template
    avaliacoes = []
    for a, rest_id, rest_nome, coop_id, coop_nome in rows:
        item = {
            "criado_em": a.criado_em,
            "rest_id":   rest_id,
            "rest_nome": rest_nome,
            "coop_id":   coop_id,
            "coop_nome": coop_nome,
            "geral":     getattr(a, "estrelas_geral", 0) or 0,
            "comentario": (getattr(a, "comentario", "") or "").strip(),
            "media":       getattr(a, "media_ponderada", None),
            "sentimento":  getattr(a, "sentimento", None),
            "temas":       getattr(a, "temas", None),
            "alerta":      bool(getattr(a, "alerta_crise", False)),
        }

        if tipo == "restaurante":
            trat = getattr(a, "estrelas_tratamento", None) or getattr(a, "estrelas_pontualidade", None)
            amb  = getattr(a, "estrelas_ambiente",   None) or getattr(a, "estrelas_educacao", None)
            sup  = getattr(a, "estrelas_suporte",    None) or getattr(a, "estrelas_eficiencia", None)
            item.update({"trat": trat or 0, "amb": amb or 0, "sup": sup or 0})
        else:
            item.update({
                "pont":  getattr(a, "estrelas_pontualidade", 0) or 0,
                "educ":  getattr(a, "estrelas_educacao", 0) or 0,
                "efic":  getattr(a, "estrelas_eficiencia", 0) or 0,
                "apres": getattr(a, "estrelas_apresentacao", 0) or 0,
            })

        avaliacoes.append(SimpleNamespace(**item))

    # ===== KPIs
    def avg_or_zero(coluna):
        if coluna is None:
            return 0.0
        q = db.session.query(func.coalesce(func.avg(coluna), 0.0))
        if filtros:
            q = q.filter(and_(*filtros))
        return float(q.scalar() or 0.0)

    kpis = {"qtd": total, "geral": avg_or_zero(f_geral)}

    if tipo == "restaurante":
        kpis.update({
            "trat": avg_or_zero(f_trat),
            "amb":  avg_or_zero(f_amb),
            "sup":  avg_or_zero(f_sup),
        })
    else:
        kpis.update({
            "pont":  avg_or_zero(f_pont),
            "educ":  avg_or_zero(f_educ),
            "efic":  avg_or_zero(f_efic),
            "apres": avg_or_zero(f_apres),
        })

    # ===== Ranking
    if tipo == "restaurante":
        q_rank = (
            db.session.query(
                Restaurante.id.label("id"),
                Restaurante.nome.label("nome"),
                func.count(Model.id).label("qtd"),
                func.coalesce(func.avg(f_geral), 0.0).label("m_geral"),
                (func.coalesce(func.avg(f_trat), 0.0) if f_trat is not None else literal(0.0)).label("m_trat"),
                (func.coalesce(func.avg(f_amb),  0.0) if f_amb  is not None else literal(0.0)).label("m_amb"),
                (func.coalesce(func.avg(f_sup),  0.0) if f_sup  is not None else literal(0.0)).label("m_sup"),
            )
            .join(Model, Model.restaurante_id == Restaurante.id)
        )
        if filtros:
            q_rank = q_rank.filter(and_(*filtros))
        ranking_rows = q_rank.group_by(Restaurante.id, Restaurante.nome).all()
        ranking = [{
            "rest_nome": r.nome, "qtd": int(r.qtd or 0),
            "m_geral": float(r.m_geral or 0),
            "m_trat":  float(r.m_trat or 0),
            "m_amb":   float(r.m_amb or 0),
            "m_sup":   float(r.m_sup or 0),
        } for r in ranking_rows]
        top = sorted([x for x in ranking if x["qtd"] >= 3], key=lambda x: x["m_geral"], reverse=True)[:10]
        chart_top = {"labels": [r["rest_nome"] for r in top], "values": [round(r["m_geral"], 2) for r in top]}
    else:
        q_rank = (
            db.session.query(
                Cooperado.id.label("id"),
                Cooperado.nome.label("nome"),
                func.count(Model.id).label("qtd"),
                func.coalesce(func.avg(f_geral), 0.0).label("m_geral"),
                (func.coalesce(func.avg(f_pont), 0.0) if f_pont is not None else literal(0.0)).label("m_pont"),
                (func.coalesce(func.avg(f_educ), 0.0) if f_educ is not None else literal(0.0)).label("m_educ"),
                (func.coalesce(func.avg(f_efic), 0.0) if f_efic is not None else literal(0.0)).label("m_efic"),
                (func.coalesce(func.avg(f_apres),0.0) if f_apres is not None else literal(0.0)).label("m_apres"),
            )
            .join(Model, Model.cooperado_id == Cooperado.id)
        )
        if filtros:
            q_rank = q_rank.filter(and_(*filtros))
        ranking_rows = q_rank.group_by(Cooperado.id, Cooperado.nome).all()
        ranking = [{
            "coop_nome": r.nome, "qtd": int(r.qtd or 0),
            "m_geral": float(r.m_geral or 0),
            "m_pont":  float(r.m_pont or 0),
            "m_educ":  float(r.m_educ or 0),
            "m_efic":  float(r.m_efic or 0),
            "m_apres": float(r.m_apres or 0),
        } for r in ranking_rows]
        top = sorted([x for x in ranking if x["qtd"] >= 3], key=lambda x: x["m_geral"], reverse=True)[:10]
        chart_top = {"labels": [r["coop_nome"] for r in top], "values": [round(r["m_geral"], 2) for r in top]}

    # ===== Compatibilidade Cooperado × Restaurante
    compat_map = {}
    for a in avaliacoes:
        key = (a.coop_id, a.rest_id)
        d = compat_map.get(key)
        if not d:
            d = {"coop": a.coop_nome, "rest": a.rest_nome, "sum": 0.0, "count": 0}
        d["sum"] += (a.geral or 0)
        d["count"] += 1
        compat_map[key] = d
    compat = []
    for d in compat_map.values():
        avg = (d["sum"] / d["count"]) if d["count"] else 0.0
        compat.append({"coop": d["coop"], "rest": d["rest"], "avg": avg, "count": d["count"]})
    compat.sort(key=lambda x: (-(x["avg"] or 0), -(x["count"] or 0), x["coop"], x["rest"]))

    # Filtros p/ repopular o form
    _flt = SimpleNamespace(
        restaurante_id=restaurante_id,
        cooperado_id=cooperado_id,
        data_inicio=data_inicio or "",
        data_fim=data_fim or "",
    )

    # Preservar filtros para a paginação
    preserve = request.args.to_dict(flat=True)
    preserve.pop("page", None)

    # Renderiza o dashboard com a aba "avaliacoes" ativa
    return render_template(
        "admin_dashboard.html",
        tab="avaliacoes",
        tipo=tipo,
        avaliacoes=avaliacoes,
        kpis=kpis,
        ranking=ranking,
        chart_top=chart_top,
        compat=compat,
        _flt=_flt,
        restaurantes=Restaurante.query.order_by(Restaurante.nome).all(),
        cooperados=Cooperado.query.order_by(Cooperado.nome).all(),
        pager=pager,
        page=pager.page,
        per_page=pager.per_page,
        preserve=preserve,
    )

    # ===== Achata nos nomes que o SEU TEMPLATE usa
    avaliacoes = []
    for a, rest_id, rest_nome, coop_id, coop_nome in rows:
        # comuns
        item = {
            "criado_em": a.criado_em,
            "rest_id":   rest_id,
            "rest_nome": rest_nome,
            "coop_id":   coop_id,
            "coop_nome": coop_nome,
            "geral":     getattr(a, "estrelas_geral", 0) or 0,
            "comentario": (getattr(a, "comentario", "") or "").strip(),
            "media":       getattr(a, "media_ponderada", None),
            "sentimento":  getattr(a, "sentimento", None),
            "temas":       getattr(a, "temas", None),
            "alerta":      bool(getattr(a, "alerta_crise", False)),
        }

        if tipo == "restaurante":
            # Cooperado -> Restaurante: Trat/Amb/Sup (com fallbacks)
            trat = getattr(a, "estrelas_tratamento", None)
            amb  = getattr(a, "estrelas_ambiente", None)
            sup  = getattr(a, "estrelas_suporte", None)
            if trat is None: trat = getattr(a, "estrelas_pontualidade", None)
            if amb  is None: amb  = getattr(a, "estrelas_educacao", None)
            if sup  is None: sup  = getattr(a, "estrelas_eficiencia", None)
            item.update({"trat": trat or 0, "amb": amb or 0, "sup": sup or 0})
        else:
            # Restaurante -> Cooperado
            item.update({
                "pont":  getattr(a, "estrelas_pontualidade", 0) or 0,
                "educ":  getattr(a, "estrelas_educacao", 0) or 0,
                "efic":  getattr(a, "estrelas_eficiencia", 0) or 0,
                "apres": getattr(a, "estrelas_apresentacao", 0) or 0,
            })

        avaliacoes.append(SimpleNamespace(**item))

    # ===== KPIs
    def avg_or_zero(coluna):
        if coluna is None:
            return 0.0
        q = db.session.query(func.coalesce(func.avg(coluna), 0.0))
        if filtros: q = q.filter(and_(*filtros))
        return float(q.scalar() or 0.0)

    total_qtd = (db.session.query(func.count(Model.id)).filter(and_(*filtros)).scalar()
                 if filtros else db.session.query(func.count(Model.id)).scalar())
    kpis = {"qtd": int(total_qtd or 0), "geral": avg_or_zero(f_geral)}

    if tipo == "restaurante":
        kpis.update({
            "trat": avg_or_zero(f_trat),
            "amb":  avg_or_zero(f_amb),
            "sup":  avg_or_zero(f_sup),
        })
    else:
        kpis.update({
            "pont":  avg_or_zero(f_pont),
            "educ":  avg_or_zero(f_educ),
            "efic":  avg_or_zero(f_efic),
            "apres": avg_or_zero(f_apres),
        })

    # ===== Ranking
    if tipo == "restaurante":
        q_rank = (db.session.query(
                    Restaurante.id.label("id"),
                    Restaurante.nome.label("nome"),
                    func.count(Model.id).label("qtd"),
                    func.coalesce(func.avg(f_geral), 0.0).label("m_geral"),
                    (func.coalesce(func.avg(f_trat), 0.0) if f_trat is not None else literal(0.0)).label("m_trat"),
                    (func.coalesce(func.avg(f_amb),  0.0) if f_amb  is not None else literal(0.0)).label("m_amb"),
                    (func.coalesce(func.avg(f_sup),  0.0) if f_sup  is not None else literal(0.0)).label("m_sup"),
                 )
                 .join(Model, Model.restaurante_id == Restaurante.id))
        if filtros: q_rank = q_rank.filter(and_(*filtros))
        ranking_rows = q_rank.group_by(Restaurante.id, Restaurante.nome).all()
        ranking = [{
            "rest_nome": r.nome, "qtd": int(r.qtd or 0),
            "m_geral": float(r.m_geral or 0),
            "m_trat":  float(r.m_trat or 0),
            "m_amb":   float(r.m_amb or 0),
            "m_sup":   float(r.m_sup or 0),
        } for r in ranking_rows]
        top = sorted([x for x in ranking if x["qtd"] >= 3], key=lambda x: x["m_geral"], reverse=True)[:10]
        chart_top = {"labels": [r["rest_nome"] for r in top], "values": [round(r["m_geral"], 2) for r in top]}
    else:
        q_rank = (db.session.query(
                    Cooperado.id.label("id"),
                    Cooperado.nome.label("nome"),
                    func.count(Model.id).label("qtd"),
                    func.coalesce(func.avg(f_geral), 0.0).label("m_geral"),
                    (func.coalesce(func.avg(f_pont), 0.0) if f_pont is not None else literal(0.0)).label("m_pont"),
                    (func.coalesce(func.avg(f_educ), 0.0) if f_educ is not None else literal(0.0)).label("m_educ"),
                    (func.coalesce(func.avg(f_efic), 0.0) if f_efic is not None else literal(0.0)).label("m_efic"),
                    (func.coalesce(func.avg(f_apres),0.0) if f_apres is not None else literal(0.0)).label("m_apres"),
                 )
                 .join(Model, Model.cooperado_id == Cooperado.id))
        if filtros: q_rank = q_rank.filter(and_(*filtros))
        ranking_rows = q_rank.group_by(Cooperado.id, Cooperado.nome).all()
        ranking = [{
            "coop_nome": r.nome, "qtd": int(r.qtd or 0),
            "m_geral": float(r.m_geral or 0),
            "m_pont":  float(r.m_pont or 0),
            "m_educ":  float(r.m_educ or 0),
            "m_efic":  float(r.m_efic or 0),
            "m_apres": float(r.m_apres or 0),
        } for r in ranking_rows]
        top = sorted([x for x in ranking if x["qtd"] >= 3], key=lambda x: x["m_geral"], reverse=True)[:10]
        chart_top = {"labels": [r["coop_nome"] for r in top], "values": [round(r["m_geral"], 2) for r in top]}

    # ===== Compatibilidade Cooperado × Restaurante (média de "geral" por par)
    compat_map = {}
    for a in avaliacoes:
        key = (a.coop_id, a.rest_id)
        d = compat_map.get(key)
        if not d:
            d = {"coop": a.coop_nome, "rest": a.rest_nome, "sum": 0.0, "count": 0}
        d["sum"] += (a.geral or 0)
        d["count"] += 1
        compat_map[key] = d
    compat = []
    for d in compat_map.values():
        avg = (d["sum"] / d["count"]) if d["count"] else 0.0
        compat.append({"coop": d["coop"], "rest": d["rest"], "avg": avg, "count": d["count"]})
    # pré-ordena por média desc e, em caso de empate, por qtd desc
    compat.sort(key=lambda x: (-(x["avg"] or 0), -(x["count"] or 0), x["coop"], x["rest"]))

    # Filtros p/ repopular o form
    _flt = SimpleNamespace(
        restaurante_id=restaurante_id,
        cooperado_id=cooperado_id,
        data_inicio=data_inicio or "",
        data_fim=data_fim or "",
    )

    # >>> IMPORTANTÍSSIMO: renderiza o DASHBOARD com a aba "avaliacoes" ativa
    return render_template(
        "admin_dashboard.html",
        tab="avaliacoes",        # <- garante que a UI fique na aba Avaliações
        tipo=tipo,
        avaliacoes=avaliacoes,   # lista de SimpleNamespace com campos *planos* esperados no seu template
        kpis=kpis,
        ranking=ranking,
        chart_top=chart_top,
        compat=compat,
        _flt=_flt,
        restaurantes=Restaurante.query.order_by(Restaurante.nome).all(),
        cooperados=Cooperado.query.order_by(Cooperado.nome).all(),
    )

# =========================
# /admin/avaliacoes/export.csv — Exportação CSV (combina com o template)
# =========================
@app.route("/admin/avaliacoes/export.csv", methods=["GET"])
@admin_required
def admin_export_avaliacoes_csv():
    tipo = (request.args.get("tipo", "restaurante") or "restaurante").strip().lower()
    restaurante_id = request.args.get("restaurante_id", type=int)
    cooperado_id   = request.args.get("cooperado_id", type=int)
    data_inicio    = request.args.get("data_inicio")
    data_fim       = request.args.get("data_fim")

    Model = AvaliacaoRestaurante if (tipo == "restaurante") else AvaliacaoCooperado

    base = (db.session.query(
                Model,
                Restaurante.nome.label("rest_nome"),
                Cooperado.nome.label("coop_nome"))
            .join(Restaurante, Model.restaurante_id == Restaurante.id)
            .join(Cooperado,   Model.cooperado_id   == Cooperado.id))

    filtros = []
    if restaurante_id: filtros.append(Model.restaurante_id == restaurante_id)
    if cooperado_id:   filtros.append(Model.cooperado_id   == cooperado_id)
    if data_inicio:    filtros.append(func.date(Model.criado_em) >= data_inicio)
    if data_fim:       filtros.append(func.date(Model.criado_em) <= data_fim)
    if filtros:        base = base.filter(and_(*filtros))

    rows = base.order_by(Model.criado_em.desc()).all()

    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";")

    if tipo == "restaurante":
        # Cooperado -> Restaurante
        w.writerow(["Data/Hora","Restaurante","Cooperado","Geral","Tratamento","Ambiente","Suporte",
                    "Comentário","Média Ponderada","Sentimento","Temas","Crítico?"])
        for a, rest_nome, coop_nome in rows:
            trat = getattr(a, "estrelas_tratamento", None) or getattr(a, "estrelas_pontualidade", None)
            amb  = getattr(a, "estrelas_ambiente",   None) or getattr(a, "estrelas_educacao", None)
            sup  = getattr(a, "estrelas_suporte",    None) or getattr(a, "estrelas_eficiencia", None)
            w.writerow([
                a.criado_em.strftime("%d/%m/%Y %H:%M") if a.criado_em else "",
                rest_nome, coop_nome,
                a.estrelas_geral or 0,
                (trat or 0), (amb or 0), (sup or 0),
                (getattr(a, "comentario", "") or "").strip(),
                (getattr(a, "media_ponderada", "") or ""),
                (getattr(a, "sentimento", "") or ""),
                (getattr(a, "temas", "") or ""),
                "SIM" if getattr(a, "alerta_crise", False) else "NÃO",
            ])
    else:
        # Restaurante -> Cooperado
        w.writerow(["Data/Hora","Restaurante","Cooperado","Geral","Pontualidade","Educação","Eficiência","Apresentação",
                    "Comentário","Média Ponderada","Sentimento","Temas","Crítico?"])
        for a, rest_nome, coop_nome in rows:
            w.writerow([
                a.criado_em.strftime("%d/%m/%Y %H:%M") if a.criado_em else "",
                rest_nome, coop_nome,
                a.estrelas_geral or 0,
                (getattr(a, "estrelas_pontualidade", 0) or 0),
                (getattr(a, "estrelas_educacao", 0) or 0),
                (getattr(a, "estrelas_eficiencia", 0) or 0),
                (getattr(a, "estrelas_apresentacao", 0) or 0),
                (getattr(a, "comentario", "") or "").strip(),
                (getattr(a, "media_ponderada", "") or ""),
                (getattr(a, "sentimento", "") or ""),
                (getattr(a, "temas", "") or ""),
                "SIM" if getattr(a, "alerta_crise", False) else "NÃO",
            ])

    mem = io.BytesIO(buf.getvalue().encode("utf-8-sig"))
    return send_file(mem, as_attachment=True,
                     download_name=f"avaliacoes_{tipo}.csv",
                     mimetype="text/csv")

# =========================
# CRUD Receitas/Despesas Coop (Admin)
# =========================
@app.route("/receitas/add", methods=["POST"])
@admin_required
def add_receita():
    f = request.form
    r = ReceitaCooperativa(
        descricao=f.get("descricao", "").strip(),
        valor_total=f.get("valor", type=float),
        data=_parse_date(f.get("data"))
    )
    db.session.add(r)
    db.session.commit()
    flash("Receita adicionada.", "success")
    return redirect(url_for("admin_dashboard", tab="receitas"))

@app.route("/receitas/<int:id>/edit", methods=["POST"])
@admin_required
def edit_receita(id):
    r = ReceitaCooperativa.query.get_or_404(id)
    f = request.form
    r.descricao = f.get("descricao", "").strip()
    # CORREÇÃO: campo correto é valor_total
    r.valor_total = f.get("valor", type=float)
    r.data = _parse_date(f.get("data"))
    db.session.commit()
    flash("Receita atualizada.", "success")
    return redirect(url_for("admin_dashboard", tab="receitas"))

@app.route("/receitas/<int:id>/delete")
@admin_required
def delete_receita(id):
    r = ReceitaCooperativa.query.get_or_404(id)
    db.session.delete(r)
    db.session.commit()
    flash("Receita excluída.", "success")
    return redirect(url_for("admin_dashboard", tab="receitas"))

@app.route("/despesas/add", methods=["POST"])
@admin_required
def add_despesa():
    f = request.form
    d = DespesaCooperativa(
        descricao=f.get("descricao", "").strip(),
        valor=f.get("valor", type=float),
        data=_parse_date(f.get("data"))
    )
    db.session.add(d)
    db.session.commit()
    flash("Despesa adicionada.", "success")
    return redirect(url_for("admin_dashboard", tab="despesas"))

@app.route("/despesas/<int:id>/edit", methods=["POST"])
@admin_required
def edit_despesa(id):
    d = DespesaCooperativa.query.get_or_404(id)
    f = request.form
    d.descricao = f.get("descricao", "").strip()
    d.valor = f.get("valor", type=float)
    d.data = _parse_date(f.get("data"))
    db.session.commit()
    flash("Despesa atualizada.", "success")
    return redirect(url_for("admin_dashboard", tab="despesas"))

@app.route("/despesas/<int:id>/delete")
@admin_required
def delete_despesa(id):
    d = DespesaCooperativa.query.get_or_404(id)
    db.session.delete(d)
    db.session.commit()
    flash("Despesa excluída.", "success")
    return redirect(url_for("admin_dashboard", tab="despesas"))

# =========================
# Avisos (admin + públicos)
# =========================
import re
from datetime import datetime, time
from flask import request, session, render_template, redirect, url_for, flash

@app.get("/avisos", endpoint="avisos_publicos")
def avisos_publicos():
    t = session.get("user_tipo")
    if t == "cooperado":
        return redirect(url_for("portal_cooperado_avisos"))
    if t == "restaurante":
        return redirect(url_for("portal_restaurante"))
    return redirect(url_for("login"))


@app.route("/admin/avisos", methods=["GET", "POST"])
@admin_required
def admin_avisos():
    cooperados = Cooperado.query.order_by(Cooperado.nome.asc()).all()
    restaurantes = Restaurante.query.order_by(Restaurante.nome.asc()).all()

    if request.method == "POST":
        f = request.form

        # ===== Alcance/destino vindos do form =====
        destino_tipo = (f.get("destino_tipo") or "").strip()  # 'cooperados' | 'restaurantes' | 'ambos'
        coop_alc = f.get("coop_alcance") or f.get("coop_alcance_ambos")   # 'todos' | 'selecionados'
        rest_alc = f.get("rest_alcance") or f.get("rest_alcance_ambos")   # 'todos' | 'selecionados'
        sel_coops = request.form.getlist("dest_cooperados[]") or request.form.getlist("dest_cooperados_ambos[]")
        sel_rests = request.form.getlist("dest_restaurantes[]") or request.form.getlist("dest_restaurantes_ambos[]")

        # ===== Conteúdo =====
        def _pick_msg(form):
            for key in (
                "corpo_html", "html", "mensagem_html", "conteudo_html", "descricao_html", "texto_html",
                "mensagem", "corpo", "conteudo", "descricao", "texto", "resumo", "body", "content"
            ):
                v = form.get(key)
                if v and v.strip():
                    return v.strip()
            return ""

        titulo = (f.get("titulo") or "").strip()
        msg    = _pick_msg(f)
        prioridade = (f.get("prioridade") or "normal")
        ativo = bool(f.get("ativo"))
        exigir_confirmacao = bool(f.get("exigir_confirmacao")) if hasattr(Aviso, "exigir_confirmacao") else False

        inicio_em = (lambda d=_parse_date(f.get("inicio_em")): datetime.combine(d, time()) if d else None)()
        fim_em    = (lambda d=_parse_date(f.get("fim_em")):    datetime.combine(d, time()) if d else None)()

        def _mk_aviso(tipo: str):
            a = Aviso(
                titulo=titulo,
                corpo=msg,
                tipo=tipo,  # 'global' | 'cooperado' | 'restaurante'
                prioridade=prioridade,
                fixado=False,
                ativo=ativo if hasattr(Aviso, "ativo") else True,
                criado_por_id=session.get("user_id"),
                inicio_em=inicio_em,
                fim_em=fim_em,
            )
            if hasattr(a, "exigir_confirmacao"):
                a.exigir_confirmacao = exigir_confirmacao
            return a

        # ===== Regras =====
        avisos_para_criar = []

        if destino_tipo == "cooperados":
            # só cooperados
            if coop_alc == "selecionados":
                if not sel_coops:
                    flash("Selecione ao menos um cooperado.", "warning")
                    return redirect(url_for("admin_avisos"))
                try:
                    coop_id = int(sel_coops[0])  # modelo atual aceita 1 cooperado
                except Exception:
                    flash("Seleção de cooperado inválida.", "warning")
                    return redirect(url_for("admin_avisos"))
                a = _mk_aviso("cooperado")
                a.destino_cooperado_id = coop_id
                avisos_para_criar.append(a)
            else:
                # todos cooperados = broadcast (destino_cooperado_id NULL)
                a = _mk_aviso("cooperado")
                a.destino_cooperado_id = None
                avisos_para_criar.append(a)

        elif destino_tipo == "restaurantes":
            # só restaurantes
            if rest_alc == "selecionados":
                if not sel_rests:
                    flash("Selecione ao menos um restaurante.", "warning")
                    return redirect(url_for("admin_avisos"))
                try:
                    ids = [int(x) for x in sel_rests]
                except Exception:
                    flash("Seleção de restaurante inválida.", "warning")
                    return redirect(url_for("admin_avisos"))
                a = _mk_aviso("restaurante")
                a.restaurantes = Restaurante.query.filter(Restaurante.id.in_(ids)).all()
                avisos_para_criar.append(a)
            else:
                # todos restaurantes = broadcast (lista vazia)
                a = _mk_aviso("restaurante")
                a.restaurantes = []
                avisos_para_criar.append(a)

        elif destino_tipo == "ambos":
            # cria DOIS avisos: um para cooperados e outro para restaurantes

            # Cooperados
            if coop_alc == "selecionados":
                if not sel_coops:
                    flash("Selecione ao menos um cooperado para o aviso dos cooperados.", "warning")
                    return redirect(url_for("admin_avisos"))
                try:
                    coop_id = int(sel_coops[0])
                except Exception:
                    flash("Seleção de cooperado inválida.", "warning")
                    return redirect(url_for("admin_avisos"))
                a_coop = _mk_aviso("cooperado")
                a_coop.destino_cooperado_id = coop_id
            else:
                a_coop = _mk_aviso("cooperado")
                a_coop.destino_cooperado_id = None  # broadcast cooperados
            avisos_para_criar.append(a_coop)

            # Restaurantes
            if rest_alc == "selecionados":
                if not sel_rests:
                    flash("Selecione ao menos um restaurante para o aviso dos restaurantes.", "warning")
                    return redirect(url_for("admin_avisos"))
                try:
                    ids = [int(x) for x in sel_rests]
                except Exception:
                    flash("Seleção de restaurante inválida.", "warning")
                    return redirect(url_for("admin_avisos"))
                a_rest = _mk_aviso("restaurante")
                a_rest.restaurantes = Restaurante.query.filter(Restaurante.id.in_(ids)).all()
            else:
                a_rest = _mk_aviso("restaurante")
                a_rest.restaurantes = []  # broadcast restaurantes
            avisos_para_criar.append(a_rest)

        else:
            # fallback antigo: se nada veio, publica global
            avisos_para_criar.append(_mk_aviso("global"))

        for a in avisos_para_criar:
            db.session.add(a)
        db.session.commit()

        flash("Aviso(s) publicado(s).", "success")
        return redirect(url_for("admin_avisos"))

    # GET
    avisos = Aviso.query.order_by(Aviso.fixado.desc(), Aviso.criado_em.desc()).all()
    return render_template(
        "admin_avisos.html",
        avisos=avisos,
        cooperados=cooperados,
        restaurantes=restaurantes,
    )


# --------- ROTAS QUE FALTAVAM (usadas no template) ---------

# Toggle VISIBILIDADE/FIXAÇÃO (aceita GET e POST)
@app.route("/admin/avisos/<int:aviso_id>/toggle", methods=["POST", "GET"], endpoint="admin_avisos_toggle")
@admin_required
def admin_avisos_toggle(aviso_id):
    a = Aviso.query.get_or_404(aviso_id)
    if hasattr(a, "ativo"):
        a.ativo = not bool(a.ativo)
    else:
        a.fixado = not bool(a.fixado)
    db.session.commit()
    flash("Aviso atualizado.", "success")
    return redirect(request.referrer or url_for("admin_avisos"))


# Excluir aviso (limpando relações)
@app.route("/admin/avisos/<int:aviso_id>/excluir", methods=["POST"], endpoint="admin_avisos_excluir")
@admin_required
def admin_avisos_excluir(aviso_id):
    a = Aviso.query.get_or_404(aviso_id)

    # apaga confirmações/leitorias
    try:
        AvisoLeitura.query.filter_by(aviso_id=aviso_id).delete(synchronize_session=False)
    except Exception:
        pass

    # limpa M2M com restaurantes, se existir
    try:
        if hasattr(a, "restaurantes"):
            a.restaurantes.clear()
    except Exception:
        pass

    db.session.delete(a)
    db.session.commit()
    flash("Aviso excluído.", "success")
    return redirect(url_for("admin_avisos"))


# Marcar aviso como lido (funciona com GET e POST)
@app.route("/avisos/<int:aviso_id>/lido", methods=["POST", "GET"])
def marcar_aviso_lido_universal(aviso_id: int):
    # se não logado, bloqueia
    if "user_id" not in session:
        return redirect(url_for("login")) if request.method == "GET" else ("", 401)

    user_id = session.get("user_id")
    user_tipo = session.get("user_tipo")
    Aviso.query.get_or_404(aviso_id)

    def _ok_response():
        if request.method == "POST":
            return ("", 204)  # útil para fetch/AJAX
        return redirect(request.referrer or url_for("portal_cooperado_avisos"))

    if user_tipo == "cooperado":
        coop = Cooperado.query.filter_by(usuario_id=user_id).first()
        if not coop:
            return ("", 403) if request.method == "POST" else redirect(url_for("login"))
        if not AvisoLeitura.query.filter_by(aviso_id=aviso_id, cooperado_id=coop.id).first():
            db.session.add(AvisoLeitura(aviso_id=aviso_id, cooperado_id=coop.id, lido_em=datetime.utcnow()))
            db.session.commit()
        return _ok_response()

    if user_tipo == "restaurante":
        rest = Restaurante.query.filter_by(usuario_id=user_id).first()
        if not rest:
            return ("", 403) if request.method == "POST" else redirect(url_for("login"))
        if not AvisoLeitura.query.filter_by(aviso_id=aviso_id, restaurante_id=rest.id).first():
            db.session.add(AvisoLeitura(aviso_id=aviso_id, restaurante_id=rest.id, lido_em=datetime.utcnow()))
            db.session.commit()
        return _ok_response()

    return ("", 403) if request.method == "POST" else redirect(url_for("login"))

# =========================
# CRUD Cooperados / Restaurantes / Senhas (Admin)
# =========================
@app.route("/cooperados/add", methods=["POST"])
@admin_required
def add_cooperado():
    f = request.form
    nome = f.get("nome", "").strip()
    usuario_login = f.get("usuario", "").strip()
    senha = f.get("senha", "")
    foto = request.files.get("foto")

    if Usuario.query.filter_by(usuario=usuario_login).first():
        flash("Usuário já existente.", "warning")
        return redirect(url_for("admin_dashboard", tab="cooperados"))

    u = Usuario(usuario=usuario_login, tipo="cooperado", senha_hash="")
    u.set_password(senha)
    db.session.add(u)
    db.session.flush()

    c = Cooperado(nome=nome, usuario_id=u.id, ultima_atualizacao=datetime.now())
    db.session.add(c)
    db.session.flush()  # garante c.id

    if foto and foto.filename:
        _save_foto_to_db(c, foto, is_cooperado=True)

    db.session.commit()
    flash("Cooperado cadastrado.", "success")
    return redirect(url_for("admin_dashboard", tab="cooperados"))

@app.route("/cooperados/<int:id>/edit", methods=["POST"])
@admin_required
def edit_cooperado(id):
    c = Cooperado.query.get_or_404(id)
    f = request.form
    c.nome = f.get("nome", "").strip()
    c.usuario_ref.usuario = f.get("usuario", "").strip()
    foto = request.files.get("foto")
    if foto and foto.filename:
        _save_foto_to_db(c, foto, is_cooperado=True)
    c.ultima_atualizacao = datetime.now()
    db.session.commit()
    flash("Cooperado atualizado.", "success")
    return redirect(url_for("admin_dashboard", tab="cooperados"))

from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError

@app.route("/cooperados/<int:id>/delete", methods=["POST"])
@admin_required
def delete_cooperado(id):
    c = Cooperado.query.get_or_404(id)
    u = c.usuario_ref

    try:
        # --- 1) Escalas do cooperado (e trocas ligadas a essas escalas) ---
        escala_ids = [eid for (eid,) in db.session.query(Escala.id)
                      .filter(Escala.cooperado_id == id).all()]

        if escala_ids:
            # Trocas que apontam para essas escalas
            db.session.execute(
                sa_delete(TrocaSolicitacao)
                .where(TrocaSolicitacao.origem_escala_id.in_(escala_ids))
            )
            # As próprias escalas
            db.session.execute(
                sa_delete(Escala)
                .where(Escala.id.in_(escala_ids))
            )

        # --- 2) Trocas onde o cooperado é solicitante ou destino ---
        db.session.execute(
            sa_delete(TrocaSolicitacao)
            .where(or_(
                TrocaSolicitacao.solicitante_id == id,
                TrocaSolicitacao.destino_id == id
            ))
        )

        # --- 3) Avaliações que guardam o cooperado_id ---
        db.session.execute(
            sa_delete(AvaliacaoCooperado).where(AvaliacaoCooperado.cooperado_id == id)
        )
        db.session.execute(
            sa_delete(AvaliacaoRestaurante).where(AvaliacaoRestaurante.cooperado_id == id)
        )

        # --- 4) Lançamentos desse cooperado (se o CASCADE por lancamento_id não estiver ativo) ---
        db.session.execute(
            sa_delete(Lancamento).where(Lancamento.cooperado_id == id)
        )

        # --- 5) Movimentações financeiras do cooperado ---
        db.session.execute(
            sa_delete(ReceitaCooperado).where(ReceitaCooperado.cooperado_id == id)
        )
        db.session.execute(
            sa_delete(DespesaCooperado).where(DespesaCooperado.cooperado_id == id)
        )

        # --- 6) Leituras de avisos atreladas ao cooperado ---
        db.session.execute(
            sa_delete(AvisoLeitura).where(AvisoLeitura.cooperado_id == id)
        )

        # --- 7) Finalmente, o Cooperado e o Usuario vinculado ---
        db.session.delete(c)
        if u:
            db.session.delete(u)

        db.session.commit()
        flash("Cooperado excluído.", "success")

    except IntegrityError as e:
        db.session.rollback()
        current_app.logger.exception(e)
        flash("Não foi possível excluir: existem vínculos ativos.", "danger")

    return redirect(url_for("admin_dashboard", tab="cooperados"))

@app.route("/cooperados/<int:id>/reset_senha", methods=["POST"])
@admin_required
def reset_senha_cooperado(id):
    c = Cooperado.query.get_or_404(id)
    ns = request.form.get("nova_senha") or ""
    cs = request.form.get("confirmar_senha") or ""
    if ns != cs:
        flash("As senhas não conferem.", "warning")
        return redirect(url_for("admin_dashboard", tab="cooperados"))
    c.usuario_ref.set_password(ns)
    db.session.commit()
    flash("Senha do cooperado atualizada.", "success")
    return redirect(url_for("admin_dashboard", tab="cooperados"))

@app.route("/restaurantes/add", methods=["POST"])
@admin_required
def add_restaurante():
    f = request.form
    nome = f.get("nome", "").strip()
    periodo = f.get("periodo", "seg-dom")
    usuario_login = f.get("usuario", "").strip()
    senha = f.get("senha", "")
    foto = request.files.get("foto")

    if Usuario.query.filter_by(usuario=usuario_login).first():
        flash("Usuário já existente.", "warning")
        return redirect(url_for("admin_dashboard", tab="restaurantes"))

    u = Usuario(usuario=usuario_login, tipo="restaurante", senha_hash="")
    u.set_password(senha)
    db.session.add(u)
    db.session.flush()
    r = Restaurante(nome=nome, periodo=periodo, usuario_id=u.id)
    db.session.add(r)
    db.session.flush()  # garante r.id
    if foto and foto.filename:
        _save_foto_to_db(r, foto, is_cooperado=False)
    db.session.commit()
    flash("Estabelecimento cadastrado.", "success")
    return redirect(url_for("admin_dashboard", tab="restaurantes"))

@app.route("/restaurantes/<int:id>/edit", methods=["POST"])
@admin_required
def edit_restaurante(id):
    r = Restaurante.query.get_or_404(id)
    f = request.form
    r.nome = f.get("nome", "").strip()
    r.periodo = f.get("periodo", "seg-dom")
    r.usuario_ref.usuario = f.get("usuario", "").strip()
    foto = request.files.get("foto")
    if foto and foto.filename:
        _save_foto_to_db(r, foto, is_cooperado=False)
    db.session.commit()
    flash("Estabelecimento atualizado.", "success")
    return redirect(url_for("admin_dashboard", tab="restaurantes"))

from sqlalchemy import delete as sa_delete
from sqlalchemy.exc import IntegrityError
from flask import current_app

@app.route("/restaurantes/<int:id>/delete", methods=["POST"])
@admin_required
def delete_restaurante(id):
    r = Restaurante.query.get_or_404(id)
    u = r.usuario_ref
    try:
        # 1) Coletar IDs das escalas do restaurante
        escala_ids = [e.id for e in Escala.query.with_entities(Escala.id)
                      .filter(Escala.restaurante_id == id).all()]

        if escala_ids:
            # 2) Apagar trocas que referenciam essas escalas
            db.session.execute(sa_delete(TrocaSolicitacao)
                               .where(TrocaSolicitacao.origem_escala_id.in_(escala_ids)))
            # 3) Apagar as escalas
            db.session.execute(sa_delete(Escala)
                               .where(Escala.restaurante_id == id))

        # 4) Apagar lançamentos do restaurante
        db.session.execute(sa_delete(Lancamento)
                           .where(Lancamento.restaurante_id == id))

        # 5) Agora apaga o restaurante e o usuário vinculado
        db.session.delete(r)
        if u:
            db.session.delete(u)

        db.session.commit()
        flash("Estabelecimento excluído.", "success")
    except IntegrityError as e:
        db.session.rollback()
        current_app.logger.exception(e)
        flash("Não foi possível excluir: existem vínculos ativos.", "danger")
    return redirect(url_for("admin_dashboard", tab="restaurantes"))

@app.route("/restaurantes/<int:id>/reset_senha", methods=["POST"])
@admin_required
def reset_senha_restaurante(id):
    r = Restaurante.query.get_or_404(id)
    ns = request.form.get("nova_senha") or ""
    cs = request.form.get("confirmar_senha") or ""
    if ns != cs:
        flash("As senhas não conferem.", "warning")
        return redirect(url_for("admin_dashboard", tab="restaurantes"))
    r.usuario_ref.set_password(ns)
    db.session.commit()
    flash("Senha do restaurante atualizada.", "success")
    return redirect(url_for("admin_dashboard", tab="restaurantes"))
# app.py (ou onde ficam suas rotas)
from flask import request, redirect, url_for, flash, session
from werkzeug.security import check_password_hash, generate_password_hash
@app.route("/rest/alterar-senha", methods=["POST"], endpoint="rest_alterar_senha")
@role_required("restaurante")
def alterar_senha_rest():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first_or_404()
    user = rest.usuario_ref  # Usuario vinculado ao restaurante

    atual = (request.form.get("senha_atual") or "").strip()
    nova  = (request.form.get("senha_nova")  or "").strip()
    conf  = (request.form.get("senha_conf")  or "").strip()

    if not (nova and conf):
        flash("Preencha todos os campos.", "warning")
        return redirect(url_for("portal_restaurante", view="config"))
    if nova != conf:
        flash("A confirmação não confere com a nova senha.", "warning")
        return redirect(url_for("portal_restaurante", view="config"))
    if len(nova) < 6:
        flash("A nova senha deve ter pelo menos 6 caracteres.", "warning")
        return redirect(url_for("portal_restaurante", view="config"))

    # exige senha atual somente se já houver uma definida
    if user.senha_hash and not atual:
        flash("Informe a senha atual.", "warning")
        return redirect(url_for("portal_restaurante", view="config"))
    if user.senha_hash and not check_password_hash(user.senha_hash, atual):
        flash("Senha atual incorreta.", "danger")
        return redirect(url_for("portal_restaurante", view="config"))

    user.senha_hash = generate_password_hash(nova)
    db.session.commit()
    flash("Senha alterada com sucesso!", "success")
    return redirect(url_for("portal_restaurante", view="config"))

@app.route("/config/update", methods=["POST"])
@admin_required
def update_config():
    cfg = get_config()
    cfg.salario_minimo = request.form.get("salario_minimo", type=float) or 0.0
    db.session.commit()
    flash("Configuração atualizada.", "success")
    return redirect(url_for("admin_dashboard", tab="config"))

@app.route("/admin/alterar_admin", methods=["POST"])
@admin_required
def alterar_admin():
    admin = Usuario.query.filter_by(tipo="admin").first()
    admin.usuario = request.form.get("usuario", admin.usuario).strip()
    nova = request.form.get("nova_senha", "")
    confirmar = request.form.get("confirmar_senha", "")
    if nova or confirmar:
        if nova != confirmar:
            flash("As senhas não conferem.", "warning")
            return redirect(url_for("admin_dashboard", tab="config"))
        admin.set_password(nova)
    db.session.commit()
    flash("Conta do administrador atualizada.", "success")
    return redirect(url_for("admin_dashboard", tab="config"))

# =========================
# Receitas/Despesas Cooperado (Admin)
# =========================
@app.route("/coop/receitas/add", methods=["POST"])
@admin_required
def add_receita_coop():
    f = request.form
    rc = ReceitaCooperado(
        cooperado_id=f.get("cooperado_id", type=int),
        descricao=f.get("descricao", "").strip(),
        valor=f.get("valor", type=float),
        data=_parse_date(f.get("data"))
    )
    db.session.add(rc)
    db.session.commit()
    flash("Receita do cooperado adicionada.", "success")
    return redirect(url_for("admin_dashboard", tab="coop_receitas"))

@app.route("/coop/receitas/<int:id>/edit", methods=["POST"])
@admin_required
def edit_receita_coop(id):
    rc = ReceitaCooperado.query.get_or_404(id)
    f = request.form
    rc.cooperado_id = f.get("cooperado_id", type=int)
    rc.descricao = f.get("descricao", "").strip()
    rc.valor = f.get("valor", type=float)
    rc.data = _parse_date(f.get("data"))
    db.session.commit()
    flash("Receita do cooperado atualizada.", "success")
    return redirect(url_for("admin_dashboard", tab="coop_receitas"))

@app.route("/coop/receitas/<int:id>/delete")
@admin_required
def delete_receita_coop(id):
    rc = ReceitaCooperado.query.get_or_404(id)
    db.session.delete(rc)
    db.session.commit()
    flash("Receita do cooperado excluída.", "success")
    return redirect(url_for("admin_dashboard", tab="coop_receitas"))

@app.route("/coop/despesas/add", methods=["POST"])
@admin_required
def add_despesa_coop():
    f = request.form
    ids = request.form.getlist("cooperado_ids[]")
    descricao = f.get("descricao", "").strip()
    valor_total = f.get("valor", type=float) or 0.0
    d = _parse_date(f.get("data"))

    cooperados = Cooperado.query.order_by(Cooperado.nome).all()
    dest_ids = [c.id for c in cooperados] if "all" in ids else [int(i) for i in ids if i.isdigit()]
    if not dest_ids:
        flash("Selecione pelo menos um cooperado.", "warning")
        return redirect(url_for("admin_dashboard", tab="coop_despesas"))

    valor_unit = round(valor_total / max(1, len(dest_ids)), 2)
    for cid in dest_ids:
        db.session.add(DespesaCooperado(cooperado_id=cid, descricao=descricao, valor=valor_unit, data=d))
    db.session.commit()
    flash("Despesa(s) lançada(s).", "success")
    return redirect(url_for("admin_dashboard", tab="coop_despesas"))

@app.route("/coop/despesas/<int:id>/edit", methods=["POST"])
@admin_required
def edit_despesa_coop(id):
    dc = DespesaCooperado.query.get_or_404(id)
    f = request.form
    dc.cooperado_id = f.get("cooperado_id", type=int)
    dc.descricao = f.get("descricao", "").strip()
    dc.valor = f.get("valor", type=float)
    dc.data = _parse_date(f.get("data"))
    db.session.commit()
    flash("Despesa do cooperado atualizada.", "success")
    return redirect(url_for("admin_dashboard", tab="coop_despesas"))

@app.route("/coop/despesas/<int:id>/delete")
@admin_required
def delete_despesa_coop(id):
    dc = DespesaCooperado.query.get_or_404(id)
    db.session.delete(dc)
    db.session.commit()
    flash("Despesa do cooperado excluída.", "success")
    return redirect(url_for("admin_dashboard", tab="coop_despesas"))

# =========================
# Benefícios — Rateio (Admin)
# =========================
@app.route("/beneficios/ratear", methods=["POST"])
@admin_required
def ratear_beneficios():
    f = request.form
    di = _parse_date(f.get("data_inicial"))
    df = _parse_date(f.get("data_final"))
    hoje = date.today()

    cooperados = Cooperado.query.order_by(Cooperado.nome).all()
    coop_map = {c.id: c for c in cooperados}

    def proc(tipo_key: str, valor_key: str):
        valor_total = f.get(valor_key, type=float) or 0.0
        if valor_total <= 0:
            return
        ids_selected = request.form.getlist(f"{tipo_key}_beneficiarios[]")
        sel_ids = [int(x) for x in ids_selected if x.isdigit()]
        nomes = [coop_map[i].nome for i in sel_ids if i in coop_map]

        b = BeneficioRegistro(
            data_inicial=di, data_final=df, data_lancamento=hoje,
            tipo={"hosp": "hospitalar", "farm": "farmaceutico", "alim": "alimentar"}[tipo_key],
            valor_total=valor_total,
            recebedores_nomes=";".join(nomes),
            recebedores_ids=";".join(str(i) for i in sel_ids)
        )
        db.session.add(b)

        todos_ids = {c.id for c in cooperados}
        nao_recebem_ids = sorted(list(todos_ids.difference(sel_ids)))
        if nao_recebem_ids:
            valor_unit = round(valor_total / len(nao_recebem_ids), 2)
            ref = di.strftime("%m/%Y") if di else ""
            desc = f"Benefício {b.tipo} (rateio ref {ref})"
            for cid in nao_recebem_ids:
                db.session.add(DespesaCooperado(cooperado_id=cid, descricao=desc, valor=valor_unit, data=hoje))

    proc("hosp", "hosp_valor")
    proc("farm", "farm_valor")
    proc("alim", "alim_valor")

    db.session.commit()
    flash("Rateios aplicados.", "success")
    return redirect(url_for("admin_dashboard", tab="beneficios"))



-- =========================
-- AVALIAÇÕES (já existia)# =========================
# Escalas — Upload (substituição TOTAL sempre)
# =========================
@app.route("/escalas/upload", methods=["POST"])
@admin_required
def upload_escala():
    from datetime import datetime, date
    import os, re as _re, unicodedata as _u, difflib as _dif

    file = request.files.get("file")
    if not file or not file.filename.lower().endswith(".xlsx"):
        flash("Envie um arquivo .xlsx válido.", "warning")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    # salva o arquivo (o nome não influencia a lógica)
    path = os.path.join(UPLOAD_DIR, secure_filename(file.filename))
    file.save(path)

    # abre com openpyxl
    try:
        import openpyxl
    except Exception:
        flash("Arquivo salvo, mas falta a biblioteca 'openpyxl' (pip install openpyxl).", "warning")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    try:
        wb = openpyxl.load_workbook(path, data_only=True)
        ws = wb.active
    except Exception as e:
        flash(f"Erro ao abrir a planilha: {e}", "danger")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    # ------- helpers -------
    def _norm_local(s: str) -> str:
        s = _u.normalize("NFD", str(s or "").strip().lower())
        s = "".join(ch for ch in s if _u.category(ch) != "Mn")
        return _re.sub(r"[^a-z0-9]+", " ", s).strip()

    def _norm_login_local(s: str) -> str:
        s = _u.normalize("NFD", s or "")
        s = "".join(ch for ch in s if _u.category(ch) != "Mn")
        return _re.sub(r"\s+", "", s.lower().strip())

    def to_css_color_local(v: str) -> str:
        t = str(v or "").strip()
        if not t: return ""
        if _re.fullmatch(r"[0-9a-fA-F]{8}", t):
            a = int(t[0:2], 16) / 255.0
            r = int(t[2:4], 16); g = int(t[4:6], 16); b = int(t[6:8], 16)
            return f"rgba({r},{g},{b},{a:.3f})"
        if _re.fullmatch(r"[0-9a-fA-F]{6}", t):
            return f"#{t}"
        if _re.fullmatch(r"#?[0-9a-fA-F]{6,8}", t):
            if not t.startswith("#"): t = f"#{t}"
            if len(t) == 9:
                a = int(t[1:3], 16) / 255.0
                r = int(t[3:5], 16); g = int(t[5:7], 16); b = int(t[7:9], 16)
                return f"rgba({r},{g},{b},{a:.3f})"
            return t
        m = _re.fullmatch(r"\s*(\d{1,3})\s*[,;]\s*(\d{1,3})\s*[,;]\s*(\d{1,3})\s*", t)
        if m:
            r, g, b = [max(0, min(255, int(x))) for x in m.groups()]
            return f"rgb({r},{g},{b})"
        mapa = {"azul":"blue","vermelho":"red","verde":"green","amarelo":"yellow",
                "cinza":"gray","preto":"black","branco":"white","laranja":"orange","roxo":"purple"}
        return mapa.get(t.lower(), t)

    def fmt_data_cell(v) -> str:
        if v is None or str(v).strip() == "": return ""
        if isinstance(v, datetime): return v.date().strftime("%d/%m/%Y")
        if isinstance(v, date):     return v.strftime("%d/%m/%Y")
        s = str(v).strip()
        m = _re.fullmatch(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
        if m:
            y, mth, d = map(int, m.groups())
            try: return date(y, mth, d).strftime("%d/%m/%Y")
            except Exception: return s
        return s

    # ------- cabeçalhos -------
    headers_norm = { _norm_local(str(c.value or "")) : j for j, c in enumerate(ws[1], start=1) }

    def find_col(*aliases):
        al = [_norm_local(a) for a in aliases]
        for a in al:
            if a in headers_norm: return headers_norm[a]
        for k_norm, j in headers_norm.items():
            for a in al:
                if a and a in k_norm: return j
        return None

    col_data     = find_col("data", "dia", "data do plantao")
    col_turno    = find_col("turno")
    col_horario  = find_col("horario", "horário", "hora", "periodo", "período")
    col_contrato = find_col("contrato", "restaurante", "unidade", "local")
    col_login    = find_col("login", "usuario", "usuário", "username", "user", "nome de usuario", "nome de usuário")
    col_nome     = find_col("nome", "nome do cooperado", "cooperado", "motoboy", "entregador")
    col_cor      = find_col("cor","cores","cor da celula","cor celula")

    if not col_login and not col_nome:
        flash("Não encontrei a coluna de LOGIN nem a de NOME do cooperado na planilha.", "danger")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    # ------- cache entidades -------
    restaurantes = Restaurante.query.order_by(Restaurante.nome).all()
    cooperados   = Cooperado.query.order_by(Cooperado.nome).all()

    def match_restaurante_id(contrato_txt: str) -> int | None:
        a = _norm_local(contrato_txt)
        if not a: return None
        for r in restaurantes:
            b = _norm_local(r.nome)
            if a == b or a in b or b in a: return r.id
        try:
            nomes_norm = [_norm_local(r.nome) for r in restaurantes]
            close = _dif.get_close_matches(a, nomes_norm, n=1, cutoff=0.87)
            if close:
                alvo = close[0]
                for r in restaurantes:
                    if _norm_local(r.nome) == alvo: return r.id
        except Exception:
            pass
        return None

    def match_cooperado_by_login(login_txt: str) -> Cooperado | None:
        key = _norm_login_local(login_txt)
        if not key: return None
        for c in cooperados:
            login = getattr(c, "usuario_ref", None)
            login_val = getattr(login, "usuario", "") if login else ""
            if _norm_login_local(login_val) == key:
                return c
        return None

    # helper global por nome (já existe no seu arquivo)
    from_here_match_by_name = _match_cooperado_by_name

    # ------- parse linhas -------
    linhas_novas, total_linhas_planilha = [], 0
    for i in range(2, ws.max_row + 1):
        login_txt = str(ws.cell(i, col_login).value).strip() if col_login else ""
        nome_txt  = str(ws.cell(i, col_nome ).value).strip() if col_nome  else ""
        if not login_txt and not nome_txt:
            continue
        total_linhas_planilha += 1

        data_v     = ws.cell(i, col_data).value     if col_data     else None
        turno_v    = ws.cell(i, col_turno).value    if col_turno    else None
        horario_v  = ws.cell(i, col_horario).value  if col_horario  else None
        contrato_v = ws.cell(i, col_contrato).value if col_contrato else None
        cor_v      = ws.cell(i, col_cor).value      if col_cor      else None

        contrato_txt = (str(contrato_v).strip() if contrato_v is not None else "")
        rest_id      = match_restaurante_id(contrato_txt)

        coop_match = match_cooperado_by_login(login_txt) if login_txt else None
        if not coop_match and nome_txt:
            coop_match = from_here_match_by_name(nome_txt, cooperados)

        nome_fallback = (nome_txt or login_txt)

        linhas_novas.append({
            "cooperado_id":   (coop_match.id if coop_match else None),
            "cooperado_nome": (None if coop_match else nome_fallback),
            "data":           fmt_data_cell(data_v),
            "turno":          (str(turno_v).strip() if turno_v is not None else ""),
            "horario":        (str(horario_v).strip() if horario_v is not None else ""),
            "contrato":       contrato_txt,
            "cor":            to_css_color_local(cor_v),
            "restaurante_id": rest_id,
        })

    if not linhas_novas:
        flash("Nada importado: nenhum registro válido encontrado.", "warning")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    # ------- SUBSTITUIÇÃO TOTAL -------
    try:
        # apaga todas as escalas antigas
        db.session.execute(sa_delete(Escala))

        # insere novas linhas
        for row in linhas_novas:
            db.session.add(Escala(**row))

        # marca atualização para cooperados reconhecidos
        ids_reconhecidos = {int(r["cooperado_id"]) for r in linhas_novas if r.get("cooperado_id")}
        for cid in ids_reconhecidos:
            c = Cooperado.query.get(cid)
            if c:
                c.ultima_atualizacao = datetime.now()

        db.session.commit()
        flash(f"Escala substituída com sucesso. {len(linhas_novas)} linha(s) importada(s) (de {total_linhas_planilha}).", "success")

    except Exception as e:
        db.session.rollback()
        app.logger.exception("Erro ao importar a escala")
        flash(f"Erro ao importar a escala: {e}", "danger")

    return redirect(url_for("admin_dashboard", tab="escalas"))


# =========================
# Ações de exclusão de escalas
# =========================
@app.post("/escalas/purge_all")
@admin_required
def escalas_purge_all():
    res = db.session.execute(sa_delete(Escala))
    db.session.commit()
    flash(f"Todas as escalas foram excluídas ({res.rowcount or 0}).", "info")
    return redirect(url_for("admin_dashboard", tab="escalas"))

@app.post("/escalas/purge_cooperado/<int:coop_id>")
@admin_required
def escalas_purge_cooperado(coop_id):
    res = db.session.execute(sa_delete(Escala).where(Escala.cooperado_id == coop_id))
    db.session.commit()
    flash(f"Escalas do cooperado #{coop_id} excluídas ({res.rowcount or 0}).", "info")
    return redirect(url_for("admin_dashboard", tab="escalas"))

@app.post("/escalas/purge_restaurante/<int:rest_id>")
@admin_required
def escalas_purge_restaurante(rest_id):
    res = db.session.execute(sa_delete(Escala).where(Escala.restaurante_id == rest_id))
    db.session.commit()
    flash(f"Escalas do restaurante #{rest_id} excluídas ({res.rowcount or 0}).", "info")
    return redirect(url_for("admin_dashboard", tab="escalas"))


# =========================
# Trocas (Admin aprovar/recusar)
# =========================
@app.post("/admin/trocas/<int:id>/aprovar")
@admin_required
def admin_aprovar_troca(id):
    t = TrocaSolicitacao.query.get_or_404(id)
    if t.status != "pendente":
        flash("Esta solicitação já foi tratada.", "warning")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    orig_e = Escala.query.get(t.origem_escala_id)
    if not orig_e:
        flash("Plantão de origem inválido.", "danger")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    solicitante = Cooperado.query.get(t.solicitante_id)
    destinatario = Cooperado.query.get(t.destino_id)
    if not solicitante or not destinatario:
        flash("Cooperado(s) inválido(s) na solicitação.", "danger")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    wd_o = _weekday_from_data_str(orig_e.data)
    buck_o = _turno_bucket(orig_e.turno, orig_e.horario)
    minhas = (Escala.query
              .filter_by(cooperado_id=destinatario.id)
              .order_by(Escala.id.asc()).all())
    candidatas = [e for e in minhas
                  if _weekday_from_data_str(e.data) == wd_o
                  and _turno_bucket(e.turno, e.horario) == buck_o]

    if len(candidatas) != 1:
        if len(candidatas) == 0:
            flash("Destino não possui plantões compatíveis (mesmo dia da semana e mesmo turno).", "danger")
        else:
            flash("Mais de um plantão compatível encontrado para o destino. Aprove pelo portal do cooperado (onde é possível escolher).", "warning")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    dest_e = candidatas[0]

    linhas = [
        {
            "dia": _escala_label(orig_e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(orig_e.turno or "").strip(), (orig_e.horario or "").strip()] if x]),
            "contrato": (orig_e.contrato or "").strip(),
            "saiu": solicitante.nome,
            "entrou": destinatario.nome,
        },
        {
            "dia": _escala_label(dest_e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(dest_e.turno or "").strip(), (dest_e.horario or "").strip()] if x]),
            "contrato": (dest_e.contrato or "").strip(),
            "saiu": destinatario.nome,
            "entrou": solicitante.nome,
        }
    ]
    afetacao_json = {"linhas": linhas}

    solicitante_id = orig_e.cooperado_id
    destino_id = dest_e.cooperado_id
    orig_e.cooperado_id, dest_e.cooperado_id = destino_id, solicitante_id

    t.status = "aprovada"
    t.aplicada_em = datetime.utcnow()
    prefix = "" if not (t.mensagem and t.mensagem.strip()) else (t.mensagem.rstrip() + "\n")
    t.mensagem = prefix + "__AFETACAO_JSON__:" + json.dumps(afetacao_json, ensure_ascii=False)

    db.session.commit()
    flash("Troca aprovada e aplicada com sucesso!", "success")
    return redirect(url_for("admin_dashboard", tab="escalas"))

@app.post("/admin/trocas/<int:id>/recusar")
@admin_required
def admin_recusar_troca(id):
    t = TrocaSolicitacao.query.get_or_404(id)
    if t.status != "pendente":
        flash("Esta solicitação já foi tratada.", "warning")
        return redirect(url_for("admin_dashboard", tab="escalas"))
    t.status = "recusada"
    db.session.commit()
    flash("Solicitação recusada.", "info")
    return redirect(url_for("admin_dashboard", tab="escalas"))

@app.get("/admin/tools/backfill_trocas_afetacao")
@admin_required
def backfill_trocas_afetacao():
    alteradas = 0
    for t in TrocaSolicitacao.query.filter_by(status="aprovada").all():
        if _parse_linhas_from_msg(t.mensagem):
            continue
        solicitante = Cooperado.query.get(t.solicitante_id)
        destinatario = Cooperado.query.get(t.destino_id)
        orig = Escala.query.get(t.origem_escala_id)
        if not (solicitante and destinatario and orig):
            continue
        wd_o = _weekday_from_data_str(orig.data)
        buck_o = _turno_bucket(orig.turno, orig.horario)

        def _linha_from_escala_local(e, saiu, entrou):
            return {
                "dia": _escala_label(e).split(" • ")[0],
                "turno_horario": " • ".join([x for x in [(e.turno or "").strip(), (e.horario or "").strip()] if x]),
                "contrato": (e.contrato or "").strip(),
                "saiu": saiu, "entrou": entrou,
            }

        linhas = [_linha_from_escala_local(orig, saiu=solicitante.nome, entrou=destinatario.nome)]
        candidatas = Escala.query.filter_by(cooperado_id=solicitante.id).all()
        best = None
        for e in candidatas:
            if _weekday_from_data_str(e.data) == wd_o and _turno_bucket(e.turno, e.horario) == buck_o:
                if (orig.contrato or "").strip().lower() == (e.contrato or "").strip().lower():
                    best = e; break
                if best is None: best = e
        if best:
            linhas.append(_linha_from_escala_local(best, saiu=destinatario.nome, entrou=solicitante.nome))
        afetacao_json = {"linhas": linhas}
        prefix = "" if not (t.mensagem and t.mensagem.strip()) else (t.mensagem.rstrip() + "\n")
        t.mensagem = prefix + "__AFETACAO_JSON__:" + json.dumps(afetacao_json, ensure_ascii=False)
        alteradas += 1
    db.session.commit()
    flash(f"Backfill concluído: {alteradas} troca(s) atualizada(s).", "success")
    return redirect(url_for("admin_dashboard", tab="escalas"))


# --- Admin tool: aplicar ON DELETE CASCADE nas FKs de avaliacoes* ---
@app.get("/admin/tools/apply_fk_cascade")
@admin_required
def apply_fk_cascade():
    sql = """
BEGIN;

-- =========================
-- AVALIAÇÕES (já existia)
-- =========================
-- ajusta FK de avaliacoes.lancamento_id
ALTER TABLE public.avaliacoes
  DROP CONSTRAINT IF EXISTS avaliacoes_lancamento_id_fkey;
ALTER TABLE public.avaliacoes
  ADD CONSTRAINT avaliacoes_lancamento_id_fkey
  FOREIGN KEY (lancamento_id)
  REFERENCES public.lancamentos (id)
  ON DELETE CASCADE;

-- cria (se faltar) FK de avaliacoes_restaurante.lancamento_id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_name = 'av_rest_lancamento_id_fkey'
          AND table_name = 'avaliacoes_restaurante'
    ) THEN
        ALTER TABLE public.avaliacoes_restaurante
          ADD CONSTRAINT av_rest_lancamento_id_fkey
          FOREIGN KEY (lancamento_id)
          REFERENCES public.lancamentos (id)
          ON DELETE CASCADE;
    ELSE
        -- se já existir, garante o CASCADE (drop/add)
        EXECUTE 'ALTER TABLE public.avaliacoes_restaurante
                 DROP CONSTRAINT IF EXISTS av_rest_lancamento_id_fkey';
        EXECUTE 'ALTER TABLE public.avaliacoes_restaurante
                 ADD CONSTRAINT av_rest_lancamento_id_fkey
                 FOREIGN KEY (lancamento_id)
                 REFERENCES public.lancamentos (id)
                 ON DELETE CASCADE';
    END IF;
END $$;

-- =========================
-- ESCALAS
-- =========================
-- cooperado_id -> cooperados(id) ON DELETE CASCADE
ALTER TABLE public.escalas
  DROP CONSTRAINT IF EXISTS escalas_cooperado_id_fkey;
ALTER TABLE public.escalas
  ADD CONSTRAINT escalas_cooperado_id_fkey
  FOREIGN KEY (cooperado_id)
  REFERENCES public.cooperados (id)
  ON DELETE CASCADE;

-- restaurante_id -> restaurantes(id) ON DELETE CASCADE
ALTER TABLE public.escalas
  DROP CONSTRAINT IF EXISTS escalas_restaurante_id_fkey;
ALTER TABLE public.escalas
  ADD CONSTRAINT escalas_restaurante_id_fkey
  FOREIGN KEY (restaurante_id)
  REFERENCES public.restaurantes (id)
  ON DELETE CASCADE;

-- =========================
-- TROCAS
-- =========================
-- solicitante_id -> cooperados(id) ON DELETE CASCADE
ALTER TABLE public.trocas
  DROP CONSTRAINT IF EXISTS trocas_solicitante_id_fkey;
ALTER TABLE public.trocas
  ADD CONSTRAINT trocas_solicitante_id_fkey
  FOREIGN KEY (solicitante_id)
  REFERENCES public.cooperados (id)
  ON DELETE CASCADE;

-- destino_id -> cooperados(id) ON DELETE CASCADE
ALTER TABLE public.trocas
  DROP CONSTRAINT IF EXISTS trocas_destino_id_fkey;
ALTER TABLE public.trocas
  ADD CONSTRAINT trocas_destino_id_fkey
  FOREIGN KEY (destino_id)
  REFERENCES public.cooperados (id)
  ON DELETE CASCADE;

-- origem_escala_id -> escalas(id) ON DELETE CASCADE
ALTER TABLE public.trocas
  DROP CONSTRAINT IF EXISTS trocas_origem_escala_id_fkey;
ALTER TABLE public.trocas
  ADD CONSTRAINT trocas_origem_escala_id_fkey
  FOREIGN KEY (origem_escala_id)
  REFERENCES public.escalas (id)
  ON DELETE CASCADE;

COMMIT;
"""
    try:
        if _is_sqlite():
            flash("SQLite local: esta operação é específica de Postgres (sem efeito aqui).", "warning")
            return redirect(url_for("admin_dashboard", tab="config"))

        db.session.execute(sa_text(sql))
        db.session.commit()
        flash("FKs com ON DELETE CASCADE aplicadas com sucesso.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Erro ao aplicar FKs: {e}", "danger")
    return redirect(url_for("admin_dashboard", tab="config"))

-- =========================
-- ajusta FK de avaliacoes.lancamento_id
ALTER TABLE public.avaliacoes
  DROP CONSTRAINT IF EXISTS avaliacoes_lancamento_id_fkey;
ALTER TABLE public.avaliacoes
  ADD CONSTRAINT avaliacoes_lancamento_id_fkey
  FOREIGN KEY (lancamento_id)
  REFERENCES public.lancamentos (id)
  ON DELETE CASCADE;

-- cria (se faltar) FK de avaliacoes_restaurante.lancamento_id
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_name = 'av_rest_lancamento_id_fkey'
          AND table_name = 'avaliacoes_restaurante'
    ) THEN
        ALTER TABLE public.avaliacoes_restaurante
          ADD CONSTRAINT av_rest_lancamento_id_fkey
          FOREIGN KEY (lancamento_id)
          REFERENCES public.lancamentos (id)
          ON DELETE CASCADE;
    ELSE
        -- se já existir, garante o CASCADE (drop/add)
        EXECUTE 'ALTER TABLE public.avaliacoes_restaurante
                 DROP CONSTRAINT IF EXISTS av_rest_lancamento_id_fkey';
        EXECUTE 'ALTER TABLE public.avaliacoes_restaurante
                 ADD CONSTRAINT av_rest_lancamento_id_fkey
                 FOREIGN KEY (lancamento_id)
                 REFERENCES public.lancamentos (id)
                 ON DELETE CASCADE';
    END IF;
END $$;

-- =========================
-- ESCALAS
-- =========================
-- cooperado_id -> cooperados(id) ON DELETE CASCADE
ALTER TABLE public.escalas
  DROP CONSTRAINT IF EXISTS escalas_cooperado_id_fkey;
ALTER TABLE public.escalas
  ADD CONSTRAINT escalas_cooperado_id_fkey
  FOREIGN KEY (cooperado_id)
  REFERENCES public.cooperados (id)
  ON DELETE CASCADE;

-- restaurante_id -> restaurantes(id) ON DELETE CASCADE
ALTER TABLE public.escalas
  DROP CONSTRAINT IF EXISTS escalas_restaurante_id_fkey;
ALTER TABLE public.escalas
  ADD CONSTRAINT escalas_restaurante_id_fkey
  FOREIGN KEY (restaurante_id)
  REFERENCES public.restaurantes (id)
  ON DELETE CASCADE;

-- =========================
-- TROCAS
-- =========================
-- solicitante_id -> cooperados(id) ON DELETE CASCADE
ALTER TABLE public.trocas
  DROP CONSTRAINT IF EXISTS trocas_solicitante_id_fkey;
ALTER TABLE public.trocas
  ADD CONSTRAINT trocas_solicitante_id_fkey
  FOREIGN KEY (solicitante_id)
  REFERENCES public.cooperados (id)
  ON DELETE CASCADE;

-- destino_id -> cooperados(id) ON DELETE CASCADE
ALTER TABLE public.trocas
  DROP CONSTRAINT IF EXISTS trocas_destino_id_fkey;
ALTER TABLE public.trocas
  ADD CONSTRAINT trocas_destino_id_fkey
  FOREIGN KEY (destino_id)
  REFERENCES public.cooperados (id)
  ON DELETE CASCADE;

-- origem_escala_id -> escalas(id) ON DELETE CASCADE
ALTER TABLE public.trocas
  DROP CONSTRAINT IF EXISTS trocas_origem_escala_id_fkey;
ALTER TABLE public.trocas
  ADD CONSTRAINT trocas_origem_escala_id_fkey
  FOREIGN KEY (origem_escala_id)
  REFERENCES public.escalas (id)
  ON DELETE CASCADE;

COMMIT;
"""
    
    try:
        if _is_sqlite():
            flash("SQLite local: esta operação é específica de Postgres (sem efeito aqui).", "warning")
            return redirect(url_for("admin_dashboard", tab="config"))

        db.session.execute(sa_text(sql))
        db.session.commit()
        flash("FKs com ON DELETE CASCADE aplicadas com sucesso.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Erro ao aplicar FKs: {e}", "danger")
    return redirect(url_for("admin_dashboard", tab="config"))

# =========================
# Documentos (Admin)
# =========================
@app.route("/documentos/<int:coop_id>", methods=["GET", "POST"])
@admin_required
def editar_documentos(coop_id):
    c = Cooperado.query.get_or_404(coop_id)

    if request.method == "POST":
        f = request.form
        c.cnh_numero = f.get("cnh_numero")
        c.placa = f.get("placa")
        def parse_date_local(s):
            try:
                return datetime.strptime(s, "%Y-%m-%d").date() if s else None
            except Exception:
                return None
        c.cnh_validade = parse_date_local(f.get("cnh_validade"))
        c.placa_validade = parse_date_local(f.get("placa_validade"))
        c.ultima_atualizacao = datetime.now()
        db.session.commit()
        flash("Documentos atualizados.", "success")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    tpl = os.path.join("templates", "editar_documentos.html")
    hoje = date.today()
    prazo_final = date(hoje.year, 12, 31)
    if os.path.exists(tpl):
        docinfo = {
            "prazo_final": prazo_final,
            "dias_ate_prazo": max(0, (prazo_final - hoje).days),
            "cnh": {
                "numero": c.cnh_numero,
                "validade": c.cnh_validade,
                "prox_validade": _prox_ocorrencia_anual(c.cnh_validade),
                "ok": (c.cnh_validade is not None and c.cnh_validade >= hoje),
                "modo": "auto",
            },
            "placa": {
                "numero": c.placa,
                "validade": c.placa_validade,
                "prox_validade": _prox_ocorrencia_anual(c.placa_validade),
                "ok": (c.placa_validade is not None and c.placa_validade >= hoje),
                "modo": "auto",
            }
        }
        return render_template("editar_documentos.html", cooperado=c, docinfo=docinfo)

    return f"""
    <div style="max-width:560px;margin:30px auto;font-family:Arial">
      <h3>Documentos — {c.nome}</h3>
      <form method="POST">
        <label>CNH (número)</label><br>
        <input name="cnh_numero" value="{c.cnh_numero or ''}" style="width:100%;padding:8px"><br><br>
        <label>Validade CNH</label><br>
        <input type="date" name="cnh_validade" value="{c.cnh_validade.strftime('%Y-%m-%d') if c.cnh_validade else ''}" style="width:100%;padding:8px"><br><br>
        <label>Placa</label><br>
        <input name="placa" value="{c.placa or ''}" style="width:100%;padding:8px"><br><br>
        <label>Validade da Placa</label><br>
        <input type="date" name="placa_validade" value="{c.placa_validade.strftime('%Y-%m-%d') if c.placa_validade else ''}" style="width:100%;padding:8px"><br><br>
        <button style="padding:10px 16px">Salvar</button>
        <a href="{url_for('admin_dashboard', tab='escalas')}" style="margin-left:10px">Voltar</a>
      </form>
    </div>
    """

# =========================
# PORTAL COOPERADO
# =========================
@app.route("/portal/cooperado")
@role_required("cooperado")
def portal_cooperado():
    u_id = session.get("user_id")
    coop = Cooperado.query.filter_by(usuario_id=u_id).first()
    if not coop:
        return "<p style='font-family:Arial;margin:40px'>Seu usuário não está vinculado a um cooperado. Avise o administrador.</p>"

    try:
        coop.usuario = coop.usuario_ref.usuario
    except Exception:
        coop.usuario = ""

    # ---------- FILTRO POR DATA (padrão = HOJE) ----------
    di = _parse_date(request.args.get("data_inicio"))
    df = _parse_date(request.args.get("data_fim"))

    # padrão: mostrar SOMENTE a data do lançamento (hoje)
    if di and not df:
        df = di
    if df and not di:
        di = df
    if not di and not df:
        di = df = date.today()

    def in_range(qs, col):
        return qs.filter(col >= di, col <= df)

    ql = in_range(Lancamento.query.filter_by(cooperado_id=coop.id), Lancamento.data)
    producoes = ql.order_by(Lancamento.data.desc(), Lancamento.id.desc()).all()
    
        # --- Marca se o cooperado já avaliou cada produção ---
    ids = [l.id for l in producoes]
    minhas = {}
    if ids:
        rows = (
            db.session.query(
                AvaliacaoRestaurante.lancamento_id,
                AvaliacaoRestaurante.estrelas_geral
            )
            .filter(
                AvaliacaoRestaurante.lancamento_id.in_(ids),
                AvaliacaoRestaurante.cooperado_id == coop.id
            )
            .all()
        )
        minhas = {lid: nota for lid, nota in rows}

    for l in producoes:
        l.minha_avaliacao = minhas.get(l.id)

    qr = in_range(ReceitaCooperado.query.filter_by(cooperado_id=coop.id), ReceitaCooperado.data)
    receitas_coop = qr.order_by(ReceitaCooperado.data.desc(), ReceitaCooperado.id.desc()).all()

    qd = in_range(DespesaCooperado.query.filter_by(cooperado_id=coop.id), DespesaCooperado.data)
    despesas_coop = qd.order_by(DespesaCooperado.data.desc(), DespesaCooperado.id.desc()).all()

    # INSS calculado por lançamento e somado APENAS dentro do período filtrado
    total_bruto = sum((l.valor or 0.0) for l in producoes) + sum((r.valor or 0.0) for r in receitas_coop)
    inss_valor = sum((l.valor or 0.0) * 0.045 for l in producoes)
    total_descontos = sum((d.valor or 0.0) for d in despesas_coop)
    total_liquido = total_bruto - inss_valor - total_descontos

    cfg = get_config()
    salario_minimo = cfg.salario_minimo or 0.0
    inss_complemento = salario_minimo * 0.20

    today = date.today()
    def dias_para_3112():
        alvo = date(today.year, 12, 31)
        if today > alvo:
            alvo = date(today.year + 1, 12, 31)
        return (alvo - today).days

    doc_cnh = {
        "numero": coop.cnh_numero,
        "vencimento": coop.cnh_validade,
        "ok": (coop.cnh_validade is not None and coop.cnh_validade >= today),
        "dias_para_prazo": dias_para_3112(),
    }
    doc_placa = {
        "numero": coop.placa,
        "vencimento": coop.placa_validade,
        "ok": (coop.placa_validade is not None and coop.placa_validade >= today),
        "dias_para_prazo": dias_para_3112(),
    }

    raw_escala = (Escala.query
                  .filter_by(cooperado_id=coop.id)
                  .order_by(Escala.id.asc())
                  .all())

    import unicodedata as _u, re as _re
    def _norm_c(s: str) -> str:
        s = _u.normalize("NFD", str(s or "").lower())
        s = "".join(ch for ch in s if _u.category(ch) != "Mn")
        return _re.sub(r"[^a-z0-9]+", " ", s).strip()

    def _score(e):
        h = (e.horario or "").strip()
        return (1 if h else 0, len(h), e.id)

    best = {}
    for e in raw_escala:
        key = (_norm_c(e.data), _norm_c(e.turno), _norm_c(e.contrato))
        cur = best.get(key)
        if not cur or _score(e) > _score(cur):
            best[key] = e

    minha_escala = sorted(best.values(), key=lambda x: x.id)

    def _parse_data_escala_local(s: str):
        m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', str(s or ''))
        if not m:
            return None
        d_, mth, y = map(int, m.groups())
        if y < 100:
            y += 2000
        try:
            return date(y, mth, d_)
        except Exception:
            return None

    for e in minha_escala:
        dt = _parse_data_escala_local(e.data)
        if dt is None:
            status = 'unknown'
        else:
            if dt < today:
                status = 'past'
            elif dt == today:
                status = 'today'
            elif dt == today + timedelta(days=1):
                status = 'tomorrow'
            else:
                status = 'future'
        e.status = status
        e.status_color = (
            '#ef4444' if status == 'past' else
            '#22c55e' if status == 'today' else
            '#3b82f6' if status in ('tomorrow', 'future') else
            'transparent'
        )

    minha_escala_json = []
    for e in minha_escala:
        minha_escala_json.append({
            "id": e.id,
            "data": e.data or "",
            "turno": e.turno or "",
            "horario": e.horario or "",
            "contrato": e.contrato or "",
            "weekday": _weekday_from_data_str(e.data),
            "turno_bucket": _turno_bucket(e.turno, e.horario),
        })

    coops = (Cooperado.query
             .filter(Cooperado.id != coop.id)
             .order_by(Cooperado.nome.asc())
             .all())
    cooperados_json = [
        {"id": c.id, "nome": c.nome, "foto_url": (c.foto_url or "")}
        for c in coops
    ]

    def _escala_desc(e: Escala | None) -> str:
        return _escala_label(e)

    rx = (TrocaSolicitacao.query
          .filter(TrocaSolicitacao.destino_id == coop.id)
          .order_by(TrocaSolicitacao.id.desc())
          .all())

    trocas_recebidas_pendentes = []
    trocas_recebidas_historico = []
    for t in rx:
        solicitante = Cooperado.query.get(t.solicitante_id)
        orig = Escala.query.get(t.origem_escala_id)

        mensagem_limpa = _strip_afetacao_blob(t.mensagem)
        linhas_afetadas = _parse_linhas_from_msg(t.mensagem) if t.status == "aprovada" else []

        item = {
            "id": t.id,
            "status": t.status,
            "mensagem": mensagem_limpa,
            "criada_em": t.criada_em,
            "aplicada_em": t.aplicada_em,
            "solicitante": solicitante,
            "origem": orig,
            "origem_desc": _escala_desc(orig),
            "linhas_afetadas": linhas_afetadas,
            "origem_weekday": _weekday_from_data_str(orig.data) if orig else None,
            "origem_turno_bucket": _turno_bucket(orig.turno if orig else None, orig.horario if orig else None),
        }

        (trocas_recebidas_pendentes if t.status == "pendente" else trocas_recebidas_historico).append(item)

    ex = (TrocaSolicitacao.query
          .filter(TrocaSolicitacao.solicitante_id == coop.id)
          .order_by(TrocaSolicitacao.id.desc())
          .all())

    trocas_enviadas = []
    for t in ex:
        destino = Cooperado.query.get(t.destino_id)
        orig = Escala.query.get(t.origem_escala_id)
        mensagem_limpa = _strip_afetacao_blob(t.mensagem)
        linhas_afetadas = _parse_linhas_from_msg(t.mensagem) if t.status == "aprovada" else []
        trocas_enviadas.append({
            "id": t.id,
            "status": t.status,
            "mensagem": mensagem_limpa,
            "criada_em": t.criada_em,
            "aplicada_em": t.aplicada_em,
            "destino": destino,
            "origem": orig,
            "origem_desc": _escala_desc(orig),
            "linhas_afetadas": linhas_afetadas,
        })

    return render_template(
        "painel_cooperado.html",
        cooperado=coop,
        producoes=producoes,
        receitas_coop=receitas_coop,
        despesas_coop=despesas_coop,
        total_bruto=total_bruto,
        inss_valor=inss_valor,
        total_descontos=total_descontos,
        total_liquido=total_liquido,
        inss_complemento=inss_complemento,
        salario_minimo=salario_minimo,
        current_year=today.year,
        doc_cnh=doc_cnh,
        doc_placa=doc_placa,
        minha_escala=minha_escala,
        minha_escala_json=minha_escala_json,
        cooperados_json=cooperados_json,
        trocas_recebidas_pendentes=trocas_recebidas_pendentes,
        trocas_recebidas_historico=trocas_recebidas_historico,
        trocas_enviadas=trocas_enviadas,
    )

from datetime import datetime
from flask import request, redirect, url_for, flash, abort, session

@app.post("/coop/avaliar/restaurante/<int:lanc_id>")
@role_required("cooperado")
def coop_avaliar_restaurante(lanc_id):
    # 1) Cooperado logado
    u_id = session.get("user_id")
    coop = Cooperado.query.filter_by(usuario_id=u_id).first_or_404()

    # 2) Lançamento existe e é dele
    lanc = Lancamento.query.get_or_404(lanc_id)
    if lanc.cooperado_id != coop.id:
        abort(403)

    # 3) Já existe avaliação DESTE cooperado para ESTE lançamento?
    ja = (AvaliacaoRestaurante.query
          .filter_by(lancamento_id=lanc.id, cooperado_id=coop.id)
          .first())
    if ja:
        flash("Você já avaliou esta produção.", "info")
        return redirect(request.referrer or url_for("coop_dashboard"))

    # 4) Lê os campos do form (suporta 'nota' simples OU 'av_geral')
    f  = request.form
    g  = _clamp_star(f.get("nota") or f.get("av_geral"))   # obrigatório
    p  = _clamp_star(f.get("av_pontualidade"))
    ed = _clamp_star(f.get("av_educacao"))
    ef = _clamp_star(f.get("av_eficiencia"))
    ap = _clamp_star(f.get("av_apresentacao"))
    tx = (f.get("av_comentario") or "").strip()

    if not g:
        flash("Selecione uma nota de 1 a 5.", "warning")
        return redirect(request.referrer or url_for("coop_dashboard"))

    # 5) Cria e salva
    a = AvaliacaoRestaurante(
        restaurante_id=lanc.restaurante_id,
        cooperado_id=coop.id,
        lancamento_id=lanc.id,
        estrelas_geral=g,
        estrelas_pontualidade=p,
        estrelas_educacao=ed,
        estrelas_eficiencia=ef,
        estrelas_apresentacao=ap,
        comentario=tx,
        media_ponderada=_media_ponderada(g, p, ed, ef, ap),
        sentimento=_analise_sentimento(tx),
        temas="; ".join(_identifica_temas(tx)),
        alerta_crise=_sinaliza_crise(g, tx),
        criado_em=datetime.utcnow(),
    )
    db.session.add(a)
    db.session.commit()

    flash("Avaliação do restaurante registrada.", "success")
    return redirect(request.referrer or url_for("coop_dashboard"))

# Alias para manter compatibilidade com o action do formulário
@app.post("/producoes/<int:lanc_id>/avaliar", endpoint="producoes_avaliar")
@role_required("cooperado")
def producoes_avaliar(lanc_id):
    # IMPORTANTÍSSIMO: retornar o que a função real retorna
    return coop_avaliar_restaurante(lanc_id)

@app.route("/painel/cooperado")
@role_required("cooperado")
def coop_dashboard():
    return portal_cooperado()

@app.route("/escala/solicitar_troca", methods=["POST"])
@role_required("cooperado")
def solicitar_troca():
    u_id = session.get("user_id")
    me = Cooperado.query.filter_by(usuario_id=u_id).first()
    if not me:
        abort(403)

    from_escala_id = request.form.get("from_escala_id", type=int)
    to_cooperado_id = request.form.get("to_cooperado_id", type=int)
    mensagem = (request.form.get("mensagem") or "").strip()

    if not from_escala_id or not to_cooperado_id:
        flash("Selecione a escala e o cooperado de destino.", "warning")
        return redirect(url_for("portal_cooperado"))

    origem = Escala.query.get(from_escala_id)
    if not origem or origem.cooperado_id != me.id:
        flash("Escala inválida para solicitação.", "danger")
        return redirect(url_for("portal_cooperado"))

    destino = Cooperado.query.get(to_cooperado_id)
    if not destino or destino.id == me.id:
        flash("Cooperado de destino inválido.", "danger")
        return redirect(url_for("portal_cooperado"))

    t = TrocaSolicitacao(
        solicitante_id=me.id,
        destino_id=destino.id,
        origem_escala_id=origem.id,
        mensagem=mensagem or None,
        status="pendente",
    )
    db.session.add(t)
    db.session.commit()
    flash("Solicitação de troca enviada ao administrador.", "success")
    return redirect(url_for("portal_cooperado"))

@app.post("/trocas/<int:troca_id>/aceitar")
@role_required("cooperado")
def aceitar_troca(troca_id):
    u_id = session.get("user_id")
    me = Cooperado.query.filter_by(usuario_id=u_id).first()
    t = TrocaSolicitacao.query.get_or_404(troca_id)
    if not me or t.destino_id != me.id:
        abort(403)
    if t.status != "pendente":
        flash("Esta solicitação já foi tratada.", "warning")
        return redirect(url_for("portal_cooperado"))

    destino_escala_id = request.form.get("destino_escala_id", type=int)
    orig_e = Escala.query.get(t.origem_escala_id)
    if not orig_e:
        flash("Plantão de origem inválido.", "danger")
        return redirect(url_for("portal_cooperado"))

    if not destino_escala_id:
        minhas = Escala.query.filter_by(cooperado_id=me.id).order_by(Escala.id.asc()).all()
        wd_o = _weekday_from_data_str(orig_e.data)
        buck_o = _turno_bucket(orig_e.turno, orig_e.horario)
        candidatas = [e for e in minhas
                      if _weekday_from_data_str(e.data) == wd_o and _turno_bucket(e.turno, e.horario) == buck_o]
        if len(candidatas) == 1:
            destino_escala_id = candidatas[0].id
        elif len(candidatas) == 0:
            flash("Você não tem plantões compatíveis (mesmo dia da semana e turno).", "danger")
            return redirect(url_for("portal_cooperado"))
        else:
            flash("Selecione no modal qual dos seus plantões compatíveis deseja usar.", "warning")
            return redirect(url_for("portal_cooperado"))

    dest_e = Escala.query.get(destino_escala_id)
    if not dest_e or dest_e.cooperado_id != me.id:
        flash("Seleção de escala inválida.", "danger")
        return redirect(url_for("portal_cooperado"))

    wd_orig = _weekday_from_data_str(orig_e.data)
    wd_dest = _weekday_from_data_str(dest_e.data)
    buck_orig = _turno_bucket(orig_e.turno, orig_e.horario)
    buck_dest = _turno_bucket(dest_e.turno, dest_e.horario)
    if wd_orig is None or wd_dest is None or wd_orig != wd_dest or buck_orig != buck_dest:
        flash("Troca incompatível: precisa ser mesmo dia da semana e mesmo turno (dia/noite).", "danger")
        return redirect(url_for("portal_cooperado"))

    solicitante = Cooperado.query.get(t.solicitante_id)
    destinatario = me
    linhas = [
        {
            "dia": _escala_label(orig_e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(orig_e.turno or "").strip(), (orig_e.horario or "").strip()] if x]),
            "contrato": (orig_e.contrato or "").strip(),
            "saiu": solicitante.nome,
            "entrou": destinatario.nome,
        },
        {
            "dia": _escala_label(dest_e).split(" • ")[0],
            "turno_horario": " • ".join([x for x in [(dest_e.turno or "").strip(), (dest_e.horario or "").strip()] if x]),
            "contrato": (dest_e.contrato or "").strip(),
            "saiu": destinatario.nome,
            "entrou": solicitante.nome,
        }
    ]
    afetacao_json = {"linhas": linhas}

    solicitante_id = orig_e.cooperado_id
    destino_id = dest_e.cooperado_id
    orig_e.cooperado_id, dest_e.cooperado_id = destino_id, solicitante_id

    t.status = "aprovada"
    t.aplicada_em = datetime.utcnow()
    prefix = "" if not (t.mensagem and t.mensagem.strip()) else (t.mensagem.rstrip() + "\n")
    t.mensagem = prefix + "__AFETACAO_JSON__:" + json.dumps(afetacao_json, ensure_ascii=False)

    db.session.commit()
    flash("Troca aplicada com sucesso!", "success")
    return redirect(url_for("portal_cooperado"))

@app.post("/trocas/<int:troca_id>/recusar")
@role_required("cooperado")
def recusar_troca(troca_id):
    u_id = session.get("user_id")
    me = Cooperado.query.filter_by(usuario_id=u_id).first()
    t = TrocaSolicitacao.query.get_or_404(troca_id)
    if not me or t.destino_id != me.id:
        abort(403)
    if t.status != "pendente":
        flash("Esta solicitação já foi tratada.", "warning")
        return redirect(url_for("portal_cooperado"))

    t.status = "recusada"
    db.session.commit()
    flash("Solicitação recusada.", "info")
    return redirect(url_for("portal_cooperado"))

# =========================
# PORTAL RESTAURANTE
# =========================
@app.route("/portal/restaurante")
@role_required("restaurante")
def portal_restaurante():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    if not rest:
        return "<p style='font-family:Arial;margin:40px'>Seu usuário não está vinculado a um estabelecimento. Avise o administrador.</p>"

    # Abas/visões: 'lancar', 'escalas', 'lancamentos', 'config', 'avisos'
    view = (request.args.get("view", "lancar") or "lancar").strip().lower()

    # ---- helper mês YYYY-MM
    import re
    def _parse_yyyy_mm_local(s: str):
        if not s:
            return None, None
        m = re.fullmatch(r"(\d{4})-(\d{2})", s.strip())
        if not m:
            return None, None
        y = int(m.group(1)); mth = int(m.group(2))
        try:
            di_ = date(y, mth, 1)
            if mth == 12:
                df_ = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                df_ = date(y, mth + 1, 1) - timedelta(days=1)
            return di_, df_
        except Exception:
            return None, None

    # -------------------- LANÇAMENTOS (totais por período) --------------------
    di = _parse_date(request.args.get("data_inicio"))
    df = _parse_date(request.args.get("data_fim"))

    # NOVO: filtro por mês (?mes=YYYY-MM)
    mes = (request.args.get("mes") or "").strip()
    periodo_desc = None
    if mes:
        di_mes, df_mes = _parse_yyyy_mm_local(mes)
        if di_mes and df_mes:
            di, df = di_mes, df_mes
            periodo_desc = "mês"

    if not di or not df:
        # Sem filtro => janela semanal baseada no período do restaurante
        wd_map = {"seg-dom": 0, "sab-sex": 5, "sex-qui": 4}  # seg=0 ... dom=6
        start_wd = wd_map.get(rest.periodo, 0)
        hoje = date.today()
        delta = (hoje.weekday() - start_wd) % 7
        di_auto = hoje - timedelta(days=delta)
        df_auto = di_auto + timedelta(days=6)
        di = di or di_auto
        df = df or df_auto
        periodo_desc = periodo_desc or rest.periodo
    else:
        periodo_desc = periodo_desc or "personalizado"

    cooperados = Cooperado.query.order_by(Cooperado.nome).all()

    total_bruto = 0.0
    total_qtd = 0
    total_entregas = 0
    for c in cooperados:
        q = (
            Lancamento.query
            .filter_by(restaurante_id=rest.id, cooperado_id=c.id)
            .filter(Lancamento.data >= di, Lancamento.data <= df)
            .order_by(Lancamento.data.desc(), Lancamento.id.desc())
        )
        c.lancamentos = q.all()
        c.total_periodo = sum((l.valor or 0.0) for l in c.lancamentos)
        total_bruto += c.total_periodo
        total_qtd += len(c.lancamentos)
        total_entregas += sum((l.qtd_entregas or 0) for l in c.lancamentos)

    total_inss = total_bruto * 0.045
    total_liquido = total_bruto - total_inss

    # -------------------- ESCALA (Quem trabalha) --------------------
    def contrato_bate_restaurante(contrato: str, rest_nome: str) -> bool:
        a = " ".join(_normalize_name(contrato or ""))
        b = " ".join(_normalize_name(rest_nome or ""))
        if not a or not b:
            return False
        return a == b or a in b or b in a

    ref = _parse_date(request.args.get("ref")) or date.today()
    modo = request.args.get("modo", "semana")  # 'semana' ou 'dia'
    if modo == "dia":
        dias_list = [ref]
    else:
        semana_inicio = ref - timedelta(days=ref.weekday())
        dias_list = [semana_inicio + timedelta(days=i) for i in range(7)]

    escalas_all = Escala.query.order_by(Escala.id.asc()).all()
    eff_map = _carry_forward_contrato(escalas_all)

    escalas_rest = [
        e for e in escalas_all
        if contrato_bate_restaurante(eff_map.get(e.id, e.contrato or ""), rest.nome)
    ]
    if not escalas_rest:
        escalas_rest = [e for e in escalas_all if (e.contrato or "").strip() == rest.nome.strip()]

    agenda = {d: [] for d in dias_list}
    seen = {d: set() for d in dias_list}  # evita duplicar

    for e in escalas_rest:
        dt = _parse_data_escala_str(e.data)       # date | None
        wd = _weekday_from_data_str(e.data)       # 1..7 | None
        for d in dias_list:
            hit = (dt and dt == d) or (wd and wd == ((d.weekday() % 7) + 1))
            if not hit:
                continue

            coop = Cooperado.query.get(e.cooperado_id) if e.cooperado_id else None
            nome_fallback = (e.cooperado_nome or "").strip()
            nome_show = (coop.nome if coop else nome_fallback) or "—"
            contrato_eff = (eff_map.get(e.id, e.contrato or "") or "").strip()

            key = (
                (coop.id if coop else _norm(nome_show)),
                _norm(e.turno),
                _norm(e.horario),
                _norm(contrato_eff),
            )
            if key in seen[d]:
                break
            seen[d].add(key)

            agenda[d].append({
                "coop": coop,
                "cooperado_nome": nome_fallback or None,
                "nome_planilha": nome_show,
                "turno": (e.turno or "").strip(),
                "horario": (e.horario or "").strip(),
                "contrato": contrato_eff,
                "cor": (e.cor or "").strip(),
            })
            break

    for d in dias_list:
        agenda[d].sort(
            key=lambda x: (
                (x["contrato"] or "").lower(),
                (x.get("nome_planilha") or (x["coop"].nome if x["coop"] else "")).lower()
            )
        )

    # -------------------- Lista de lançamentos (aba "lancamentos") --------------------
    lancamentos_periodo = []
    total_lanc_valor = 0.0
    total_lanc_entregas = 0

    if view == "lancamentos":
        q = (
            db.session.query(Lancamento, Cooperado)
            .join(Cooperado, Cooperado.id == Lancamento.cooperado_id)
            .filter(
                Lancamento.restaurante_id == rest.id,
                Lancamento.data >= di,
                Lancamento.data <= df,
            )
            .order_by(Lancamento.data.asc(), Lancamento.id.asc())
        )
        for lanc, coop in q.all():
            item = {
                "id": lanc.id,
                "data": lanc.data.strftime("%d/%m/%Y") if lanc.data else "",
                "hora_inicio": (lanc.hora_inicio if isinstance(lanc.hora_inicio, str)
                                else (lanc.hora_inicio.strftime("%H:%M") if lanc.hora_inicio else "")),
                "hora_fim": (lanc.hora_fim if isinstance(lanc.hora_fim, str)
                             else (lanc.hora_fim.strftime("%H:%M") if lanc.hora_fim else "")),
                "qtd_entregas": lanc.qtd_entregas or 0,
                "valor": float(lanc.valor or 0.0),  # já em R$
                "cooperado_id": coop.id,
                "cooperado_nome": coop.nome,
                "contrato_nome": rest.nome,
            }
            lancamentos_periodo.append(item)

        total_lanc_valor = sum(x["valor"] for x in lancamentos_periodo)
        total_lanc_entregas = sum(x["qtd_entregas"] for x in lancamentos_periodo)

    # ---- URLs/flags para template
    from werkzeug.routing import BuildError
    try:
        url_lancar_producao = url_for("lancar_producao")
    except BuildError:
        url_lancar_producao = "/restaurante/lancar_producao"

    has_editar_lanc = ("editar_lancamento" in app.view_functions)

    # -------------------- Render --------------------
    return render_template(
        "restaurante_dashboard.html",
        rest=rest,
        cooperados=cooperados,
        filtro_inicio=di,
        filtro_fim=df,
        filtro_mes=(mes or ""),
        periodo_desc=periodo_desc,
        total_bruto=total_bruto,
        total_inss=total_inss,
        total_liquido=total_liquido,
        total_qtd=total_qtd,
        total_entregas=total_entregas,
        view=view,
        agenda=agenda,
        dias_list=dias_list,
        ref_data=ref,
        modo=modo,
        lancamentos_periodo=(lancamentos_periodo if view == "lancamentos" else []),
        total_lanc_valor=total_lanc_valor,
        total_lanc_entregas=total_lanc_entregas,
        url_lancar_producao=url_lancar_producao,
        has_editar_lanc=has_editar_lanc,
    )


# =========================
# Rotas de CRUD de lançamento
# =========================
@app.post("/restaurante/lancar_producao")
@role_required("restaurante")
def lancar_producao():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    if not rest:
        abort(403)
    f = request.form

    # 1) cria o lançamento
    l = Lancamento(
        restaurante_id=rest.id,
        cooperado_id=f.get("cooperado_id", type=int),
        descricao="produção",
        valor=f.get("valor", type=float),
        data=_parse_date(f.get("data")) or date.today(),
        hora_inicio=f.get("hora_inicio"),
        hora_fim=f.get("hora_fim"),
        qtd_entregas=f.get("qtd_entregas", type=int),
    )
    db.session.add(l)
    db.session.flush()  # garante l.id

    # 2) avaliação (opcional)
    g   = _clamp_star(f.get("av_geral"))
    p   = _clamp_star(f.get("av_pontualidade"))
    ed  = _clamp_star(f.get("av_educacao"))
    ef  = _clamp_star(f.get("av_eficiencia"))
    ap  = _clamp_star(f.get("av_apresentacao"))
    txt = (f.get("av_comentario") or "").strip()

    tem_avaliacao = any(x is not None for x in (g, p, ed, ef, ap)) or bool(txt)
    if tem_avaliacao:
        media = _media_ponderada(g, p, ed, ef, ap)
        senti = _analise_sentimento(txt)
        temas = _identifica_temas(txt)
        crise = _sinaliza_crise(g, txt)
        feed  = _gerar_feedback(p, ed, ef, ap, txt, senti)

        av = AvaliacaoCooperado(
            restaurante_id=rest.id,
            cooperado_id=l.cooperado_id,
            lancamento_id=l.id,
            estrelas_geral=g,
            estrelas_pontualidade=p,
            estrelas_educacao=ed,
            estrelas_eficiencia=ef,
            estrelas_apresentacao=ap,
            comentario=txt,
            media_ponderada=media,
            sentimento=senti,
            temas="; ".join(temas),
            alerta_crise=crise,
            feedback_motoboy=feed,
        )
        db.session.add(av)
        if crise:
            flash("⚠️ Avaliação crítica registrada (1★ + termo de risco). A gerência deve revisar.", "danger")

    db.session.commit()
    flash("Produção lançada" + (" + avaliação salva." if tem_avaliacao else "."), "success")
    return redirect(url_for("portal_restaurante", view="lancar"))

@app.route("/lancamentos/<int:id>/editar", methods=["GET", "POST"])
@role_required("restaurante")
def editar_lancamento(id):
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    l = Lancamento.query.get_or_404(id)
    if not rest or l.restaurante_id != rest.id:
        abort(403)

    if request.method == "POST":
        f = request.form
        l.valor = f.get("valor", type=float)
        l.data = _parse_date(f.get("data")) or l.data
        l.hora_inicio = f.get("hora_inicio")
        l.hora_fim = f.get("hora_fim")
        l.qtd_entregas = f.get("qtd_entregas", type=int)
        db.session.commit()
        flash("Lançamento atualizado.", "success")
        return redirect(url_for("portal_restaurante", view="lancamentos",
                                data_inicio=(l.data and l.data.strftime("%Y-%m-%d"))))

    return render_template("editar_lancamento.html", lanc=l)

@app.get("/lancamentos/<int:id>/excluir")
@role_required("restaurante")
def excluir_lancamento(id):
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    l = Lancamento.query.get_or_404(id)
    if not rest or l.restaurante_id != rest.id:
        abort(403)

    db.session.execute(sa_delete(AvaliacaoCooperado).where(AvaliacaoCooperado.lancamento_id == id))
    db.session.execute(sa_delete(AvaliacaoRestaurante).where(AvaliacaoRestaurante.lancamento_id == id))
    db.session.delete(l)
    db.session.commit()
    flash("Lançamento excluído.", "success")
    return redirect(url_for("portal_restaurante", view="lancamentos"))

# =========================
# Documentos (Admin + Público)
# =========================
@app.route("/admin/documentos")
@admin_required
def admin_documentos():
    documentos = Documento.query.order_by(Documento.enviado_em.desc()).all()
    return render_template("admin_documentos.html", documentos=documentos)

@app.post("/admin/documentos/upload")
@admin_required
def admin_upload_documento():
    f = request.form
    titulo = (f.get("titulo") or "").strip()
    categoria = (f.get("categoria") or "outro").strip()
    descricao = (f.get("descricao") or "").strip()
    arquivo = request.files.get("arquivo")

    if not titulo or not (arquivo and arquivo.filename):
        flash("Preencha o título e selecione o arquivo.", "warning")
        return redirect(url_for("admin_documentos"))

    # salva arquivo no diretório DOCS_DIR e cria URL estática
    fname = secure_filename(arquivo.filename)
    base, ext = os.path.splitext(fname)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_base = re.sub(r"[^A-Za-z0-9_-]+", "_", base).strip("_") or "arquivo"
    final_name = f"{safe_base}_{ts}{ext}"
    full_path = os.path.join(DOCS_DIR, final_name)
    arquivo.save(full_path)

    doc_url = f"/static/uploads/docs/{final_name}"

    d = Documento(
        titulo=titulo,
        categoria=categoria,
        descricao=descricao,
        arquivo_url=doc_url,
        arquivo_nome=fname,
        enviado_em=datetime.utcnow(),
    )
    db.session.add(d)
    db.session.commit()    
    flash("Documento enviado.", "success")
    return redirect(url_for("admin_documentos"))

@app.get("/admin/documentos/<int:doc_id>/delete")
@admin_required
def admin_delete_documento(doc_id):
    d = Documento.query.get_or_404(doc_id)
    try:
        local_path = os.path.join(BASE_DIR, d.arquivo_url.lstrip("/"))
        if os.path.exists(local_path):
            os.remove(local_path)
    except Exception:
        pass
    db.session.delete(d)
    db.session.commit()
    flash("Documento removido.", "success")
    return redirect(url_for("admin_documentos"))

@app.route("/documentos")
def documentos_publicos():
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login"))
    documentos = Documento.query.order_by(Documento.enviado_em.desc()).all()
    return render_template("documentos_publicos.html", documentos=documentos)

@app.route('/documentos/<int:doc_id>/baixar')
def baixar_documento(doc_id):
    doc = Documento.query.get_or_404(doc_id)
    path = os.path.join(BASE_DIR, doc.arquivo_url.lstrip("/"))
    if not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=doc.arquivo_nome)

# =========================
# Inicialização automática do DB em servidores (Gunicorn/Render)
# =========================
try:
    with app.app_context():
        init_db()
except Exception as _e:
    # Evita crash no import; logs úteis no servidor
    try:
        app.logger.warning(f"Falha ao inicializar DB: {_e}")
    except Exception:
        pass


@app.errorhandler(413)
def too_large(e):
    flash("Arquivo excede o tamanho máximo permitido (32MB).", "danger")
    return redirect(url_for('admin_documentos'))

# =========================
# Inicialização automática do DB em servidores (Gunicorn/Render)
# =========================
try:
    with app.app_context():
        init_db()
except Exception as _e:
    ...

# ==== TABELAS (upload/abrir/baixar) =========================================
from flask import (
    render_template, request, redirect, url_for, flash, session,
    send_file, abort, current_app, jsonify
)
from werkzeug.utils import secure_filename
from datetime import datetime
from pathlib import Path
import os, re, unicodedata, mimetypes, logging

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BASE_DIR e TABELAS_DIR (sempre salva/serve de static/uploads/tabelas)
# ---------------------------------------------------------------------------
try:
    BASE_DIR  # type: ignore[name-defined]
except NameError:
    BASE_DIR = Path(__file__).resolve().parent

# SEMPRE neste local:
TABELAS_DIR = str(Path(BASE_DIR) / "static" / "uploads" / "tabelas")

# Requer no app principal:
# - db (SQLAlchemy)
# - modelos: Tabela(id, titulo, descricao?, arquivo_url, arquivo_nome?, enviado_em)
#            Restaurante(usuario_id, nome?, usuario?, usuario_ref?.usuario?)
# - decorators: admin_required, role_required (se usar avisos/portais)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _tabelas_base_dir() -> Path:
    p = Path(TABELAS_DIR)
    p.mkdir(parents=True, exist_ok=True)
    return p

def _norm_txt(s: str) -> str:
    s = unicodedata.normalize("NFD", (s or "").strip())
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s.lower()

def _guess_mimetype_from_path(path: str) -> str:
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"

def _enforce_restaurante_titulo(tabela, restaurante):
    """
    Regra: restaurante só acessa a tabela cujo TÍTULO == NOME/LOGIN do restaurante (normalizado).
    """
    login_nome = (
        getattr(getattr(restaurante, "usuario_ref", None), "usuario", None)
        or getattr(restaurante, "usuario", None)
        or (restaurante.nome or "")
    )
    if _norm_txt(tabela.titulo) != _norm_txt(login_nome):
        abort(403)

def _serve_tabela_or_redirect(tabela, *, as_attachment: bool):
    """
    Resolve e serve o arquivo da Tabela:
    - http(s) => redirect
    - sempre tenta primeiro static/uploads/tabelas/<arquivo>
    - aceita absoluto, relativo e só o nome
    - ignora querystring/fragments (ex.: foo.pdf?v=123#x)
    """
    url = (tabela.arquivo_url or "").strip()
    if not url:
        abort(404)

    # URL externa
    if url.startswith(("http://", "https://")):
        return redirect(url)

    base_dir    = Path(BASE_DIR)
    tabelas_dir = _tabelas_base_dir()

    # normaliza: remove "/" inicial, query e fragment
    raw = url.lstrip("/")
    raw_no_q = raw.split("?", 1)[0].split("#", 1)[0]
    fname = (raw_no_q.split("/")[-1] if raw_no_q else "").strip()

    candidates = []

    # 1) SEMPRE prioriza nosso diretório oficial
    if fname:
        candidates.append(tabelas_dir / fname)

    # 2) Como veio, relativo ao BASE_DIR (compat c/ legado: static/uploads/tabelas/...)
    candidates.append(base_dir / raw_no_q)

    # 3) Absoluto (se alguém gravou caminho completo por engano)
    p = Path(url)
    if p.is_absolute():
        candidates.append(p)

    # 4) Mais dois legados comuns
    if fname:
        candidates.append(base_dir / "uploads" / "tabelas" / fname)
        candidates.append(base_dir / "static" / "uploads" / "tabelas" / fname)

    file_path = next((c for c in candidates if c.exists() and c.is_file()), None)
    if not file_path:
        try:
            log.warning(
                "Arquivo de Tabela não encontrado. id=%s titulo=%r arquivo_url=%r tents=%r",
                getattr(tabela, "id", None),
                getattr(tabela, "titulo", None),
                tabela.arquivo_url,
                [str(c) for c in candidates],
            )
        except Exception:
            pass
        abort(404)

    return send_file(
        str(file_path),
        as_attachment=as_attachment,
        download_name=(tabela.arquivo_nome or file_path.name),
        mimetype=_guess_mimetype_from_path(str(file_path)),
    )

# ---------------------------------------------------------------------------
# Admin: listar / upload / delete
# ---------------------------------------------------------------------------
@app.get("/admin/tabelas", endpoint="admin_tabelas")
@admin_required
def admin_tabelas():
    tabelas = Tabela.query.order_by(Tabela.enviado_em.desc(), Tabela.id.desc()).all()
    return render_template("admin_tabelas.html", tabelas=tabelas)

@app.post("/admin/tabelas/upload", endpoint="admin_upload_tabela")
@admin_required
def admin_upload_tabela():
    f = request.form
    titulo    = (f.get("titulo") or "").strip()
    descricao = (f.get("descricao") or "").strip() or None

    # aceita vários nomes possíveis do input file
    arquivo = (
        request.files.get("arquivo")
        or request.files.get("file")
        or request.files.get("tabela")
    )

    if not titulo or not (arquivo and arquivo.filename):
        flash("Preencha o título e selecione o arquivo.", "warning")
        return redirect(url_for("admin_tabelas"))

    base_dir = _tabelas_base_dir()

    # nome seguro + timestamp pra não colidir
    raw = secure_filename(arquivo.filename)
    stem, ext = os.path.splitext(raw)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_stem = re.sub(r"[^A-Za-z0-9_.-]+", "-", stem) or "arquivo"
    final_name = f"{safe_stem}_{ts}{ext or ''}"

    dest = base_dir / final_name
    arquivo.save(str(dest))

    t = Tabela(
        titulo=titulo,
        descricao=descricao,
        # Importante: gravar apenas o NOME, não o caminho.
        # O _serve_tabela_or_redirect vai resolver para TABELAS_DIR.
        arquivo_url=final_name,
        arquivo_nome=arquivo.filename,
        enviado_em=datetime.utcnow(),
    )
    db.session.add(t)
    db.session.commit()

    flash("Tabela publicada.", "success")
    return redirect(url_for("admin_tabelas"))

@app.get("/admin/tabelas/<int:tab_id>/delete", endpoint="admin_delete_tabela")
@admin_required
def admin_delete_tabela(tab_id: int):
    t = Tabela.query.get_or_404(tab_id)
    try:
        url = (t.arquivo_url or "").strip()
        if url and not url.startswith(("http://", "https://")):
            path = _tabelas_base_dir() / (url.split("/")[-1])
            if path.exists():
                path.unlink()
    except Exception:
        pass
    db.session.delete(t)
    db.session.commit()
    flash("Tabela excluída.", "success")
    return redirect(url_for("admin_tabelas"))

# ---------------------------------------------------------------------------
# Cooperado/Admin/Restaurante: listagem (cooperado vê TODAS)
# Mantém endpoints esperados pelo HTML: 'tabela_abrir' e 'baixar_tabela'
# ---------------------------------------------------------------------------
@app.get("/tabelas", endpoint="tabelas_publicas")
def tabelas_publicas():
    if session.get("user_tipo") not in {"admin", "cooperado", "restaurante"}:
        return redirect(url_for("login"))

    tabs = Tabela.query.order_by(Tabela.enviado_em.desc(), Tabela.id.desc()).all()

    items = [{
        "id": t.id,
        "titulo": t.titulo,
        "descricao": getattr(t, "descricao", None),
        "enviado_em": t.enviado_em,
        "arquivo_nome": getattr(t, "arquivo_nome", None),
        "abrir_url":  url_for("tabela_abrir",  tab_id=t.id),
        "baixar_url": url_for("baixar_tabela", tab_id=t.id),
    } for t in tabs]

    back_href = url_for("portal_cooperado") if (
        session.get("user_tipo") == "cooperado" and "portal_cooperado" in current_app.view_functions
    ) else ""

    return render_template("tabelas_publicas.html", tabelas=tabs, items=items, back_href=back_href)

# ---------------------------------------------------------------------------
# Abrir / Baixar compartilhado (cooperado/admin/restaurante)
# Endpoints compatíveis com HTML existente:
#   - 'tabela_abrir'
#   - 'baixar_tabela'
# ---------------------------------------------------------------------------
@app.get("/tabelas/<int:tab_id>/abrir", endpoint="tabela_abrir")
def tabela_abrir(tab_id: int):
    if session.get("user_tipo") not in {"admin", "cooperado", "restaurante"}:
        return redirect(url_for("login"))
    t = Tabela.query.get_or_404(tab_id)

    if session.get("user_tipo") == "restaurante":
        rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
        _enforce_restaurante_titulo(t, rest)

    return _serve_tabela_or_redirect(t, as_attachment=False)

@app.get("/tabelas/<int:tab_id>/baixar", endpoint="baixar_tabela")
def tabela_baixar(tab_id: int):
    if session.get("user_tipo") not in {"admin", "cooperado", "restaurante"}:
        return redirect(url_for("login"))
    t = Tabela.query.get_or_404(tab_id)

    if session.get("user_tipo") == "restaurante":
        rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
        _enforce_restaurante_titulo(t, rest)

    return _serve_tabela_or_redirect(t, as_attachment=True)

# ---------------------------------------------------------------------------
# Restaurante: vê/abre/baixa SOMENTE a própria tabela
# Endpoints compatíveis com HTML existente:
#   - 'rest_tabela_abrir'
#   - 'rest_tabela_download'
# ---------------------------------------------------------------------------
@app.get("/rest/tabelas", endpoint="rest_tabelas")
def rest_tabelas():
    if session.get("user_tipo") != "restaurante":
        return redirect(url_for("login"))

    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first_or_404()

    login_nome = (
        getattr(getattr(rest, "usuario_ref", None), "usuario", None)
        or getattr(rest, "usuario", None)
        or (rest.nome or "")
    )
    alvo_norm = _norm_txt(login_nome)

    candidatos = Tabela.query.order_by(Tabela.enviado_em.desc()).all()
    tabela_exata = next((t for t in candidatos if _norm_txt(t.titulo) == alvo_norm), None)

    has_portal_restaurante = ("portal_restaurante" in current_app.view_functions)

    return render_template(
        "restaurantes_tabelas.html",
        restaurante=rest,
        login_nome=login_nome,
        tabela=tabela_exata,  # o template deve lidar com None (sem tabela)
        has_portal_restaurante=has_portal_restaurante,
        back_href=url_for("portal_restaurante") if has_portal_restaurante else url_for("rest_tabelas"),
        current_year=datetime.utcnow().year,
    )

@app.get("/rest/tabelas/<int:tabela_id>/abrir", endpoint="rest_tabela_abrir")
def rest_tabela_abrir(tabela_id: int):
    if session.get("user_tipo") != "restaurante":
        return redirect(url_for("login"))
    rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    t = Tabela.query.get_or_404(tabela_id)
    _enforce_restaurante_titulo(t, rest)
    return _serve_tabela_or_redirect(t, as_attachment=False)

@app.get("/rest/tabelas/<int:tabela_id>/download", endpoint="rest_tabela_download")
def rest_tabela_download(tabela_id: int):
    if session.get("user_tipo") != "restaurante":
        return redirect(url_for("login"))
    rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    t = Tabela.query.get_or_404(tabela_id)
    _enforce_restaurante_titulo(t, rest)
    return _serve_tabela_or_redirect(t, as_attachment=True)

# ---------------------------------------------------------------------------
# Diagnóstico rápido (admin)
# ---------------------------------------------------------------------------
@app.get("/admin/tabelas/scan", endpoint="admin_tabelas_scan")
@admin_required
def admin_tabelas_scan():
    base = _tabelas_base_dir()
    items = []
    for t in Tabela.query.order_by(Tabela.enviado_em.desc()).all():
        url = (t.arquivo_url or "").strip()
        # sempre tentar resolver como gravamos hoje (fname dentro de static/uploads/tabelas)
        fname = (url.split("?", 1)[0].split("#", 1)[0]).split("/")[-1] if url else ""
        resolved = str(base / fname) if fname else url
        exists = (Path(resolved).exists() if fname else False)
        items.append({
            "id": t.id,
            "titulo": t.titulo,
            "arquivo_nome": t.arquivo_nome,
            "arquivo_url": t.arquivo_url,
            "resolved": resolved,
            "exists": bool(exists),
        })
    return jsonify({"tabelas_dir": str(base), "items": items})

# ---------------------------------------------------------------------------
# (Opcional) Normalizador: deixa arquivo_url só com o NOME do arquivo
# Use se no seu banco ficaram caminhos tipo "static/uploads/tabelas/foo.pdf"
# ---------------------------------------------------------------------------
@app.post("/admin/tabelas/normalize-arquivo-url", endpoint="admin_tabelas_normalize_arquivo_url")
@admin_required
def admin_tabelas_normalize_arquivo_url():
    alterados = 0
    for t in Tabela.query.all():
        url = (t.arquivo_url or "").strip()
        if not url or url.startswith(("http://", "https://")):
            continue
        fname = url.split("?", 1)[0].split("#", 1)[0].split("/")[-1]
        if fname and fname != url:
            t.arquivo_url = fname
            alterados += 1
    if alterados:
        db.session.commit()
    return jsonify({"ok": True, "alterados": alterados})

# =========================
# AVISOS — Ações (cooperado)
# =========================

@app.post("/avisos/<int:aviso_id>/lido", endpoint="marcar_aviso_lido")
@role_required("cooperado")
def marcar_aviso_lido(aviso_id: int):
    u_id = session.get("user_id")
    coop = Cooperado.query.filter_by(usuario_id=u_id).first_or_404()

    aviso = Aviso.query.get_or_404(aviso_id)

    ja_lido = AvisoLeitura.query.filter_by(
        cooperado_id=coop.id, aviso_id=aviso.id
    ).first()

    if not ja_lido:
        db.session.add(AvisoLeitura(
            cooperado_id=coop.id,
            aviso_id=aviso.id,
            lido_em=datetime.utcnow(),
        ))
        db.session.commit()

    # volta para a lista; se quiser voltar ancorado: + f"#aviso-{aviso.id}"
    return redirect(url_for("portal_cooperado_avisos"))

@app.post("/avisos/marcar-todos", endpoint="marcar_todos_avisos_lidos")
@role_required("cooperado")
def marcar_todos_avisos_lidos():
    u_id = session.get("user_id")
    coop = Cooperado.query.filter_by(usuario_id=u_id).first_or_404()

    # todos avisos visíveis ao cooperado
    avisos = get_avisos_for_cooperado(coop)

    # ids já lidos
    lidos_ids = {
        a_id for (a_id,) in db.session.query(AvisoLeitura.aviso_id)
        .filter(AvisoLeitura.cooperado_id == coop.id).all()
    }

    # persiste só os que faltam
    now = datetime.utcnow()
    for a in avisos:
        if a.id not in lidos_ids:
            db.session.add(AvisoLeitura(
                cooperado_id=coop.id,
                aviso_id=a.id,
                lido_em=now,
            ))

    db.session.commit()
    return redirect(url_for("portal_cooperado_avisos"))

@app.get("/portal/restaurante/avisos")
@role_required("restaurante")
def portal_restaurante_avisos():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first_or_404()

    # avisos aplicáveis
    try:
        avisos_db = get_avisos_for_restaurante(rest)
    except NameError:
        # fallback: global + restaurante (associados ou broadcast)
        avisos_db = (Aviso.query
                     .filter(Aviso.ativo.is_(True))
                     .filter(or_(Aviso.tipo == "global", Aviso.tipo == "restaurante"))
                     .order_by(Aviso.fixado.desc(), Aviso.criado_em.desc())
                     .all())

    # ids já lidos
    lidos_ids = {
        a_id for (a_id,) in db.session.query(AvisoLeitura.aviso_id)
        .filter(AvisoLeitura.restaurante_id == rest.id).all()
    }

    def corpo_do_aviso(a: Aviso) -> str:
        for k in ("corpo_html","html","conteudo_html","mensagem_html","descricao_html","texto_html",
                  "corpo","conteudo","mensagem","descricao","texto","resumo","body","content"):
            v = getattr(a, k, None)
            if isinstance(v, str) and v.strip():
                return v
        return ""

    avisos = [{
        "id": a.id,
        "titulo": a.titulo or "Aviso",
        "criado_em": a.criado_em,
        "lido": (a.id in lidos_ids),
        "prioridade_alta": (str(a.prioridade or "").lower() == "alta"),
        "corpo_html": corpo_do_aviso(a),
    } for a in avisos_db]

    avisos_nao_lidos_count = sum(1 for x in avisos if not x["lido"])
    return render_template(
        "portal_restaurante_avisos.html",   # crie/clone seu template
        avisos=avisos,
        avisos_nao_lidos_count=avisos_nao_lidos_count,
        current_year=datetime.now().year,
    )

@app.post("/avisos-restaurante/marcar-todos", endpoint="marcar_todos_avisos_lidos_restaurante")
@role_required("restaurante")
def marcar_todos_avisos_lidos_restaurante():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first_or_404()

    try:
        avisos = get_avisos_for_restaurante(rest)
    except NameError:
        avisos = (Aviso.query
                  .filter(Aviso.ativo.is_(True))
                  .filter(or_(Aviso.tipo == "global", Aviso.tipo == "restaurante"))
                  .all())

    lidos_ids = {
        a_id for (a_id,) in db.session.query(AvisoLeitura.aviso_id)
        .filter(AvisoLeitura.restaurante_id == rest.id).all()
    }

    now = datetime.utcnow()
    for a in avisos:
        if a.id not in lidos_ids:
            db.session.add(AvisoLeitura(
                restaurante_id=rest.id, aviso_id=a.id, lido_em=now
            ))
    db.session.commit()
    return redirect(url_for("portal_restaurante_avisos"))

# =========================
# Main
# =========================
if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))


