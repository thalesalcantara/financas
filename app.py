from __future__ import annotations

import os
import csv
import io
import re
import json
import difflib
import unicodedata
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
app.secret_key = os.environ.get("SECRET_KEY", "coopex-secret")
app.config["SQLALCHEMY_DATABASE_URI"] = _build_db_uri()
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["JSON_SORT_KEYS"] = False  # evita comparar None com int ao serializar |tojson

db = SQLAlchemy(app)

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
    cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=True)  # pode não ter cadastro
    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=True)

    data = db.Column(db.String(40))
    turno = db.Column(db.String(50))
    horario = db.Column(db.String(50))
    contrato = db.Column(db.String(80))
    cor = db.Column(db.String(200))
    cooperado_nome = db.Column(db.String(120))  # nome bruto da planilha quando não há cadastro


class TrocaSolicitacao(db.Model):
    __tablename__ = "trocas"
    id = db.Column(db.Integer, primary_key=True)
    solicitante_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=False)
    destino_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=False)
    origem_escala_id = db.Column(db.Integer, db.ForeignKey("escalas.id"), nullable=False)
    mensagem = db.Column(db.Text)  # guarda texto e, quando aprovada, um sufixo __AFETACAO_JSON__:{...}
    status = db.Column(db.String(20), default="pendente")  # pendente | aprovada | recusada
    criada_em = db.Column(db.DateTime, default=datetime.utcnow)
    aplicada_em = db.Column(db.DateTime)


class Config(db.Model):
    __tablename__ = "config"
    id = db.Column(db.Integer, primary_key=True)
    salario_minimo = db.Column(db.Float, default=0.0)

# =========================
# Helpers
# =========================
def init_db():
    db.create_all()

    # --- qtd_entregas em lancamentos ---
    try:
        cols = db.session.execute(sa_text("PRAGMA table_info(lancamentos);")).fetchall()
        colnames = {row[1] for row in cols}
        if "qtd_entregas" not in colnames:
            db.session.execute(sa_text("ALTER TABLE lancamentos ADD COLUMN qtd_entregas INTEGER"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # --- cooperado_nome em escalas ---
    try:
        cols = db.session.execute(sa_text("PRAGMA table_info(escalas);")).fetchall()
        colnames = {row[1] for row in cols}
        if "cooperado_nome" not in colnames:
            db.session.execute(sa_text("ALTER TABLE escalas ADD COLUMN cooperado_nome VARCHAR(120)"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # --- restaurante_id em escalas ---
    try:
        cols = db.session.execute(sa_text("PRAGMA table_info(escalas);")).fetchall()
        colnames = {row[1] for row in cols}
        if "restaurante_id" not in colnames:
            db.session.execute(sa_text("ALTER TABLE escalas ADD COLUMN restaurante_id INTEGER"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # --- usuário admin + config padrão ---
    if not Usuario.query.filter_by(tipo="admin").first():
        admin = Usuario(usuario="coopex", tipo="admin", senha_hash="")
        admin.set_password("coopex05289")
        db.session.add(admin)
        db.session.commit()

    if not Config.query.get(1):
        db.session.add(Config(id=1, salario_minimo=0.0))
        db.session.commit()


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
        if len(hits) == 1:
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
    if not file_storage:
        return None
    fname = secure_filename(file_storage.filename or "")
    if not fname:
        return None
    path = os.path.join(UPLOAD_DIR, fname)
    file_storage.save(path)
    return f"/static/uploads/{fname}"


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


def _dow(dt: date) -> str:
    return str((dt.weekday() % 7) + 1)

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
            payload = json.loads(raw.replace("'", '"'))
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
    db.session.delete(l)
    db.session.commit()
    flash("Lançamento excluído.", "success")
    return redirect(url_for("admin_dashboard", tab="lancamentos"))

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
    r.valor = f.get("valor", type=float)
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

    foto_url = _save_upload(foto)
    c = Cooperado(nome=nome, usuario_id=u.id, foto_url=foto_url, ultima_atualizacao=datetime.now())
    db.session.add(c)
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
        c.foto_url = _save_upload(foto)
    c.ultima_atualizacao = datetime.now()
    db.session.commit()
    flash("Cooperado atualizado.", "success")
    return redirect(url_for("admin_dashboard", tab="cooperados"))

@app.route("/cooperados/<int:id>/delete")
@admin_required
def delete_cooperado(id):
    c = Cooperado.query.get_or_404(id)
    u = c.usuario_ref
    db.session.delete(c)
    if u:
        db.session.delete(u)
    db.session.commit()
    flash("Cooperado excluído.", "success")
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
    foto_url = _save_upload(foto)
    r = Restaurante(nome=nome, periodo=periodo, usuario_id=u.id, foto_url=foto_url)
    db.session.add(r)
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
        r.foto_url = _save_upload(foto)
    db.session.commit()
    flash("Estabelecimento atualizado.", "success")
    return redirect(url_for("admin_dashboard", tab="restaurantes"))

@app.route("/restaurantes/<int:id>/delete")
@admin_required
def delete_restaurante(id):
    r = Restaurante.query.get_or_404(id)
    u = r.usuario_ref
    db.session.delete(r)
    if u:
        db.session.delete(u)
    db.session.commit()
    flash("Estabelecimento excluído.", "success")
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

# =========================
# Escalas — Upload
# =========================
@app.route("/escalas/upload", methods=["POST"])
@admin_required
def upload_escala():
    file = request.files.get("file")
    if not file or not file.filename.lower().endswith(".xlsx"):
        flash("Envie um arquivo .xlsx válido.", "warning")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    path = os.path.join(UPLOAD_DIR, secure_filename(file.filename))
    file.save(path)

    try:
        import openpyxl
    except Exception:
        flash("Arquivo salvo, mas falta a biblioteca 'openpyxl' (pip install openpyxl).", "warning")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active

    import unicodedata as _u, re as _re

    def _norm_local(s: str) -> str:
        s = _u.normalize("NFD", str(s or "").strip().lower())
        s = "".join(ch for ch in s if _u.category(ch) != "Mn")
        return _re.sub(r"[^a-z0-9]+", " ", s).strip()

    def to_css_color_local(v: str) -> str:
        t = str(v or "").strip()
        if not t:
            return ""
        t_low = t.lower().strip()
        if _re.fullmatch(r"[0-9a-fA-F]{8}", t):
            a = int(t[0:2], 16) / 255.0
            r = int(t[2:4], 16); g = int(t[4:6], 16); b = int(t[6:8], 16)
            return f"rgba({r},{g},{b},{a:.3f})"
        if _re.fullmatch(r"[0-9a-fA-F]{6}", t):
            return f"#{t}"
        if _re.fullmatch(r"#?[0-9a-fA-F]{6,8}", t):
            if not t.startswith("#"):
                t = f"#{t}"
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
        return mapa.get(t_low, t)

    headers_norm = { _norm_local(str(cell.value or "")) : j for j, cell in enumerate(ws[1], start=1) }

    def find_col(*aliases):
        n_aliases = [_norm_local(a) for a in aliases]
        for a in n_aliases:
            if a in headers_norm:
                return headers_norm[a]
        for k_norm, j in headers_norm.items():
            for a in n_aliases:
                if a and a in k_norm:
                    return j
        return None

    # Data | QTD | Turno | Horário | Contrato | NOME DO COOPERADO | [Cor]
    col_data     = find_col("data", "dia", "data do plantao")
    col_qtd      = find_col("qtd", "quantidade")  # opcional
    col_turno    = find_col("turno")
    col_horario  = find_col("horario", "horário", "hora", "periodo", "período")
    col_contrato = find_col("contrato", "restaurante", "unidade")
    col_nome     = find_col("nome do cooperado", "cooperado", "nome", "colaborador")
    col_cor      = find_col("cor", "cores", "cor da celula", "cor celula")

    if not col_nome:
        flash("Não encontrei a coluna de nome do cooperado na planilha.", "danger")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    restaurantes = Restaurante.query.order_by(Restaurante.nome).all()
    def match_restaurante_id(contrato_txt: str) -> int | None:
        a = _norm_local(contrato_txt)
        if not a:
            return None
        for r in restaurantes:
            b = _norm_local(r.nome)
            if a == b or a in b or b in a:
                return r.id
        try:
            import difflib as _dif
            nomes_norm = [_norm_local(r.nome) for r in restaurantes]
            close = _dif.get_close_matches(a, nomes_norm, n=1, cutoff=0.87)
            if close:
                alvo = close[0]
                for r in restaurantes:
                    if _norm_local(r.nome) == alvo:
                        return r.id
        except Exception:
            pass
        return None

    cooperados = Cooperado.query.order_by(Cooperado.nome).all()

    def fmt_data_cell(v) -> str:
        if v is None or str(v).strip() == "":
            return ""
        if isinstance(v, datetime):
            return v.date().strftime("%d/%m/%Y")
        if isinstance(v, date):
            return v.strftime("%d/%m/%Y")
        s = str(v).strip()
        m = _re.fullmatch(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", s)
        if m:
            y, mth, d = map(int, m.groups())
            try:
                return date(y, mth, d).strftime("%d/%m/%Y")
            except Exception:
                return s
        return s

    linhas_novas: list[dict] = []
    coops_na_planilha: set[int] = set()
    total_linhas_planilha = 0

    for i in range(2, ws.max_row + 1):
        nome_raw = ws.cell(i, col_nome).value if col_nome else None
        nome = (str(nome_raw).strip() if nome_raw is not None else "")
        if not nome:
            continue
        total_linhas_planilha += 1

        data_v     = ws.cell(i, col_data).value     if col_data     else None
        turno_v    = ws.cell(i, col_turno).value    if col_turno    else None
        horario_v  = ws.cell(i, col_horario).value  if col_horario  else None
        contrato_v = ws.cell(i, col_contrato).value if col_contrato else None
        cor_v      = ws.cell(i, col_cor).value      if col_cor      else None
        _qtd       = ws.cell(i, col_qtd).value      if col_qtd      else None  # ignorado

        contrato_txt = (str(contrato_v).strip() if contrato_v is not None else "")
        data_txt     = fmt_data_cell(data_v)
        turno_txt    = (str(turno_v).strip() if turno_v is not None else "")
        horario_txt  = (str(horario_v).strip() if horario_v is not None else "")
        cor_txt      = to_css_color_local(cor_v)
        rest_id      = match_restaurante_id(contrato_txt)

        match = _match_cooperado_by_name(nome, cooperados)
        payload = {
            "cooperado_id":   (match.id if match else None),
            "cooperado_nome": (None if match else nome),
            "data":           data_txt,
            "turno":          turno_txt,
            "horario":        horario_txt,
            "contrato":       contrato_txt,
            "cor":            cor_txt,
            "restaurante_id": rest_id,
        }
        if match:
            coops_na_planilha.add(match.id)
        linhas_novas.append(payload)

    if not linhas_novas:
        flash("Nada importado: nenhum registro válido encontrado.", "warning")
        return redirect(url_for("admin_dashboard", tab="escalas"))

    try:
        deleted = 0
        if coops_na_planilha:
            res = db.session.execute(
                sa_delete(Escala).where(Escala.cooperado_id.in_(list(coops_na_planilha)))
            )
            deleted = res.rowcount or 0

        for row in linhas_novas:
            db.session.add(Escala(**row))

        for cid in coops_na_planilha:
            c = Cooperado.query.get(cid)
            if c:
                c.ultima_atualizacao = datetime.now()

        db.session.commit()
        msg = (
            f"Escala importada. {len(linhas_novas)} linha(s) adicionada(s). "
            f"{deleted} escala(s) antigas removidas para {len(coops_na_planilha)} cooperado(s) reconhecido(s)."
        )
        if total_linhas_planilha > 0 and len(linhas_novas) < total_linhas_planilha:
            msg += f" (Linhas processadas: {total_linhas_planilha})"
        flash(msg, "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Erro ao importar a escala: {e}", "danger")

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

    di = _parse_date(request.args.get("data_inicio"))
    df = _parse_date(request.args.get("data_fim"))

    def in_range(qs, col):
        if di:
            qs = qs.filter(col >= di)
        if df:
            qs = qs.filter(col <= df)
        return qs

    ql = in_range(Lancamento.query.filter_by(cooperado_id=coop.id), Lancamento.data)
    producoes = ql.order_by(Lancamento.data.desc(), Lancamento.id.desc()).all()

    qr = in_range(ReceitaCooperado.query.filter_by(cooperado_id=coop.id), ReceitaCooperado.data)
    receitas_coop = qr.order_by(ReceitaCooperado.data.desc(), ReceitaCooperado.id.desc()).all()

    qd = in_range(DespesaCooperado.query.filter_by(cooperado_id=coop.id), DespesaCooperado.data)
    despesas_coop = qd.order_by(DespesaCooperado.data.desc(), DespesaCooperado.id.desc()).all()

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

    view = request.args.get("view", "lancar")  # 'lancar' ou 'escalas'

    # -------------------- LANÇAMENTOS --------------------
    di = _parse_date(request.args.get("data_inicio"))
    df = _parse_date(request.args.get("data_fim"))
    if not di or not df:
        wd_map = {"seg-dom": 0, "sab-sex": 5, "sex-qui": 4}  # seg=0 ... dom=6
        start_wd = wd_map.get(rest.periodo, 0)
        hoje = date.today()
        delta = (hoje.weekday() - start_wd) % 7
        di_auto = hoje - timedelta(days=delta)
        df_auto = di_auto + timedelta(days=6)
        di = di or di_auto
        df = df or df_auto
        periodo_desc = rest.periodo
    else:
        periodo_desc = "personalizado"

    cooperados = Cooperado.query.order_by(Cooperado.nome).all()

    total_bruto = 0.0
    total_qtd = 0
    total_entregas = 0
    for c in cooperados:
        q = (Lancamento.query
             .filter_by(restaurante_id=rest.id, cooperado_id=c.id)
             .filter(Lancamento.data >= di, Lancamento.data <= df)
             .order_by(Lancamento.data.desc(), Lancamento.id.desc()))
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

    escalas_rest = [e for e in escalas_all if contrato_bate_restaurante(eff_map.get(e.id, e.contrato or ""), rest.nome)]
    if not escalas_rest:
        escalas_rest = [e for e in escalas_all if (e.contrato or "").strip() == rest.nome.strip()]

    agenda = {d: [] for d in dias_list}
    seen = {d: set() for d in dias_list}  # evita duplicar (mesmo nome/turno/horario/contrato no mesmo dia)

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

            # chave de dedupe por dia
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
        agenda[d].sort(key=lambda x: ((x["contrato"] or "").lower(),
                                      (x.get("nome_planilha") or (x["coop"].nome if x["coop"] else "")).lower()))

    return render_template(
        "restaurante_dashboard.html",
        rest=rest,
        cooperados=cooperados,
        filtro_inicio=di,
        filtro_fim=df,
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
    )

@app.post("/restaurante/lancar_producao")
@role_required("restaurante")
def lancar_producao():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    if not rest:
        abort(403)
    f = request.form
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
    db.session.commit()
    flash("Produção lançada.", "success")
    return redirect(url_for("portal_restaurante"))

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
        return redirect(url_for("portal_restaurante"))

    return render_template("editar_lancamento.html", lanc=l)

@app.get("/lancamentos/<int:id>/excluir")
@role_required("restaurante")
def excluir_lancamento(id):
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    l = Lancamento.query.get_or_404(id)
    if not rest or l.restaurante_id != rest.id:
        abort(403)
    db.session.delete(l)
    db.session.commit()
    flash("Lançamento excluído.", "success")
    return redirect(url_for("portal_restaurante"))

# =========================
# Main
# =========================
if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
