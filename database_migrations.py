"""Migrações e bootstrap explícito do banco COOPEX.

Este módulo não roda automaticamente no import. Ele é carregado apenas quando
app.init_db() é chamado, reduzindo o trabalho de boot dos workers.
"""
from __future__ import annotations


def run_init_db(namespace: dict):
    # Usa os mesmos modelos/helpers já inicializados pelo app principal,
    # preservando integralmente as regras antigas de migração.
    globals().update(namespace)
    """
    Versão unificada e idempotente:
      1) Ajustes de performance para SQLite (WAL/synchronous)
      2) Criação de todas as tabelas (create_all)
      3) Índices úteis (cooperado/restaurante/criado_em)
      4) Migrações leves (qtd_entregas, ativo em usuarios, colunas de escalas, fotos, tabela avaliacoes_restaurante)
      5) Bootstrap mínimo (admin e config) — só se os modelos existirem
    """
    
    # 1) Perf no SQLite
    try:
        if _is_sqlite():
            db.session.execute(sa_text("PRAGMA journal_mode=WAL;"))
            db.session.execute(sa_text("PRAGMA synchronous=NORMAL;"))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 2) Tabelas/mapeamentos
    try:
        db.create_all()
    except Exception:
        db.session.rollback()
    
    # 3) Índices de performance (idempotentes)
    try:
        if _is_sqlite():
            stmts = [
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_criado_em   ON avaliacoes (criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_rest_criado ON avaliacoes (restaurante_id, criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_coop_criado ON avaliacoes (cooperado_id,  criado_em)",
    
                "CREATE INDEX IF NOT EXISTS ix_av_rest_criado_em      ON avaliacoes_restaurante (criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_av_rest_rest_criado    ON avaliacoes_restaurante (restaurante_id, criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_av_rest_coop_criado    ON avaliacoes_restaurante (cooperado_id,  criado_em)",
            ]
        else:
            stmts = [
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_criado_em   ON public.avaliacoes (criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_rest_criado ON public.avaliacoes (restaurante_id, criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_coop_criado ON public.avaliacoes (cooperado_id,  criado_em)",
    
                "CREATE INDEX IF NOT EXISTS ix_av_rest_criado_em      ON public.avaliacoes_restaurante (criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_av_rest_rest_criado    ON public.avaliacoes_restaurante (restaurante_id, criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_av_rest_coop_criado    ON public.avaliacoes_restaurante (cooperado_id,  criado_em)",
            ]
    
        for sql in stmts:
            db.session.execute(sa_text(sql))
        db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 3.9) is_master em usuarios
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(usuarios);")).fetchall()
            colnames = {row[1] for row in cols}
            if "is_master" not in colnames:
                db.session.execute(sa_text("ALTER TABLE usuarios ADD COLUMN is_master BOOLEAN DEFAULT 0"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.usuarios
                ADD COLUMN IF NOT EXISTS is_master BOOLEAN DEFAULT FALSE
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 3.95) nome em usuarios
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(usuarios);")).fetchall()
            colnames = {row[1] for row in cols}
            if "nome" not in colnames:
                db.session.execute(sa_text("ALTER TABLE usuarios ADD COLUMN nome VARCHAR(120)"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.usuarios
                ADD COLUMN IF NOT EXISTS nome VARCHAR(120)
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4) Migração leve: garantir coluna qtd_entregas em lancamentos
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(lancamentos);")).fetchall()
            colnames = {row[1] for row in cols}
            if "qtd_entregas" not in colnames:
                db.session.execute(sa_text("ALTER TABLE lancamentos ADD COLUMN qtd_entregas INTEGER"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS public.lancamentos "
                "ADD COLUMN IF NOT EXISTS qtd_entregas INTEGER"
            ))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 3.99) corrige registros antigos com ativo NULL
    try:
        if _is_sqlite():
            db.session.execute(sa_text("""
                UPDATE usuarios
                   SET ativo = 1
                 WHERE ativo IS NULL
            """))
        else:
            db.session.execute(sa_text("""
                UPDATE public.usuarios
                   SET ativo = TRUE
                 WHERE ativo IS NULL
            """))
        db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.x) período em despesas_cooperado (data_inicio / data_fim) + backfill
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(despesas_cooperado);")).fetchall()
            colnames = {row[1] for row in cols}
    
            if "data_inicio" not in colnames:
                db.session.execute(sa_text("ALTER TABLE despesas_cooperado ADD COLUMN data_inicio DATE"))
            if "data_fim" not in colnames:
                db.session.execute(sa_text("ALTER TABLE despesas_cooperado ADD COLUMN data_fim DATE"))
            db.session.commit()
    
            # Retropreenche linhas antigas
            db.session.execute(sa_text("""
                UPDATE despesas_cooperado
                   SET data_inicio = COALESCE(data_inicio, data),
                       data_fim    = COALESCE(data_fim,    data)
                 WHERE data IS NOT NULL
                   AND (data_inicio IS NULL OR data_fim IS NULL)
            """))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.despesas_cooperado
                ADD COLUMN IF NOT EXISTS data_inicio DATE
            """))
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.despesas_cooperado
                ADD COLUMN IF NOT EXISTS data_fim DATE
            """))
            db.session.commit()
    
            db.session.execute(sa_text("""
                UPDATE public.despesas_cooperado
                   SET data_inicio = COALESCE(data_inicio, data),
                       data_fim    = COALESCE(data_fim,    data)
                 WHERE data IS NOT NULL
                   AND (data_inicio IS NULL OR data_fim IS NULL)
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.y) beneficio_id em despesas_cooperado (FK p/ beneficios_registro)
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(despesas_cooperado);")).fetchall()
            colnames = {row[1] for row in cols}
            if "beneficio_id" not in colnames:
                db.session.execute(sa_text("ALTER TABLE despesas_cooperado ADD COLUMN beneficio_id INTEGER"))
            db.session.commit()
            # OBS: SQLite não permite adicionar uma FK com ON DELETE CASCADE via ALTER TABLE;
            # para ter a FK de fato, teria que recriar a tabela. Em dev, costuma bastar só a coluna.
        else:
            # Index para consultas
            db.session.execute(sa_text("""
                CREATE INDEX IF NOT EXISTS ix_despesas_beneficio_id
                ON public.despesas_cooperado (beneficio_id)
            """))
            # Normaliza sequência: DROP antes do ADD (idempotente entre deploys)
            db.session.execute(sa_text("""
                ALTER TABLE public.despesas_cooperado
                DROP CONSTRAINT IF EXISTS despesas_cooperado_beneficio_id_fkey
            """))
            db.session.execute(sa_text("""
                ALTER TABLE public.despesas_cooperado
                ADD CONSTRAINT despesas_cooperado_beneficio_id_fkey
                FOREIGN KEY (beneficio_id) REFERENCES public.beneficios_registro (id)
                ON DELETE CASCADE
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.z) eh_adiantamento em despesas_cooperado (marca adiantamento separado)
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(despesas_cooperado);")).fetchall()
            colnames = {row[1] for row in cols}
            if "eh_adiantamento" not in colnames:
                db.session.execute(sa_text("ALTER TABLE despesas_cooperado ADD COLUMN eh_adiantamento BOOLEAN DEFAULT 0"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.despesas_cooperado
                ADD COLUMN IF NOT EXISTS eh_adiantamento BOOLEAN DEFAULT FALSE
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    
    
    # 4.za) competencia_desconto em despesas_cooperado
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(despesas_cooperado);")).fetchall()
            colnames = {row[1] for row in cols}
            if "competencia_desconto" not in colnames:
                db.session.execute(sa_text("ALTER TABLE despesas_cooperado ADD COLUMN competencia_desconto VARCHAR(20) DEFAULT 'atual'"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.despesas_cooperado
                ADD COLUMN IF NOT EXISTS competencia_desconto VARCHAR(20) DEFAULT 'atual'
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.zab) bloqueio global de solicitação de adiantamento
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(config); ")).fetchall()
            colnames = {row[1] for row in cols}
            if "bloquear_adiantamento" not in colnames:
                db.session.execute(sa_text("ALTER TABLE config ADD COLUMN bloquear_adiantamento BOOLEAN DEFAULT 0"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.config
                ADD COLUMN IF NOT EXISTS bloquear_adiantamento BOOLEAN DEFAULT FALSE
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.zb) tabela de abatimentos de despesas do cooperado
    try:
        if _is_sqlite():
            db.session.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS despesas_cooperado_abatimentos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    despesa_id INTEGER NOT NULL,
                    data DATE NOT NULL,
                    valor FLOAT DEFAULT 0.0,
                    origem VARCHAR(30) DEFAULT 'manual',
                    observacao VARCHAR(255),
                    criado_em DATETIME,
                    FOREIGN KEY(despesa_id) REFERENCES despesas_cooperado(id) ON DELETE CASCADE
                )
            """))
            db.session.execute(sa_text("CREATE INDEX IF NOT EXISTS ix_despesas_cooperado_abatimentos_despesa_id ON despesas_cooperado_abatimentos (despesa_id)"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS public.despesas_cooperado_abatimentos (
                    id SERIAL PRIMARY KEY,
                    despesa_id INTEGER NOT NULL REFERENCES public.despesas_cooperado(id) ON DELETE CASCADE,
                    data DATE NOT NULL,
                    valor DOUBLE PRECISION DEFAULT 0.0,
                    origem VARCHAR(30) DEFAULT 'manual',
                    observacao VARCHAR(255),
                    criado_em TIMESTAMP
                )
            """))
            db.session.execute(sa_text("""
                CREATE INDEX IF NOT EXISTS ix_despesas_cooperado_abatimentos_despesa_id
                ON public.despesas_cooperado_abatimentos (despesa_id)
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.zc) índices de performance para filtros e login rápido
    try:
        if _is_sqlite():
            idx_sql = [
                "CREATE INDEX IF NOT EXISTS ix_lancamentos_data ON lancamentos (data)",
                "CREATE INDEX IF NOT EXISTS ix_lancamentos_rest_data ON lancamentos (restaurante_id, data)",
                "CREATE INDEX IF NOT EXISTS ix_lancamentos_coop_data ON lancamentos (cooperado_id, data)",
                "CREATE INDEX IF NOT EXISTS ix_lancamentos_rest_coop_data ON lancamentos (restaurante_id, cooperado_id, data)",
                "CREATE INDEX IF NOT EXISTS ix_receitas_coop_coop_data ON receitas_cooperado (cooperado_id, data)",
                "CREATE INDEX IF NOT EXISTS ix_despesas_cooperado_coop_periodo ON despesas_cooperado (cooperado_id, data_inicio, data_fim)",
                "CREATE INDEX IF NOT EXISTS ix_escalas_cooperado_id ON escalas (cooperado_id)",
                "CREATE INDEX IF NOT EXISTS ix_escalas_restaurante_id ON escalas (restaurante_id)",
                "CREATE INDEX IF NOT EXISTS ix_escalas_contrato ON escalas (contrato)",
                "CREATE INDEX IF NOT EXISTS ix_usuarios_tipo_ativo ON usuarios (tipo, ativo)",
            ]
        else:
            idx_sql = [
                "CREATE INDEX IF NOT EXISTS ix_lancamentos_data ON public.lancamentos (data)",
                "CREATE INDEX IF NOT EXISTS ix_lancamentos_rest_data ON public.lancamentos (restaurante_id, data)",
                "CREATE INDEX IF NOT EXISTS ix_lancamentos_coop_data ON public.lancamentos (cooperado_id, data)",
                "CREATE INDEX IF NOT EXISTS ix_lancamentos_rest_coop_data ON public.lancamentos (restaurante_id, cooperado_id, data)",
                "CREATE INDEX IF NOT EXISTS ix_receitas_coop_coop_data ON public.receitas_cooperado (cooperado_id, data)",
                "CREATE INDEX IF NOT EXISTS ix_despesas_cooperado_coop_periodo ON public.despesas_cooperado (cooperado_id, data_inicio, data_fim)",
                "CREATE INDEX IF NOT EXISTS ix_escalas_cooperado_id ON public.escalas (cooperado_id)",
                "CREATE INDEX IF NOT EXISTS ix_escalas_restaurante_id ON public.escalas (restaurante_id)",
                "CREATE INDEX IF NOT EXISTS ix_escalas_contrato ON public.escalas (contrato)",
                "CREATE INDEX IF NOT EXISTS ix_usuarios_tipo_ativo ON public.usuarios (tipo, ativo)",
            ]
        for _sql in idx_sql:
            db.session.execute(sa_text(_sql))
        db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.1) cooperado_nome em escalas
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(escalas);")).fetchall()
            colnames = {row[1] for row in cols}
            if "cooperado_nome" not in colnames:
                db.session.execute(sa_text("ALTER TABLE escalas ADD COLUMN cooperado_nome VARCHAR(120)"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS escalas "
                "ADD COLUMN IF NOT EXISTS cooperado_nome VARCHAR(120)"
            ))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.2) restaurante_id em escalas
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(escalas);")).fetchall()
            colnames = {row[1] for row in cols}
            if "restaurante_id" not in colnames:
                db.session.execute(sa_text("ALTER TABLE escalas ADD COLUMN restaurante_id INTEGER"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS escalas "
                "ADD COLUMN IF NOT EXISTS restaurante_id INTEGER"
            ))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.3) fotos no banco (cooperados)
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
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_bytes BYTEA"))
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_mime VARCHAR(100)"))
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_filename VARCHAR(255)"))
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_url VARCHAR(255)"))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.3.x) telefone em cooperados
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(cooperados);")).fetchall()
            colnames = {row[1] for row in cols}
            if "telefone" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN telefone VARCHAR(30)"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS cooperados "
                "ADD COLUMN IF NOT EXISTS telefone VARCHAR(30)"
            ))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.4) tabela avaliacoes_restaurante (se não existir)
    try:
        if _is_sqlite():
            db.session.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS avaliacoes_restaurante (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                restaurante_id INTEGER NOT NULL,
                cooperado_id INTEGER NOT NULL,
                lancamento_id INTEGER UNIQUE,
                estrelas_geral INTEGER,
                estrelas_ambiente INTEGER,
                estrelas_tratamento INTEGER,
                estrelas_suporte INTEGER,
                comentario TEXT,
                media_ponderada DOUBLE PRECISION,
                sentimento VARCHAR(12),
                temas VARCHAR(255),
                alerta_crise BOOLEAN DEFAULT FALSE,
                criado_em TIMESTAMP
              )
           """))
            db.session.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS ix_av_rest_rest ON avaliacoes_restaurante(restaurante_id, criado_em)"))
            db.session.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS ix_av_rest_coop ON avaliacoes_restaurante(cooperado_id)"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS avaliacoes_restaurante (
                  id SERIAL PRIMARY KEY,
                  restaurante_id INTEGER NOT NULL,
                  cooperado_id   INTEGER NOT NULL,
                  lancamento_id  INTEGER UNIQUE,
                  estrelas_geral INTEGER,
                  estrelas_ambiente INTEGER,
                  estrelas_tratamento INTEGER,
                  estrelas_suporte INTEGER,
                  comentario TEXT,
                  media_ponderada DOUBLE PRECISION,
                  sentimento VARCHAR(12),
                  temas VARCHAR(255),
                  alerta_crise BOOLEAN DEFAULT FALSE,
                  criado_em TIMESTAMP
                )
            """))
            db.session.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS ix_av_rest_rest ON avaliacoes_restaurante(restaurante_id, criado_em)"))
            db.session.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS ix_av_rest_coop ON avaliacoes_restaurante(cooperado_id)"))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.4.z) eh_farmacia em restaurantes
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(restaurantes);")).fetchall()
            colnames = {row[1] for row in cols}
            if "eh_farmacia" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN eh_farmacia BOOLEAN DEFAULT 0"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.restaurantes
                ADD COLUMN IF NOT EXISTS eh_farmacia BOOLEAN DEFAULT FALSE
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.5) fotos no banco (restaurantes)
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
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_bytes BYTEA"))
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_mime VARCHAR(100)"))
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_filename VARCHAR(255)"))
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_url VARCHAR(255)"))
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    # 4.5.x) taxa administrativa em restaurantes + receitas_coop
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(restaurantes);")).fetchall()
            colnames = {row[1] for row in cols}
            if "taxa_admin_valor" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN taxa_admin_valor FLOAT DEFAULT 0"))
            if "taxa_admin_data_base" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN taxa_admin_data_base DATE"))
            if "taxa_admin_multa_percentual" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN taxa_admin_multa_percentual FLOAT DEFAULT 2.0"))
            if "taxa_admin_juros_dia_percentual" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN taxa_admin_juros_dia_percentual FLOAT DEFAULT 0.03"))
            if "ativo" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN ativo BOOLEAN DEFAULT 1"))
            db.session.commit()
    
            cols = db.session.execute(sa_text("PRAGMA table_info(receitas_coop);")).fetchall()
            colnames = {row[1] for row in cols}
            adds = {
                "restaurante_id": "INTEGER",
                "auto_taxa_adm": "BOOLEAN DEFAULT 0",
                "competencia": "VARCHAR(7)",
                "valor_previsto": "FLOAT DEFAULT 0",
                "valor_principal": "FLOAT DEFAULT 0",
                "valor_pago": "FLOAT DEFAULT 0",
                "valor_multa": "FLOAT DEFAULT 0",
                "valor_juros": "FLOAT DEFAULT 0",
                "data_vencimento": "DATE",
                "data_pagamento": "DATE",
                "status_pagamento": "VARCHAR(20) DEFAULT 'nao_pago'",
                "multa_percentual": "FLOAT DEFAULT 2.0",
                "juros_dia_percentual": "FLOAT DEFAULT 0.03",
            }
            for col, ddl in adds.items():
                if col not in colnames:
                    db.session.execute(sa_text(f"ALTER TABLE receitas_coop ADD COLUMN {col} {ddl}"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.restaurantes ADD COLUMN IF NOT EXISTS taxa_admin_valor DOUBLE PRECISION DEFAULT 0;
                ALTER TABLE IF EXISTS public.restaurantes ADD COLUMN IF NOT EXISTS taxa_admin_data_base DATE;
                ALTER TABLE IF EXISTS public.restaurantes ADD COLUMN IF NOT EXISTS taxa_admin_multa_percentual DOUBLE PRECISION DEFAULT 2.0;
                ALTER TABLE IF EXISTS public.restaurantes ADD COLUMN IF NOT EXISTS taxa_admin_juros_dia_percentual DOUBLE PRECISION DEFAULT 0.03;
                ALTER TABLE IF EXISTS public.restaurantes ADD COLUMN IF NOT EXISTS ativo BOOLEAN DEFAULT TRUE;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS restaurante_id INTEGER;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS auto_taxa_adm BOOLEAN DEFAULT FALSE;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS competencia VARCHAR(7);
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS valor_previsto DOUBLE PRECISION DEFAULT 0;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS valor_principal DOUBLE PRECISION DEFAULT 0;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS valor_pago DOUBLE PRECISION DEFAULT 0;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS valor_multa DOUBLE PRECISION DEFAULT 0;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS valor_juros DOUBLE PRECISION DEFAULT 0;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS data_vencimento DATE;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS data_pagamento DATE;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS status_pagamento VARCHAR(20) DEFAULT 'nao_pago';
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS multa_percentual DOUBLE PRECISION DEFAULT 2.0;
                ALTER TABLE IF EXISTS public.receitas_coop ADD COLUMN IF NOT EXISTS juros_dia_percentual DOUBLE PRECISION DEFAULT 0.03;
            """))
            db.session.commit()
        try:
            db.session.execute(sa_text("UPDATE receitas_coop SET status_pagamento = 'nao_pago' WHERE status_pagamento IS NULL"))
            db.session.execute(sa_text("UPDATE receitas_coop SET auto_taxa_adm = FALSE WHERE auto_taxa_adm IS NULL"))
            db.session.commit()
        except Exception:
            db.session.rollback()
    except Exception:
        db.session.rollback()
    
    # 5) Bootstrap mínimo (admin e config) — só se os modelos existirem
    try:
        # Garante que o model Usuario está acessível
        _ = Usuario  # type: ignore[name-defined]
    
        # Admin
        try:
            tem_admin = Usuario.query.filter_by(tipo="admin").first()  # type: ignore[name-defined]
        except Exception:
            tem_admin = None
    
        if not tem_admin:
            admin_user = os.environ.get("ADMIN_USER", "admin")
            admin_pass = os.environ.get("ADMIN_PASS", os.urandom(8).hex())
            admin = Usuario(
                usuario=admin_user,
                tipo="admin",
                senha_hash="",
                is_master=True
            )  # type: ignore[name-defined]
            try:
                admin.set_password(admin_pass)  # type: ignore[attr-defined]
            except Exception:
                try:
                    from werkzeug.security import generate_password_hash
                    admin.senha_hash = generate_password_hash(admin_pass)  # type: ignore[attr-defined]
                except Exception:
                    pass
            db.session.add(admin)
            db.session.commit()
    except Exception:
        db.session.rollback()
    
    try:
        admin_master = Usuario.query.filter_by(tipo="admin", is_master=True).first()
        if not admin_master:
            primeiro_admin = Usuario.query.filter_by(tipo="admin").order_by(Usuario.id.asc()).first()
            if primeiro_admin:
                primeiro_admin.is_master = True
                db.session.commit()
    except Exception:
        db.session.rollback()
    
    try:
        # Se o model Config existir, cria default
        try:
            Config  # type: ignore[name-defined]
            has_config_model = True
        except NameError:
            has_config_model = False
    
        if has_config_model:
            if not Config.query.get(1):  # type: ignore[name-defined]
                db.session.add(Config(id=1, salario_minimo=0.0, bloquear_adiantamento=False))  # type: ignore[name-defined]
                db.session.commit()
    except Exception:
        db.session.rollback()
