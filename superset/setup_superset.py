#!/usr/bin/env python3
# setup_superset.py  – финальная правка

import subprocess, time, sys, logging
from sqlalchemy.exc import OperationalError

MAX_RETRIES = 3
DELAY       = 60           # секунд

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ────────────────────── helpers ──────────────────────
CREATE_ADMIN = [
    "superset", "fab", "create-admin",
    "--username",  "admin",
    "--firstname", "Admin",
    "--lastname",  "User",
    "--email",     "admin@finbest.io",
    "--password",  "admin",
]

def create_admin():
    rs = subprocess.run(CREATE_ADMIN, capture_output=True, text=True)
    if rs.returncode == 0:
        log.info("✔ admin-user создан")
    elif "already exists" in rs.stderr:
        log.info("ℹ admin-user уже существует")
    else:
        raise RuntimeError(rs.stderr.strip())

def register_databases():
    from superset.app import create_app
    app = create_app()
    from superset.extensions import db
    with app.app_context():
        from superset.connectors.sqla.models import Database
        if not db.session.query(Database).filter_by(database_name="FinBest Postgres").first():
            db.session.add(Database(
                database_name="FinBest Postgres",
                sqlalchemy_uri="postgresql://finbest:finbest_password@postgres:5432/finbest"))
            log.info("✔ добавлена БД FinBest Postgres")
        db.session.commit()


# ───────────────────── main loop ─────────────────────
for attempt in range(1, MAX_RETRIES + 1):
    log.info("Попытка %s/%s – инициализируем Superset…", attempt, MAX_RETRIES)
    try:
        create_admin()
        register_databases()
        log.info("🎉 Superset готов")
        sys.exit(0)
    except OperationalError as e:
        log.warning("Postgres ещё не готов: %s", e)
    except Exception as e:
        log.error("Ошибка: %s", e)

    if attempt < MAX_RETRIES:
        log.info("⏳ ждём %s с и повторяем…", DELAY)
        time.sleep(DELAY)

log.critical("💥 не удалось подключиться к Postgres за %s попыток – выходим", MAX_RETRIES)
sys.exit(1)
