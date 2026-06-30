import os
from pathlib import Path
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .config import *

load_dotenv()


def build_mysql_url(user, password, host, port, database):
    if not host or not database:
        return None

    return (
        "mysql+pymysql://"
        f"{quote_plus(user or '')}:{quote_plus(password or '')}"
        f"@{host}:{port}/{database}"
    )

message_url = os.getenv("MESSAGEHUB_DATABASE_URL") or build_mysql_url(
    message_db_user,
    message_db_password,
    message_db_host,
    message_db_port or "3306",
    message_db_name,
)
trade_url = os.getenv("TRADE_DATABASE_URL") or build_mysql_url(
    trade_db_user,
    trade_db_password,
    trade_db_host,
    trade_db_port or "3306",
    trade_db_name,
)

if not trade_url:
    raise ValueError("TRADE_DATABASE_URL or trade DB env vars are not configured")
if not message_url:
    raise ValueError("MESSAGEHUB_DATABASE_URL or message hub DB env vars are not configured")

SSL_CA_PATH = os.getenv("SSL_CA_PATH")
connect_args = {}
if SSL_CA_PATH:
    ssl_path = Path(SSL_CA_PATH).expanduser()
    if not ssl_path.is_absolute():
        repo_root = Path(__file__).resolve().parents[3]
        ssl_path = repo_root / ssl_path
    if ssl_path.exists():
        connect_args["ssl"] = {"ca": str(ssl_path)}

trade_engine = create_engine(
    trade_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args=connect_args if connect_args else None,
)
message_engine = create_engine(
    message_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args=connect_args if connect_args else None,
)

TradeSessionLocal = sessionmaker(bind=trade_engine)
MessageSessionLocal = sessionmaker(bind=message_engine)
