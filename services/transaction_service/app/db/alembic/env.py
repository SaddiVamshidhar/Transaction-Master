import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy.pool import NullPool

from alembic import context

# Make the 'app' directory available for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from app.db.models import Base
from app.core.config import settings

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Overwrite the sqlalchemy.url from the alembic.ini with the one from our settings,
# but make it synchronous for this script by removing '+asyncpg'
sync_db_url = settings.DB_URL.replace("+asyncpg", "")
config.set_main_option('sqlalchemy.url', sync_db_url)

target_metadata = Base.metadata

def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section), # This is the corrected line
        prefix="sqlalchemy.",
        poolclass=NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()