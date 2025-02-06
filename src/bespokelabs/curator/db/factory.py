"""Factory for creating MetadataDB instances."""

import os
from urllib.parse import urlparse

from bespokelabs.curator.db.base import MetadataDB
from bespokelabs.curator.db.postgres import PostgresDB
from bespokelabs.curator.db.sqlite import SQLiteDB

_CURATOR_DEFAULT_CACHE_DIR = "~/.cache/curator"


def create_metadata_db() -> MetadataDB:
    """Create a MetadataDB instance based on environment variables.

    Environment Variables:
        CURATOR_DB: The type of database to use ("postgres" or "sqlite")
        CURATOR_POSTGRES_URL: The URL for PostgreSQL connection (for postgres)
        CURATOR_POSTGRES_PASSWORD: The password for PostgreSQL connection (for postgres)
        CURATOR_CACHE_DIR: The cache directory for SQLite database (for sqlite)

    Returns:
        MetadataDB: An instance of either PostgresDB or SQLiteDB

    Raises:
        ValueError: If CURATOR_DB is set to an invalid value or required env vars are missing
    """
    db_type = os.getenv("CURATOR_DB", "sqlite").lower()

    if db_type == "postgres":
        postgres_url = os.getenv("CURATOR_POSTGRES_URL")
        if not postgres_url:
            raise ValueError("CURATOR_POSTGRES_URL environment variable must be set when using postgres")

        # Parse the URL to get connection parameters
        url = urlparse(postgres_url)
        dbname = url.path.lstrip("/")
        user = url.username
        password = os.getenv("CURATOR_POSTGRES_PASSWORD")
        if not password:
            raise ValueError("CURATOR_POSTGRES_PASSWORD environment variable must be set when using postgres")

        return PostgresDB(
            dbname=dbname,
            user=user,
            password=password,
            host=url.hostname,
            port=url.port or 5432,
        )
    elif db_type == "sqlite":
        curator_cache_dir = os.path.expanduser(os.getenv("CURATOR_CACHE_DIR", _CURATOR_DEFAULT_CACHE_DIR))
        db_path = os.path.join(curator_cache_dir, "metadata.db")
        return SQLiteDB(db_path)
    else:
        raise ValueError(f"Invalid CURATOR_DB value: {db_type}. Must be either 'postgres' or 'sqlite'")
