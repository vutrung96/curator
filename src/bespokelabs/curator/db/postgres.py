"""PostgreSQL implementation of the metadata database."""

from typing import Any, Dict, List

import psycopg
from psycopg.rows import dict_row

from bespokelabs.curator.db.base import MetadataDB


class PostgresDB(MetadataDB):
    """PostgreSQL implementation of the metadata database."""

    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str = "localhost",
        port: int = 5432,
    ):
        """Initialize PostgresDB with connection parameters.

        Args:
            dbname: Name of the database
            user: Username for database connection
            password: Password for database connection
            host: Database host address
            port: Database port number
        """
        self.conn_string = (
            f"dbname={dbname} user={user} password={password} "
            f"host={host} port={port}"
        )
        self._ensure_table_exists()

    def _get_connection(self) -> psycopg.Connection:
        """Create and return a new database connection.

        Returns:
            psycopg connection object
        """
        return psycopg.connect(self.conn_string, row_factory=dict_row)

    def _ensure_table_exists(self):
        """Create the runs table if it doesn't exist."""
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runs (
                        run_hash TEXT PRIMARY KEY,
                        dataset_hash TEXT,
                        prompt_func TEXT,
                        model_name TEXT,
                        response_format TEXT,
                        batch_mode BOOLEAN,
                        created_time TIMESTAMP,
                        last_edited_time TIMESTAMP
                    )
                    """
                )

    def _get_current_schema(self) -> List[str]:
        """Get the current schema of the runs table from the database.

        Returns:
            List[str]: List of column names in the runs table
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'runs'
                    ORDER BY ordinal_position
                    """
                )
                return [row["column_name"] for row in cursor]

    def validate_schema(self):
        """Validate that the current database schema matches the expected schema.

        Raises:
            RuntimeError: If there is a mismatch between the current schema and expected schema
        """
        expected_columns = {
            "run_hash",
            "dataset_hash",
            "prompt_func",
            "model_name",
            "response_format",
            "batch_mode",
            "created_time",
            "last_edited_time",
        }
        current_columns = set(self._get_current_schema())

        if current_columns != expected_columns:
            msg = (
                "Detected a mismatch between the PostgreSQL schema and the expected schema. "
                f"Expected: {sorted(expected_columns)}, Got: {sorted(current_columns)}"
            )
            raise RuntimeError(msg)

    def store_metadata(self, metadata: Dict[str, Any]):
        """Store metadata about a Bella run in the database.

        Args:
            metadata: Dictionary containing run metadata with keys:
                - timestamp: ISO format timestamp
                - dataset_hash: Unique hash of input dataset
                - prompt_func: Source code of prompt function
                - model_name: Name of model used
                - response_format: JSON schema of response format
                - run_hash: Unique hash identifying the run
                - batch_mode: Boolean indicating batch mode or online mode
        """
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                # Check if run_hash exists
                cursor.execute(
                    "SELECT run_hash FROM runs WHERE run_hash = %s",
                    (metadata["run_hash"],),
                )
                existing_run = cursor.fetchone()

                if existing_run:
                    # Update last_edited_time for existing entry
                    cursor.execute(
                        """
                        UPDATE runs
                        SET last_edited_time = %s
                        WHERE run_hash = %s
                        """,
                        (metadata["timestamp"], metadata["run_hash"]),
                    )
                else:
                    # Insert new entry
                    cursor.execute(
                        """
                        INSERT INTO runs (
                            run_hash, dataset_hash, prompt_func, model_name,
                            response_format, batch_mode, created_time, last_edited_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
                        """,
                        (
                            metadata["run_hash"],
                            metadata["dataset_hash"],
                            metadata["prompt_func"],
                            metadata["model_name"],
                            metadata["response_format"],
                            metadata["batch_mode"],
                            metadata["timestamp"],
                        ),
                    )
            conn.commit() 