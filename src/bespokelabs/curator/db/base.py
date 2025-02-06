"""Base class for metadata database implementations."""

from abc import ABC, abstractmethod


class MetadataDB(ABC):
    """Abstract base class for storing Bella run metadata."""

    @abstractmethod
    def validate_schema(self):
        """Validate that the current database schema matches the expected schema.

        Raises:
            RuntimeError: If there is a mismatch between the current schema and expected schema.
        """
        pass

    @abstractmethod
    def store_metadata(self, metadata: dict):
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
        pass 