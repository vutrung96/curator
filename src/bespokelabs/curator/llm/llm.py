"""Curator: Bespoke Labs Synthetic Data Generation Library."""

import inspect
import logging
import os
from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type, TypeVar

from datasets import Dataset
from pydantic import BaseModel
from xxhash import xxh64

from bespokelabs.curator.db import create_metadata_db
from bespokelabs.curator.db.factory import create_metadata_db
from bespokelabs.curator.llm.prompt_formatter import PromptFormatter
from bespokelabs.curator.request_processor._factory import _RequestProcessorFactory
from bespokelabs.curator.request_processor.config import BackendParamsType

if TYPE_CHECKING:
    from datasets import Dataset

_CURATOR_DEFAULT_CACHE_DIR = "~/.cache/curator"
T = TypeVar("T")
_DictOrBaseModel = Dict[str, Any] | BaseModel
logger = logging.getLogger(__name__)


class LLM:
    """Interface for prompting LLMs."""

    response_format: Type[BaseModel] | None = None
    return_completions_object: bool = False

    def prompt(self, input: _DictOrBaseModel) -> _DictOrBaseModel:
        """Prompt the LLM.

        Args:
            input: The input row used to construct the prompt

        Returns:
            The prompt to send to the LLM. Can follow the following formats:

            1. A string, corresponding to a single user prompt, e.g.
            The string "Write a poem about love" will be converted
            to [{"role": "user", "content": "Write a poem about love"}]

            2. A list of dictionaries, corresponding to a list of messages, e.g.
            The list [{"role": "user", "content": "Write a poem about love"},
            {"role": "assistant", "content": "Here is a poem about love"}]
        """
        return input["prompt"]

    def parse(self, input: _DictOrBaseModel, response: _DictOrBaseModel) -> _DictOrBaseModel:
        """Parse the response from the LLM and combine it with the input.

        Args:
            input: The input row used to construct the prompt
            response: The response from the LLM

        Returns:
            The parsed output row that combines the input and response,
        """
        if isinstance(response, str):
            return {"response": response}
        elif isinstance(response, BaseModel):
            return response.model_dump()
        return response

    def __init__(
        self,
        model_name: str,
        response_format: Type[BaseModel] | None = None,
        batch: bool = False,
        backend: Optional[str] = None,
        generation_params: dict | None = None,
        backend_params: BackendParamsType | None = None,
    ):
        """Initialize a LLM.

        Args:
            model_name: The name of the LLM to use
            response_format: A Pydantic model specifying the
                response format from the LLM
            batch: Whether to use batch processing
            backend: The backend to use ("openai", "litellm", or "vllm"). If None, will be auto-determined
            generation_params: Additional parameters to pass to the generation API
            backend_params: Dictionary parameters for request processing
                    - max_retries: The maximum number of retries to use for the LLM
                    - require_all_responses: Whether to require all responses
                    - base_url: Optional base URL for API endpoint
                    - request_timeout: Timeout in seconds for each request

            Other backend params:
                - Online:
                    - max_requests_per_minute: Maximum number of requests per minute for rate limiting
                    - max_tokens_per_minute: Maximum number of tokens per minute for rate limiting
                    - seconds_to_pause_on_rate_limit: Number of seconds to pause when rate limited

                - Batch:
                    - batch_size: The size of the batch to use, only used if batch is True
                    - batch_check_interval: The interval to check for batch completions, only used if batch is True
                    - delete_successful_batch_files: Whether to delete successful batch files, only used if batch is True
                    - delete_failed_batch_files: Whether to delete failed batch files, only used if batch is True

                - Offline:
                    - tensor_parallel_size: The tensor parallel size to use for the VLLM backend
                    - enforce_eager: Whether to enforce eager execution for the VLLM backend
                    - max_model_length: The maximum model length to use for the VLLM backend
                    - max_tokens: The maximum tokens to use for the VLLM backend
                    - min_tokens: The minimum tokens to use for the VLLM backend
                    - gpu_memory_utilization: The GPU memory utilization to use for the VLLM backend
                    - batch_size: The size of the batch to use, only used if batch is True
        """
        generation_params = generation_params or {}

        if response_format is not None:
            self.response_format = response_format

        self.prompt_formatter = PromptFormatter(
            model_name=model_name,
            prompt_func=self.prompt,
            parse_func=self.parse,
            response_format=self.response_format,
            generation_params=_remove_none_values(generation_params),
        )
        self.batch_mode = batch

        self._request_processor = _RequestProcessorFactory.create(
            params=backend_params,
            model_name=model_name,
            batch=batch,
            response_format=response_format,
            backend=backend,
            generation_params=generation_params,
            return_completions_object=self.return_completions_object,
        )

    def _hash_fingerprint(self, dataset_hash, disable_cache):
        if disable_cache:
            fingerprint = xxh64(os.urandom(8)).hexdigest()
        else:
            # Get the source code of the prompt and parse methods
            prompt_func_hash = _get_function_hash(self.prompt_formatter.prompt_func)

            fingerprint_str = "_".join(
                [
                    str(dataset_hash),
                    str(prompt_func_hash),
                    str(self.prompt_formatter.model_name),
                    str(self.prompt_formatter.response_format.model_json_schema() if self.prompt_formatter.response_format else "text"),
                    str(self.batch_mode),
                ]
            )

            if self.prompt_formatter.generation_params:
                generation_params_str = str(sorted(self.prompt_formatter.generation_params.items()))
                fingerprint_str += f"_{generation_params_str}"

            fingerprint = xxh64(fingerprint_str.encode("utf-8")).hexdigest()
            logger.debug(f"Curator Cache Fingerprint String: {fingerprint_str}")
            logger.debug(f"Curator Cache Fingerprint: {fingerprint}")

        return fingerprint

    def __call__(
        self,
        dataset: Optional[Iterable] = None,
        working_dir: str = None,
        batch_cancel: bool = False,
    ) -> "Dataset":
        """Apply structured completions in parallel to a dataset using specified model and prompts.

        Args:
            dataset (Iterable): A dataset consisting of a list of items to apply completions
            working_dir (str): The working directory to save the requests.jsonl, responses.jsonl, and dataset.arrow files.
            batch_cancel (bool): Whether to cancel the batch if it is running
        Returns:
            Iterable: A list of structured outputs from the completions
        """
        # We convert from iterable to Dataset because Dataset has random access via row_idx
        if dataset:
            dataset = _convert_to_dataset(dataset)

        if working_dir is None:
            curator_cache_dir = os.environ.get(
                "CURATOR_CACHE_DIR",
                os.path.expanduser(_CURATOR_DEFAULT_CACHE_DIR),
            )
        else:
            curator_cache_dir = working_dir

        dataset_hash = dataset._fingerprint if dataset is not None else xxh64("").hexdigest()

        disable_cache = os.getenv("CURATOR_DISABLE_CACHE", "").lower() in ["true", "1"]
        fingerprint = self._hash_fingerprint(dataset_hash, disable_cache)

        # Use the factory to create the appropriate database instance
        metadata_db = create_metadata_db()

        # Get the source code of the prompt function
        prompt_func_source = _get_function_source(self.prompt_formatter.prompt_func)
        if self.prompt_formatter.parse_func is not None:
            parse_func_source = _get_function_source(self.prompt_formatter.parse_func)
        else:
            parse_func_source = ""

        metadata_dict = {
            "timestamp": datetime.now().isoformat(),
            "dataset_hash": dataset_hash,
            "prompt_func": prompt_func_source,
            "parse_func": parse_func_source,
            "model_name": self.prompt_formatter.model_name,
            "response_format": (str(self.prompt_formatter.response_format.model_json_schema()) if self.prompt_formatter.response_format else "text"),
            "run_hash": fingerprint,
            "batch_mode": self.batch_mode,
        }
        metadata_db.store_metadata(metadata_dict)

        run_cache_dir = os.path.join(curator_cache_dir, fingerprint)

        if batch_cancel:
            from bespokelabs.curator.request_processor.batch.openai_batch_request_processor import OpenAIBatchRequestProcessor

            if not isinstance(self._request_processor, OpenAIBatchRequestProcessor):
                raise ValueError("batch_cancel can only be used with batch mode")

            dataset = self._request_processor.cancel_batches(
                working_dir=run_cache_dir,
            )
        else:
            parse_func_hash = _get_function_hash(self.prompt_formatter.parse_func)
            dataset = self._request_processor.run(
                dataset=dataset,
                working_dir=run_cache_dir,
                parse_func_hash=parse_func_hash,
                prompt_formatter=self.prompt_formatter,
            )

        return dataset


def _get_function_hash(func) -> str:
    """Get a hash of a function's source code."""
    if func is None:
        return xxh64("").hexdigest()

    # Remove parameter annotations to avoid dill complaining about pickling
    # pydantic BaseModel: https://github.com/bespokelabsai/curator/issues/229.
    # For class/instance methods, get the underlying function
    if hasattr(func, "__func__"):
        func = func.__func__

    # Clear annotations if they exist
    if hasattr(func, "__annotations__"):
        func.__annotations__ = {}

    file = BytesIO()
    from datasets.utils._dill import Pickler

    Pickler(file, recurse=True).dump(func)
    return xxh64(file.getvalue()).hexdigest()


def _get_function_source(func) -> str:
    """Get the source code of a function.

    Purpose of this function is that during Python interpreter (REPL),
    `inspect.getsource` will fail with an OSError because functions defined in the
    interpreter don't have an associated source file. We have to use this wrapper
    to gracefully handle this case.
    """
    try:
        return inspect.getsource(func)
    except OSError:
        return ""


def _remove_none_values(d: dict) -> dict:
    """Remove all None values from a dictionary."""
    return {k: v for k, v in d.items() if v is not None}


def _is_message_list(list: list) -> bool:
    """Check if a list is a list of messages."""
    return all(isinstance(item, dict) and "role" in item and "content" in item for item in list)


def _convert_to_dataset(iterable: Iterable) -> "Dataset":
    """Convert an iterable to a Dataset.

    The prompt is expected to be a prompt string or a list of messages.
    """
    if isinstance(iterable, str) or _is_message_list(iterable):
        # A single string or list of messages is converted to a dataset with a single row
        dataset = Dataset.from_list([{"prompt": iterable}])
    elif not isinstance(iterable, Dataset) and iterable is not None:
        # Wrap the iterable in a generator, the prompt is expected to be a prompt string or a list of messages
        def wrapped_iterable():
            for input in iterable:
                if isinstance(input, str) or _is_message_list(input):
                    yield {"prompt": input}
                else:
                    yield input

        dataset = Dataset.from_generator(wrapped_iterable)
    else:
        dataset = iterable

    return dataset
