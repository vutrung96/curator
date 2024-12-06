import pytest
import time
import os
from tests.batch_tests.test_helpers import run_script
from tests.batch_tests.test_helpers import clean_caches

"""
USAGE:
pytest -s tests/batch_tests/test_batch_resume.py
"""


@pytest.mark.cache_dir(os.path.expanduser("~/.cache/curator-tests/test-batch-resume"))
@pytest.mark.usefixtures("clean_caches")
def test_batch_resume():
    script = [
        "python",
        "tests/batch_tests/simple_batch.py",
        "--log-level",
        "DEBUG",
        "--n-requests",
        "3",
        "--batch-size",
        "1",
        "--batch-check-interval",
        "10",
    ]

    env = os.environ.copy()

    print("FIRST RUN")
    stop_line_pattern = r"Marked batch ID batch_[a-f0-9]{32} as downloaded"
    output1, _ = run_script(script, stop_line_pattern, env=env)
    print(output1)

    # Small delay to ensure files are written
    time.sleep(1)

    # Second run should process the remaining batch
    print("SECOND RUN")
    output2, _ = run_script(script, env=env)
    print(output2)

    # checks
    assert "2 out of 2 remaining batches are already submitted." in output2
    assert "1 out of 1 batches already downloaded." in output2
