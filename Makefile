PYTHON = python3
CLEANUP_DIRS = ~/.cache/curator __pycache__ .pytest_cache .tox .coverage .nox *.egg-info dist build 

lint: 
	@echo "Running Linter (black)..."
	black src/ tests/

test:
	@echo "Running tests with pytest..."
	pytest tests/ --maxfail=1 --disable-warnings -q

test_integration:
	@read integration_name && \
	@echo "Running integration test..."
	pytest tests/integrations/$$integration_name --maxfail=1 --disable-warnings -q

check: 
	@echo "Checking Linter (black)..."
	black --check src/ 
clean:
	@echo "Cleaning up build artifacts and cache..."
	rm -rf $(CLEANUP_DIRS)

install:
	@echo "Installing dependencies..."
	pip install -e .

all: lint test clean
