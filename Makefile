export APP := wrapbytes/deltalake-redis-lock
export TAG := 0.0.1

.PHONY: clean-environment
clean-environment:
	rm -rf build dist .eggs *.egg-info
	rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	find . -type d -name '.mypy_cache' -exec rm -rf {} +
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	find . -type f -name "*.py[co]" -exec rm -rf {} +

.PHONY: install-environment
install-environment:
	poetry env use 3.9
	poetry install

.PHONY: info-environment
info-environment:
	poetry env info
	poetry show --tree

.PHONY: test
test:
	poetry run python -m pytest tests/$(type)/ --cov-config=tests/$(type)/.coveragerc --cov=. --quiet $(test_argument)

.PHONY: update-environment
update-environment:
	poetry update

.PHONY: poetry-path
poetry-path:
	@echo $(shell eval poetry show -v 2> /dev/null | head -n1 | cut -d ' ' -f 3)

.PHONY: run
run:
	poetry run uvicorn main:app --reload

.PHONY: job
job:
	poetry run python job.py $(JOB_NAME) $(SQL)

.PHONY: linter
linter:
	poetry run pre-commit run --all-files

.PHONY: run-container-linter
run-container-linter:
	docker run $(APP):$(TAG) make --directory app/ linter

.PHONY: build-container-image
build-container-image:
	docker build -t $(APP):$(TAG) -f tools/docker/Dockerfile .
