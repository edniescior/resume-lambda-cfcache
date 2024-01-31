SOURCE_FILES := $(shell find . -type f -path './src/*')

help: ## This help
	@grep -E -h "^[a-zA-Z_-]+:.*?## " $(MAKEFILE_LIST) \
	  | sort \
	  | awk -v width=36 'BEGIN {FS = ":.*?## "} {printf "\033[36m%-*s\033[0m %s\n", width, $$1, $$2}'

install-dependencies: ## Install pip and dependencies
	@echo '*** installing dependencies ***'
	python -m pip install --upgrade pip
	poetry install
	@echo '*** dependencies installed ***'

upgrade-dependencies: ## Upgrade pip and dependencies
	@echo '*** upgrading dependencies ***'
	python -m pip install --upgrade pip
	poetry update
	@echo '*** dependencies upgraded ***'

lint: ## Run linters
	@echo '*** running lint checks ***'
	isort .
	black .
	flake8 .
	@echo '*** all lint checks complete ***'

lint-diff: ## Run linters as dry-run
	@echo '*** running lint checks as dry-run ***'
	isort --diff .
	black --diff .
	@echo '*** linter diff complete ***'

test: ## Run tests
	@echo '*** running tests ***'
	pytest
	@echo '*** tests complete ***'

build: ## Package up the artifact
	@echo '*** running build ***'
	rm -rf artifact.zip dist/* package/*
	poetry build
	poetry run pip install --upgrade -t package dist/*.whl
	cd package ; zip -r ../artifact.zip . -x '*.pyc'
	@echo '*** build complete ***'

deploy: ## Deploy to AWS
	@echo 'Not here: Managed by Terraform'