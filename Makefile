
MAX_LINE = 99

.PHONY: install check flake8 black black-check mypy

check: black-check mypy
	@ echo ✅

install:
	poetry install

black-check: install
	poetry run black --check --line-length $(MAX_LINE) *.py

black: install
	poetry run black --line-length $(MAX_LINE) *.py

mypy: install
	poetry run mypy *.py 

