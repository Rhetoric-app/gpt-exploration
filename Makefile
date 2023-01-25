install:
	pipenv install

gpt-sql:
	PYTHONPATH=. pipenv run python app/generate_sql.py

streamlit:
	PYTHONPATH=. pipenv run streamlit run app/generate_sql.py

python:
	PYTHONPATH=. pipenv run python

lint-black:
	ENV=development pipenv run black --line-length=120 --skip-string-normalization --check .

lint-isort:
	ENV=development pipenv run isort --profile black --check-only .

flake8:
	ENV=development pipenv run flake8 --config setup.cfg .

mypy:
	ENV=development pipenv run mypy . --config-file setup.cfg

lint: lint-black lint-isort flake8 mypy

fmt:
	pipenv run black --line-length=120 --skip-string-normalization .
	pipenv run autoflake --in-place --recursive --remove-all-unused-imports --ignore-init-module-imports --exclude=namespace.py .
	pipenv run isort --profile black .
	make lint
