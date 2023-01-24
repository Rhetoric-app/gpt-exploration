python:
	DJANGO_SETTINGS_MODULE='app.settings' pipenv run python manage.py shell

start:
	PYTHONPATH=. pipenv run python manage.py runserver

migrate:
	pipenv run python manage.py migrate

migrations:
	pipenv run python manage.py makemigrations --settings=app.settings && make fmt

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

schema:
	pipenv run python manage.py generateschema --file openapi-schema.yml && \
	cd fe && \
	npx openapi --input ../openapi-schema.yml --output src/schema --name ApiClient --useOptions
