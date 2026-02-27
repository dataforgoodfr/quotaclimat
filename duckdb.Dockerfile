#from https://medium.com/@albertazzir/blazing-fast-python-docker-builds-with-poetry-a78a66f5aed0
FROM python:3.12.10 AS builder

ENV VIRTUAL_ENV=/app/.venv

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN pip install poetry==2.1.3

RUN poetry install --no-root

# The runtime image, used to just run the code provided its virtual environment
FROM python:3.12.10-slim AS runtime

WORKDIR /app

ENV VIRTUAL_ENV=/app/.venv 
ENV PATH="/app/.venv/bin:$PATH"
ENV PATH="$PYENV_ROOT/bin:$PATH"
ENV PYTHONPATH=/app

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
RUN curl https://install.duckdb.org | sh

# App code is include with docker-compose as well
 
COPY quotaclimat ./quotaclimat
COPY postgres ./postgres
COPY pyproject.toml pyproject.toml
COPY alembic/ ./alembic
COPY alembic.ini ./alembic.ini
COPY transform_program.py ./transform_program.py


