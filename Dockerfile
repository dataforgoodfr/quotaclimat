#from https://medium.com/@albertazzir/blazing-fast-python-docker-builds-with-poetry-a78a66f5aed0
# Stage the runtime image starts from: runtime-base, or runtime-media for the
# quotaclimat_with_media_tools image (adds ffmpeg, before the code so that it stays cached)
ARG RUNTIME=runtime-base

# pyproject.toml with its version replaced by a fixed one: the version is bumped on main
# at every merge, and it would otherwise invalidate the cache of poetry install below.
# The real pyproject.toml is copied into the runtime image.
FROM python:3.12.10-slim AS deps
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN sed -i 's/^version = .*/version = "0.0.0"/' pyproject.toml

FROM python:3.12.10 as builder

ENV VIRTUAL_ENV=/app/.venv

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY --from=deps /app/pyproject.toml /app/poetry.lock ./

RUN pip install poetry==2.1.3

RUN poetry install --no-root && rm -rf $POETRY_CACHE_DIR

# Poetry itself in the venv, for the entrypoints that use poetry run (and streamlit)
RUN /app/.venv/bin/pip install --no-cache-dir poetry

# System packages of the runtime image. APT_REFRESH (the current month in CI) is part of
# the cache key: the packages are reinstalled, with their security fixes, once a month
# instead of being frozen in the layer cache.
FROM python:3.12.10-slim AS runtime-base
ARG APT_REFRESH
RUN echo "apt refresh: ${APT_REFRESH}" && apt update && apt-get install -y git

FROM runtime-base AS runtime-media
RUN apt-get update && apt-get install -y ffmpeg

# The runtime image, used to just run the code provided its virtual environment
FROM ${RUNTIME} AS runtime

WORKDIR /app

ENV VIRTUAL_ENV=/app/.venv 
ENV PATH="/app/.venv/bin:$PATH"
ENV PATH="$PYENV_ROOT/bin:$PATH"
ENV PYTHONPATH=/app
ENV DBT_PROFILES_DIR=/app/my_dbt_project/dbt
ENV DBT_PROJECT_DIR=/app/my_dbt_project

# From here on, only COPY --link and no RUN: each layer is built on its own, so a code
# change doesn't need the venv layer below it to be downloaded from the registry cache.
COPY --link --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

# App code is include with docker-compose as well

COPY --link alembic_factiva.ini ./alembic_factiva.ini
COPY --link alembic_factiva/ ./alembic_factiva
COPY --link alembic.ini ./alembic.ini
COPY --link alembic/ ./alembic
COPY --link decisions/rapports/ ./decisions/rapports
COPY --link entrypoints/ ./entrypoints
COPY --link i8n/ ./i8n
COPY --link metabase/ ./metabase
COPY --link my_dbt_project/ ./my_dbt_project
COPY --link postgres ./postgres
COPY --link pyproject.toml poetry.lock ./
COPY --link quotaclimat ./quotaclimat

# healthcheck
EXPOSE 5050

# Docker compose overwrite this config to have only one Dockerfile
CMD ["ls"]
