#from https://medium.com/@albertazzir/blazing-fast-python-docker-builds-with-poetry-a78a66f5aed0
FROM python:3.12.10 as builder

ENV VIRTUAL_ENV=/app/.venv

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN pip install poetry==2.1.2

 RUN poetry install --no-root

# The runtime image, used to just run the code provided its virtual environment
FROM python:3.12.10-slim as runtime

RUN apt update && apt-get install -y git

WORKDIR /app

ENV VIRTUAL_ENV=/app/.venv 
ENV PATH="/app/.venv/bin:$PATH"
ENV PATH="$PYENV_ROOT/bin:$PATH"
ENV PYTHONPATH=/app
ENV DBT_PROFILES_DIR=/app/my_dbt_project/dbt
ENV DBT_PROJECT_DIR=/app/my_dbt_project

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

# For streamlit only
COPY pyproject.toml poetry.lock ./
RUN pip install poetry 

# App code is include with docker-compose as well
 
COPY quotaclimat ./quotaclimat
COPY postgres ./postgres
COPY alembic/ ./alembic
COPY transform_program.py ./transform_program.py
COPY my_dbt_project/ ./my_dbt_project

# Docker compose overwrite this config to have only one Dockerfile
CMD ["ls"]
