# Image that applies the repo's alembic migrations. Build context: a synced subset of the
# repo root (pyproject.toml, poetry.lock, alembic.ini, alembic/, postgres/, quotaclimat/),
# see corlab/ansible/roles/migrations. Some migrations import quotaclimat modules
# (keywords, program metadata, pandas...), hence the full dependency install.
FROM python:3.12.10

# Dependencies go into a project virtualenv (.venv), like the root Dockerfile: installing them
# in the system Python, next to Poetry, replaces Poetry's own dependencies (virtualenv...)
# and breaks the install midway.
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH=/app

WORKDIR /app

RUN pip install --no-cache-dir poetry==2.1.3

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root

COPY alembic.ini ./alembic.ini
COPY alembic/ ./alembic
COPY postgres ./postgres
COPY quotaclimat ./quotaclimat

CMD ["alembic", "upgrade", "head"]
