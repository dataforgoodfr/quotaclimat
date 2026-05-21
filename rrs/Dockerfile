FROM python:3.12.10 AS builder

ENV VIRTUAL_ENV=/app/.venv \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN pip install poetry==2.1.3 && poetry install --no-root --without dev --with rrs

FROM python:3.12.10-slim AS runtime

WORKDIR /app

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH=/app

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

COPY pyproject.toml poetry.lock ./
COPY rrs/ ./rrs/
COPY quotaclimat/ ./quotaclimat/

CMD ["python", "-c", "import rrs; print('rrs ready')"]
