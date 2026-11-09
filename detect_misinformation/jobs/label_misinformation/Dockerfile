# Build context: repository root
#   docker build -f detect_misinformation/jobs/label_misinformation/Dockerfile .
# Dependencies come from the `misinformation` Poetry group of the root pyproject.toml.
# The shadow service adds the `rrs` group (CPU torch + transformers): --build-arg POETRY_GROUPS=misinformation,rrs
FROM python:3.12.10 AS builder

ARG POETRY_GROUPS=misinformation

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# The venv lives outside /app because docker-compose mounts ./app over /app
WORKDIR /opt/deps

COPY pyproject.toml poetry.lock ./

RUN pip install poetry==2.1.3 \
    && poetry install --no-root --only ${POETRY_GROUPS} \
    && rm -rf ${POETRY_CACHE_DIR}

FROM python:3.12.10-slim

ENV VIRTUAL_ENV=/opt/deps/.venv \
    PATH="/opt/deps/.venv/bin:$PATH"

COPY --from=builder /opt/deps/.venv /opt/deps/.venv

WORKDIR /app

# Job version (read by sentry_utils.py), bumped by bump2version
COPY detect_misinformation/jobs/label_misinformation/pyproject.toml ./
COPY detect_misinformation/jobs/label_misinformation/pytest.ini ./

# Copy app code
COPY detect_misinformation/jobs/label_misinformation/app/ .

RUN mkdir -p s3

CMD ["python", "main.py"]
