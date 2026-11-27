# Build context: repository root
#   docker build -f detect_misinformation/jobs/get_audio_video/Dockerfile .
# Dependencies come from the `germany-audio-video-collector` Poetry group of the root pyproject.toml.
FROM python:3.12.10 AS builder

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /opt/deps

COPY pyproject.toml poetry.lock ./

RUN pip install poetry==2.1.3 \
    && poetry install --no-root --only germany-audio-video-collector \
    && rm -rf ${POETRY_CACHE_DIR}

FROM python:3.12.10-slim

ENV VIRTUAL_ENV=/opt/deps/.venv \
    PATH="/opt/deps/.venv/bin:$PATH"

COPY --from=builder /opt/deps/.venv /opt/deps/.venv

WORKDIR /app

COPY detect_misinformation/jobs/get_audio_video/get_audio_video.py .

ENTRYPOINT ["python", "get_audio_video.py"]
