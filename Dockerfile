# Docker Hardened Image (consigne Partie 3) : recherché sur dhi.io, image
# trouvée sous dhi/python (basée sur Alpine, labels CIS/FIPS/STIG). On ne
# l'utilise pas ici : elle est réservée aux comptes Docker Hub avec l'accès
# "DHI Enterprise" activé (essai payant), qu'on n'a pas. La basculer sans cet
# accès ferait échouer le "docker pull" et casserait tout le pipeline CI.
FROM python:3.12-slim

ENV PYTHONPATH=/app \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=2.1.3

WORKDIR /app

RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root

COPY . .

CMD ["python", "--version"]
