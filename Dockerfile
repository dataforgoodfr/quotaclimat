FROM python:3.10-buster

WORKDIR /app

RUN pip install poetry==1.6.1

ENV PYENV_ROOT="/opt/pyenv" \
    PATH="/opt/pyenv/shims:/opt/pyenv/bin:$PATH"

COPY . .
RUN curl https://pyenv.run | bash
RUN export PYENV_ROOT="$HOME/.pyenv"
RUN command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"
RUN eval "$(pyenv init -)"
RUN eval "$(pyenv virtualenv-init -)"
RUN pyenv install 3.10.2
RUN pyenv virtualenv 3.10.2 quotaclimat
RUN  pyenv local quotaclimat
RUN pip install poetry

# Verify poetry.lock agrees with pyproject.toml
RUN poetry check --lock 
RUN poetry source add pypi
RUN poetry show pandas

# Install dependencies
RUN poetry install --no-ansi

RUN mkdir streamlit 
ENTRYPOINT ["ls"]