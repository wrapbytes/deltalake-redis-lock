FROM python:3.9-slim-bullseye

COPY src /app/src/
COPY tests /app/tests/
COPY .git /app/.git/
COPY [".isort.cfg", ".pre-commit-config.yaml", "Makefile", "poetry.lock", "pyproject.toml", "README.md", "/app/"]

ENV PYTHONDONTWRITEBYTECODE 1
ENV PIP_VERSION 23.1.2
ENV POETRY_VERSION 1.1.13
ENV SETUPTOOLS_VERSION 65.5.1

RUN apt-get update && \
    apt-get -y install --no-install-recommends libc-bin && \
    apt-get -y install --no-install-recommends libc6 && \
    apt-get -y install --no-install-recommends git && \
    apt-get -y install --no-install-recommends make && \
    apt-get -y install --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/* && \
    python -m pip install --no-cache-dir pip=="${PIP_VERSION}" && \
    python -m pip install --no-cache-dir setuptools=="${SETUPTOOLS_VERSION}"
    pip install --no-cache-dir poetry=="${POETRY_VERSION}" && \
    make --directory /app/ install-environment
