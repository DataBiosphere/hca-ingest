## Builds the dagster "user code deployment" image that will
## hold our pipelines/solids/etc.

FROM python:3.9.1-slim as base

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_VERSION=1.1.8

RUN pip install "poetry==$POETRY_VERSION"

# ==> Add Dagster layer
RUN apt-get update -yqq

COPY pyproject.toml /

RUN poetry config virtualenvs.create false \
     && poetry install --no-interaction --no-ansi --no-dev

# Cleanup
RUN  rm -rf /var \
    &&  rm -rf /root/.cache  \
    &&  rm -rf /usr/lib/python2.7 \
    &&  rm -rf /usr/lib/x86_64-linux-gnu/guile

# ==> Add user code layer
COPY ./ /
