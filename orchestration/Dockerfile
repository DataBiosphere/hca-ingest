## Builds the dagster "user code deployment" image that will
## hold our pipelines/solids/etc.

FROM python:3.9.12-slim as base

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

RUN mkdir /logs \
    poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-dev | tee /logs/poetry_install.log

# Cleanup
RUN  rm -rf /var/lib/apt/lists/* \
    &&  rm -rf /root/.cache  \
    &&  rm -rf /usr/lib/python2.7 \
    &&  rm -rf /usr/lib/x86_64-linux-gnu/guile

# ==> Add user code layer
COPY ./ /

# builds with GitHub Action "Master Validation and Release" ../.github/workflows/build-and-publish-master.yaml
# tag = us.gcr.io/broad-dsp-gcr-public/monster-hca-dagster:${{steps.get-artifact-slug.outputs.slug}}
#
# For Dev
# to build and run locally
# docker build -t us.gcr.io/broad-dsp-gcr-public/monster-hca-dagster:<new_version> .
# docker run --rm -it us.gcr.io/broad-dsp-gcr-public/monster-hca-dagster:<new_version>