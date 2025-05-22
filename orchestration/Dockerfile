## Builds the dagster "user code deployment" image that will hold our ingest ops & jobs.

# base python image, blessed by AppSec
# FROM us.gcr.io/broad-dsp-gcr-public/base/python:3.12-debian AS base
FROM us.gcr.io/broad-dsp-gcr-public/base/python:3.10-debian AS base

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONDONTWRITEBYTECODE=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  # Poetry settings for multistage build
  # includes PYTHONDONTWRITEBYTECODE=1 to prevent __pycache__ files from being written to disk - see above
  # https://github.com/python-poetry/poetry/blob/246e181483a10e0ef75bca00dc5ccf38ac38f05b/docker-examples/poetry-multistage/Dockerfile
  POETRY_VERSION=2.0 \
  POETRY_HOME=/opt/poetry \
  POETRY_NO_INTERACTION=1 \
  POETRY_VIRTUALENVS_IN_PROJECT=1 \
  POETRY_VIRTUALENVS_CREATE=1 \
  POETRY_CACHE_DIR=/opt/.cache

RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /app

# TODO add app back
COPY pyproject.toml poetry.lock /app/

# Install Dagster and its dependencies
# RUN apt-get update -yqq - pretty sure this is vestigial

RUN mkdir /logs \
    # TODO figure out how to expose this install to the dev env so we don't have to install twice  \
    # https://github.com/python-poetry/poetry/blob/246e181483a10e0ef75bca00dc5ccf38ac38f05b/docker-examples/poetry-multistage/Dockerfile
    # && poetry config virtualenvs.create false \
    # && poetry install --no-interaction --no-ansi --no-dev | tee /logs/poetry_install.log \
    # not sure about no-root, and should refactor pyproject.toml to no longer have a dev section \
    && poetry install --no-root --without dev && rm -rf $POETRY_CACHE_DIR

# Cleanup extra files - though if we are using a multi-stage build, this is not necessary... right?
RUN  rm -rf /var/lib/apt/lists/* \
    &&  rm -rf /root/.cache  \
    &&  rm -rf /usr/lib/python3.10 \
    &&  rm -rf /usr/lib/x86_64-linux-gnu/guile

# copy the user code into the blessed final image with minimal footprint
FROM base AS runtime

# Accept the SENTRY_DSN as a build arg
ARG SENTRY_DSN
ENV SENTRY_DSN=$SENTRY_DSN
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# ==> Add user code layer
# TODO what if we just copied hca_orchestration & logs?
COPY --from=base ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY . .

# builds with GitHub Action "Main Validation and Release" ../.github/workflows/build-and-publish-main.yaml
# tag = us.gcr.io/broad-dsp-gcr-public/monster-hca-dagster:${{steps.get-artifact-slug.outputs.slug}}
#
# For Dev
# to build and run locally
# docker build -t us.gcr.io/broad-dsp-gcr-public/monster-hca-dagster:<new_version>
# TODO - look at the M42 Dockerfile for how to build for multiple platforms
# probably need to review this https://docs.docker.com/desktop/multi-arch/ and this https://docs.docker.com/buildx/working-with-buildx/
# since I'm using colima and not Docker Desktop
# TODO - is that necessary for the orch image? or just my dev env?
# docker buildx build --platform=linux/amd64,linux/arm64 .
# docker run --rm -it us.gcr.io/broad-dsp-gcr-public/monster-hca-dagster:<new_version>