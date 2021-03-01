FROM python:3.7.8-slim

ARG DAGSTER_VERSION

# ==> Add Dagster layer
RUN \
# Cron
       apt-get update -yqq \
    && apt-get install -yqq cron \
# Dagster TODO convert this to poetry
    && pip install \
        dagster==${DAGSTER_VERSION} \
        dagster-postgres==${DAGSTER_VERSION} \
        dagster-celery[flower,redis,kubernetes]==${DAGSTER_VERSION} \
        dagster-aws==${DAGSTER_VERSION} \
        dagster-k8s==${DAGSTER_VERSION} \
        dagster-celery-k8s==${DAGSTER_VERSION} \
        google-cloud-bigquery==2.4.0 \
        google-cloud-storage==1.33.0 \
        requests-cache==0.5.2 \
        data-repo-client==1.0.194.post5 \
        argo-workflows==5.0.0 \
# Cleanup
    &&  rm -rf /var \
    &&  rm -rf /root/.cache  \
    &&  rm -rf /usr/lib/python2.7 \
    &&  rm -rf /usr/lib/x86_64-linux-gnu/guile

# ==> Add user code layer
# Example pipelines
COPY ./ /
