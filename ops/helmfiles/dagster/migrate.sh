#!/usr/bin/env bash

# Migrates our dagster cluster DB schema
# (taken from https://docs.dagster.io/deployment/guides/kubernetes/how-to-migrate-your-instance)

function msg() {
  echo $1 >&2
}

function scale_down() {
  kubectl -n dagster scale deploy $1 --replicas=0
}

function scale_up() {
  kubectl -n dagster scale deploy $1 --replicas=$2
}

export HELM_DAGSTER_RELEASE_NAME="monster"

# Get the names of Dagster's Dagit and Daemon Deployments created by Helm
DAGIT_DEPLOYMENT_NAME=$(kubectl -n dagster get deploy \
  --selector=component=dagit -o jsonpath="{.items[0].metadata.name}")
export DAGIT_DEPLOYMENT_NAME
DAEMON_DEPLOYMENT_NAME=$(kubectl -n dagster get deploy \
  --selector=component=dagster-daemon -o jsonpath="{.items[0].metadata.name}")
export DAEMON_DEPLOYMENT_NAME

# Save each deployment's replica count to scale back up after migrating
DAGIT_DEPLOYMENT_REPLICA_COUNT=$(kubectl -n dagster get deploy \
  --selector=component=dagit -o jsonpath="{.items[0].status.replicas}")
export DAGIT_DEPLOYMENT_REPLICA_COUNT
DAEMON_DEPLOYMENT_REPLICA_COUNT=$(kubectl -n dagster get deploy \
  --selector=component=dagster-daemon -o jsonpath="{.items[0].status.replicas}")
export DAEMON_DEPLOYMENT_REPLICA_COUNT

msg "scaling down..."
scale_down "$DAGIT_DEPLOYMENT_NAME"
scale_down "$DAEMON_DEPLOYMENT_NAME"

msg "applying migration..."
helm template $HELM_DAGSTER_RELEASE_NAME dagster/dagster \
  --set "migrate.enabled=true" \
  --show-only templates/job-instance-migrate.yaml |
  kubectl apply -f -

echo "scaling up..." >&2
scale_up "$DAGIT_DEPLOYMENT_NAME" "$DAGIT_DEPLOYMENT_REPLICA_COUNT"
scale_up "$DAEMON_DEPLOYMENT_NAME" "$DAEMON_DEPLOYMENT_REPLICA_COUNT"