#!/usr/bin/env bash

# Migrates our dagster cluster DB schema
# (taken from https://docs.dagster.io/deployment/guides/kubernetes/how-to-migrate-your-instance)

# Get the names of Dagster's Dagit and Daemon Deployments created by Helm
export DAGIT_DEPLOYMENT_NAME=`kubectl -n dagster get deploy \
    --selector=component=dagit -o jsonpath="{.items[0].metadata.name}"`
export DAEMON_DEPLOYMENT_NAME=`kubectl -n dagster get deploy \
    --selector=component=dagster-daemon -o jsonpath="{.items[0].metadata.name}"`

# Save each deployment's replica count to scale back up after migrating
export DAGIT_DEPLOYMENT_REPLICA_COUNT=`kubectl -n dagster get deploy \
    --selector=component=dagit -o jsonpath="{.items[0].status.replicas}"`
export DAEMON_DEPLOYMENT_REPLICA_COUNT=`kubectl -n dagster get deploy \
    --selector=component=dagster-daemon -o jsonpath="{.items[0].status.replicas}"`

echo "scaling down..."
kubectl -n dagster scale deploy $DAGIT_DEPLOYMENT_NAME --replicas=0
kubectl -n dagster scale deploy $DAEMON_DEPLOYMENT_NAME --replicas=0

export HELM_DAGSTER_RELEASE_NAME="monster"

echo "applying migration..."
helm template $HELM_DAGSTER_RELEASE_NAME dagster/dagster \
    --set "migrate.enabled=true" \
    --show-only templates/job-instance-migrate.yaml \
    | kubectl apply -f -

echo "scaling up..."
kubectl -n dagster scale deploy $DAGIT_DEPLOYMENT_NAME --replicas=$DAGIT_DEPLOYMENT_REPLICA_COUNT
kubectl -n dagster scale deploy $DAEMON_DEPLOYMENT_NAME --replicas=$DAEMON_DEPLOYMENT_REPLICA_COUNT

