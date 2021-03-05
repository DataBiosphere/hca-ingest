#!/bin/bash
# This script forwards port 8080 to the Kubernetes cluster running our Dagster install so you can access
# the Dagit dashboard from your machine.
export DAGIT_POD_NAME=$(kubectl get pods --namespace dagster -l "app.kubernetes.io/name=dagster,app.kubernetes.io/instance=monster,component=dagit" -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace dagster port-forward $DAGIT_POD_NAME 8080:80
