#!/usr/bin/env bash

# Imports a given staging area to the production DCP2 dataset. This script expects to be run from the
# scripts/release_tools subdirectory of the HCA repo.
# Prerequisites:
#   * the argo CLI tool (https://argoproj.github.io/argo-workflows/cli/)
#   * gcloud + kubectl
#   * gdate from coreutils (brew install coreutils)
#   * The currently logged in gcloud credentials have access to the broad-dsp-monster-hca-prod google project
#
# Invoking the script:
# ./import_prod.sh <bucket name> <subdir of bucket where the staging area resides> <staging bucket prefix>
#
# Example:
# ./import_prod.sh broad-foo-bar-bucket-name /prod/baz-staging-area dcp2

SOURCE_BUCKET_NAME=$1
SOURCE_BUCKET_PREFIX=$2

STAGING_BUCKET_PREFIX=$3
STAGING_BUCKET=${STAGING_BUCKET_PREFIX}_$(gdate +%Y_%m_%d_%H%M%S)

# always run these imports in our prod k8s cluster
CURRENT_CLUSTER=$(kubectl config current-context)
if [ $CURRENT_CLUSTER != "gke_mystical-slate-284720_us-central1-c_hca-cluster" ] ; then
  echo "Connecting to HCA prod cluster..."
  gcloud container clusters get-credentials hca-cluster --zone us-central1-c --project mystical-slate-284720
fi

echo "Importing to Staging Bucket: "$STAGING_BUCKET
argo submit ../../orchestration/workflows/prod/run-import-hca-total.yaml \
     -p source-bucket-name="$SOURCE_BUCKET_NAME" \
     -p source-bucket-prefix="$SOURCE_BUCKET_PREFIX" \
     -p staging-bucket-prefix="$STAGING_BUCKET" \
     -p dataset-id="d30e68f8-c826-4639-88f3-ae35f00d4185" \
     -p data-repo-name="datarepo_hca_prod_20201120_dcp2"
