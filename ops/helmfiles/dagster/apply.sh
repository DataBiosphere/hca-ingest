#!/bin/bash
# quick shorthand for deploying changes to the helm chart.
# usage: ./apply.sh [env [target_branch_or_sha]]
# e.g. ./apply.sh               # deploys the current branch's head to dev
#      ./apply.sh prod          # deploys the current branch's head to prod
#      ./apply.sh dev master    # deploys the current head commit on master to dev
#      ./apply.sh prod a81cc3f  # deploys the commit with the sha a81cc3f to prod
# note that the specified commit will only affect the version of our python code that gets deployed to the cluster.
# this command will always use the version of the helm chart you have saved locally.
export TARGET_HEAD=${2:-HEAD}
ENV=${1:-dev} GIT_SHORTHASH=$(git rev-parse --short $TARGET_HEAD) helmfile apply
