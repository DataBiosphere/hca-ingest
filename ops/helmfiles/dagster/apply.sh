#!/bin/bash
# quick shorthand for running helmfile commands without typing out all the args
# usage: ./apply.sh [env [target_branch_or_sha [command]]]
# e.g. ./apply.sh                   # deploys the current branch's head to dev
#      ./apply.sh prod              # deploys the current branch's head to prod
#      ./apply.sh dev master        # deploys the current head commit on master to dev
#      ./apply.sh prod a81cc3f      # deploys the commit with the sha a81cc3f to prod
#      ./apply.sh prod master diff  # diffs the local helmfile config with the state in prod using the master branch's image
# note that the specified commit will only affect the version of our python code that gets deployed to the cluster.
# this command will always use the version of the helm chart you have saved locally.
export TARGET_HEAD=${2:-HEAD}
export COMMAND=${3:-apply}
ENV=${1:-dev} GIT_SHORTHASH=$(git rev-parse --short $TARGET_HEAD) helmfile $COMMAND
