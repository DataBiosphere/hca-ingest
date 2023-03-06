#!/bin/bash
# quick shorthand for running helmfile commands without typing out all the args
# usage: ./apply.sh [env [target_branch_or_sha [command]]]
# e.g. ./apply.sh                   # deploys the current branch's head to dev
#      ./apply.sh prod              # deploys the current branch's head to prod
#      ./apply.sh dev main          # deploys the current head commit on main to dev
#      ./apply.sh prod a81cc3f      # deploys the commit with the sha a81cc3f to prod
#      ./apply.sh prod main diff  # diffs the local helmfile config with the state in prod using the main branch's image
# note that the specified commit will only affect the version of our python code that gets deployed to the cluster.
# this command will always use the version of the helm chart you have saved locally.
export TARGET_HEAD=${2:-HEAD}
export COMMAND=${3:-apply}
export ENV=${1:-dev}
GIT_SHORTHASH=$(git rev-parse --short "$TARGET_HEAD")
export GIT_SHORTHASH

function fire_slack_deployment_notification () {
  local -r environment=$1 rev=$2
  local -r user=$(git config user.email)
  local -r token=$(vault read -field=oauth-token secret/dsde/monster/dev/slack-notifier)
  curl --silent --output /dev/null \
    --location --request POST 'https://slack.com/api/chat.postMessage' \
    --header "Authorization: Bearer ${token}" \
    --header "Content-Type: application/json" \
    --data-raw "{
        'channel': 'monster-deploy',
        'text': 'Deployment',
        'blocks': [
            {
                'type': 'section',
                'text': {
                    'type': 'mrkdwn',
                    'text': '*Deployment Complete*'
                }
            },
            {
                'type': 'divider'
            },
            {
                'type': 'section',
                'fields': [
                    {
                        'type': 'mrkdwn',
                        'text': '*Artifact*\n*Environment*\n*Revision*\n*User*'
                    },
                    {
                        'type': 'mrkdwn',
                        'text': 'HCA (dagster) \n${environment}\n${rev}\n${user}'
                    }
                ]
            }
        ]
    }"
}


echo "Deploying to ${ENV}"

if [ "$ENV" == "prod" ]; then
	gcloud container clusters get-credentials hca-cluster --project mystical-slate-284720 --region us-central1-c
else
	gcloud container clusters get-credentials hca-cluster --project broad-dsp-monster-hca-dev --region us-central1-c
fi

helmfile --interactive "$COMMAND"
fire_slack_deployment_notification "${ENV}" "${GIT_SHORTHASH}"
