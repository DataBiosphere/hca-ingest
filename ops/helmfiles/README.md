# Deployment

We are deploying code via helmfile. Code is deployed by applying the desired SHA1 via helmfile (via the `apply.sh`)
script in this directory. 

## Process

* Install the helmfile tool via this [repo](https://github.com/roboll/helmfile)
* Install helmfile diff by running `helm plugin install https://github.com/databus23/helm-diff`
* Run `apply.sh <env> <SHA1 | ref>`
* This will deploy a new helm release to the relevant K8S cluster

## Web UI access
We are using port forwarding for access to the Dagster web UI for now. To run:
`forward_ports.sh <env>`