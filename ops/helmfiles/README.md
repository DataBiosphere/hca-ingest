# Deployment

Code is deployed by applying the desired SHA1 via helmfile (via the `apply.sh`) \
script in this directory, and assumes that you have set up Vault and a GitHub Token. \
This is covered in the prerequisites of the [Monster Dev Playbook](https://docs.google.com/document/d/1b03-YphH6Uac5huBopLYTYjzgDAlwS6qf-orMqaph64/edit?usp=sharing)
and should be done locally, \
*NOT* in the Docker container, if you are using the Docker Compose dev env in order to avoid accidentally publishing \
your token to the Docker image.
<details>
  <summary>Here are the relevant steps:</summary>

To set up GitHub token: \
* In your GitHub account go to Settings> Developer Settings> Personal Access Tokens
* Generate a new token “For Vault” (or something like that) and give it “read:org” permissions - Save
* Copy the token
* Create a new file in your home dir “.github-token” - past the copied token there.
To set up Vault: \
* In your .bashrc or .bash-profile (etc) add the following:
  * `export VAULT_ADDR="https://clotho.broadinstitute.org:8200`
* Then authenticate to Vault:
  * `vault login -method=github token=$(cat ~/.github-token)`
* Then add that token to your .bash-profile:
  * `export VAULT_TOKEN=$(cat ~/.vault-token)`
</details>

## Process
_Note that this should be done locally, not in the Docker container, if you are using the Docker Compose dev env._
* Install the helmfile tool via this [repository](https://github.com/helmfile/helmfile)
    * https://helmfile.readthedocs.io/en/latest/#installation (you can use scoop, homebrew, or download a specific release)
* Install helmfile diff by running `helm plugin install https://github.com/databus23/helm-diff`
* Run `apply.sh <env> <SHA1 | ref>`
  * For example, to deploy `master`: `apply.sh dev master`
* This will deploy a new helm release to the relevant K8S cluster and send a Slack notification with relevant
deployment info.

## Web UI access
We are using port forwarding for access to the Dagster web UI for now. 
To run:`dagster/forward_ports.sh <env>`
* For example, to access the dev environment: `dagster/forward_ports.sh dev`

<details>
  <summary>Possible future dev work for accessing the web UI via Docker</summary> 
  Below was an attempt to get around the fact that the port-forward command needs --address to listen on the host. \
  By default, it only listens on localhost and when the connection comes through Docker, it’s not to localhost. \
  https://kubernetes.io/docs/reference/generated/kubectl/kubectl-commands#port-forward \
  This almost works, but the connection times out. \
  I've decided that since the deploy should be done locally for security reasons, \
  it's not worth the effort to get this working. \
  
  (these would go at the end of `forward_ports.sh`), \
  replacing `kubectl --namespace dagster port-forward $DAGIT_POD_NAME 8080:80` \
  `kubectl config set-context --current --namespace dagster` \
  `kubectl port-forward --address localhost,172.18.0.2 $DAGIT_POD_NAME 8080:80`

</details>