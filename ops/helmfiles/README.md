This is a WIP.

* Install the helmfile tool via this [repo](https://github.com/roboll/helmfile)
* Install helmfile diff by running `helm plugin install https://github.com/databus23/helm-diff`
* Make sure you're connected to the appropriate GKE cluster
* Run `helmfile diff` to see what changes would be applied
* Run `helmfile apply` to deploy your changes.

To access the ArgoCD instance, make sure you're connected to
the HCA cluster for your desired environment and set up port-forwarding:
`kubectl port-forward svc/hca-{env}-argocd-server -n argocd 8080:443`

Then go to `localhost:8080` to access the UI.