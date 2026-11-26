# Dali split deployment (Dali-only + app release)

This example deploys HPCC in two releases:

1. A Dali-only release that owns the Dali StatefulSet.
2. An application release that runs the rest of the HPCC stack without a local Dali entry and points its Dali clients at the first release via `global.externalDaliSvc`.

This pattern uses `hpcc.getDali`. The application release points at the Dali-only
release with `externalDaliSvc`, a `host:port` endpoint that can be set globally
or on an individual component.

- Dali clients read the Dali endpoint from component-level `externalDaliSvc`,
  falling back to `global.externalDaliSvc` (`host:port`).
- `global.externalDaliSvc` is set on the app release only; the Dali-only release
  still defines its own `values.dali[]` entry so it can create the StatefulSet.

## Prerequisites

- Kubernetes cluster and namespace create permissions
- Helm 3
- This example deploys both releases to the same namespace and uses the
  same-namespace service-name shorthand `mydali:7070`.
- Commands omit `-n`/`--namespace` flags and use the current kubectl context;
  ensure your context is configured for the target namespace.

## Values files

- `values-dali-only.yaml`: deploys only Dali StatefulSet (no ESP/Thor/Roxie/ECLAgent/etc).
- `values-no-dali.yaml`: deploys normal app stack with no local Dali entry; sets
  `global.externalDaliSvc` to point at the Dali-only release's service.

## Namespace resource ownership

The release containing an enabled entry in `values.dali[]` automatically owns the namespace-wide,
fixed-name base policies and shared ServiceAccounts/Roles/RoleBindings. The app
release does not recreate those resources; it generates policies for its active
ESP and Sasha components instead. Configure namespace-wide egress settings such
as `global.egress.restricted`, `kubeSystemLabel`, `kubeApiCidr`, and `kubeApiPort`
on the Dali-owning release; that release alone renders the base NetworkPolicies
that enforce those settings.

## Deployment

Both releases are deployed to the same namespace using the current kubectl context.
Before running the commands below:

1. Verify or set the active namespace: `kubectl config set-context --current --namespace=<namespace>`
2. Ensure this is the namespace where you want both releases to run.

`dali-release` automatically owns the namespace-wide base `NetworkPolicy` and shared RBAC resources.
`app-release` uses the external Dali endpoint and generates policies only for its active components.

Use the following commands to start the Dali and app pods, then stop them with
the cleanup command below.

```bash
helm install dali-release hpcc/hpcc -f helm/examples/dali-split-same-namespace/values-dali-only.yaml

helm install app-release hpcc/hpcc -f helm/examples/dali-split-same-namespace/values-no-dali.yaml

kubectl get pods
```

Expected result:

- `dali-release` creates the only Dali StatefulSet.
- `app-release` creates no Dali StatefulSet.
- `app-release` pods become healthy and use `--daliServers=mydali:7070`.

Run the [end-to-end validation](#end-to-end-validation) below, then clean up:

```bash
helm uninstall app-release dali-release
```

## End-to-end validation

After `app-release` is healthy, run a simple ECL query end-to-end from the app release:

1. Access ECL Watch for app release (service created by app release).
2. Submit a simple workunit (for example, a trivial `OUTPUT('ok');`).
3. Confirm compile and run both succeed.

You can also validate Dali wiring from pod args:

```bash
kubectl get pods -o yaml | grep -- "--daliServers="
```

## Notes

- `values-no-dali.yaml` sets `dali: []` to remove the chart's default Dali
  entry. This prevents the app release from creating a local Dali StatefulSet
  and from owning the shared Dali policies and RBAC resources.
- If `dali: []` is set and `global.externalDaliSvc` is not set, template
  rendering fails for components that require Dali.
- Set `global.externalDaliSvc` only in the app release's values file
  (`values-no-dali.yaml`), pointing it at the Dali service endpoint created by
  the Dali-only release. `externalDaliSvc` accepts a `host:port` endpoint; this
  example uses the same-namespace shorthand `mydali:7070`, while a Dali service
  in another namespace can use
  `mydali.<dali-namespace>.svc.cluster.local:7070`. A component-level
  `externalDaliSvc` in the app release values can override the global endpoint.
- This example does not configure a cross-namespace deployment. The generated
  Dali client and server NetworkPolicies use namespace-scoped pod selectors;
  provide additional ingress and egress policies for Dali and its clients when
  they are in different namespaces.
- Namespace-wide base NetworkPolicies and shared RBAC resources are generated
  only by a release with an enabled entry in `values.dali[]`.
- The `prometheusMetricsReporter` pod label is applied per-release from that
  release's own `global.metrics` config, so configure `global.metrics` on
  every release whose pods should be labeled/scraped. The release containing
  the enabled local Dali owns the fixed-name `hpcc-metrics-prometheus`
  `NetworkPolicy`, so it must also configure a Prometheus sink with
  `settings.autodiscovery: true` if metrics scraping is required in the
  namespace. Use the same scrape port in every release's Prometheus sink,
  because the owner-generated policy allows that port for all labeled pods.
