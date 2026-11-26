# Split Dali Deployment

Split a deployment across two Helm releases when Dali and the application stack
need separate ownership:

- The Dali release defines an enabled `dali[]` entry and owns the Dali StatefulSet.
- The application release sets `dali: []`, runs the remaining components, and
  connects to the Dali release through `global.externalDaliSvc`.

Set `global.externalDaliSvc` only in the application release's values. It takes
a `host:port` endpoint; a component-level `externalDaliSvc` can override it. The
Dali release retains its local Dali configuration and does not use this
setting.

## Shared Resources

The Dali-owning release (the one with an enabled dali[] entry) renders the
namespace-wide base NetworkPolicies and shared ServiceAccounts/Roles/RoleBindings,
including policies that enforce namespace-wide egress settings. Configure those
settings only on the Dali release. The application release omits the shared
resources and creates policies for its active components.

NetworkPolicies in this pattern use namespace-scoped pod selectors. A
cross-namespace deployment therefore needs additional Dali ingress and client
egress policies. For Prometheus scraping, configure metrics on each release
whose pods should be labeled. Configure a Prometheus sink with
`settings.autodiscovery: true` on the Dali-owning release to generate the shared
scrape policy, and use the same scrape port across releases.

For values files, deployment commands, and end-to-end validation, see the
[same-namespace split Dali example](../../examples/dali-split-same-namespace/README.md).
