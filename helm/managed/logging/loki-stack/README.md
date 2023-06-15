## This folder contains lightweight Loki Stack deployment chart and HPCC Systems preferred values

This chart describes a local, minimal Loki Stack instance for HPCC Systems component log processing.
Once successfully deployed, HPCC component logs produced within the same namespace are forwarded to the Loki aggregation system via Promtail, and exposed via Grafana. 

Users can query those logs by issuing Grafana DataSource API queries, or interactively through the GrafanaUI.

A Loki Datasource is created automatically, which allowers users to monitor/query HPCC component logs via Grafana.

### Helm Deployment
To deploy the light-weight Loki Stack for HPCC component log processing issue the following command:

>helm install myloki HPCC-Systems/helm/managed/logging/loki-stack/

### Dependencies
This chart is dependent on the Grafana Loki-stack Helm charts which in turn is dependent on Loki, Grafana, Promtail.

#### Dependency update
##### HELM Command
Helm provides a convenient command to automatically pull appropriate dependencies to the /charts directory:
> helm dependency update HPCC-Systems/helm/managed/logging/loki-stack/

##### HELM Install parameter
Otherwise, provide the "--dependency-update" argument in the helm install command
For example:
> helm install myloki HPCC-Systems/helm/managed/logging/loki-stack/ --dependency-update

### Components
Grafana Loki Stack is comprised of a set of components that which serve as a full-featured logging stack.

The Stack is described as such:

>Unlike other logging systems, Loki is built around the idea of only indexing metadata about your logs: labels (just like Prometheus labels). Log data itself is then compressed and stored in chunks in object stores such as S3 or GCS, or even locally on the filesystem. A small index and highly compressed chunks simplifies the operation and significantly lowers the cost of Loki.

#### Promtail

>Promtail is an agent which ships the contents of local logs to a Loki instance

Further reading here: https://github.com/grafana/helm-charts/tree/main/charts/promtail

#### Loki

>A horizontally-scalable, highly-available, multi-tenant log aggregation system inspired by Prometheus. It is designed to be very cost effective and easy to operate. It does not index the contents of the logs, but rather a set of labels for each log stream.is self described as "Like Prometheus, but for logs"

Further reading here: https://github.com/grafana/loki

#### Grafana

>Grafana allows users to query, visualize, alert on and understand metrics and logs regardless of target storage.

Further reading here: https://github.com/grafana/grafana

### HPCC Component Log Queries

HPCC component logs can be queried from the Grafana UI. Out of the box, Grafana is exposed as a LoadBalanced service reachable on 'http://localhost:3000'.

The Managed Grafana service is declared as type LoadBalancer for convenience to the user. However it is imperative to control external access to the service.
The service is defaulted to "internal load balancer" on Azure, the user is encouraged to set similar values on the target cloud provider. See the Grafana.service.annotations section:

Grafana access is restricted by default to user 'admin', and the dynamic password can be fetched by issuing the following command:

```console
kubectl get secret myloki-grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo
```
Where 'myloki' is the helm release name used to deploy the Loki-stack.

From the 'Explore' view in Grafana, and the 'Loki' datasource selected, the following query can be applied to filter in logs from desired HPCC components:

```console
{component=~"dafilesrv|dali|dfuserver|eclagent|eclccserver|eclscheduler|esp|sasha"}
```


### Persistance
The default Loki-Stack chart will not declare permanent storage and therefore log data will be lost when the deployment is removed. If persistance is required, set loki.persistence.enabled to true, and provide the appropriate PV

```console
loki:
  persistence:
    enabled: true
```