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

## Configure HPCC logAccess
The logAccess feature allows HPCC to query and package relevant logs for various features such as ZAP report, WorkUnit helper logs, ECLWatch log viewer, etc.

### Provide target Grafana/Loki access information 

HPCC logAccess requires access to the Grafana username/password credentials. Those values must be provided via a secure secret object.

The secret is expected to be in the 'esp' category, and be named 'grafana-logaccess'. The following key-value pairs are required (key names must be spelled exactly as shown here)

    username - This should contain the Grafana username
    password - This should contain the Grafana password

#### Create secret using script
The included 'create-grafana-logaccess-secret.sh' helper can be used to create the necessary secret.

Example scripted secret creation command:

```
  create-grafana-logaccess-secret.sh -u admin -p somepass -n hpcc
```

#### Create secret manually from file
Otherwise, users can create the secret manually.

Example manual secret creation command (assuming ./secrets-templates contains files named exactly as the above keys):

```
  kubectl create secret generic grafana-logaccess --from-file=HPCC-Platform/helm/managed/logging/loki-stack/secrets-templates/ -n hpcc
```

#### Create secret manually from manifest
Otherwise, users can create the secret through a manifest file.

First, base64 encode the credentials:

```
echo -n 'admin' | base64
echo -n 'whatevergrafanapassword' | base64
```

Add the encoded values to the provided manifest file 'grafana-logaccess-secret.yaml'

```
apiVersion: v1
kind: Secret
metadata:
  name: grafana-logaccess
type: Opaque
data:
  #Base64 encoded username and password for Grafana
  #can be encoded using the following command:
  # echo -n 'admin' | base64
  username: YWRtaW4=
  # echo -n 'whatevergrafanapassword' | base64
  password: d2hhdGV2ZXJncmFmYW5hcGFzc3dvcmQ=
```

Then apply the manifest values:

```
kubectl apply -f ./grafana-logaccess-secret.yaml --namespace hpcc --server-side
```

#### Verify secret

At this point, confirm the secret has been created with the expected key values:

```
kubectl describe secret grafana-logaccess -n hpcc
```

The output should be something like this:

```
kubectl describe secret grafana-logaccess -n hpcc
Name:         grafana-logaccess
Namespace:    hpcc
Labels:       <none>
Annotations:  <none>

Type:  Opaque

Data
====
password:  40 bytes
username:  5 bytes
```

### Configure HPCC logAccess

The target HPCC deployment should be directed to use the desired Grafana endpoint with the Loki datasource, and the newly created secret by providing appropriate logAccess values (such as ./grafana-hpcc-logaccess.yaml).

Example use:

```
  helm install myhpcc hpcc/hpcc -f HPCC-Platform/helm/managed/logging/loki-stack/grafana-hpcc-logaccess.yaml
```

####

The grafana hpcc logaccess values should provide Grafana connection information, such as the host, and port; the Loki datasource where the logs reside; the k8s namespace under which the logs were created (non-default namespace highly recommended); and the hpcc component log format (table|json|xml)

```
Example use:
  global:
    logAccess:
      name: "Grafana/loki stack log access"
      type: "GrafanaCurl"
      connection:
        protocol: "http"
        host: "myloki4hpcclogs-grafana.default.svc.cluster.local"
        port: 3000
      datasource:
        id: "1"
        name: "Loki"
      namespace:
        name: "hpcc"
      logFormat:
        type: "json"
```
