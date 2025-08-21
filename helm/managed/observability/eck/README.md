# elastic4hpccobservability

**elastic4hpccobservability** is a self-contained observability solution for the HPCC Platform. It leverages the Elastic Stack (Elasticsearch, Kibana, APM Server) and OpenTelemetry Collector to provide tracing and logging observability data for HPCC clusters. This project delivers a streamlined, secure, and automated way to deploy, configure, and access observability infrastructure for testing and development using Helm charts. This chart makes several assumptions including no modifications are made to the base HPCC Platform helm deployment. Customization of the HPCC Platform cluster might require counterpart changes to this chart.

---

## Features

- Automated deployment of Elastic Stack components and OpenTelemetry Collector via Helm
- Secure integration with HPCC Platform for trace data export
- Pre-configured dashboards and APM UI in Kibana
- Simple debugging and connectivity validation steps

---

## Quick Start

### Pre-Requisites

#### Kubernetes Cluster

A running Kubernetes cluster (v1.21+ recommended).
Sufficient resources (CPU, memory, storage) for HPCC components and observability stack.
kubectl configured and authenticated to access the cluster.

#### Helm

Helm v3.6+ installed and configured.

#### HPCC Platform

Local access to HPCC-Systems/HPCC-Platform's git repository

```sh
git clone https://github.com/hpcc-systems/HPCC-Platform.git
```

#### Access to the ECK4HPCCObservability chart

```sh
cd HPCC-Platform/helm/managed/observability/eck
```

#### Elastic, OTel and HPCC Systems Helm Repositories

```sh
helm repo add elastic https://helm.elastic.co
helm repo add otel https://open-telemetry.github.io/opentelemetry-helm-charts
helm repo add hpcc https://hpcc-systems.github.io/helm-chart/
```

> **Note:** If any of the above repos were previously added, take note of the local aliases and use them where appropriate

### 1. Fetch Dependency Helm Charts

From the `HPCC-Platform/helm/managed/observability/eck/` directory:

```sh
HPCC-Platform/helm/managed/observability/eck/> helm dependency build .
```

### 2. Install Elastic ECK Operator

> **Note:** The release must be named `elastic-operator`.

```sh
helm install elastic-operator elastic/eck-operator -n elastic-system --create-namespace
```

### 3. Install the Observability Chart

> **Note:** The release must be named `eck-apm`.


From the `HPCC-Platform/helm/managed/observability/eck/` directory:

```sh
HPCC-Platform/helm/managed/observability/eck> helm install eck-apm .
```

### 4. Configure HPCC to Export Traces

Inject the sample [jtrace configuration values file](./otlp-http-collector-k8s.yaml) onto your HPCC cluster.

Details on HPCC trace configuration can be found in [helm/examples/tracing/README](https://github.com/hpcc-systems/HPCC-Platform/blob/master/helm/examples/tracing/README.md).

> **Note:** Choose one of the next two steps:

```sh
#Choose this option if HPCC-Platform is currently NOT deployed
#deploy fresh HPCC cluster
helm install myhpcc hpcc/hpcc -f ./otlp-http-collector-k8s.yaml
```

```sh
#Choose this option if HPCC-Platform is already deployed
#upgrade pre-existing HPCC cluster
#Note: this command assumes the existing cluster is named myhpcc, use the actual cluster name
helm upgrade myhpcc hpcc/hpcc -f ./otlp-http-collector-k8s.yaml
```

---

## Accessing Observability Data

### Access via Kibana

- Kibana UI: [https://localhost:5601](https://localhost:5601)
    - Your browser may warn about a self-signed certificate.
- Login with username: `elastic`
- Fetch the password for the `elastic` user:

    ```sh
    kubectl get secret elasticsearch-es-elastic-user -o=jsonpath='{.data.elastic}' | base64 --decode; echo
    ```

#### Access Trace data

- Once logged in, navigate to **Observability (top-right hamburger menu) then Applications → Traces**
- Ensure the time range (top right) is set appropriately
- Traces are grouped by originating service (esp, thor, etc.) or transaction type (wsstore/feth, wsworkunits/wuquery, run_workunit, etc)
- Specific traces can be queried by many keywords such as trace.id, span.id
- Traces can be interrogated for timings, errors, dependency order, and other transaction span idiosyncrasies

#### Access Log data

- If HPCC logs are available, trace-relevant log information is trivially accessed by choosing "Investigate->Trace Logs" from the trace view.
- This action will forward the user to the "Discover" view with a default query searching for all logs which contain the trace.id
- This query can be changed to expand or focus the log information presented
- The view can be customized to display fields of interest such as the log message, the source pod name, etc.
- Otherwise, logs can be queried via Elastic's query language (KQL) by navigating to "Discover" from the hamburger button.
---

## Debugging & Validation

### Test OpenTelemetry Collector

Generate sample traces:

```sh
docker run --rm ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest \
  traces \
  --otlp-endpoint=host.docker.internal:4317 \
  --otlp-insecure \
  --duration=30s \
  --rate=5 \
  --service="test-service"
```

### Test APM Server Certificate-Based Connectivity

By default the APM server is not accessible outside of the cluster. To test, perform the following steps from within the cluster, or expose the service outside of the cluster.

#### Fetch Elastic APM Token and CA Certificate

```sh
export ELASTIC_APM_SECRET_TOKEN=$(kubectl get secret/eck-apm-eck-apm-server-apm-token --template '{{index .data "secret-token"}}' | base64 -d)
export ELASTIC_APM_SERVER_CA_CERT_FILE=$(mktemp)
```

#### Confirm environment variables:

```sh
env | grep ELASTIC_APM_
```

#### Contact APM server

```sh
curl --resolve *:8200:127.0.0.1 --cacert ${ELASTIC_APM_SERVER_CA_CERT_FILE} \
  -H "Authorization: Bearer ${ELASTIC_APM_SECRET_TOKEN}" \
  https://eck-apm-eck-apm-server-apm-http.default.svc:8200
```

---

## Project Structure

- **Helm charts:** Automated deployment of Elastic Stack and OpenTelemetry Collector
- **Pre-configured values:** HPCC jTrace pipeline
- **Documentation:** Step-by-step setup, access, and troubleshooting

---
