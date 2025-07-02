# elastic4hpccobservability

**elastic4hpccobservability** is a comprehensive, self-contained observability solution for the HPCC Platform. It leverages the Elastic Stack (Elasticsearch, Kibana, APM Server) and OpenTelemetry Collector to provide distributed tracing, metrics, and observability data for HPCC clusters. This project delivers a streamlined, secure, and automated way to deploy, configure, and access observability infrastructure using Helm charts.

---

## Features

- Automated deployment of Elastic Stack components and OpenTelemetry Collector via Helm
- Secure integration with HPCC Platform for trace data export
- Pre-configured dashboards and APM UI in Kibana
- Simple debugging and connectivity validation steps

---

## Quick Start

### 1. Add Helm Repositories

```sh
helm repo add elastic https://helm.elastic.co
helm repo add otel https://open-telemetry.github.io/opentelemetry-helm-charts
```

### 2. Fetch Dependency Helm Charts

From the `HPCC-Platform/helm/managed/observability/eck/` directory:

```sh
helm dependency build .
```

### 3. Install Elastic ECK Operator

> **Note:** The release must be named `elastic-operator`.

```sh
helm install elastic-operator elastic/eck-operator -n elastic-system --create-namespace
```
### 4. Install the Observability Chart

> **Note:** The release must be named `eck-apm`.


From the `HPCC-Platform/helm/managed/observability/eck/` directory:

```sh
helm install eck-apm .
```

### 5. Configure HPCC to Export Traces

From the HPCC Platform root directory, run one of:

```sh
helm upgrade myhpcc helm/hpcc -f helm/managed/observability/eck/otlp-http-collector-k8s.yaml
# or
helm install myhpcc helm/hpcc -f helm/managed/observability/eck/otlp-http-collector-k8s.yaml
```

---

## Accessing Trace Data

### Access via Kibana

- Kibana UI: [https://localhost:5601](https://localhost:5601)
    - Your browser may warn about a self-signed certificate.
- Login with username: `elastic`
- Fetch the password for the `elastic` user:

    ```sh
    kubectl get secret elasticsearch-es-elastic-user -o=jsonpath='{.data.elastic}' | base64 --decode; echo
    ```

- Once logged in, navigate to **Observability → Traces**
- Ensure the time range (top right) is set appropriately

---

## Debugging & Validation

### Fetch Elastic APM Token and CA Certificate

```sh
export ELASTIC_APM_SECRET_TOKEN=$(kubectl get secret/eck-apm-eck-apm-server-apm-token --template '{{index .data "secret-token"}}' | base64 -d)
export ELASTIC_APM_SERVER_CA_CERT_FILE=$(mktemp)
```

Confirm environment variables:

```sh
env | grep ELASTIC_APM_
```
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
