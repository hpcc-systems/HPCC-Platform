- Add elastic and opentelemtry helm repos
-- helm repo add elastic https://helm.elastic.co
-- helm repo add otel  https://open-telemetry.github.io/opentelemetry-helm-charts

- Install elastic eck operator, **must be labeled elastic-operator**
-- helm install elastic-operator elastic/eck-operator -n elastic-system --create-namespace

- Fetch dependancy helm charts 
-- From HPCC-Platform/helm/managed/observability/eck/
-- helm dependency build .

- Install observability chart, **must be labeled eck-apm**
-- From HPCC-Platform/helm/managed/observability/eck/
-- helm install eck-apm .

- Instruct HPCC to export traces to recently deployed otel collector
-- helm upgrade myhpcc HPCC-Platform/helm/hpcc -f HPCC-Platform/helm/managed/observability/eck/otlp-http-collector-k8s.yaml

or 

-- helm install myhpcc HPCC-Platform/helm/hpcc -f HPCC-Platform/helm/managed/observability/eck/otlp-http-collector-k8s.yaml

- Access trace data
- Access via Kibana
-- Kibana should be available on https://localhost:5601
-- Browser might warn about self-signed certificate
-- use 'elastic' username
-- Fetch 'elastic' user pass for kibana ui access
--- kubectl get secret elasticsearch-es-elastic-user -o=jsonpath='{.data.elastic}' | base64 --decode; echo
-- Once logged on, navigate to Observability->APM->Traces
-- Ensure time range on top right is set appropriately

- Debugging:
-- Fetch elastic token and ca_cert:
--- export ELASTIC_APM_SECRET_TOKEN=$(kubectl get secret/eck-apm-eck-apm-server-apm-token --template '{{index .data "secret-token"}}' | base64 -d)
--- export ELASTIC_APM_SERVER_CA_CERT_FILE=$(mktemp)
-- Confirm:
--- env | grep ELASTIC_APM_
-- Test APM server cert based connectivity
> curl --resolve *:8200:127.0.0.1 --cacert ${ELASTIC_APM_SERVER_CA_CERT_FILE} -H "Authorization: Bearer ${ELASTIC_APM_SECRET_TOKEN}" https://eck-apm-eck-apm-server-apm-http.default.svc:8200

-- Test Otel collector
--- Generate sample traces
---- docker run --rm ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest   traces   --otlp-endpoint=host.docker.internal:4317   --otlp-insecure   --duration=30s   --rate=5   --service="test-service"
