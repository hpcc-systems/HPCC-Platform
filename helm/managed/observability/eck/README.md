- Add elastic and opentelemtry helm repos
-- helm repo add elastic https://helm.elastic.co
-- helm repo add otel  https://open-telemetry.github.io/opentelemetry-helm-charts

- Install elastic eck operator, must be labeled elastic-operator 
-- helm install elastic-operator elastic/eck-operator -n elastic-system --create-namespace

- Fetch dependancy helm charts 
-- From HPCC-Platform/helm/managed/observability/eck/
-- helm dependency build .

- Install observability chart
-- From HPCC-Platform/helm/managed/observability/eck/
-- helm install eck-apm .

- Fetch elastic user pass for kibana ui access
-- kubectl get secret elasticsearch-es-elastic-user -o=jsonpath='{.data.elastic}' | base64 --decode; echo

TODO
- configure HPCC to export out to collector
  create sample values file akin to this one: https://github.com/hpcc-systems/HPCC-Platform/blob/master/helm/examples/tracing/otlp-http-collector-k8s.yaml
