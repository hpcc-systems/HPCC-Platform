- Add elastic and opentelemtry helm repos
-- helm repo add elastic https://helm.elastic.co
-- helm repo add elastic  https://open-telemetry.github.io/opentelemetry-helm-charts
- Install elastic eck operator, must be labeled elastic-operator and might only need to be done once 
-- helm install elastic-operator elastic/eck-operator -n elastic-system --create-namespace
- Perform helm dependency update
-- From HPCC-Platform/helm/managed/observability/eck/
-- helm dependency build
- Install observability chart
-- helm install eck-apm HPCC-Platform/helm/managed/observability/eck/ 
- port forward any service to be accessed externally
-- Kibana for example:
--- kubectl port-forward -n elastic-system service/kibana-kb-http 5601:5601
- Fetch elastic user pass for kibana ui access
-- kubectl get secret elasticsearch-es-elastic-user -o=jsonpath='{.data.elastic}' | base64 --decode; echo

TODO
- configure HPCC to export out to collector
  create sample values file akin to this one: https://github.com/hpcc-systems/HPCC-Platform/blob/master/helm/examples/tracing/otlp-http-collector-k8s.yaml
- ensure collector is able to communicate with APM server
  currently, experiencing cert issues:
2025-05-31T01:15:35.600Z        warn    grpc@v1.71.0/clientconn.go:1406 [core] [Channel #5 SubChannel #6]grpc: addrConn.createTransport failed to connect to {Addr: "eck-apm-eck-apm-server-apm-http.default.svc:8200", ServerName: "eck-apm-eck-apm-server-apm-http.default.svc:8200", }. Err: connection error: desc = "transport: authentication handshake failed: tls: failed to verify certificate: x509: certificate signed by unknown authority"       {"grpc_log": true}

