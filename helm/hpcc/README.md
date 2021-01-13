## HPCC Systems HELM Chart

### Dependencies
- HPCC Systems is designed to deploy a local, minimal Elastic Stack instance for log processing. This dependency requires a local copy of the Elastic Stack Helm charts for ElasticSearch, Filebeats and optionally Kibana. Helm provides a convenient command to pull appropriate dependencies locally:
> helm dependency update

- Note, the above command should be issued from the hpcc chart location.

- Otherwise, provide the "--dependency-update" argument in the helm install command
> For example:
> helm install myhpcccluster . --set global.image.version --dependency-update

- The managed Elastic Stack instance can be disabled by setting the following values to false:
>--set elasticsearch.enabled=false
and
>--set kibana.enabled=false

- Please note, even when above dependency flags are disabled, the helm install process enforces all declared dependency charts are available.