## HPCC Systems HELM Chart

### Dependencies

#### Elastic Stack
HPCC Systems is designed to deploy a local, minimal Elastic Stack instance for log processing out of the box. This feature creates a local dependency on the Elastic Stack Helm charts for ElasticSearch, Filebeats and optionally Kibana.

##### HELM Command
Helm provides a convenient command to automatically pull appropriate dependencies to the /charts directory:
> helm dependency update HPCC-Platform/helm/hpcc

Note: *HPCC-Platform/helm/hpcc* denotes the location of the HPCC Platform HELM chart
##### HELM Install parameter
Otherwise, provide the "--dependency-update" argument in the helm install command
For example:
> helm install myhpcccluster HPCC-Platform/helm/hpcc  --set global.image.version=latest --dependency-update

##### Disabling
The managed Elastic Stack instance can be disabled by setting the following values to false:
>--set elasticsearch.enabled=false
and
>--set kibana.enabled=false

For example:
> helm install myhpcccluster HPCC-Platform/helm/hpcc  --set global.image.version=latest --set elasticsearch.enabled=false --set kibana.enabled=false

However, the HELM install command verifies that the required charts, as expressed in Chart.yaml, are present in charts/ and are at an acceptable version even when above dependency flags are disabled.

##### Removing
If the user chooses to avoid the Elastic Stack dependency altogether, the depedencies declaration in the Chart.yml can be removed. Please note, absent of the Elastic Stack instance, the user would be resposible for HPCC component log processing and persitance strategies. Please see for more information /HPCC-Platform/helm/examples/logging/README.md

##### Cleanup
The Elasticsearch chart will declare a PVC which is used to persist data related to its indexes, and thus, the HPCC Component logs. The PVCs by nature can outlive the HPCC and Elastic deployments, it is up to the user to manage the PVC appropriately, which includes deleting the PVC when they are no longer needed.
