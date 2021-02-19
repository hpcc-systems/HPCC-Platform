## This folder contains lightweigth Elastic Stack deployment charts and HPCC prefered values

This chart describes a local, minimal Elastic Stack instance for HPCC Systems component log processing.

### Dependencies
This chart is dependent on the Elastic Stack Helm charts for ElasticSearch, Filebeats and Kibana.

#### Dependency update
##### HELM Command
Helm provides a convenient command to automatically pull appropriate dependencies to the /charts directory:
> helm dependency update

##### HELM Install parameter
Otherwise, provide the "--dependency-update" argument in the helm install command
For example:
> helm install myelastic ./ --dependency-update

##### Cleanup
The Elastic Search chart will declare a PVC which is used to persist data related to its indexes, and thus, the HPCC Component logs. The PVCs by nature can outlive the HPCC and Elastic deployments, it is up to the user to manage the PVC appropriately, which includes deleting the PVC when they are no longer needed.
