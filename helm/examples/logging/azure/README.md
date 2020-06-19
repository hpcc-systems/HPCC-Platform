# HPCC Log Processing via Azure's AKS Insights

Azure's AKS Insights is an optional feature designed to help monitor performance and health of kubernetes based clusters. 
Once enabled and associated with a given AKS with an active HPCC System cluster, the HPCC component logs are automatically captured by Insights, since all STDERR/STDOUT data is captured and made available for monitoring and/or querying purposes. As is usually the case with cloud provider features, cost is a significant consideration and should be well understood by the user. Log content is written to the logs store associated with your Log Analytics workspace.

The AKS Insights interface on Azure provides Kubernetes-centric cluster/node/container-level health metrics visualizations, and direct links to container logs via "log analytics" interfaces. The logs can be queried via “Kusto” query language (KQL). 

    Example query for Transaction summary log entries from known ESP component container
    let ContainerIdList = KubePodInventory
    | where ContainerName =~ 'xyz/myesp'
    | where ClusterId =~ '/subscriptions/xyz/resourceGroups/xyz/providers/Microsoft.ContainerService/managedClusters/aks-clusterxyz'
    | distinct ContainerID;
    ContainerLog
    | where LogEntry contains "TxSummary["
    | where ContainerID in (ContainerIdList)
    | project LogEntrySource, LogEntry, TimeGenerated, Computer, Image, Name, ContainerID
    | order by TimeGenerated desc
    | render table

    Sample output
    > 6/20/2020, 1:00:00.244 AM	stderr	1 TxSummary[activeReqs=6 auth=NA contLen=352 rcv=0ms handleHttp=3ms user=@10.240.0.4 req=POST wsstore.SET v1.0 total=3ms ] 	aks-default-12315622-vmss00000i			bc1555515a09e12a129c3ea5df0b76fb74c4227354dc2b643182c8f910b33ed4	
	  > 6/20/2020, 12:59:58.910 AM	stderr	1 TxSummary[activeReqs=5 auth=NA contLen=99 rcv=0ms handleHttp=2ms user=@10.240.0.5 req=POST wsstore.FETCH v1.0 total=2ms ] 	aks-default-12315622-vmss00000i			bc1555515a09e12a129c3ea5df0b76fb74c4227354dc2b643182c8f910b33ed4

More complex queries can be formulated to fetch specific information provided in any of the log columns including unformatted data in the log message. The Insights interface facilitates creation of alerts based on those queries, which can be used to trigger emails, SMS, Logic App execution, and many other actions.

Log and/or metric capture behavior can be controled via kubernetes yaml:

    https://github.com/microsoft/OMS-docker/blob/ci_feature_prod/Kubernetes/container-azm-ms-agentconfig.yaml
    
Overly chatty streams can be filtered out, capturing K8s events can be turned off, etc.
Always keep in mind sensitive data could be logged, therefore access restriction to insights is strongly advised

