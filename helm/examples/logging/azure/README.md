# HPCC Log Processing via Azure's AKS Insights

Azure's AKS Insights is an optional feature designed to help monitor performance and health of kubernetes based clusters.

***See Azure's documentation on logging architecture:*** [Azure Logging Documentation](https://docs.microsoft.com/en-us/azure/architecture/microservices/logging-monitoring#logging)

Once enabled and associated with a given AKS with an active HPCC System cluster, the HPCC component logs are automatically captured by Insights, since all STDERR/STDOUT data is captured and made available for monitoring and/or querying purposes. As is usually the case with cloud provider features, cost is a significant consideration and should be well understood by the user. Log content is written to the logs store associated with your Log Analytics workspace.

***Informative 3rd party blog on setting up logging to Azure from AKS Cluster:*** [External AKS Logging blog](https://trstringer.com/native-azure-logging-aks/)

The AKS Insights interface on Azure provides Kubernetes-centric cluster/node/container-level health metrics visualizations, and direct links to container logs via "log analytics" interfaces. The logs can be queried via “Kusto” query language (KQL). 

***See Azure's documentation on how to query logs:*** [Azure Log Query Documentation](https://docs.microsoft.com/en-us/azure/azure-monitor/containers/container-insights-log-query)

## Example KQL query for fetching "Transaction summary" log entries from an ECLWatch container:

```bash
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
```
    Sample output
    > 6/20/2020, 1:00:00.244 AM	stderr	1 TxSummary[activeReqs=6 auth=NA contLen=352 rcv=0ms handleHttp=3ms user=@10.240.0.4 req=POST wsstore.SET v1.0 total=3ms ] aks-default-12315622-vmss00000i  bc1555515a09e12a129c3ea5df0b76fb74c4227354dc2b643182c8f910b33ed4
    > 6/20/2020, 1:59:58.910 AM stderr	1 TxSummary[activeReqs=5 auth=NA contLen=99 rcv=0ms handleHttp=2ms user=@10.240.0.5 req=POST wsstore.FETCH v1.0 total=2ms ] aks-default-12315622-vmss00000i bc1555515a09e12a129c3ea5df0b76fb74c4227354dc2b643182c8f910b33ed4

More complex queries can be formulated to fetch specific information provided in any of the log columns including unformatted data in the log message. The Insights interface facilitates creation of alerts based on those queries, which can be used to trigger emails, SMS, Logic App execution, and many other actions.

## General Considerations
- Overly chatty log stream can impact costs and performance, see [HPCC Logging Details](https://github.com/hpcc-systems/HPCC-Platform/tree/master/helm/examples/logging#hpcc-systems-application-level-logging-details) for information on how to control HPCC component log verbosity.
- Unexpected billing surges could occur anyway, preventive Azure filters/actions should be created
- Setting strong access restriction to insights is strongly advised to prevent information leakage.
- Alerts/Triggers can be defined based on KQL queries and/or other metrics which can be tied to sms/email/azure functions/azure logic apps and several others action types are possible
