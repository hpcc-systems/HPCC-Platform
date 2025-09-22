# HPCC Metrics
HPCC components collect metrics describing overall component health and state. In order to use component metrics for
activities such as scaling and alerting, the collected metrics must be reported to a collection system such as
Prometheus, Elasticsearch, or Azure Insights.

## Enabling Metric Reporting
To enable reporting of metrics to a collection system, add metric configuration settings to the helm chart. The
configuration includes the collection system in use and the settings necessary to report metrics.

The provided HPCC helm chart provides all global settings from its values.yml file to all components. To enable
metrics reporting, either include the metrics configuration as a permanent part of your HPCC helm chart values.yml
file, or add it as command line settings at chart installation. Since the configuration consists of multiple settings,
if enabling metrics via the command line, use the _-f filename_ option for convenience.

This directory contains examples of configuration settings for HPCC provided collection systems. See each file for
a description of its available settings. Use the file as is, or customize for your installation.

For example, the following adds the Prometheus configuration as the collection system.

```bash
helm install mycluster ./hpcc -f <path>/prometheus_metrics.yml
```
### Prometheus Collection System
When enabled and properly configured, the HPCC metrics for Prometheus feature exposes an HTTP service on each pod of any HPCC component
which reports metrics. It also annotates all HPCC pods to be discoverable by Prometheus servers and provides the connectivity information required, such as
the port to listen on, and the url path on which to serve Prometheus formatted metrics. Auto-discovery annotations can be disabled via metrics.sinks[type=prometheus].settings.autodiscovery

## Harvesting HPCC metrics
###  Managed Prometheus deployment for HPCC (prometheus4hpccmetrics)
HPCC Systems provides a convenient Helm chart which deploys and configures a local Prometheus Kube Stack instance for the purpose of HPCC component metrics processing. Enabling of the Prometheus metrics collection system as detailed above is required.

- Add HPCC helm repository helm repo
  ```bash
    > helm repo add hpcc https://hpcc-systems.github.io/helm-chart/
    ```
- Deploy prometheus4hpccmetrics
  ```bash
    > helm install mypromstack hpcc/prometheus4hpccmetrics
     NAME: mypromstack
     LAST DEPLOYED: Fri Nov  5 10:44:17 2021
     NAMESPACE: default
     STATUS: deployed
     REVISION: 1
    ```
- Confirm Prometheus Stack pods are in ready state
  ```bash
    > kubectl get pods
    NAME                                                  READY   STATUS    RESTARTS   AGE
    mypromstack-grafana-57bbff469b-lf4z7                  2/2     Running   0          102s
    mypromstack-kube-prometheu-operator-94674f97f-86dk5   1/1     Running   0          102s
    mypromstack-kube-state-metrics-86d7f754f4-8r44w       1/1     Running   0          102s
    mypromstack-prometheus-node-exporter-dpsss            1/1     Running   0          102s
    mypromstack-prometheus-node-exporter-h5nwn            1/1     Running   0          102s
    mypromstack-prometheus-node-exporter-jhzwx            1/1     Running   0          102s
    prometheus-mypromstack-kube-prometheu-prometheus-0    2/2     Running   0          95s
    ```
- Confirm Prometheus stack services have deployed successfully
  ```bash
    > kubectl get svc
    NAME                                    TYPE           CLUSTER-IP     EXTERNAL-IP     PORT(S)             AGE
    kubernetes                              ClusterIP      <Removed>      <none>          443/TCP             24h
    mypromstack-grafana                     ClusterIP      <Removed>      <none>          80/TCP              9m9s
    mypromstack-kube-prometheu-operator     ClusterIP      <Removed>      <none>          443/TCP             9m9s
    mypromstack-kube-prometheu-prometheus   ClusterIP      <Removed>      <none>          9090/TCP            9m9s
    mypromstack-kube-state-metrics          ClusterIP      <Removed>      <none>          8080/TCP            9m9s
    mypromstack-prometheus-node-exporter    ClusterIP      <Removed>      <none>          9100/TCP            9m9s
    prometheus-operated                     ClusterIP      None           <none>          9090/TCP            9m1s
    ```
- Prometheus exposes available metrics and various tools via dashboard on port 9090 (address will vary - see svc listing)
- Confirm HPCC Metrics services are targeted by Prometheus
  - Find the "hpcc-per-pod-metrics" section under Status -> Targets (on the Prometheus dashboard)
  ```bash
      Endpoint	State	Labels	Last Scrape	Scrape Duration	Error
      http://<removed>:8767/metrics	UP	containername="eclwatch"job="hpcc-per-pod-metrics"	8m 47s ago	1.048ms
      ```
- Confirm HPCC metrics are scraped from above services
   - On the 'Graph' section of the dashboard, search for known HPCC metrics such as 'esp_requests_active'
  ```bash
      esp_requests_active{containername="eclqueries", job="hpcc-per-pod-metrics"}	3
      esp_requests_active{containername="eclservices",  job="hpcc-per-pod-metrics"}	0
      esp_requests_active{containername="eclwatch", job="hpcc-per-pod-metrics"}	3
      esp_requests_active{containername="esdl-sandbox",  job="hpcc-per-pod-metrics"}	3
      esp_requests_active{containername="sql2ecl", job="hpcc-per-pod-metrics"} 0
      ```
- The available Grafana service provides advanced tools for higher level metrics processing and alerting
- please see [Managed Prometheus Helm chart](https://github.com/hpcc-systems/HPCC-Platform/tree/master/helm/managed/metrics/prometheus) for further details.

### Self-Managed Prometheus
One of the advantages of the aforementioned managed Prometheus Helm chart, is the pre-configured scrape job 'hpcc-per-pod-metrics' which instructs Prometheus to scrape metrics from all HPCC pods which report metrics.
On a self-managed Prometheus set-up, an "additionalScrapeConfigs" entry is necessary to link the HPCC metrics services to Prometheus

For example, the following Prometheus scrape job can be applied to Prometheus deployments as part of the 'additionalScrapeConfigs' configuration:

```bash
    - job_name: 'hpcc-per-pod-metrics'
      scrape_interval: 5s
      metrics_path: /metrics
      kubernetes_sd_configs:
      - role: pod
      relabel_configs:
      - source_labels: [ __meta_kubernetes_pod_annotation_prometheus_io_scrape ]
        action: keep
        regex: true
      - source_labels: [ __meta_kubernetes_pod_annotation_prometheus_io_path ]
        action: replace
        target_label: __metrics_path__
        regex: (.+)
      - source_labels: [ __address__, __meta_kubernetes_pod_annotation_prometheus_io_port ]
        action: replace
        regex: ([^:]+)(:\d+)?;(\d+)
        replacement: ${1}:${3}
        target_label: __address__
```

- More information on the "additionalScrapeConfigs" options provided by Prometheus here: <https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config>
- 3rd Party write up on the relabel_configs used in the additionalscrapeconfigs here: <https://gist.github.com/reachlin/a98b90afcbff4604c90c183a0169474f>

### Azure Insights (Monitoring and Analytics)
HPCC metrics can be easily routed to Azure Insights for application level monitoring and analytics via the aforementioned Prometheus metrics services. This process requires the HPCC Prometheus metrics collection system, but does not involve any instances the Prometheus server. Since Azure Insights is able to scrape metrics from Prometheus styled services, this process relies on enabling Azure Insights on the target AKS cluster, and configuring the automatic scraping of metrics from the Prometheus metric endpoints provided by HPCC components.

- Enable HPCC's Prometheus metrics on the target HPCC deployment.
  - For example (utilizing prometheus_metrics.yml values file provided in hpcc-systems/HPCC-Platform github repo:
  ```bash
    helm install mycluster ./hpcc -f helm/examples/metrics/prometheus_metrics.yml
    ```
- Enable Azure's Insights on the target AKS cluster
  - Can be done from Azure portal or via CLI
  - Detailed Azure documentation: [Enable Container insights](https://docs.microsoft.com/en-us/azure/azure-monitor/containers/container-insights-onboard)
  - On Azure portal, select target AKS cluster -> Monitoring -> Insights -> Enable
  - From command line, create log-analytics workspace [optional - default workspace otherwise]
  ```bash
    az monitor log-analytics workspace create  -g myresourcegroup -n myworkspace --query-access Enabled
    ```
  - Enable on target AKS cluster (referencing workspace resource id from previous step)
  ```bash
    az aks enable-addons -g myresourcegroup  -n myaks -a monitoring --workspace-resource-id "/subscriptions/xyz/resourcegroups/myresourcegroup/providers/microsoft.operationalinsights/workspaces/myworkspace"
    ```
 - Configure Per-pod metrics scraping
   - Apply provided [Log Analytics agent ConfigMap](https://github.com/hpcc-systems/HPCC-Platform/blob/master/helm/examples/metrics/container-azm-ms-agentconfig-prometheusperpod.yaml)  - which enables per-pod Prometheus metrics scraping
  ```bash
     kubectl apply -f <hpcc-systems/HPCC-Platform repo path>/helm/examples/metrics/container-azm-ms-agentconfig-prometheusperpod.yaml
     ```
   - Alternatively, manually copy [default ConfigMap yaml](https://raw.githubusercontent.com/microsoft/Docker-Provider/ci_prod/kubernetes/container-azm-ms-agentconfig.yaml) locally and set monitor_kubernetes_pods = true in the prometheus-data-collection-settings section. Then apply that file as above.

   - Confirm the value was applied - the following command should reflect the above value
  ```bash
     kubectl get ConfigMap  -n kube-system container-azm-ms-agentconfig --output jsonpath='{.data.prometheus-data-collection-settings}'
     ```

  - Fetch available metrics
    - From Azure portal, select target AKS cluster -> Monitoring -> Logs, in Kusto Query field, enter the following:
  ```bash
      InsightsMetrics
      | where Namespace == "prometheus"
      | extend tags=parse_json(Tags)
      | summarize count() by Name
      ```
    - Sample output:
  ```bash
      esp_requests_active	1,200
      dali_nq_requests_received	1,440
      dali_sds_requests_completed	1,440
      dali_sds_requests_pending	1,440
      dali_sds_requests_received	1,440
      dali_sds_requests_started	1,440
      esp_requests_received	1,200
      esp_soap_requests_received	1,200
  - Query specific HPCC metric
    - Replace previous query with following:
  ```bash
      InsightsMetrics
      | where Namespace == "prometheus"
      | where Name == "esp_requests_active"
      ```
    - Sample output:
  ```bash
      11/5/2021, 9:02:00.000 PM	prometheus	esp_requests_active	0	{"app":"eclservices","namespace":"default","pod_name":"eclservices-778477d679-vgpj2"}
      11/5/2021, 9:02:00.000 PM	prometheus	esp_requests_active	3	{"app":"eclservices","namespace":"default","pod_name":"eclservices-778477d679-vgpj2"}
      ```
###  ElasticSearch Support
HPCC metrics can be routed to ElasticSearch for advanced metrics processing and 
alerting. This process involves two requirements: enabling the ElasticSearch metric 
sink, and configuring the index in ElasticSearch to receive the metrics.

Since the metrics configuration is common across all HPCC components, the ElasticSearch
sink will report metrics from all components to the same index. Therefore, the
index must be configured to receive metrics from all components. 

#### Index Configuration
The sink requires the index to be created and configured fully in ElasticSearch in order for the sink to report
metrics. The sink reads configuration data from the index in order to properly report metrics. Future versions
of the sink may include the capability to create the index if it does not exist. The name of the index is passed 
to the sink as a configuration setting.
 
The following must be configured in the index in order for the sink to properly report metrics:

##### Dynamic Mapping
The index must be configured with dynamic mapping enabled. Dynamic mapping allows storing of framework metric 
values using native types. Without dynamic mapping, the ElasticSearch default mapping does not properly map 
values to unsigned 64-bit integers. 

To create an index with dynamic mapping, use the following object, or other means, when creating the index:
```json
{
  "mappings": {
    "dynamic_templates": [
      {
        "hpcc_metrics_count_suffix": {
          "match": "*_count",
          "mapping": {
            "type": "unsigned_long"
          }
        }
      },
      {
        "hpcc_metrics_gauge_suffix": {
          "match": "*_gauge",
          "mapping": {
            "type": "unsigned_long"
          }
        }
      },
      {
        "hpcc_metrics_histogram_suffix": {
          "match": "*_histogram",
          "mapping": {
            "type": "histogram"
          }
        }
      }
    ]
  }
}  
```

The object names beginning with "hpcc_metric_" represent the mappings for the three affected hpcc metrics types.
The _match_ value for each defines the expected suffix for each HPCC metric value, based on type, when metrics
are reported and indexed. The object names above are the default values used by the sink. The index can be
configured with different object names (for environment flexibility), but the object names must be provided to the
sink as additional configuration settings (defined below).

#### Enabling ElasticSearch Metrics Sink for Kubernetes
To enable reporting of metrics to ElasticSearch, add the metric configuration settings to 
the helm chart. 

The provided HPCC helm chart provides all global settings from its values.yml file to all components. 
To enable metrics reporting, either include the metrics configuration as a permanent part of 
your HPCC helm chart values.yml file, or add it as command line settings at chart installation. 
To enable the ElasticSearch sink on the command line, use the following to add the ElasticSearch 
settings:

```bash
helm install mycluster ./hpcc -f <path>/elasticsearch_metrics.yml
```
An example _yml_ file can be found in the repository at helm/examples/metrics/elasticsearch_metrics.yml.
Make a copy and modify as needed for your installation.

##### Configuration Settings
The ElasticSearch sink defines the following settings

**Host**
The host settings define the server hosting ElasticSearch to which metrics are reported. The settings are:

* domain - The domain or IP address of the ElasticSearch server. (required)
* protocol - The protocol used to connect to the ElasticSearch server. (default: https)
* port - The port number of the ElasticSearch server. (default: 9200)
* certificateFilePath - Path to the file containing the certificate used to connect to the ElasticSearch server. 
(optional)
* connectTimeout - The time in seconds to wait for a connection to be established to the (default: 5)
* readTimeout - The time in seconds to wait for a response from the server for a read operation. (default: 5)
* writeTimeout - The time in seconds to wait for a response from the server for a write operation. (default: 5)

**Authentication**

Optional child of the host configuration where authentication settings are defined. If missing, 
no authentication is used. If defined, the settings are:

* type - Required Authentication type used to connect to the ElasticSearch server. Value defines the 
remaining settings. The allowed values are:
  * basic - Basic authentication is used.
* credentialsSecret - The name of the secret containing the credentials used to authenticate to 
the ElasticSearch server. (optional, valid for Kubernetes only)
* credentialsVaultId - The vault ID containing the credentials used to authenticate to the 
ElasticSearch server. (optional, valid for Vault only)

For **basic** authentication, the following settings are required. If a secret or vault is defined, these
values are store there and are not required in the configuration file. Otherwise, they are required in the
configuration file (environment.xml).
* username - The username used to authenticate to the ElasticSearch server. 
* password - The password used to authenticate to the ElasticSearch server. When stored in the 
environment.xml file, it shall be encrypted using standard environment.xml encryption.

**Index**

The index must be created and configred in ElasticSearch before the sink will load and report
metrics. The following settings describe what must be configured in ElasticSearch.

* name - The name of the index to which metrics are reported. (required)
* countSuffixMappingName - See below. (default: hpcc_metrics_count_suffix)
* gaugeSuffixMappingName - See below. (default: hpcc_metrics_gauge_suffix)
* histogramSuffixMappingName - See below. (default: hpcc_metrics_histogram_suffix)

The _*SuffixMappingName_ values are object names in the index _dynamic_templates_ object of the
index's _mappings_ object. These MUST be configured in the index. The mapping name objects contain
a _match_ member whose value is used as the suffix for the related HPCC metric type. The format
is expected to be "*\<string\>" where "*" is part of the defined match pattern syntax defined by
ElasticSearch and "\<string\>" is the suffix appended to each HPCC metric based on type. For example,
If a _match_ value is defined as "*_count", then the sink would add "_count" to the end of each
metric when indexing measurements during a report. This ensures that each HPCC metric is stored in 
the index with the correct type. Please refer to the ElasticSearch documentation for more information 
on dynamically mapping types when indexing documents.

For convenience, if the index dynamic templates configuration settings do not include a gauge 
suffix setting, the value for the count suffix setting is used. 

Standard periodic metric sink settings are also available.

#### Enabling ElasticSearch Metrics Sink for Bare Metal
To enable reporting of metrics to ElasticSearch, add the metric configuration settings to
the environment configuration file (enviroment.xml). These settings must be added manually
since there is no support in the config manager.

Add the following to the environment.xml configuration file (note that some values may not be required): 

```xml
<Environment>
    <Software>
        <metrics name="mymetricsconfig">
            <sinks name="myelasticsink" type="elastic">
                <settings period="30" ignoreZeroMetrics="1">
          <host domain="\<domainname\>" port="\<port\>" protocol="http|https" 
              certificateFilePath="\<path to cert file\>">
            <authentication type="basic" username="\<username\>" password="\<password\>"/>
                    </host>
          <index name="\<index\>" countSuffixMappingName="\<name\>" histogramSuffixMappingName="\<name\>" gaugeSuffixMappingName="\<name\>"/> 
                <settings/>
            </sinks>
        </metrics>
    </Software>
</Environment>
```
See section above for additional settings that can be added to the ElasticSearch sink.