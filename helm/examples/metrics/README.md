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

```code
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
    ```code
    > helm repo add hpcc https://hpcc-systems.github.io/helm-chart/
    ```
- Deploy prometheus4hpccmetrics
    ```code
    > helm install mypromstack hpcc/prometheus4hpccmetrics
     NAME: mypromstack
     LAST DEPLOYED: Fri Nov  5 10:44:17 2021
     NAMESPACE: default
     STATUS: deployed
     REVISION: 1
    ```
- Confirm Prometheus Stack pods are in ready state
    ```code
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
    ```code
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
      ```code
      Endpoint	State	Labels	Last Scrape	Scrape Duration	Error
      http://<removed>:8767/metrics	UP	containername="eclwatch"job="hpcc-per-pod-metrics"	8m 47s ago	1.048ms
      ```
- Confirm HPCC metrics are scraped from above services
   - On the 'Graph' section of the dashboard, search for known HPCC metrics such as 'esp_requests_active'
      ```code
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

```code
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
    ```code
    helm install mycluster ./hpcc -f helm/examples/metrics/prometheus_metrics.yml
    ```
- Enable Azure's Insights on the target AKS cluster
  - Can be done from Azure portal or via CLI
  - Detailed Azure documentation: [Enable Container insights](https://docs.microsoft.com/en-us/azure/azure-monitor/containers/container-insights-onboard)
  - On Azure portal, select target AKS cluster -> Monitoring -> Insights -> Enable
  - From command line, create log-analytics workspace [optional - default workspace otherwise]
    ```code
    az monitor log-analytics workspace create  -g myresourcegroup -n myworkspace --query-access Enabled
    ```
  - Enable on target AKS cluster (referencing workspace resource id from previous step)
    ```code
    az aks enable-addons -g myresourcegroup  -n myaks -a monitoring --workspace-resource-id "/subscriptions/xyz/resourcegroups/myresourcegroup/providers/microsoft.operationalinsights/workspaces/myworkspace"
    ```
 - Configure Per-pod metrics scraping
   - Apply provided [Log Analytics agent ConfigMap](https://github.com/hpcc-systems/HPCC-Platform/blob/master/helm/examples/metrics/container-azm-ms-agentconfig-prometheusperpod.yaml)  - which enables per-pod Prometheus metrics scraping
     ```code
     kubectl apply -f <hpcc-systems/HPCC-Platform repo path>/helm/examples/metrics/container-azm-ms-agentconfig-prometheusperpod.yaml
     ```
   - Alternatively, manually copy [default ConfigMap yaml](https://raw.githubusercontent.com/microsoft/Docker-Provider/ci_prod/kubernetes/container-azm-ms-agentconfig.yaml) locally and set monitor_kubernetes_pods = true in the prometheus-data-collection-settings section. Then apply that file as above.

   - Confirm the value was applied - the following command should reflect the above value
     ```code
     kubectl get ConfigMap  -n kube-system container-azm-ms-agentconfig --output jsonpath='{.data.prometheus-data-collection-settings}'
     ```

  - Fetch available metrics
    - From Azure portal, select target AKS cluster -> Monitoring -> Logs, in Kusto Query field, enter the following:
      ```code
      InsightsMetrics
      | where Namespace == "prometheus"
      | extend tags=parse_json(Tags)
      | summarize count() by Name
      ```
    - Sample output:
      ```code
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
      ```code
      InsightsMetrics
      | where Namespace == "prometheus"
      | where Name == "esp_requests_active"
      ```
    - Sample output:
      ```code
      11/5/2021, 9:02:00.000 PM	prometheus	esp_requests_active	0	{"app":"eclservices","namespace":"default","pod_name":"eclservices-778477d679-vgpj2"}
      11/5/2021, 9:02:00.000 PM	prometheus	esp_requests_active	3	{"app":"eclservices","namespace":"default","pod_name":"eclservices-778477d679-vgpj2"}
      ```
