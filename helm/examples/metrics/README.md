# HPCC Metrics
HPCC components collect metrics describing overall component health and state. In order to use component metrics for
activities such as scaling and alerting, the collected metrics must be reported to a collection system such as
Prometheus or Elasticsearch.

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
which reports metrics. It also annotates all HPCC pods to be discoverable by Prometheus servers and provides the connectivity inforation required, such as
the port to liston on, and the url path on which to serve Prometheus formatted metrics. Autodiscovery annotations can be disabled via metrics.sinks[type=prometheus].settings.autodiscovery

####Prometheus Metric Service Discovery
On the Prometheus server side, an "additionalScrapeConfigs" entry is necessary to link the HPCC metrics services to Prometheus

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

 More information on the "additionalScrapeConfigs" options provided by Prometheus here: https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config
 3rd Party write up on the relabel_configs used in the additionalscrapeconfigs here: https://gist.github.com/reachlin/a98b90afcbff4604c90c183a0169474f