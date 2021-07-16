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
