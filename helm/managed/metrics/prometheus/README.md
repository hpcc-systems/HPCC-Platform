## This folder contains Prometheus deployment chart and HPCC Systems preferred values

This chart describes a local Prometheus Kube Stack instance for HPCC Systems component metrics processing.
Once successfully deployed, the Prometheus Stack instance can scrape metrics from HPCC metrics services on a per-node basis.

Please see HPCC-Platform/helm/examples/metrics/README.md for details on enabling HPCC's Prometheus metrics services

### Dependencies
This chart is dependent on the prometheus-community's kube-prometheus-stack chart. Please perform helm dependancy update before deploying.

#### Dependency update
##### HELM Command
Helm provides a convenient command to automatically pull appropriate dependencies to the /charts directory:
> helm dependency update <HPCC-Systems Git clone location>/helm/managed/metrics/prometheus

##### HELM Install parameter
Otherwise, provide the "--dependency-update" argument in the helm install command
For example:
> helm install myprometheus <HPCC-Systems Git clone location>/helm/managed/metrics/prometheus --dependency-update

### Chart Values
This chart is largely based on the default prometheus-community/kube-prometheus-stack chart, to see HPCC manipulated values:

> helm show values helm/managed/metrics/prometheus

To see prometheus-community/kube-prometheus-stack available values:

> helm show values prometheus-community/kube-prometheus-stack

### Prometheus Kube Stack
Some of the included compentst are:
- alertmanager
- grafana
- nodeExporter
- prometheus
- prometheus-node-exporter
- prometheusOperator

### Service Accesibility
The Prometheus and Grafana services are accessible internally within the cluster using ClusterIP. In order to make them available externally, edit them to either NodePort or Loadbalancer.
For Prometheus, edit the service by name:

>kubectl edit svc myprometheus-kube-promethe-prometheus

And change the type 'ClusterIP' value accordingly, and save/close the editor.

### Resources
- [Prometheus kube-prometheus-stack github repo and documentation](https://github.com/prometheus-community/helm-charts/blob/main/charts/kube-prometheus-stack/README.md)
- [Third party blog on Prometheus use](https://www.scalyr.com/blog/prometheus-tutorial-detailed-guide-to-getting-started/)
- [Grafana documentation](https://grafana.com/docs/grafana/latest/getting-started/)