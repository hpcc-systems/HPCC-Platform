## This folder contains lightweight Elastic Stack deployment chart and HPCC Systems preferred values

<table>
  <thead>
    <tr>
      <td align="left">
        :zap: <b>Note:</b> Elastic Stack components prior to 7.16.0 have been reported to be affected by the high-severity vulnerability (CVE-2021-44228) impacting multiple versions of the Apache Log4j 2 utility
      </td>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td>
        <ul>
          <li>Users of elastic4hpcclogs are strongly encouraged to update to chart version 1.2.0 which references Elastic Stack 7.16.1</li>
          <li>Learn more about Elastic's response to the vulnerability: https://discuss.elastic.co/t/apache-log4j2-remote-code-execution-rce-vulnerability-cve-2021-44228-esa-2021-31/291476</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <ul>
          <li>elastic4hpcclogs chart version 1.2.2 references Elastic Stack 7.16.3 (Log4j 2.17.1) which "By default, Elasticsearch and Logstash have no known vulnerabilities to CVE-2021-44832."</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <ul>
          <li>elastic4hpcclogs chart version 1.2.1 references Elastic Stack 7.16.2 (Log4j 2.17.0) which reportedly fully mitigates CVE-2021-44228 and should avoid false positives in vulnerability scanners.</li>
          <li>Learn more about Elastic's 7.16.2 release and their response to the vulnerability: https://discuss.elastic.co/t/apache-log4j2-remote-code-execution-rce-vulnerability-cve-2021-44228-esa-2021-31/291476</li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

This chart describes a local, minimal Elastic Stack instance for HPCC Systems component log processing.
Once successfully deployed, HPCC component logs produced within the same namespace should be automatically indexed
on the Elastic Search end-point. Users can query those logs by issuing Elastic Search RESTful API queries, or via
the Kibana UI (after creating a simple index pattern).

Out of the box, the Filebeat forwards the HPCC component logs to a generically named index: 'hpcc-logs-'<DateStamp> and specifically into a field labeled 'message'. It also aggregates k8s, Docker, and system metadata to
help the user query the log entries of their interest.

A Kibana index pattern is created automatically based on the default filebeat index layout.

### Dependencies
This chart is dependent on the Elastic Stack Helm charts for ElasticSearch, Filebeats and Kibana.

#### Dependency update
##### HELM Command
Helm provides a convenient command to automatically pull appropriate dependencies to the /charts directory:
> helm dependency update <HPCC-Systems Git clone location>/helm/managed/logging/elastic/

##### HELM Install parameter
Otherwise, provide the "--dependency-update" argument in the helm install command
For example:
> helm install myelastic <HPCC-Systems Git clone location>/helm/managed/logging/elastic/ --dependency-update

### Log Query Improvements
User are encouraged to make use of Elsatic Search "Ingest Pipelines" to improve log query performance and tailor the structure of the log record to meet their needs. The pipelines can be applied to specific indices via Elastic Search API, or via Kibana's UI.

> further reading here: https://www.elastic.co/blog/structuring-elasticsearch-data-with-grok-on-ingest-for-faster-analytics

The following example creates a pipeline named 'hpccpipeline' via the Elastic Search API. Alternatively, the processors can be added via Kibana's "Ingest Node Pipelines".

```JSON
PUT _ingest/pipeline/hpccpipeline
{
    "description" : "Parses and structures HPCC Systems component log entries",
    "processors" : [
      {
        "grok" : {
          "field" : "message",
          "patterns" : [
            """%{BASE16NUM:hpcc.log.sequence}\s+(%{HPCC_LOG_AUDIENCE:hpcc.log.audience})\s+%{HPCC_LOG_CLASS:hpcc.log.class}\s+%{TIMESTAMP_ISO8601:hpcc.log.timestamp}\s+%{POSINT:hpcc.log.procid}\s+%{POSINT:hpcc.log.threadid}\s+%{HPCC_LOG_WUID:hpcc.log.jobid}\s+%{QUOTEDSTRING:hpcc.log.message}""",
            """%{BASE16NUM:hpcc.log.sequence}\s+%{HPCC_LOG_AUDIENCE:hpcc.log.audience}\s+%{HPCC_LOG_CLASS:hpcc.log.class}\s%{TIMESTAMP_ISO8601:hpcc.log.timestamp}\s+%{POSINT:hpcc.log.procid}\s+%{POSINT:hpcc.log.threadid}\s+%{HPCC_LOG_WUID:hpcc.log.jobid}\s+%{GREEDYDATA:hpcc.log.message}"""
          ],
          "pattern_definitions" : {
            "HPCC_LOG_WUID" : "([A-Z][0-9]{8}-[0-9]{6})|(UNK)",
            "HPCC_LOG_CLASS" : "DIS|ERR|WRN|INF|PRO|MET|UNK",
            "HPCC_LOG_AUDIENCE" : "OPR|USR|PRG|AUD|UNK"
          }
        }
      },
      {
        "date": {
        "field": "hpcc.log.timestamp",
        "formats": [
          "yyyy-MM-dd' 'HH:mm:ss.SSS"
          ]
        }
      }
    ],
    "on_failure" : [
      {
        "set" : {
          "field" : "hpccpipeline.error.message",
          "value" : "{{ _ingest.on_failure_message }}"
        }
      }
    ]
}
```

Once you've verified your pipeline was created successfully, the target Elastic Search index should be associated with this pipeline.
This is done by adding the option "filebeat.pipeline.filebeatConfig.filebeat.yml.output.elasticsearch.pipeline:'hpccpipeline'"

For convenience a values file with that option is provided here: [HPCC-Platform github repo](https://raw.githubusercontent.com/hpcc-systems/HPCC-Platform/master/helm/managed/logging/elastic/filebeat-filebeatConfig-hpccpipeline.yaml)
Which allows users to deploy elastic4hpcclogs with the hpccpipeline setting with out the need to edit the chart.

For example:
```bash
helm install myelk hpcc/elastic4hpcclogs -f https://raw.githubusercontent.com/hpcc-systems/HPCC-Platform/master/helm/managed/logging/elastic/filebeat-filebeatConfig-hpccpipeline.yaml
```

Or the user can choose to upgrade a deployed elastic4hpcclogs

for example:
```bash
helm upgrade -f https://raw.githubusercontent.com/hpcc-systems/HPCC-Platform/master/helm/managed/logging/elastic/filebeat-filebeatConfig-hpccpipeline.yaml myelk hpcc/elastic4hpcclogs
```

Warning: if the named pipeline is not found, the filebeat component might not function correctly.

The targeted index should now contain a series of "hpcc.log.*" fields which can be used to query and extract specific HPCC component log data.

For example, search all HPCC component reported errors:

```json
{
  "query": {
    "match_phrase": {
      "hpcc.log.class": "ERR"
    }
  }
}
```

### Kibana Service Access

The Managed Kibana service is declared as type LoadBalancer for convenience to the user. However it is imperative to control external access to the service.
The service is defaulted to "internal load balancer" on Azure, the user is encouraged to set similar values on the target cloud provider. See the kibana.service.annotations section:

```yaml
kibana:
  enabled: true
  description: "HPCC Managed Kibana"
  ##See https://github.com/elastic/helm-charts/blob/master/kibana/values.yaml for all available options
  labels: {"managedby" : "HPCC"}
  stedkey: value
  service:
    type: "LoadBalancer"
    annotations:
      # This annotation delcares the Azure load balancer for the service as internal rather than internet-visible
      service.beta.kubernetes.io/azure-load-balancer-internal: "true"

      # Enable appropriate annotation for target cloud provider to ensure Kibana access is internal
      #
      #service.beta.kubernetes.io/cce-load-balancer-internal-vpc: "true"
      #cloud.google.com/load-balancer-type: "Internal"
      #service.beta.kubernetes.io/aws-load-balancer-internal: "true"
      #service.beta.kubernetes.io/openstack-internal-load-balancer: "true"
```


### Cleanup
The Elastic Search chart will declare a PVC which is used to persist data related to its indexes, and thus, the HPCC Component logs. The PVCs by nature can outlive the HPCC and Elastic deployments, it is up to the user to manage the PVC appropriately, which includes deleting the PVC when they are no longer needed.
