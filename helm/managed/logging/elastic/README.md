## This folder contains lightweight Elastic Stack deployment chart and HPCC Systems preferred values

This chart describes a local, minimal Elastic Stack instance for HPCC Systems component log processing.
Once successfully deployed, HPCC component logs produced within the same namespace should be automatically index
on the Elastic Search end-point. Users can query those logs by issuing Elastic Search RESTful API queries, or via
the Kibana UI (after creating a simple index pattern).

Out of the box, the Filebeat forwards the HPCC component logs to a generically named index: 'fielbeat'-<FB_VER>-<DateStamp> and specifically into a field labeled 'message'. It also aggregates k8s, Docker, and system metadata to
help the user query the log entries of their interest.

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

##### Log Query Improvements
User are encouraged to make use of Elsatic Search "Ingest Pipelines" to improve log query performance and tailor the structure of the log record to meet their needs. The pipelines can be applied to specific indices via Elastic Search API, or via Kibana's UI.

> further reading here: https://www.elastic.co/blog/structuring-elasticsearch-data-with-grok-on-ingest-for-faster-analytics

The following example creates a pipeline named 'hpccpipeline' via the Elastic Search API. Alternatively, the processors can be added via Kibana's "Ingest Node Pipelines".

```JSON
PUT _ingest/pipeline/hpccpipeline
{
    "processors" : [
      {
        "grok" : {
          "field" : "message",
          "patterns" : [
            """%{BASE16NUM:hpcc.log.sequence}\s+(%{WORD:hpcc.log.audience})\s+%{WORD:hpcc.log.class}\s+%{TIMESTAMP_ISO8601:hpcc.log.timestamp}\s+%{POSINT:hpcc.log.procid\s+%{POSINT:hpcc.log.threadid}\s+%{WORD:hpcc.log.jobid}\s+%{QUOTEDSTRING:hpcc.log.message}""",
            """%{BASE16NUM:hpcc.log.sequence}\s+%{WORD:hpcc.log.audience}\s+%{WORD:hpcc.log.class}\s%{TIMESTAMP_ISO8601:hpcc.log.timestamp}\s+%{POSINT:hpcc.log.procid}\s+%{POSINT:hpcc.log.threadid}\s+%{WORD:hpcc.log.jobid}\s+%{GREEDYDATA:hpcc.log.message}"""
          ]
        }
      }
    ],
    "on_failure" : [
      {
        "set" : {
          "field" : "error.message",
          "value" : "{{ _ingest.on_failure_message }}"
        }
      }
    ]
}
```

Once you've verified your request was applied successfully, the target Elastic Search index should be associated with this pipeline. In the managed Elastic Stack values.yml file, set "filebeat.pipeline.filebeatConfig.filebeat.yml.output.elasticsearch.pipeline" to 'hpccpipeline':

```yaml
filebeat:
  description: "HPCC Managed filebeat"
  filebeatConfig:
    filebeat.yml: |
      filebeat.inputs:
      - type: container
        paths:
          - /var/log/containers/esdl-sandbox-*.log
          - /var/log/containers/eclwatch-*.log
          - /var/log/containers/mydali-*.log
          - /var/log/containers/eclqueries-*.log
          - /var/log/containers/sql2ecl-*.log
          - /var/log/containers/eclservices-*.log
          - /var/log/containers/dfuserver-*.log
          - /var/log/containers/eclscheduler-*.log
          - /var/log/containers/hthor-*.log
          - /var/log/containers/myeclccserver-*.log
          - /var/log/containers/roxie-*.log
          - /var/log/containers/sasha-*.log
          - /var/log/containers/thor-*.log
        processors:
        - add_kubernetes_metadata:
            host: ${NODE_NAME}
            matchers:
            - logs_path:
                logs_path: "/var/log/containers/"
      output.elasticsearch:
        host: '${NODE_NAME}'
        hosts: '${ELASTICSEARCH_HOSTS:elasticsearch-master:9200}'
        pipeline: 'hpccpipeline'
```

Warning: if the named pipeline is not found, the filebeat component might not deploy successfully.

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


##### Cleanup
The Elastic Search chart will declare a PVC which is used to persist data related to its indexes, and thus, the HPCC Component logs. The PVCs by nature can outlive the HPCC and Elastic deployments, it is up to the user to manage the PVC appropriately, which includes deleting the PVC when they are no longer needed.
