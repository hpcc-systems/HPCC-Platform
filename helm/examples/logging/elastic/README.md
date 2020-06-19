# HPCC Log Processing via Elastic Stack

Setting up a base Elastic Stack cluster to process HPCC Systems component logs is straightforward. Elastic provides Helm charts to deploy each of their components, so we'll add the Elastic helm-charts repository locally:

	> helm repo add elastic https://helm.elastic.co

We'll install the Filebeat component (log agent) , and ElasticSearch (log store and indexer). By default, Filebeat will forward the log entries to the ElasticSearch default endpoint:

    > helm install filebeat elastic/filebeat
    > helm install elasticsearch elastic/elasticsearch

Finally, the Kibana component can also be installed to be used as a front end, which allows log index management, log querying, and visualization:

    > helm install kibana elastic/kibana

Each of the Elastic components should be configured appropriately based on the cluster needs, detailed documentation can be found on the elastic GitHub page https://github.com/elastic/helm-charts/

Inspecting the Elastic pods and services, expect a Filebeat pod for each of the nodes in your cluster, a configurable number of ElasticSearch pods, and a service for both Kibana and ElasticSearch. 

Of utmost importance are the persistent volumes created by ElasticSearch on which the log indexes are stored. Review the ElasticSearch helm-charts GitHub page for details on all available configuration options.

Port forwarding might be required to expose the Kibana interface

    > kubectl port-forward service/kibana-kibana 5601

Once all the components are working successfully, the logs will be written to a filebeat prefixed index on ElasticSearch, and it can be managed from the Kibana interface. Some index content retention policy rules can be configured here.

The default filebeat configuration aggregates several Kubernetes metadata fields to each log entry forwarded to ElasticSearch. The Kubernetes fields can be used to identify the HPCC component responsible for each entry, and the source pod and or node on which the event was reported. 

<kibana discovery page screenshot>
  
Kibana discovery page showing several HPCC Systems component log entries. All other log entries not created by HPCC components are filtered out by the filter at the top left corner. The Kubernetes container name, node name, and pod name accompany the “message” field which contains the actual log entry.
