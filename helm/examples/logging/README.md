# Containerized HPCC Systems Logging

## HPCC Systems logging background

Containerized HPCC Systems components provide informative application level logs for the purpose of debugging problems, auditing actions, and progress monitoring. Following the most widely accepted containerized methodologies, those logs are written to the standard error (stderr) stream. At the node level, the contents of the standard error and out streams are redirected to a target location by a container engine. In a Kubernetes environment, the Docker container engine redirects the streams to a logging driver, which Kubernetes configures to write to a file in JSON format. The logs are exposed by Kubernetes via the aptly named "logs" command. 

    For example:
    >kubectl logs myesp-6476c6659b-vqckq
    >0000CF0F PRG INF 2020-05-12 17:10:34.910     1 10690 "HTTP First Line: GET / HTTP/1.1"
    >0000CF10 PRG INF 2020-05-12 17:10:34.911     1 10690 "GET /, from 10.240.0.4"
    >0000CF11 PRG INF 2020-05-12 17:10:34.911     1 10690 “TxSummary[activeReqs=22; rcv=5ms;total=6ms;]"
 
It is important to understand that these logs are ephemeral in nature, and may be lost if the pod is evicted, the container crashes, the node dies, etc. Also, due to the nature of containerized solutions, related logs are likely to originate from various locations and might need to be collected and processed. It is highly recommended to develop a retention and processing strategy based on your needs.

Many tools are available to help create an appropriate solution based on either a do-it-yourself approach, or managed features available from cloud providers.

For the simplest of environments, it might be acceptable to rely on the standard Kubernetes process which forwards all contents of stdout/stderr to file. However, as the complexity of the cluster grows or the importance of retaining the logs' content grows, a cluster-level logging architecture should be employed.

Cluster-level logging for the containerized HPCC Systems cluster can be accomplished by including a logging agent on each node. The task of each of agent is to expose the logs or push them to a log processing backend. Logging agents are generally not provided out of the box, but there are several available such as Elasticsearch and Stackdriver Logging. Various cloud providers offer built-in solutions which automatically harvest all stdout/err streams and provide dynamic storage and powerful analytic tools, and the ability to create custom alerts based on log data.

_Specific logging tool examples can be found in the child folders_

## HPCC Systems log access

Out of the box, HPCC component logs are handled by K8s drivers, and exposed via the kubectl logs pod command.
Access to logs processed by other processes such as Elastic Stack, Azure log analytics, etc. is provided by the chosen log processor solution.
However, HPCC provides a standard interface into the logs. To enable this feature, information about the chosen log processor solution must be provided in
the global.logAccess portion of the Helm chart.
For example, if an elastic stack is deployed under the default namespace, and it contains hpcc logs in indexes prefixed 'hpcc-logs' the following log access configuration would allow log access through HPCC:

    
    global:
      logAccess:
        name: "LocalElasticStack"
        type: "elasticstack"
        connection:
            protocol: "http"
            host: "elasticsearch-master.default.svc.cluster.local"
            port: 9200
        logMaps:
          - type: "global"                             #These settings apply to all log mappings
            storeName: "hpcc-logs*"                    #Logs are expected to be housed in ES indexes prefixed 'filebeat-'
            searchColumn: "message"                    #The 'message' field is to be targeted for wilcard text searches
            timeStampColumn: "@timestamp"              #The '@timestamp' field contains time log entry timestamp
          - type: "workunits"                          #Search by workunits specific log mapping
            storeName: "hpcc-logs*"                    # Only needed if differs from global.storeName
            searchColumn: "hpcc.log.jobid"             # Field containing WU information
          - type: "components"                         #Search by components specific log mapping
            searchColumn: "kubernetes.container.name"  # Field containing container information
          - type: "audience"                           #Search by audience specific log mapping
            searchColumn: "hpcc.log.audience"          # Field containing audience information
          - type: "class"                              #Search by log class specific log mapping
            searchColumn: "hpcc.log.class"             # Field containing log class information
          - type: "instance"                           #Search by log source instance specific mapping
            searchColumn: "kubernetes.pod.name"        # Field containing source instance information
          - type: "host"                               #Search by log source host specific mapping
            searchColumn: "kubernetes.node.hostname"   # Field containing source host information

This configuration coincides with the elastic4hpcclogs managed solution found in HPCC-Systems/helm/managed/logging/elastic, and can be provided as part of an HPCC Systems deployment as follows:
    
    >helm install HPCC-Systems/helm/hpcc -f HPCC-Platform/helm/managed/logging/elastic/elastic4hpcclogs-hpcc-logaccess.yaml


## HPCC Systems application-level logging details

As mentioned earlier, the HPCC Systems logs provide a wealth of information which can be used for benchmarking, auditing, debugging, monitoring, etc. The type of information provided in the logs and its format is trivially controlled via standard Helm configuration.

By default, the component logs are not filtered, are reported in a space delimited values format, and contain the following columns:
    
    MessageID TargetAudience LogEntryClass JobID DateStamp TimeStamp ProcessId ThreadID QuotedLogMessage

The logs can be filtered by TargetAudience, Category or Detail Level, and the output columns can be configured. The logs are reported in 'table' (space delimited values) format by default, and can be re-configured to XML or JSON format. Logging configuration settings can be applied at the global, or component level.

### Target Audience Filtering

The availble target audiences include operator(OPR), user(USR), programmer(PRO), audit(ADT), or all. The filter is controlled by the
`<section>`.logging.audiences value. The string value is comprised of 3 letter codes delimited by the aggregation operator (+) or the removal operator (-).
    
    For example, all component log output to include Programmer and User messages only:
    helm install myhpcc ./hpcc --set global.logging.audiences="PRO+USR"
    
### Target Category Filtering

The available target categories include disaster(DIS), error(ERR), warning(WRN),information(INF),progress(PRO),metrics(MET). The category (or class) filter is controlled by the `<section>`.logging.classes value, comprised of 3 letter codes delimited by the aggregation operator (+) or the removal operator (-).
    
    For example, the mydali instance's log output to include all classes except for progress:
    helm install myhpcc ./hpcc --set dali[0].logging.classes="ALL-PRO" --set dali[0].name="mydali"

### Log Detail Level Configuration
Log output verbosity can be adjusted from "critical messages only" (1) up to "report all messages" (100). The default log level is rather high (80) and should be adjusted accordingly.

    For example, verbosity should be medium for all components:
    helm install myhpcc ./hpcc --set global.logging.detail="50"
    
### Log Data Column Configuration

The available log data columns include messageid(MID), audience(AUD), class(CLS), date(DAT), time(TIM), millitime(MLT), microtime(MCT), nanotime(NNT), processid(PID), threadid(TID), node(NOD), job(JOB), use(USE), session(SES), code(COD), component(COM), quotedmessage(QUO), prefix(PFX), all(ALL), and standard(STD). The log data columns (or fields) configuration is controlled by the `<section>`.logging.fields value, comprised of 3 letter codes delimited by the aggregation operator (+) or the removal operator (-).
    
    For example, all component log output should include the standard columns except the job ID column:
    helm install myhpcc ./hpcc --set global.logging.fields="STD-JOB"
    
Adjustment of per-component logging values can require assertion of multiple component specific values, which can be inconvinient to do via the --set command line parameter. In these cases, a custom values file could be used to set all required fields.

    For example, the ESP component instance 'eclwatch' should output minimal log:
    helm install myhpcc ./hpcc --set -f ./examples/logging/esp-eclwatch-low-logging-values.yaml

### Asychronous logging configuration

By default log entries will be created and logged asynchronously, so as not to block the client that is logging.
Log entries will be held in a queue and output on a background thread.
This queue has a maximum depth, once hit, the client will block waiting for capacity.
Alternatively, the behaviour can be be configured such that when this limit is hit, logging entries are dropped and lost to avoid any potential blocking.

NB: normally it is expected that the logging stack will keep up and the default queue limit will be sufficient to avoid any blocking.

The defaults can be configured by setting `<section>`.logging.queueLen and/or `<section>`.logging.queueDrop.
Setting `<section>`.logging.queueLen to 0, will disabled asynchronous logging, i.e. each log will block until completed.
Setting `<section>`.logging.queueDrop to a non-zero (N) value will cause N logging entries from the queue to be discarded if the queueLen is reached.

### Log data format configuration

By default, HPCC Systems component logs are reported in the format known as 'table' which consists of space delimited value columns.

    For example:
    0000CF0F PRG INF 2020-05-12 17:10:34.910     1 10690 "HTTP First Line: GET / HTTP/1.1"
    0000CF10 PRG INF 2020-05-12 17:10:34.911     1 10690 "GET /, from 10.240.0.4"
    0000CF11 PRG INF 2020-05-12 17:10:34.911     1 10690 “TxSummary[activeReqs=22; rcv=5ms;total=6ms;]"

The log format can be re-configured to xml or json. To do so, specify the desired format in the logging.format section:

Sample configuration for json format:

      logging:
        format: json

Sample json formated log output:

    { "MSG": "HTTP connection from 1.1.1.1:52392 on persistent socket", "MID": "593", "AUD": "PRG", "CLS": "INF", "DATE": "2023-02-23", "TIME": "17:42:42.808", "PID": "8", "TID": "117", "JOBID": "UNK" }
    { "MSG": "HTTP First Line: POST /WsWorkunits/WUQuery.json HTTP/1.1", "MID": "594", "AUD": "PRG", "CLS": "INF", "DATE": "2023-02-23", "TIME": "17:42:42.809", "PID": "8", "TID": "225", "JOBID": "UNK" }
    { "MSG": "TxSummary[activeReqs=1;auth=NA;contLen=31;rcv=1;handleHttp=9;req=POST wsworkunits.WUQUERY v1.95;total=9;]", "MID": "592", "AUD": "PRG", "CLS": "INF", "DATE": "2023-02-23", "TIME": "17:42:39.277", "PID": "8", "TID": "218", "JOBID": "UNK" }

Sample configuration for xml format:

      logging:
        format: xml

Sample xml formatted log output:

    <msg MessageID="150" Audience="Programmer" Class="Warning"
     date="2023-02-23" time="17:47:33.516" PID="8" TID="114"
     JobID="UNK"
     text="TxSummary entry with name 'rcv' and suffix 'ms'; suffix is ignored" />
    <msg MessageID="151" Audience="Programmer" Class="Information"
     date="2023-02-23" time="17:47:33.516" PID="8" TID="114"
     JobID="UNK"
     text="POST /WsWorkunits/WUDetails.json, from 1.1.1.41" />
    <msg MessageID="152" Audience="User" Class="Progress"
     date="2023-02-23" time="17:47:33.517" PID="8" TID="114"
     JobID="UNK"
     text="WUDetails: W20230223-174728" />