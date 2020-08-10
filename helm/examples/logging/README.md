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

## HPCC Systems application-level logging details

As mentioned earlier, the HPCC Systems logs provide a wealth of information which can be used for benchmarking, auditing, debugging, monitoring, etc. The type of information provided in the logs and its format is trivially controlled via standard Helm configuration.

By default, the component logs are not filtered, and contain the following columns:
    
    MessageID TargetAudience DateStamp TimeStamp ProcessId ThreadID QuotedLogMessage

The logs can be filtered by TargetAudience, Category or Detail Level, and the output columns can be configured. Logging configuration settings can be applied at the global, or component level.

### Target Audience Filtering

The availble target audiences include operator(OPR), user(USR), programmer(PRO), audit(ADT), or all. The filter is controlled by the
`<section>`.logging.audiences value. The string value is comprised of 3 letter codes delimited by the aggregation operator (+) or the removal operator (-).
    
    For example, all component log output to include Programmer and User messages only:
    helm install myhpcc ./hpcc --set global.logging.audiences="PRO+USR"
    
### Target Category Filtering

The available target categories include disaster(DIS), error(ERR), warning(WRN),information(INF),progress(PRO). The category (or class) filter is controlled by the `<section>`.logging.classes value, comprised of 3 letter codes delimited by the aggregation operator (+) or the removal operator (-).
    
    For example, the mydali instance's log output to include all classes except for progress:
    helm install myhpcc ./hpcc --set dali[0].logging.classes="ALL-PRO" --set dali[0].name="mydali"

### Log Detail Level Configuration
Log output verbosity can be adjusted from "critical messages only" (1) up to "report all messages" (100). By default, the log level is set highest (100). 

    For example, verbosity should be medium for all components:
    helm install myhpcc ./hpcc --set global.logging.detail="50"
    
### Log Data Column Configuration

The available log data columns include messageid(MID), audience(AUD), class(CLS), date(DAT), time(TIM), millitime(MLT), microtime(MCT), nanotime(NNT), processid(PID), threadid(TID), node(NOD), job(JOB), use(USE), session(SES), code(COD), component(COM), quotedmessage(QUO), prefix(PFX), all(ALL), and standard(STD). The log data columns (or fields) configuration is controlled by the `<section>`.logging.fields value, comprised of 3 letter codes delimited by the aggregation operator (+) or the removal operator (-).
    
    For example, all component log output should include the standard columns and the log message class name:
    helm install myhpcc ./hpcc --set global.logging.fields="STD+CLS"
    
Adjustment of per-component logging values can require assertion of multiple component specific values, which can be inconvinient to do via the --set command line parameter. In these cases, a custom values file could be used to set all required fields.

    For example, the ESP component instance 'eclwatch' should output minimal log:
    helm install myhpcc ./hpcc --set -f ./examples/logging/esp-eclwatch-low-logging-values.yaml
 
