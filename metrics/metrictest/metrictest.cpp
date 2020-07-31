

#include <cstdio>

#include "Metrics.hpp"
#include "MetricsRegistry.hpp"
#include "MetricSet.hpp"
//#include "FileSink.hpp"
//#include "CountMetric.hpp"
//#include "RateMetric.hpp"
//#include "GaugeMetric.hpp"
#include "triggers/periodic/PeriodicTrigger.hpp"
//#include "PeriodicMetricsCollector.hpp"
#include "MetricsReporter.hpp"
#include <thread>
#include <chrono>


using namespace hpccMetrics;

MetricsRegistry mr;

void processThread(int, unsigned);
std::shared_ptr<CountMetric> pCountableMetric;
std::shared_ptr<GaugeMetric<uint32_t>> pQueueSizeMetric;
//std::shared_ptr<QueueLatencyMetric> pQueueLatencyMetric;
std::shared_ptr<RateMetric> pRateMetric;


/*

 Some notes on configuration

Global level:

 Defines the storage sinks for the platform and how reporting is done
 This information should not be repeated or changed at the component level.
 It can, however, be expanded by adding an addition sink

  metrics:
    name: config_name
    sinks:                  # array of sink definitions (usually one)
    - name: sinkname        # used to reference the sink in the config (probably best to use a global type name such as 'cluster')
      type: sinktype        # used to load the library for the sink (component may reference sink by type or name)
      config:               # sink specific configuration data
        key1: data1         # key/value pairs that are passed to the sink during config
        key2: data2
        Key3:
          key3.1: data3.1
          key3.2: data3.2
        ipaddr: {{sys.io.helm/kube.xxx}}

    report_trigger:
      type: trigger_type      # type of trigger (may be an external lib)
      config:                 # configuration data for the trigger passed in during config
        key1: data1
        key2: data2

    report_config:              # this report config is applied to all components
      - sink: sinkname          # name of the sink defined in the sinks section above
        optional: bool          # the report is optional if the component can't configure it
        metric_sets:            # defines the metric sets that are part of the report
          sets: [ <set names> ] # list of set names
          exact: bool           # true if all sets must be present, false if a subset is allowed
                                #   if option is true, its an error if non sets found
                                #     or exact is true and not all sets found

Component level
 component may define any of the same items allowed at the global level, but should not override any, only add
 (not sure about that statement yet)

  metrics:                            # may define a sink if necessary, cannot define a trigger
    prefix: {{name}}                  # define domain prefix for all sets (probably a substituted variable reference, could include cluster and component information

    # absent any report_config section, all metric sets are reported to all sinks
    report_config:                    # same as global level, but optional not supported
      - sink:
        name: sinkname                # reference sink by name or type
        type: sinktype                # one of name or type is required
        metric_sets:                  # same as global, but exact is not supported
          sets: [ set1, set2, ... ]
      - sink: sinkname2
        metric_sets:
          sets: [ set1, set2, ... ]


 A configuration class a component can instantiate, then pass in the global and component config

 configuration class
 - reads the global and component configs and creates an internal template of the config
 - maybe creates the metric sets that are referenced
 - component creates the required metrics
   - calls method in configclass to add metric to a named set  pConfig->addMetricToSet(pmetric, setname)
 - Once done creating and adding call pConfig->startCollection();
   creates the metric sets, sinks, report trigger
   creates metric report config
   starts the whole process

 - When done, pConfig->stopCollection() to clean up and close down










*/


int main(int argc, char *argv[])
{

    MetricsReportConfig reportConfig;

    //
    // Create a metric set for request type metrics
    std::vector<std::shared_ptr<IMetric>> metrics;
    pCountableMetric     = std::make_shared<CountMetric>("requests");
    metrics.emplace_back(pCountableMetric);

    pRateMetric = std::make_shared<RateMetric>("rate");
    metrics.emplace_back(pRateMetric);

    auto pRequestMetricSet = std::make_shared<MetricSet>("set", "myprefix.", metrics);
    mr.add(pCountableMetric);  // demo use of the registry (optional)

    //
    // create a metric set for queues
    metrics.clear();
    pQueueSizeMetric = std::make_shared<GaugeMetric<uint32_t>>("queuesize");
    pQueueSizeMetric->setType(MetricType::INTEGER);
    metrics.emplace_back(pQueueSizeMetric);
    auto pQueueMetricSet = std::make_shared<MetricSet>("set2", "myprefix2", metrics);

    //
    // Create a file sink for saving metric values

    //
    // Create a collector
//    MetricsReporter *pReporter = collector.getReporter();
    //collector.addMetricSet(pRequestMetricSet);
    //collector.addMetricSet(pQueueMetricSet);
    std::map<std::string, std::string> parms = { {"filename", "/home/ken/metricsreport.txt"}};
    auto pSink = MetricSink::getSinkFromLib("filesink", nullptr, "es", parms);
    reportConfig.addReportConfig(pSink, pRequestMetricSet);
    reportConfig.addReportConfig(pSink, pQueueMetricSet);

    std::map<std::string, std::string> triggerParms;
    triggerParms["period"] = "10";
    IMetricsReportTrigger *pTrigger = MetricsReportTrigger::getTriggerFromLib("periodic", nullptr, triggerParms, reportConfig);


    //
    // start collection
    pTrigger->start();

    std::thread first (processThread, 20, 1);
    std::thread second (processThread, 15, 3);

    first.join();
    second.join();

    pTrigger->stop();

    printf("Test complete\n");
}


void processThread(int numLoops, unsigned delay)
{
    for (int i=0; i<numLoops; ++i)
    {
        pCountableMetric->inc(1u);
        //mr.get<CountMetric>("requests")->inc(1u);  would need to get the metric name since placing in a metric set may adjust it's name
        pQueueSizeMetric->inc(1);
        pRateMetric->inc(1);
        std::this_thread::sleep_for(std::chrono::seconds(delay));
        pQueueSizeMetric->dec(1);
    }
}
