

#include <cstdio>

#include "Metrics.hpp"
#include "MetricsRegistry.hpp"
#include "MetricSet.hpp"
//#include "FileSink.hpp"
//#include "CountMetric.hpp"
//#include "RateMetric.hpp"
//#include "GaugeMetric.hpp"
#include "PeriodicMetricsReportTrigger.hpp"
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

 Metric is a collected value
 Metrics must be part of a MetricSet. A metric can only be in one metric set. Name of metric must be unique
 MetricSet is determined by the component
 MetricSets are named and are well known by component
 MetricSink is the component that reports metric values to a store (file, elasticsearch, datadog, prometheus, etc)
 MetricReporter is a container of
   one or more MetricSets
   one or more MetricSinks
   represents a reporting set. All MetricSets are collected and reported to all MetricSinks.


 Global level:

 Things that can be defined for reuse at the component level

 metrics:
   sinks:
   - name: sinkname for reference
     type: elasticsearch
     host: <hostname>
     port: <port>
   - name: sink2
     type: file
     other stuff...

   reporters:
   - name: myreporter    can be referred to as a set of values
     type: periodic
     period: 10
   - name: reporter2
     type: polled


 Component level:

 Defines specifics of metrics for the component. Can reference global values

 metrics:
   sinks:  <see global for format>

   reporters:  <see global for format>

   metric_sets:
   - set
     set_names:
     - <set name>   # this is the
     - <set name>   # metric set name
     sinks:
     - <sink name>
     - <sink name2>
     reporter: reporter name



 Component level
 metrics:
   sinks:
   - name:  if same, then overrides existing, otherwise creates a new one


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
    auto pSink = MetricSink::getMetricSinkFromLib("filesink", "getMetricSinkInstance", "es", parms);
    reportConfig.addReportConfig(pSink, pRequestMetricSet);
    reportConfig.addReportConfig(pSink, pQueueMetricSet);

    PeriodicMetricsReportTrigger reportTrigger(10, reportConfig);
    //collector.addSink(pSink);

    //
    // start collection
    reportTrigger.start();

    std::thread first (processThread, 20, 1);
    std::thread second (processThread, 15, 3);

    first.join();
    second.join();

    reportTrigger.stop();

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
