

#include <cstdio>

#include "Metrics.hpp"
//#include "MetricSet.hpp"
//#include "MetricSink.hpp"
//#include "triggers/periodic/PeriodicTrigger.hpp"
//#include "MetricsReporter.hpp"
#include <thread>
#include <chrono>
#include <jptree.hpp>


using namespace hpccMetrics;

void processThread(int, unsigned);
std::shared_ptr<EventCountMetric> pEventCountMetric;
std::shared_ptr<GaugeMetric<uint32_t>> pQueueSizeMetric;
//std::shared_ptr<QueueLatencyMetric> pQueueLatencyMetric;
std::shared_ptr<RateMetric> pRateMetric;

MetricsReporter *pReporter;


//const char *configYml = R"!!(metrics:
//  name: config_name
//  sinks:
//    - type: sinktype
//      name: sinkname
//      config:
//        key1: data1
//        key2: data2
//        Key3:
//          key3.1: data3.1
//          key3.2: data3.2
//    )!!";


int main(int argc, char *argv[])
{
    InitModuleObjects();

//    IPropertyTree *pSettings = createPTreeFromYAMLString(configYml, ipt_none, ptr_ignoreWhiteSpace, nullptr);

    MetricsReportConfig reportConfig;

    //
    // Create a metric set for request type metrics
    std::vector<std::shared_ptr<IMetric>> metrics;
    pEventCountMetric     = std::make_shared<EventCountMetric>("requests", "The number of requests that have come in");
    metrics.emplace_back(pEventCountMetric);

    pRateMetric = std::make_shared<RateMetric>("rate", "");
    metrics.emplace_back(pRateMetric);

    auto pRequestMetricSet = std::make_shared<MetricSet>("set", "myprefix.", metrics);

    //
    // create a metric set for queues
    metrics.clear();
    pQueueSizeMetric = std::make_shared<GaugeMetric<uint32_t>>("queuesize", "", ValueType::INTEGER);
    metrics.emplace_back(pQueueSizeMetric);
    auto pQueueMetricSet = std::make_shared<MetricSet>("set2", "myprefix2", metrics);

    //
    // Get the name of thee report file
    auto pSinkSettings = createPTree("SinkSettings");
    std::string sinkReportFilename;
    if (argc > 1)
    {
        StringBuffer fname;
        sinkReportFilename = std::string(argv[1]);
        pSinkSettings->addProp("filename", sinkReportFilename.c_str());
        pSinkSettings->getProp("filename", fname);
    }
    else
    {
        printf("You must provide the full path to the report file\n\n");
        exit(0);
    }

    auto pSink = MetricSink::getSinkFromLib("filesink", nullptr, "es", pSinkSettings);
    reportConfig.addReportConfig(pSink, pRequestMetricSet);
    reportConfig.addReportConfig(pSink, pQueueMetricSet);

    IPropertyTree *pTriggerSettings = createPTree("TriggerSettings");;
    pTriggerSettings->addPropInt("period", 10);
    IMetricsReportTrigger *pTrigger = MetricsReportTrigger::getTriggerFromLib("periodic", nullptr, pTriggerSettings);

    pReporter = new MetricsReporter(reportConfig, pTrigger);

    //
    // start collection
    pReporter->start();

    std::thread first (processThread, 20, 1);
    std::thread second (processThread, 15, 3);

    first.join();
    second.join();

    pReporter->stop();

    printf("Test complete\n");
}


void processThread(int numLoops, unsigned delay)
{
    for (int i=0; i<numLoops; ++i)
    {
        pEventCountMetric->inc(1u);
        pQueueSizeMetric->inc(1);
        pRateMetric->inc(1);
        std::this_thread::sleep_for(std::chrono::seconds(delay));
        pQueueSizeMetric->dec(1);
    }
}
