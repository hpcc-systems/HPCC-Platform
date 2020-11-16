

#include <cstdio>

#include "Metrics.hpp"
#include "MetricsRegistry.hpp"
#include "MetricSet.hpp"
#include "triggers/periodic/PeriodicTrigger.hpp"
#include "MetricsReporter.hpp"
#include <thread>
#include <chrono>
#include <jptree.hpp>


using namespace hpccMetrics;

MetricsRegistry mr;

void processThread(int, unsigned);
std::shared_ptr<CountMetric> pCountableMetric;
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
    pCountableMetric     = std::make_shared<CountMetric>("requests", "The number of requests that have come in");
    metrics.emplace_back(pCountableMetric);

    pRateMetric = std::make_shared<RateMetric>("rate");
    metrics.emplace_back(pRateMetric);

    auto pRequestMetricSet = std::make_shared<MetricSet>("set", "myprefix_", metrics);
    mr.add(pCountableMetric);  // demo use of the registry (optional)

    //
    // create a metric set for queues
    metrics.clear();
    pQueueSizeMetric = std::make_shared<GaugeMetric<uint32_t>>("queuesize");
    pQueueSizeMetric->setValueType(ValueType::INTEGER);
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

    auto pSink = MetricSink::getSinkFromLib("prometheus", nullptr, "prometheus", pSinkSettings);
    reportConfig.addReportConfig(pSink, pRequestMetricSet);
    //reportConfig.addReportConfig(pSink, pQueueMetricSet);

    IPropertyTree *pTriggerSettings = createPTree("TriggerSettings");;
    pTriggerSettings->addPropInt("period", 10);
    IMetricsReportTrigger *pTrigger = MetricsReportTrigger::getTriggerFromLib("prometheus", nullptr, pTriggerSettings);

    pReporter = new MetricsReporter(reportConfig, pTrigger);

    //
    // start collection
    pReporter->start();

    std::thread first (processThread, 20, 1);
    std::thread second (processThread, 15, 3);

    first.join();
    second.join();

    pTrigger->stop();

    printf("Test complete\n");
}


void processThread(int numLoops, unsigned delay)
{
    while (true)//for (int i=0; i<numLoops; ++i)
    {
        pCountableMetric->inc(1u);
        //mr.get<CountMetric>("requests")->inc(1u);  would need to get the metric name since placing in a metric set may adjust it's name
        pQueueSizeMetric->inc(1);
        pRateMetric->inc(1);
        std::this_thread::sleep_for(std::chrono::seconds(delay));
        pQueueSizeMetric->dec(1);
    }
}
