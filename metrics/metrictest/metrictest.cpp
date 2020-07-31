

#include <cstdio>
#include <thread>
#include <chrono>
#include <jptree.hpp>
#include "Metrics.hpp"
//#include "ComponentConfigHelper.hpp"


using namespace hpccMetrics;

void processThread(int, unsigned, bool, const std::string&, unsigned, unsigned);
std::shared_ptr<CounterMetric> pEventCountMetric;
std::shared_ptr<GaugeMetric<uint32_t>> pQueueSizeMetric;
//std::shared_ptr<QueueLatencyMetric> pQueueLatencyMetric;
//std::shared_ptr<RateMetric> pRateMetric;

MetricsReporter *pReporter;


//const char *testYml = R"!!(config:
//  metrics:
//    name: config_name
//    sinks:
//      - type: sinktype
//        name: sinkname
//        config:
//          key1: data1
//          key2: data2
//          Key3:
//            key3.1: data3.1
//            key3.2: data3.2
//    prefixes:
//      - key1: val1
//      - key2: val2
//)!!";


const char *globalConfigYml = R"!!(config:
  metrics:
    name: cluster config
    prefix: global_prefix.
    sinks:
      - type: filesink
        name: default
        settings:
          filename: testout.txt
          clear: true
          period: 5
)!!";


const char *localConfigYml = R"!!(roxie:
  metrics:
    name: config_name
    prefix: component_prefix.
    sinks:
      - name: default
        metrics:
          - name: requests
            measurement_type: count
          - name: requests
            measurement_type: resetting_count
          - name: requests
            measurement_type: rate
            description: Number of request arriving per second
          - name: queuesize
          - name: requests_dynamic
            measurement_type: count
)!!";

//ComponentConfigHelper testCfgHelper;
MetricsReporter reporter;

int main(int argc, char *argv[])
{
    InitModuleObjects();

    //
    // Simulate retrieving the component and global config
    IPropertyTree *pSettingsGlobal = createPTreeFromYAMLString(globalConfigYml, ipt_none, ptr_ignoreWhiteSpace, nullptr);
    IPropertyTree *pSettingsLocal = createPTreeFromYAMLString(localConfigYml, ipt_none, ptr_ignoreWhiteSpace, nullptr);

    //
    // Retrieve the global and component metrics config
    IPropertyTree *pGlobalMetricsTree = pSettingsGlobal->getPropTree("config/metrics");
    IPropertyTree *pLocalMetricsTree = pSettingsLocal->getPropTree("roxie/metrics");

    //
    // Allow override of output file for the file sink
    if (argc > 1)
    {
        std::string sinkReportFilename;
        sinkReportFilename = std::string(argv[1]);
        auto pSinkTree = pSettingsGlobal->getPropTree("config/metrics/sinks[1]/settings");
        pSinkTree->removeProp("@filename");
        pSinkTree->addProp("@filename", sinkReportFilename.c_str());
    }

    //
    // Init reporter with config
    reporter.init(pGlobalMetricsTree, pLocalMetricsTree);

    //
    // Now create the metrics and add them to the reporter
    pEventCountMetric = std::make_shared<CounterMetric>("requests", "The number of requests");
    reporter.addMetric(pEventCountMetric);

    pQueueSizeMetric = std::make_shared<GaugeMetric<uint32_t>>("queuesize", "request queue size");
    reporter.addMetric(pQueueSizeMetric);

    reporter.startCollecting();


    //
    // Starts some threads, each updating metrics
    std::thread first (processThread, 20, 2, true, "requests_dynamic", 4, 10);
    std::thread second (processThread, 15, 3, false, "", 0, 0);

    first.join();
    second.join();

    reporter.stopCollecting();

    printf("Test complete\n");
}


void processThread(int numLoops, unsigned delay, bool addDynamic, const std::string& name, unsigned addAfter, unsigned deleteAfter)
{
    std::shared_ptr<CounterMetric> pDynamicMetric;
    for (unsigned i=0; i<numLoops; ++i)
    {
        if (addDynamic && i == addAfter)
        {
            pDynamicMetric = std::make_shared<CounterMetric>(name.c_str(), "The dynamic number of requests");
            reporter.addMetric(pDynamicMetric);
        }
        else if (addDynamic && i == (addAfter + deleteAfter))
        {
            reporter.removeMetric(name);
            pDynamicMetric.reset();
        }

        if (pDynamicMetric)
        {
            pDynamicMetric->inc(1u);
        }

        pEventCountMetric->inc(2u);
        pQueueSizeMetric->inc(3);
        std::this_thread::sleep_for(std::chrono::seconds(delay));
        pQueueSizeMetric->dec(1);
    }
}
