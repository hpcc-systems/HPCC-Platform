#include <cstdio>
#include <thread>
#include <chrono>
#include <jptree.hpp>
#include "jmetrics.hpp"


using namespace hpccMetrics;

void processThread(int, unsigned, bool, const std::string&, unsigned, unsigned);
std::shared_ptr<CounterMetric> pEventCountMetric;
std::shared_ptr<GaugeMetric> pQueueSizeMetric;


MetricsReporter *pReporter;


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


const char *testConfigYml = R"!!(component:
  metrics:
    name: config_name
    prefix: component_prefix.
    sinks:
      - sink: placeholder
        type: filesink
        name: default
        settings:
          filename: testout.txt
          clear: true
          period: 5
)!!";


int main(int argc, char *argv[])
{
    InitModuleObjects();

    //
    // Simulate retrieving the component and global config
    IPropertyTree *pSettings = createPTreeFromYAMLString(testConfigYml, ipt_none, ptr_ignoreWhiteSpace, nullptr);

    //
    // Retrieve the global and component metrics config
    IPropertyTree *pMetricsTree = pSettings->getPropTree("component/metrics");

    //
    // Allow override of output file for the file sink
    if (argc > 1)
    {
        std::string sinkReportFilename;
        sinkReportFilename = std::string(argv[1]);
        auto pSinkTree = pMetricsTree->getPropTree("component/metrics/sinks[1]/settings");
        pSinkTree->removeProp("@filename");
        pSinkTree->addProp("@filename", sinkReportFilename.c_str());
    }


    //
    // Get singleton
    MetricsReporter &myReporter = getMetricsReporter();

    //
    // Init reporter with config
    myReporter.init(pMetricsTree);

    //
    // Now create the metrics and add them to the reporter
    pEventCountMetric = std::make_shared<CounterMetric>("requests", "The number of requests");
    myReporter.addMetric(pEventCountMetric);

    pQueueSizeMetric = std::make_shared<GaugeMetric>("queuesize", "request queue size");
    myReporter.addMetric(pQueueSizeMetric);

    myReporter.startCollecting();

    //
    // Starts some threads, each updating metrics
    std::thread first (processThread, 20, 2, true, "requests_dynamic", 4, 10);
    std::thread second (processThread, 15, 3, false, "", 0, 0);

    first.join();
    second.join();

    myReporter.stopCollecting();

    printf("Test complete\n");
}


void processThread(int numLoops, unsigned delay, bool addDynamic, const std::string& name, unsigned addAfter, unsigned deleteAfter)
{
    std::shared_ptr<GaugeMetric> pDynamicMetric;
    for (unsigned i=0; i<numLoops; ++i)
    {
        if (addDynamic && i == addAfter)
        {
            MetricsReporter &myReporter = getMetricsReporter();
            pDynamicMetric = std::make_shared<GaugeMetric>(name.c_str(), "The dynamic number of requests");
            myReporter.addMetric(pDynamicMetric);
        }
        else if (addDynamic && i == (addAfter + deleteAfter))
        {
            //reporter.removeMetric(name);
            pDynamicMetric.reset();
        }

        if (pDynamicMetric)
        {
            pDynamicMetric->add(1);
        }

        pEventCountMetric->inc(2u);
        pQueueSizeMetric->add(3);
        std::this_thread::sleep_for(std::chrono::seconds(delay));
        pQueueSizeMetric->add(-11);
    }
}

