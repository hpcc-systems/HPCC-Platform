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

const char *testFileSinkConfigYml = R"!!(component:
  metrics:
    name: config_name
    prefix: component_prefix.
    sinks:
      - type: file
        name: default
        settings:
          filename: testout.txt
          clear: true
          period: 5
)!!";

const char *testLogSinkConfigYml = R"!!(component:
  metrics:
    name: config_name
    prefix: component_prefix.
    sinks:
      - type: log
        name: default
        settings:
          period: 5
)!!";

const char *testPrometheusSinkConfigYml = R"!!(component:
  metrics:
    name: config_name
    prefix: component_prefix.
    sinks:
      - type: prometheus
        name: PrometheusMetricsSink
        settings:
          port: 8767
          serviceName: metrics
          verbose : true
)!!";

int main(int argc, char *argv[])
{
    try
    {
        InitModuleObjects();

        //
        // Simulate retrieving the component and global config
        //Owned<IPropertyTree> pSettings = createPTreeFromYAMLString(testLogSinkConfigYml, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        Owned<IPropertyTree> pSettings = createPTreeFromYAMLString(testPrometheusSinkConfigYml, ipt_none, ptr_ignoreWhiteSpace, nullptr);

        //
        // Retrieve the global and component metrics config
        Owned<IPropertyTree> pMetricsTree = pSettings->getPropTree("component/metrics");

        //
        // Allow override of output file for the file sink
        if (argc > 1)
        {
            auto pSinkTree = pMetricsTree->getPropTree("component/metrics/sinks[1]/settings");
            pSinkTree->setProp("@filename", argv[1]);
        }

        //
        // Get singleton
        MetricsReporter &myReporter = queryMetricsReporter();

        //
        // Init reporter with config
        myReporter.init(pMetricsTree);

        //
        // Now create the metrics and add them to the reporter
        pEventCountMetric = std::make_shared<CounterMetric>("requests", "The number of requests");
        myReporter.addMetric(pEventCountMetric);

        pQueueSizeMetric = createMetricAndAddToReporter<GaugeMetric>("queuesize", "request queue size");

        myReporter.startCollecting();

        //
        // Starts some threads, each updating metrics
        std::thread first (processThread, 20, 2, true, "requests_dynamic", 4, 10);
        std::thread second (processThread, 15, 3, false, "", 0, 0);

        first.join();
        second.join();

        printf("Stopping the collection...");
        myReporter.stopCollecting();
        printf("Stopped. Test complete\n");
    }

    catch (...)
    {
        printf("Exception detected, test stopped");
    }

}


void processThread(int numLoops, unsigned delay, bool addDynamic, const std::string& name, unsigned addAfter, unsigned deleteAfter)
{
    std::shared_ptr<GaugeMetric> pDynamicMetric;
    for (unsigned i=0; i<numLoops; ++i)
    {
        if (addDynamic && i == addAfter)
        {
            MetricsReporter &myReporter = queryMetricsReporter();
            pDynamicMetric = std::make_shared<GaugeMetric>(name.c_str(), "The dynamic number of requests");
            myReporter.addMetric(pDynamicMetric);
        }
        else if (addDynamic && i == (addAfter + deleteAfter))
        {
            pDynamicMetric.reset();
        }

        if (pDynamicMetric)
        {
            pDynamicMetric->add(1);
        }

        pEventCountMetric->inc(2u);
        pQueueSizeMetric->add(3);
        std::this_thread::sleep_for(std::chrono::seconds(delay));
        pQueueSizeMetric->add(-1);
    }
}
