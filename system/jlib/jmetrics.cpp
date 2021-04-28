/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include "jmetrics.hpp"
#include "jlog.hpp"

using namespace hpccMetrics;

static Singleton<MetricsReporter> metricsReporter;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    delete metricsReporter.queryExisting();
}


struct hpccMetrics::SinkInfo
{
    explicit SinkInfo(MetricSink *_pSink) : pSink{_pSink} {}
    MetricSink *pSink = nullptr;             // ptr to the sink
    std::vector<std::string> reportMetrics;   // vector of metrics to report (empty for none)
};

MetricsReporter &hpccMetrics::queryMetricsReporter()
{
    return *metricsReporter.query([] { return new MetricsReporter; });
}


MetricsReporter::~MetricsReporter()
{
    for (auto const &sinkIt : sinks)
    {
        sinkIt.second->pSink->stopCollection();
        delete sinkIt.second->pSink;
    }
}


void MetricsReporter::init(IPropertyTree *pMetricsTree)
{
    Owned<IPropertyTreeIterator> sinkElementsIt = pMetricsTree->getElements("sinks");
    initializeSinks(sinkElementsIt);
}


void MetricsReporter::addMetric(const std::shared_ptr<IMetric> &pMetric)
{
    std::unique_lock<std::mutex> lock(metricVectorMutex);
    auto it = metrics.find(pMetric->queryName());
    if (it == metrics.end())
    {
        metrics.insert({pMetric->queryName(), pMetric});
    }
    else
    {
        //If there is a match only report an error if the metric has not been destroyed in the meantime
        auto match = it->second.lock();
        if (match)
        {
#ifdef _DEBUG
            throw MakeStringException(MSGAUD_operator, "addMetric - Attempted to add duplicate named metric with name '%s'", pMetric->queryName().c_str());
#else
            OERRLOG("addMetric - Adding a duplicate named metric '%s', old metric replaced", pMetric->queryName().c_str());
#endif
        }
        it->second = pMetric;
    }
}


void MetricsReporter::startCollecting()
{
    for (auto const &sinkIt : sinks)
    {
        sinkIt.second->pSink->startCollection(this);
    }
}


void MetricsReporter::stopCollecting()
{
    for (auto const &sinkIt : sinks)
    {
        sinkIt.second->pSink->stopCollection();
    }
}


std::vector<std::shared_ptr<IMetric>> MetricsReporter::queryMetricsForReport(const std::string &sinkName)
{
    std::vector<std::shared_ptr<IMetric>> reportMetrics;
    reportMetrics.reserve(metrics.size());

    auto it = sinks.find(sinkName);
    if (it != sinks.end())
    {
        //
        // Lock the list of metrics while it's in use
        std::unique_lock<std::mutex> lock(metricVectorMutex);   // no one else can mess with it for a bit
        auto metricIt=metrics.begin();
        while (metricIt != metrics.end())
        {
            auto pMetric = metricIt->second.lock();
            if (pMetric)
            {
                // This is where the metric would be compared against the list of metrics to be reported
                // by the sink (probably a regex). This allows limiting the metrics reported to the sink.
                // for now, only the default is supported which is reporting all metrics.
                reportMetrics.emplace_back(std::move(pMetric));
                ++metricIt;
            }
            else
            {
                metricIt = metrics.erase(metricIt);
            }
        }
    }
    else
    {
        throw MakeStringException(MSGAUD_operator, "queryMetricsForReport - sink name %s not found", sinkName.c_str());
    }
    return reportMetrics;
}


void MetricsReporter::initializeSinks(IPropertyTreeIterator *pSinkIt)
{
    for (pSinkIt->first(); pSinkIt->isValid(); pSinkIt->next())
    {
        Owned<IPropertyTree> pSinkTree = &pSinkIt->get(); // pSinkIt->query();

        StringBuffer cfgSinkType, cfgSinkName;
        pSinkTree->getProp("@type", cfgSinkType);
        pSinkTree->getProp("@name", cfgSinkName);

        //
        // Make sure both name and type are provided
        if (cfgSinkType.isEmpty() || cfgSinkName.isEmpty())
        {
            throw MakeStringException(MSGAUD_operator, "initializeSinks - All sinks definitions must specify a name and a type");
        }

        //
        // If sink already registered, use it, otherwise it's new.
        auto sinkIt = sinks.find(cfgSinkName.str());
        if (sinkIt == sinks.end())
        {
            Owned<IPropertyTree> pSinkSettings = pSinkTree->getPropTree("settings");
            MetricSink *pSink = getSinkFromLib(cfgSinkType.str(), cfgSinkName.str(), pSinkSettings);
            sinks.insert({std::string(cfgSinkName.str()), std::unique_ptr<SinkInfo>(new SinkInfo(pSink))});
        }
    }
}


MetricSink *MetricsReporter::getSinkFromLib(const char *type, const char *sinkName, const IPropertyTree *pSettingsTree)
{
    std::string libName;

    libName.append("libhpccmetrics_").append(type).append("sink").append(SharedObjectExtension);

    HINSTANCE libHandle = LoadSharedObject(libName.c_str(), true, false);

    //
    // If able to load the lib, get the instance proc and create the sink instance
    MetricSink *pSink = nullptr;
    if (libHandle != nullptr)
    {
        auto getInstanceProc = (getSinkInstance) GetSharedProcedure(libHandle, "getSinkInstance");
        if (getInstanceProc != nullptr)
        {
            pSink = getInstanceProc(sinkName, pSettingsTree);
            if (pSink == nullptr)
            {
                throw MakeStringException(MSGAUD_operator, "getSinkFromLib - Unable to get sink instance");
            }
        }
        else
        {
            throw MakeStringException(MSGAUD_operator, "getSinkFromLib - Unable to get shared procedure (getSinkInstance)");
        }
    }
    else
    {
        throw MakeStringException(MSGAUD_operator, "getSinkFromLib - Unable to load sink lib (%s)", libName.c_str());
    }
    return pSink;
}


PeriodicMetricSink::PeriodicMetricSink(const char *name, const char *type, const IPropertyTree *pSettingsTree) :
    MetricSink(name, type),
    collectionPeriodSeconds{60}
{
    if (pSettingsTree->hasProp("@period"))
    {
        collectionPeriodSeconds = pSettingsTree->getPropInt("@period");
    }
}


PeriodicMetricSink::~PeriodicMetricSink()
{
    if (isCollecting)
    {
        doStopCollecting();
    }
}


void PeriodicMetricSink::startCollection(MetricsReporter *_pReporter)
{
    pReporter = _pReporter;
    prepareToStartCollecting();
    isCollecting = true;
    collectThread = std::thread(&PeriodicMetricSink::collectionThread, this);
}


void PeriodicMetricSink::collectionThread()
{
    //
    // The initial wait for the first report
    waitSem.wait(collectionPeriodSeconds * 1000);
    while (!stopCollectionFlag)
    {
        doCollection();

        // Wait again
        waitSem.wait(collectionPeriodSeconds * 1000);
    }
}


void PeriodicMetricSink::stopCollection()
{
    if (isCollecting)
    {
        doStopCollecting();
    }
}


void PeriodicMetricSink::doStopCollecting()
{
    //
    // Set the stop collecting flag, then signal the wait semaphore
    // to wake up and stop the collection thread
    stopCollectionFlag = true;
    waitSem.signal();
    isCollecting = false;
    collectThread.join();
    collectingHasStopped();
}
