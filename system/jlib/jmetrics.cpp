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
#include "jmutex.hpp"
#include "jlog.hpp"

using namespace hpccMetrics;

static Singleton<MetricsReporter> metricsReporter;
static Singleton<MetricsRegistry> metricsRegistry;
MODULE_INIT(INIT_PRIORITY_JLOG)
{
    return true;
}
MODULE_EXIT()
{
    delete metricsReporter.queryExisting();
    delete metricsRegistry.queryExisting();
}


MetricsReporter &hpccMetrics::getMetricsReporter()
{
    return *metricsReporter.query([] { return new MetricsReporter; });
}


bool jlib_decl hpccMetrics::isMetricsInitialized()
{
    return metricsReporter.queryExisting() != nullptr;
}


MetricsRegistry &hpccMetrics::getMetricsRegistry()
{
    return *metricsRegistry.query([] { return new MetricsRegistry; });
}


void CounterMetric::getReportValues(MeasurementVector &mv) const
{
    mv.emplace_back((std::make_shared<Measurement<uint32_t>>(name, description, ValueType::METRICS_INTEGER, count.load())));
}


void GaugeMetric::getReportValues(MeasurementVector &mv) const
{
    mv.emplace_back((std::make_shared<Measurement<float>>(name, description, ValueType::METRICS_FLOAT, gaugeValue.load())));
}


MetricsReporter::~MetricsReporter()
{
    for (auto const &sinkIt : sinks)
    {
        delete sinkIt.second.pSink;
    }
}


void MetricsReporter::init(IPropertyTree *pMetricsTree)
{
    Owned<IPropertyTree> pSinkTree = pMetricsTree->getPropTree("sinks");
    auto sinkElementsIt = pSinkTree->getElements("sink");
    initializeSinks(sinkElementsIt);
}


void MetricsReporter::addMetric(const std::shared_ptr<IMetric> &pMetric)
{
    std::unique_lock<std::mutex> lock(metricVectorMutex);
    auto it = metrics.find(pMetric->getName());
    if (it == metrics.end())
    {
        metrics.insert({pMetric->getName(), pMetric});
    }
    // note that
}


void MetricsReporter::removeMetric(const std::string &metricName)
{
    std::unique_lock<std::mutex> lock(metricVectorMutex);
    auto it = metrics.find(metricName);
    if (it != metrics.end())
    {
        metrics.erase(it);
    }
}


void MetricsReporter::startCollecting()
{
    for (auto const &sinkIt : sinks)
    {
        sinkIt.second.pSink->startCollection(this);
    }
}


void MetricsReporter::stopCollecting()
{
    for (auto const &sinkIt : sinks)
    {
        sinkIt.second.pSink->stopCollection();
    }
}


std::vector<std::shared_ptr<IMetric>> MetricsReporter::getReportMetrics(const std::string &sinkName)
{
    std::vector<std::shared_ptr<IMetric>> reportMetrics;

    auto it = sinks.find(sinkName);
    if (it != sinks.end())
    {
        //
        // This is where metric names are matched against patterns, however for now, just return all the metrics
        std::unique_lock<std::mutex> lock(metricVectorMutex);   // no one else can mess with it for a bit
        auto metricIt=metrics.begin();
        while (metricIt != metrics.end())
        {
            auto pMetric = metricIt->second.lock();
            if (pMetric)
            {
                reportMetrics.emplace_back(pMetric);
                metricIt++;
            }
            else
            {
                metricIt = metrics.erase(metricIt);
            }
        }
    }
    // else throw? Detect that the sink Name is no longer valid?


    return reportMetrics;  // All metrics
}



bool MetricsReporter::initializeSinks(IPropertyTreeIterator *pSinkIt)
{
    bool rc = true;
    for (pSinkIt->first(); pSinkIt->isValid() && rc; pSinkIt->next())
    {
        SinkInfo *pSinkInfo;
        Owned<IPropertyTree> pSinkTree = &pSinkIt->get(); // pSinkIt->query();

        StringBuffer cfgSinkType, cfgSinkName;
        pSinkTree->getProp("@type", cfgSinkType);  // this one is required
        pSinkTree->getProp("@name", cfgSinkName);

        std::string sinkName = cfgSinkName.isEmpty() ? "default" : cfgSinkName.str();

        //
        // If sink already registered, use it, otherwise it's new.
        auto sinkIt = sinks.find(sinkName);
        if (sinkIt != sinks.end())
        {
            pSinkInfo = &(sinkIt->second);
        }
        else
        {
            Owned<IPropertyTree> pSinkSettings = pSinkTree->getPropTree("settings");
            MetricSink *pSink = getSinkFromLib(cfgSinkType.str(), (const char *)sinkName.c_str(), pSinkSettings);
            if (pSink != nullptr)
            {
                auto insertRc = sinks.insert({pSink->getName(), SinkInfo{pSink}});
                pSinkInfo = &insertRc.first->second;
            }
        }

        //
        // Retrieve metrics to be reported
        if (pSinkInfo != nullptr)
        {
            //
            // Now add defined metrics if present
            Owned<IPropertyTreeIterator> pSinkMetricsIt = pSinkTree->getElements("metrics");
            for (pSinkMetricsIt->first(); pSinkMetricsIt->isValid(); pSinkMetricsIt->next())
            {
                StringBuffer metricName, measurementType, description;
                pSinkMetricsIt->query().getProp("@name", metricName);
                pSinkInfo->reportMetrics.emplace_back(metricName);
            }
        }
        else
        {
            rc = false;
            // todo - need to decide how to handle bad metrics configurations. Hobble along with what is valid? Log the error?
        }
    }
    return rc;
}


MetricSink *MetricsReporter::getSinkFromLib(const char *type, const char *sinkName, const IPropertyTree *pSettingsTree)
{
    std::string libName;

    libName.append("libhpccmetrics_").append(type).append(SharedObjectExtension);

    HINSTANCE libHandle = LoadSharedObject(libName.c_str(), true, false);

    //
    // If able to load the lib, get the instance proc and create the sink instance
    MetricSink *pSink = nullptr;
    if (libHandle != nullptr)
    {
        auto getInstanceProc = (getSinkInstance) GetSharedProcedure(libHandle, "getSinkInstance");
        if (getInstanceProc != nullptr)
        {
            const char *name = isEmptyString(sinkName) ? type : sinkName;
            pSink = getInstanceProc(name, pSettingsTree);
        }
    }
    return pSink;
}


void MetricsRegistry::addMetric(const std::shared_ptr<IMetric> &pMetric)
{
    metrics.insert({pMetric->getName(), pMetric});
}


void MetricsRegistry::removeMetric(const std::string &name)
{
    metrics.erase(name);
}
