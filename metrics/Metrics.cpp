/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "Metrics.hpp"

using namespace hpccMetrics;

void EventCountMetric::collect(MeasurementVector &values)
{
    auto pMeasurement = std::make_shared<Measurement<uint32_t>>(this, reportingName, count.exchange(0, std::memory_order_relaxed));
    values.emplace_back(pMeasurement);
}

void CounterMetric::collect(MeasurementVector &values)
{
    auto pMeasurement = std::make_shared<Measurement<uint32_t>>(this, reportingName, count.load(std::memory_order_relaxed));
    values.emplace_back(pMeasurement);
}

void RateMetric::collect(MeasurementVector &values)
{
    std::chrono::time_point<std::chrono::high_resolution_clock> now, start;
    unsigned numEvents;

    now = std::chrono::high_resolution_clock::now();
    start = periodStart;
    numEvents = count.exchange(0);
    periodStart = now;

    auto seconds = (std::chrono::duration_cast<std::chrono::seconds>(now - start)).count();
    float rate = (seconds != 0) ? (static_cast<float>(numEvents) / static_cast<float>(seconds)) : 0;
    auto pMeasurement = std::make_shared<Measurement<float>>(this, reportingName, rate);
    values.emplace_back(pMeasurement);
}

MetricSet::MetricSet(const char *_name, const char *_prefix, const std::vector<std::shared_ptr<IMetric>> &_metrics) :
    name{_name},
    reportNamePrefix{_prefix}
{
    setMetrics(_metrics);
}

std::vector<std::shared_ptr<const IMetric>> MetricSet::getMetrics()
{
    std::vector<std::shared_ptr<const IMetric>> returnMetrics;
    for (const auto& metricIt : metrics)
    {
        returnMetrics.emplace_back(metricIt.second);
    }
    return returnMetrics;
}

void MetricSet::init()
{
    for (const auto& metricIt : metrics)
    {
        metricIt.second->init();
    }
}

void MetricSet::collect(MeasurementVector &values)
{
    for (const auto &metricIt : metrics)
    {
        metricIt.second->collect(values);
    }
}

void MetricSet::setMetrics(const std::vector<std::shared_ptr<IMetric>> &_metrics)
{
    for (auto const &pMetric : _metrics)
    {
        //
        // Make sure the metric has a type
        if (pMetric->getValueType() == ValueType::NONE)
        {
            throw std::exception();
        }

        //
        // A metric may only be added to one metric set
        if (!pMetric->isInMetricSet())
        {
            //
            // Set the metric name used for reporting by prefixing the metric name
            pMetric->setReportingName(reportNamePrefix + pMetric->getName());

            //
            // Insert the metric, but ensure the name is unique
            auto rc = metrics.insert({pMetric->getName(), pMetric});
            if (!rc.second)
            {
                throw std::exception();   // metric already added with the same name
            }
            pMetric->setInMetricSet(true);
        }
        else
        {
            throw std::exception();  // not sure if this is the right thing to do yet
        }
    }
}


void MetricSink::setMetricSets(const std::vector<std::shared_ptr<IMetricSet>> &sets)
{
    for (const auto& pSet : sets)
    {
        metricSets[pSet->getName()] = pSet;
    }
}


IMetricSink *MetricSink::getSinkFromLib(const char *type, const char *sinkName, const IPropertyTree *pSettingsTree)
{
    return MetricSink::getSinkFromLib(type, "", sinkName, pSettingsTree);
}

IMetricSink *MetricSink::getSinkFromLib(const char *type, const char *getInstanceProcName, const char *sinkName, const IPropertyTree *pSettingsTree)
{
    std::string libName;

    //
    // First, treat type as a full library name
    libName = type;
    if (libName.find(SharedObjectExtension) == std::string::npos)
    {
        libName.append(SharedObjectExtension);
    }
    HINSTANCE libHandle = LoadSharedObject(libName.c_str(), true, false);

    //
    // If type wasn't the lib name, treat it as a type
    if (libHandle == nullptr)
    {
        libName.clear();
        libName.append("libhpccmetrics_").append(type).append(SharedObjectExtension);
        libHandle = LoadSharedObject(libName.c_str(), true, false);
    }

    //
    // If able to load the lib, get the instance proc and create the sink instance
    IMetricSink *pSink = nullptr;
    if (libHandle != nullptr)
    {
        const char *epName = !isEmptyString(getInstanceProcName) ? getInstanceProcName : "getSinkInstance";
        auto getInstanceProc = (getSinkInstance) GetSharedProcedure(libHandle, epName);
        if (getInstanceProc != nullptr)
        {
            pSink = getInstanceProc(isEmptyString(sinkName) ? type : sinkName, pSettingsTree);
        }
    }
    return pSink;
}


void MetricsReportConfig::addReportConfig(IMetricSink *pSink, const std::shared_ptr<IMetricSet> &set)
{
    auto reportCfgIt = metricReportConfig.find(pSink);
    if (reportCfgIt == metricReportConfig.end())
    {
        reportCfgIt = metricReportConfig.insert({pSink, std::vector<std::shared_ptr<IMetricSet>>()}).first;
    }
    reportCfgIt->second.emplace_back(set);
    metricSets.insert(set);
}


void MetricsReporter::init()
{
    //
    // Tell the trigger who we are
    pTrigger->setReporter(this);

    //
    // Initialization consists of initializing each sink, informing
    // each sink about the metric sets for which it shall report
    // measurements, and initializing each metric set.
    for (auto reportConfigIt : reportConfig.metricReportConfig)
    {
        reportConfigIt.first->setMetricSets(reportConfigIt.second);
    }

    //
    // Tell each metric that collection is beginning
    for (const auto& pMetricSet : reportConfig.metricSets)
    {
        pMetricSet->init();
    }
}

bool MetricsReporter::report(std::map<std::string, MetricsReportContext *> &reportContexts)
{
    std::unique_lock<std::mutex> lock(reportMutex);
    //
    // vectors of measurements for each metric set
    std::map<std::shared_ptr<IMetricSet>, MeasurementVector> metricSetReportValues;

    //
    // Collect all the values
    for (auto &pMetricSet : reportConfig.metricSets)
    {
        metricSetReportValues[pMetricSet] = MeasurementVector();
        pMetricSet->collect(metricSetReportValues[pMetricSet]);
    }

    MetricsReportContext defaultReportContext;

    //
    // Send registered metric sets to each sink
    for (const auto &reportConfigIt : reportConfig.metricReportConfig)
    {
        //
        // Obtain the context for the sink. If no specific context has been set, build a default
        // context and use it.
        MetricsReportContext *pReportContext;
        auto it = reportContexts.find(reportConfigIt.first->getName());
        if (it != reportContexts.end())
        {
            pReportContext = it->second;
        }
        else
        {
            pReportContext = &defaultReportContext;
        }

        //
        // call each sink
        for (auto const &pMetricSet : reportConfigIt.second)
        {
            reportConfigIt.first->handle(metricSetReportValues[pMetricSet], pMetricSet, pReportContext);
        }
    }
    return true;
}

IMetricsReportTrigger *MetricsReportTrigger::getTriggerFromLib(const char *type, const IPropertyTree *pSettingsTree)
{
    return MetricsReportTrigger::getTriggerFromLib(type, "", pSettingsTree);
}

IMetricsReportTrigger *MetricsReportTrigger::getTriggerFromLib(const char *type, const char *getInstanceProcName, const IPropertyTree *pSettingsTree)
{
    IMetricsReportTrigger *pTrigger = nullptr;

    std::string libName;

    //
    // First, treat type as a full library name
    libName = type;
    if (libName.find(SharedObjectExtension) == std::string::npos)
    {
        libName.append(SharedObjectExtension);
    }
    HINSTANCE libHandle = LoadSharedObject(libName.c_str(), true, false);

    //
    // If not, use type as a part of the standard metrics lib naming convention
    if (libHandle == nullptr)
    {
        libName.clear();
        libName.append("libhpccmetrics_").append(type).append(SharedObjectExtension);
        libHandle = LoadSharedObject(libName.c_str(), true, false);
    }

    if (libHandle != nullptr)
    {
        const char *epName = (getInstanceProcName != nullptr && strlen(getInstanceProcName) != 0) ?
                             getInstanceProcName : "getTriggerInstance";
        auto getInstanceProc = (getTriggerInstance) GetSharedProcedure(libHandle, epName);
        if (getInstanceProc != nullptr)
        {
            pTrigger = getInstanceProc(pSettingsTree);
        }
    }
    return pTrigger;
}


void MetricsReportTrigger::doReport(const std::string& sinkName, MetricsReportContext *pReportContext)
{
    std::map<std::string, MetricsReportContext *> reportContexts;
    if (pReportContext != nullptr)
    {
        reportContexts[sinkName] = pReportContext;
    }
    pReporter->report(reportContexts);
}
