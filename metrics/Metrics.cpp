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


bool CounterMetric::prepareMeasurements(const std::string &context)
{
    bool rc = false;

    //
    // Find the context state for the input context. If found, update its state.
    // If not found, create one and save the current state.
    auto it = contextState.find(context);
    if (it != contextState.end())
    {
        it->second.lastValue = it->second.curValue;
        it->second.curValue = count.load(std::memory_order_relaxed);
        it->second.lastCollectedAt = it->second.curCollectedAt;
        it->second.curCollectedAt = std::chrono::high_resolution_clock::now();
        rc = true;   // metric is ready to provide measurements
    }
    else
    {
        CounterMetric::metricState initialContextValues;
        initialContextValues.curValue = initialContextValues.lastValue = 0;
        initialContextValues.curCollectedAt = std::chrono::high_resolution_clock::now();
        contextState.insert({context, initialContextValues});
    }
    return rc;
}


void CounterMetric::getMeasurement(const std::string &measType, const std::string &measurementReportName, const std::string &context, MeasurementVector &measurements)
{
    std::shared_ptr<MeasurementBase> pMeasurement;
    auto it = contextState.find(context);
    if (it != contextState.end())
    {
        std::string reportName(measurementReportName);
        reportName.append(".").append(measType);
        if (measType == "count" || measType == "default" || measType.empty())
        {
            pMeasurement = std::make_shared<Measurement<uint32_t>>(this, reportName, description, ValueType::INTEGER, it->second.curValue);
        }
        else if (measType == "resetting_count")
        {
            pMeasurement = std::make_shared<Measurement<uint32_t>>(this, reportName, description, ValueType::INTEGER, it->second.curValue - it->second.lastValue);
        }
        else if (measType == "rate")
        {
            unsigned numEvents = it->second.curValue - it->second.lastValue;
            auto seconds = (std::chrono::duration_cast<std::chrono::seconds>(it->second.curCollectedAt - it->second.lastCollectedAt)).count();
            float rate = (seconds != 0) ? (static_cast<float>(numEvents) / static_cast<float>(seconds)) : 0;
            pMeasurement = std::make_shared<Measurement<float>>(this, reportName, description, ValueType::FLOAT, rate);
        }
        // how to handle a bad measurement type
    }
    // else throw ?

    //
    // Push the measurement to the vector if created
    if (pMeasurement)
    {
        measurements.emplace_back(pMeasurement);
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
            const char *name = isEmptyString(sinkName) ? type : sinkName;
            pSink = getInstanceProc(name, pSettingsTree);
        }
    }
    return pSink;
}



void MetricsReporter::addMetric(const std::shared_ptr<IMetric> &pMetric)
{
    std::unique_lock<std::mutex> lock(metricVectorMutex);
    auto it = metrics.find(pMetric->getName());
    if (it == metrics.end())
    {
        metrics.insert({pMetric->getName(), pMetric});

        //
        // Mark metrics changed for each sink so that reported measurements are recalculated
        for (auto &pSink : sinks)
        {
            pSink.second.metricsChanged = true;
        }
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

        //
        // Mark metrics changed for each sink so that reported measurements are recalculated
        for (auto &pSink : sinks)
        {
            pSink.second.metricsChanged = true;
        }
    }
}


void MetricsReporter::collectMeasurements(const std::string &context)
{
    auto sinkIt = sinks.find(context);
    if (sinkIt != sinks.end())
    {
        std::unique_lock<std::mutex> lock(reportMutex);  // only one at a time

        //
        // Update the metrics to be reported (normally no changes)
        updateSinkMeasurementsToReport(sinkIt->second);


        MeasurementVector measurements;
        std::string reportNamePrefix;
        reportNamePrefix.append(globalPrefix).append(componentPrefix);

        //
        // Process the report metrics for the sink.
        for (auto &reportMetric : sinkIt->second.reportMetrics)
        {
            //
            // Lock each metric for the collection. If the lock fails, remove the metric
            std::shared_ptr<IMetric> pMetric = reportMetric.second.pMetric.lock();
            if (!pMetric)
            {
                removeMetric(reportMetric.first);
            }
            else
            {
                if (pMetric->prepareMeasurements(context))
                {
                    for (auto const &measType : reportMetric.second.measurementTypes)
                    {
                        std::string measurementReportName = reportNamePrefix;
                        measurementReportName.append(pMetric->getName());
                        pMetric->getMeasurement(measType, measurementReportName, context, measurements);
                    }
                }
            }
        }

        //
        // Send the measurements, if any, to the sink for reporting
        if (!measurements.empty())
        {
            sinkIt->second.pSink->reportMeasurements(measurements);
        }
    }
}


void MetricsReporter::updateSinkMeasurementsToReport(SinkInfo &sinkInfo)
{
    //
    // If the metrics have changed, update the sink's report metric vector. Note this should not be happening that often.
    if (sinkInfo.metricsChanged)
    {
        sinkInfo.reportMetrics.clear();

        //
        // If the sink has a specific list of measurements to report, build the list, otherwise report the default measurement for all metrics
        if (!sinkInfo.reportMeasurements.empty())
        {
            for (auto const &measurementInfo : sinkInfo.reportMeasurements)
            {
                //
                // Look through the metrics for any that match. Note that if any metrics are no longer
                // valid, just remove it. Other
                for (auto const &metricIt : metrics)
                {
                    //
                    // Lock the weak pointer. If no longer present, remove it and move on
                    std::shared_ptr<IMetric> pMetric = metricIt.second.lock();
                    if (!pMetric)
                    {
                        removeMetric(metricIt.first);
                        break;
                    }

                    //
                    // reportMetricName is to be a pattern to match against each metric name.
                    // Not sure if we'll just user a '*' or regex. Will be added later. Just
                    // do an exact match for now
                    bool isMatch = pMetric->getName() == measurementInfo.metricName;

                    //
                    //
                    // If the metric is a match, add it to the report info
                    if (isMatch)
                    {
                        auto it = sinkInfo.reportMetrics.find(pMetric->getName());
                        if (it == sinkInfo.reportMetrics.end())
                        {
                            MetricReportInfo mri;
                            mri.pMetric = pMetric;
                            it = sinkInfo.reportMetrics.insert({pMetric->getName(), mri}).first;
                        }
                        it->second.measurementTypes.emplace_back(measurementInfo.measurementType);
                    }
                }
            }
        }
        else
        {
            for (auto const &metricIt : metrics)
            {
                //
                // Lock the weak pointer. If no longer present, remove it and move on
                std::shared_ptr<IMetric> pMetric = metricIt.second.lock();
                if (!pMetric)
                {
                    removeMetric(metricIt.first);
                    break;
                }
                MetricReportInfo mri;
                mri.pMetric = pMetric;
                mri.measurementTypes.emplace_back("default");
                sinkInfo.reportMetrics.insert({pMetric->getName(), mri});
            }
        }
        sinkInfo.metricsChanged = false;
    }
}


// true if new, false if already existed
SinkInfo *MetricsReporter::addSink(IMetricSink *pSink)
{
    SinkInfo sinkInfo;
    sinkInfo.pSink = pSink;
    sinkInfo.metricsChanged = true;
    auto rc = sinks.insert({pSink->getName(), sinkInfo});
    return &(rc.first->second);
}


bool MetricsReporter::init(IPropertyTree *pGlobalMetricsTree, IPropertyTree *pComponentMetricsTree)
{
    bool rc = false;

    //
    // Process the global config (which is really just sinks)
    processSinks(pGlobalMetricsTree->getElements("sinks"));
    pGlobalMetricsTree->getProp("@prefix", globalPrefix);

    //
    // Process component config
    if (pComponentMetricsTree != nullptr)
    {
        pComponentMetricsTree->getProp("@prefix", componentPrefix);

        //
        // Process sinks for the component, which should include metrics to report
        processSinks(pComponentMetricsTree->getElements("sinks"));
    }
    return rc;
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


bool MetricsReporter::processSinks(IPropertyTreeIterator *pSinkIt)
{
    bool rc = true;
    for (pSinkIt->first(); pSinkIt->isValid() && rc; pSinkIt->next())
    {
        SinkInfo *pSinkInfo;
        IPropertyTree &sinkTree = pSinkIt->query();

        StringBuffer cfgSinkType, cfgLibName, cfgSinkName, cfgProcName;
        sinkTree.getProp("@type", cfgSinkType);  // this one is required
        sinkTree.getProp("@libname", cfgLibName);
        sinkTree.getProp("@name", cfgSinkName);
        sinkTree.getProp("@instance_proc", cfgProcName);

        std::string sinkName = cfgSinkName.isEmpty() ? "default" : cfgSinkName.str();

        //
        // If sink already registered, use it, otherwise it's new
        auto sinkIt = sinks.find(sinkName);
        if (sinkIt != sinks.end())
        {
            pSinkInfo = &(sinkIt->second);
        }
        else
        {
            const char *type = cfgLibName.isEmpty() ? cfgSinkType.str() : cfgLibName.str();
            IPropertyTree *pSinkSettings = sinkTree.getPropTree("./settings");
            IMetricSink *pSink = MetricSink::getSinkFromLib(type, cfgProcName.str(), sinkName.c_str(), pSinkSettings);
            if (pSink != nullptr)
            {
                pSinkInfo = addSink(pSink);
            }
        }

        //
        // Retrieve metrics to be reported
        if (pSinkInfo != nullptr)
        {
            //
            // Now add defined metrics if present
            IPropertyTreeIterator *pSinkMetricsIt = sinkTree.getElements("metrics");
            for (pSinkMetricsIt->first(); pSinkMetricsIt->isValid(); pSinkMetricsIt->next())
            {
                StringBuffer metricName, measurementType, description;
                pSinkMetricsIt->query().getProp("@name", metricName);
                pSinkMetricsIt->query().getProp("@measurement_type", measurementType);
                pSinkMetricsIt->query().getProp("@description", description);
                measurementType = measurementType.isEmpty() ? "default" : measurementType;
                SinkInfo::measurementInfo mi{std::string(metricName.str()), std::string(measurementType.str()), std::string(description.str())};
                pSinkInfo->reportMeasurements.emplace_back(mi);
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
