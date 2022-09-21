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

static Singleton<MetricsManager> metricsManager;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    metricsManager.destroy();
}


HistogramMetric::HistogramMetric(const char *_name, const char *_desc, MetricUnits _units, const std::vector<__uint64> &_bucketLimits, const MetricMetaData &_metaData) :
    MetricBase(_name, _desc, MetricType::METRICS_HISTOGRAM, _units, _metaData)
{
    buckets.reserve(_bucketLimits.size());
    for (auto &b: _bucketLimits)
    {
        buckets.emplace_back(Bucket{b});
    }
}


void HistogramMetric::recordMeasurement(__uint64 measurement)
{
    CriticalBlock block(cs);
    sum += measurement;                // sum of all measurements
    findBucket(measurement).count++;   // count by buckets
}


std::vector<__uint64> HistogramMetric::queryHistogramValues() const
{
    std::vector<__uint64> histogramValues;
    histogramValues.reserve(buckets.size()+1);
    CriticalBlock block(cs);
    for (auto const &bucket: buckets)
    {
        histogramValues.push_back(bucket.count);
    }
    histogramValues.push_back(inf.count);
    return histogramValues;
}


std::vector<__uint64> HistogramMetric::queryHistogramBucketLimits() const
{
    std::vector<__uint64> limits;
    limits.reserve(buckets.size());
    for (auto const &bucket: buckets)
    {
        limits.emplace_back(bucket.limit);
    }
    return limits;
}


HistogramMetric::Bucket &HistogramMetric::findBucket(__uint64 measurement)
{
    for (auto &b: buckets)
    {
        if (measurement <= b.limit)
        {
            return b;
        }
    }
    return inf;
}


ScaledHistogramMetric::ScaledHistogramMetric(const char *_name, const char *_desc, MetricUnits _units, const std::vector<__uint64> &_bucketLimits, double _limitsToMeasurementUnitsScaleFactor, const MetricMetaData &_metaData) :
    HistogramMetric{_name, _desc, _units, _bucketLimits, _metaData}
{
    for (auto &b: buckets)
    {
        b.limit = (__uint64)((double)(b.limit) * _limitsToMeasurementUnitsScaleFactor);
    }
    outputScaleFactor = 1.0 / _limitsToMeasurementUnitsScaleFactor;
}


std::vector<__uint64> ScaledHistogramMetric::queryHistogramBucketLimits() const
{
    std::vector<__uint64> limits;
    limits.reserve(buckets.size());
    for (auto const &bucket: buckets)
    {
        limits.emplace_back(bucket.limit * outputScaleFactor);
    }
    return limits;
}


MetricsManager &hpccMetrics::queryMetricsManager()
{
    return *metricsManager.query([] { return new MetricsManager; });
}


MetricsManager::~MetricsManager()
{
    for (auto const &sinkIt : sinks)
    {
        sinkIt.second->pSink->stopCollection();
        delete sinkIt.second->pSink;
    }
}


void MetricsManager::init(IPropertyTree *pMetricsTree)
{
    Owned<IPropertyTreeIterator> sinkElementsIt = pMetricsTree->getElements("sinks");
    initializeSinks(sinkElementsIt);
}


// returns true if the metric was unique when added, false if an existing metric was found.
bool MetricsManager::addMetric(const std::shared_ptr<IMetric> &pMetric)
{
    bool rc = false;
    std::unique_lock<std::mutex> lock(metricVectorMutex);
    std::string name = pMetric->queryName();
    auto metaData = pMetric->queryMetaData();
    for (auto &metaDataIt: metaData)
    {
        name.append(".").append(metaDataIt.value);
    }

    auto it = metrics.find(name);
    if (it == metrics.end())
    {
        metrics.insert({name, pMetric});
        rc = true;
    }
    else
    {
        // If there is a match only report an error if the metric has not been destroyed in the meantime
        auto match = it->second.lock();
        if (match)
        {
#ifdef _DEBUG
            throw MakeStringException(MSGAUD_operator, "addMetric - Attempted to add duplicate named metric with name '%s'", name.c_str());
#else
            OERRLOG("addMetric - Adding a duplicate named metric '%s', old metric replaced", name.c_str());
#endif
        }
        else
        {
            rc = true;  // old metric no longer present, so it's considered unique
        }
        it->second = pMetric;
    }
    return rc;
}


void MetricsManager::startCollecting()
{
    for (auto const &sinkIt : sinks)
    {
        sinkIt.second->pSink->startCollection(this);
    }
}


void MetricsManager::stopCollecting()
{
    for (auto const &sinkIt : sinks)
    {
        sinkIt.second->pSink->stopCollection();
    }
}


std::vector<std::shared_ptr<IMetric>> MetricsManager::queryMetricsForReport(const std::string &sinkName)
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


void MetricsManager::initializeSinks(IPropertyTreeIterator *pSinkIt)
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


MetricSink *MetricsManager::getSinkFromLib(const char *type, const char *sinkName, const IPropertyTree *pSettingsTree)
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

// Method for use when testing
void MetricsManager::addSink(MetricSink *pSink, const char *name)
{
    //
    // Add the sink if it does not already exist, otherwise delete the sink because
    // we are taking ownership.
    auto sinkIt = sinks.find(name);
    if (sinkIt == sinks.end())
    {
        sinks.insert({std::string(name), std::unique_ptr<SinkInfo>(new SinkInfo(pSink))});
    }
    else
    {
        delete pSink;
    }
}


const char *MetricsManager::queryUnitsString(MetricUnits units) const
{
    switch (units)
    {
    case METRICS_UNITS_COUNT:       return "count";
    case METRICS_UNITS_NANOSECONDS: return "ns";
    case METRICS_UNITS_BYTES:       return "bytes";
    }
    return nullptr;
}


PeriodicMetricSink::PeriodicMetricSink(const char *_name, const char *_type, const IPropertyTree *_pSettingsTree) :
    MetricSink(_name, _type),
    collectionPeriodSeconds{60}
{
    if (_pSettingsTree->hasProp("@period"))
    {
        collectionPeriodSeconds = _pSettingsTree->getPropInt("@period");
    }
}


PeriodicMetricSink::~PeriodicMetricSink()
{
    if (isCollecting)
    {
        doStopCollecting();
    }
}


void PeriodicMetricSink::startCollection(MetricsManager *_pManager)
{
    pManager = _pManager;
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


std::shared_ptr<CounterMetric> hpccMetrics::registerCounterMetric(const char *name, const char* desc, MetricUnits units, const MetricMetaData &metaData)
{
    return createMetricAndAddToManager<CounterMetric>(name, desc, units, metaData);
}


std::shared_ptr<GaugeMetric> hpccMetrics::registerGaugeMetric(const char *name, const char* desc, MetricUnits units, const MetricMetaData &metaData)
{
    return createMetricAndAddToManager<GaugeMetric>(name, desc, units, metaData);
}


std::shared_ptr<GaugeMetricFromCounters> hpccMetrics::registerGaugeFromCountersMetric(const char *name, const char* desc, MetricUnits units,
                                                                         const std::shared_ptr<CounterMetric> &pBeginCounter, const std::shared_ptr<CounterMetric> &pEndCounter,
                                                                         const MetricMetaData &metaData)
{
    //
    // std::make_shared is not used so that there are separate memory allocations for the object (metric) and the shared pointer control structure. This ensures
    // the vmt for the control structure does not live in memory allocated by a shared object (dll) that could be unloaded. If make_shared is used and a shared
    // object is unloaded, the vmt is also deleted which causes unpredictable results during weak pointer access.
    std::shared_ptr<GaugeMetricFromCounters> pMetric = std::shared_ptr<GaugeMetricFromCounters>(new GaugeMetricFromCounters(name, desc, units, pBeginCounter, pEndCounter, metaData));
    queryMetricsManager().addMetric(pMetric);
    return pMetric;
}


std::shared_ptr<HistogramMetric> hpccMetrics::registerHistogramMetric(const char *name, const char* desc, MetricUnits units, const std::vector<__uint64> &bucketLimits, const MetricMetaData &metaData)
{
    std::shared_ptr<HistogramMetric> pMetric = std::shared_ptr<HistogramMetric>(new HistogramMetric(name, desc, units, bucketLimits, metaData));
    queryMetricsManager().addMetric(pMetric);
    return pMetric;
}


std::shared_ptr<ScaledHistogramMetric> hpccMetrics::registerScaledHistogramMetric(const char *name, const char* desc, MetricUnits units, const std::vector<__uint64> &bucketLimits,
                                                                               double limitsToMeasurementUnitsScaleFactor, const MetricMetaData &metaData)
{
    std::shared_ptr<ScaledHistogramMetric> pMetric = std::shared_ptr<ScaledHistogramMetric>(new ScaledHistogramMetric(name, desc, units, bucketLimits, limitsToMeasurementUnitsScaleFactor, metaData));
    queryMetricsManager().addMetric(pMetric);
    return pMetric;
}


std::shared_ptr<ScaledHistogramMetric> hpccMetrics::registerCyclesToNsScaledHistogramMetric(const char *name, const char* desc, const std::vector<__uint64> &bucketLimits, const MetricMetaData &metaData)
{
    double nsToCyclesScaleFactor = 1.0 / getCycleToNanoScale();
    std::shared_ptr<ScaledHistogramMetric> pMetric = std::shared_ptr<ScaledHistogramMetric>(new ScaledHistogramMetric(name, desc, METRICS_UNITS_NANOSECONDS, bucketLimits, nsToCyclesScaleFactor, metaData));
    queryMetricsManager().addMetric(pMetric);
    return pMetric;
}
