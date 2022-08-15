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

#pragma once

#include <string>
#include <utility>
#include <vector>
#include <map>
#include <atomic>
#include <chrono>
#include <mutex>
#include <memory>
#include <thread>
#include <unordered_set>
#include "jiface.hpp"
#include "jptree.hpp"
#include "platform.h"
#include "jstatcodes.h"
#include "jatomic.hpp"


namespace hpccMetrics {

class MetricsManager;

MetricsManager jlib_decl &queryMetricsManager();

/*
 * Enumerates the metric type.
 */
enum MetricType
{
    METRICS_COUNTER,
    METRICS_GAUGE,
    METRICS_HISTOGRAM
};


struct MetricMetaDataItem
{
    MetricMetaDataItem(const char *_key, const char *_value)
        : key{_key}, value{_value} {}
    std::string key;
    std::string value;
};


typedef std::vector<MetricMetaDataItem> MetricMetaData;

/*
 * IMetric
 *
 * Interface defining a metric
 */
interface IMetric
{
    virtual ~IMetric() = default;

    /*
     * Returns the metric name
     */
    virtual const std::string &queryName() const = 0;

    /*
     * Returns metric description
     */
    virtual const std::string &queryDescription() const = 0;

    /*
     * Returns the metric type.
     */
    virtual MetricType queryMetricType() const = 0;

    /*
     * Get current measurement
     */
    virtual __uint64 queryValue() const = 0;

    /*
     * Query the meta data for the metric
     */
    virtual const MetricMetaData &queryMetaData() const = 0;

    /*
     * Get the units for the metric
     */
    virtual StatisticMeasure queryUnits() const = 0;

    /*
     * Query histogram values (for histogram metrics)
     */
    virtual std::vector<__uint64> queryHistogramValues() const = 0;

    /*
     * Query histogram labels
     */
    virtual std::vector<std::string> queryHistogramLabels() const = 0;
};


/*
 * Concrete base class implementation of the IMetric interface. All metrics inherit
 * from this class.
*/
class jlib_decl MetricBase : public IMetric
{
public:
    virtual ~MetricBase() = default;
    virtual const std::string &queryName() const override { return name; }
    virtual const std::string &queryDescription() const override { return description; }
    virtual MetricType queryMetricType() const override { return metricType; }
    const MetricMetaData &queryMetaData() const override { return metaData; }
    StatisticMeasure queryUnits() const override { return units; }
    virtual std::vector<__uint64> queryHistogramValues() const override { return {}; }
    virtual std::vector<std::string> queryHistogramLabels() const override { return {}; }


protected:
    // No one should be able to create one of these
    MetricBase(const char *_name, const char *_desc, MetricType _metricType, StatisticMeasure _units, const MetricMetaData &_metaData) :
        name{_name},
        description{_desc},
        metricType{_metricType},
        units{_units},
        metaData{_metaData} { }

protected:
    std::string name;
    std::string description;
    MetricType metricType;
    StatisticMeasure units;
    MetricMetaData metaData;
};


class jlib_decl MetricVal : public MetricBase
{
public:
    virtual __uint64 queryValue() const override { return value; }

protected:
    MetricVal(const char *name, const char *desc, MetricType metricType, StatisticMeasure _units, const MetricMetaData &_metaData) :
        MetricBase(name, desc, metricType, _units, _metaData) {}

    RelaxedAtomic<__uint64> value{0};
};


/*
 * Metric used to count events. Count is a monotonically increasing value
 */
class jlib_decl CounterMetric : public MetricVal
{
public:
    CounterMetric(const char *name, const char *description, StatisticMeasure _units, const MetricMetaData &_metaData = MetricMetaData()) :
        MetricVal{name, description, MetricType::METRICS_COUNTER, _units, _metaData}  { }
    void inc(uint64_t val)
    {
        value.fetch_add(val);
    }

    void fastInc(uint16_t val)
    {
        value.fastAdd(val);
    }
};


/*
 * Metric used to track the current state of some internal measurement.
 */
class jlib_decl GaugeMetric : public MetricVal
{
public:
    GaugeMetric(const char *name, const char *description, StatisticMeasure _units, const MetricMetaData &_metaData = MetricMetaData()) :
        MetricVal{name, description, MetricType::METRICS_GAUGE, _units, _metaData}  { }

    void adjust(int64_t delta)
    {
        value += delta;
    }

    void fastAdjust(int64_t delta)
    {
        value.fastAdd(delta);
    }

    /*
     * Set the value
     */
    void set(int64_t val)
    {
        value = val;
    }
};


class jlib_decl GaugeMetricFromCounters : public MetricVal
{
public:
    GaugeMetricFromCounters(const char *name, const char *description, StatisticMeasure _units,
                            const std::shared_ptr<CounterMetric> &_pBeginCounter, const std::shared_ptr<CounterMetric> &_pEndCounter,
                            const MetricMetaData &_metaData = MetricMetaData()) :
        MetricVal{name, description, MetricType::METRICS_GAUGE, _units, _metaData},
        pBeginCounter{_pBeginCounter},
        pEndCounter{_pEndCounter}
    {
        assert(pBeginCounter->queryUnits() == pEndCounter->queryUnits());
    }

    virtual __uint64 queryValue() const override
    {
        auto endValue = pEndCounter->queryValue();
        return pBeginCounter->queryValue() - endValue;
    }

protected:
    std::shared_ptr<CounterMetric> pBeginCounter;
    std::shared_ptr<CounterMetric> pEndCounter;
};


template<typename T>
class CustomMetric : public MetricBase
{
public:
    CustomMetric(const char *name, const char *desc, MetricType metricType, T &_value, StatisticMeasure _units, const MetricMetaData &_metaData = MetricMetaData()) :
        MetricBase(name, desc, metricType, _units, _metaData),
        value{_value} { }

    virtual __uint64 queryValue() const override
    {
        return static_cast<__uint64>(value);
    }

protected:
    T &value;
};


struct BucketDef
{
    std::string label;
    __uint64 limit;
};


struct Bucket
{
    Bucket(const std::string &_label, __uint64 _limit) :
        label{_label}, limit{_limit}, count{0} {}
    std::string label;
    __uint64 limit;
    __uint64 count;
};


class jlib_decl HistogramMetric : public MetricBase
{
public:
    HistogramMetric(const char *name, const char *desc, StatisticMeasure _units, const std::vector<BucketDef> &bucketDefs, const MetricMetaData &_metaData = MetricMetaData());

    virtual __uint64 queryValue() const override
    {
        return sum;
    }

    void recordMeasurement(__uint64 measurement);
    virtual std::vector<__uint64> queryHistogramValues() const override;
    virtual std::vector<std::string> queryHistogramLabels() const override;

protected:
    Bucket &findBucket(__uint64 measurement);

protected:
    std::vector<Bucket> buckets;
    mutable CriticalSection cs;
    Bucket inf{"inf", 0};
    __uint64 sum{0};
};


class jlib_decl MetricSink
{
public:
    virtual ~MetricSink() = default;
    virtual void startCollection(MetricsManager *pManager) = 0;
    virtual void stopCollection() = 0;
    const std::string &queryName() const { return name; }
    const std::string &queryType() const { return type; }

protected:
    MetricSink(const char *_name, const char *_type) :
        name{_name},
        type{_type} { }

protected:
    std::string name;
    std::string type;
    MetricsManager *pManager = nullptr;
};


class jlib_decl PeriodicMetricSink : public MetricSink
{
public:
    virtual ~PeriodicMetricSink() override;
    virtual void startCollection(MetricsManager *pManager) override;
    virtual void stopCollection() override;

protected:
    explicit PeriodicMetricSink(const char *name, const char *type, const IPropertyTree *pSettingsTree);
    virtual void prepareToStartCollecting() = 0;
    virtual void collectingHasStopped() = 0;
    virtual void doCollection() = 0;
    void collectionThread();
    void doStopCollecting();

protected:
    unsigned collectionPeriodSeconds;
    std::thread collectThread;
    std::atomic<bool> stopCollectionFlag{false};
    bool isCollecting = false;
    Semaphore waitSem;
};


extern "C" { typedef hpccMetrics::MetricSink* (*getSinkInstance)(const char *, const IPropertyTree *pSettingsTree); }

struct SinkInfo
{
    explicit SinkInfo(MetricSink *_pSink) : pSink{_pSink} {}
    MetricSink *pSink = nullptr;             // ptr to the sink
    std::vector<std::string> reportMetrics;   // vector of metrics to report (empty for none)
};

class jlib_decl MetricsManager
{
public:
    MetricsManager() {}
    ~MetricsManager();
    void init(IPropertyTree *pMetricsTree);
    void addSink(MetricSink *pSink, const char *name);  // for use by unit tests
    bool addMetric(const std::shared_ptr<IMetric> &pMetric);
    void startCollecting();
    void stopCollecting();
    std::vector<std::shared_ptr<IMetric>> queryMetricsForReport(const std::string &sinkName);
    const char * queryUnitsString(StatisticMeasure units) const;

protected:
    void initializeSinks(IPropertyTreeIterator *pSinkIt);
    static MetricSink *getSinkFromLib(const char *type, const char *sinkName, const IPropertyTree *pSettingsTree);

protected:
    StringBuffer componentPrefix;
    StringBuffer globalPrefix;
    std::map<std::string, std::unique_ptr<SinkInfo>> sinks;
    std::map<std::string, std::weak_ptr<IMetric>> metrics;
    std::mutex metricVectorMutex;
};

jlib_decl std::shared_ptr<CounterMetric> registerCounterMetric(const char *name, const char* desc, StatisticMeasure units, const MetricMetaData &metaData = MetricMetaData());
jlib_decl std::shared_ptr<GaugeMetric> registerGaugeMetric(const char *name, const char* desc, StatisticMeasure units, const MetricMetaData &metaData = MetricMetaData());
jlib_decl std::shared_ptr<GaugeMetricFromCounters> registerGaugeFromCountersMetric(const char *name, const char* desc, StatisticMeasure units,
                                                                         const std::shared_ptr<CounterMetric> &pBeginCounter, const std::shared_ptr<CounterMetric> &pEndCounter,
                                                                         const MetricMetaData &metaData = MetricMetaData());

//
// Convenience function templates to create metrics and add to the manager
template <typename T>
std::shared_ptr<T> createMetricAndAddToManager(const char *name, const char* desc, StatisticMeasure units, const MetricMetaData &metaData = MetricMetaData())
{
    //
    // std::make_shared is not used so that there are separate memory allocations for the object (metric) and the shared pointer control structure. This ensures
    // the vmt for the control structure does not live in memory allocated by a shared object (dll) that could be unloaded. If make_shared is used and a shared
    // object is unloaded, the vmt is also deleted which causes unpredictable results during weak pointer access.
    std::shared_ptr<T> pMetric = std::shared_ptr<T>(new T(name, desc, units, metaData));
    queryMetricsManager().addMetric(pMetric);
    return pMetric;
}

template <typename T>
std::shared_ptr<CustomMetric<T>> registerCustomMetric(const char *name, const char *desc, MetricType metricType, T &value, StatisticMeasure units, const MetricMetaData &metaData = MetricMetaData())
{
    //
    // std::make_shared is not used so that there are separate memory allocations for the object (metric) and the shared pointer control structure. This ensures
    // the vmt for the control structure does not live in memory allocated by a shared object (dll) that could be unloaded. If make_shared is used and a shared
    // object is unloaded, the vmt is also deleted which causes unpredictable results during weak pointer access.
    std::shared_ptr<CustomMetric<T>> pMetric = std::shared_ptr<CustomMetric<T>>(new CustomMetric<T>(name, desc, metricType, value, units, metaData));
    queryMetricsManager().addMetric(pMetric);
    return pMetric;
}


class jlib_decl ScopedGaugeUpdater
{
public:
    explicit ScopedGaugeUpdater(GaugeMetric &_pGauge, int64_t _amount=1)
        : gauge{_pGauge}, amount{_amount}
    {
        gauge.adjust(amount);
    }
    ScopedGaugeUpdater(const ScopedGaugeUpdater&) = delete;
    ScopedGaugeUpdater(ScopedGaugeUpdater&) = delete;
    ScopedGaugeUpdater& operator=(const ScopedGaugeUpdater&) = delete;
    ScopedGaugeUpdater& operator=(ScopedGaugeUpdater&) = delete;
    ~ScopedGaugeUpdater()
    {
        gauge.adjust(-amount);
    }

protected:
    GaugeMetric &gauge;
    int64_t amount;
};


class jlib_decl ScopedGaugeDecrementer
{
public:
    explicit ScopedGaugeDecrementer(GaugeMetric &_pGauge, int64_t _amount=1)
        : gauge{_pGauge}, amount{_amount}
    { }
    ScopedGaugeDecrementer(const ScopedGaugeDecrementer&) = delete;
    ScopedGaugeDecrementer(ScopedGaugeDecrementer&&) = delete;
    ScopedGaugeDecrementer& operator=(const ScopedGaugeDecrementer&) = delete;
    ScopedGaugeDecrementer& operator=(ScopedGaugeDecrementer&&) = delete;

    ~ScopedGaugeDecrementer()
    {
        gauge.adjust(-amount);
    }

protected:
    GaugeMetric &gauge;
    int64_t amount;
};

}
