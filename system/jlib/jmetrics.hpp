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
#include <unordered_set>
#include "jiface.hpp"
#include "jptree.hpp"


namespace hpccMetrics {

class MetricsReporter;

MetricsReporter jlib_decl &queryMetricsReporter();

/*
 * Enumerates the metric type.
 */
enum MetricType
{
    METRICS_COUNTER,
    METRICS_GAUGE
};


/*
 * IMetric
 *
 * Interface defining a metric
 */
interface IMetric
{
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
    virtual uint64_t queryValue() const = 0;
};


/*
 * Concrete base class implementation of the IMetric interface. All metrics inherit
 * from this class.
*/
class jlib_decl Metric : public IMetric
{
public:
    virtual ~Metric() = default;
    const std::string &queryName() const override { return name; }
    const std::string &queryDescription() const override { return description; }
    MetricType queryMetricType() const override { return metricType; }
    uint64_t queryValue() const override { return value; }

protected:
    // No one should be able to create one of these
    Metric(const char *_name, const char *_desc, MetricType _metricType) :
            name{_name},
            description{_desc},
            metricType{_metricType} { }

protected:
    std::string name;
    std::string description;
    MetricType metricType;
    std::atomic<uint64_t> value{0};
};

/*
 * Metric used to count events. Count is a monotonically increasing value
 */
class jlib_decl CounterMetric : public Metric
{
public:
    CounterMetric(const char *name, const char *description) :
            Metric{name, description, MetricType::METRICS_COUNTER}  { }
    void inc(uint64_t val)
    {
        value.fetch_add(val);
    }
};


/*
 * Metric used to track the current state of some internal measurement.
 */
class jlib_decl GaugeMetric : public Metric
{
public:
    GaugeMetric(const char *name, const char *description) :
        Metric{name, description, MetricType::METRICS_GAUGE}  { }

    /*
     * Update the value as indicated
     */
    void add(int64_t delta)
    {
        value += delta;
    }

    /*
     * Set the value
     */
    void set(int64_t val)
    {
        value = val;
    }
};



class jlib_decl MetricSink
{
public:
    virtual ~MetricSink() = default;
    virtual void startCollection(MetricsReporter *pReporter) = 0;
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
    MetricsReporter *pReporter = nullptr;
};

extern "C" { typedef hpccMetrics::MetricSink* (*getSinkInstance)(const char *, const IPropertyTree *pSettingsTree); }

struct SinkInfo;

class jlib_decl MetricsReporter
{
public:
    MetricsReporter() = default;
    ~MetricsReporter();
    void init(IPropertyTree *pMetricsTree);
    void addMetric(const std::shared_ptr<IMetric> &pMetric);
    void startCollecting();
    void stopCollecting();
    std::vector<std::shared_ptr<IMetric>> queryMetricsForReport(const std::string &sinkName);

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


//
// Convenience function template to create a metric and add it to the reporter
template <typename T>
std::shared_ptr<T> createMetric(const char *name, const char* desc)
{
    std::shared_ptr<T> pMetric = std::make_shared<T>(name, desc);
    queryMetricsReporter().addMetric(pMetric);
    return pMetric;
}

}
