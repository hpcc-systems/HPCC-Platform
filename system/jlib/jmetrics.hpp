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
class MetricsRegistry;

bool jlib_decl isMetricsInitialized();
bool jlib_decl isMetricsInitialized(const char *);
MetricsReporter jlib_decl &getMetricsReporter();
MetricsReporter jlib_decl &getMetricsReporter(const char *);
MetricsRegistry jlib_decl &getMetricsRegistry();
bool jlib_decl initializeMetrics();

/*
 * Enumerates the type of the value in a measurement
 */
enum ValueType
{
    METRICS_NONE,
    METRICS_STRING,
    METRICS_LONG,
    METRICS_INTEGER,
    METRICS_DOUBLE,
    METRICS_FLOAT,
    METRICS_DATE,
    METRICS_DATE_NANOS,
    METRICS_BOOLEAN
} ;

/*
 * Enumerates the metric type.
 */
enum MetricType
{
    METRICS_COUNTER,
    METRICS_GAUGE
};

/*
 * MeasurementBase
 *
 * Base class representing a measurement from a metric. Base class allows creating a vector
 * of measurements. Class not intended to be instantiated.
 */
class MeasurementBase
{
    public:
        const std::string &getName() const { return name; }
        const std::string &getDescription() const { return description; }
        ValueType getValueType() { return(valueType); }
        virtual std::string valueToString() const = 0;

    protected:
        MeasurementBase(std::string _name, ValueType _type, std::string _description) :
                name{std::move(_name)},
                description{std::move(_description)},
                valueType{_type} {}
        std::string name;
        std::string description;
        ValueType valueType;
};

/*
 * Measurement
 *
 * Templated class that holds a measurement from a metric.
 */
template<typename T>
class Measurement : public MeasurementBase
{
    public:
        Measurement(const std::string &name, const std::string &description, ValueType valueType, T val) :
                MeasurementBase(name, valueType, description),
                value{val}  { }
        std::string valueToString() const override { return std::to_string(value); }
        T getValue() const;

    protected:
        T value;
};

/*
 * Templated method of Measurement class giving the caller direct access to the raw measurement value
 */
template <typename T>
T Measurement<T>::getValue() const { return value; }


/*
 * Vector of measurements
 */
typedef std::vector<std::shared_ptr<MeasurementBase>> MeasurementVector;


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
    virtual const std::string &getName() const = 0;

    /*
     * Returns metric description
     */
    virtual const std::string &getDescription() const = 0;

    /*
     * Returns the metric type.
     */
    virtual MetricType getMetricType() const = 0;

    /*
     * Get report values
     */
    virtual void getReportValues(MeasurementVector &mv) const = 0;
};


/*
 * Concrete base class implementation of the IMetric interface. All metrics inherit
 * from this class.
*/
class jlib_decl Metric : public IMetric
{
    public:
        virtual ~Metric() = default;
        const std::string &getName() const override { return name; }
        const std::string &getDescription() const override { return description; }
        MetricType getMetricType() const override { return metricType; }

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
};

/*
 * Metric used to count events. Count is a monotonically increasing value
 */
class jlib_decl CounterMetric : public Metric
{
    public:
        CounterMetric(const char *name, const char *description) :
                Metric{name, description, MetricType::METRICS_COUNTER}  { }
        void inc(uint32_t val)
        {
            count.fetch_add(val);
        }

        void getReportValues(MeasurementVector &mv) const override;

    protected:
        std::atomic<uint32_t> count{0};
};


/*
 * Metric used to track the current state of some internal measurement.
 */
class jlib_decl GaugeMetric : public Metric {
    public:
        GaugeMetric(const char *name, const char *description) :
                Metric{name, description, MetricType::METRICS_GAUGE}  { }

        /*
         * Update the value as indicated
         */
        void add(int32_t delta)
        {
            gaugeValue += delta;
        }

        /*
         * Set the value
         */
        void set(int32_t val) { gaugeValue = val; }

        /*
         * Read the current value
         */
        int32_t getValue() const { return gaugeValue; }

        /*
         * Get current gauge value
         */
        void getReportValues(MeasurementVector &mv) const override;

    protected:
        std::atomic<int32_t> gaugeValue{0};
};



class jlib_decl MetricSink
{
    public:
        virtual ~MetricSink() = default;
        virtual void startCollection(MetricsReporter *pReporter) = 0;
        virtual void stopCollection() = 0;
        const std::string &getName() const { return name; }
        const std::string &getType() const { return type; }

    protected:
        MetricSink(std::string _name, std::string _type) :
                name{std::move(_name)},
                type{std::move(_type)} { }

    protected:
        std::string name;
        std::string type;
        MetricsReporter *pReporter = nullptr;
};

extern "C" { typedef hpccMetrics::MetricSink* (*getSinkInstance)(const char *, const IPropertyTree *pSettingsTree); }

struct SinkInfo
{
    explicit SinkInfo(MetricSink *_pSink) : pSink{_pSink} {}
    MetricSink *pSink = nullptr;             // ptr to the sink
    std::vector<std::string> reportMetrics;   // vector of metrics to report (empty for none)
};


class jlib_decl MetricsReporter
{
    public:
        MetricsReporter() = default;
        ~MetricsReporter();
        void init(IPropertyTree *pMetricsTree);
        void addMetric(const std::shared_ptr<IMetric> &pMetric);
        void removeMetric(const std::string &metricName);
        void startCollecting();
        void stopCollecting();
        std::vector<std::shared_ptr<IMetric>> getReportMetrics(const std::string &sinkName);

    protected:
        bool initializeSinks(IPropertyTreeIterator *pSinkIt);
        static MetricSink *getSinkFromLib(const char *type, const char *sinkName, const IPropertyTree *pSettingsTree);

    protected:
        StringBuffer componentPrefix;
        StringBuffer globalPrefix;
        std::map<std::string, SinkInfo> sinks;
        std::map<std::string, std::weak_ptr<IMetric>> metrics;
        std::mutex metricVectorMutex;
};


class jlib_decl MetricsRegistry
{
    public:
        MetricsRegistry() = default;
        ~MetricsRegistry() = default;
        void addMetric(const std::shared_ptr<IMetric> &pMetric);
        void removeMetric(const std::string &name);
        template<class T> std::shared_ptr<T> getMetric(const std::string &name) const;

    private:
        std::map<std::string, std::shared_ptr<IMetric>> metrics;
};


template <class T>
std::shared_ptr<T> MetricsRegistry::getMetric(const std::string &name) const
{
    return std::dynamic_pointer_cast<T>(metrics.find(name)->second);
}

}
