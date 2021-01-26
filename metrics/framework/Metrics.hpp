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

#ifdef METRICSDLL
#define METRICS_API DECL_EXPORT
#else
#define METRICS_API DECL_IMPORT
#endif

namespace hpccMetrics {

/*
 * Enumerates the type of the value in a measurement
 */
enum ValueType {
    NONE,
    STRING,
    LONG,
    INTEGER,
    DOUBLE,
    FLOAT,
    DATE,
    DATE_NANOS,
    BOOLEAN
} ;

/*
 * Enumerates the metric type.
 */
enum MetricType {
    COUNTER,
    GAUGE
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
        std::string getName() const { return name; }
        std::string getDescription() const { return description; }
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
    virtual std::string getName() const = 0;

    /*
     * Returns metric description
     */
    virtual std::string getDescription() const = 0;

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
class METRICS_API Metric : public IMetric
{
    public:
        virtual ~Metric() = default;
        std::string getName() const override { return name; }
        std::string getDescription() const override { return description; }
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

class CounterMetric : public Metric
{
    public:

        CounterMetric(const char *name, const char *description) :
                Metric{name, description, MetricType::GAUGE}  { }
        ~CounterMetric() override = default;
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
class GaugeMetric : public Metric {
    public:
        GaugeMetric(const char *name, const char *description) :
                Metric{name, description, MetricType::GAUGE}  { }

        /*
         * Update the value as indicated
         */
        void add(float val)
        {
            auto current = gaugeValue.load();
            while (!gaugeValue.compare_exchange_weak(current, current + val));
        }
        //void inc(float inc) { value += inc; }

        /*
         * Decrement the value
         */
        //void dec(float dec) { value -= dec; }

        /*
         * Set the value
         */
        void set(float val) { gaugeValue = val; }

        /*
         * Read the current value
         */
        float getValue() const { return gaugeValue; }

        /*
         * Get current gauge value
         */
        void getReportValues(MeasurementVector &mv) const override;


    protected:
        std::atomic<float> gaugeValue{0};
};


class MetricsReporter;
interface IMetricSink
{
    virtual void startCollection(MetricsReporter *pReporter) = 0;
    virtual void stopCollection() = 0;
    virtual std::string getName() const = 0;
    virtual std::string getType() const = 0;
};


extern "C" { typedef hpccMetrics::IMetricSink* (*getSinkInstance)(const char *, const IPropertyTree *pSettingsTree); }

class METRICS_API MetricSink : public IMetricSink
{
    public:
        virtual ~MetricSink() = default;
        std::string getName() const override { return name; }
        std::string getType() const override { return type; }
        static IMetricSink *getSinkFromLib(const char *type, const char *sinkName, const IPropertyTree *pSettingsTree);
        static IMetricSink *getSinkFromLib(const char *type, const char *getInstanceProcName, const char *sinkName, const IPropertyTree *pSettingsTree);

    protected:
        MetricSink(std::string _name, std::string _type) :
                name{std::move(_name)},
                type{std::move(_type)} { }

    protected:
        std::string name;
        std::string type;
        MetricsReporter *pReporter = nullptr;
};


struct SinkInfo
{
    IMetricSink *pSink = nullptr;             // ptr to the sink
    std::vector<std::string> reportMetrics;   // vector of metrics to report (empty for none)
};


class METRICS_API MetricsReporter
{
    public:
        MetricsReporter() = default;
        virtual ~MetricsReporter() = default;
        void init(IPropertyTree *pMetricsTree);

        /*
         * Add the metric. The metric name must be unique.
         */
        void addMetric(const std::shared_ptr<IMetric> &pMetric);
        void removeMetric(const std::string &metricName);
        void startCollecting();
        void stopCollecting();
        std::vector<std::shared_ptr<IMetric>> getReportMetrics(const std::string &sinkName);

    protected:
        bool initializeSinks(IPropertyTreeIterator *pSinkIt);

    protected:
        StringBuffer componentPrefix;
        StringBuffer globalPrefix;
        std::map<std::string, SinkInfo> sinks;
        std::map<std::string, std::weak_ptr<IMetric>> metrics;
        //std::map<std::string, std::shared_ptr<IMetric>> metrics;
        std::mutex reportMutex;
        std::mutex metricVectorMutex;
};



}
