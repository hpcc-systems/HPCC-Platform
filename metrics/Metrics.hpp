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
    CUSTOM,
    COUNTER,
    GAUGE
};

interface IMetric;

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
        const IMetric *getMetric() const { return pMetric; }
        virtual std::string valueToString() const = 0;

    protected:
        MeasurementBase(const IMetric *_pMetric, std::string _name, ValueType _type, std::string _description) :
                name{std::move(_name)},
                description{std::move(_description)},
                valueType{_type},
                pMetric{_pMetric} {}
        const IMetric *pMetric;
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
        Measurement(const IMetric *pMetric, const std::string &name, const std::string &description, ValueType valueType, T val) :
                MeasurementBase(pMetric, name, valueType, description),
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
     * Prepares the metric to provide measurements by saving any necessary state.
     * The context value isolates saved state from other contexts so that
     * the metric supports multiple clients requesting measurements. The same
     * context value used for this call shall be used when calling the
     * getMeasurement method.
     *
     * Return value indicates if metric is prepared to return measurements.
     * getMeasurement shall only be called after a call to this method returns true>
     *
     */
    virtual bool prepareMeasurements(const std::string &context) = 0;

    /*
     * Take the requested measurement and using the provided information push
     * a measurement to the passed vector.
     */
    virtual void getMeasurement(const std::string &type, const std::string &measurementReportName,
                                const std::string &context, MeasurementVector &measurements) = 0;
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


/*
 * Metric used to count the occurrences of a specific event.
 */
class CounterMetric : public Metric
{
    public:
        CounterMetric(const char *name, const char *description) :
                Metric{name, description, MetricType::COUNTER}  { }

        /*
         * Increment the number of event occurrences by the indicated value.
         */
        void inc(uint32_t val) { count.fetch_add(val, std::memory_order_relaxed);  }

        bool prepareMeasurements(const std::string &context) override;

        /*
         * get a measurement. Supported measurements are
         * - count - The current count value  (the default returned if measType == "")
         * - resetting_count - The count since the last time read
         * - rate - The number of events per second since the last read
         */
        void getMeasurement(const std::string &measType, const std::string &measurementReportName, const std::string &context, MeasurementVector &measurements) override;


    protected:
        struct metricState {
            uint32_t lastValue = 0;
            uint32_t curValue = 0;
            std::chrono::time_point<std::chrono::high_resolution_clock> lastCollectedAt;
            std::chrono::time_point<std::chrono::high_resolution_clock> curCollectedAt;
        };

        std::atomic<uint32_t> count{0};
        std::map<std::string, metricState> contextState;
};


/*
 * Metric used to track the current state of some internal measurement.
 */
template<typename T>
class GaugeMetric : public Metric {
    public:
        GaugeMetric(const char *name, const char *description) :
                Metric{name, description, MetricType::GAUGE}  { }

        /*
         * Increment the value as indicated
         */
        void inc(T inc) { value += inc; }

        /*
         * Decrement the value
         */
        void dec(T dec) { value -= dec; }

        /*
         * Set the value
         */
        void set(T val) { value = val; }

        /*
         * No preparation is required since the measurement is the current value when read.
         */
        bool prepareMeasurements(const std::string &context) override { return true; }
        void getMeasurement(const std::string &type, const std::string &measurementReportName, const std::string &context, MeasurementVector &measurements) override;

    protected:
        std::atomic<T> value{0};
};

template<typename T>
void GaugeMetric<T>::getMeasurement(const std::string &type, const std::string &measurementReportName, const std::string &context, MeasurementVector &measurements)
{
    std::shared_ptr<MeasurementBase> pMeasurement;
    std::string reportName(measurementReportName);
    reportName.append(".").append(type);
    if (type == "reading" || type == "default" || type.empty())
    {
        pMeasurement = std::make_shared<Measurement<T>>(this, reportName, description, ValueType::FLOAT, value.load());
        measurements.emplace_back(pMeasurement);
    }
    // else throw ?
}

class MetricsReporter;
interface IMetricSink
{
    virtual void startCollection(MetricsReporter *pReporter) = 0;
    virtual void stopCollection() = 0;
    virtual std::string getName() const = 0;
    virtual std::string getType() const = 0;
    virtual void reportMeasurements(const MeasurementVector &measurements) = 0;
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


struct MetricReportInfo
{
    std::weak_ptr<IMetric> pMetric;
    std::vector<std::string> measurementTypes;
};

struct SinkInfo
{
    IMetricSink *pSink = nullptr;
    struct measurementInfo
    {
        std::string metricName;
        std::string measurementType;
        std::string description;
    };
    bool metricsChanged;
    std::map<std::string, MetricReportInfo>  reportMetrics;
    std::vector<measurementInfo> reportMeasurements;
};


class METRICS_API MetricsReporter
{
    public:
        MetricsReporter() = default;
        virtual ~MetricsReporter() = default;
        bool init(IPropertyTree *pGlobalMetricsTree, IPropertyTree *pComponentMetricsTree);
        void addMetric(const std::shared_ptr<IMetric> &pMetric);
        void removeMetric(const std::string &metricName);
        void startCollecting();
        void stopCollecting();
        void collectMeasurements(const std::string &context);
        SinkInfo *addSink(IMetricSink *pSink);

    protected:
        bool processSinks(IPropertyTreeIterator *pSinkIt);
        void updateSinkMeasurementsToReport(SinkInfo &sinkInfo);

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
