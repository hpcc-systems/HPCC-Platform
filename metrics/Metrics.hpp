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


namespace hpccMetrics
{

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


enum MetricType {
    CUSTOM,
    EVENTCOUNT,
    COUNTER,
    RATE,
    GAUGE
};

class IMetric;
class MeasurementBase
{
    public:
        MeasurementBase(const IMetric *_pMetric, const std::string &_reportName) :
            reportName{_reportName},
            pMetric{pMetric} {}
        const std::string &getReportName() const { return reportName; }
        const IMetric *getMetric() const { return pMetric; }
        virtual std::string valueToString() const = 0;

    protected:
        const IMetric *pMetric;
        const std::string &reportName;
};


template<typename T>
class Measurement : public MeasurementBase
{
    public:
        Measurement(const IMetric *pMetric, const std::string &reportName, T _value) :
            MeasurementBase(pMetric, reportName),
            value{_value}  { }
        std::string valueToString() const override { return std::to_string(value); }
        T getValue() const;

    protected:
        T value;
};

template <typename T>
T Measurement<T>::getValue() const { return value; }


typedef std::vector<std::shared_ptr<MeasurementBase>> MeasurementVector;

interface IMetric
{
    virtual std::string getName() const = 0;
    virtual std::string getDescription() const = 0;
    virtual MetricType getMetricType() const = 0;
    virtual void setReportingName(const std::string &name) = 0;
    virtual std::string getReportingName() const = 0;
    virtual ValueType getValueType() const = 0;
    virtual bool isInMetricSet() const = 0;
    virtual void setInMetricSet(bool val) = 0;
    virtual void init() = 0;
    virtual void collect(MeasurementVector &values) = 0;
};


class METRICS_API Metric : public IMetric
{
    public:
        virtual ~Metric() = default;
        std::string getName() const override { return name; }
        std::string getDescription() const override { return description; }
        void setReportingName(const std::string &_name) override { reportingName = _name; }
        std::string getReportingName() const override { return reportingName; }
        ValueType getValueType() const override { return valueType; }
        MetricType getMetricType() const override { return metricType; }
        bool isInMetricSet() const override { return inMetricSet; }
        void setInMetricSet(bool val) override { inMetricSet = val; }
        void init() override { }

    protected:
        // No one should be able to create one of these
        Metric(const char *_name, const char *_desc, ValueType _type, MetricType _metricType) :
            name{_name},
            reportingName{name},
            description{_desc},
            valueType{_type},
            metricType{_metricType} { }

    protected:
        std::string name;
        std::string description;
        std::string reportingName;
        ValueType valueType;
        MetricType metricType;
        bool inMetricSet = false;
};


class EventCountMetric : public Metric
{
    public:
        EventCountMetric(const char *name, const char *description) :
            Metric{name, description, ValueType::INTEGER, MetricType::EVENTCOUNT}  { }
        void inc(uint32_t val) { count.fetch_add(val, std::memory_order_relaxed); }
        void collect(MeasurementVector &values) override;

    protected:
        std::atomic<uint32_t> count{0};
};


class CounterMetric : public Metric
{
    public:
        CounterMetric(const char *name, const char *description) :
            Metric{name, description, ValueType::INTEGER, MetricType::COUNTER}  { }
        void inc(uint32_t val) { count.fetch_add(val, std::memory_order_relaxed);  }
        void collect(MeasurementVector &values) override;

    protected:
        std::atomic<uint32_t> count{0};
};


class RateMetric : public Metric
{
    public:
        RateMetric(const char *name, const char *description) :
            Metric{name, description, ValueType::INTEGER, MetricType::RATE}  { }
        void init() override { periodStart = std::chrono::high_resolution_clock::now(); }
        void inc(uint32_t val) { count += val; }
        void collect(MeasurementVector &values) override;

    protected:
        std::atomic<uint32_t> count{0};
        std::chrono::time_point<std::chrono::high_resolution_clock> periodStart;
};


template<typename T>
class GaugeMetric : public Metric {
    public:
        GaugeMetric(const char *name, const char *description, ValueType valueType) :
            Metric{name, description, valueType, MetricType::GAUGE}  { }
        void inc(T inc) { value += inc; }
        void dec(T dec) { value -= dec; }
        void collect(MeasurementVector &values) override;

    protected:
        std::atomic<T> value{0};
};


template<typename T>
void GaugeMetric<T>::collect(MeasurementVector &values)
{
    auto pMeasurement = std::make_shared<Measurement<T>>(this, reportingName, value.load());
    values.emplace_back(pMeasurement);
}


interface IMetricSet
{
    virtual std::string getName() const = 0;
    virtual void init() = 0;
    virtual void collect(MeasurementVector &values) = 0;
    virtual std::vector<std::shared_ptr<const IMetric>> getMetrics() = 0;
};


class METRICS_API MetricSet : public IMetricSet
{
    public:
        MetricSet(const char *_name, const char *_prefix, const std::vector<std::shared_ptr<IMetric>> &_metrics);
        std::string getName() const override { return name; }
        std::vector<std::shared_ptr<const IMetric>> getMetrics() override;
        void init() override;
        void collect(MeasurementVector &values) override;

    protected:
        void setMetrics(const std::vector<std::shared_ptr<IMetric>> &_metrics);

    protected:
        std::map<std::string, std::shared_ptr<IMetric>> metrics;
        std::string name;
        std::string reportNamePrefix;
};


class MetricsReportContext;
interface IMetricSink
{
    virtual void handle(const MeasurementVector &values, const std::shared_ptr<IMetricSet> &pMetricSet, MetricsReportContext *pContext) = 0;
    virtual void setMetricSets(const std::vector<std::shared_ptr<IMetricSet>> &sets) = 0;
    virtual std::string getName() const = 0;
    virtual std::string getType() const = 0;
};


extern "C" { typedef hpccMetrics::IMetricSink* (*getSinkInstance)(const std::string &sinkName, const IPropertyTree *pSettingsTree); }

class METRICS_API MetricSink : public IMetricSink
{
    public:

        virtual ~MetricSink() = default;
        void setMetricSets(const std::vector<std::shared_ptr<IMetricSet>> &sets) override;
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
        std::map<std::string, std::shared_ptr<IMetricSet>> metricSets;
};


struct MetricsReportConfig
{
    void addReportConfig(IMetricSink *pSink, const std::shared_ptr<IMetricSet> &set);
    std::unordered_set<std::shared_ptr<IMetricSet>> metricSets;
    std::map<IMetricSink *, std::vector<std::shared_ptr<IMetricSet>>> metricReportConfig;
};


class MetricsReporter;
interface IMetricsReportTrigger
{
    virtual void setReporter(MetricsReporter *metricsReporter) = 0;
    virtual void start() = 0;
    virtual void stop() = 0;
};


extern "C" { typedef hpccMetrics::IMetricsReportTrigger* (*getTriggerInstance)(const IPropertyTree *pSettingsTree); }

class METRICS_API MetricsReportTrigger : public IMetricsReportTrigger
{
    public:
        virtual ~MetricsReportTrigger() = default;
        void setReporter(MetricsReporter *pMetricsReporter) override  { pReporter = pMetricsReporter; }
        static IMetricsReportTrigger *getTriggerFromLib(const char *type, const IPropertyTree *pSettingsTree);
        static IMetricsReportTrigger *getTriggerFromLib(const char *type, const char *getInstanceProcName, const IPropertyTree *pSettingsTree);

    protected:
        explicit MetricsReportTrigger() = default;
        void doReport(const std::string& sinkName, MetricsReportContext *pReportContext);

    private:
        MetricsReporter *pReporter = nullptr;
};


class MetricsReportContext
{
    public:

        MetricsReportContext() = default;
        virtual ~MetricsReportContext() = default;
        const char * getBufferContents() const { return buffer.str();  }
        void setBufferContents(const char *data)  { buffer.set(data); }

    private:
        StringBuffer buffer;
};


class METRICS_API MetricsReporter
{
    public:
        explicit MetricsReporter(MetricsReportConfig &_reportConfig, IMetricsReportTrigger *_pTrigger) :
            reportConfig{_reportConfig},
            pTrigger{_pTrigger}  { }
        virtual ~MetricsReporter() = default;
        void start() { init(); pTrigger->start(); }
        void stop()  { pTrigger->stop(); }
        bool report(std::map<std::string, MetricsReportContext *> &reportContexts);

    protected:
        void init();

    protected:
        MetricsReportConfig reportConfig;
        IMetricsReportTrigger *pTrigger;
        std::mutex reportMutex;
};

}
