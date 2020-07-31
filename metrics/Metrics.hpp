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
#include "IMetric.hpp"
#include "IMetricSet.hpp"
#include "Measurement.hpp"


namespace hpccMetrics
{

class Metric : public IMetric
{
    public:
        virtual ~Metric() = default;
        std::string getName() const override { return name; }
        std::string getDescription() const override { return description; }
        void setReportingName(const std::string &_name) override { reportingName = _name; }
        std::string getReportingName() const override { return reportingName; }
        void setValueType(ValueType _type) override { valueType = _type; }
        ValueType getValueType() const override { return valueType; }
        bool isInMetricSet() const override { return inMetricSet; }
        void setInMetricSet(bool val) override { inMetricSet = val; }
        void init() override { }


    protected:
        // No one should be able to create one of these
        explicit Metric(std::string _name, std::string _desc, ValueType _type = ValueType::NONE) :
            name(std::move(_name)),
            description{std::move(_desc)},
            valueType{_type}
        {
            reportingName = name;  // default to the given name
        }


    protected:
        std::string name;
        std::string description;
        std::string reportingName;
        ValueType valueType;
        bool inMetricSet = false;
};


class CountMetric : public Metric
{
    public:
        explicit CountMetric(const std::string& name, const std::string &description = std::string()) :
            Metric{name, description, ValueType::INTEGER}  { }
        ~CountMetric() override = default;
        void inc(uint32_t val)
        {
            count.fetch_add(val);
        }

        MetricType getMetricType() const override
        {
            return COUNTER;
        }

        //
        // Clears the count on collection
        bool collect(MeasurementVector &values) override
        {
            auto pMeasurement = std::make_shared<Measurement<uint32_t>>(reportingName, valueType, COUNTER, count.exchange(0), description);
            values.emplace_back(pMeasurement);
            return true;
        }

    protected:
        std::atomic<uint32_t> count{0};
};


class RateMetric : public Metric
{
    public:
        explicit RateMetric(std::string name, const std::string &description = std::string()) :
            Metric{std::move(name), description, ValueType::FLOAT} { }
        ~RateMetric() override = default;

        MetricType getMetricType() const override
        {
            return RATE;
        }

        void init() override
        {
            periodStart = std::chrono::high_resolution_clock::now();
        }

        void inc(uint32_t val)
        {
            count += val;
        }

        //
        // Clears the rate on collection
        bool collect(MeasurementVector &values) override
        {
            std::chrono::time_point<std::chrono::high_resolution_clock> now, start;
            unsigned numEvents;

            now = std::chrono::high_resolution_clock::now();
            start = periodStart;

            // Synchronize with update of count
            {
                std::unique_lock<std::mutex> lock(mutex);
                numEvents = count.exchange(0);
                periodStart = now;
            }

            auto seconds = (std::chrono::duration_cast<std::chrono::seconds>(now - start)).count();
            float rate = static_cast<float>(numEvents) / static_cast<float>(seconds);
            auto pMeasurement = std::make_shared<Measurement<float>>(reportingName, valueType, RATE, rate, description);
            values.emplace_back(pMeasurement);
            return true;
        }

    protected:
        std::mutex mutex;
        std::atomic<uint32_t> count{0};
        std::chrono::time_point<std::chrono::high_resolution_clock> periodStart;
};

template<typename T>
class GaugeMetric : public Metric {
    public:
        explicit GaugeMetric(std::string name, const std::string &description = std::string()) :
            Metric{std::move(name), description} { }
        ~GaugeMetric() override = default;

        MetricType getMetricType() const override
        {
            return GAUGE;
        }

        void inc(T inc)
        {
            value += inc;
        }

        void dec(T dec)
        {
            value -= dec;
        }

        //
        // Value is not cleared on collection
        bool collect(MeasurementVector &values) override
        {
            auto pMeasurement = std::make_shared<Measurement<T>>(reportingName, valueType, GAUGE, value.load(), description);
            values.emplace_back(pMeasurement);
            return true;
        }

    protected:
        std::atomic<T> value{0};
};


// This distribution metric is still being worked out
//template <typename T>
//class DistributionMetric : public Metric {
//    public:
//        DistributionMetric(std::string &name, const std::vector<T> &bucketLevels) :
//                Metric{std::move(name)}
//        {
//            if (bucketLevels[0] != std::numeric_limits<T>::min())
//                buckets[0] = new std::atomic<uint32_t>();
//
//            for (auto const &distValue : bucketLevels)
//            {
//                buckets.insert(std::pair<T, std::atomic<unsigned> *>(distValue, new std::atomic<unsigned>()));
//            }
//
//            if (bucketLevels[bucketLevels.size() - 1] != std::numeric_limits<T>::max())
//                buckets.insert(std::pair<T, std::atomic<uint32_t> *>(std::numeric_limits<T>::max(),
//                                                                     new std::atomic<uint32_t>()));
//        }
//
//        void inc(T level, uint32_t val)
//        {
//            auto it = buckets.lower_bound(level);
//            if (it != buckets.end())
//            {
//                it.second += val;
//            }
//        }
//
//        // TBD
//        bool collect(MeasurementVector &values) override
//        {
//            return false;
//        }
//
//    protected:
//        std::map<T, std::atomic<uint32_t> *> buckets;
//};

}
