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
#include "IMeasurement.hpp"
#include "IMetric.hpp"

namespace hpccMetrics
{

template<typename T>
class Measurement : public IMeasurement
{
    public:
        Measurement(std::string &_name, ValueType _valueType, MetricType _metricType, T _value, std::string &desc) :
            name{_name},
            valueType{_valueType},
            metricType{_metricType},
            value{_value},
            metricDescription{desc}
            { }

        const std::string &getName() const override { return name; }
        std::string valueToString() const override { return std::to_string(value); }
        ValueType getValueType() const override { return valueType; }
        MetricType getMetricType() const override { return metricType; };
        const std::string &getDescription() const override { return metricDescription; }

    protected:
        const std::string &name;
        const std::string &metricDescription;
        ValueType valueType;
        MetricType metricType;
        T value;
};

}
