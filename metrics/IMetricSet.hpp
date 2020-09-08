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


#include <map>
#include <string>
#include <memory>
#include <vector>
#include "../../system/jlib/jiface.hpp"
#include "IMeasurement.hpp"
#include "IMetric.hpp"

namespace hpccMetrics
{

//
// Interface representing as set of metrics for collection and reporting. All metrics must be a member
// of one and only one metric set
interface IMetricSet
{
    //
    // Return the name of the metric set
    virtual std::string getName() const = 0;

    //
    // Initialize the set for collection and reporting
    virtual void init() = 0;

    //
    // Collect the measurements for the set
    virtual void collect(MeasurementVector &values) = 0;

    //
    // return a vector of the set's metrics
    virtual std::vector<std::shared_ptr<const IMetric>> getMetrics() = 0;

};

}
