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
#include "../../system/jlib/jiface.hpp"

namespace hpccMetrics
{

//
// Defined metric value types. Mainly used by the sink when reporting
enum MetricType {
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


//
// Interface for object used to hold and report a metric value obtained during collection
interface IMeasurement
{

    //
    // Return the value as a string object
    virtual std::string valueToString() const = 0;

    //
    // Return the name of the metric value for reporting purposes
    virtual const std::string &getName() const = 0;

    //
    // Get the metric type
    virtual MetricType getType() const = 0;
};

}
