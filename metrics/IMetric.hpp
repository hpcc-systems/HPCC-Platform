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
#include <vector>
#include <memory>
#include "../../system/jlib/jiface.hpp"
#include "IMeasurement.hpp"


namespace hpccMetrics
{

typedef std::vector<std::shared_ptr<IMeasurement>> MeasurementVector;

//
// Interface for all metrics
interface IMetric
{

    //
    // Return the name of the metric.
    virtual std::string getName() const = 0;

    //
    // Set the name of the value when reported. By default, the report name
    // is the metric name.
    virtual void setReportingName(const std::string &name) = 0;

    //
    // Return the reporting name
    virtual std::string getReportingName() const = 0;

    //
    // Optional method to set the type of the reported value (intended
    // for use by a sink that may want to type values in its report).
    // Types follow those defined by Elasticsearch
    virtual void setType(MetricType type) = 0;

    //
    // Returns the type (empty string if no type set).
    virtual MetricType getType() const = 0;

    //
    // Indicates if the metric has been added to a metric set or not.
    virtual bool isInMetricSet() const = 0;

    //
    // Set whether the metric has been added to a metric set or not
    virtual void setInMetricSet(bool val) = 0;

    //
    // Initialize the metric for collection and reporting
    virtual void init() = 0;

    //
    // Collect the value(s) of the metric.
    virtual bool collect(MeasurementVector &values) = 0;

};

}
