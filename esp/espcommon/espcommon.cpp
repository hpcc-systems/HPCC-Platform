/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

############################################################################## */

#include "platform.h"
#include "jexcept.hpp"
#include "jutil.hpp"

#include "espcommon.hpp"

// Converts the bucket limit in logical units to the standard units used by the metric framework
static __uint64 convertToBucketLimit(const char *units, const char *value)
{
    __uint64 limit = std::stoull(value);  // the raw value

    if (!strncmp(units, "s", 1))
        limit *= 1000000000;
    else if (!strncmp(units, "ms", 2))
        limit *= 1000000;
    else if (!strncmp(units, "us", 2))
        limit *= 1000;
    else if (strncmp(units, "ns", 2))
        throw MakeStringException(-1, "Invalid units, '%s', detected while converting to bucket limits", units);

    return limit;
}

// function to return a set of buckets for an execution profile definitions
static std::vector<__uint64> getExecutionProfileBuckets(const char *profileOptions)
{
    StringArray executionProfileParms;
    executionProfileParms.appendList(profileOptions, ",");

    std::vector<__uint64> buckets;
    for (int i=1; i<executionProfileParms.ordinality(); ++i)
    {
        buckets.emplace_back(convertToBucketLimit(executionProfileParms.item(0), executionProfileParms.item(i) ));
    }
    return buckets;
}

// Generate metric name for the indicated service method and register a profiling metric
ESPCOMMON_API std::shared_ptr<hpccMetrics::ScaledHistogramMetric> registerServiceMethodProfilingMetric(
        const char *processName, const char *serviceName, const char *methodName,
        const char *desc, const char *profilingOptions)
{
    std::string metricName(processName);
    metricName.append(".").append(serviceName).append(".").append(methodName);
    return registerProfilingMetric(metricName.c_str(), desc, profilingOptions);
}

// Registers a profiling metric
ESPCOMMON_API std::shared_ptr<hpccMetrics::ScaledHistogramMetric> registerProfilingMetric(const char *histogramMetricName, const char *desc, const char *profilingOptions)
{
    return hpccMetrics::registerCyclesToNsScaledHistogramMetric(histogramMetricName, desc, getExecutionProfileBuckets(profilingOptions));
}