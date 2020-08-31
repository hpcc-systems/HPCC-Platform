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

#include "../../system/jlib/jiface.hpp"

namespace hpccMetrics
{
    class MetricsReporter;

    interface IMetricsReportTrigger
    {
        //
        // Give the reporter to the trigger so it can do reports
        virtual void setReporter(MetricsReporter *metricsReporter) = 0;

        //
        // Start collecting metrics
        virtual void start() = 0;

        //
        // Stop collecting metrics
        virtual void stop() = 0;
    };

}
