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

#include <jptree.hpp>
#include "espmetrics.hpp"

using namespace hpccMetrics;

void EspMetrics::init(const char *testConfigYml)
{
    IPropertyTree *pSettings = createPTreeFromYAMLString(testConfigYml, ipt_none, ptr_ignoreWhiteSpace, nullptr);
    IPropertyTree *pEspMetricsConfig = pSettings->getPropTree("esp/metrics");

    //
    // Init reporter with config
    metricsReporter.init(pEspMetricsConfig);

    //
    // Now create the counter for requests and add it to the reporter
    pCountRequests = std::make_shared<CounterMetric>("requests", "Number of requests");
    metricsReporter.addMetric(pCountRequests);

    //
    // Finally, start reporting
    metricsReporter.startCollecting();
}
