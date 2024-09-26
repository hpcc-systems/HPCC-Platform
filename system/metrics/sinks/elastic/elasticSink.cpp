/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
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

#include "elasticSink.hpp"

#include "nlohmann/json.hpp"

//including cpp-httplib single header file REST client
//  doesn't work with format-nonliteral as an error
//
#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif

#undef INVALID_SOCKET
#include "httplib.h"

#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

using namespace hpccMetrics;

extern "C" MetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree)
{
    MetricSink *pSink = new ElasticMetricSink(name, pSettingsTree);
    return pSink;
}


ElasticMetricSink::ElasticMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
    PeriodicMetricSink(name, "elastic", pSettingsTree)
{
    ignoreZeroMetrics = pSettingsTree->getPropBool("@ignoreZeroMetrics", true);
    pSettingsTree->getProp("@elasticHost", elasticHost);
    pSettingsTree->getProp("@indexName", indexName);
}


void ElasticMetricSink::prepareToStartCollecting()
{

}


void ElasticMetricSink::doCollection()
{

}


void ElasticMetricSink::collectingHasStopped()
{
    ;
}

