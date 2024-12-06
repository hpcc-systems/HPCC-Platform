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

    StringBuffer hostName;
    StringBuffer hostProtocol;
    StringBuffer hostPort;

    Owned<IPropertyTree> pHostConfigTree = pSettingsTree->getPropTree("host");
    if (pHostConfigTree)
    {
        pHostConfigTree->getProp("@name", hostName);

        if (!pHostConfigTree->getProp("@protocol", hostProtocol))
        {
            hostProtocol.append("https");
        }

        if (!pHostConfigTree->getProp("@port", hostPort))
        {
            hostPort.append("9200");
        }
    }

    if (!hostName.isEmpty() && !hostPort.isEmpty() && !hostProtocol.isEmpty())
    {
        elasticHostUrl.append(hostProtocol).append("://").append(hostName).append(":").append(hostPort);
    }
    else
    {
        WARNLOG("ElasticMetricSink: Host configuration missing or invalid");
    }

    Owned<IPropertyTree> pIndexConfigTree = pSettingsTree->getPropTree("index");
    if (pIndexConfigTree)
    {
        pSettingsTree->getProp("@name", indexName);
    }

    if (indexName.isEmpty())
    {
        WARNLOG("ElasticMetricSink: Index configuration missing or invalid");
    }


    // Both a host url and an index name are required
    configurationValid = !elasticHostUrl.isEmpty() && !indexName.isEmpty();

    // Initialize standard suffixes
    if (!pSettingsTree->getProp("@countMetricSuffix", countMetricSuffix))
    {
        countMetricSuffix.append("count");
    }

    if (!pSettingsTree->getProp("@gaugeMetricSuffix", gaugeMetricSuffix))
    {
        gaugeMetricSuffix.append("gauge");
    }

    if (!pSettingsTree->getProp("@histogramMetricSuffix", histogramMetricSuffix))
    {
        histogramMetricSuffix.append("histogram");
    }
}


bool ElasticMetricSink::prepareToStartCollecting()
{
    return false;
}


void ElasticMetricSink::doCollection()
{

}


void ElasticMetricSink::collectingHasStopped()
{

}

