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


#pragma once

#include "jmetrics.hpp"
#include "jptree.hpp"
#include "jstring.hpp"

//including cpp-httplib single header file REST client
//  doesn't work with format-nonliteral as an error
//
#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#pragma GCC diagnostic ignored "-Warray-bounds"
#endif

#undef INVALID_SOCKET
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#ifdef ELASTICSINK_EXPORTS
#define ELASTICSINK_API DECL_EXPORT
#else
#define ELASTICSINK_API DECL_IMPORT
#endif

class ELASTICSINK_API ElasticMetricSink : public hpccMetrics::PeriodicMetricSink
{
public:
    explicit ElasticMetricSink(const char *name, const IPropertyTree *pSettingsTree);
    ~ElasticMetricSink() override = default;

protected:
    virtual bool prepareToStartCollecting() override;
    virtual void collectingHasStopped() override;
    virtual void doCollection() override;
    bool getHostConfig(const IPropertyTree *pSettingsTree);
    bool getIndexConfig(const IPropertyTree *pSettingsTree);
    bool getDynamicMappingSuffixesFromIndex(const IPropertyTree *pIndexConfigTree);
    static bool convertPatternToSuffix(const char *pattern, StringBuffer &suffix);
    bool validateIndex();
    void intializeElasticClient();

protected:
    StringBuffer indexName;
    bool ignoreZeroMetrics = false;
    StringBuffer elasticHostUrl;
    StringBuffer certificateFilePath;
    StringBuffer authenticationType;
    StringBuffer username;
    StringBuffer password;
    StringBuffer countMetricSuffix;
    StringBuffer gaugeMetricSuffix;
    StringBuffer histogramMetricSuffix;
    int connectTimeout = 5;
    int readTimeout = 5;
    int writeTimeout = 5;
    bool configurationValid = false;
    std::shared_ptr<httplib::Client> pClient;
    httplib::Headers elasticHeaders;
};
