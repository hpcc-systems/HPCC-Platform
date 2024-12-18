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
#include "jsecrets.hpp"
#include "jencrypt.hpp"

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
    // Standard sink settings
    ignoreZeroMetrics = pSettingsTree->getPropBool("@ignoreZeroMetrics", true);

    // Get the host and index configuration
    if (getHostConfig(pSettingsTree) && getIndexConfig(pSettingsTree))
    {
        configurationValid = true;
        PROGLOG("ElasticMetricSink: Loaded and configured");
    }
}


bool ElasticMetricSink::getHostConfig(const IPropertyTree *pSettingsTree)
{
    StringBuffer hostDomain;
    StringBuffer hostProtocol;
    StringBuffer hostPort;

    Owned<IPropertyTree> pHostConfigTree = pSettingsTree->getPropTree("host");
    if (pHostConfigTree)
    {
        pHostConfigTree->getProp("@domain", hostDomain);

        if (!pHostConfigTree->getProp("@protocol", hostProtocol))
            hostProtocol.append("https");

        if (!pHostConfigTree->getProp("@port", hostPort))
            hostPort.append("9200");
    }

    // Validate the host configuration minimal settings are present
    if (hostDomain.isEmpty() || hostProtocol.isEmpty())
    {
        WARNLOG("ElasticMetricSink: Host configuration missing domain and/or protocol");
        return false;
    }

    // build url for use with httplib Client
    elasticHostUrl.append(hostProtocol).append("://").append(hostDomain);
    if (!hostPort.isEmpty())
        elasticHostUrl.append(":").append(hostPort);

    // Read optional certificate file path
    pHostConfigTree->getProp("@certificateFilePath", certificateFilePath);

    // Get authentication settings, if present
    Owned<IPropertyTree> pAuthConfigTree = pSettingsTree->getPropTree("authentication");
    if (!pAuthConfigTree)
        return true;

    // Retrieve the authentication type and validate (only basic is supported)
    if (!pAuthConfigTree->getProp("@type", authenticationType) || !streq(authenticationType, "basic"))
    {
        WARNLOG("ElasticMetricSink: Only basic authentication is supported");
        return false;
    }

    StringBuffer credentialsSecretKey;
    pAuthConfigTree->getProp("@credentialsSecret", credentialsSecretKey);  // vault/secrets key
    if (!credentialsSecretKey.isEmpty())
    {
        StringBuffer credentialsVaultId;
        pAuthConfigTree->getProp("@credentialsVaultId", credentialsVaultId);//optional HashiCorp vault ID

        PROGLOG("Retrieving ElasticSearch host authentication username/password from secrets tree '%s', from vault '%s'",
               credentialsSecretKey.str(), !credentialsVaultId.isEmpty() ? credentialsVaultId.str() : "");

        Owned<const IPropertyTree> secretTree(getSecret("authn", credentialsSecretKey.str(), credentialsVaultId, nullptr));
        if (secretTree == nullptr)
        {
            WARNLOG("ElasticMetricSink: Unable to load secret tree '%s', from vault '%s'", credentialsSecretKey.str(),
                    !credentialsVaultId.isEmpty() ? credentialsVaultId.str() : "n/a");
            return false;
        }

        // authentication type defines the secret key name/value pairs to retrieve
        if (streq(authenticationType, "basic"))
        {
            if (!getSecretKeyValue(username, secretTree, "username") || !getSecretKeyValue(password, secretTree, "password"))
            {
                WARNLOG("ElasticMetricSink: Missing username and/or password from secrets tree '%s', vault '%s'",
                        credentialsSecretKey.str(), !credentialsVaultId.isEmpty() ? credentialsVaultId.str() : "n/a");
                return false;
            }
        }
    }
    else
    {
        // if basic auth, username and password are stored directly in the configuration
        if (streq(authenticationType, "basic"))
        {
            StringBuffer encryptedPassword;
            if (!pAuthConfigTree->getProp("@username", username) || !pAuthConfigTree->getProp("@password", encryptedPassword))
            {
                WARNLOG("ElasticMetricSink: Missing username and/or password from configuration");
                return false;
            }
            decrypt(password, encryptedPassword.str()); //MD5 encrypted in config
        }
    }
    return true;
}


bool ElasticMetricSink::getIndexConfig(const IPropertyTree *pSettingsTree)
{
    Owned<IPropertyTree> pIndexConfigTree = pSettingsTree->getPropTree("index");
    if (!pIndexConfigTree)
    {
        WARNLOG("ElasticMetricSink: Index configuration missing");
        return false;
    }

    if (!pIndexConfigTree->getProp("@name", indexName))
    {
        WARNLOG("ElasticMetricSink: Index configuration missing name");
        return false;
    }

    // Initialize standard suffixes
    if (!pIndexConfigTree->getProp("@countMetricSuffix", countMetricSuffix))
        countMetricSuffix.append("count");

    if (!pSettingsTree->getProp("@gaugeMetricSuffix", gaugeMetricSuffix))
        gaugeMetricSuffix.append("gauge");

    if (!pSettingsTree->getProp("@histogramMetricSuffix", histogramMetricSuffix))
        histogramMetricSuffix.append("histogram");

    return true;
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

