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
    else
    {
        WARNLOG("ElasticMetricSink: Unable to complete initialization, sink not collecting");
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
    Owned<IPropertyTree> pAuthConfigTree = pHostConfigTree->getPropTree("authentication");
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

    // Read optional timeout values
    connectTimeout = pHostConfigTree->getPropInt("@connectionTimeout", connectTimeout);
    readTimeout = pHostConfigTree->getPropInt("@readTimeout", readTimeout);
    writeTimeout = pHostConfigTree->getPropInt("@writeTimeout", writeTimeout);

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

    intializeElasticClient();

    if (!validateIndex())
        return false;

    return getDynamicMappingSuffixesFromIndex(pIndexConfigTree);
}


bool ElasticMetricSink::getDynamicMappingSuffixesFromIndex(const IPropertyTree *pIndexConfigTree)
{
    StringBuffer countSuffixMappingName;
    if (!pIndexConfigTree->getProp("@countSuffixMappingName", countSuffixMappingName))
        countSuffixMappingName.append("hpcc_metrics_count_suffix");

    StringBuffer gaugeSuffixMappingName;
    if (!pIndexConfigTree->getProp("@gaugeSuffixMappingName", gaugeSuffixMappingName))
        gaugeSuffixMappingName.append("hpcc_metrics_gauge_suffix");

    StringBuffer histogramSuffixMappingName;
    if (!pIndexConfigTree->getProp("@histogramSuffixMappingName", histogramSuffixMappingName))
        histogramSuffixMappingName.append("hpcc_metrics_histogram_suffix");

    std::string endpoint;
    endpoint.append("/").append(indexName.str()).append("/_mapping");
    httplib::Result res = pClient->Get(endpoint.c_str(), elasticHeaders);

    if (res == nullptr)
    {
        httplib::Error err = res.error();
        WARNLOG("ElasticMetricSink: Unable to connect to ElasticSearch host '%s', httplib Error = %d", elasticHostUrl.str(), err);
        return false;
    }

    if (res->status != 200)
    {
        WARNLOG("ElasticMetricSink: Error response status = %d, unable to retrieve mapping for index '%s'", res->status, indexName.str());
        return false;
    }

    nlohmann::json data = nlohmann::json::parse(res->body);

    auto indexConfig = data[indexName.str()];
    if (indexConfig.is_null())
    {
        WARNLOG("ElasticMetricSink: Unable to load configuration for Index '%s'", indexName.str());
        return false;
    }

    auto mappings = indexConfig["mappings"];
    if (mappings.is_null())
    {
        WARNLOG("ElasticMetricSink: Required 'mappings' section does not exist in Index '%s'", indexName.str());
        return false;
    }

    auto dynamicTemplates = mappings["dynamic_templates"];
    if (dynamicTemplates.is_null())
    {
        WARNLOG("ElasticMetricSink: Required 'dynamic_templates' section does not exist in Index '%s'", indexName.str());
        return false;
    }

    // validate type is as expected from the data
    if (dynamicTemplates.is_array())
    {
        for (auto & dynamicTemplate : dynamicTemplates.items())
        {
            auto mappingObject = dynamicTemplate.value().get<nlohmann::json::object_t>();

            for (const auto& it: mappingObject)
            {
                auto const &mappingName = it.first;
                auto kvPairs = it.second;
                auto const &matchValue = kvPairs["match"];
                if (mappingName == countSuffixMappingName.str())
                {
                    if (!convertPatternToSuffix(matchValue.get<std::string>().c_str(), countMetricSuffix))
                    {
                        WARNLOG("ElasticMetricSink: Invalid count suffix pattern");
                        return false;
                    }
                }
                else if (mappingName == histogramSuffixMappingName.str())
                {
                    if (!convertPatternToSuffix(matchValue.get<std::string>().c_str(), histogramMetricSuffix))
                    {
                        WARNLOG("ElasticMetricSink: Invalid histogram suffix pattern");
                        return false;
                    }
                }
                else if (mappingName == gaugeSuffixMappingName.str())
                {
                    if (!convertPatternToSuffix(matchValue.get<std::string>().c_str(), gaugeMetricSuffix))
                    {
                        WARNLOG("ElasticMetricSink: Invalid gauge suffix pattern");
                        return false;
                    }
                }
            }
        }

        // If no gauge suffix pattern configured, use the count suffix pattern
        if (gaugeMetricSuffix.isEmpty())
            gaugeMetricSuffix.append(countMetricSuffix);

        // All three must be configured
        return !countMetricSuffix.isEmpty() && !gaugeMetricSuffix.isEmpty() && !histogramMetricSuffix.isEmpty();
    }
    return false;
}


bool ElasticMetricSink::convertPatternToSuffix(const char *pattern, StringBuffer &suffix)
{
    if (pattern && (strlen(pattern) > 2) && pattern[0] == '*')
    {
        suffix.append(pattern + 1);
        return true;
    }
    return false;
}


void ElasticMetricSink::intializeElasticClient()
{
    elasticHeaders = {
            {"Content-Type", "application/json"},
            {"Accept",       "application/json"}
    };

    // Add authorization header if needed
    if (streq(authenticationType, "basic"))
    {
        StringBuffer token;
        StringBuffer usernamePassword;
        usernamePassword.append(username).append(":").append(password);
        JBASE64_Encode(usernamePassword.str(), usernamePassword.length(), token, false);
        StringBuffer basicAuth;
        basicAuth.append("Basic ").append(token);
        elasticHeaders.insert({"Authorization", basicAuth.str()});
    }

    pClient = std::make_shared<httplib::Client>(elasticHostUrl.str());

    // Add cert path if needed
    if (!certificateFilePath.isEmpty())
        pClient->set_ca_cert_path(certificateFilePath.str());

    pClient->set_connection_timeout(connectTimeout);
    pClient->set_read_timeout(readTimeout);
    pClient->set_write_timeout(writeTimeout);
}


bool ElasticMetricSink::prepareToStartCollecting()
{
    return configurationValid;
}


void ElasticMetricSink::doCollection()
{

}


void ElasticMetricSink::collectingHasStopped()
{

}


bool ElasticMetricSink::validateIndex()
{
    std::string endpoint;
    endpoint.append("/").append(indexName.str());
    auto res = pClient->Get(endpoint.c_str(), elasticHeaders);

    if (res == nullptr)
    {
        httplib::Error err = res.error();
        WARNLOG("ElasticMetricSink: Unable to connect to ElasticSearch host '%s, httplib Error = %d", elasticHostUrl.str(), err);
        return false;
    }

    else if (res->status != 200)
    {
        WARNLOG("ElasticMetricSink: Error response status = %d accessing Index '%s'", res->status, indexName.str());
        return false;
    }
    return true;
}
