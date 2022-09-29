/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#ifndef _ESPWIZ_WsCloud_HPP__
#define _ESPWIZ_WsCloud_HPP__

#include "ws_cloud_esp.ipp"
#include "InfoCacheReader.hpp"

const unsigned defaultK8sResourcesInfoCacheForceBuildSeconds = 10;
const unsigned defaultK8sResourcesInfoCacheAutoRebuildSeconds = 120;

class CK8sResourcesInfoCache  : public CInfoCache
{
    StringBuffer pods;
    StringBuffer services;

    void readToBuffer(const char* command, StringBuffer& output);
public:
    void read();
    inline const char* queryPODs() { return pods.str(); };
    inline const char* queryServices() { return services.str(); };
};

class CK8sResourcesInfoCacheReader  : public CInfoCacheReader
{
public:
    CK8sResourcesInfoCacheReader (const char* _name, unsigned _autoRebuildSeconds, unsigned _forceRebuildSeconds)
        : CInfoCacheReader(_name, _autoRebuildSeconds, _forceRebuildSeconds) {}

    virtual CInfoCache* read() override
    {
        Owned<CK8sResourcesInfoCache> info = new CK8sResourcesInfoCache();
        info->read();
        return info.getClear();
    };
};

class CWsCloudEx : public CWsCloud
{
    Owned<CInfoCacheReader> k8sResourcesInfoCacheReader;

    const char* buildJsonPublicServices(const char* allServices, StringBuffer& publicServices);
    void addJsonPublicService(IPropertyTree& serviceTree, StringBuffer& publicServices);

public:
    IMPLEMENT_IINTERFACE;
    virtual void init(IPropertyTree* cfg, const char* process, const char* service) override;

    virtual bool onGetPODs(IEspContext& context, IEspGetPODsRequest& req, IEspGetPODsResponse& resp) override;
    virtual bool onGetServices(IEspContext& context, IEspGetServicesRequest& req, IEspGetServicesResponse& resp) override;
};

#endif //_ESPWIZ_WsCloud_HPP__
