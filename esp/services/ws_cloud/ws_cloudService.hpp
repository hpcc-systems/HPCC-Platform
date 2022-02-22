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

const unsigned defaultPODInfoCacheForceBuildSecond = 10;
const unsigned defaultPODInfoCacheAutoRebuildSecond = 120;

class CPODs : public CInfoCache
{
    StringBuffer pods;
public:
    const char* read();
    inline const char* queryPODs() { return pods.str(); };
};

class CPODInfoCacheReader : public CInfoCacheReader
{
public:
    CPODInfoCacheReader(const char* _name, unsigned _autoRebuildSeconds, unsigned _forceRebuildSeconds)
        : CInfoCacheReader(_name, _autoRebuildSeconds, _forceRebuildSeconds) {}

    virtual CInfoCache* read() override
    {
        Owned<CPODs> info = new CPODs();
        info->read();
        return info.getClear();
    };
};

class CWsCloudEx : public CWsCloud
{
    Owned<CInfoCacheReader> podInfoCacheReader;

public:
    IMPLEMENT_IINTERFACE;
    virtual void init(IPropertyTree* cfg, const char* process, const char* service) override;

    virtual bool onGetPODs(IEspContext& context, IEspGetPODsRequest& req, IEspGetPODsResponse& resp) override;
};

#endif //_ESPWIZ_WsCloud_HPP__
