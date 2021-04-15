/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems.

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

#ifndef _ESPWIZ_WS_STORE_HPP__
#define _ESPWIZ_WS_STORE_HPP__

#include "ws_store.hpp"
#include "ws_store_esp.ipp"

#include "dautils.hpp"
#include "espStoreShare.hpp"

static StringBuffer g_wsstoreBuildVersion;

class CwsstoreSoapBindingEx : public CwsstoreSoapBinding
{
public:
    CwsstoreSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CwsstoreSoapBinding(cfg, name, process, llevel)
    {
    }
};

class CwsstoreEx : public Cwsstore
{
private:
    Owned<IEspStore> m_storeProvider;
    Owned<IPropertyTree> m_serviceConfig;
    StringAttr m_defaultStore;

    IEspStore * loadStoreProvider(const char* instanceName, const char* libName, const char * factoryMethodName);
public:
    IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *_cfg, const char *_process, const char *_service);

    bool onListStores(IEspContext &context, IEspListStoresRequest &req, IEspListStoresResponse &resp);
    bool onListNamespaces(IEspContext &context, IEspListNamespacesRequest &req, IEspListNamespacesResponse &resp);
    bool onListKeys(IEspContext &context, IEspListKeysRequest &req, IEspListKeysResponse &resp);
    bool onSet(IEspContext &context, IEspSetRequest &req, IEspSetResponse &resp);
    bool onFetch(IEspContext &context, IEspFetchRequest &req, IEspFetchResponse &resp);
    bool onFetchAll(IEspContext &context, IEspFetchAllRequest &req, IEspFetchAllResponse &resp);
    //bool onAddNamespace(IEspContext &context, IEspAddNamespaceRequest &req, IEspAddNamespaceResponse &resp);
    bool onCreateStore(IEspContext &context, IEspCreateStoreRequest &req, IEspCreateStoreResponse &resp);
    bool onDelete(IEspContext &context, IEspDeleteRequest &req, IEspDeleteResponse &resp);
    bool onDeleteNamespace(IEspContext &context, IEspDeleteNamespaceRequest &req, IEspDeleteNamespaceResponse &resp);
    bool onFetchKeyMetadata(IEspContext &context, IEspFetchKeyMDRequest &req, IEspFetchKeyMDResponse &resp);

    bool attachServiceToDali() override
    {
        m_isDetachedFromDali = false;
        return true;
    }

    bool detachServiceFromDali() override
    {
        m_isDetachedFromDali = true;
        return true;
    }

    bool isAttachedToDali()
    {
        return m_isDetachedFromDali;
    }

private:
    bool m_isDetachedFromDali = false;
};

#endif //_ESPWIZ_WS_STORE_HPP__
