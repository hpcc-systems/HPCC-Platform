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

#include "ws_storeService.hpp"
#include "exception_util.hpp"

#define SDS_LOCK_TIMEOUT_ESPSTORE (30*1000)
#define DEFAULT_ESP_STORE_FACTORY_METHOD "newEspStore"

typedef IEspStore* (*newEspStore_t_)();

void CwsstoreEx::init(IPropertyTree *_cfg, const char *_process, const char *_service)
{
   if(_cfg == nullptr)
        throw MakeStringException(-1, "CwsstoreEx::init: Empty configuration provided.");

#ifdef _DEBUG
    StringBuffer thexml;
    toXML(_cfg, thexml,0,0);
    fprintf(stderr, "%s", thexml.str());
#endif

    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", _process, _service);
    m_serviceConfig.setown(_cfg->getPropTree(xpath.str()));

    if(!m_serviceConfig)
        throw MakeStringException(-1, "CwsstoreEx::init: Config not found for service %s/%s",_process, _service);

    IPropertyTree * storeProviderTree = m_serviceConfig->queryPropTree("StoreProvider[1]");

    if (storeProviderTree == nullptr)
         throw MakeStringException(-1, "CwsstoreEx::init: Store provider configuration not found for service %s/%s",_process, _service);

    const char * providerLibraryName = storeProviderTree->queryProp("@lib");
    if (!providerLibraryName || !*providerLibraryName)
        throw MakeStringException(-1, "CwsstoreEx::init: Must provide store provider library name for service %s/%s",_process, _service);

    const char * providerInstanceName = storeProviderTree->queryProp("@name");
    const char * providerFactoryMethod = storeProviderTree->queryProp("@factoryMethod");

    m_storeProvider.setown(loadStoreProvider(providerInstanceName, providerLibraryName, (providerFactoryMethod && *providerFactoryMethod) ? providerFactoryMethod : DEFAULT_ESP_STORE_FACTORY_METHOD));
    if (!m_storeProvider)
        throw MakeStringException(-1, "CwsstoreEx::init: Couldn't instantiate storeprovider lib: '%s' method: '%s'",providerLibraryName, (providerFactoryMethod && *providerFactoryMethod) ? providerFactoryMethod : DEFAULT_ESP_STORE_FACTORY_METHOD);

    m_storeProvider->init(providerInstanceName, "type", storeProviderTree);

    if (!m_isDetachedFromDali)
    {
        ESPLOG(LogMin, "CwsstoreEx: Ensuring configured stores are created:");
        Owned<IPropertyTreeIterator> iter = m_serviceConfig->getElements("Stores/Store");

        StringBuffer owner;
        owner.setf("%s/%s", _process, _service);

        m_defaultStore.clear();

        ForEach(*iter)
        {
            StringBuffer id;
            StringBuffer type;
            StringBuffer description;
            bool isDefault = false;

            iter->query().getProp("@name", id);
            iter->query().getProp("@type", type);
            iter->query().getProp("@description", description);
            isDefault = iter->query().getPropBool("@default", false);

            ESPLOG(LogMin, "CwsstoreEx: Creating Store: '%s'%s", id.str(), isDefault ? " - as Default" : "");
            m_storeProvider->createStore(type.str(), id.str(), description.str(), new CSecureUser(owner.str(), nullptr));
            if (isDefault)
            {
                if (!m_defaultStore.isEmpty())
                   throw MakeStringException(-1, "ws_store init(): Multiple stores erroneously configured as default store!");

                m_defaultStore.set(id.str());
            }
        }
    }
}

IEspStore* CwsstoreEx::loadStoreProvider(const char* instanceName, const char* libName, const char * factoryMethodName)
{
    if (!libName || !*libName)
       throw MakeStringException(-1, "CwsstoreEx::loadStoreProvider: Library name not provided!");

    HINSTANCE espstorelib = LoadSharedObject(libName, true, false);
    if(!espstorelib)
    {
        StringBuffer realName;
        // add suffix and prefix if needed
        realName.append(SharedObjectPrefix).append(libName).append(SharedObjectExtension);
        espstorelib = LoadSharedObject(realName.str(), true, false);
        if(!espstorelib)
            throw MakeStringException(-1, "CwsstoreEx::loadStoreProvider: Cannot load library '%s'", realName.str());
    }

    newEspStore_t_ xproc = (newEspStore_t_)GetSharedProcedure(espstorelib, factoryMethodName);
    if (!xproc)
        throw MakeStringException(-1, "CwsstoreEx::loadStoreProvider: Cannot load procedure '%s' from library '%s'", factoryMethodName, libName);

    return (IEspStore*) xproc();
}

bool CwsstoreEx::onCreateStore(IEspContext &context, IEspCreateStoreRequest &req, IEspCreateStoreResponse &resp)
{
    const char *user = context.queryUserId();
    m_storeProvider->createStore(req.getType(), req.getName(), req.getDescription(), new CSecureUser(user, nullptr));
    resp.setName(req.getName());
    resp.setType(req.getType());
    resp.setDescription(req.getDescription());
    resp.setOwner(user);

    return true;
}

bool CwsstoreEx::onDelete(IEspContext &context, IEspDeleteRequest &req, IEspDeleteResponse &resp)
{
    const char *user = context.queryUserId();
    const char *storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }
    return m_storeProvider->deletekey(storename, req.getNamespace(), req.getKey(), new CSecureUser(user, nullptr), !req.getUserSpecific());
}

bool CwsstoreEx::onDeleteNamespace(IEspContext &context, IEspDeleteNamespaceRequest &req, IEspDeleteNamespaceResponse &resp)
{
    const char *user = context.queryUserId();
    const char *storename = req.getStoreName();
    bool global = !req.getUserSpecific();
    const char *targetUser = req.getTargetUser();

    if (!global && !isEmptyString(targetUser))
    {
        ESPLOG(LogMin, "CwsstoreEx::onDeleteNamespace: '%s' requesting to delete namespace on behalve of '%s'", user, targetUser);
        user = targetUser;
    }

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    return m_storeProvider->deleteNamespace(storename, req.getNamespace(), new CSecureUser(user, nullptr), !req.getUserSpecific());
}

bool CwsstoreEx::onListNamespaces(IEspContext &context, IEspListNamespacesRequest &req, IEspListNamespacesResponse &resp)
{
    const char *user = context.queryUserId();
    const char *storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    StringArray namespaces;
    m_storeProvider->fetchAllNamespaces(namespaces, storename, new CSecureUser(user, nullptr), !req.getUserSpecific());
    resp.setNamespaces(namespaces);
    return true;
}

bool CwsstoreEx::onListKeys(IEspContext &context, IEspListKeysRequest &req, IEspListKeysResponse &resp)
{
    const char * ns = req.getNamespace();
    const char *user = context.queryUserId();
    const char *storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    StringArray keys;
    m_storeProvider->fetchKeySet(keys, storename, ns, new CSecureUser(user, nullptr), !req.getUserSpecific());
    resp.setKeySet(keys);
    resp.setNamespace(ns);

    return true;
}

bool CwsstoreEx::onSet(IEspContext &context, IEspSetRequest &req, IEspSetResponse &resp)
{
    const char * ns = req.getNamespace();
    const char * key = req.getKey();
    const char * value = req.getValue();
    const char * storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    const char *user = context.queryUserId();
    m_storeProvider->set(storename, ns, key, value, new CSecureUser(user, nullptr), !req.getUserSpecific());
    return true;
}

bool CwsstoreEx::onFetch(IEspContext &context, IEspFetchRequest &req, IEspFetchResponse &resp)
{
    StringBuffer value;
    const char *user = context.queryUserId();
    const char * storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    m_storeProvider->fetch(storename, req.getNamespace(), req.getKey(), value, new CSecureUser(user, nullptr), !req.getUserSpecific());
    resp.setValue(value.str());
    return true;
}

bool CwsstoreEx::onFetchKeyMetadata(IEspContext &context, IEspFetchKeyMDRequest &req, IEspFetchKeyMDResponse &resp)
{
    const char * ns = req.getNamespace();
    const char * user = context.queryUserId();
    const char * storename = req.getStoreName();
    const char * key = req.getKey();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    Owned<IPropertyTree> nstree = m_storeProvider->getAllKeyProperties(storename, ns, key, new CSecureUser(user, nullptr), !req.getUserSpecific());

    if (nstree)
    {
        IArrayOf<IEspKVPair> pairs;

        Owned<IAttributeIterator> attributes = nstree->getAttributes();
        ForEach(*attributes)
        {
            Owned<IEspKVPair> kvpair = createKVPair("","");

            kvpair->setKey(attributes->queryName());
            kvpair->setValue(attributes->queryValue());
            pairs.append(*kvpair.getClear());
        }
        resp.setPairs(pairs);
    }

    resp.setStoreName(storename);
    resp.setNamespace(req.getNamespace());
    resp.setKey(req.getKey());
    return true;
}

bool CwsstoreEx::onFetchAll(IEspContext &context, IEspFetchAllRequest &req, IEspFetchAllResponse &resp)
{
    const char * ns = req.getNamespace();
    const char * user = context.queryUserId();
    const char * storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    Owned<IPropertyTree> nstree = m_storeProvider->getAllPairs(storename, ns, new CSecureUser(user, nullptr), !req.getUserSpecific());

    IArrayOf<IEspKVPair> pairs;

    Owned<IPropertyTreeIterator> iter = nstree->getElements("*");
    ForEach(*iter)
    {
        StringBuffer name;
        StringBuffer value;
        iter->query().getName(name);
        nstree->getProp(name.str(), value);

        Owned<IEspKVPair> kvpair = createKVPair("","");
        kvpair->setKey(name.str());
        kvpair->setValue(value.str());
        pairs.append(*kvpair.getClear());
    }

    resp.setPairs(pairs);
    resp.setNamespace(req.getNamespace());
    return true;
}

