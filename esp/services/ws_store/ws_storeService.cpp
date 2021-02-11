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
#define DEFAULT_ESP_STORE_MAX_VAL_SIZE 1024
#define ESP_STORE_NAME_ATT "@name"
#define ESP_STORE_TYPE_ATT "@type"
#define ESP_STORE_DESCRIPTION_ATT "@description"
#define ESP_STORE_MAXVALSIZE_ATT "@maxvalsize"
#define ESP_STORE_DEFAULT_ATT "@default"

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
        Owned<ISecUser> secuser = new CSecureUser(owner.str(), nullptr);

        m_defaultStore.clear();

        Owned<IPropertyTree> stores = m_storeProvider->getStores(nullptr, nullptr, nullptr, secuser.get());

        ForEach(*iter)
        {
            StringBuffer id;
            StringBuffer type;
            StringBuffer description;
            bool isDefault = false;

            iter->query().getProp(ESP_STORE_NAME_ATT, id);
            iter->query().getProp(ESP_STORE_TYPE_ATT, type);
            iter->query().getProp(ESP_STORE_DESCRIPTION_ATT, description);
            unsigned int maxvalsize = iter->query().getPropInt(ESP_STORE_MAXVALSIZE_ATT, DEFAULT_ESP_STORE_MAX_VAL_SIZE);
            isDefault = iter->query().getPropBool(ESP_STORE_DEFAULT_ATT, false);

            VStringBuffer xpath("Stores/Store[%s='%s']", ESP_STORE_NAME_ATT, id.str());
            if (stores && stores->hasProp(xpath.str()))
            {
                ESPLOG(LogMin, "CwsstoreEx: Detected previously created store '%s'.", id.str());
            }
            else
            {
                ESPLOG(LogMin, "CwsstoreEx: Creating Store: '%s'%s", id.str(), isDefault ? " - as Default" : "");

                m_storeProvider->createStore(type.str(), id.str(), description.str(), secuser.get(), maxvalsize);
            }

            if (isDefault)
            {
                if (!m_defaultStore.isEmpty())
                   throw MakeStringException(-1, "ws_store init(): Multiple stores erroneously configured as default store!");

                ESPLOG(LogMin, "CwsstoreEx: setting '%s' as default store", id.str());
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

bool CwsstoreEx::onListStores(IEspContext &context, IEspListStoresRequest &req, IEspListStoresResponse &resp)
{
    const char *user = context.queryUserId();
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);
    double version = context.getClientVersion();

    const char * namefilter = req.getNameFilter();
    const char * ownerfilter = req.getOwnerFilter();
    const char * typefilter  = req.getTypeFilter();

    IArrayOf<IEspStoreInfo> storeinfos;
    Owned<IPropertyTree> stores = m_storeProvider->getStores(namefilter, ownerfilter, typefilter, secuser.get());
    if (stores)
    {
        Owned<IPropertyTreeIterator> iter = stores->getElements("Store");
        ForEach(*iter)
        {
            IPropertyTree * tree = &iter->query();
            Owned<IEspStoreInfo> store = createStoreInfo();
            store->setOwner(tree->queryProp("@createUser"));
            store->setName(tree->queryProp("@name"));
            store->setCreateTime(tree->queryProp("@createTime"));
            store->setType(tree->queryProp("@type"));
            store->setDescription(tree->queryProp("@description"));
            store->setMaxValSize(tree->queryProp("@maxValSize"));
            store->setIsDefault (!m_defaultStore.isEmpty() && stricmp(m_defaultStore.str(), tree->queryProp("@name"))==0);

            storeinfos.append(*store.getClear());
        }
        resp.setStores(storeinfos);
    }
    return true;
}

bool CwsstoreEx::onCreateStore(IEspContext &context, IEspCreateStoreRequest &req, IEspCreateStoreResponse &resp)
{
    const char *user = context.queryUserId();
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);
    double version = context.getClientVersion();
    unsigned int maxvalsize = DEFAULT_ESP_STORE_MAX_VAL_SIZE;

    if (version >= 1.02)
    {
        maxvalsize = req.getMaxValueSize();
    }

    bool success = m_storeProvider->createStore(req.getType(), req.getName(), req.getDescription(), secuser.get(), maxvalsize);

    if (version > 1)
      resp.setSuccess(success);

    resp.setName(req.getName());
    resp.setType(req.getType());
    resp.setDescription(req.getDescription());
    resp.setOwner(user);

    return true;
}

bool CwsstoreEx::onDelete(IEspContext &context, IEspDeleteRequest &req, IEspDeleteResponse &resp)
{
    const char *user = context.queryUserId();
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);
    const char *storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    resp.setSuccess( m_storeProvider->deletekey(storename, req.getNamespace(), req.getKey(), secuser.get(), !req.getUserSpecific()));
    return true;
}

bool CwsstoreEx::onDeleteNamespace(IEspContext &context, IEspDeleteNamespaceRequest &req, IEspDeleteNamespaceResponse &resp)
{
    const char *user = context.queryUserId();
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);
    const char *storename = req.getStoreName();
    bool global = !req.getUserSpecific();
    const char *targetUser = req.getTargetUser();

    if (!global && !isEmptyString(targetUser))
    {
        ESPLOG(LogMin, "CwsstoreEx::onDeleteNamespace: '%s' requesting to delete namespace on behalf of '%s'", user, targetUser);
        user = targetUser;
    }

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    resp.setSuccess(m_storeProvider->deleteNamespace(storename, req.getNamespace(), secuser.get(), !req.getUserSpecific()));
    return true;
}

bool CwsstoreEx::onListNamespaces(IEspContext &context, IEspListNamespacesRequest &req, IEspListNamespacesResponse &resp)
{
    const char *user = context.queryUserId();
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);
    const char *storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    StringArray namespaces;
    m_storeProvider->fetchAllNamespaces(namespaces, storename, secuser.get(), !req.getUserSpecific());
    resp.setNamespaces(namespaces);
    resp.setStoreName(storename);
    return true;
}

bool CwsstoreEx::onListKeys(IEspContext &context, IEspListKeysRequest &req, IEspListKeysResponse &resp)
{
    const char * ns = req.getNamespace();
    const char *user = context.queryUserId();
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);
    const char *storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    StringArray keys;
    m_storeProvider->fetchKeySet(keys, storename, ns, secuser.get(), !req.getUserSpecific());
    resp.setKeySet(keys);
    resp.setNamespace(ns);
    resp.setStoreName(storename);

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
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);
    resp.setSuccess(m_storeProvider->set(storename, ns, key, value, secuser.get(), !req.getUserSpecific()));

    return true;
}

bool CwsstoreEx::onFetch(IEspContext &context, IEspFetchRequest &req, IEspFetchResponse &resp)
{
    StringBuffer value;
    const char *user = context.queryUserId();
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);

    const char * storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    m_storeProvider->fetch(storename, req.getNamespace(), req.getKey(), value, secuser.get(), !req.getUserSpecific());
    resp.setValue(value.str());

    return true;
}

bool CwsstoreEx::onFetchKeyMetadata(IEspContext &context, IEspFetchKeyMDRequest &req, IEspFetchKeyMDResponse &resp)
{
    const char * ns = req.getNamespace();
    const char * user = context.queryUserId();
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);
    const char * storename = req.getStoreName();
    const char * key = req.getKey();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    Owned<IPropertyTree> nstree = m_storeProvider->getAllKeyProperties(storename, ns, key, secuser.get(), !req.getUserSpecific());

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
    Owned<ISecUser> secuser = new CSecureUser(user, nullptr);
    const char * storename = req.getStoreName();

    if (!storename || !*storename)
    {
        if (!m_defaultStore.isEmpty())
            storename = m_defaultStore.get();
    }

    Owned<IPropertyTree> nstree = m_storeProvider->getAllPairs(storename, ns, secuser.get(), !req.getUserSpecific());

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

