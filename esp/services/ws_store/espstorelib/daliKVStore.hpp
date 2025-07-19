/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef DALIKVSTORE_HPP_
#define DALIKVSTORE_HPP_

#ifdef DALIKVSTORE_EXPORTS
    #define DALIKVSTORE_API DECL_EXPORT
#else
    #define DALIKVSTORE_API DECL_IMPORT
#endif

#include "jiface.hpp"

#include "dautils.hpp"
#include "dasds.hpp"
#include "SecureUser.hpp"
#include "espStoreShare.hpp"

static const char* DALI_KVSTORE_PATH="/KVStore";

class DALIKVSTORE_API CDALIKVStore : public CInterface, implements IEspStore
{
public:
    IMPLEMENT_IINTERFACE;

    CDALIKVStore() {};
    virtual ~CDALIKVStore() {};

    virtual bool init(const char * name, const char * type, IPropertyTree * cfg)
    {
        ensureSDSPath(DALI_KVSTORE_PATH);
        return true;
    }

    IPropertyTree * getStores(const char * namefilter, const char * ownerfilter, const char * typefilter, ISecUser * user);
    bool fetchAllNamespaces(StringArray & namespaces, const char * storename, ISecUser * user, bool global);
    bool createStore(const char * apptype, const char * storename, const char * description, ISecUser * owner, unsigned int maxvalsize);
    bool addNamespace(const char * storename, const char * thenamespace, ISecUser * owner, bool global);
    bool set(const char * storename, const char * thenamespace, const char * key, const char * value, ISecUser * owner, bool global);
    bool fetchKeySet(StringArray & keyset, const char * storename, const char * ns, ISecUser * user, bool global);
    bool fetch(const char * storename, const char * ns, const char * key, StringBuffer & value, ISecUser * user, bool global);
    IPropertyTree * getAllPairs(const char * storename, const char * ns, ISecUser * user, bool global);
    bool deletekey(const char * storename, const char * thenamespace, const char * key, ISecUser * user, bool global);
    bool deleteNamespace(const char * storename, const char * thenamespace, ISecUser * user, bool global);
    bool fetchKeyProperty(StringBuffer & propval , const char * storename, const char * ns, const char * key, const char * property, ISecUser * username, bool global);
    IPropertyTree * getAllKeyProperties(const char * storename, const char * ns, const char * key, ISecUser * username, bool global);

    bool setOfflineMode(bool offline)
    {
        m_isDetachedFromDali = offline;
        return true;
    }

    bool isAttachedToDali()
    {
        return m_isDetachedFromDali;
    }

    void ensureAttachedToDali()
    {
        if (m_isDetachedFromDali)
            throw MakeStringException(-1, "DALI Keystore Unavailable while in offline mode!");
    }

private:
    bool m_isDetachedFromDali = false;
};



class CCfgStore : public CInterface
{
private:

    static constexpr const char* DALI_KVSTORE_CFG_STORE_NAME = "__ConfigStore__";
    static constexpr const char* DALI_KVSTORE_CFG_NAMESPACE = "ConfigStore";
    static constexpr const char* DALI_KVSTORE_CFG_DESCRIPTION = "Component Config Store for ECL Watch Display"; 
    static constexpr unsigned int DALI_KVSTORE_CFG_MAXVALSIZE = 20480; // 20KB - a bit larger than the largest known config, eclwatch

    CDALIKVStore m_store;

public:
    IMPLEMENT_IINTERFACE;

    CCfgStore() {}

    ~CCfgStore() {}

    void init()
    {
        m_store.init("unused", nullptr, nullptr);
        Owned<IPropertyTree> existingStore = m_store.getStores(DALI_KVSTORE_CFG_STORE_NAME, nullptr, nullptr, nullptr);
        VStringBuffer xpath("Store[@name='%s']", DALI_KVSTORE_CFG_STORE_NAME);
        unsigned int count = existingStore->getCount(xpath);

        if (count == 0)
            m_store.createStore(nullptr, DALI_KVSTORE_CFG_STORE_NAME, DALI_KVSTORE_CFG_DESCRIPTION, nullptr, DALI_KVSTORE_CFG_MAXVALSIZE);
    }

    void storeComponentConfig(const char *key, IPropertyTree *value)
    {
        StringBuffer serializedValue;
        if (isContainerized())
            toYAML(value, serializedValue, 2, 0);
        else
            toXML(value, serializedValue);
        m_store.set(DALI_KVSTORE_CFG_STORE_NAME, DALI_KVSTORE_CFG_NAMESPACE, key, serializedValue.str(), nullptr, true);
    }

    bool getComponentConfig(const char *key, StringBuffer &value)
    {
        return m_store.fetch(DALI_KVSTORE_CFG_STORE_NAME, DALI_KVSTORE_CFG_NAMESPACE, key, value, nullptr, true);
    }
};

#endif /* DALIKVSTORE_HPP_ */
