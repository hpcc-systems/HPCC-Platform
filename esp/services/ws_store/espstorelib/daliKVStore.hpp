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

#define SDS_LOCK_TIMEOUT_KVSTORE (30*1000)

static const char* DALI_KVSTORE_PATH="/KVStore";
static const char* DALI_KVSTORE_GLOBAL="GLOBAL";
static const char* DALI_KVSTORE_NAME_ATT="@name";
static const char* DALI_KVSTORE_DESCRIPTION_ATT="@description";
static const char* DALI_KVSTORE_CREATEDBY_ATT="@createUser";
static const char* DALI_KVSTORE_CREATEDTIME_ATT="@createTime";
static const char* DALI_KVSTORE_EDITEDBY_ATT="@editBy";
static const char* DALI_KVSTORE_EDITEDTIME_ATT="@editTime";

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

    bool fetchAllNamespaces(StringArray & namespaces, const char * storename, ISecUser * user, bool global);
    bool createStore(const char * apptype, const char * storename, const char * description, ISecUser * owner);
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

#endif /* DALIKVSTORE_HPP_ */
