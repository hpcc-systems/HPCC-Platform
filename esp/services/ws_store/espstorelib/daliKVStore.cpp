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

#include "daliKVStore.hpp"

bool CDALIKVStore::createStore(const char * apptype, const char * storename, const char * description, ISecUser * owner, unsigned int maxvalsize=DALI_KVSTORE_MAXVALSIZE_DEFAULT)
{
    if (!storename || !*storename)
        throw MakeStringException(-1, "DALI Keystore createStore(): Store name not provided");

    ensureAttachedToDali(); //throws if in offline mode

    Owned<IRemoteConnection> conn = querySDS().connect(DALI_KVSTORE_PATH, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "Unable to connect to DALI KeyValue store root: '%s'", DALI_KVSTORE_PATH);

    Owned<IPropertyTree> root = conn->getRoot();
    if (!root.get())
        throw MakeStringException(-1, "Unable to open DALI KeyValue store root: '%s'", DALI_KVSTORE_PATH);

    VStringBuffer xpath("Store[%s='%s'][1]", DALI_KVSTORE_NAME_ATT,  storename);
    if (root->hasProp(xpath.str()))
    {
        LOG(MCuserInfo,"DALI Keystore createStore(): '%s' entry already exists", storename);
        return false;
    }

    Owned<IPropertyTree> apptree = createPTree();
    apptree->setProp(DALI_KVSTORE_NAME_ATT, storename);
    CDateTime dt;
    dt.setNow();
    StringBuffer str;

    apptree->setProp(DALI_KVSTORE_CREATEDTIME_ATT,dt.getString(str).str());

    if (apptype && *apptype)
        apptree->setProp(DALI_KVSTORE_TYPE_ATT, apptype);

    if (description && *description)
        apptree->setProp(DALI_KVSTORE_DESCRIPTION_ATT, description);

    if (owner && !isEmptyString(owner->getName()))
        apptree->setProp(DALI_KVSTORE_CREATEDBY_ATT, owner->getName());

    if (maxvalsize != 0)
        apptree->setPropInt(DALI_KVSTORE_MAXVALSIZE_ATT, maxvalsize);

    root->addPropTree("Store", LINK(apptree));

    conn->commit();
    return true;
}

bool CDALIKVStore::addNamespace(const char * storename, const char * thenamespace, ISecUser * owner, bool global)
{
    throw MakeStringException(-1, "CDALIKVStore::addNamespace - NOT IMPLEMENTED - USE setkey()");
    return false;
}

bool CDALIKVStore::set(const char * storename, const char * thenamespace, const char * key, const char * value, ISecUser * owner, bool global)
{
    if (isEmptyString(storename))
        throw MakeStringException(-1, "DALI Keystore set(): Store name not provided");

    if (!global && (!owner || isEmptyString(owner->getName())))
        throw MakeStringException(-1, "DALI Keystore set(): Attempting to set non-global entry but owner name not provided");

    if (isEmptyString(thenamespace))
        throw MakeStringException(-1, "DALI Keystore set(): namespace not provided");

    ensureAttachedToDali(); //throws if in offline mode

    VStringBuffer xpath("%s/Store[%s='%s'][1]", DALI_KVSTORE_PATH, DALI_KVSTORE_NAME_ATT, storename);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore set(): Unable to connect to DALI KeyValue store path '%s'", xpath.str()); //rodrigo, not sure if this is too much info

    Owned<IPropertyTree> storetree = conn->getRoot();
    if (!storetree.get())
        throw MakeStringException(-1, "DALI KV Store set(): Unable to access store '%s'", storename); //this store doesn't exist

    int maxval = storetree->getPropInt(DALI_KVSTORE_MAXVALSIZE_ATT, 0);
    if (maxval > 0 && strlen(value) > maxval)
        throw MakeStringException(-1, "DALI Keystore set(): Size of the value exceeds maximum size allowed (%i)", maxval);

    if (global)
        xpath.set(DALI_KVSTORE_GLOBAL);
    else
        xpath.set(owner->getName()).toLowerCase();

    Owned<IPropertyTree> ownertree = storetree->getPropTree(xpath.str());
    if (!ownertree)
        ownertree.setown(createPTree(xpath.str()));

    CDateTime dt;
    dt.setNow();
    StringBuffer str;

    Owned<IPropertyTree> nstree = ownertree->getPropTree(thenamespace);
    if (!nstree)
    {
        nstree.setown(createPTree(thenamespace));
        nstree->setProp(DALI_KVSTORE_CREATEDTIME_ATT,dt.getString(str).str());
    }

    Owned<IPropertyTree> valuetree = nstree->getPropTree(key);
    if (!valuetree)
    {
        nstree->setProp(key, value);
        valuetree.setown(nstree->getPropTree(key));
        valuetree->setProp(DALI_KVSTORE_CREATEDTIME_ATT,dt.getString(str).str());
        valuetree->setProp(DALI_KVSTORE_CREATEDBY_ATT, owner ? owner->getName(): "");
    }
    else
    {
        valuetree->setProp(DALI_KVSTORE_EDITEDTIME_ATT,dt.getString(str).str());
        valuetree->setProp(DALI_KVSTORE_EDITEDBY_ATT, owner ? owner->getName(): "");
        valuetree->setProp(".", value);
    }

    ownertree->setPropTree(thenamespace, LINK(nstree));
    storetree->setPropTree(xpath.str(), LINK(ownertree));

    conn->commit();

    return true;
}

IPropertyTree * CDALIKVStore::getAllKeyProperties(const char * storename, const char * ns, const char * key, ISecUser * username, bool global)
{
    if (isEmptyString(storename))
        throw MakeStringException(-1, "DALI Keystore fetchKeyProperties(): Store name not provided");

    if (!global && (!username || isEmptyString(username->getName())))
        throw MakeStringException(-1, "DALI Keystore fetchKeyProperties(): Attempting to set non-global entry but owner name not provided");

    if (isEmptyString(ns))
        throw MakeStringException(-1, "DALI Keystore fetchKeyProperties(): namespace not provided");

    ensureAttachedToDali(); //throws if in offline mode

    VStringBuffer xpath("%s/Store[%s='%s'][1]/", DALI_KVSTORE_PATH, DALI_KVSTORE_NAME_ATT, storename);

    if (global)
        xpath.append(DALI_KVSTORE_GLOBAL);
    else
    {
        StringBuffer userlowercased;
        userlowercased.set(username->getName()).toLowerCase();
        xpath.append(userlowercased);
    }

    xpath.appendf("/%s/%s", ns, key);

    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore fetchKeyProperties(): Unable to connect to DALI KeyValue store path '%s'", xpath.str()); //rodrigo, not sure if this is too much info

    Owned<IPropertyTree> keytree = conn->getRoot();
    if (!keytree.get())
        throw MakeStringException(-1, "DALI KV Store fetchKeyProperties(): Unable to access key '%s'", key); //this store doesn't exist

    return(keytree->getPropTree("."));
}

bool CDALIKVStore::fetchKeyProperty(StringBuffer & propval , const char * storename, const char * ns, const char * key, const char * property, ISecUser * username, bool global)
{
    if (isEmptyString(storename))
        throw MakeStringException(-1, "DALI Keystore fetchKeyProperty(): Store name not provided");

    if (!global && (!username || isEmptyString(username->getName())))
        throw MakeStringException(-1, "DALI Keystore fetchKeyProperty(): Attempting to set non-global entry but owner name not provided");

    if (isEmptyString(ns))
        throw MakeStringException(-1, "DALI Keystore fetchKeyProperty(): namespace not provided");

    ensureAttachedToDali(); //throws if in offline mode

    VStringBuffer xpath("%s/Store[%s='%s'][1]/", DALI_KVSTORE_PATH, DALI_KVSTORE_NAME_ATT, storename);

    if (global)
        xpath.append(DALI_KVSTORE_GLOBAL);
    else
    {
        StringBuffer userlowercased;
        userlowercased.set(username->getName()).toLowerCase();
        xpath.append(userlowercased);
    }

    xpath.appendf("/%s/%s", ns, key);

    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore fetchKeyProperty(): Unable to connect to DALI KeyValue store path '%s'", xpath.str()); //rodrigo, not sure if this is too much info

    Owned<IPropertyTree> keytree = conn->getRoot();
    if (!keytree.get())
        throw MakeStringException(-1, "DALI KV Store fetchKeyProperty(): Unable to access key '%s'", key); //this store doesn't exist

    keytree->getProp(property,propval.clear());
    return true;
}

bool CDALIKVStore::deletekey(const char * storename, const char * thenamespace, const char * key, ISecUser * user, bool global)
{
    if (!storename || !*storename)
        throw MakeStringException(-1, "DALI Keystore deletekey(): Store name not provided");

    if (!thenamespace || !*thenamespace)
        throw MakeStringException(-1, "DALI KV Store deletekey(): target namespace not provided!");

    if (!key || !*key)
        throw MakeStringException(-1, "DALI KV Store deletekey(): target key not provided!");

    if (!global && (!user || isEmptyString(user->getName())))
        throw MakeStringException(-1, "DALI Keystore set(): Attempting to set non-global entry but user not provided");

    ensureAttachedToDali(); //throws if in offline mode

    VStringBuffer xpath("%s/Store[%s='%s'][1]", DALI_KVSTORE_PATH, DALI_KVSTORE_NAME_ATT, storename);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore deletekey(): Unable to connect to DALI KeyValue store root path '%s'", DALI_KVSTORE_PATH);

    Owned<IPropertyTree> storetree = conn->getRoot();

    if(!storetree.get())
        throw MakeStringException(-1, "DALI Keystore deletekey(): invalid store name '%s' detected!", storename);

    if (global)
        xpath.set(DALI_KVSTORE_GLOBAL);
    else
        xpath.set(user->getName()).toLowerCase();

    xpath.appendf("/%s/%s", thenamespace,key);
    if(!storetree->hasProp(xpath.str()))
        throw MakeStringException(-1, "DALI KV Store deletekey(): Could not find '%s/%s/%s' for user '%s'", storename, thenamespace, key, global ? "GLOBAL USER" : user->getName());

    storetree->removeProp(xpath.str());

    conn->commit();

    return true;
}

bool CDALIKVStore::deleteNamespace(const char * storename, const char * thenamespace, ISecUser * user, bool global)
{
    if (!storename || !*storename)
        throw MakeStringException(-1, "DALI Keystore deletekey(): Store name not provided");

    if (!global && (!user || isEmptyString(user->getName())))
        throw MakeStringException(-1, "DALI Keystore deleteNamespace(): Attempting to fetch non-global keys but user not provided");

    if (isEmptyString(thenamespace))
       throw MakeStringException(-1, "DALI KV Store deleteNamespace(): target namespace not provided!");

    ensureAttachedToDali(); //throws if in offline mode

    VStringBuffer xpath("%s/Store[%s='%s']", DALI_KVSTORE_PATH, DALI_KVSTORE_NAME_ATT, storename);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore deleteNamespace(): Unable to connect to DALI KeyValue store path '%s'", xpath.str());

    Owned<IPropertyTree> storetree = conn->getRoot();

    if(!storetree.get())
        throw MakeStringException(-1, "DALI Keystore deleteNamespace(): invalid store name '%s' detected!", storename);

    if (global)
        xpath.set(DALI_KVSTORE_GLOBAL);
    else
        xpath.set(user->getName()).toLowerCase();

    xpath.appendf("/%s", thenamespace); //we're interested in the children of the namespace
    if(!storetree->hasProp(xpath.str()))
        throw MakeStringException(-1, "DALI KV Store deleteNamespace(): invalid namespace detected '%s/%s' for user '%s'", storename, thenamespace, global ? "GLOBAL USER" : user->getName());

    storetree->removeProp(xpath.str());

    conn->commit();

    return true;
}

bool CDALIKVStore::fetchAllNamespaces(StringArray & namespaces, const char * storename, ISecUser * user, bool global)
{
    if (!storename || !*storename)
        throw MakeStringException(-1, "DALI Keystore fetchAllNamespaces(): Store name not provided");

     if (!global && (!user || isEmptyString(user->getName())))
        throw MakeStringException(-1, "DALI Keystore fetchAllNamespaces(): Attempting to fetch non-global keys but requester name not provided");

    ensureAttachedToDali(); //throws if in offline mode

    VStringBuffer xpath("%s/Store[%s='%s']", DALI_KVSTORE_PATH, DALI_KVSTORE_NAME_ATT, storename);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore fetchAllNamespaces: Unable to connect to DALI KeyValue store path '%s'", xpath.str());

    Owned<IPropertyTree> storetree = conn->getRoot();
    if(!storetree.get())
        throw MakeStringException(-1, "DALI Keystore fetchAllNamespaces: invalid store name '%s' detected!", storename);

    if (global)
        xpath.setf("%s/*", DALI_KVSTORE_GLOBAL); //we're interested in the children of the namespace
    else
        xpath.setf("%s/*", user->getName()).toLowerCase(); //we're interested in the children of the namespace

    StringBuffer name;
    Owned<IPropertyTreeIterator> iter = storetree->getElements(xpath.str());
    ForEach(*iter)
    {
        iter->query().getName(name.clear());
        namespaces.append(name.str());
    }

    return true;
}

bool CDALIKVStore::fetchKeySet(StringArray & keyset, const char * storename, const char * ns, ISecUser * user, bool global)
{
    if (!storename || !*storename)
        throw MakeStringException(-1, "DALI Keystore fetchKeySet(): Store name not provided");

     if (!global && (!user || isEmptyString(user->getName())))
        throw MakeStringException(-1, "DALI Keystore fetchKeySet(): Attempting to fetch non-global keys but requester name not provided");

    if (isEmptyString(ns))
        throw MakeStringException(-1, "DALI Keystore fetchKeySet: Namespace not provided!");

    ensureAttachedToDali(); //throws if in offline mode

    VStringBuffer xpath("%s/Store[%s='%s']", DALI_KVSTORE_PATH, DALI_KVSTORE_NAME_ATT, storename);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore fetchKeySet: Unable to connect to DALI KeyValue store path '%s'", DALI_KVSTORE_PATH);

    Owned<IPropertyTree> storetree = conn->getRoot();

    if(!storetree.get())
        throw MakeStringException(-1, "DALI Keystore fetchKeySet: invalid store name '%s' detected!", storename);

    if (global)
        xpath.set(DALI_KVSTORE_GLOBAL);
    else
        xpath.set(user->getName()).toLowerCase();

    xpath.appendf("/%s/*", ns); //we're interested in the children of the namespace
    if(!storetree->hasProp(xpath.str()))
        throw MakeStringException(-1, "DALI Keystore fetchKeySet: invalid namespace '%s' detected!", ns);

    StringBuffer name;
    Owned<IPropertyTreeIterator> iter = storetree->getElements(xpath);
    ForEach(*iter)
    {
        iter->query().getName(name.clear());
        keyset.append(name.str());
    }

    return true;
}

bool CDALIKVStore::fetch(const char * storename, const char * ns, const char * key, StringBuffer & value, ISecUser * user, bool global)
{
    if (!storename || !*storename)
        throw MakeStringException(-1, "DALI Keystore fetch(): Store name not provided");

     if (!global && (!user || isEmptyString(user->getName())))
        throw MakeStringException(-1, "DALI Keystore fetch(): Attempting to fetch non-global entry but requester name not provided");

     if (isEmptyString(ns))
         throw MakeStringException(-1, "DALI Keystore fetch: key not provided!");

    ensureAttachedToDali(); //throws if in offline mode

    VStringBuffer xpath("%s/Store[%s='%s']", DALI_KVSTORE_PATH, DALI_KVSTORE_NAME_ATT, storename);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore fetch: Unable to connect to DALI KeyValue store path '%s'", xpath.str());

    Owned<IPropertyTree> storetree = conn->getRoot();

    if(!storetree.get())
        throw MakeStringException(-1, "DALI Keystore fetch: invalid store name '%s' detected!", storename);

    if (global)
        xpath.set(DALI_KVSTORE_GLOBAL);
    else
        xpath.set(user->getName()).toLowerCase();

    xpath.appendf("/%s", ns);
    if(!storetree->hasProp(xpath.str()))
        throw MakeStringException(-1, "DALI Keystore fetch: invalid namespace '%s' detected!", ns);

    if (key && *key)
    {
        xpath.appendf("/%s", key);
        if(!storetree->hasProp(xpath.str()))
        {
            throw makeStringExceptionV(ECLWATCH_INVALID_QUERY_KEY, "DALI Keystore fetch: invalid key '%s' detected!", key);
        }
        else
        {
            value.set(storetree->queryProp(xpath.str()));
        }

        return value.str();
    }
    else
        throw makeStringException(-1, "DALI Keystore fetch: Key not provided!");

    return true;
}

IPropertyTree * CDALIKVStore::getAllPairs(const char * storename, const char * ns, ISecUser * user, bool global)
{
    if (!storename || !*storename)
        throw MakeStringException(-1, "DALI Keystore fetchAll(): Store name not provided");

    if (!global && (!user || isEmptyString(user->getName())))
        throw MakeStringException(-1, "DALI Keystore fetchAll(): Attempting to fetch non-global entries but requester name not provided");

    if (isEmptyString(ns))
        throw MakeStringException(-1, "DALI Keystore fetchAll: Namespace not provided!");

    ensureAttachedToDali(); //throws if in offline mode

    VStringBuffer xpath("%s/Store[%s='%s']", DALI_KVSTORE_PATH, DALI_KVSTORE_NAME_ATT, storename);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore fetchAll: Unable to connect to DALI KeyValue store path '%s'", xpath.str());

    Owned<IPropertyTree> storetree = conn->getRoot();
    if (!storetree.get())
        throw MakeStringException(-1, "DALI Keystore fetchAll: invalid store name '%s' detected!", storename);

    if (global)
        xpath.set(DALI_KVSTORE_GLOBAL);
    else
        xpath.set(user->getName()).toLowerCase();

    xpath.appendf("/%s", ns);
    if(!storetree->hasProp(xpath.str()))
        throw MakeStringException(-1, "DALI Keystore fetchAll: invalid namespace '%s' detected!", ns);

    return(storetree->getPropTree(xpath.str()));
}

static bool wildcardmatch(const char *filter, const char * value, bool casesensitive = false)
{
    if (isEmptyString(filter) && isEmptyString(value))
        return true;

    if (*filter == '*' && *(filter+1) != 0 && *value == 0)
        return false;

    if (*filter == '?' || (casesensitive ? *filter == *value : tolower(*filter) == tolower(*value)))
        return wildcardmatch(filter+1, value+1, casesensitive);

    if (*filter == '*')
        return wildcardmatch(filter+1, value, casesensitive) || wildcardmatch(filter, value+1, casesensitive); //already lowercased if needed
    return false;
}

IPropertyTree * CDALIKVStore::getStores(const char * namefilter, const char * ownerfilter, const char * typefilter, ISecUser * user)
{
    ensureAttachedToDali(); //throws if in offline mode

    if (isEmptyString(namefilter))
        namefilter="*";

    VStringBuffer xpath("%s", DALI_KVSTORE_PATH);
    Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_KVSTORE);
    if (!conn)
        throw MakeStringException(-1, "DALI Keystore fetch: Unable to connect to DALI KeyValue store path '%s'", xpath.str());

    Owned<IPropertyTree> filteredstores = createPTree("Stores");
    Owned<IPropertyTree> storetree = conn->getRoot();

    if(!storetree.get())
        throw MakeStringException(-1, "DALI Keystore fetch: ");

      StringBuffer name;
      Owned<IPropertyTreeIterator> iter = storetree->getElements("*");
      ForEach(*iter)
      {
          name.set(iter->query().queryProp(DALI_KVSTORE_NAME_ATT));
          if (name.length() == 0 || !wildcardmatch(namefilter, name.str()))
              continue;

          if (!isEmptyString(ownerfilter))
          {
              const char * owner = iter->query().queryProp(DALI_KVSTORE_CREATEDBY_ATT);
              if (!isEmptyString(owner) && !wildcardmatch(ownerfilter, owner))
                  continue;
          }
          if (!isEmptyString(typefilter))
          {
              const char * type = iter->query().queryProp(DALI_KVSTORE_TYPE_ATT);
              if (!isEmptyString(type) && !wildcardmatch(typefilter, type))
                  continue;
          }
          filteredstores->addPropTree("Store", LINK(&iter->query()));
      }
    return(filteredstores.getClear());
}

extern "C"
{
    DALIKVSTORE_API IEspStore* newEspStore()
    {
        return new CDALIKVStore();
    }
}
