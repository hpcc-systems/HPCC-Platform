/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#include "jexcept.hpp"
#include "jsmartsock.hpp"
#include "dasds.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dadfs.hpp"
#include "dasess.hpp"
#include "dautils.hpp"
#include "wsexcept.hpp"
#include "httpclient.hpp"
#include "espcontext.hpp"
#include "esdl_store.hpp"
#include "esdl_binding.hpp"
#include <memory>

static const char* ESDL_DEFS_ROOT_PATH="/ESDL/Definitions/";
static const char* ESDL_DEF_PATH="/ESDL/Definitions/Definition";
static const char* ESDL_DEF_ENTRY="Definition";
static const char* ESDL_BINDINGS_ROOT_PATH="/ESDL/Bindings/";
static const char* ESDL_BINDING_PATH="/ESDL/Bindings/Binding";
static const char* ESDL_BINDING_ENTRY="Binding";

extern bool trimXPathToParentSDSElement(const char *element, const char * xpath, StringBuffer & parentNodeXPath);

class CEsdlSDSStore : implements IEsdlStore, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CEsdlSDSStore()
    {
        ensureSDSPath(ESDL_DEFS_ROOT_PATH);
        ensureSDSPath(ESDL_BINDINGS_ROOT_PATH);
    }
    virtual ~CEsdlSDSStore() { }

    virtual void fetchDefinition(const char* definitionId, StringBuffer& esxdl) override
    {
        if (!definitionId || !*definitionId)
            throw MakeStringException(-1, "Unable to fetch ESDL Service definition information, definition id is not available");

        DBGLOG("ESDL Binding: Fetching ESDL Definition from Dali: %s ", definitionId);

        VStringBuffer xpath("%s[@id='%s'][1]/esxdl", ESDL_DEF_PATH, definitionId);
        Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
           throw MakeStringException(-1, "Unable to connect to ESDL Service definition information in dali '%s'", xpath.str());

        conn->close(false); //release lock right away

        IPropertyTree * deftree = conn->queryRoot();
        if (!deftree)
           throw MakeStringException(-1, "Unable to open ESDL Service definition information in dali '%s'", xpath.str());

        //There shouldn't be multiple entries here, but if so, we'll use the first one
        toXML(deftree, esxdl, 0, 0);
    }

    virtual void fetchLatestDefinition(const char* definitionName, StringBuffer& esxdl) override
    {
        if (!definitionName || !*definitionName)
            throw MakeStringException(-1, "Unable to fetch ESDL Service definition information, definition name is not available");

        DBGLOG("ESDL Binding: Fetching ESDL Definition from Dali based on name: %s ", definitionName);

        Owned<IRemoteConnection> conn = querySDS().connect(ESDL_DEFS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
           throw MakeStringException(-1, "Unable to connect to ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

        IPropertyTree * esdlDefinitions = conn->queryRoot();

        VStringBuffer xpath("%s[@name='%s']", ESDL_DEF_ENTRY, definitionName);
        Owned<IPropertyTreeIterator> iter = esdlDefinitions->getElements(xpath.str());

        unsigned latestSeq = 1;
        ForEach(*iter)
        {
            IPropertyTree &item = iter->query();
            unsigned thisSeq = item.getPropInt("@seq");
            if (thisSeq > latestSeq)
                latestSeq = thisSeq;
        }

        xpath.setf("%s[@id='%s.%d'][1]/esxdl", ESDL_DEF_ENTRY, definitionName, latestSeq);
        IPropertyTree * deftree = esdlDefinitions->getPropTree(xpath);
        if (deftree)
            toXML(deftree, esxdl, 0,0);
        else
            throw MakeStringException(-1, "Unable to fetch ESDL Service definition from dali: '%s'", definitionName);
    }

    virtual IPropertyTree* fetchBinding(const char* espProcess, const char* espStaticBinding) override
    {
        try
        {
            //There shouldn't be multiple entries here, but if so, we'll use the first one
            VStringBuffer xpath("%s[@id='%s.%s'][1]", ESDL_BINDING_ENTRY, espProcess, espStaticBinding);

            Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);
            if (!conn)
            {
                ESPLOG(LogMin, "Unable to connect to ESDL Service binding information in dali %s", ESDL_BINDINGS_ROOT_PATH);
                return nullptr;
            }

            ESPLOG(LogNormal, "ESDL Binding: Fetching ESDL Binding from Dali %s[@EspProcess='%s'][@EspBinding='%s'][1]", ESDL_BINDING_ENTRY, espProcess, espStaticBinding);

            if (conn->queryRoot()->hasProp(xpath))
                return createPTreeFromIPT(conn->queryRoot()->queryPropTree(xpath));
            else
                return nullptr;
        }
        catch (IException *E)
        {
            VStringBuffer message("ESDL Binding: Error fetching ESDL Binding %s[@EspProcess='%s'][@EspBinding='%s'][1] from Dali.", ESDL_BINDING_ENTRY, espProcess, espStaticBinding);
            EXCLOG(E, message);
            E->Release();
        }
        catch(...)
        {
            ESPLOG(LogMin, "ESDL Binding: Unknown error encountered while fetching ESDL Binding from Dali %s[@EspProcess='%s'][@EspBinding='%s'][1]", ESDL_BINDING_ENTRY, espProcess, espStaticBinding);
        }

        return nullptr;
    }

    virtual bool addDefinition(IPropertyTree* definitionRegistry, const char* definitionName, IPropertyTree* definitionInfo, StringBuffer& newId, unsigned& newSeq, const char* userid, bool deleteprev, StringBuffer & message) override
    {
        StringBuffer lcName(definitionName);
        lcName.toLowerCase();
        StringBuffer xpath;
        xpath.append(ESDL_DEF_ENTRY).append("[@name=\"").append(lcName.str()).append("\"]");

        Owned<IPropertyTreeIterator> iter = definitionRegistry->getElements(xpath);

        newSeq = 1;
        ForEach(*iter)
        {
            IPropertyTree &item = iter->query();
            unsigned thisSeq = item.getPropInt("@seq");
            if (thisSeq >= newSeq)
                newSeq = thisSeq + 1;
        }

        StringBuffer origTimestamp;
        StringBuffer origOwner;

        if (deleteprev && newSeq > 1)
        {
            if (!isDefinitionBound(lcName, newSeq -1))
            {
                newSeq--;
                xpath.appendf("[@seq='%d']", newSeq);

                IPropertyTree * definition = definitionRegistry->queryPropTree(xpath);
                if (definition)
                {
                    origTimestamp.set(definition->queryProp("@created"));
                    origOwner.set(definition->queryProp("@publishedBy"));
                    definitionRegistry->removeTree(definition);
                }
                else
                {
                    message.setf("Could not overwrite Definition: '%s.%d'", definitionName, newSeq);
                    ESPLOG(LogMin, "%s", message.str());
                    return false;
                }
            }
            else
            {
                message.setf("Will not delete previous ESDL definition version because it is referenced in an ESDL binding.");
                ESPLOG(LogMin, "%s", message.str());
                return false;
            }
        }

        CDateTime dt;
        dt.setNow();
        StringBuffer str;

        newId.set(lcName).append(".").append(newSeq);
        definitionInfo->setProp("@name", lcName);
        definitionInfo->setProp("@id", newId);
        definitionInfo->setPropInt("@seq", newSeq);
        if (origOwner.length())
        {
            definitionInfo->setProp("@lastEditedBy", (userid && *userid) ? userid : "Anonymous") ;
            definitionInfo->setProp("@publishedBy", origOwner.str()) ;
        }
        else
            definitionInfo->setProp("@publishedBy", (userid && *userid) ? userid : "Anonymous") ;

        if (origTimestamp.length())
        {
            definitionInfo->setProp("@created", origTimestamp.str());
            definitionInfo->setProp("@lastEdit",dt.getString(str).str());
        }
        else
            definitionInfo->setProp("@created",dt.getString(str).str());

        definitionRegistry->addPropTree(ESDL_DEF_ENTRY, LINK(definitionInfo));
        return true;
    }

    virtual IPropertyTree* getDefinitionRegistry(bool readonly) override
    {
        Owned<IRemoteConnection> conn = querySDS().connect(ESDL_DEFS_ROOT_PATH, myProcessSession(), readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
            throw MakeStringException(-1, "Unexpected error while attempting to access ESDL definition dali registry.");
        return (conn) ? conn->getRoot() : NULL;
    }

    virtual bool definitionExists(const char* definitionId) override
    {
        bool found = false;
        if (!definitionId)
            return found;

        StringBuffer lcid (definitionId);
        lcid.toLowerCase();
        VStringBuffer xpath("%s[@id='%s']", ESDL_DEF_PATH, lcid.str());
        Owned<IRemoteConnection> globalLock = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);

        if (globalLock)
        {
            found = true;
            globalLock->close(false);
        }

        return found;
    }

    virtual bool isMethodDefined(const char* definitionId, StringBuffer& esdlServiceName, const char* methodName) override
    {
        bool found = false;
        if (!definitionId || !*definitionId)
            return found;

        StringBuffer lcdefid (definitionId);
        lcdefid.toLowerCase();
        VStringBuffer xpath("%s[@id='%s']/esxdl", ESDL_DEF_PATH, lcdefid.str());
        Owned<IRemoteConnection> globalLock = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);

        if (globalLock)
        {
            IPropertyTree * esxdl = globalLock->queryRoot();
            if (esxdl)
            {
                Owned<IPropertyTreeIterator> it = esxdl->getElements("EsdlService");
                int servicesCount = esxdl->getCount("EsdlService");

                ForEach(*it)
                {
                    IPropertyTree* pCurrService = &it->query();
                    if ((servicesCount == 1 && !esdlServiceName.length()) || stricmp(pCurrService->queryProp("@name"), esdlServiceName.str())==0)
                    {
                        Owned<IPropertyTreeIterator> it2 = pCurrService->getElements("EsdlMethod");
                        ForEach(*it2)
                        {
                            IPropertyTree* pChildNode = &it2->query();
                            if (stricmp(pChildNode->queryProp("@name"), methodName)==0)
                            {
                                if (!esdlServiceName.length())
                                    esdlServiceName.set(pCurrService->queryProp("@name"));
                                found = true;
                                break;
                            }
                        }
                    }
                    if (found)
                        break;
                }
            }

            globalLock->close(false);
        }
        globalLock.clear();
        return found;
    }

    virtual int configureMethod(const char* espProcName, const char* espBindingName, const char* definitionId, const char* methodName, IPropertyTree* configTree, bool overwrite, StringBuffer& message) override
    {
        if (!espProcName || !*espProcName)
        {
            message.set("Unable to configure method, ESP Process Name not available");
            return -1;
        }
        if (!espBindingName || !*espBindingName)
        {
            message.setf("Unable to configure method, ESP Binding Name not available");
            return -1;
        }
        if (!definitionId || !*definitionId)
        {
            message.setf("Unable to configure method, ESDL Binding ID not available");
            return -1;
        }
        if (!methodName || !*methodName)
        {
            message.setf("Unable to configure method, name not available");
            return -1;
        }
        if (!configTree)
        {
            message.setf("Unable to configure method '%s', configuration attributes not available", methodName);
            return -1;
        }

        VStringBuffer rxpath("%sBinding[@espprocess='%s'][@espbinding='%s']/Definition[@id='%s']/Methods", ESDL_BINDINGS_ROOT_PATH, espProcName, espBindingName, definitionId);

        Owned<IRemoteConnection> conn;

        try
        {
            conn.setown(querySDS().connect(rxpath, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_DESDL));
        }
        catch (ISDSException * e)
        {
            if (conn)
                conn->close(true);

            message.setf("Unable to operate on Dali path: %s", rxpath.str());
            e->Release();
            return -1;
        }

        //Only lock the branch for the target we're interested in.
        if (!conn)
            throw MakeStringException(-1, "Unable to connect to %s", rxpath.str());

        Owned<IPropertyTree> root = conn->getRoot();
        if (!root.get())
            throw MakeStringException(-1, "Unable to open %s", rxpath.str());

        VStringBuffer xpath("Method[@name='%s']", methodName);
        Owned<IPropertyTree> oldEnvironment = root->getPropTree(xpath.str());
        if (oldEnvironment.get())
        {
            if (overwrite)
            {
                message.set("Existing method configuration overwritten!");
                root->removeTree(oldEnvironment);
            }
            else
            {
                message.set("Method configuration exists will not overwrite!");
                return -1;
            }
        }

        root->addPropTree("Method", configTree);

        conn->commit();
        conn->close(false);

        message.appendf("\nSuccessfully configured Method '%s', associated with ESDL definition '%s', on ESP '%s' and binding '%s'", methodName, definitionId, espProcName, espBindingName);
        return 0;
    }

    virtual int bindService(const char* bindingName,
                                             IPropertyTree* methodsConfig,
                                             const char* espProcName,
                                             const char* espPort,
                                             const char* definitionId,
                                             const char* esdlServiceName,
                                             StringBuffer& message,
                                             bool overwrite,
                                             const char* user) override
    {
        if (!definitionId || !*definitionId)
        {
            message.set("Could not configure DESDL service: Target Esdl definition id not available");
            return -1;
        }

        if (!esdlServiceName || !*esdlServiceName)
        {
            message.set("Could not configure DESDL service: Target Esdl definition service name not available");
            return -1;
        }

        if (!bindingName || !*bindingName)
        {
            message.setf("Could not configure '%s' - Target binding name not available", esdlServiceName);
            return -1;
        }

        Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_WRITE | RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
           throw MakeStringException(-1, "Unexpected error while attempting to access ESDL definition dali registry.");

        IPropertyTree * bindings = conn->queryRoot();

        StringBuffer xpath;
        xpath.appendf("%s[@id='%s.%s']", ESDL_BINDING_ENTRY, espProcName, bindingName);

        bool duplicateBindings = bindings->hasProp(xpath.str());

        StringBuffer origTimestamp;
        StringBuffer origOwner;

        if (duplicateBindings)
        {
            if (overwrite)
            {
                IPropertyTree * binding = bindings->queryPropTree(xpath);
                if (binding)
                {
                    origTimestamp.set(binding->queryProp("@created"));
                    origOwner.set(binding->queryProp("@publishedBy"));
                    bindings->removeTree(binding);
                }
                else
                {
                    message.setf("Could not overwrite binding '%s.%s'!", espProcName, bindingName);
                    conn->close(false);
                    return -1;
                }
            }
            else
            {
               message.setf("Could not configure Service '%s' because this service has already been configured for binding '%s' on ESP Process '%s'", esdlServiceName, bindingName, espProcName);
               conn->close(false);
               return -1;
            }
        }

        VStringBuffer qbindingid("%s.%s", espProcName, bindingName);
        Owned<IPropertyTree> bindingtree  = createPTree();
        bindingtree->setProp("@espprocess", espProcName);
        bindingtree->setProp("@espbinding", bindingName);
        bindingtree->setProp("@id", qbindingid.str());

        CDateTime dt;
        dt.setNow();
        StringBuffer str;

        if (origTimestamp.length())
        {
            bindingtree->setProp("@created", origTimestamp.str());
            bindingtree->setProp("@lastEdit", dt.getString(str).str());
        }
        else
            bindingtree->setProp("@created",  dt.getString(str).str());

        if (origOwner.length())
        {
            bindingtree->setProp("@publisheBy", origOwner.str()) ;
            bindingtree->setProp("@lastEditedBy", (user && *user) ? user : "Anonymous");
        }
        else
            bindingtree->setProp("@publishedBy", (user && *user) ? user : "Anonymous") ;

        StringBuffer lcId(definitionId);
        lcId.toLowerCase();
        StringBuffer esdlDefinitionName;
        int esdlver = 0;
        const char * esdlDefId = definitionId;
        while(esdlDefId && *esdlDefId && *esdlDefId != '.')
            esdlDefinitionName.append(*esdlDefId++);
        if(esdlDefId && *esdlDefId == '.')
            esdlDefId++;
        if (esdlDefId && *esdlDefId)
            esdlver = atoi(esdlDefId);
        if (esdlver <= 0)
            esdlver = 1;
        StringBuffer newId;
        newId.set(esdlDefinitionName).append(".").append(esdlver);

        Owned<IPropertyTree> esdldeftree  = createPTree();

        esdldeftree->setProp("@name", esdlDefinitionName.str());
        esdldeftree->setProp("@id", newId);
        esdldeftree->setProp("@esdlservice", esdlServiceName);


        if(methodsConfig != nullptr)
            esdldeftree->addPropTree("Methods", LINK(methodsConfig));
        else
            esdldeftree->addPropTree("Methods");

        bindingtree->addPropTree(ESDL_DEF_ENTRY, LINK(esdldeftree));
        bindings->addPropTree(ESDL_BINDING_ENTRY, LINK(bindingtree));

        conn->commit();
        conn->close(false);
        message.setf("Successfully configured Service '%s', associated with ESDL definition '%s', on ESP '%s' and binding '%s'", esdlServiceName, newId.str(), espProcName, bindingName);

        ESPLOG(LogMin, "ESDL Binding '%s' published by user='%s' overwrite flag: %s", newId.str(), (user && *user) ? user : "Anonymous", overwrite ? "TRUE" : "FALSE");
        return 0;
    }

    virtual IPropertyTree* getBindingTree(const char* espProcName, const char* espBindingName, StringBuffer& msg) override
    {
        if (!espProcName || !*espProcName)
        {
            msg.set("Could not get configuration: Target ESP proc name not available");
            return NULL;
        }

        if (!espBindingName || !*espBindingName)
        {
            msg.set("Could not get configuration: Target ESP Binding name not available");
            return NULL;
        }

        VStringBuffer xpath("%s[@espprocess='%s'][@espbinding='%s']", ESDL_BINDING_PATH, espProcName, espBindingName);
        Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(), myProcessSession(), RTM_LOCK_READ , SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
        {
            msg.setf("Could not find binding for ESP proc: %s, and binding: %s", espProcName, espBindingName);
            return NULL;
        }
        else
        {
            conn->close(false);
            return conn->getRoot();
        }
    }

    virtual bool deleteDefinition(const char* definitionId, StringBuffer& errmsg, StringBuffer* defxml) override
    {
        if (!definitionId || !*definitionId)
            return false;
        bool ret = false;
        Owned<IRemoteConnection> conn = querySDS().connect(ESDL_DEFS_ROOT_PATH, myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
            throw MakeStringException(-1, "Unable to connect to %s dali path", ESDL_DEFS_ROOT_PATH);

        Owned<IPropertyTree> root = conn->getRoot();

        if (!root)
            throw MakeStringException(-1, "Unable to open %s dali path", ESDL_DEFS_ROOT_PATH);

        if (isDefinitionBound(definitionId))
            throw MakeStringException(-1, "Unable to delete ESDL definition %s - It is currently bound", definitionId);

        VStringBuffer xpath("%s[@id='%s']", ESDL_DEF_ENTRY, definitionId);
        Owned<IPropertyTree> oldEnvironment = root->getPropTree(xpath.str());
        if (oldEnvironment.get())
        {
            if(defxml)
                toXML(oldEnvironment.get(), *defxml,0,0);
            root->removeTree(oldEnvironment);
            conn->commit();
            ret = true;
        }
        else
        {
            errmsg.append("Could not find ESDL definition. Verify Id (name.version).");
        }
        conn->close();
        return ret;
    }

    virtual bool deleteBinding(const char* bindingId, StringBuffer& errmsg, StringBuffer* bindingxml) override
    {
        if (!bindingId || !*bindingId)
            return false;
        bool ret = false;
        Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), 0, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
            throw MakeStringException(-1, "Unable to connect to %s dali path", ESDL_BINDINGS_ROOT_PATH);

        Owned<IPropertyTree> root = conn->getRoot();
        if (!root)
            throw MakeStringException(-1, "Unable to open %s dali path", ESDL_BINDINGS_ROOT_PATH);

        VStringBuffer xpath("%s[@id='%s']", ESDL_BINDING_ENTRY, bindingId);
        Owned<IPropertyTree> oldEnvironment = root->getPropTree(xpath.str());
        if (oldEnvironment.get())
        {
            if(bindingxml)
                toXML(oldEnvironment.get(), *bindingxml,0,0);
            root->removeTree(oldEnvironment);
            conn->commit();
            ret = true;
        }
        else
        {
            errmsg.append("Could not find ESDL Binding.");
        }
        conn->close();
        return ret;
    }

    virtual IPropertyTree* getDefinitions() override
    {
        Owned<IRemoteConnection> conn = querySDS().connect(ESDL_DEFS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
           throw MakeStringException(-1, "Unable to connect to ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

        conn->close(false); //release lock right away

        Owned<IPropertyTree> esdlDefinitions = conn->getRoot();
        if (!esdlDefinitions.get())
           throw MakeStringException(-1, "Unable to open ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

        return esdlDefinitions.getLink();
    }

    virtual IPropertyTree* getBindings() override
    {
        Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
           throw MakeStringException(-1, "Unable to connect to ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

        conn->close(false); //release lock right away

        Owned<IPropertyTree> esdlBindings = conn->getRoot();
        if (!esdlBindings.get())
           throw MakeStringException(-1, "Unable to open ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);
        return esdlBindings.getLink();
    }

private:
    bool isDefinitionBound(const char* esdldefid)
    {
        if (!esdldefid || !*esdldefid)
               return false;

        Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
           return false;

        conn->close(false);

        StringBuffer lcName(esdldefid);
        lcName.toLowerCase();

        IPropertyTree * bindings = conn->queryRoot();

        if (!bindings)
           return false;

        VStringBuffer xpath("%s/Definition[@id='%s']", ESDL_BINDING_ENTRY, lcName.str());

        bool has = bindings->hasProp(xpath);

        return has;
    }

    bool isDefinitionBound(const char* esdldefname, int version)
    {
        if (!esdldefname || !*esdldefname)
            return false;

        if (version <= 0)
            return false;

        StringBuffer id;
        id.appendf("%s.%d", esdldefname, version);

        return isDefinitionBound(id);
    }
};

class CEsdlSDSSubscription : implements IEsdlSubscription, public CInterface, implements ISDSSubscription
{
private:
    IEsdlListener* mListener;
    CriticalSection daliSubscriptionCritSec;
    SubscriptionId binding_sub_id;
    SubscriptionId definition_sub_id;
    StringBuffer mProcessName, mBindingName;
public:
    IMPLEMENT_IINTERFACE;
    CEsdlSDSSubscription(IEsdlListener* listener, const char* process, const char* binding)
    {
        mListener = listener;
        if(process)
            mProcessName.set(process);
        if(binding)
            mBindingName.set(binding);
        binding_sub_id = 0;
        definition_sub_id = 0;
        subscribe();
    }

    virtual ~CEsdlSDSSubscription()
    {
        unsubscribe();
    }

    virtual void unsubscribe()
    {
        CriticalBlock b(daliSubscriptionCritSec);
        try
        {
            if (binding_sub_id)
            {
                querySDS().unsubscribe(binding_sub_id);
                binding_sub_id = 0;
            }
            if (definition_sub_id)
            {
                querySDS().unsubscribe(definition_sub_id);
                definition_sub_id = 0;
            }
            DBGLOG("Esdl SDS subscription successfully unsubscribed.");
        }
        catch (IException *E)
        {
            StringBuffer msg;
            E->errorMessage(msg);
            DBGLOG("Error unsubscribing: %s", msg.str());
            E->Release();
        }
    }

    void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        if (!mListener)
        {
            DBGLOG("Can't handle subscription notification because the listener is NULL");
            return;
        }
        EsdlNotifyType ntype = EsdlNotifyType::BindingUpdate;
        bool isBindingNotify = false;
        if (id != binding_sub_id && id != definition_sub_id)
        {
            DBGLOG("ESDL Binding %s.%s Dali subscription received notification for unrecognized dali subscription id: (%" I64F "d)", mProcessName.str(), mBindingName.str(),  (__int64) id);
            return;
        }
        else if (id == binding_sub_id)
            isBindingNotify = true;
        else
            ntype = EsdlNotifyType::DefinitionUpdate;

        DBGLOG("ESDL %s change reported to %s.%s: (%" I64F "d) of %s - flags = %d",
                isBindingNotify?"binding":"definition", mProcessName.str(), mBindingName.str(), (__int64) id, xpath, flags);

        StringBuffer parentElementXPath;
        //path is reported with sibbling number annotation ie /ESDL/Bindings/Binding[2]/...
        if (!trimXPathToParentSDSElement(isBindingNotify?"Binding[":"Definition[", xpath, parentElementXPath))
            return;

        StringBuffer bindingName;
        StringBuffer processName;
        StringBuffer definitionId;
        {
            Owned<IRemoteConnection> conn = querySDS().connect(parentElementXPath.str(), myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);
            if (!conn)
            {
                //Can't find this path, is this a delete?
                if (flags == SDSNotify_Deleted && isBindingNotify)
                {
                    bindingName.set(mBindingName);
                    processName.set(mProcessName);
                    ntype = EsdlNotifyType::BindingDelete;
                }
                else
                    return;
            }
            else
            {
                if (isBindingNotify)
                {
                    IPropertyTree * bindingSubscription = conn->queryRoot();

                    bindingName = bindingSubscription->queryProp("@espbinding");
                    if (bindingName.length() == 0)
                        return;

                    processName = bindingSubscription->queryProp("@espprocess");
                    if (processName.length() == 0)
                        return;
                }
                else
                {
                    IPropertyTree * definitionSubscription = conn->queryRoot();

                    definitionId.set(definitionSubscription->queryProp("@id"));
                    if (definitionId.length() == 0)
                        return;
                }
            }
        }
        std::unique_ptr<EsdlNotifyData> dataptr(new EsdlNotifyData);
        dataptr->type = ntype;
        if (isBindingNotify)
        {
            dataptr->espBinding.set(bindingName);
            dataptr->espProcess.set(processName);
        }
        else
            dataptr->id.set(definitionId);

        mListener->onNotify(dataptr.get());
    }

private:
    void subscribe()
    {
        VStringBuffer fullBindingPath("/ESDL/Bindings/Binding[@id=\'%s.%s\']",mProcessName.str(), mBindingName.str());
        CriticalBlock b(daliSubscriptionCritSec);
        if(binding_sub_id == 0)
        {
            try
            {
                binding_sub_id = querySDS().subscribe(fullBindingPath.str(), *this, true);
                DBGLOG("Esdl binding subscription to DALI (%s) succeeded.", fullBindingPath.str());
            }
            catch (IException *E)
            {
                DBGLOG("ESDL Binding %s.%s failed to subscribe to DALI (%s)", mProcessName.str(), mBindingName.str(), fullBindingPath.str());
                E->Release();
            }
        }
        else
            DBGLOG("Esdl binding subscription already exists.");
        if(definition_sub_id == 0)
        {
            try
            {
                definition_sub_id = querySDS().subscribe(ESDL_DEF_PATH, *this, true);
                DBGLOG("Esdl definition subscription to DALI (%s) succeeded.", ESDL_DEF_PATH);
            }
            catch (IException *E)
            {
                DBGLOG("ESDL failed to subscribe to DALI (%s)", ESDL_DEF_PATH);
                E->Release();
            }
        }
        else
            DBGLOG("Esdl definition subscription already exists.");
    }
};

Owned<IEsdlStore> gEsdlCentralStore;

esdl_engine_decl IEsdlStore* createEsdlCentralStore()
{
    if(gEsdlCentralStore.get() == nullptr)
        gEsdlCentralStore.setown(new CEsdlSDSStore);
    return gEsdlCentralStore.getLink();
}

esdl_engine_decl IEsdlSubscription* createEsdlSubscription(IEsdlListener* listener, const char* process, const char* binding)
{
    return new CEsdlSDSSubscription(listener, process, binding);
}
