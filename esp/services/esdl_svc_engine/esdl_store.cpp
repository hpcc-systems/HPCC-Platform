/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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
#include <memory>

static const char* ESDL_DEFS_ROOT_PATH="/ESDL/Definitions/";
static const char* ESDL_DEF_PATH="/ESDL/Definitions/Definition";
static const char* ESDL_DEF_ENTRY="Definition";

static const char* ESDL_BINDINGS_ROOT_PATH="/ESDL/Bindings/";
static const char* ESDL_BINDING_PATH="/ESDL/Bindings/Binding";
static const char* ESDL_BINDING_ENTRY="Binding";
static const char* ESDL_METHOD_DESCRIPTION="description";
static const char* ESDL_METHOD_HELP="help";

#define SDS_LOCK_TIMEOUT_DESDL (30*1000)

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

    virtual void fetchDefinition(const char* id, StringBuffer& exsdl)
    {
        if (!id || !*id)
            throw MakeStringException(-1, "Unable to fetch ESDL Service definition information, service id is not available");

        //There shouldn't be multiple entries here, but if so, we'll use the first one
        VStringBuffer xpath("%s[@id='%s'][1]", ESDL_DEF_ENTRY, id);

        ESPLOG(LogNormal, "ESDL Binding: Fetching ESDL Definition from Dali: %s ", id);

        Owned<IRemoteConnection> conn = querySDS().connect(ESDL_DEFS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);
        if (!conn)
           throw MakeStringException(-1, "Unable to connect to ESDL Service definition information in dali '%s'", ESDL_DEFS_ROOT_PATH);

        IPropertyTree * esdlDefinitions = conn->queryRoot();

        toXML(esdlDefinitions->queryPropTree(xpath), exsdl);
    }
    virtual void fetchDefinition(const char* esdlServiceName, unsigned targetVersion, StringBuffer& exsdl)
    {
        if (!esdlServiceName || !*esdlServiceName)
            throw MakeStringException(-1, "Unable to fetch ESDL Service definition information, service name is not available");
        if (targetVersion == 0 )
            throw MakeStringException(-1, "Unable to fetch ESDL Service definition information, target version number is invalid: '%s.%d", esdlServiceName, targetVersion);

        DBGLOG("ESDL Binding: Fetching ESDL Definition from Dali: %s.%d ", esdlServiceName, targetVersion);

        StringBuffer id(esdlServiceName);
        id.toLowerCase().append(".").append(targetVersion);

        return fetchDefinition(id, exsdl);
    }
    virtual IPropertyTree* fetchBinding(const char* espProcess, const char* bindingName)
    {
        try
        {
            //There shouldn't be multiple entries here, but if so, we'll use the first one
            VStringBuffer xpath("%s[@id='%s.%s'][1]", ESDL_BINDING_ENTRY, espProcess, bindingName);

            Owned<IRemoteConnection> conn = querySDS().connect(ESDL_BINDINGS_ROOT_PATH, myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT_DESDL);
            if (!conn)
            {
                ESPLOG(LogMin, "Unable to connect to ESDL Service binding information in dali %s", ESDL_BINDINGS_ROOT_PATH);
                return nullptr;
            }

            ESPLOG(LogNormal, "ESDL Binding: Fetching ESDL Binding from Dali %s[@EspProcess='%s'][@EspBinding='%s'][1]", ESDL_BINDING_ENTRY, espProcess, bindingName);

            if (conn->queryRoot()->hasProp(xpath))
                return createPTreeFromIPT(conn->queryRoot()->queryPropTree(xpath));
            else
                return nullptr;
        }
        catch (IException *E)
        {
            VStringBuffer message("ESDL Binding: Error fetching ESDL Binding %s[@EspProcess='%s'][@EspBinding='%s'][1] from Dali.", ESDL_BINDING_ENTRY, espProcess, bindingName);
            EXCLOG(E, message);
            E->Release();
        }
        catch(...)
        {
            ESPLOG(LogMin, "ESDL Binding: Unknown error encountered while fetching ESDL Binding from Dali %s[@EspProcess='%s'][@EspBinding='%s'][1]", ESDL_BINDING_ENTRY, espProcess, bindingName);
        }

        return nullptr;
    }
};

class CEsdlSDSSubscription : implements IEsdlSubscription, public CInterface, implements ISDSSubscription
{
protected:
    IEsdlListener* mListener;
    CriticalSection daliSubscriptionCritSec;
    SubscriptionId binding_sub_id;
    SubscriptionId definition_sub_id;
    StringBuffer mProcessName, mBindingName;
    bool subscribed;
public:
    IMPLEMENT_IINTERFACE;
    CEsdlSDSSubscription(IEsdlListener* listener, const char* process, const char* binding)
    {
        mListener = listener;
        mProcessName.set(process);
        mBindingName.set(binding);
        binding_sub_id = 0;
        definition_sub_id = 0;
        subscribed = false;
        subscribe();
    }

    virtual ~CEsdlSDSSubscription()
    {
        unsubscribe();
    }

    virtual void unsubscribe()
    {
        CriticalBlock b(daliSubscriptionCritSec);
        if(!subscribed)
            return;
        try
        {
            if (binding_sub_id)
                querySDS().unsubscribe(binding_sub_id);
            if (definition_sub_id)
                querySDS().unsubscribe(definition_sub_id);
            DBGLOG("Esdl SDS subscription successfully unsubscribed.");
        }
        catch (IException *E)
        {
            StringBuffer msg;
            E->errorMessage(msg);
            DBGLOG("Error unsubscribing: %s", msg.str());
            E->Release();
        }
        binding_sub_id = 0;
        definition_sub_id = 0;
        subscribed = false;
    }

    void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        if(!mListener)
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
        else if(id == binding_sub_id)
            isBindingNotify = true;
        else
            ntype = EsdlNotifyType::DefinitionUpdate;

        DBGLOG("ESDL %s change reported to %s.%s: (%" I64F "d) of %s - flags = %d",
                isBindingNotify?"binding":"definition", mProcessName.str(), mBindingName.str(), (__int64) id, xpath, flags);

        StringBuffer parentElementXPath;
        //path is reported with sibbling number annotation ie /ESDL/Bindings/Binding[2]/...
        if(!trimXPathToParentSDSElement(isBindingNotify?"Binding[":"Definition[", xpath, parentElementXPath))
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
                if(isBindingNotify)
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
        if(isBindingNotify)
        {
            dataptr->espBinding.set(bindingName);
            dataptr->espProcess.set(processName);
        }
        else
            dataptr->id.set(definitionId);

        mListener->onNotify(dataptr.get());
    }

protected:
    virtual void subscribe()
    {
        VStringBuffer fullBindingPath("/ESDL/Bindings/Binding[@id=\'%s.%s\']",mProcessName.str(), mBindingName.str());
        CriticalBlock b(daliSubscriptionCritSec);
        if(subscribed)
            return;
        try
        {
            binding_sub_id = querySDS().subscribe(fullBindingPath.str(), *this, true);
            subscribed = true;
            DBGLOG("Esdl SDS subscription successfully subscribed.");
        }
        catch (IException *E)
        {
            DBGLOG("ESDL Binding %s.%s failed to subscribe to DALI (%s)", mProcessName.str(), mBindingName.str(), fullBindingPath.str());
            E->Release();
        }
        try
        {
            definition_sub_id = querySDS().subscribe(ESDL_DEF_PATH, *this, true);
        }
        catch (IException *E)
        {
            DBGLOG("ESDL failed to subscribe to DALI (%s)", ESDL_DEF_PATH);
            E->Release();
        }
    }
};

IEsdlStore* createEsdlCentralStore()
{
    return new CEsdlSDSStore;
}

IEsdlSubscription* createEsdlSubscription(IEsdlListener* listener, const char* process, const char* binding)
{
    return new CEsdlSDSSubscription(listener, process, binding);
}
