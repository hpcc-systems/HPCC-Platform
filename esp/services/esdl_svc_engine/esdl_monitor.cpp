/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "esdl_monitor.hpp"
#include "jmd5.hpp"
#include "esdl_binding.hpp"
#include "esdl_svc_engine.hpp"
#include <memory>

class CEsdlInstance : public CInterface
{
public:
    CEsdlInstance() { }
    CEsdlInstance(const char* defId, IEsdlDefinition* def)
    {
        StringBuffer lcid(defId);
        lcid.trim().toLowerCase();
        m_defId.set(lcid.str());
        m_def.set(def);
    }
    virtual ~CEsdlInstance()
    {
    }
    StringBuffer m_defId;
    Owned<IEsdlDefinition> m_def;
};

//This class is for storing esdl definition objects to be shared among
//ESDL services to save memory space and increase efficiency
class CEsdlShare : implements IEsdlShare, public Thread
{
public:
    IMPLEMENT_IINTERFACE;
    CEsdlShare() : m_stop(false)
    {
    }
    virtual ~CEsdlShare() {}

    virtual void add(const char* defId, IEsdlDefinition* def) override
    {
        if (!defId || !*defId || !def)
            return;
        DBGLOG("Adding esdl definition %s to shared cache", defId);
        Owned<CEsdlInstance> instance = new CEsdlInstance(defId, def);
        CriticalBlock cb(m_critSect);
        m_esdlInstanceMap.setValue(defId, instance.getLink());
    }

    virtual void remove(const char* defId) override
    {
        if (!defId || !*defId)
            return;
        StringBuffer lcid(defId);
        lcid.trim().toLowerCase();
        if (lcid.length() == 0)
            return;
        defId = lcid.str();
        DBGLOG("Removing esdl definition %s from shared cache", defId);
        {
            CriticalBlock cb(m_critSect);
            m_esdlInstanceMap.remove(defId);
        }
    }

    virtual Linked<IEsdlDefinition> lookup(const char* defId) override
    {
        if (!defId || !*defId)
            return nullptr;
        StringBuffer lcid(defId);
        lcid.trim().toLowerCase();
        if (lcid.length() == 0)
            return nullptr;
        defId = lcid.str();
        {
            CriticalBlock cb(m_critSect);
            Owned<CEsdlInstance>* instancepp = m_esdlInstanceMap.getValue(defId);
            if (instancepp)
                return (*instancepp)->m_def.get();
        }
        return nullptr;
    }

    //Thread periodically cleaning definition objects that are no longer being used
    virtual int run() override
    {
        while (!m_stop)
        {
            m_waitsem.wait(300000);
            if(m_stop)
                break;
            {
                CriticalBlock cb(m_critSect);
                StringArray ids;
                for (auto& item : m_esdlInstanceMap)
                {
                    CEsdlInstance* instance = item.getValue();
                    if (!instance || !instance->m_def.get())
                        continue;
                    if (!instance->m_def->isShared())
                        ids.append(instance->m_defId.str());
                }
                ForEachItemIn (x, ids)
                {
                    const char* id = ids.item(x);
                    DBGLOG("Definition %s is no longer being used, remove it from esdl shared cache", id);
                    m_esdlInstanceMap.remove(id);
                }
            }
        }

        return 0;
    }

    virtual void stop()
    {
        m_stop = true;
        m_waitsem.signal();
        join();
    }
private:
    CriticalSection m_critSect;
    MapStringTo<Owned<CEsdlInstance>> m_esdlInstanceMap;
    Semaphore m_waitsem;
    bool m_stop;
};

static CriticalSection gEsdlMonitorCritSection;

class CEsdlMonitor : implements IEsdlMonitor, public CInterface, implements IEsdlListener
{
private:
    MapStringTo<EsdlBindingImpl*> m_esdlBindingMap;
    CriticalSection m_CritSect;
    Owned<IPropertyTree> m_envptTemplate;
    Owned<IEsdlStore> m_pCentralStore;
    Owned<IEsdlSubscription> m_pSubscription;
    Owned<CEsdlShare> m_esdlShare;
    bool m_isSubscribed;

public:
    IMPLEMENT_IINTERFACE;

    CEsdlMonitor() : m_isSubscribed(false)
    {
        constructEnvptTemplate();
        m_pCentralStore.setown(createEsdlCentralStore());
        m_esdlShare.setown(new CEsdlShare());
        m_esdlShare->start();
        DBGLOG("EsdlMonitor started.");
    }

    virtual ~CEsdlMonitor()
    {
        if (m_pSubscription && m_isSubscribed)
            m_pSubscription->unsubscribe();
    }

    CEsdlShare* queryEsdlShare()
    {
        return m_esdlShare.get();
    }

    void setupSubscription()
    {
        m_isSubscribed = true;
        m_pSubscription.setown(createEsdlSubscription(this));
    }

    virtual void subscribe() override
    {
        CriticalBlock cb(m_CritSect);
        if(m_isSubscribed)
            return;
        m_isSubscribed = true;
        m_pSubscription->subscribe();
    }

    virtual void unsubscribe() override
    {
        CriticalBlock cb(m_CritSect);
        if(!m_isSubscribed)
            return;
        m_isSubscribed = false;
        m_pSubscription->unsubscribe();
    }

    //Reference count increment is done by the function
    virtual void registerBinding(const char* bindingId, IEspRpcBinding* binding) override
    {
        if (!bindingId || !binding)
            return;
        EsdlBindingImpl* esdlbinding = dynamic_cast<EsdlBindingImpl*>(binding);
        if (esdlbinding)
        {
            CriticalBlock cb(m_CritSect);
            m_esdlBindingMap.setValue(bindingId, esdlbinding);
        }
    }

    void loadDynamicBindings()
    {
        Owned<IPropertyTree> esdlBindings = m_pCentralStore->getBindings();
        if (!esdlBindings)
           throw MakeStringException(-1, "Unable to retrieve ESDL bindings information");

        Owned<IPropertyTreeIterator> iter = esdlBindings->getElements("Binding");
        ForEach (*iter)
        {
            std::unique_ptr<EsdlNotifyData> data(new EsdlNotifyData);
            IPropertyTree & cur = iter->query();
            cur.getProp("@id", data->id);
            if(data->id.length() == 0)
            {
                DBGLOG("ESDL binding id is missing, skip.");
                continue;
            }
            cur.getProp("@espprocess", data->espProcess);
            if (!espProcessMatch(data->espProcess.str()))
            {
                DBGLOG("ESDL binding %s is not for this esp process, skip.", data->id.str());
                continue;
            }
            cur.getProp("@espbinding", data->name);
            data->port = cur.getPropInt("@port");
            if (data->port == 0)
            {
                DBGLOG("ESDL binding %s doesn't have port specified, skip", data->id.str());
                continue;
            }

            try
            {
                addBinding(data.get());
            }
            catch(IException* e)
            {
                StringBuffer msg;
                e->errorMessage(msg);
                IERRLOG("Exception while loading dynamic binding %s: %d - %s", data->id.str(), e->errorCode(), msg.str());
                e->Release();
            }
        }
    }

    //IEsdlListener
    virtual void onNotify(EsdlNotifyData* data) override
    {
        if (!m_isSubscribed || !data)
            return;

        EsdlNotifyType ntype = data->type;

        if (ntype == EsdlNotifyType::DefinitionDelete)
        {
            CriticalBlock cb(m_CritSect);
            m_esdlShare->remove(data->id.str());
        }
        else if (ntype == EsdlNotifyType::DefinitionUpdate)
        {
            CriticalBlock cb(m_CritSect);
            m_esdlShare->remove(data->id.str());
            for (auto& sb:m_esdlBindingMap)
            {
                EsdlBindingImpl* binding = sb.getValue();
                if (binding->usesESDLDefinition(data->id.str()))
                {
                    DBGLOG("Requesting reload of ESDL definitions for binding %s...", (const char*)sb.getKey());
                    StringBuffer loaded;
                    binding->reloadDefinitionsFromCentralStore(nullptr, loaded);
                }
            }
        }
        else if (ntype == EsdlNotifyType::BindingDelete)
        {
            CriticalBlock cb(m_CritSect);
            EsdlBindingImpl* theBinding = findBinding(data->id.str());
            if (theBinding)
            {
                DBGLOG("Requesting clearing of binding %s", data->id.str());
                theBinding->clearBindingState();
                if (data->port <= 0)
                    data->port = theBinding->getPort();
                DBGLOG("Removing binding from port %d", data->port);
                queryEspServer()->removeBinding(data->port, *theBinding);
                removeBindingFromMap(data->id.str());
            }
            else
                OWARNLOG("Can't delete binding %s, it's not currently registered", data->id.str());
        }
        else if (ntype == EsdlNotifyType::BindingUpdate)
        {
            CriticalBlock cb(m_CritSect);
            EsdlBindingImpl* theBinding = findBinding(data->id.str());
            if (!theBinding)
            {
                OWARNLOG("Binding %s not found, can't update", data->id.str());
                return;
            }
            DBGLOG("Reloading ESDL binding %s", data->id.str());
            theBinding->reloadBindingFromCentralStore(data->id.str());
        }
        else if (ntype == EsdlNotifyType::BindingAdd)
        {
            if (!espProcessMatch(data->espProcess.str()))
            {
                OWARNLOG("ESDL binding %s is not for this esp process, ignore.", data->id.str());
                return;
            }

            CriticalBlock cb(m_CritSect);
            EsdlBindingImpl* theBinding = findBinding(data->id.str());
            if (theBinding)
            {
                DBGLOG("Binding %s already exists, reload...", data->id.str());
                theBinding->reloadBindingFromCentralStore(data->id.str());
                return;
            }

            if (data->name.length() == 0)
                data->name.set(data->id);
            if (data->port == 0)
            {
                OWARNLOG("Port is not provided for binding, can't create binding.");
                return;
            }
            addBinding(data);
        }
        else
        {
            IWARNLOG("Unexpected notify type received, ignore.");  //DefintionAdd and DefinitionDelete shouldn't happen
        }
    }

private:
    EsdlBindingImpl* findBinding(const char* bindingId)
    {
        if (!bindingId)
            return nullptr;
        EsdlBindingImpl** ptrptr = m_esdlBindingMap.getValue(bindingId);
        if (ptrptr)
            return *ptrptr;
        else
            return nullptr;
    }

    void removeBindingFromMap(const char* bindingId)
    {
        if (!bindingId)
            return;
        CriticalBlock cb(m_CritSect);
        m_esdlBindingMap.remove(bindingId);
    }

    void addBinding(EsdlNotifyData* data)
    {
        DBGLOG("Creating new binding %s", data->id.str());
        CriticalBlock cb(m_CritSect);
        if (data->name.length() == 0)
            data->name.set(data->id);
        StringBuffer protocol, serviceName;
        Owned<IPropertyTree> envpt = getEnvpt(data, protocol, serviceName);
        if (protocol.length() == 0)
            protocol.set("http");
        StringBuffer envptxml;
        toXML(envpt, envptxml);
        DBGLOG("Use the following config tree to create the binding and service:\n%s\n", envptxml.str());
        IEspServer* server = queryEspServer();
        IEspProtocol* espProtocol = server->queryProtocol(protocol.str());
        Owned<EsdlBindingImpl> esdlbinding = new CEsdlSvcEngineSoapBindingEx(envpt,  data->name.str(), data->espProcess.str());
        Owned<EsdlServiceImpl> esdlservice = new CEsdlSvcEngine();
        esdlservice->init(envpt, data->espProcess.str(), serviceName.str());
        esdlbinding->addService(esdlservice->getServiceType(), nullptr, data->port, *esdlservice.get());
        esdlbinding->addProtocol(protocol.str(), *espProtocol);
        server->addBinding(data->name.str(), nullptr, data->port, *espProtocol, *esdlbinding.get(), false, envpt);
        DBGLOG("Successfully instantiated new DESDL binding %s and service", data->id.str());
    }

    bool espProcessMatch(const char* espProcess)
    {
        if (!espProcess)
            return false;
        return (strcmp(espProcess, queryEspServer()->getProcName()) == 0);
    }

    void constructEnvptTemplate()
    {
        m_envptTemplate.setown(createPTreeFromXMLString("<Environment><Software/></Environment>"));
        IPropertyTree* procpt = queryEspServer()->queryProcConfig();
        if (procpt)
        {
            Owned<IPropertyTree> proctemplate = createPTreeFromIPT(procpt);
            proctemplate->removeProp("EspBinding");
            proctemplate->removeProp("EspService");
            m_envptTemplate->addPropTree("Software/EspProcess", proctemplate.getClear());
        }
    }

    IPropertyTree* getEnvpt(EsdlNotifyData* notifyData, StringBuffer& protocol, StringBuffer& serviceName)
    {
        VStringBuffer portStr("%d", notifyData->port);
        return getEnvpt(notifyData->espProcess.str(), notifyData->name.str(), notifyData->id.str(),
                portStr.str(), protocol, serviceName);
    }

    IPropertyTree* getEnvpt(const char* espProcess, const char* bindingName, const char* bindingId, const char* port, StringBuffer& protocol, StringBuffer& serviceName)
    {
        if (!bindingName || !*bindingName)
            bindingName = bindingId;
        serviceName.set(bindingId);
        StringBuffer envxmlbuf;
        IPropertyTree* procpt = queryEspServer()->queryProcConfig();
        if (procpt)
        {
            //If esp's original config has one configured for this binding, use it
            VStringBuffer xpath("EspBinding[@name='%s']", bindingName);
            IPropertyTree* bindingtree = procpt->queryPropTree(xpath.str());
            if (!bindingtree)
            {
                //Otherwise check if there's binding configured on the same port
                xpath.setf("EspBinding[@type='EsdlBinding'][@port=%s][1]", port);
                bindingtree = procpt->queryPropTree(xpath.str());
            }
            if (!bindingtree)
            {
                //Otherwise check if there's binding configured with port 0
                xpath.setf("EspBinding[@type='EsdlBinding'][@port=0]");
                bindingtree = procpt->queryPropTree(xpath.str());
            }
            if (bindingtree)
            {
                bindingtree->getProp("@protocol", protocol);
                const char* service = bindingtree->queryProp("@service");
                xpath.setf("EspService[@name='%s']", service);
                IPropertyTree* servicetree = procpt->queryPropTree(xpath.str());
                if (servicetree)
                {
                    bindingtree = createPTreeFromIPT(bindingtree);
                    servicetree = createPTreeFromIPT(servicetree);
                    bindingtree->setProp("@name", bindingName);
                    bindingtree->setProp("@service", serviceName.str());
                    bindingtree->setProp("@port", port);
                    servicetree->setProp("@name", serviceName.str());
                    servicetree->setProp("@type", "DynamicESDL");
                    Owned<IPropertyTree> envpttree = createPTreeFromIPT(m_envptTemplate.get());
                    envpttree->addPropTree("Software/EspProcess/EspBinding", bindingtree);
                    envpttree->addPropTree("Software/EspProcess/EspService", servicetree);
                    return envpttree.getClear();
                }
            }
        }
        throw MakeStringException(-1, "There's no template esp binding configured on port %s, or port 0.", port);
    }
};

static Owned<CEsdlMonitor> gEsdlMonitor;
static bool isEsdlMonitorStarted = false;

extern "C" void startEsdlMonitor()
{
    CriticalBlock cb(gEsdlMonitorCritSection);
    if (gEsdlMonitor.get() == nullptr)
    {
        CEsdlMonitor* monitor = new CEsdlMonitor();
        gEsdlMonitor.setown(monitor);
        isEsdlMonitorStarted = true;
        monitor->loadDynamicBindings();
        monitor->setupSubscription();
    }
}

extern "C" void stopEsdlMonitor()
{
    DBGLOG("stopping esdl monitor...");
    CriticalBlock cb(gEsdlMonitorCritSection);
    if (gEsdlMonitor.get() != nullptr)
    {
        gEsdlMonitor->queryEsdlShare()->stop();
        gEsdlMonitor.clear();
    }
}

IEsdlMonitor* queryEsdlMonitor()
{
    if (!isEsdlMonitorStarted)
        startEsdlMonitor();
    return gEsdlMonitor.get();
}

IEsdlShare* queryEsdlShare()
{
    if (!isEsdlMonitorStarted)
        startEsdlMonitor();
    return gEsdlMonitor->queryEsdlShare();
}
