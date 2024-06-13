/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#pragma warning (disable : 4786)
// TpWrapper.cpp: implementation of the CTpWrapper class.
//
//////////////////////////////////////////////////////////////////////

#include "TpWrapper.hpp"
#include <stdio.h>
#include "jconfig.hpp"
#include "workunit.hpp"
#include "exception_util.hpp"
#include "portlist.h"
#include "daqueue.hpp"
#include "dautils.hpp"
#include "dameta.hpp"
#include "hpccconfig.hpp"
#include "securesocket.hpp"

static CConfigUpdateHook configUpdateHook;

const char* MSG_FAILED_GET_ENVIRONMENT_INFO = "Failed to get environment information.";

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

void CTpWrapper::getClusterMachineList(double clientVersion,
                                       CTpMachineType ClusterType,
                                       const char* ClusterPath,
                                       const char* ClusterDirectory,
                                       IArrayOf<IEspTpMachine> &MachineList,
                                       bool& hasThorSpareProcess,
                                       const char* ClusterName)
{
    IWARNLOG("UNIMPLEMENTED: CONTAINERIZED(CTpWrapper::getClusterMachineList)");
}

void CTpWrapper::getTpDaliServers(double clientVersion, IArrayOf<IConstTpDali>& list)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::getTpEclServers(IArrayOf<IConstTpEclServer>& list, const char* serverName)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::getTpEclCCServers(IArrayOf<IConstTpEclServer>& list, const char* serverName)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::getTpEclCCServers(IPropertyTree* environmentSoftware, IArrayOf<IConstTpEclServer>& list, const char* serverName)
{
}

void CTpWrapper::getTpEclAgents(IArrayOf<IConstTpEclAgent>& list, const char* agentName)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::getTpEclSchedulers(IArrayOf<IConstTpEclScheduler>& list, const char* serverName)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::getTpEspServers(IArrayOf<IConstTpEspServer>& list)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

static IEspTpMachine * createHostTpMachine(const char * hostname, const char *path)
{
    Owned<IEspTpMachine> machine = createTpMachine();
    machine->setName(hostname);
    machine->setNetaddress(hostname);
    machine->setConfigNetaddress(hostname); //May be used by legacy ECLWatch. Leave it for now.
    machine->setDirectory(path);
    machine->setOS(getPathSepChar(path) == '/' ? MachineOsLinux : MachineOsW2K);
    return machine.getClear();
}

static void gatherDropZoneMachinesFromHosts(IArrayOf<IEspTpMachine> & tpMachines, IPropertyTree & planeOrGroup, const char * prefix)
{
    Owned<IPropertyTreeIterator> iter = planeOrGroup.getElements("hosts");
    ForEach(*iter)
    {
        const char * host = iter->query().queryProp(nullptr);
        tpMachines.append(*createHostTpMachine(host, prefix));
    }
}

static void gatherDropZoneMachines(IArrayOf<IEspTpMachine> & tpMachines, IPropertyTree & plane)
{
    const char * prefix = plane.queryProp("@prefix");
    if (plane.hasProp("hosts"))
        gatherDropZoneMachinesFromHosts(tpMachines, plane, prefix);
    else
        tpMachines.append(*createHostTpMachine("localhost", prefix));
}


void CTpWrapper::getTpDfuServers(IArrayOf<IConstTpDfuServer>& list)
{
    Owned<IPropertyTreeIterator> dfuQueues = getComponentConfigSP()->getElements("dfuQueues");
    ForEach(*dfuQueues)
    {
        IPropertyTree & dfuQueue = dfuQueues->query();
        const char * dfuName = dfuQueue.queryProp("@name");
        StringBuffer queue;
        getDfuQueueName(queue, dfuName);
        Owned<IEspTpDfuServer> pService = createTpDfuServer("","");
        pService->setName(dfuName);
        pService->setDescription(dfuName);
        pService->setBuild("");
        pService->setQueue(queue);
        pService->setType(eqDfu);
        IArrayOf<IEspTpMachine> tpMachines;
        //MORE: The ip and directory don't make any sense on the cloud version
        tpMachines.append(*createHostTpMachine("localhost", "/var/lib/HPCCSystems"));
        pService->setTpMachines(tpMachines);
        list.append(*pService.getClear());
    }
}


void CTpWrapper::getTpSashaServers(IArrayOf<IConstTpSashaServer>& list)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::getTpLdapServers(IArrayOf<IConstTpLdapServer>& list)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::getTpFTSlaves(IArrayOf<IConstTpFTSlave>& list)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::getTpGenesisServers(IArrayOf<IConstTpGenesisServer>& list)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::getTargetClusterList(IArrayOf<IEspTpLogicalCluster>& clusters, const char* clusterType, const char* clusterName)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::queryTargetClusterProcess(double version, const char* processName, const char* clusterType, IArrayOf<IConstTpCluster>& clusterList)
{
    throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, MSG_FAILED_GET_ENVIRONMENT_INFO);
}

void CTpWrapper::queryTargetClusters(double version, const char* clusterType, const char* clusterName, IArrayOf<IEspTpTargetCluster>& targetClusterList)
{
    IWARNLOG("UNIMPLEMENTED: CONTAINERIZED(CTpWrapper::queryTargetClusters)");
}

void CTpWrapper::getClusterProcessList(const char* ClusterType, IArrayOf<IEspTpCluster>& clusterList, bool ignoreduplicatqueues, bool ignoreduplicategroups)
{
    IWARNLOG("UNIMPLEMENTED: CONTAINERIZED(CTpWrapper::getClusterProcessList)");
}

void CTpWrapper::getHthorClusterList(IArrayOf<IEspTpCluster>& clusterList)
{
    IWARNLOG("UNIMPLEMENTED: CONTAINERIZED(CTpWrapper::getHthorClusterList)");
}


void CTpWrapper::getGroupList(double espVersion, const char* kindReq, IArrayOf<IEspTpGroup> &GroupList)
{
    try
    {
        Owned<IPropertyTreeIterator> dataPlanes = getGlobalConfigSP()->getElements("storage/planes[@category='data']");
        ForEach(*dataPlanes)
        {
            IPropertyTree & plane = dataPlanes->query();
            const char * name = plane.queryProp("@name");
            IEspTpGroup* pGroup = createTpGroup("","");
            pGroup->setName(name);
            if (espVersion >= 1.21)
            {
                pGroup->setKind("Plane");
                pGroup->setReplicateOutputs(false);
            }
            GroupList.append(*pGroup);
        }
    }
    catch(IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...)
    {
        IWARNLOG("Unknown Exception caught within CTpWrapper::getGroupList");
    }
}

bool CTpWrapper::checkGroupReplicateOutputs(const char* groupName, const char* kind)
{
    return false;
}

void CTpWrapper::getMachineInfo(double clientVersion, const char* name, const char* netAddress, IEspTpMachine& machineInfo)
{
    UNIMPLEMENTED_X("CONTAINERIZED(CTpWrapper::getMachineInfo)");
}

void CTpWrapper::setTpMachine(IConstMachineInfo* machine, IEspTpMachine& tpMachine)
{
    if (!machine)
        return;

    SCMStringBuffer machineName, netAddress;
    machine->getName(machineName);
    machine->getNetAddress(netAddress);
    tpMachine.setName(machineName.str());
    tpMachine.setNetaddress(netAddress.str());
    tpMachine.setOS(machine->getOS());

    switch(machine->getState())
    {
        case MachineStateAvailable:
            tpMachine.setAvailable("Available");
            break;
        case MachineStateUnavailable:
            tpMachine.setAvailable("Unavailable");
            break;
        case MachineStateUnknown:
            tpMachine.setAvailable("Unknown");
            break;
    }
    Owned<IConstDomainInfo> pDomain = machine->getDomain();
    if (pDomain != 0)
    {
        SCMStringBuffer sName;
        tpMachine.setDomain(pDomain->getName(sName).str());
    }
}

void CTpWrapper::appendThorMachineList(double clientVersion, IConstEnvironment* constEnv, INode& node, const char* clusterName,
    const char* machineType, unsigned& processNumber, unsigned channels, const char* directory, IArrayOf<IEspTpMachine>& machineList)
{
    StringBuffer netAddress;
    node.endpoint().getHostText(netAddress);
    if (netAddress.length() == 0)
    {
        OWARNLOG("Net address not found for a node of %s", clusterName);
        return;
    }

    processNumber++;

    Owned<IEspTpMachine> machineInfo = createTpMachine("","");
    machineInfo->setType(machineType);
    machineInfo->setNetaddress(netAddress.str());
    if (!isEmptyString(directory))
        machineInfo->setDirectory(directory);

    Owned<IConstMachineInfo> pMachineInfo =  constEnv->getMachineByAddress(netAddress.str());
    if (pMachineInfo.get())
    {
        setTpMachine(pMachineInfo, *machineInfo);
        if (clientVersion > 1.17)
        {
            machineInfo->setProcessNumber(processNumber);
        }
    }
    else
    {
        machineInfo->setName("external");
        machineInfo->setOS(MachineOsUnknown);
    }

    if (clientVersion >= 1.30)
        machineInfo->setChannels(channels);

    machineList.append(*machineInfo.getLink());
}

void CTpWrapper::getThorSlaveMachineList(double clientVersion, const char* clusterName, const char* directory, IArrayOf<IEspTpMachine>& machineList)
{
    IWARNLOG("UNIMPLEMENTED: CONTAINERIZED(CTpWrapper::getThorSlaveMachineList)");
}

void CTpWrapper::getThorSpareMachineList(double clientVersion, const char* clusterName, const char* directory, IArrayOf<IEspTpMachine>& machineList)
{
    IWARNLOG("UNIMPLEMENTED: CONTAINERIZED(CTpWrapper::getThorSpareMachineList)");
}

void CTpWrapper::getMachineList(double clientVersion, const char* MachineType, const char* ParentPath,
    const char* Status, const char* Directory, IArrayOf<IEspTpMachine>& MachineList, set<string>* pMachineNames/*=NULL*/)
{
    IWARNLOG("UNIMPLEMENTED: CONTAINERIZED(CTpWrapper::getMachineList)");
}

void CTpWrapper::listLogFiles(const char* host, const char* path, IArrayOf<IConstLogFileStruct>& files)
{
    IWARNLOG("UNIMPLEMENTED: CONTAINERIZED(CTpWrapper::listLogFiles)");
}

const char* CTpWrapper::getNodeNameTag(const char* MachineType)
{
    if (strcmp(MachineType,"Computer")==0)
        return "@name";
    else
        return "@computer";
}

void CTpWrapper::getDropZoneMachineList(double clientVersion, bool ECLWatchVisibleOnly, IArrayOf<IEspTpMachine> &MachineList)
{
    try
    {
        IArrayOf<IConstTpDropZone> list;
        getTpDropZones(clientVersion, nullptr, ECLWatchVisibleOnly, list);
        ForEachItemIn(i, list)
        {
            IConstTpDropZone& dropZone = list.item(i);

            IArrayOf<IConstTpMachine>& tpMachines = dropZone.getTpMachines();
            ForEachItemIn(ii, tpMachines)
            {
                IConstTpMachine& tpMachine = tpMachines.item(ii);
                Owned<IEspTpMachine> machine = createTpMachine();
                machine->copy(tpMachine);

                MachineList.append(*machine.getLink());
            }
        }
    }
    catch(IException* e)
    {
        EXCLOG(e);
        e->Release();
    }
    catch(...)
    {
        IWARNLOG("Unknown Exception caught within CTpWrapper::getDropZoneMachineList");
    }
}

//For a given dropzone or every dropzones (check ECLWatchVisible if needed), read: "@name",
// "@description", "@build", "@directory", "@ECLWatchVisible" into an IEspTpDropZone object.
//For each ServerList, read "@name" and "@server" (hostname or IP) into an IEspTpMachine object.
//Add the IEspTpMachine object into the IEspTpDropZone.

void CTpWrapper::getTpDropZones(double clientVersion, const char* name, bool ECLWatchVisibleOnly, IArrayOf<IConstTpDropZone>& list)
{
    Owned<IPropertyTreeIterator> planes = getDropZonePlanesIterator(name);
    ForEach(*planes)
    {
        IPropertyTree & plane = planes->query();
        bool eclwatchVisible = plane.getPropBool("@eclwatchVisible", true);
        if (ECLWatchVisibleOnly && !eclwatchVisible)
            continue;
        const char * dropzonename = plane.queryProp("@name");
        const char * path = plane.queryProp("@prefix");
        Owned<IEspTpDropZone> dropZone = createTpDropZone();
        dropZone->setName(dropzonename);
        dropZone->setDescription("");
        dropZone->setPath(path);
        dropZone->setBuild("");
        dropZone->setECLWatchVisible(eclwatchVisible);
        IArrayOf<IEspTpMachine> tpMachines;
        gatherDropZoneMachines(tpMachines, plane);
        dropZone->setTpMachines(tpMachines);
        list.append(*dropZone.getClear());
    }
}

void CTpWrapper::getTpSparkThors(double clientVersion, const char* name, IArrayOf<IConstTpSparkThor>& list)
{
    UNIMPLEMENTED_X("CONTAINERIZED(CTpWrapper::getTpSparkThors)");
}

IEspTpMachine* CTpWrapper::createTpMachineEx(const char* name, const char* type, IConstMachineInfo* machineInfo)
{
    if (!machineInfo)
        return nullptr;

    Owned<IEspTpMachine> machine = createTpMachine();
    machine->setName(name);
    machine->setType(type);
    machine->setOS(machineInfo->getOS());

    Owned<IConstDomainInfo> domain = machineInfo->getDomain();
    if (domain)
    {
        SCMStringBuffer sName;
        machine->setDomain(domain->getName(sName).str());
    }

    SCMStringBuffer netAddr;
    machineInfo->getNetAddress(netAddr);
    if (netAddr.length() > 0)
    {
        StringBuffer networkAddress;
        IpAddress ipAddr;
        ipAddr.ipset(netAddr.str());
        ipAddr.getHostText(networkAddress);
        machine->setNetaddress(networkAddress.str());
    }

    switch(machineInfo->getState())
    {
        case MachineStateAvailable:
            machine->setAvailable("Available");
            break;
        case MachineStateUnavailable:
            machine->setAvailable("Unavailable");
            break;
        default:
            machine->setAvailable("Unknown");
            break;
    }
    return machine.getClear();
}

void CTpWrapper::setAttPath(StringBuffer& Path,const char* PathToAppend,const char* AttName,const char* AttValue,StringBuffer& returnStr)
{
    Path.append("/");
    Path.append(PathToAppend);
    Path.append("[@");
    Path.append(AttName);
    Path.append("=\"");
    Path.append(AttValue);
    Path.append("\"]");
    StringBuffer rawPath;
    const void* buff = (void*)Path.str();
    JBASE64_Encode(buff,Path.length(),rawPath, false);
    returnStr.append(rawPath.str());
}

void CTpWrapper::getAttPath(const char* Path,StringBuffer& returnStr)
{
    StringBuffer decodedStr;
    JBASE64_Decode(Path, returnStr);
}

void CTpWrapper::getServices(double version, const char* serviceType, const char* serviceName, IArrayOf<IConstHPCCService>& services)
{
    Owned<IPropertyTreeIterator> itr = getGlobalConfigSP()->getElements("services");
    ForEach(*itr)
    {
        IPropertyTree& service = itr->query();
        //Only show the public services for now
        if (!service.getPropBool("@public"))
            continue;

        const char* type = service.queryProp("@type");
        if (isEmptyString(type) || (!isEmptyString(serviceType) && !strieq(serviceType, type)))
            continue;

        const char* name = service.queryProp("@name");
        if (isEmptyString(name) || (!isEmptyString(serviceName) && !strieq(serviceName, name)))
            continue;

        Owned<IEspHPCCService> svc = createHPCCService();
        svc->setName(name);
        svc->setType(type);
        svc->setPort(service.getPropInt("@port"));
        if (service.getPropBool("@tls"))
            svc->setTLSSecure(true);
        services.append(*svc.getLink());
        if (!isEmptyString(serviceName))
            break;
    }
}

class CContainerWUClusterInfo : public CSimpleInterfaceOf<IConstWUClusterInfo>
{
    StringAttr name;
    StringAttr serverQueue;
    StringAttr agentQueue;
    StringAttr thorQueue;
    StringAttr ldapUser;
    ClusterType platform;
    unsigned clusterWidth;
    StringArray thorProcesses;
    RoxieTargetType roxieTargetType = RTTQueued;

public:
    CContainerWUClusterInfo(const char* _name, const char* type, const char* _ldapUser,
        unsigned _clusterWidth, bool _queriesOnly)
        : name(_name), ldapUser(_ldapUser), clusterWidth(_clusterWidth)
    {
        StringBuffer queue;
        if (strieq(type, "thor"))
        {
            thorQueue.set(getClusterThorQueueName(queue.clear(), name));
            platform = ThorLCRCluster;
            thorProcesses.append(name);
        }
        else if (strieq(type, "roxie"))
        {
            agentQueue.set(getClusterEclAgentQueueName(queue.clear(), name));
            platform = RoxieCluster;
            if (_queriesOnly)
                roxieTargetType = RTTPublished;
        }
        else
        {
            agentQueue.set(getClusterEclAgentQueueName(queue.clear(), name));
            platform = HThorCluster;
        }

        serverQueue.set(getClusterEclCCServerQueueName(queue.clear(), name));
    }

    virtual IStringVal& getName(IStringVal& str) const override
    {
        str.set(name.get());
        return str;
    }
    virtual IStringVal& getAgentQueue(IStringVal& str) const override
    {
        str.set(agentQueue);
        return str;
    }
    virtual IStringVal& getServerQueue(IStringVal& str) const override
    {
        str.set(serverQueue);
        return str;
    }
    virtual IStringVal& getThorQueue(IStringVal& str) const override
    {
        str.set(thorQueue);
        return str;
    }
    virtual ClusterType getPlatform() const override
    {
        return platform;
    }
    virtual unsigned getSize() const override
    {
        return clusterWidth;
    }
    virtual bool isLegacyEclServer() const override
    {
        return false;
    }
    virtual RoxieTargetType getRoxieTargetType() const override
    {
        return roxieTargetType;
    }
    virtual bool canPublishQueries() const override
    {
        return roxieTargetType & RTTPublished;
    }
    virtual bool onlyPublishedQueries() const override
    {
        return roxieTargetType == RTTPublished;
    }
    virtual IStringVal& getScope(IStringVal& str) const override
    {
        UNIMPLEMENTED;
    }
    virtual unsigned getNumberOfSlaveLogs() const override
    {
        UNIMPLEMENTED;
    }
    virtual IStringVal & getAgentName(IStringVal & str) const override
    {
        UNIMPLEMENTED;
    }
    virtual IStringVal & getECLSchedulerName(IStringVal & str) const override
    {
        UNIMPLEMENTED;
    }
    virtual const StringArray & getECLServerNames() const override
    {
        UNIMPLEMENTED;
    }
    virtual IStringVal & getRoxieProcess(IStringVal & str) const override
    {
        str.set(name.get());
        return str;
    }
    virtual const StringArray & getThorProcesses() const override
    {
        return thorProcesses;
    }
    virtual const StringArray & getPrimaryThorProcesses() const override
    {
        UNIMPLEMENTED;
    }
    virtual const SocketEndpointArray & getRoxieServers() const override
    {
        UNIMPLEMENTED;
    }
    virtual const char *getLdapUser() const override
    {
        return ldapUser.get();
    }
    virtual const char *getLdapPassword() const override
    {
        return nullptr;
    }
    virtual unsigned getRoxieRedundancy() const override
    {
        return 1;
    }
    virtual unsigned getChannelsPerNode() const override
    {
        return 1;
    }
    virtual int getRoxieReplicateOffset() const override
    {
        return 0;
    }
    virtual const char *getAlias() const override
    {
        UNIMPLEMENTED;
    }
};

extern TPWRAPPER_API unsigned getContainerWUClusterInfo(CConstWUClusterInfoArray& clusters)
{
    Owned<IPropertyTreeIterator> queues = getComponentConfigSP()->getElements("queues");
    ForEach(*queues)
    {
        IPropertyTree& queue = queues->query();

        // auxillary queues are additional queues and do not have a 1:1 mapping to clusters
        if (queue.getPropBool("@isAuxQueue"))
            continue;
        Owned<IConstWUClusterInfo> cluster = new CContainerWUClusterInfo(queue.queryProp("@name"),
            queue.queryProp("@type"), queue.queryProp("@ldapUser"), (unsigned) queue.getPropInt("@width", 1),
            queue.getPropBool("@queriesOnly"));
        clusters.append(*cluster.getClear());
    }

    return clusters.ordinality();
}

extern TPWRAPPER_API unsigned getWUClusterInfo(CConstWUClusterInfoArray& clusters)
{
    return getContainerWUClusterInfo(clusters);
}

static IPropertyTree * getContainerClusterConfig(const char * clusterName)
{
    VStringBuffer xpath("queues[@name='%s']", clusterName);
    return getComponentConfigSP()->getPropTree(xpath);
}

extern TPWRAPPER_API IConstWUClusterInfo* getWUClusterInfoByName(const char* clusterName)
{
    Owned<IPropertyTree> queue = getContainerClusterConfig(clusterName);
    if (!queue)
        return nullptr;

    return new CContainerWUClusterInfo(queue->queryProp("@name"), queue->queryProp("@type"),
        queue->queryProp("@ldapUser"), (unsigned) queue->getPropInt("@width", 1),
        queue->getPropBool("@queriesOnly"));
}

extern TPWRAPPER_API void initContainerRoxieTargets(MapStringToMyClass<ISmartSocketFactory>& connMap)
{
    Owned<IPropertyTreeIterator> services = getGlobalConfigSP()->getElements("services[@type='roxie']");
    ForEach(*services)
    {
        IPropertyTree& service = services->query();
        const char* target = service.queryProp("@target");

        if (isEmptyString(target) || isEmptyString(service.queryProp("@name"))) //bad config?
            continue;

        bool tls = service.getPropBool("@tls", false);
        Owned<ISmartSocketFactory> sf = tls ? createSecureSmartSocketFactory(service, false, 60, 0) : createSmartSocketFactory(service, false, 60, 0);
        connMap.setValue(target, sf.get());
    }
}

extern TPWRAPPER_API void initBareMetalRoxieTargets(MapStringToMyClass<ISmartSocketFactory>& connMap)
{
    IWARNLOG("UNIMPLEMENTED: CONTAINERIZED(CTpWrapper::initBareMetalRoxieTargets)");
}

extern TPWRAPPER_API void getRoxieTargetsSupportingPublishedQueries(StringArray& names)
{
    Owned<IPropertyTreeIterator> queues = getComponentConfigSP()->getElements("queues[@type='roxie']");
    ForEach(*queues)
    {
        IPropertyTree& queue = queues->query();
        if (queue.getPropBool("@queriesOnly"))
            names.append(queue.queryProp("@name"));
    }
}

extern TPWRAPPER_API unsigned getThorClusterNames(StringArray& targetNames, StringArray& queueNames)
{
    Owned<IStringIterator> targets = config::getContainerTargets("thor", nullptr);
    ForEach(*targets)
    {
        SCMStringBuffer target;
        targets->str(target);
        targetNames.append(target.str());

        StringBuffer qName;
        queueNames.append(getClusterThorQueueName(qName, target.str()));
    }
    return targetNames.ordinality();
}

static std::set<std::string> validTargets;
static std::set<std::string> validDataPlaneNames;
static CriticalSection configUpdateSect;

static void refreshValidTargets()
{
    validTargets.clear();
    Owned<IStringIterator> it = config::getContainerTargets(nullptr, nullptr);
    ForEach(*it)
    {
        SCMStringBuffer s;
        IStringVal& val = it->str(s);
        if (validTargets.find(val.str()) == validTargets.end())
        {
            validTargets.insert(val.str());
            PROGLOG("adding valid target: %s", val.str());
        }
    }
}

static void refreshValidDataPlaneNames()
{
    validDataPlaneNames.clear();
    Owned<IPropertyTreeIterator> dataPlanes = getGlobalConfigSP()->getElements("storage/planes[@category='data']");
    ForEach(*dataPlanes)
        validDataPlaneNames.insert(dataPlanes->query().queryProp("@name"));
}

static void configUpdate(const IPropertyTree *oldComponentConfiguration, const IPropertyTree *oldGlobalConfiguration)
{
    CriticalBlock block(configUpdateSect);
    // as much as effort [small] to check if different as to refresh
    refreshValidTargets();
    refreshValidDataPlaneNames();
    PROGLOG("Valid targets updated");
}

extern TPWRAPPER_API void validateTargetName(const char* target)
{
    if (isEmptyString(target))
        throw makeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Empty target name.");

    configUpdateHook.installOnce(configUpdate, true);
    CriticalBlock block(configUpdateSect);
    if (validTargets.find(target) == validTargets.end())
        throw makeStringExceptionV(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid target name: %s", target);
}

extern TPWRAPPER_API bool validateDataPlaneName(const char * remoteDali, const char * name)
{
    if (isEmptyString(name))
        throw makeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Empty data plane name.");

    configUpdateHook.installOnce(configUpdate, true);
    CriticalBlock block(configUpdateSect);
    return validDataPlaneNames.find(name) != validDataPlaneNames.end();
}

// NB: bare-metal has a different implementation in TpWrapper.cpp
bool getSashaService(StringBuffer &serviceAddress, const char *serviceName, bool failIfNotFound)
{
    return getService(serviceAddress, serviceName, failIfNotFound);
}

bool getSashaServiceEP(SocketEndpoint &serviceEndpoint, const char *service, bool failIfNotFound)
{
    StringBuffer serviceAddress;
    if (!getSashaService(serviceAddress, service, failIfNotFound))
        return false;
    serviceEndpoint.set(serviceAddress);
    return true;
}

static StringBuffer & getRoxieDefaultPlane(IPropertyTree * queue, StringBuffer & plane, const char * roxieName)
{
    if (queue->getProp("@dataPlane", plane))
        return plane;

    //Find the first data plane - better if it was retrieved from roxie config
    Owned<IPropertyTreeIterator> dataPlanes = getGlobalConfigSP()->getElements("storage/planes[@category='data']");
    if (!dataPlanes->first())
        throwUnexpectedX("No default data plane defined");
    return plane.append(dataPlanes->query().queryProp("@name"));
}

StringBuffer & getRoxieDefaultPlane(StringBuffer & plane, const char * roxieName)
{
    Owned<IPropertyTree> queue = getContainerClusterConfig(roxieName);
    if (!queue)
        throw makeStringExceptionV(ECLWATCH_INVALID_CLUSTER_NAME, "Unknown queue name %s", roxieName);

    return getRoxieDefaultPlane(queue, plane, roxieName);
}

//By default roxie will copy files from planes other than it's default, if a plane is added to directAccessPlanes
//  roxie will continue to read the file directly without making a copy
StringArray & getRoxieDirectAccessPlanes(StringArray & planes, StringBuffer &defaultPlane, const char * roxieName, bool includeDefaultPlane)
{
    Owned<IPropertyTree> queue = getContainerClusterConfig(roxieName);
    if (!queue)
        throw makeStringExceptionV(ECLWATCH_INVALID_CLUSTER_NAME, "Unknown queue name %s", roxieName);

    getRoxieDefaultPlane(queue, defaultPlane, roxieName);
    if (defaultPlane.length() && includeDefaultPlane)
        planes.appendUniq(defaultPlane);

    Owned<IPropertyTreeIterator> iter = queue->getElements("directAccessPlanes");
    ForEach(*iter)
    {
        const char *plane = iter->query().queryProp("");
        if (!isEmptyString(plane))
            planes.appendUniq(plane);
    }

    return planes;
}
