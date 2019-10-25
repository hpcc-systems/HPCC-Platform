/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#include "ws_machineService.hpp"
#include "jarray.hpp"
#include "dadfs.hpp"
#include "exception_util.hpp"
#include "workunit.hpp"
#include "roxiecommlib.hpp"
#include "componentstatus.hpp"
#include "rmtssh.hpp"
#include "platform.h"
#include "TpWrapper.hpp"

static const int THREAD_POOL_SIZE = 40;
static const int THREAD_POOL_STACK_SIZE = 64000;
static const char* FEATURE_URL = "MachineInfoAccess";

const unsigned ROXIECONTROLSTATETIMEOUT = 5000; //5 second

class CMachineInfoThreadParam : public CWsMachineThreadParam
{
public:
    IMPLEMENT_IINTERFACE;

    IEspContext&                    m_context;
    CGetMachineInfoUserOptions&     m_options;              //From request
    CMachineData&                   m_machineData;          //From request
    IArrayOf<IEspMachineInfoEx>&    m_machineInfoTable;     //For response
    StringArray&                    m_machineInfoColumns;   //For response
    MapStringTo<int>&               channelsMap;            //For response

    CMachineInfoThreadParam(Cws_machineEx* pService, IEspContext& context, CGetMachineInfoUserOptions&  options,
        CMachineData& machineData, IArrayOf<IEspMachineInfoEx>& machineInfoTable, StringArray& machineInfoColumns,
        MapStringTo<int>& _channelsMap)
       : CWsMachineThreadParam(NULL, NULL, NULL, pService),
         m_context(context),
         m_options(options),
         m_machineData(machineData),
         m_machineInfoTable(machineInfoTable),
         m_machineInfoColumns(machineInfoColumns),
         channelsMap(_channelsMap)
    {
    }

    virtual void doWork()
    {
        m_pService->doGetMachineInfo(m_context, this);
    }

    void addColumn(const char* columnName)
    {
        synchronized block(s_mutex);

        if (m_machineInfoColumns.find(columnName) == NotFound)
            m_machineInfoColumns.append(columnName);
    }
    int* getChannels(const char* key) { return channelsMap.getValue(key); };
private:
    static Mutex s_mutex;
};

Mutex CMachineInfoThreadParam::s_mutex;

class CRoxieStateInfoThreadParam : public CWsMachineThreadParam
{
public:
    StringAttr                      clusterName;
    IArrayOf<IEspMachineInfoEx>&    machineInfoTable;     //For response
    MapStringTo<int>&               channelsMap;          //For response

    CRoxieStateInfoThreadParam(Cws_machineEx* pService, const char* _clusterName,
        IArrayOf<IEspMachineInfoEx>& _machineInfoTable, MapStringTo<int>& _channelsMap)
        : CWsMachineThreadParam(pService), clusterName(_clusterName), machineInfoTable(_machineInfoTable),
        channelsMap(_channelsMap)
    {
    }

    virtual void doWork()
    {
        m_pService->getRoxieStateInfo(this);
    }
    int* getChannels(const char* key) { return channelsMap.getValue(key); };
};

class CGetMachineUsageThreadParam : public CWsMachineThreadParam
{
public:
    IEspContext&   espContext;
    IPropertyTree* request;

    CGetMachineUsageThreadParam(Cws_machineEx* pService, IEspContext& _espContext, IPropertyTree* _request)
       : CWsMachineThreadParam(pService), espContext(_espContext), request(_request) {}

    virtual void doWork()
    {
        m_pService->getMachineUsage(espContext, this);
    }
};

void Cws_machineEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    //Read settings from esp.xml
    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    Owned<IPropertyTree> pServiceNode = cfg->getPropTree(xpath.str());

    m_bMonitorDaliFileServer = pServiceNode->getPropBool("@monitorDaliFileServer", false);
    m_processFilters.setown( pServiceNode->getPropTree("ProcessFilters") );
    const char* pchExcludePartitions = pServiceNode->queryProp("@excludePartitions");
    if (pchExcludePartitions && *pchExcludePartitions)
    {
        StringArray sPartitions;
        sPartitions.appendList(pchExcludePartitions, ", ;");
        unsigned int numOfPartitions = sPartitions.ordinality();
        for (unsigned int i=0; i<numOfPartitions; i++)
        {
            const char* partition = sPartitions.item(i);
            if (!partition || !*partition)
                continue;

            if (strchr(partition, '*'))
                m_excludePartitionPatterns.insert( partition );
            else
                m_excludePartitions.insert( partition );
        }
    }

    m_useDefaultHPCCInit = pServiceNode->getPropBool("UseDefaultHPCCInit", true);//Still used by Rexec for now

    m_SSHConnectTimeoutSeconds = pServiceNode->getPropInt("SSHConnectTimeoutSeconds", 5);
    const char* machineInfoScript = pServiceNode->queryProp("MachineInfoFile");
    if (machineInfoScript && *machineInfoScript)
        m_machineInfoFile.append(machineInfoScript);
    else
        m_machineInfoFile.append("preflight");

    //Read settings from environment.xml
    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
    Owned<IPropertyTree> pEnvironmentRoot = &constEnv->getPTree();

    Owned<IPropertyTree> pEnvSettings = pEnvironmentRoot->getPropTree("EnvSettings");
    if (pEnvSettings)
    {
        pEnvSettings->getProp("configs", environmentConfData.m_configsPath.clear());
        pEnvSettings->getProp("path", environmentConfData.m_executionPath.clear());
        pEnvSettings->getProp("runtime", environmentConfData.m_runtimePath.clear());
        pEnvSettings->getProp("lock", environmentConfData.m_lockPath.clear());
        pEnvSettings->getProp("pid", environmentConfData.m_pidPath.clear());
        pEnvSettings->getProp("user", environmentConfData.m_user.clear());
    }

    machineUsageCache.setown(new MachineUsageCache(MACHINE_USAGE_MAX_CACHE_SIZE));
    machineUsageCacheMinutes = pServiceNode->getPropInt("MachineUsageCacheMinutes", MACHINE_USAGE_CACHE_MINUTES);

    m_threadPoolSize = pServiceNode->getPropInt("ThreadPoolSize", THREAD_POOL_SIZE);
    m_threadPoolStackSize = pServiceNode->getPropInt("ThreadPoolStackSize", THREAD_POOL_STACK_SIZE);

    //Start thread pool
    Owned<IThreadFactory> pThreadFactory = new CWsMachineThreadFactory();
    m_threadPool.setown(createThreadPool("WsMachine Thread Pool", pThreadFactory,
        NULL, m_threadPoolSize, 10000, m_threadPoolStackSize)); //10 sec timeout for available thread; use stack size of 2MB

    setupLegacyFilters();

    Owned<IComponentStatusFactory> factory = getComponentStatusFactory();
    factory->init(pServiceNode);
}

StringBuffer& Cws_machineEx::getAcceptLanguage(IEspContext& context, StringBuffer& acceptLanguage)
{
    context.getAcceptLanguage(acceptLanguage);
    if (!acceptLanguage.length())
    {
        acceptLanguage.set("en");
        return acceptLanguage;
    }
    acceptLanguage.setLength(2);
    VStringBuffer languageFile("%ssmc_xslt/nls/%s/hpcc.xml", getCFD(), acceptLanguage.str());
    if (!checkFileExists(languageFile.str()))
        acceptLanguage.set("en");
    return acceptLanguage;
}

bool Cws_machineEx::onGetMachineInfo(IEspContext &context, IEspGetMachineInfoRequest & req,
                                             IEspGetMachineInfoResponse & resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Machine Information. Permission denied.");

        StringArray& addresses = req.getAddresses();
        if (addresses.empty())
            throw MakeStringException(ECLWATCH_INVALID_IP_OR_COMPONENT, "No network address specified.");

        CGetMachineInfoData machineInfoData;
        readMachineInfoRequest(context, req.getGetProcessorInfo(), req.getGetStorageInfo(), req.getLocalFileSystemsOnly(), req.getGetSoftwareInfo(),
            req.getApplyProcessFilter(), addresses, req.getAddProcessesToFilter(), machineInfoData);
        getMachineInfo(context, machineInfoData);
        setMachineInfoResponse(context, req, machineInfoData, resp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_machineEx::onGetMachineInfoEx(IEspContext &context, IEspGetMachineInfoRequestEx & req, IEspGetMachineInfoResponseEx & resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Machine Information. Permission denied.");

        StringArray& addresses = req.getAddresses();
        if (addresses.empty())
            throw MakeStringException(ECLWATCH_INVALID_IP_OR_COMPONENT, "No network address specified.");

        double version = context.getClientVersion();
        CGetMachineInfoData machineInfoData;
        readMachineInfoRequest(context, true, true, true, true, true, addresses, NULL, machineInfoData);
        getMachineInfo(context, machineInfoData);
        if (machineInfoData.getMachineInfoTable().ordinality())
            resp.setMachines(machineInfoData.getMachineInfoTable());
        if (version >= 1.12)
        {
            StringBuffer acceptLanguage;
            resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool Cws_machineEx::onGetTargetClusterInfo(IEspContext &context, IEspGetTargetClusterInfoRequest & req,
                                             IEspGetTargetClusterInfoResponse & resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Target Cluster Information. Permission denied.");

        StringArray& targetClusters = req.getTargetClusters();
        if (targetClusters.empty())
            throw MakeStringException(ECLWATCH_INVALID_IP_OR_COMPONENT, "No target cluster specified.");

        CGetMachineInfoData machineInfoData;
        Owned<IPropertyTree> targetClustersOut = createPTreeFromXMLString("<Root/>");
        readMachineInfoRequest(context, req.getGetProcessorInfo(), req.getGetStorageInfo(), req.getLocalFileSystemsOnly(), req.getGetSoftwareInfo(),
            req.getApplyProcessFilter(), req.getAddProcessesToFilter(), targetClusters, machineInfoData, targetClustersOut);
        getMachineInfo(context, machineInfoData);
        setTargetClusterInfoResponse(context, req, machineInfoData, targetClustersOut, resp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void Cws_machineEx::addChannels(CGetMachineInfoData& machineInfoData, IPropertyTree* envRoot,
    const char* componentType, const char* componentName)
{
    VStringBuffer key("%s|%s", componentType, componentName);
    int* channels = machineInfoData.getChannels(key);
    if (channels)
        return;

    StringBuffer path("Software/");
    if (strieq(componentType, eqThorSlaveProcess))
        path.append(eqThorCluster);
    else if (strieq(componentType, eqRoxieServerProcess))
        path.append(eqRoxieCluster);
    else
        throw MakeStringException(ECLWATCH_INVALID_COMPONENT_TYPE, "Invalid %s in Cws_machineEx::addChannels().", componentType);

    path.appendf("[@name=\"%s\"]", componentName);

    Owned<IPropertyTree> component;
    if (envRoot)
    {
        component.setown(envRoot->getPropTree(path));
    }
    else
    {
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
        Owned<IPropertyTree> root = &constEnv->getPTree();
        component.setown(root->getPropTree(path));
    }
    if (!component)
        throw MakeStringException(ECLWATCH_INVALID_IP_OR_COMPONENT, "%s not found.", componentName);

    StringAttr attr;
    if (strieq(componentType, eqThorSlaveProcess))
        attr.set("@channelsPerSlave");
    else
        attr.set("@channelsPerNode");
    if (component->hasProp(attr.get()))
        machineInfoData.addToChannelsMap(key, component->getPropInt(attr.get()));
}
////////////////////////////////////////////////////////////////////////////////////////
// Read Machine Infomation request and collect related settings from environment.xml  //
////////////////////////////////////////////////////////////////////////////////////////

void Cws_machineEx::readMachineInfoRequest(IEspContext& context, bool getProcessorInfo, bool getStorageInfo, bool localFileSystemsOnly, bool getSoftwareInfo, bool applyProcessFilter,
                                           StringArray& processes, const char* addProcessesToFilters, CGetMachineInfoData& machineInfoData)
{
    double version = context.getClientVersion();
    StringBuffer userID, password;
    context.getUserID(userID);
    context.getPassword(password);
    machineInfoData.getOptions().setUserName(userID.str());
    machineInfoData.getOptions().setPassword(password.str());

    machineInfoData.getOptions().setGetProcessorInfo(getProcessorInfo);
    machineInfoData.getOptions().setGetStorageInfo(getStorageInfo);
    machineInfoData.getOptions().setLocalFileSystemsOnly(localFileSystemsOnly);
    machineInfoData.getOptions().setGetSoftwareInfo(getSoftwareInfo);
    machineInfoData.getOptions().setApplyProcessFilter(applyProcessFilter);

    machineInfoData.getOptions().getAdditionalProcessFilters().appendList(addProcessesToFilters, " ,\t");

    BoolHash uniqueProcesses;
    for (unsigned i=0; i<processes.ordinality(); i++)
    {
        StringArray address;
        address.appendList(processes.item(i), ":");

        StringBuffer address1, address2, processType, compName, path;
        unsigned processNumber = 0;
        if (!machineInfoData.getOptions().getGetSoftwareInfo())
        {
            parseAddresses(address.item(0), address1, address2);
        }
        else
        {
            if (address.ordinality() < 5)
                throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Invalid address format in '%s'.", processes.item(i));

            parseProcessString(address, address1, address2, processType, compName, path, processNumber);
        }

        setProcessRequest(machineInfoData, uniqueProcesses, address1.str(), address2.str(), processType.str(), compName.str(), path.str(), processNumber);
        if (strieq(processType.str(), eqRoxieServerProcess))
            machineInfoData.appendRoxieClusters(compName.str());
        if ((version >= 1.16) && (strieq(processType, eqThorSlaveProcess) || strieq(processType, eqRoxieServerProcess)))
            addChannels(machineInfoData, nullptr, processType, compName);
    }
}


void Cws_machineEx::readMachineInfoRequest(IEspContext& context, bool getProcessorInfo, bool getStorageInfo, bool localFileSystemsOnly, bool getSoftwareInfo, bool applyProcessFilter,
                                           const char* addProcessesToFilters, StringArray& targetClustersIn, CGetMachineInfoData& machineInfoData, IPropertyTree* targetClusterTreeOut)
{
    StringBuffer userID, password;
    context.getUserID(userID);
    context.getPassword(password);
    machineInfoData.getOptions().setUserName(userID.str());
    machineInfoData.getOptions().setPassword(password.str());

    machineInfoData.getOptions().setGetProcessorInfo(getProcessorInfo);
    machineInfoData.getOptions().setGetStorageInfo(getStorageInfo);
    machineInfoData.getOptions().setLocalFileSystemsOnly(localFileSystemsOnly);
    machineInfoData.getOptions().setGetSoftwareInfo(getSoftwareInfo);
    machineInfoData.getOptions().setApplyProcessFilter(applyProcessFilter);

    machineInfoData.getOptions().getAdditionalProcessFilters().appendList(addProcessesToFilters, " ,\t");

    readSettingsForTargetClusters(context, targetClustersIn, machineInfoData, targetClusterTreeOut);
}

//Parses address request from machine information request in the form "192.168.1.4-6|."
//The second address is the address retrieved from environment setting (could be a '.').
void Cws_machineEx::parseAddresses(const char *address, StringBuffer& address1, StringBuffer& address2)
{
    address1 = address;
    address2.clear();

    const char* props1 = strchr(address, '|');
    if (props1)
    {
        address2 = props1+1;
        address1.setLength(props1 - address);
    }

    address1.trim();
    address2.trim();
}

//Parses machine information request for each process in the form "192.168.1.4-6|.:ThorSlaveProcess:thor1:2:/var/lib/..."
void Cws_machineEx::parseProcessString(StringArray& process, StringBuffer& address1, StringBuffer& address2,
                                       StringBuffer& processType, StringBuffer& compName, StringBuffer& path, unsigned& processNumber)
{
    parseAddresses(process.item(0), address1, address2);

    processType.clear().append( process.item(1) ).trim();
    compName.clear().append( process.item(2) ).trim();
    EnvMachineOS os  = (EnvMachineOS) atoi( process.item(3) );

    path.clear().append( process.item(4) ).trim();
    if (path.length())
    {
        char pat1, pat2;
        char rep1, rep2;

        if (os == MachineOsLinux)
        {
            pat1 = ':'; rep1 = '$';
            pat2 = '\\';rep2 = '/';
        }
        else
        {
            pat1 = '$'; rep1 = ':';
            pat2 = '/';rep2 = '\\';
        }

        path.replace( pat1, rep1 );
        path.replace( pat2, rep2 );

        if ((os == MachineOsLinux) && (path.charAt(0) != '/'))
            path.insert(0, '/');
    }

    if (process.ordinality() < 6)
        return;

    processNumber  = atoi( process.item(5) );
}

void Cws_machineEx::setProcessRequest(CGetMachineInfoData& machineInfoData, BoolHash& uniqueProcesses, const char* address1, const char* address2,
                                          const char* processType, const char* compName,  const char* path, unsigned processNumber)
{
    IpAddress ipAddr;
    unsigned numIps = ipAddr.ipsetrange(address1);
    //address is like 192.168.1.4-6
    //so process each address in the range

    if (!ipAddr.isIp4())
        IPV6_NOT_IMPLEMENTED();

    //Always use "EclAgentProcess" to retrieve machine info for "AgentExecProcess"
    StringBuffer processTypeStr;
    if (processType && *processType)
    {
        if (strieq(processType, eqAgentExec))
            processTypeStr.append(eqEclAgent);
        else
            processTypeStr = processType;
    }

    while (numIps--)
    {
        unsigned numAddr;
        if (ipAddr.getNetAddress(sizeof(numAddr),&numAddr)!=sizeof(numAddr))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid network address.");

        ipAddr.ipincrement(1);

        //Clean possible duplication
        StringBuffer valuesToBeChecked;
        valuesToBeChecked.append(numAddr);
        if (machineInfoData.getOptions().getGetSoftwareInfo())
            valuesToBeChecked.appendf(":%s:%s:%d", processTypeStr.str(), compName, processNumber);
        bool* found = uniqueProcesses.getValue(valuesToBeChecked.str());
        if (found && *found)
            continue;
        uniqueProcesses.setValue(valuesToBeChecked.str(), true);

        addProcessRequestToMachineInfoData(machineInfoData, address1, address2, processTypeStr.str(), compName, path, processNumber);
    }
}

void Cws_machineEx::addProcessRequestToMachineInfoData(CGetMachineInfoData& machineInfoData, const char* address1, const char* address2,
                                          const char* processType, const char* compName,  const char* path, unsigned processNumber)
{
    CIArrayOf<CMachineData>& machines = machineInfoData.getMachineData();
    ForEachItemIn(idx, machines)
    {
        CMachineData& machine = machines.item(idx);
        if (streq(address1, machine.getNetworkAddress()))
        {
            addProcessData(&machine, processType, compName, path, processNumber);
            return;
        }
    }

    char pathSep;
    EnvMachineOS os;
    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IConstMachineInfo> pMachineInfo = constEnv->getMachineByAddress(address1);
    if (pMachineInfo.get())
        os = pMachineInfo->getOS();
    else
        os = MachineOsUnknown;
    if (os == MachineOsW2K)
        pathSep = '\\';
    else
        pathSep = '/';

    Owned<CMachineData> machineNew = new CMachineData(address1, address2, os, pathSep);

    //Read possible dependencies for all processes
    set<string>& dependenciesForAllProcesses = machineNew->getDependencies();
    StringBuffer xPath;
    xPath.appendf("Platform[@name='%s']/ProcessFilter[@name='any']/Process", machineNew->getOS() == MachineOsW2K ? "Windows" : "Linux");
    Owned<IPropertyTreeIterator> processes = m_processFilters->getElements(xPath.str());
    ForEach (*processes)
    {
        StringBuffer processName;
        processes->query().getProp("@name", processName);
        processName.toLowerCase().replaceString(".exe", "");

        if ((processName.length() > 0) && (!streq(processName.str(), "hoagentd"))) //hoagentd is not needed anymore
            dependenciesForAllProcesses.insert(processName.str());
    }
    if (m_bMonitorDaliFileServer && (dependenciesForAllProcesses.find("dafilesrv") == dependenciesForAllProcesses.end()))
        dependenciesForAllProcesses.insert("dafilesrv");

    addProcessData(machineNew, processType, compName, path, processNumber);

    machines.append(*machineNew.getClear());
}

//Create a CProcessData object and add it to CMachineData
void Cws_machineEx::addProcessData(CMachineData* machine, const char* processType, const char* compName,
                                   const char* path, unsigned processNumber)
{
    if (!machine)
        return;

    StringBuffer pathStr(path);
    if (pathStr.length() > 0)
    {
        char pathSep = machine->getPathSep();
        if (pathStr.charAt(pathStr.length() - 1) != pathSep)
            pathStr.append(pathSep);
    }

    Owned<CProcessData> process = new CProcessData(compName, processType, pathStr.str(), processNumber);

    //Copy dependencies for all processes
    set<string>& dependenciesForThisProcess = process->getDependencies();
    set<string>& dependenciesForAllProcesses = machine->getDependencies();
    set<string>::const_iterator it   = dependenciesForAllProcesses.begin();
	set<string>::const_iterator iEnd = dependenciesForAllProcesses.end();
	for (; it != iEnd; it++) //add in sorted order simply by traversing the map
        dependenciesForThisProcess.insert((*it).c_str());

    //now collect "process-specific" dependencies
    StringBuffer xPath;
    xPath.appendf("Platform[@name='%s']/ProcessFilter[@name='%s']", machine->getOS() == MachineOsW2K ? "Windows" : "Linux", processType);
    IPropertyTree* processFilterNode = m_processFilters->queryPropTree( xPath.str() );
    if (!processFilterNode)
    {
        machine->getProcesses().append(*process.getClear());
        return;
    }

    Owned<IPropertyTreeIterator> processes = processFilterNode->getElements("Process");
    ForEach (*processes)
    {
        IPropertyTree* pProcess = &processes->query();
        const char* name = pProcess->queryProp("@name");
        if (!name || streq(name, "."))
            continue;

        StringBuffer processName(name);
        processName.toLowerCase().replaceString(".exe", "");
        if (processName.length() < 1)
            continue;

        //Environment.xml may contain old filter settings.
        if (isLegacyFilter(processType, processName.str()))
            continue;

        if (pProcess->getPropBool("@remove", false))
            dependenciesForThisProcess.erase(processName.str());
        else
            dependenciesForThisProcess.insert(processName.str());
    }

    process->setMultipleInstances(machine->getOS() == MachineOsLinux && processFilterNode->getPropBool("@multipleInstances", false));

    machine->getProcesses().append(*process.getClear());
}

//Collect process settings for the requested target clusters
void Cws_machineEx::readSettingsForTargetClusters(IEspContext& context, StringArray& targetClusters, CGetMachineInfoData& machineInfoData, IPropertyTree* targetClustersOut)
{
    unsigned ordinality= targetClusters.ordinality();
    if (ordinality < 1)
        return;

    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IPropertyTree> pEnvironmentRoot = &constEnv->getPTree();
    if (!pEnvironmentRoot)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

    BoolHash uniqueProcesses;
    for (unsigned index=0; index<ordinality; index++)
    {
        StringBuffer clusterType;
        const char* clusterName = targetClusters.item(index);
        const char* pClusterName = strchr(clusterName, ':');
        if (pClusterName)
        {
            clusterType.append(clusterName, 0, pClusterName - clusterName);
            pClusterName++;
        }

        if (!pClusterName || !*pClusterName)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Cluster name not specified.");
        if (clusterType.length() < 1)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Cluster type not specified.");

        StringBuffer path;
        path.appendf("Software/Topology/Cluster[@name='%s']", pClusterName);
        IPropertyTree* pCluster = pEnvironmentRoot->queryPropTree(path.str());
        if (!pCluster)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Cluster %s not found in environment setting.", pClusterName);

        Owned<IPropertyTreeIterator> clusterProcesses;
        if (strieq(clusterType.str(), eqThorCluster) || strieq(clusterType.str(), eqRoxieCluster))
        {
            clusterProcesses.setown(pCluster->getElements(clusterType.str()));
            if (!clusterProcesses->first())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Cluster %s not found in environment setting.", clusterType.str());
        }

        Owned<IPropertyTreeIterator> eclCCServerProcesses= pCluster->getElements(eqEclCCServer);
        Owned<IPropertyTreeIterator> eclServerProcesses= pCluster->getElements(eqEclServer);
        Owned<IPropertyTreeIterator> eclAgentProcesses= pCluster->getElements(eqEclAgent);
        Owned<IPropertyTreeIterator> eclSchedulerProcesses= pCluster->getElements(eqEclScheduler);

        IPropertyTree *targetClusterOut = targetClustersOut->addPropTree("TargetCluster", createPTree("TargetCluster"));
        targetClusterOut->setProp("@Name", pClusterName);
        targetClusterOut->setProp("@Type", clusterType.str());

        //Read Cluster processes
        if (clusterProcesses && clusterProcesses->first())
            ForEach(*clusterProcesses)
                readTargetClusterProcesses(context, clusterProcesses->query(), clusterType.str(), uniqueProcesses, machineInfoData, targetClusterOut);

        //Read eclCCServer process
        if (eclCCServerProcesses->first())
            readTargetClusterProcesses(context, eclCCServerProcesses->query(), eqEclCCServer, uniqueProcesses, machineInfoData, targetClusterOut);

        //Read eclServer process
        if (eclServerProcesses->first())
            readTargetClusterProcesses(context, eclServerProcesses->query(), eqEclServer, uniqueProcesses, machineInfoData, targetClusterOut);

        //Read eclAgent process
        if (eclAgentProcesses->first())
            readTargetClusterProcesses(context, eclAgentProcesses->query(), eqEclAgent, uniqueProcesses, machineInfoData, targetClusterOut);

        //Read eclScheduler process
        if (eclSchedulerProcesses->first())
            readTargetClusterProcesses(context, eclSchedulerProcesses->query(), eqEclScheduler, uniqueProcesses, machineInfoData, targetClusterOut);
    }
}

//Collect settings for one group of target cluster processes
void Cws_machineEx::readTargetClusterProcesses(IEspContext& context, IPropertyTree &processNode, const char* nodeType, BoolHash& uniqueProcesses, CGetMachineInfoData& machineInfoData,
                                              IPropertyTree* targetClustersOut)
{
    const char* process = processNode.queryProp("@process");
    if (!process || !*process)
        throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Process attribute not set for ECLCCServer in environment setting.");

    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IPropertyTree> pEnvironmentRoot = &constEnv->getPTree();
    if (!pEnvironmentRoot)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

    IPropertyTree* pEnvironmentSoftware = pEnvironmentRoot->queryPropTree("Software");
    if (!pEnvironmentSoftware)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

    double version = context.getClientVersion();
    IPropertyTree* pClusterProcess = NULL;
    if (strieq(nodeType, eqThorCluster) || strieq(nodeType, eqRoxieCluster))
    {
        StringBuffer path;
        path.appendf("Software/%s[@name='%s']", nodeType, process);
        pClusterProcess = pEnvironmentRoot->queryPropTree(path.str());
        if (!pClusterProcess)
            throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Process not set for %s in environment setting.", path.str());

        if (strieq(nodeType, eqRoxieCluster))
            machineInfoData.appendRoxieClusters(process);
    }

    IPropertyTree *pInfo = targetClustersOut->addPropTree("Process", createPTree("Process"));
    pInfo->setProp("@Name", process);
    pInfo->setProp("@Type", nodeType);

    StringBuffer dirStr;
    IPropertyTree* pEnvironmentDirectories = pEnvironmentSoftware->queryPropTree("Directories");
    if (!pClusterProcess)
    {
        if (!pEnvironmentDirectories || !getConfigurationDirectory(pEnvironmentDirectories, "run", nodeType, process, dirStr))
            dirStr.clear().append(processNode.queryProp("@directory"));

        getProcesses(constEnv, pEnvironmentSoftware, process, nodeType, dirStr.str(), machineInfoData, false, uniqueProcesses);
        return;
    }

    if (!pEnvironmentDirectories || !getConfigurationDirectory(pEnvironmentDirectories, "run", nodeType, process, dirStr))
        dirStr.clear().append(pClusterProcess->queryProp("@directory"));

    if (strieq(nodeType, eqThorCluster))
    {
        getProcesses(constEnv, pClusterProcess, process, eqThorMasterProcess, dirStr.str(), machineInfoData, true, uniqueProcesses);
        getThorProcesses(constEnv, pClusterProcess, process, eqThorSlaveProcess, dirStr.str(), machineInfoData, uniqueProcesses);
        getThorProcesses(constEnv, pClusterProcess, process, eqThorSpareProcess, dirStr.str(), machineInfoData, uniqueProcesses);
        if (version >= 1.16)
           addChannels(machineInfoData, pEnvironmentRoot, eqThorSlaveProcess, process);

    }
    else if (strieq(nodeType, eqRoxieCluster))
    {
        BoolHash uniqueRoxieProcesses;
        getProcesses(constEnv, pClusterProcess, process, eqRoxieServerProcess, dirStr.str(), machineInfoData, true, uniqueProcesses, &uniqueRoxieProcesses);
        if (version >= 1.16)
           addChannels(machineInfoData, pEnvironmentRoot, eqRoxieServerProcess, process);
    }
}

void Cws_machineEx::getThorProcesses(IConstEnvironment* constEnv, IPropertyTree* cluster, const char* processName,
                                       const char* processType, const char* directory, CGetMachineInfoData& machineInfoData, BoolHash& uniqueProcesses)
{
    if (!constEnv || !cluster)
        return;

    Owned<IGroup> nodeGroup;
    StringBuffer groupName;
    if (strieq(processType, eqThorSlaveProcess))
    {
        getClusterGroupName(*cluster, groupName);
        if (groupName.length() < 1)
        {
            OWARNLOG("Cannot find group name for %s", processName);
            return;
        }
        nodeGroup.setown(getClusterProcessNodeGroup(processName, eqThorCluster));
    }
    else
    {
        getClusterSpareGroupName(*cluster, groupName);
        if (groupName.length() < 1)
        {
            OWARNLOG("Cannot find group name for %s", processName);
            return;
        }
        nodeGroup.setown(queryNamedGroupStore().lookup(groupName.str()));
    }
    if (!nodeGroup || (nodeGroup->ordinality() == 0))
    {
        OWARNLOG("Cannot find node group for %s", processName);
        return;
    }

    int slavesPerNode = cluster->getPropInt("@slavesPerNode");
    Owned<INodeIterator> gi = nodeGroup->getIterator();
    ForEach(*gi)
    {
        StringBuffer addressRead;
        gi->query().endpoint().getIpText(addressRead);
        if (addressRead.length() == 0)
        {
            OWARNLOG("Network address not found for a node in node group %s", groupName.str());
            continue;
        }

        StringBuffer netAddress;
        const char* ip = addressRead.str();
        if (!streq(ip, "."))
        {
            netAddress.append(ip);
        }
        else
        {
            IpAddress ipaddr = queryHostIP();
            ipaddr.getIpText(netAddress);
        }

        if (netAddress.length() == 0)
        {
            OWARNLOG("Network address not found for a node in node group %s", groupName.str());
            continue;
        }

        Owned<IConstMachineInfo> pMachineInfo =  constEnv->getMachineByAddress(addressRead.str());
        if (!pMachineInfo.get())
        {
            OWARNLOG("Machine not found at network address %s", addressRead.str());
            continue;
        }

        //Each thor slave is a process. The i is used to check whether the process is running or not.
        for (unsigned i = 1; i <= slavesPerNode; i++)
            setProcessRequest(machineInfoData, uniqueProcesses, netAddress.str(), addressRead.str(),
                processType, processName, directory, i);
    }

    return;
}

void Cws_machineEx::getProcesses(IConstEnvironment* constEnv, IPropertyTree* environment, const char* processName,
                                 const char* processType, const char* directory, CGetMachineInfoData& machineInfoData,
                                 bool isThorOrRoxieProcess, BoolHash& uniqueProcesses, BoolHash* uniqueRoxieProcesses)
{
    Owned<IPropertyTreeIterator> processes= environment->getElements(processType);
    ForEach(*processes)
    {
        StringArray processInstances, directories;

        IPropertyTree &process = processes->query();
        //Thor master and roxie server has been checked before this call.
        if (!isThorOrRoxieProcess)
        {
            const char* name = process.queryProp("@name");
            if (!name || !*name || !streq(name, processName))
                continue;
        }

        const char* computerName = process.queryProp("@computer");
        if (computerName && *computerName)
            appendProcessInstance(computerName, directory, NULL, processInstances, directories);
        else
        {
            Owned<IPropertyTreeIterator> instances= process.getElements("Instance");
            ForEach(*instances)
            {
                IPropertyTree &instance = instances->query();
                appendProcessInstance(instance.queryProp("@computer"), directory, instance.queryProp("@directory"), processInstances, directories);
            }
        }

        if (processInstances.length() < 1)
            continue;

        for (unsigned i = 0; i < processInstances.length(); i++)
        {
            const char* name0 = processInstances.item(i);
            const char* directory0 = directories.item(i);
            if (uniqueRoxieProcesses)//to avoid duplicate entries for roxie (one machine has only one roxie process).
            {
                bool* found = uniqueRoxieProcesses->getValue(name0);
                if (found && *found)
                    continue;
                uniqueRoxieProcesses->setValue(name0, true);
            }

            Owned<IConstMachineInfo> pMachineInfo =  constEnv->getMachine(name0);
            if (!pMachineInfo.get())
            {
                OWARNLOG("Machine %s not found in environment setting", name0);
                continue;
            }

            SCMStringBuffer ep;
            pMachineInfo->getNetAddress(ep);

            const char* ip = ep.str();
            if (!ip)
            {
                OWARNLOG("Network address not found for machine %s", name0);
                continue;
            }

            StringBuffer netAddress;
            StringBuffer configNetAddress(ip);
            if (!streq(ip, "."))
            {
                netAddress.set(ip);
            }
            else
            {
                IpAddress ipaddr = queryHostIP();
                ipaddr.getIpText(netAddress);
            }

            setProcessRequest(machineInfoData, uniqueProcesses, netAddress.str(), configNetAddress.str(), processType, processName, directory0);
        }
    }

    return;
}

void Cws_machineEx::setupLegacyFilters()
{
    unsigned idx = 0;
    while (legacyFilterStrings[idx])
    {
        m_legacyFilters.setValue(legacyFilterStrings[idx], true);
        idx++;
    }
    return;
}

bool Cws_machineEx::isLegacyFilter(const char* processType, const char* dependency)
{
    if (!processType || !*processType || !dependency || !*dependency)
        return false;

    StringBuffer filterString;
    filterString.appendf("%s:%s", processType, dependency);
    bool* found = m_legacyFilters.getValue(filterString.str());
    if (found && *found)
        return true;

    return false;
}

//The stateHashes stores different state hashes in one roxie cluster.
//It also stores how many roxie nodes have the same state hashes.
unsigned Cws_machineEx::addRoxieStateHash(const char* hash, StateHashes& stateHashes, unsigned& totalUniqueHashes)
{
    if (!hash || !*hash)
        return -1;

    unsigned hashID = 0;
    IStateHash* stateHash = stateHashes.getValue(hash);
    if (stateHash)
    {
        //if the stateHashes already has the same 'hash', increases the count for the 'stateHash'.
        //The 'StateHash' with the highest count will be the 'Major StateHash'.
        //If a roxie node does not contain the 'Major StateHash', it has a 'mismatch' state hash.
        hashID = stateHash->queryID();
        stateHash->incrementCount();
    }
    else
    {
        //Add a new 'StateHash'. Set its hashID to totalUniqueHashes and set its count to 1.
        hashID = totalUniqueHashes;
        Owned<IStateHash> newStateHash = new CStateHash(hashID, 1);
        stateHashes.setValue(hash, newStateHash);
        totalUniqueHashes++;
    }
    return hashID;
}

void Cws_machineEx::updateMajorRoxieStateHash(StateHashes& stateHashes, CIArrayOf<CRoxieStateData>& roxieStates)
{
    //Find out which state hash is for the most of the roxie nodes inside this roxie cluster.
    unsigned majorHashID = 0;
    unsigned majorHashCount = 0;
    HashIterator hashes(stateHashes);
    ForEach(hashes)
    {
        IStateHash *hash = stateHashes.mapToValue(&hashes.query());
        unsigned hashCount = hash->queryCount();
        if (majorHashCount >= hashCount)
            continue;
        majorHashCount = hashCount;
        majorHashID = hash->queryID();
    }

    //Set the MajorHash to false if the roxie node's HashID() != majorHashID.
    ForEachItemIn(ii, roxieStates)
    {
        CRoxieStateData& roxieState = roxieStates.item(ii);
        if (roxieState.getHashID() != majorHashID)
            roxieState.setMajorHash(false);
    }
}

void Cws_machineEx::readRoxieStatus(const Owned<IPropertyTree> controlResp, CIArrayOf<CRoxieStateData>& roxieStates)
{
    StateHashes stateHashes;
    unsigned totalUniqueHashes = 0;
    Owned<IPropertyTreeIterator> roxieEndpoints = controlResp->getElements("Endpoint");
    ForEach(*roxieEndpoints)
    {
        IPropertyTree& roxieEndpoint = roxieEndpoints->query();
        const char *ep = roxieEndpoint.queryProp("@ep");
        if (!ep || !*ep)
            continue;

        bool ok = false, attached = false, detached = false;
        const char *status = roxieEndpoint.queryProp("Status");
        if (status && strieq(status, "ok"))
            ok = true;
        const char *stateHash = roxieEndpoint.queryProp("State/@hash");
        if (roxieEndpoint.hasProp("Dali/@connected"))
        {
            if (roxieEndpoint.getPropBool("Dali/@connected"))
                attached = true;
            else
                detached = true;
        }

        StringArray locations;
        locations.appendListUniq(ep, ":");
        Owned<CRoxieStateData> roxieState = new CRoxieStateData(locations.item(0), addRoxieStateHash(stateHash, stateHashes, totalUniqueHashes));
        roxieState->setState(ok, attached, detached, stateHash);
        roxieStates.append(*roxieState.getClear());
    }
    if (totalUniqueHashes > 1)
        updateMajorRoxieStateHash(stateHashes, roxieStates);
}

void Cws_machineEx::getRoxieStateInfo(CRoxieStateInfoThreadParam* param)
{
    const char* clusterName = param->clusterName.get();
    if (!clusterName || !*clusterName)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Roxie cluster not specified.");

    SocketEndpointArray servers;
    getRoxieProcessServers(clusterName, servers);
    if (!servers.length())
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Roxie Process server not found.");

    Owned<IRoxieCommunicationClient> roxieClient = createRoxieCommunicationClient(servers.item(0), ROXIECONTROLSTATETIMEOUT);
    Owned<IPropertyTree> controlResp = roxieClient->sendRoxieControlAllNodes("<control:state/>", true);
    if (!controlResp)
        throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to get control response from roxie %s.", clusterName);

    CIArrayOf<CRoxieStateData> roxieStates;
    readRoxieStatus(controlResp, roxieStates);

    ForEachItemIn(i, param->machineInfoTable)
    {
        IEspMachineInfoEx& machineInfo = param->machineInfoTable.item(i);
        if (!streq(machineInfo.getProcessType(), eqRoxieServerProcess) || !streq(machineInfo.getComponentName(), clusterName))
            continue;

        //This method is thread safe because each machineInfo (for one roxie node) belongs to only one Roxie cluster.
        //It is impossible for different threads to update the same machineInfo.
        bool foundRoxieState = false;
        ForEachItemIn(ii, roxieStates)
        {
            CRoxieStateData& roxieState = roxieStates.item(ii);
            if (!roxieState.matchIPAddress(machineInfo.getAddress()))
                continue;

            StringBuffer state, stateDetails;
            roxieState.reportState(state, stateDetails);
            machineInfo.setRoxieState(state.str());
            machineInfo.setRoxieStateDetails(stateDetails.str());
            foundRoxieState = true;
        }
        if (!foundRoxieState)
        {
            machineInfo.setRoxieState("??");
            machineInfo.setRoxieStateDetails("Roxie state not found");
        }
    }
}

void Cws_machineEx::getMachineInfo(IEspContext& context, bool getRoxieState, CGetMachineInfoData& machineInfoData)
{
    UnsignedArray threadHandles;
    if (!getRoxieState)
    {
        CIArrayOf<CMachineData>& machines = machineInfoData.getMachineData();
        ForEachItemIn(idx, machines)
        {
            Owned<CMachineInfoThreadParam> pThreadReq = new CMachineInfoThreadParam(this, context, machineInfoData.getOptions(),
                machines.item(idx), machineInfoData.getMachineInfoTable(), machineInfoData.getMachineInfoColumns(),
                machineInfoData.getChannelsMap());
            PooledThreadHandle handle = m_threadPool->start( pThreadReq.getClear());
            threadHandles.append(handle);
        }
    }
    else
    {
        StringArray& roxieClusters = machineInfoData.getRoxieClusters();
        ForEachItemIn(i, roxieClusters)
        {
            Owned<CRoxieStateInfoThreadParam> pThreadReq = new CRoxieStateInfoThreadParam(this, roxieClusters.item(i),
                machineInfoData.getMachineInfoTable(), machineInfoData.getChannelsMap());
            PooledThreadHandle handle = m_threadPool->start( pThreadReq.getClear());
            threadHandles.append(handle);
        }
        machineInfoData.getMachineInfoColumns().append("Roxie State");
    }

    //Block for worker threads to finish, if necessary and then collect results
    //Not use joinAll() because multiple threads may call this method. Each call uses the pool to create
    //its own threads of checking query state. Each call should only join the ones created by that call.
    ForEachItemIn(i, threadHandles)
        m_threadPool->join(threadHandles.item(i));
}

////////////////////////////////////////////////////////////////////
// Get Machine Information based on Machine Information request   //
////////////////////////////////////////////////////////////////////

void Cws_machineEx::getMachineInfo(IEspContext& context, CGetMachineInfoData& machineInfoData)
{
    double version = context.getClientVersion();
    getMachineInfo(context, false, machineInfoData);
    if ((version >= 1.13) && !machineInfoData.getRoxieClusters().empty())
        getMachineInfo(context, true, machineInfoData);
}

// the following method is invoked on worker threads of CMachineInfoThreadParam
void Cws_machineEx::doGetMachineInfo(IEspContext& context, CMachineInfoThreadParam* pParam)
{
#ifdef DETECT_WS_MC_MEM_LEAKS
    static bool firstTime = true;
    if (firstTime)
    {
        firstTime = false;
        unsigned t = setAllocHook(true);
    }
#endif //DETECT_WS_MC_MEM_LEAKS
    int error = 0;
    StringBuffer preflightCommand, response;
    buildPreflightCommand(context, pParam, preflightCommand);
    if (preflightCommand.length() < 1)
    {
        response.append("Failed in creating Machine Information command.\n");
        error = -1;
    }
    else
    {
        error = runCommand(context, pParam->m_machineData.getNetworkAddress(), pParam->m_machineData.getNetworkAddressInEnvSetting(), pParam->m_machineData.getOS(), preflightCommand.str(), pParam->m_options.getUserName(), pParam->m_options.getPassword(), response);
        if ((error == 0) && (response.length() > 0))
            readPreflightResponse(context, pParam, response.str(), error);
    }

    //Set IArrayOf<IEspMachineInfoEx> based on Preflight Response
    setMachineInfo(context, pParam, response.str(), error);

#ifdef DETECT_WS_MC_MEM_LEAKS
    DBGLOG("Allocated=%d", setAllocHook(false));
#endif //DETECT_WS_MC_MEM_LEAKS
}

void Cws_machineEx::buildPreflightCommand(IEspContext& context, CMachineInfoThreadParam* pParam, StringBuffer& preflightCommand)
{
    preflightCommand.clear().appendf("/%s/sbin/%s -p=%s", environmentConfData.m_executionPath.str(),
        m_machineInfoFile.str(), environmentConfData.m_pidPath.str());
    if (preflightCommand.charAt(preflightCommand.length() - 1) == pParam->m_machineData.getPathSep())
        preflightCommand.remove(preflightCommand.length()-1, 1);

    bool checkDependency = false;
    CIArrayOf<CProcessData>& processes = pParam->m_machineData.getProcesses();
    ForEachItemIn(idx, processes)
    {
        CProcessData& process = processes.item(idx);
        if (!process.getName() || !*process.getName())
            continue;

        StringBuffer procName;
        if (streq(process.getType(), eqThorSlaveProcess))
            procName.appendf("thorslave_%s_%d,%s_slave_%d", process.getName(), process.getProcessNumber(), process.getName(), process.getProcessNumber());
        else if (streq(process.getType(), eqThorMasterProcess))
            procName.appendf("%s,%s_master", process.getName(), process.getName());
        else
            procName.append(process.getName());

        if (idx < 1)
            preflightCommand.appendf(" -n=%s", procName.str());
        else
            preflightCommand.appendf(",%s", procName.str());

        if (!process.getDependencies().empty())
            checkDependency = true;
    }

    if (checkDependency || !pParam->m_options.getApplyProcessFilter())
        preflightCommand.append(" -d=ALL");
    if (pParam->m_options.getGetStorageInfo() && !pParam->m_options.getLocalFileSystemsOnly())
        preflightCommand.append(" -m=YES");
}

int Cws_machineEx::runCommand(IEspContext& context, const char* sAddress, const char* sConfigAddress, EnvMachineOS os,
                              const char* sCommand, const char* sUserId, const char* sPassword, StringBuffer& response)
{
    int exitCode = -1;

    try
    {
        StringBuffer command(sCommand);
        StringBuffer userId;
        StringBuffer password;
        bool bLinux;

        if (sConfigAddress && *sConfigAddress)
            getAccountAndPlatformInfo(sConfigAddress, userId, password, bLinux);
        else
            getAccountAndPlatformInfo(sAddress, userId, password, bLinux);

        if (!sUserId || !*sUserId || !sPassword ||!*sPassword)
        {
            //BUG: 9825 - remote execution on linux needs to use individual accounts
            //use userid/password in ESP context for remote execution...
            if (bLinux)
            {
                userId.clear();
                password.clear();
                context.getUserID(userId);
                context.getPassword(password);
            }
        }
        else
        {
            userId.clear().append(sUserId);
            password.clear().append(sPassword);
        }


        // make sure there actually is something in command before we run a remote connection
        if (command.length() < 1)
            return exitCode;

        Owned<IFRunSSH> connection = createFRunSSH();
        connection->init(command.str(),NULL,NULL,NULL,m_SSHConnectTimeoutSeconds,0);
        // executed as single connection
        connection->exec(IpAddress(sAddress),NULL,false);
        response.append(connection->getReplyText()[0]);
        exitCode = connection->getReply()[0];
        int len = response.length();
        if (len > 0 && response.charAt(--len) == '\n') // strip newline
          response.setLength(len);
        if (response.length() && !exitCode)
          response.insert(0, "Response: ");
        else if (!exitCode)
          response.insert(0, "No response recieved.\n");

    }
    // CFRunSSH uses a MakeStringExceptionDirect throw to pass code and result string
    catch(IException* e)
    {
        exitCode = e->errorCode();
        // errorCode == -1 on successful CFRunSSH execution
        if(exitCode == -1)
            exitCode = 0;
        StringBuffer buf;
        e->errorMessage(buf);
        response.append(buf.str());
        int len = response.length();
        if (len > 0 && response.charAt(--len) == '\n') // strip newline
            response.setLength(len);
        // on successful connection
        if (response.length() && !exitCode)
            response.insert(0,"Response: ");
        else if (!exitCode)
            response.insert(0, "No response recieved.\n");
        e->Release();
    }
#ifndef NO_CATCHALL
    catch(...)
    {
        response.append("An unknown exception occurred!");
        exitCode = -1;
    }
#endif

    return exitCode;
}

int Cws_machineEx::invokeProgram(const char *command_line, StringBuffer& response)
{
    char   buffer[128];
    FILE   *fp;

    // Run the command so that it writes its output to a pipe. Open this
    // pipe with read text attribute so that we can read it
    // like a text file.
    if (getEspLogLevel()>LogNormal)
    {
        DBGLOG("command_line=<%s>", command_line);
    }
#ifndef NO_CONNECTION_DEBUG
    if( (fp = popen( command_line, "r" )) == NULL )
        return -1;
#else
    if( (fp = fopen( "c:\\temp\\preflight_result.txt", "r" )) == NULL )
        return -1;
#endif

    // Read pipe until end of file. End of file indicates that
    //the stream closed its standard out (probably meaning it
    //terminated).
    while ( !feof(fp) )
        if ( fgets( buffer, 128, fp) )
            response.append( buffer );

    if (getEspLogLevel()>LogNormal)
    {
        DBGLOG("response=<%s>", response.str());
    }
    // Close pipe and print return value of CHKDSK.
#ifndef NO_CONNECTION_DEBUG
    return pclose( fp );
#else
    return fclose( fp );
#endif
}

void Cws_machineEx::readPreflightResponse(IEspContext& context, CMachineInfoThreadParam* pParam, const char* response, int error)
{
    if (!response || !*response)
        return;

    StringBuffer computerUpTime;
    readALineFromResult(response, "ComputerUpTime:", computerUpTime, true);
    if (computerUpTime.length() < 1)
        computerUpTime.append("-");
    else
    {
        const char* pStr = strchr(computerUpTime.str(), ' ');
        if (pStr)
        {
            pStr++;
            pStr = strchr(pStr, ' ');
            if (pStr)
            {
                pStr++;
                if (pStr)
                    pParam->m_machineData.setComputerUpTime(pStr);
            }
        }

        if (!pStr)
            pParam->m_machineData.setComputerUpTime(computerUpTime);
    }

    if (pParam->m_options.getGetProcessorInfo())
    {
        StringBuffer CPUIdle;
        readALineFromResult(response, "CPU-Idle:", CPUIdle, true);
        if (CPUIdle.length() < 1)
            pParam->m_machineData.setCPULoad(0);
        else
        {
            if (CPUIdle.charAt(CPUIdle.length() - 1)  == '%')
                CPUIdle.setLength(CPUIdle.length() - 1);

            pParam->m_machineData.setCPULoad(100-atoi(CPUIdle.str()));
        }
    }

    if (pParam->m_options.getGetStorageInfo())
        readStorageData(response, pParam);

    if (pParam->m_options.getGetSoftwareInfo())
        readProcessData(response, pParam);
}

void Cws_machineEx::readALineFromResult(const char *result, const char *start, StringBuffer& value, bool trim)
{
    if (!result || !*result)
        return;

    const char* pStr = strstr(result, start);
    if (!pStr)
        return;

    pStr += strlen(start);
    if (!pStr)
        return;

    const char* pStr1 = strchr(pStr, 0x0a);
    if (pStr1)
        value.append(pStr, 0, pStr1 - pStr);
    else
        value.append(pStr);

    if (trim)
        value.trim();
}

void Cws_machineEx::readStorageData(const char* response, CMachineInfoThreadParam* pParam)
{
    if (!response || !*response)
        return;

    const char* pStr = strstr(response, "---SpaceUsedAndFree---");
    if (!pStr)
        DBGLOG("Storage information not found on %s", pParam->m_machineData.getNetworkAddress());

    bool isTitleLine = true;
    CIArrayOf<CStorageData>& storage = pParam->m_machineData.getStorage();
    while (pStr)
    {
        StringBuffer buf;
        const char* pStr1 = strchr(pStr, 0x0a);
        if (pStr1)
        {
            buf.append(pStr, 0, pStr1 - pStr);
            pStr = pStr1+1;
        }
        else
        {
            buf.append(pStr);
            pStr = NULL;
        }

        if (isTitleLine)
        {
            isTitleLine = false;
            continue;
        }

        if (buf.length() > 0)
        {
            StringBuffer diskSpaceTitle;
            int diskSpacePercentAvail = 0;
            __int64 diskSpaceAvailable = 0, diskSpaceTotal = 0;
            if (!readStorageSpace(buf.str(), diskSpaceTitle, diskSpaceAvailable, diskSpaceTotal, diskSpacePercentAvail))
                DBGLOG("Invalid storage information on %s: %s", pParam->m_machineData.getNetworkAddress(), buf.str());
            else if ((diskSpaceTitle.length() > 0) && !excludePartition(diskSpaceTitle.str()))
            {
                Owned<CStorageData> diskData = new CStorageData(diskSpaceTitle, diskSpaceAvailable, diskSpaceTotal, diskSpacePercentAvail);
                storage.append(*diskData.getClear());
            }
        }
        if (!pStr || (strnicmp(pStr, "---ProcessList1---", 18)==0))
            break;
    }
}

bool Cws_machineEx::readStorageSpace(const char *line, StringBuffer& title, __int64& free, __int64& total, int& percentAvail)
{
    if (!line || !*line)
        return false;

    StringBuffer freeStr, usedStr;

    const char* pStr = line;
    const char* pStr1 = strchr(pStr, ':');
    if (!pStr1)
        return false;

    title.clear().append(pStr, 0, pStr1 - pStr);
    pStr = pStr1 + 2;
    pStr1 = (char*) strchr(pStr, ' ');
    if (!pStr1)
        return false;

    usedStr.append(pStr, 0, pStr1 - pStr);
    pStr = pStr1 + 1;
    if (!pStr)
        return false;

    freeStr.append(pStr);

    __int64 factor1 = 1;
    if (freeStr.length() > 9)
    {
        freeStr.setLength(freeStr.length()-6);
        factor1 = 1000000;
    }
    free = atol(freeStr.str())*factor1;

    __int64 factor2 = 1;
    if (usedStr.length() > 9)
    {
        usedStr.setLength(usedStr.length()-6);
        factor2 = 1000000;
    }
    __int64 used = atol(usedStr.str())*factor2;

    total = free + used;
    if (total > 0)
        percentAvail = (int) ((free*100)/total);

    free = (__int64) free /1000; //MByte
    total = (__int64) total /1000; //MByte
    return true;
}

void Cws_machineEx::buildProcessPath(StringBuffer &processPath, const char * processName, CMachineInfoThreadParam * pParam)
{
    if (environmentConfData.m_pidPath.charAt(environmentConfData.m_pidPath.length() - 1) != pParam->m_machineData.getPathSep())
        processPath.setf("%s%c%s:", environmentConfData.m_pidPath.str(), pParam->m_machineData.getPathSep(), processName);
    else
        processPath.setf("%s%s:", environmentConfData.m_pidPath.str(), processName);
}

void Cws_machineEx::readProcessData(const char* response, CMachineInfoThreadParam* pParam)
{
    if (!response || !*response)
        return;

    CIArrayOf<CProcessData>& processes = pParam->m_machineData.getProcesses();
    ForEachItemIn(idx, processes)
    {
        CProcessData& process = processes.item(idx);
        if (!process.getName() || !*process.getName())
            continue;

        StringBuffer procName, catError, processPath, processData;
        if (streq(process.getType(), eqThorSlaveProcess))
        {
            procName.appendf("thorslave_%s_%d", process.getName(), process.getProcessNumber());
            buildProcessPath(processPath,procName.str(),pParam);
            catError.setf("cat: %s",processPath.str());
            catError.insert(catError.length()-1,".pid");
            if (!strstr(response,catError.str()))
                readALineFromResult(response, processPath.str(), processData, true);
            else
            {
                procName.setf("%s_slave_%d", process.getName(), process.getProcessNumber());
                buildProcessPath(processPath,procName.str(),pParam);
                readALineFromResult(response, processPath.str(), processData, true);
            }
        }
        else if (streq(process.getType(), eqThorMasterProcess))
        {
            procName.appendf("%s", process.getName());
            buildProcessPath(processPath,procName.str(),pParam);
            catError.setf("cat: %s",processPath.str());
            catError.insert(catError.length()-1,".pid");
            if (!strstr(response,catError.str()))
                readALineFromResult(response, processPath.str(), processData, true);
            else
            {
                procName.setf("%s_master", process.getName());
                buildProcessPath(processPath,procName.str(),pParam);
                readALineFromResult(response, processPath.str(), processData, true);
            }
        }
        else
        {
            procName.append(process.getName());
            buildProcessPath(processPath,procName.str(),pParam);
            readALineFromResult(response, processPath.str(), processData, true);
        }

        if (processData.length() < 1)
        {
            DBGLOG("Information for process %s not found", processPath.str());
            continue;
        }

        const char* pStr = strchr(processData.str(), ' ');
        if (!pStr)
        {
            DBGLOG("incorrect data for process %s: %s", processPath.str(), processData.str());
            continue;
        }

        unsigned len = pStr - processData.str();
        StringBuffer pid, upTime;
        pid.append(processData.str(), 0, len);
        len++;
        upTime.append(processData.str(), len, processData.length() - len);
        upTime.replaceString("-", " day(s) ");

        process.setPID(pid.str());
        process.setUpTime(upTime.str());
    }

    readRunningProcesses(response, pParam);
}

void Cws_machineEx::readRunningProcesses(const char* response, CMachineInfoThreadParam* pParam)
{
    if (!response || !*response)
        return;

    const char* pStr = strstr(response, "---ProcessList2---");
    if (!pStr)
        DBGLOG("Running process not found on %s", pParam->m_machineData.getNetworkAddress());

    IArrayOf<IEspProcessInfo>& runningProcesses = pParam->m_machineData.getRunningProcesses();
    while (pStr)
    {
        //read a line
        StringBuffer lineStr;
        const char* pStr1 = strchr(pStr, 0x0a);
        if (!pStr1)
        {
            lineStr.append(pStr);
            pStr =  NULL;
        }
        else
        {
            lineStr.append(pStr, 0, pStr1 - pStr);
            pStr = pStr1+1;
        }

        if (lineStr.length() < 1)
            continue;

        StringBuffer pidStr, desc, param;
        pStr1 = lineStr.str();
        const char* pStr2 = strchr(pStr1, ' ');
        if (!pStr2)
            continue;

        pidStr.append(pStr1, 0, pStr2 - pStr1);
        param.append(pStr2+1);
        if (param.length() < 1)
            continue;

        if (streq(param.str(), "ps"))
            continue;

        bool isNumber = true;
        for (unsigned i = 0; i < pidStr.length(); i++)
        {
            if (!isdigit(pidStr.charAt(i)))
            {
                isNumber = false;
                break;
            }
        }
        if (!isNumber)
            continue;

        int pid = atoi(pidStr.str());

        desc = param;
        if ((desc.charAt(0) == '.') && (param.charAt(1) == '/'))
            desc.remove(0, 2);
        if (desc.charAt(desc.length() - 1) == '/')
            desc.remove(desc.length() - 1, 1);
        if (desc.charAt(0) == '[')
        {
            desc.remove(0, 1);
            if (desc.charAt(desc.length() - 1) == ']')
                desc.remove(desc.length() - 1, 1);
        }

        Owned<IEspProcessInfo> info = createProcessInfo("","");
        info->setPID(pid);
        info->setParameter(param.str());
        info->setDescription(desc.str());
        runningProcesses.append(*info.getClear());
    }
}

void Cws_machineEx::setMachineInfo(IEspContext& context, CMachineInfoThreadParam* pParam, const char* response, int error)
{
    //Read additionalProcessFilters which will be used in setProcessInfo()/setProcessComponent()
    set<string>& additionalProcesses = pParam->m_machineData.getAdditinalProcessFilters();
    StringArray& additionalProcessFilters = pParam->m_options.getAdditionalProcessFilters();
    if (pParam->m_options.getApplyProcessFilter() && !additionalProcessFilters.empty())
    {
        int len = additionalProcessFilters.length();
        for (int i=0; i<len; i++)
        {
            StringBuffer processName(additionalProcessFilters.item(i));
            processName.toLowerCase().replaceString(".exe", "");
            if (processName.length() > 0)
                additionalProcesses.insert(processName.str());
        }
    }

    CIArrayOf<CProcessData>& processes = pParam->m_machineData.getProcesses();
    ForEachItemIn(idx, processes)
    {
        CProcessData& process = processes.item(idx);

        Owned<IEspMachineInfoEx> pMachineInfo = static_cast<IEspMachineInfoEx*>(new CMachineInfoEx(""));
        setProcessInfo(context, pParam, response, error, process, idx<1, pMachineInfo);

        synchronized block(mutex_machine_info_table);
        pParam->m_machineInfoTable.append(*pMachineInfo.getLink());
    }
}

void Cws_machineEx::setProcessInfo(IEspContext& context, CMachineInfoThreadParam* pParam, const char* response,
                                   int error, CProcessData& process, bool firstProcess, IEspMachineInfoEx* pMachineInfo)
{
    double version = context.getClientVersion();
    bool isEclAgentProcess = process.getType() && strieq(process.getType(), eqEclAgent);
    pMachineInfo->setAddress(pParam->m_machineData.getNetworkAddress());
    pMachineInfo->setConfigAddress(pParam->m_machineData.getNetworkAddressInEnvSetting());
    pMachineInfo->setOS(pParam->m_machineData.getOS());

    if (process.getName() && *process.getName())
        pMachineInfo->setComponentName(process.getName());
    if (process.getPath() && *process.getPath())
        pMachineInfo->setComponentPath(process.getPath());

    //set DisplayType
    if (process.getType() && *process.getType())
    {
        if (isEclAgentProcess)
        {
            pMachineInfo->setProcessType(eqAgentExec);
            pMachineInfo->setDisplayType("Agent Exec");
        }
        else
        {
            pMachineInfo->setProcessType(process.getType());
            StringBuffer displayName;
            getProcessDisplayName(process.getType(), displayName);
            pMachineInfo->setDisplayType(displayName.str());
        }
    }
    else if (process.getName() && *process.getName())
    {
        pMachineInfo->setDisplayType(process.getName());
    }

    if ((version > 1.09) && process.getType() && strieq(process.getType(), eqThorSlaveProcess))
    {
        pMachineInfo->setProcessNumber(process.getProcessNumber());
    }

    if ((version >= 1.16) && (strieq(process.getType(), eqThorSlaveProcess) || strieq(process.getType(), eqRoxieServerProcess)))
    {
        VStringBuffer key("%s|%s", process.getType(), process.getName());
        int* channels = pParam->getChannels(key);
        if (channels)
            pMachineInfo->setChannels(*channels);
    }

    if (error != 0 || !response || !*response)
    {
        StringBuffer description;
        if (!response || !*response)
            description.append("Failed in getting Machine Information");
        else
            description = response;
        pMachineInfo->setDescription(description.str());
    }
    else
    {
        //Now, add more columns based on 'response'
        pMachineInfo->setUpTime(pParam->m_machineData.getComputerUpTime());
        pParam->addColumn("Up Time");

        if (pParam->m_options.getGetStorageInfo())
        {
            IArrayOf<IEspStorageInfo> storageArray;
            CIArrayOf<CStorageData>& storage = pParam->m_machineData.getStorage();
            ForEachItemIn(idx, storage)
            {
                CStorageData& diskData = storage.item(idx);

                Owned<IEspStorageInfo> info = static_cast<IEspStorageInfo*>(new CStorageInfo(""));
                info->setDescription(diskData.getDiskSpaceTitle());
                info->setTotal(diskData.getDiskSpaceTotal());
                info->setAvailable(diskData.getDiskSpaceAvailable());
                info->setPercentAvail(diskData.getDiskSpacePercentAvail());
                storageArray.append(*info.getLink());

                pParam->addColumn(diskData.getDiskSpaceTitle());
            }

            pMachineInfo->setStorage(storageArray);
            storageArray.kill();
        }

        if (pParam->m_options.getGetProcessorInfo())
        {
            IArrayOf<IEspProcessorInfo> processorArray;
            Owned<IEspProcessorInfo> info = static_cast<IEspProcessorInfo*>(new CProcessorInfo(""));
            info->setLoad(pParam->m_machineData.getCPULoad());
            processorArray.append(*info.getLink());

            pMachineInfo->setProcessors(processorArray);
            processorArray.kill();

            pParam->addColumn("CPU Load");
        }

        if (pParam->m_options.getGetSoftwareInfo())
        {
            IArrayOf<IEspSWRunInfo> processArray;
            IEspComponentInfo* pComponentInfo = &pMachineInfo->updateComponentInfo();
            setProcessComponent(context, pParam, process, firstProcess, processArray, pComponentInfo);

            if (processArray.ordinality())
            {
                //Set running processes if ApplyProcessFilter is set to false
                //Set processes not running if ApplyProcessFilter is set to true
                pMachineInfo->setRunning(processArray);
            }

            pParam->addColumn("Processes");
            pParam->addColumn("Condition");
            pParam->addColumn("State");
            pParam->addColumn("UpTime");
        }
    }
}

void Cws_machineEx::setProcessComponent(IEspContext& context, CMachineInfoThreadParam* pParam, CProcessData& process,
     bool firstProcess, IArrayOf<IEspSWRunInfo>& processArray, IEspComponentInfo* pComponentInfo)
{
    const char* procType = process.getType();
    const char* procPID = process.getPID();
    //If a component (ex. dropzone) has no process type, it is not a process and does not have a PID.
    //FTSlaveProcess may not have a PID since it is launched dynamically during a spray.
    if (pParam->m_options.getApplyProcessFilter() && (isEmptyString(procPID) &&
        !isEmptyString(procType) && !strieq(procType, "FTSlaveProcess")))
    {
        Owned<IEspSWRunInfo> info = static_cast<IEspSWRunInfo*>(new CSWRunInfo(""));
        info->setName(process.getName());
        info->setInstances(0);
        processArray.append( *info.getLink() );
    }

    set<string>& additionalProcesses = pParam->m_machineData.getAdditinalProcessFilters();

    map<string, Linked<IEspSWRunInfo> > runningProcessMap; //save only one description of each process
    set<string>& dependencies = process.getDependencies();
    IArrayOf<IEspProcessInfo>& runningProcesses = pParam->m_machineData.getRunningProcesses();
    if (runningProcesses.length() > 0)
    {
        if (!pParam->m_options.getApplyProcessFilter()) //need to display all of the running processes
            enumerateRunningProcesses( pParam, process, &runningProcessMap, firstProcess);
        else if (!dependencies.empty() || !additionalProcesses.empty())
            enumerateRunningProcesses(pParam, process, NULL, firstProcess);
    }

    map<string, Linked<IEspSWRunInfo> >::const_iterator it = runningProcessMap.begin();
    map<string, Linked<IEspSWRunInfo> >::const_iterator iEnd = runningProcessMap.end();
    for (; it != iEnd; it++) //add in sorted order simply by traversing the map
    {
        Linked<IEspSWRunInfo> info( (*it).second );
        processArray.append( *info.getLink() );
    }

    bool dependencyDown = false;
    if (!dependencies.empty())
    {
        dependencyDown = true;
        if (pParam->m_options.getApplyProcessFilter())
        {
            set<string>::const_iterator it   = dependencies.begin();
            set<string>::const_iterator iEnd = dependencies.end();
            for (; it != iEnd; it++)
            {
                Owned<IEspSWRunInfo> info = static_cast<IEspSWRunInfo*>(new CSWRunInfo(""));
                info->setName(it->c_str());
                info->setInstances(0);
                processArray.append( *info.getLink() );
            }
        }
    }

    if (pParam->m_options.getApplyProcessFilter() && !additionalProcesses.empty())
    {
        set<string>::const_iterator it   = additionalProcesses.begin();
        set<string>::const_iterator iEnd = additionalProcesses.end();
        for (; it != iEnd; it++)
        {
            Owned<IEspSWRunInfo> info = static_cast<IEspSWRunInfo*>(new CSWRunInfo(""));
            info->setName(it->c_str());
            info->setInstances(0);
            processArray.append( *info.getLink() );
        }
    }

    if (!dependencyDown && (!isEmptyString(procPID) || isEmptyString(procType) || strieq(procType, "FTSlaveProcess")))
    {
        //conditions: unknown, normal, warning, minor, major, critical, fatal
        pComponentInfo->setCondition( 1 );
        pComponentInfo->setState(5);
        if (process.getUpTime() && *process.getUpTime())
            pComponentInfo->setUpTime( process.getUpTime() );
    }
    else
    {
        pComponentInfo->setCondition(2); //Warnning
        pComponentInfo->setState(0);
    }
}

//Erase this process from dependencies and, if firstProcess, additionalProcesses;
//If processMap is not NULL, add this process to processMap
void Cws_machineEx::enumerateRunningProcesses(CMachineInfoThreadParam* pParam, CProcessData& process, map<string, Linked<IEspSWRunInfo> >* runningProcessMap, bool firstProcess)
{
    set<string>& dependencies = process.getDependencies();
    set<string>& additionalProcesses = pParam->m_machineData.getAdditinalProcessFilters();
    IArrayOf<IEspProcessInfo>& runningProcesses = pParam->m_machineData.getRunningProcesses();
    ForEachItemIn(k, runningProcesses)
    {
        IEspProcessInfo& processInfo = runningProcesses.item(k);

        //Erase this process from dependencies and, if firstProcess, additionalProcesses
        const char* pName = processInfo.getDescription();
        if (pParam->m_machineData.getOS() == MachineOsW2K)
        {
            StringBuffer sName(pName);
            pName = sName.toLowerCase().replaceString(".exe", "").str();
            if (!dependencies.empty())
                dependencies.erase(pName);
            if (pParam->m_options.getApplyProcessFilter() && firstProcess && !additionalProcesses.empty())
                additionalProcesses.erase(pName);
        }
        else
        {
            //dafilesrv would probably be running from a global directory
            //and not component's installation directory so ignore their paths
            const char* pPath = pName;
            if ( !strieq(pName, "dafilesrv"))
            {
                const char* param = processInfo.getParameter();
                if (param && *param)
                {
                    if (strncmp(param, "bash ", 5))
                        pPath = param;
                    else
                        pPath = param + 5;

                    if (!pPath || !*pPath)
                        continue;

                    //params typically is like "/c$/esp_dir/esp [parameters...]"
                    //so just pick the full path
                    const char* pch = strchr(pPath, ' ');
                    if (pch)
                    {
                        StringBuffer sPath(pPath);
                        sPath.setLength( pch - pPath );
                        pPath = sPath.str();
                    }
                }
            }

            if (!dependencies.empty())
            {
                const char* pProcessName;
                if (process.getType() && !strncmp(process.getType(), "Thor", 4) && !strnicmp(pName, "thor", 4))
                {
                    const char* pch = strrchr(pPath, pParam->m_machineData.getPathSep());
                    pProcessName = pch ? pch+1 : pName;
                }
                else
                {
                    const char* pName0 = process.getMultipleInstances() ? pPath : pName;
                    const char* pch = strrchr(pName0, pParam->m_machineData.getPathSep());
                    pProcessName = pch ? pch+1 : pName0;
                }
                dependencies.erase(pProcessName);
                if (pParam->m_options.getApplyProcessFilter() && firstProcess && !additionalProcesses.empty())
                    additionalProcesses.erase(pProcessName);
            }
            pName = pPath;
        }

        if (!runningProcessMap)
            continue;

        //Add this process to runningProcessMap
        map<string, Linked<IEspSWRunInfo> >::iterator it = runningProcessMap->find(pName);
        if ( it != runningProcessMap->end()) //not in the set
        {
            Linked<IEspSWRunInfo>& linkedPtr = (*it).second;
            linkedPtr->setInstances( linkedPtr->getInstances() + 1);
        }
        else
        {
            Owned<IEspSWRunInfo> info = static_cast<IEspSWRunInfo*>(new CSWRunInfo(""));
            info->setName(pName);
            info->setInstances(1);
            runningProcessMap->insert(pair<string, Linked<IEspSWRunInfo> >(pName, info));
        }
    }
}

void Cws_machineEx::getProcessDisplayName(const char* processName, StringBuffer& displayName)
{
    //produces "LDAPServerProcess" as "LDAP Server" and "EspService" as "Esp Service", etc.
    const char* end = strstr(processName, "Process");
    if (!end)
        end = processName + strlen(processName);

    displayName.append(*processName);
    processName++;

    bool bLower = false;
    while (processName < end)
    {
        char ch = *processName;
        if (!isupper(ch))
            bLower = true;
        else
        {
            if (bLower || //last char was uppercase or the following character is lowercase?
            ((processName+1 < end) && islower(*(processName+1))))
            {
                displayName.append(' ');
            }

            bLower = false;
        }

        displayName.append(*processName);
        processName++;
    }
    displayName.append('\0');
    return;
}

bool Cws_machineEx::excludePartition(const char* partition) const
{
    //first see if this partition is meant to be excluded as is - for instance
    //if partition is /dev and /dev is one of the predefined partitions to be excluded
    set<string>::const_iterator it = m_excludePartitions.find( partition );
    set<string>::const_iterator itEnd = m_excludePartitions.end();
    bool found = false;

    if (it != itEnd)
        found = true;
    else
    {
        //now check if /dev* is one of the partitions to be excluded
        set<string>::const_iterator itBegin = m_excludePartitionPatterns.begin();
        itEnd = m_excludePartitionPatterns.end();
        unsigned int partitionLen = strlen(partition);

        for (it=itBegin; it != itEnd; it++)
        {
            const string& pattern = *it;
            if ((found = ::WildMatch(partition, partitionLen, pattern.c_str(), pattern.length(), false)))
                break;
        }
    }
    return found;
}

void Cws_machineEx::appendProcessInstance(const char* name, const char* directory1, const char* directory2, StringArray& processInstances, StringArray& directories)
{
    if (!name || !*name)
        return;

    processInstances.append(name);

    if (directory1 && *directory1)
        directories.append(directory1);
    else if (directory2 && *directory2)
        directories.append(directory2);
    else
        directories.append("Setting not found");
}

//////////////////////////////////////////////////////////////////
//  Set Machine Infomation for response                         //
//////////////////////////////////////////////////////////////////

void Cws_machineEx::setMachineInfoResponse(IEspContext& context, IEspGetMachineInfoRequest& req,
                                   CGetMachineInfoData& machineInfoData, IEspGetMachineInfoResponse& resp)
{
    IEspRequestInfoStruct& reqInfo = resp.updateRequestInfo();
#if 0
    StringBuffer user;
    StringBuffer pw;
    context.getUserID(user);
    context.getPassword(pw);
    reqInfo.setUserName(user.str());
    reqInfo.setPassword(pw.str());
#endif
    reqInfo.setSecurityString(req.getSecurityString());
    reqInfo.setGetProcessorInfo(req.getGetProcessorInfo());
    reqInfo.setGetStorageInfo(req.getGetStorageInfo());
    double version = context.getClientVersion();
    if (version > 1.10)
        reqInfo.setLocalFileSystemsOnly(req.getLocalFileSystemsOnly());
    reqInfo.setGetSoftwareInfo(req.getGetSoftwareInfo());
    reqInfo.setAutoRefresh( req.getAutoRefresh() );
    reqInfo.setMemThreshold(req.getMemThreshold());
    reqInfo.setDiskThreshold(req.getDiskThreshold());
    reqInfo.setCpuThreshold(req.getCpuThreshold());
    reqInfo.setMemThresholdType(req.getMemThresholdType());
    reqInfo.setDiskThresholdType(req.getDiskThresholdType());
    reqInfo.setApplyProcessFilter( req.getApplyProcessFilter() );
    reqInfo.setClusterType( req.getClusterType() );
    reqInfo.setCluster( req.getCluster() );
    reqInfo.setAddProcessesToFilter( req.getAddProcessesToFilter() );
    reqInfo.setOldIP( req.getOldIP() );
    reqInfo.setPath( req.getPath() );
    reqInfo.setSortBy("Address");

    if (machineInfoData.getMachineInfoColumns().ordinality())
        resp.setColumns(machineInfoData.getMachineInfoColumns());
    if (machineInfoData.getMachineInfoTable().ordinality())
        resp.setMachines(machineInfoData.getMachineInfoTable());

    char timeStamp[32];
    getTimeStamp(timeStamp);
    resp.setTimeStamp( timeStamp );
    if (version >= 1.12)
    {
        StringBuffer acceptLanguage;
        resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
    }
}


void Cws_machineEx::getTimeStamp(char* timeStamp)
{
    //set time stamp in the result for this machine
    time_t tNow;
    time(&tNow);

#ifdef _WIN32
    struct tm *ltNow;
    ltNow = localtime(&tNow);
    strftime(timeStamp, 32, "%m/%d/%y %H:%M:%S", ltNow);
#else
    struct tm ltNow;
    localtime_r(&tNow, &ltNow);
    strftime(timeStamp, 32, "%m/%d/%y %H:%M:%S", &ltNow);
#endif
}

void Cws_machineEx::setTargetClusterInfoResponse(IEspContext& context, IEspGetTargetClusterInfoRequest& req,
                                   CGetMachineInfoData& machineInfoData, IPropertyTree* targetClusterTree, IEspGetTargetClusterInfoResponse& resp)
{
    IEspRequestInfoStruct& reqInfo = resp.updateRequestInfo();
#if 0
    StringBuffer user;
    StringBuffer pw;
    context.getUserID(user);
    context.getPassword(pw);
    reqInfo.setUserName(user.str());
    reqInfo.setPassword(pw.str());
#endif
    reqInfo.setGetProcessorInfo(req.getGetProcessorInfo());
    reqInfo.setGetStorageInfo(req.getGetStorageInfo());
    double version = context.getClientVersion();
    if (version > 1.10)
        reqInfo.setLocalFileSystemsOnly(req.getLocalFileSystemsOnly());
    reqInfo.setGetSoftwareInfo(req.getGetSoftwareInfo());
    reqInfo.setAutoRefresh( req.getAutoRefresh() );
    reqInfo.setMemThreshold(req.getMemThreshold());
    reqInfo.setDiskThreshold(req.getDiskThreshold());
    reqInfo.setCpuThreshold(req.getCpuThreshold());
    reqInfo.setMemThresholdType(req.getMemThresholdType());
    reqInfo.setDiskThresholdType(req.getDiskThresholdType());
    reqInfo.setApplyProcessFilter( req.getApplyProcessFilter() );
    reqInfo.setAddProcessesToFilter( req.getAddProcessesToFilter() );
    reqInfo.setSortBy("Address");

    if (machineInfoData.getMachineInfoColumns().ordinality())
        resp.setColumns(machineInfoData.getMachineInfoColumns());

    if (machineInfoData.getMachineInfoTable().ordinality())
    {
        IArrayOf<IEspTargetClusterInfo> targetClusterInfoList;
        setTargetClusterInfo(targetClusterTree, machineInfoData.getMachineInfoTable(), targetClusterInfoList);
        if (targetClusterInfoList.ordinality())
            resp.setTargetClusterInfoList(targetClusterInfoList);
    }

    char timeStamp[32];
    getTimeStamp(timeStamp);
    resp.setTimeStamp( timeStamp );
    if (version >= 1.12)
    {
        StringBuffer acceptLanguage;
        resp.setAcceptLanguage(getAcceptLanguage(context, acceptLanguage).str());
    }
}

void Cws_machineEx::setTargetClusterInfo(IPropertyTree* pTargetClusterTree, IArrayOf<IEspMachineInfoEx>& machineArray, IArrayOf<IEspTargetClusterInfo>& targetClusterInfoList)
{
    if (!pTargetClusterTree)
        return;

    unsigned machineCount = machineArray.ordinality();
    if (machineCount < 1)
        return;

    Owned<IPropertyTreeIterator> targetClusters = pTargetClusterTree->getElements("TargetCluster");
    ForEach(*targetClusters)
    {
        IPropertyTree& targetCluster = targetClusters->query();

        StringBuffer targetName, targetType;
        targetCluster.getProp("@Name", targetName);
        targetCluster.getProp("@Type", targetType);

        Owned<IEspTargetClusterInfo> targetClusterInfo = static_cast<IEspTargetClusterInfo*>(new CTargetClusterInfo(""));
        targetClusterInfo->setName( targetName.str() );
        targetClusterInfo->setType( targetType.str() );

        IArrayOf<IEspMachineInfoEx> machineArrayNew;
        Owned<IPropertyTreeIterator> processes = targetCluster.getElements("Process");
        ForEach(*processes)
        {
            IPropertyTree& process = processes->query();

            StringBuffer processName, processType;
            process.getProp("@Name", processName);
            process.getProp("@Type", processType);

            for (unsigned i = 0; i < machineCount; i++)
            {
                IEspMachineInfoEx& machineInfoEx = machineArray.item(i);
                const char* name = machineInfoEx.getComponentName();
                const char* type = machineInfoEx.getProcessType();
                if (!name || !type || !strieq(name, processName.str()) || !strieq(getProcessTypeFromMachineType(type), processType.str()))
                    continue;

                Owned<IEspMachineInfoEx> pMachineInfo = static_cast<IEspMachineInfoEx*>(new CMachineInfoEx(""));
                pMachineInfo->copy(machineInfoEx);
                machineArrayNew.append(*pMachineInfo.getLink());
                //Cannot break here because more than one processes match (ex. EclAgent/AgentExec)
            }
        }

        if (machineArrayNew.ordinality())
            targetClusterInfo->setProcesses(machineArrayNew);

        targetClusterInfoList.append(*targetClusterInfo.getLink());
    }
}

const char* Cws_machineEx::getProcessTypeFromMachineType(const char* machineType)
{
    const char* processType ="Unknown";
    if (!machineType || !*machineType)
        return processType;

    if (strieq(machineType, eqThorMasterProcess) || strieq(machineType, eqThorSlaveProcess) || strieq(machineType, eqThorSpareProcess))
    {
        processType = eqThorCluster;
    }
    else    if (strieq(machineType, eqRoxieServerProcess))
    {
        processType = eqRoxieCluster;
    }
    else    if (strieq(machineType, eqAgentExec))
    {
        processType = eqEclAgent;
    }
    else
    {
        processType = machineType;
    }

    return processType;
}

IConstEnvironment* Cws_machineEx::getConstEnvironment()
{
    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
    if (!constEnv)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");
    return constEnv.getLink();
}

//Used in Rexec
IPropertyTree* Cws_machineEx::getComponent(const char* compType, const char* compName)
{
    StringBuffer xpath;
    xpath.append("Software/").append(compType).append("[@name='").append(compName).append("']");

    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IPropertyTree> pEnvRoot = &constEnv->getPTree();
    return pEnvRoot->getPropTree( xpath.str() );
}

void Cws_machineEx::getAccountAndPlatformInfo(const char* address, StringBuffer& userId, StringBuffer& password, bool& bLinux)
{
    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IConstMachineInfo> machine = constEnv->getMachineByAddress(address);
    if (!machine && strieq(address, "."))
    {
        machine.setown(constEnv->getMachineByAddress("127.0.0.1"));
    }

    if (!machine)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Machine %s is not defined in environment!", address);

    Owned<IConstDomainInfo> domain = machine->getDomain();
    if (!domain)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Machine %s does not have any domain information!", address);

    userId.clear();
    password.clear();
    StringBufferAdaptor strval1(userId);
    StringBufferAdaptor strval2(password);
    domain->getAccountInfo(strval1, strval2);

    StringBuffer domainName;
    StringBufferAdaptor strval3(domainName);
    domain->getName(strval3);

    if ((machine->getOS() == MachineOsW2K) && domainName.length())
    {
        domainName.append('\\');
        userId.insert(0, domainName);
    }

    bLinux = machine->getOS() == MachineOsLinux;
}

IPropertyTree* Cws_machineEx::createDiskUsageReq(IPropertyTree* envDirectories, const char* pathName,
    const char* componentType, const char* componentName)
{
    StringBuffer path;
    if (!getConfigurationDirectory(envDirectories, pathName, componentType, componentName, path))
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get %s Disk Path for component %s", pathName, componentName);

    Owned<IPropertyTree> diskReq = createPTree("Folder");
    diskReq->addProp("@name", pathName);
    diskReq->addProp("@path", path);
    return diskReq.getClear();
}

IPropertyTree* Cws_machineEx::createMachineUsageReq(IConstEnvironment* constEnv, const char* computer)
{
    Owned<IConstMachineInfo> machine = constEnv->getMachine(computer);
    if (!machine)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get machine %s", computer);

    Owned<IPropertyTree> machineReq = createPTree("Machine");
    machineReq->addProp("@name", computer);

    SCMStringBuffer netAddress;
    machine->getNetAddress(netAddress);
    machineReq->addProp("@netAddress", netAddress.str());
    machineReq->addPropInt("@OS", machine->getOS());
    return machineReq.getClear();
}

void Cws_machineEx::readThorUsageReq(const char* name, IConstEnvironment* constEnv, IPropertyTree* usageReq)
{
    if (isEmptyString(name))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty Thor name");

    Owned<IPropertyTree> componentReq = createPTree("Component");
    componentReq->addProp("@name", name);
    componentReq->addProp("@type", eqThorCluster);

    Owned<IPropertyTree> envRoot = &constEnv->getPTree();
    IPropertyTree* envDirectories = envRoot->queryPropTree("Software/Directories");
    Owned<IPropertyTree> logFolder = createDiskUsageReq(envDirectories, "log", "thor", name);
    Owned<IPropertyTree> dataFolder = createDiskUsageReq(envDirectories, "data", "thor", name);
    Owned<IPropertyTree> repFolder = createDiskUsageReq(envDirectories, "mirror", "thor", name);

    VStringBuffer xpath("Software/ThorCluster[@name='%s']/ThorSlaveProcess", name);
    Owned<IPropertyTreeIterator> slaveProcesses= envRoot->getElements(xpath);
    ForEach(*slaveProcesses)
    {
        IPropertyTree& slaveProcess = slaveProcesses->query();
        const char* computer = slaveProcess.queryProp("@computer");
        if (isEmptyString(computer))
            throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get @computer for %s", xpath.str());

        Owned<IPropertyTree> newMachineReq = createMachineUsageReq(constEnv, computer);

        //Not sure we need those folders here. Add them just in case.
        if (logFolder)
            newMachineReq->addPropTree(logFolder->queryName(), LINK(logFolder));
        if (dataFolder)
            newMachineReq->addPropTree(dataFolder->queryName(), LINK(dataFolder));
        if (repFolder)
            newMachineReq->addPropTree(repFolder->queryName(), LINK(repFolder));
        componentReq->addPropTree(newMachineReq->queryName(), LINK(newMachineReq));
    }

    //Read ThorMasterProcess in case it is on a different machine
    xpath.setf("Software/ThorCluster[@name='%s']/ThorMasterProcess/@computer", name);
    const char* computer = envRoot->queryProp(xpath);
    if (isEmptyString(computer))
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get %s", xpath.str());

    Owned<IPropertyTree> machineReq = createMachineUsageReq(constEnv, computer);
    xpath.setf("Machine[@netAddress='%s']", machineReq->queryProp("@netAddress"));
    if (componentReq->queryPropTree(xpath))
    {   //ThorMasterProcess is running on one of the ThorSlaveProcess machines.
        //So, we do not add this machine again.
        usageReq->addPropTree(componentReq->queryName(), LINK(componentReq));
        return;
    }

    //Not sure we need those folders here. Add them just in case.
    if (logFolder)
        machineReq->addPropTree(logFolder->queryName(), LINK(logFolder));
    if (dataFolder)
        machineReq->addPropTree(dataFolder->queryName(), LINK(dataFolder));
    if (repFolder)
        machineReq->addPropTree(repFolder->queryName(), LINK(repFolder));
    componentReq->addPropTree(machineReq->queryName(), LINK(machineReq));

    usageReq->addPropTree(componentReq->queryName(), LINK(componentReq));
}

void Cws_machineEx::readRoxieUsageReq(const char* name, IConstEnvironment* constEnv, IPropertyTree* usageReq)
{
    if (isEmptyString(name))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty Roxie name");

    Owned<IPropertyTree> componentReq = createPTree("Component");
    componentReq->addProp("@name", name);
    componentReq->addProp("@type", eqRoxieCluster);

    Owned<IPropertyTree> envRoot = &constEnv->getPTree();
    IPropertyTree* envDirectories = envRoot->queryPropTree("Software/Directories");
    Owned<IPropertyTree> logFolder = createDiskUsageReq(envDirectories, "log", "roxie", name);
    Owned<IPropertyTree> dataFolder = createDiskUsageReq(envDirectories, "data", "roxie", name);

    VStringBuffer xpath("Software/RoxieCluster[@name='%s']/RoxieServerProcess", name);
    Owned<IPropertyTreeIterator> slaveProcesses= envRoot->getElements(xpath);
    ForEach(*slaveProcesses)
    {
        IPropertyTree& slaveProcess = slaveProcesses->query();
        const char* computer = slaveProcess.queryProp("@computer");
        if (isEmptyString(computer))
            throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get @computer for %s", xpath.str());

        Owned<IPropertyTree> newMachineReq = createMachineUsageReq(constEnv, computer);
        //Not sure we need those folders here. Add them just in case.
        if (logFolder)
            newMachineReq->addPropTree(logFolder->queryName(), LINK(logFolder));
        if (dataFolder)
            newMachineReq->addPropTree(dataFolder->queryName(), LINK(dataFolder));
        componentReq->addPropTree(newMachineReq->queryName(), LINK(newMachineReq));
    }

    usageReq->addPropTree(componentReq->queryName(), LINK(componentReq));
}

void Cws_machineEx::readDropZoneUsageReq(const char* name, IConstEnvironment* constEnv, IPropertyTree* usageReq)
{
    if (isEmptyString(name))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty DropZone name");

    Owned<IConstDropZoneInfo> envDropZone = constEnv->getDropZone(name);
    if (!envDropZone || !envDropZone->isECLWatchVisible())
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Dropzone %s not found", name);

    SCMStringBuffer directory;
    envDropZone->getDirectory(directory);
    if (directory.length() == 0)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get directory for DropZone %s", name);

    Owned<IPropertyTree> dataFolder = createPTree("Folder");
    dataFolder->addProp("@name", "data");
    dataFolder->addProp("@path", directory.str());

    Owned<IPropertyTree> componentReq = createPTree("Component");
    componentReq->addProp("@name", name);
    componentReq->addProp("@type", eqDropZone);

    SCMStringBuffer computerName;
    envDropZone->getComputerName(computerName); 
    if (computerName.length() == 0)
    {
        OS_TYPE os = (getPathSepChar(directory.str()) == '/') ? OS_LINUX : OS_WINDOWS;

        Owned<IConstDropZoneServerInfoIterator> servers = envDropZone->getServers();
        ForEach(*servers)
        {
            IConstDropZoneServerInfo &server = servers->query();

            StringBuffer serverNetAddress;
            server.getServer(serverNetAddress.clear());

            Owned<IPropertyTree> machineReq = createPTree("Machine");
            machineReq->addProp("@name", serverNetAddress.str());
            machineReq->addProp("@netAddress", serverNetAddress.str());
            machineReq->addPropInt("@OS", os);
            machineReq->addPropTree(dataFolder->queryName(), LINK(dataFolder));
            componentReq->addPropTree(machineReq->queryName(), LINK(machineReq));
        }
    }
    else
    { //legacy dropzone settings
        Owned<IPropertyTree> machineReq = createMachineUsageReq(constEnv, computerName.str());
        machineReq->addPropTree(dataFolder->queryName(), LINK(dataFolder));
        componentReq->addPropTree(machineReq->queryName(), LINK(machineReq));
    }
    usageReq->addPropTree(componentReq->queryName(), LINK(componentReq));
}

void Cws_machineEx::readOtherComponentUsageReq(const char* name, const char* type, IConstEnvironment* constEnv, IPropertyTree* usageReq)
{
    if (isEmptyString(name))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty Component name");

    if (!strieq(type, eqDali) && !strieq(type, eqEclAgent) && !strieq(type, eqSashaServer))
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Component usage function is not supported for %s", type);

    Owned<IPropertyTree> envRoot = &constEnv->getPTree();
    VStringBuffer xpath("Software/%s[@name='%s']/Instance/@computer", type, name);
    const char* computer = envRoot->queryProp(xpath);
    if (isEmptyString(computer))
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get %s", xpath.str());

    Owned<IPropertyTree> machineReq = createMachineUsageReq(constEnv, computer);

    IPropertyTree* envDirectories = envRoot->queryPropTree("Software/Directories");
    Owned<IPropertyTree> logFolder = createDiskUsageReq(envDirectories, "log", type, name);
    if (logFolder)
        machineReq->addPropTree(logFolder->queryName(), LINK(logFolder));

    StringAttr componentType;
    if (strieq(type, eqDali))
        componentType.set("dali");
    else if (strieq(type, eqEclAgent))
        componentType.set("eclAgent");
    else
        componentType.set("sasha");

    Owned<IPropertyTree> dataFolder = createDiskUsageReq(envDirectories, "data", componentType.get(), name);
    if (dataFolder)
        machineReq->addPropTree(dataFolder->queryName(), LINK(dataFolder));

    if (strieq(type, eqDali))
    {
        Owned<IPropertyTree> repFolder = createDiskUsageReq(envDirectories, "mirror", "dali", name);
        if (repFolder)
            machineReq->addPropTree(repFolder->queryName(), LINK(repFolder));
    }

    Owned<IPropertyTree> componentReq = createPTree("Component");
    componentReq->addProp("@name", name);
    componentReq->addProp("@type", type);
    componentReq->addPropTree(machineReq->queryName(), LINK(machineReq));
    usageReq->addPropTree(componentReq->queryName(), LINK(componentReq));
}

void Cws_machineEx::setUniqueMachineUsageReq(IPropertyTree* usageReq, IPropertyTree* uniqueUsages)
{
    Owned<IPropertyTreeIterator> components= usageReq->getElements("Component");
    ForEach(*components)
    {
        IPropertyTree& component = components->query();
        Owned<IPropertyTreeIterator> machines= component.getElements("Machine");
        ForEach(*machines)
        {
            IPropertyTree& machine = machines->query();
            const char* netAddress = machine.queryProp("@netAddress");
    
            VStringBuffer xpath("Machine[@netAddress='%s']", netAddress);
            IPropertyTree* uniqueMachineReqTree = uniqueUsages->queryPropTree(xpath);
            if (!uniqueMachineReqTree)
            {
                uniqueUsages->addPropTree(machine.queryName(), LINK(&machine));
                continue;
            }

            //Add unique disk folders from the usageReq.
            Owned<IPropertyTreeIterator> folders = machine.getElements("Folder");
            ForEach(*folders)
            {
                IPropertyTree& folder = folders->query();
                const char* aDiskPath = folder.queryProp("@path");

                xpath.setf("Folder[@path='%s']", aDiskPath);
                IPropertyTree* uniqueFolderReqTree = uniqueMachineReqTree->queryPropTree(xpath);
                if (!uniqueFolderReqTree)
                {
                    Owned<IPropertyTree> folderReq = createPTree("Folder");
                    folderReq->addProp("@name", folder.queryProp("@name"));
                    folderReq->addProp("@path", aDiskPath);
                    uniqueMachineReqTree->addPropTree(folderReq->queryName(), LINK(folderReq));
                }
            }
        }
    }
}

IArrayOf<IConstComponent>& Cws_machineEx::listComponentsByType(IPropertyTree* envRoot,
    const char* componentType, IArrayOf<IConstComponent>& componentList)
{
    VStringBuffer xpath("Software/%s", componentType);
    Owned<IPropertyTreeIterator> components= envRoot->getElements(xpath);
    ForEach(*components)
    {
        Owned<IEspComponent> component = createComponent();
        component->setName(components->query().queryProp("@name"));
        component->setType(componentType);
        componentList.append(*component.getClear());
    }

    return componentList;
}

IArrayOf<IConstComponent>& Cws_machineEx::listComponentsForCheckingUsage(IConstEnvironment* constEnv,
    IArrayOf<IConstComponent>& componentList)
{
    Owned<IPropertyTree> envRoot = &constEnv->getPTree();
    listComponentsByType(envRoot, eqThorCluster, componentList);
    listComponentsByType(envRoot, eqRoxieCluster, componentList);
    listComponentsByType(envRoot, eqDali, componentList);
    listComponentsByType(envRoot, eqEclAgent, componentList);
    listComponentsByType(envRoot, eqSashaServer, componentList);
    listComponentsByType(envRoot, eqDropZone, componentList);

    return componentList;
}

void Cws_machineEx::readComponentUsageReq(IEspGetComponentUsageRequest& req, IConstEnvironment* constEnv, IPropertyTree* usageReq, IPropertyTree* uniqueUsages)
{
    IArrayOf<IConstComponent>& componentList = req.getComponents();
    if (!componentList.length())
        listComponentsForCheckingUsage(constEnv, componentList);

    ForEachItemIn(i, componentList)
    {
        IConstComponent& component = componentList.item(i);
        const char* type = component.getType();
        if (isEmptyString(type))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty Component Type");

        if (strieq(type, eqThorCluster))
            readThorUsageReq(component.getName(), constEnv, usageReq);
        else if (strieq(type, eqRoxieCluster))
            readRoxieUsageReq(component.getName(), constEnv, usageReq);
        else if (strieq(type, eqDropZone))
            readDropZoneUsageReq(component.getName(), constEnv, usageReq);
        else
            readOtherComponentUsageReq(component.getName(), type, constEnv, usageReq);
    }

    //Add unique machines from the usageReq to uniqueUsages.
    setUniqueMachineUsageReq(usageReq, uniqueUsages);
}

void Cws_machineEx::getMachineUsages(IEspContext& context, IPropertyTree* uniqueUsages)
{
    UnsignedArray threadHandles;
    Owned<IPropertyTreeIterator> requests= uniqueUsages->getElements("Machine");
    ForEach(*requests)
    {
        Owned<CGetMachineUsageThreadParam> threadParam = new CGetMachineUsageThreadParam(this, context, &requests->query());
        PooledThreadHandle handle = m_threadPool->start(threadParam.getClear());
        threadHandles.append(handle);
    }

    ForEachItemIn(i, threadHandles)
        m_threadPool->join(threadHandles.item(i));
}

bool Cws_machineEx::readDiskSpaceResponse(const char* response, __int64& free, __int64& used, int& percentAvail, StringBuffer& pathUsed)
{
    if (isEmptyString(response))
        return false;

    StringArray data;
    data.appendList(response, " ");
    if (data.length() < 2)
        return false;

    used = atol(data.item(0));
    free = atol(data.item(1));

    __int64 total = free + used;
    if (total > 0)
        percentAvail = (int) ((free*100)/total);

    //The given path (ex. /var/lib/HPCCSystems/hpcc-mirror/thor) in the usage request does not exist.
    //The data.item(2) is the path (ex. /var/lib/HPCCSystems/hpcc-mirror/) the usage script is used
    //to read the DiskSpace.
    if (data.length() > 2)
        pathUsed.set(data.item(2));

    return true;
}

void Cws_machineEx::getMachineUsage(IEspContext& context, CGetMachineUsageThreadParam* param)
{
    VStringBuffer command("/%s/sbin/usage -d=", environmentConfData.m_executionPath.str());

    unsigned pathCount = 0;
    Owned<IPropertyTreeIterator> diskPathList = param->request->getElements("Folder");
    ForEach(*diskPathList)
    {
        IPropertyTree& t = diskPathList->query();
        if (pathCount > 0)
            command.append(",");
        command.appendf("%s", t.queryProp("@path"));
        pathCount++;
    }
    ESPLOG(LogMax, "command(%s)", command.str());

    StringBuffer response;
    int error = runCommand(context, param->request->queryProp("@netAddress"), nullptr,
        (EnvMachineOS) param->request->getPropInt("@OS", MachineOsLinux), command.str(), nullptr, nullptr, response);
    if (error != 0 || isEmptyString(response))
    {
        if (isEmptyString(response))
            param->request->addProp("@error", "Failed in getting component usage.");
        else
            param->request->addProp("@error", response);
        return;
    }
    ESPLOG(LogMax, "response(%s)", response.str());

    ForEach(*diskPathList)
    {
        IPropertyTree& diskPathTree = diskPathList->query();

        StringBuffer aDiskPathResp, pathUsed;
        VStringBuffer diskPath("%s:", diskPathTree.queryProp("@path"));
        readALineFromResult(response, diskPath, aDiskPathResp, true);

        int percentAvail = 0;
        __int64 diskSpaceAvailable = 0, diskSpaceUsed = 0;
        if (!readDiskSpaceResponse(aDiskPathResp.str(), diskSpaceAvailable, diskSpaceUsed, percentAvail, pathUsed))
        {
            DBGLOG("Failed to read disc space on %s: %s", param->request->queryProp("@netAddress"), aDiskPathResp.str());
            diskPathTree.addProp("@error", "Failed to read disc space.");
            continue;
        }

        diskPathTree.addPropInt64("@used", diskSpaceUsed);
        diskPathTree.addPropInt64("@available", diskSpaceAvailable);
        diskPathTree.addPropInt("@percentAvail", percentAvail);
        if (!pathUsed.isEmpty())
            diskPathTree.addProp("@pathUsed", pathUsed);
    }
}

void Cws_machineEx::readComponentUsageResult(IEspContext& context, IPropertyTree* usageReq,
    IPropertyTree* uniqueUsages, IArrayOf<IEspComponentUsage>& componentUsages)
{
    Owned<IPropertyTreeIterator> components= usageReq->getElements("Component");
    ForEach(*components)
    {
        IPropertyTree& component = components->query();

        Owned<IEspComponentUsage> componentUsage = createComponentUsage();
        componentUsage->setName(component.queryProp("@name"));
        componentUsage->setType(component.queryProp("@type"));

        IArrayOf<IEspMachineUsage> machineUsages;
        Owned<IPropertyTreeIterator> machines= component.getElements("Machine");
        ForEach(*machines)
        {
            IPropertyTree& machine = machines->query();
            const char* netAddress = machine.queryProp("@netAddress");

            Owned<IEspMachineUsage> machineUsage = createMachineUsage();
            machineUsage->setName(machine.queryProp("@name"));
            machineUsage->setNetAddress(netAddress);

            VStringBuffer xpath("Machine[@netAddress='%s']", netAddress);
            IPropertyTree* uniqueMachineReqTree = uniqueUsages->queryPropTree(xpath);
            if (!uniqueMachineReqTree)
            {
                machineUsage->setDescription("No data returns.");
                machineUsages.append(*machineUsage.getClear());
                continue;
            }
            const char* error = uniqueMachineReqTree->queryProp("@error");
            if (!isEmptyString(error))
            {
                machineUsage->setDescription(error);
                machineUsages.append(*machineUsage.getClear());
                continue;
            }

            IArrayOf<IEspDiskUsage> diskUsages;
            Owned<IPropertyTreeIterator> folders = machine.getElements("Folder");
            ForEach(*folders)
            {
                IPropertyTree& folder = folders->query();
                const char* aDiskPath = folder.queryProp("@path");

                Owned<IEspDiskUsage> diskUsage = createDiskUsage();
                diskUsage->setName(folder.queryProp("@name"));
                diskUsage->setPath(aDiskPath);

                xpath.setf("Folder[@path='%s']", aDiskPath);
                IPropertyTree* folderTree = uniqueMachineReqTree->queryPropTree(xpath);
                if (!folderTree)
                    diskUsage->setDescription("No data returns.");
                else
                {
                    const char* error = folderTree->queryProp("@error");
                    if (!isEmptyString(error))
                        diskUsage->setDescription(error);
                    else
                    {
                        diskUsage->setAvailable(folderTree->getPropInt64("@available"));
                        diskUsage->setInUse(folderTree->getPropInt64("@used"));
                        diskUsage->setPercentAvailable(folderTree->getPropInt("@percentAvail"));
                        const char* pathUsed = folderTree->queryProp("@pathUsed");
                        if (!isEmptyString(pathUsed))
                        {
                            VStringBuffer desc("%s not found. Read disk usage from %s", aDiskPath, pathUsed);
                            diskUsage->setDescription(desc);
                        }
                    }
                }
                diskUsages.append(*diskUsage.getClear());
            }
            machineUsage->setDiskUsages(diskUsages);
            machineUsages.append(*machineUsage.getClear());
        }
        componentUsage->setMachineUsages(machineUsages);
        componentUsages.append(*componentUsage.getClear());
    }
}

bool Cws_machineEx::onGetComponentUsage(IEspContext& context, IEspGetComponentUsageRequest& req,
    IEspGetComponentUsageResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Machine Information. Permission denied.");

        StringBuffer cacheID;
        buildComponentUsageCacheID(cacheID, req.getComponents());

        if (!req.getBypassCachedResult() && readComponentUsageCache(context, cacheID, resp))
            return true;

        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();

        Owned<IPropertyTree> usageReq = createPTree("Req");
        Owned<IPropertyTree> uniqueUsages = createPTree("Usage");
        readComponentUsageReq(req, constEnv, usageReq, uniqueUsages);

        getMachineUsages(context, uniqueUsages);

        IArrayOf<IEspComponentUsage> componentUsages;
        readComponentUsageResult(context, usageReq, uniqueUsages, componentUsages);
        machineUsageCache->add(cacheID, componentUsages);
        resp.setComponentUsages(componentUsages);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

StringArray& Cws_machineEx::listTargetClusterNames(IConstEnvironment* constEnv, StringArray& targetClusters)
{
    Owned<IStringIterator> targets = getTargetClusters(nullptr, nullptr);
    ForEach(*targets)
    {
        SCMStringBuffer target;
        targetClusters.append(targets->str(target).str());
    }
    return targetClusters;
}

void Cws_machineEx::readTargetClusterUsageReq(IEspGetTargetClusterUsageRequest& req, IConstEnvironment* constEnv,
    IPropertyTree* usageReq, IPropertyTree* uniqueUsages)
{
    StringArray& targetClusters = req.getTargetClusters();
    if (targetClusters.empty())
        listTargetClusterNames(constEnv, targetClusters);

    Owned<IPropertyTree> envRoot = &constEnv->getPTree();
    ForEachItemIn(i, targetClusters)
    {
        const char* targetClusterName = targetClusters.item(i);
        if (isEmptyString(targetClusterName))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty Target Cluster specified.");

        Owned<IConstWUClusterInfo> targetClusterInfo = getTargetClusterInfo(targetClusterName);
        if (!targetClusterInfo)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Could not find information about target cluster %s ", targetClusterName);

        Owned<IPropertyTree> targetClusterTree = createPTree("TargetCluster");
        targetClusterTree->addProp("@name", targetClusterName);

        const StringArray& thors = targetClusterInfo->getThorProcesses();
        ForEachItemIn(i, thors)
        {
            const char* thor = thors.item(i);
            if (!isEmptyString(thor))
                readThorUsageReq(thor, constEnv, targetClusterTree);
        }

        SCMStringBuffer roxie;
        targetClusterInfo->getRoxieProcess(roxie);
        if (roxie.length())
            readRoxieUsageReq(roxie.str(), constEnv, targetClusterTree);

        SCMStringBuffer eclAgent;
        targetClusterInfo->getAgentName(eclAgent);
        if (eclAgent.length())
            readOtherComponentUsageReq(eclAgent.str(), eqEclAgent, constEnv, targetClusterTree);

        usageReq->addPropTree(targetClusterTree->queryName(), LINK(targetClusterTree));
    }

    Owned<IPropertyTreeIterator> targetClusterItr= usageReq->getElements("TargetCluster");
    ForEach(*targetClusterItr)
    {
        IPropertyTree& targetCluster = targetClusterItr->query();
        setUniqueMachineUsageReq(&targetCluster, uniqueUsages);
    }
}

void Cws_machineEx::readTargetClusterUsageResult(IEspContext& context, IPropertyTree* usageReq,
    IPropertyTree* uniqueUsages, IArrayOf<IEspTargetClusterUsage>& targetClusterUsages)
{
    Owned<IPropertyTreeIterator> targetClusters= usageReq->getElements("TargetCluster");
    ForEach(*targetClusters)
    {
        IPropertyTree& targetCluster = targetClusters->query();

        Owned<IEspTargetClusterUsage> targetClusterUsage = createTargetClusterUsage();
        targetClusterUsage->setName(targetCluster.queryProp("@name"));

        IArrayOf<IEspComponentUsage> componentUsages;
        readComponentUsageResult(context, &targetCluster, uniqueUsages, componentUsages);
        targetClusterUsage->setComponentUsages(componentUsages);

        targetClusterUsages.append(*targetClusterUsage.getClear());
    }
}

bool Cws_machineEx::onGetTargetClusterUsage(IEspContext& context, IEspGetTargetClusterUsageRequest& req,
    IEspGetTargetClusterUsageResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Machine Information. Permission denied.");

        StringBuffer cacheID;
        buildUsageCacheID(cacheID, req.getTargetClusters(), "AllTargetClusters");

        if (!req.getBypassCachedResult() && readTargetClusterUsageCache(context, cacheID, resp))
            return true;

        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();

        Owned<IPropertyTree> usageReq = createPTree("Req");
        Owned<IPropertyTree> uniqueUsages = createPTree("Usage");
        readTargetClusterUsageReq(req, constEnv, usageReq, uniqueUsages);

        getMachineUsages(context, uniqueUsages);

        IArrayOf<IEspTargetClusterUsage> targetClusterUsages;
        readTargetClusterUsageResult(context, usageReq, uniqueUsages, targetClusterUsages);
        machineUsageCache->add(cacheID, targetClusterUsages);
        resp.setTargetClusterUsages(targetClusterUsages);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_machineEx::getEclAgentNameFromNodeGroupName(const char* nodeGroupName, StringBuffer& agentName)
{
    //Node group name for an eclagent should be: 'hthor__' + ECLAgentName[ + '_' + a number]
    if ((strlen(nodeGroupName) <= 7) || strnicmp(nodeGroupName, "hthor__", 7))
        return false;

    agentName.set(nodeGroupName + 7);
    const char* ptr = strrchr(agentName.str(), '_');
    if (isEmptyString(ptr) || isEmptyString(++ptr))
        return true;

    const char* ptrSuffix = ptr - 1;
    do
    {
        if (!isdigit(ptr[0]))
            return true;
        ptr++;
    } while(!isEmptyString(ptr));

    agentName.setLength(agentName.length() - (ptr - ptrSuffix));
    return true;
}

void Cws_machineEx::getThorClusterNamesByGroupName(IPropertyTree* envRoot, const char* group, StringArray& thorClusterNames)
{
    Owned<IPropertyTreeIterator> thorClusters= envRoot->getElements("Software/ThorCluster");
    ForEach(*thorClusters)
    {
        IPropertyTree& thorCluster = thorClusters->query();
        const char *nodeGroupName = thorCluster.queryProp("@nodeGroup");
        if (!isEmptyString(nodeGroupName) && !strieq(nodeGroupName, group))
            continue;

        if (!isEmptyString(nodeGroupName))
            thorClusterNames.append(nodeGroupName);
        else
        {
            const char *name = thorCluster.queryProp("@name");
            if (strieq(name, group))
                thorClusterNames.append(name);
        }
    }
}

StringArray& Cws_machineEx::listThorHThorNodeGroups(IConstEnvironment* constEnv, StringArray& nodeGroups)
{
    BoolHash uniqueThorClusterGroupNames;
    Owned<IPropertyTree> envRoot = &constEnv->getPTree();
    Owned<IPropertyTreeIterator> it =envRoot->getElements("Software/ThorCluster");
    ForEach(*it)
    {
        IPropertyTree& cluster = it->query();

        StringBuffer thorClusterGroupName;
        getClusterGroupName(cluster, thorClusterGroupName);
        if (!thorClusterGroupName.length())
            continue;
        bool* found = uniqueThorClusterGroupNames.getValue(thorClusterGroupName);
        if (found && *found)
            continue;

        nodeGroups.append(thorClusterGroupName);
        uniqueThorClusterGroupNames.setValue(thorClusterGroupName, true);
    }

    it.setown(envRoot->getElements("Software/EclAgentProcess"));
    ForEach(*it)
    {
        IPropertyTree &cluster = it->query();
        const char* name = cluster.queryProp("@name");
        if (!name||!*name)
            continue;

        unsigned ins = 0;
        Owned<IPropertyTreeIterator> insts = cluster.getElements("Instance");
        ForEach(*insts) 
        {
            ins++;
            StringBuffer gname("hthor__");
            gname.append(name);
            if (ins>1)
                gname.append('_').append(ins);

            nodeGroups.append(gname);
        }
    }
    return nodeGroups;
}

void Cws_machineEx::readNodeGroupUsageReq(IEspGetNodeGroupUsageRequest& req, IConstEnvironment* constEnv,
    IPropertyTree* usageReq, IPropertyTree* uniqueUsages)
{
    StringArray& nodeGroups = req.getNodeGroups();
    if (nodeGroups.empty())
        listThorHThorNodeGroups(constEnv, nodeGroups);
    if (nodeGroups.empty())
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "No node group found.");

    Owned<IPropertyTree> envRoot = &constEnv->getPTree();
    ForEachItemIn(i, nodeGroups)
    {
        const char* nodeGroupName = nodeGroups.item(i);
        if (isEmptyString(nodeGroupName))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty node group specified.");

        Owned<IPropertyTree> nodeGroupTree = createPTree("NodeGroup");
        nodeGroupTree->addProp("@name", nodeGroupName);

        StringBuffer agentName;
        if (getEclAgentNameFromNodeGroupName(nodeGroupName, agentName))
            readOtherComponentUsageReq(agentName.str(), eqEclAgent, constEnv, nodeGroupTree);
        else
        {
            StringArray thorNames;
            getThorClusterNamesByGroupName(envRoot, nodeGroupName, thorNames);
            if (thorNames.length() == 0)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "No thor/hthor can be found for node group name %s.", nodeGroupName);

            ForEachItemIn(ii, thorNames)
                readThorUsageReq(thorNames.item(ii), constEnv, nodeGroupTree);
        }

        usageReq->addPropTree(nodeGroupTree->queryName(), LINK(nodeGroupTree));
    }

    Owned<IPropertyTreeIterator> nodeGroupItr= usageReq->getElements("NodeGroup");
    ForEach(*nodeGroupItr)
    {
        IPropertyTree& nodeGroup = nodeGroupItr->query();
        setUniqueMachineUsageReq(&nodeGroup, uniqueUsages);
    }
}

void Cws_machineEx::readNodeGroupUsageResult(IEspContext& context, IPropertyTree* usageReq,
    IPropertyTree* uniqueUsages, IArrayOf<IEspNodeGroupUsage>& nodeGroupUsages)
{
    Owned<IPropertyTreeIterator> nodeGroups= usageReq->getElements("NodeGroup");
    ForEach(*nodeGroups)
    {
        IPropertyTree& nodeGroup = nodeGroups->query();

        Owned<IEspNodeGroupUsage> nodeGroupUsage = createNodeGroupUsage();
        nodeGroupUsage->setName(nodeGroup.queryProp("@name"));

        IArrayOf<IEspComponentUsage> componentUsages;
        readComponentUsageResult(context, &nodeGroup, uniqueUsages, componentUsages);
        nodeGroupUsage->setComponentUsages(componentUsages);

        nodeGroupUsages.append(*nodeGroupUsage.getClear());
    }
}

bool Cws_machineEx::onGetNodeGroupUsage(IEspContext& context, IEspGetNodeGroupUsageRequest& req,
    IEspGetNodeGroupUsageResponse& resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Machine Information. Permission denied.");

        StringBuffer cacheID;
        buildUsageCacheID(cacheID, req.getNodeGroups(), "AllNodeGroups");

        if (!req.getBypassCachedResult() && readNodeGroupUsageCache(context, cacheID, resp))
            return true;

        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();

        Owned<IPropertyTree> usageReq = createPTree("Req");
        Owned<IPropertyTree> uniqueUsages = createPTree("Usage");
        readNodeGroupUsageReq(req, constEnv, usageReq, uniqueUsages);

        getMachineUsages(context, uniqueUsages);

        IArrayOf<IEspNodeGroupUsage> nodeGroupUsages;
        readNodeGroupUsageResult(context, usageReq, uniqueUsages, nodeGroupUsages);
        machineUsageCache->add(cacheID, nodeGroupUsages);
        resp.setNodeGroupUsages(nodeGroupUsages);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void Cws_machineEx::buildComponentUsageCacheID(StringBuffer& id, IArrayOf<IConstComponent>& componentList)
{
    if (!componentList.ordinality())
    {
        id.set("AllComponents");
        return;
    }

    StringArray componentNames;
    ForEachItemIn(i, componentList)
    {
        IConstComponent& component = componentList.item(i);
        StringBuffer str(component.getType());
        if (str.isEmpty())
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Empty Component Type");
        str.append(":").append(component.getName());
        componentNames.append(str);
    }
    componentNames.sortAscii();
    componentNames.getString(id, ",");
}

void Cws_machineEx::buildUsageCacheID(StringBuffer& id, StringArray& names, const char* defaultID)
{
    if (names.ordinality())
    {
        names.sortAscii();
        names.getString(id, ",");
    }
    else
        id.set(defaultID);
}

bool Cws_machineEx::readComponentUsageCache(IEspContext& context, const char* cacheID,
    IEspGetComponentUsageResponse& resp)
{
    Owned<MachineUsageCacheElement> cachedUsage = machineUsageCache->lookup(context,
        cacheID, machineUsageCacheMinutes);
    if (!cachedUsage)
        return false;

    IArrayOf<IEspComponentUsage> componentUsages;
    ForEachItemIn(i, cachedUsage->componentUsages)
        componentUsages.append(*LINK(&cachedUsage->componentUsages.item(i)));
    resp.setComponentUsages(componentUsages);
    return true;
}

bool Cws_machineEx::readTargetClusterUsageCache(IEspContext& context, const char* cacheID,
    IEspGetTargetClusterUsageResponse& resp)
{
    Owned<MachineUsageCacheElement> cachedUsage = machineUsageCache->lookup(context,
        cacheID, machineUsageCacheMinutes);
    if (!cachedUsage)
        return false;

    IArrayOf<IEspTargetClusterUsage> targetClusterUsages;
    ForEachItemIn(i, cachedUsage->tcUsages)
        targetClusterUsages.append(*LINK(&cachedUsage->tcUsages.item(i)));
    resp.setTargetClusterUsages(targetClusterUsages);
    return true;
}

bool Cws_machineEx::readNodeGroupUsageCache(IEspContext& context, const char* cacheID,
    IEspGetNodeGroupUsageResponse& resp)
{
    Owned<MachineUsageCacheElement> cachedUsage = machineUsageCache->lookup(context,
        cacheID, machineUsageCacheMinutes);
    if (!cachedUsage)
        return false;

    IArrayOf<IEspNodeGroupUsage> nodeGroupUsages;
    ForEachItemIn(i, cachedUsage->ngUsages)
        nodeGroupUsages.append(*LINK(&cachedUsage->ngUsages.item(i)));
    resp.setNodeGroupUsages(nodeGroupUsages);
    return true;
}

bool Cws_machineEx::onGetComponentStatus(IEspContext &context, IEspGetComponentStatusRequest &req, IEspGetComponentStatusResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Read, ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Component Status. Permission denied.");

        Owned<IComponentStatusFactory> factory = getComponentStatusFactory();
        Owned<IESPComponentStatusInfo> status = factory->getComponentStatus();
        if (!status) //Should never happen
            return false;

        int statusID = status->getComponentStatusID();
        if (statusID < 0)
        {
            resp.setStatus("Not reported");
        }
        else
        {
            resp.setComponentType(status->getComponentType());
            resp.setEndPoint(status->getEndPoint());
            resp.setReporter(status->getReporter());
            resp.setComponentStatus(status->getComponentStatus());
            resp.setTimeReportedStr(status->getTimeReportedStr());

            IConstStatusReport* componentStatus = status->getStatusReport();
            if (componentStatus)
                resp.setStatusReport(*componentStatus);

            resp.setComponentStatusList(status->getComponentStatusList());
        }
        resp.setComponentStatusID(statusID);
        resp.setStatusCode(0);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_machineEx::onUpdateComponentStatus(IEspContext &context, IEspUpdateComponentStatusRequest &req, IEspUpdateComponentStatusResponse &resp)
{
    try
    {
        context.ensureFeatureAccess(FEATURE_URL, SecAccess_Write, ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Update Component Status. Permission denied.");

        const char* reporter = req.getReporter();
        if (!reporter || !*reporter)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Report not specified.");
        
        Owned<IComponentStatusFactory> factory = getComponentStatusFactory();
        factory->updateComponentStatus(reporter, req.getComponentStatusList());
        resp.setStatusCode(0);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

