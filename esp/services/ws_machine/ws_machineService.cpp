/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include <math.h>
#include "ws_machineService.hpp"
#include "jarray.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"
#include "thirdparty.h"
#include "ws_topology.hpp"

#include "rmtfile.hpp"
#include "exception_util.hpp"

#ifndef eqHoleCluster
#define eqHoleCluster  "HoleCluster"
#endif

#ifndef eqThorCluster
#define eqThorCluster  "ThorCluster"
#endif

#ifndef eqRoxieCluster
#define eqRoxieCluster "RoxieCluster"
#endif

#ifndef eqEclCCServer
#define eqEclCCServer       "EclCCServerProcess"
#endif

#ifndef eqEclAgent
#define eqEclAgent          "EclAgentProcess"
#endif

#ifndef eqEclScheduler
#define eqEclScheduler      "EclSchedulerProcess"
#endif

#ifndef eqThorMasterProcess
#define eqThorMasterProcess     "ThorMasterProcess"
#endif

#ifndef eqThorSlaveProcess
#define eqThorSlaveProcess      "ThorSlaveProcess"
#endif

#ifndef eqThorSpareProcess
#define eqThorSpareProcess      "ThorSpareProcess"
#endif

#ifndef eqHOLEMACHINES
#define eqHOLEMACHINES          "HOLEMACHINES"
#endif

#ifndef eqROXIEMACHINES
#define eqROXIEMACHINES       "ROXIEMACHINES"
#endif

#ifndef eqMACHINES
#define eqMACHINES              "MACHINES"
#endif

static const int THREAD_POOL_SIZE = 40;
static const int THREAD_POOL_STACK_SIZE = 64000;
static const char* FEATURE_URL = "MachineInfoAccess";


//---------------------------------------------------------------------------------------------
//NOTE: PART I of implementation for Cws_machineEx
//      PART II and III are in ws_machineServiceMetrics.cpp and ws_machineServiceRexec.cpp resp.
//---------------------------------------------------------------------------------------------

class CMachineInfoThreadParam : public CWsMachineThreadParam
{
public:
   IMPLEMENT_IINTERFACE;

   IEspContext& m_context;
   StringBuffer m_sProcessType;
    StringBuffer m_sCompName;
    StringBuffer m_sConfigAddress;
   StringBuffer m_sUserId;
   StringBuffer m_sPassword;
    StringBuffer m_sPath;
    bool             m_bECLAgent;
   bool         m_bGetProcessorInfo;
   bool         m_bGetStorageInfo;
   bool         m_bGetSwInfo;
   bool         m_bFilterProcesses;
   bool         m_bMonitorDaliFileServer;
    bool             m_bMultipleInstances;
   Cws_machineEx::OpSysType   m_operatingSystem;
   Linked<IEspMachineInfoEx>    m_pMachineInfo;
   Linked<IEspMachineInfoEx>    m_pMachineInfo1;
   set<string>& m_columnSet;
   StringArray& m_columnArray;
   const StringArray& m_additionalProcesses;

    CMachineInfoThreadParam( const char* pszAddress, const char* pszProcessType, const char* pszCompName, const char* pszUserId,
                            const char* pszPassword, const char* pszPath, set<string>& columnSet, StringArray& columnArray,
                            bool bGetProcessorInfo, bool bGetStorageInfo, bool bGetSwInfo, bool bFilterProcesses,
                            bool bMonitorDaliFileServer, Cws_machineEx::OpSysType os, const StringArray& additionalProcesses,
                            IEspMachineInfoEx* pMachineInfo,  Cws_machineEx* pService, IEspContext& context, const char* pszConfigAddress)
       : CWsMachineThreadParam(pszAddress, pszUserId, pszPassword, pService),
         m_columnSet(columnSet),
         m_columnArray(columnArray),
         m_operatingSystem(os),
         m_context(context),
         m_additionalProcesses(additionalProcesses)
   {
        m_bECLAgent        = false;
      m_sUserId          = pszUserId;
      m_sPassword        = pszPassword;
        m_sPath              = pszPath;
      m_bFilterProcesses = bFilterProcesses;
      m_bMonitorDaliFileServer = bMonitorDaliFileServer;
      m_sProcessType     = pszProcessType;
        m_sCompName          = pszCompName;
        m_sConfigAddress     = pszConfigAddress,
      m_bGetProcessorInfo= bGetProcessorInfo;
      m_bGetStorageInfo  = bGetStorageInfo;
      m_bGetSwInfo       = bGetSwInfo;
      m_pMachineInfo.set( pMachineInfo );
        m_bMultipleInstances = false;
    }

    CMachineInfoThreadParam( const char* pszAddress, const char* pszProcessType, const char* pszCompName, const char* pszUserId,
                            const char* pszPassword, const char* pszPath, set<string>& columnSet, StringArray& columnArray,
                            bool bGetProcessorInfo, bool bGetStorageInfo, bool bGetSwInfo, bool bFilterProcesses,
                            bool bMonitorDaliFileServer, Cws_machineEx::OpSysType os, const StringArray& additionalProcesses,
                            IEspMachineInfoEx* pMachineInfo,  IEspMachineInfoEx* pMachineInfo1,  Cws_machineEx* pService, IEspContext& context, const char* pszConfigAddress)
       : CWsMachineThreadParam(pszAddress, pszUserId, pszPassword, pService),
         m_columnSet(columnSet),
         m_columnArray(columnArray),
         m_operatingSystem(os),
         m_context(context),
         m_additionalProcesses(additionalProcesses)
   {
      m_sUserId          = pszUserId;
      m_sPassword        = pszPassword;
        m_sPath              = pszPath;
      m_bFilterProcesses = bFilterProcesses;
      m_bMonitorDaliFileServer = bMonitorDaliFileServer;
      m_sProcessType     = pszProcessType;
        m_sCompName          = pszCompName;
        m_sConfigAddress     = pszConfigAddress,
      m_bGetProcessorInfo= bGetProcessorInfo;
      m_bGetStorageInfo  = bGetStorageInfo;
      m_bGetSwInfo       = bGetSwInfo;
      m_pMachineInfo.set( pMachineInfo );
      m_pMachineInfo1.set( pMachineInfo1 );
        m_bECLAgent        = true;
        m_bMultipleInstances = false;
    }

    CMachineInfoThreadParam( const char* pszAddress, const char* pszProcessType,
                                     const char* pszCompName, const char* pszSecString, const char* pszUserId,
                            const char* pszPassword, const char* pszPath, set<string>& columnSet, StringArray& columnArray,
                            bool bGetProcessorInfo, bool bGetStorageInfo, bool bGetSwInfo, bool bFilterProcesses,
                            bool bMonitorDaliFileServer, Cws_machineEx::OpSysType os, const StringArray& additionalProcesses,
                            IEspMachineInfoEx* pMachineInfo,  Cws_machineEx* pService, IEspContext& context)
       : CWsMachineThreadParam(pszAddress, pszSecString, pService),
         m_columnSet(columnSet),
         m_columnArray(columnArray),
         m_operatingSystem(os),
         m_context(context),
         m_additionalProcesses(additionalProcesses)
   {
      m_sUserId          = pszUserId;
      m_sPassword        = pszPassword;
        m_sPath              = pszPath;
      m_bFilterProcesses = bFilterProcesses;
      m_bMonitorDaliFileServer = bMonitorDaliFileServer;
      m_sProcessType     = pszProcessType;
        m_sCompName          = pszCompName;
      m_bGetProcessorInfo= bGetProcessorInfo;
      m_bGetStorageInfo  = bGetStorageInfo;
      m_bGetSwInfo       = bGetSwInfo;
      m_pMachineInfo.set( pMachineInfo );
        m_bMultipleInstances = false;
        m_bECLAgent        = false;
   }

   virtual void doWork()
   {
      m_pService->doGetMachineInfo(m_context, this);
   }

   void addColumn(const char* columnName)
   {
      synchronized block(s_mutex);

      if (m_columnSet.find(columnName) == m_columnSet.end())
      {
         m_columnSet.insert(columnName);
         m_columnArray.append(columnName);
      }
   }
private:
   static Mutex s_mutex;
};

/*static*/Mutex CMachineInfoThreadParam::s_mutex;

//---------------------------------------------------------------------------------------------
/*static*/map<string, int> Cws_machineEx::s_processTypeToSnmpIdMap;
/*static*/map<string, const char*> Cws_machineEx::s_oid2CompTypeMap;


void Cws_machineEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    Owned<IPropertyTree> pServiceNode = cfg->getPropTree(xpath.str());

    m_bMonitorDaliFileServer = pServiceNode->getPropBool("@monitorDaliFileServer", false);
    m_processFilters.setown( pServiceNode->getPropTree("ProcessFilters") );
    const char* pchExcludePartitions = pServiceNode->queryProp("@excludePartitions");
    if (pchExcludePartitions && *pchExcludePartitions)
    {
        StringArray sPartitions;
        DelimToStringArray(pchExcludePartitions, sPartitions, ", ;");
        unsigned int nPartitions = sPartitions.ordinality();
        for (unsigned int i=0; i<nPartitions; i++)
        {
            const char* partition = sPartitions.item(i);
            if (partition && *partition)
                if (strchr(partition, '*'))
                    m_excludePartitionPatterns.insert( partition );
                else
                    m_excludePartitions.insert( partition );
        }
    }
    m_envFactory.setown( getEnvironmentFactory() );

    xpath.clear().appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    Owned<IPropertyTree> pProcessNode = cfg->getPropTree(xpath.str());
    m_useDefaultHPCCInit = pProcessNode->getPropBool("UseDefaultHPCCInit", true);

    const char* machineInfoPath = pProcessNode->queryProp("MachineInfoFile");
    if (machineInfoPath && *machineInfoPath)
    {
        m_machineInfoFile.append(machineInfoPath);
    }
    else
    {
        m_machineInfoFile.append("preflight");
    }
    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IPropertyTree> pRoot = &constEnv->getPTree();
    IPropertyTree* pEnvSettings = pRoot->getPropTree("EnvSettings");
    if (pEnvSettings)
    {
        pEnvSettings->getProp("configs", environmentConfData.m_configsPath.clear());
        pEnvSettings->getProp("path", environmentConfData.m_executionPath.clear());
        pEnvSettings->getProp("runtime", environmentConfData.m_runtimePath.clear());
        pEnvSettings->getProp("lock", environmentConfData.m_lockPath.clear());
        pEnvSettings->getProp("pid", environmentConfData.m_pidPath.clear());
        pEnvSettings->getProp("user", environmentConfData.m_user.clear());
    }
    IThreadFactory* pThreadFactory = new CWsMachineThreadFactory();
    m_threadPool.setown(createThreadPool("WsMachine Thread Pool", pThreadFactory,
        NULL, THREAD_POOL_SIZE, 10000, THREAD_POOL_STACK_SIZE)); //10 sec timeout for available thread; use stack size of 2MB
    pThreadFactory->Release();

    //populate the process type to SNMP component index map
    if (s_processTypeToSnmpIdMap.empty())
    {
        //add mappings in random order to keep the map balanced
        //don't add Hole and Thor since they have numerous sub-types and are better
        //handled as special cases
        s_processTypeToSnmpIdMap.insert(pair<string, int>("ThorMasterProcess",  3));
        s_processTypeToSnmpIdMap.insert(pair<string, int>("EclServerProcess",   4));
        s_processTypeToSnmpIdMap.insert(pair<string, int>("DaliServerProcess",  7));
        s_processTypeToSnmpIdMap.insert(pair<string, int>("RoxieServerProcess", 16));
        s_processTypeToSnmpIdMap.insert(pair<string, int>("DfuServerProcess",   18));
        s_processTypeToSnmpIdMap.insert(pair<string, int>("SashaServerProcess", 20));
        s_processTypeToSnmpIdMap.insert(pair<string, int>("EspProcess",         100));
    }
    if (s_processTypeToProcessMap.empty())
    {
        s_processTypeToProcessMap.insert(pair<string, const char*>("DaliServerProcess",  "daserver"));
        s_processTypeToProcessMap.insert(pair<string, const char*>("DfuServerProcess",   "dfuserver"));
        s_processTypeToProcessMap.insert(pair<string, const char*>("EclServerProcess",   "eclserver"));
        s_processTypeToProcessMap.insert(pair<string, const char*>("FTSlaveProcess",         "ftslave"));
        s_processTypeToProcessMap.insert(pair<string, const char*>("SashaServerProcess", "saserver"));
        s_processTypeToProcessMap.insert(pair<string, const char*>("ThorMasterProcess",  "thormaster"));
        s_processTypeToProcessMap.insert(pair<string, const char*>("RoxieServerProcess", "roxie"));
    }
}

Cws_machineEx::~Cws_machineEx()
{
}

IConstEnvironment* Cws_machineEx::getConstEnvironment()
{
    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironmentByFile();
    return constEnv.getLink();
}

const char* Cws_machineEx::getEnvironmentConf(const char* confFileName)
{
    if (!confFileName || !*confFileName)
        return NULL;

    StringBuffer environmentConf;
    IFile * pFile = createIFile(confFileName);
    if (pFile->exists( ))
    {
        Owned<IFileIO> pFileIO = pFile->openShared(IFOread, IFSHfull);
        if (pFileIO)
        {
            StringBuffer tmpBuf;
            offset_t fileSize = pFile->size();
            tmpBuf.ensureCapacity((unsigned)fileSize);
            tmpBuf.setLength((unsigned)fileSize);

            size32_t nRead = pFileIO->read(0, (size32_t) fileSize, (char*)tmpBuf.str());
            if (nRead == fileSize)
            {
                environmentConf = tmpBuf;
            }
        }
    }
    return environmentConf.str();
}

bool Cws_machineEx::onGetMachineInfo(IEspContext &context, IEspGetMachineInfoRequest & req,
                                             IEspGetMachineInfoResponse & resp)
{
#ifdef DETECT_WS_MC_MEM_LEAKS
    static bool firstTime = true;
    if (firstTime)
    {
        firstTime = false;
        unsigned t = setAllocHook(true);
    }
#endif //DETECT_WS_MC_MEM_LEAKS
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Machine Information. Permission denied.");

        StringBuffer user;
        StringBuffer pw;
        context.getUserID(user);
        context.getPassword(pw);

        IEspRequestInfoStruct& reqInfo = resp.updateRequestInfo();
        reqInfo.setSecurityString(req.getSecurityString());
        reqInfo.setSortBy(req.getSortBy());
        reqInfo.setGetProcessorInfo(req.getGetProcessorInfo());
        reqInfo.setGetStorageInfo(req.getGetStorageInfo());
        reqInfo.setGetSoftwareInfo(req.getGetSoftwareInfo());
        reqInfo.setSortBy("Address");
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
        IArrayOf<IEspMachineInfoEx> machineArray;
        StringArray columnArray;

        const char* userName = m_sTestStr1.str();
        const char* password = m_sTestStr2.str();
        if (userName && *userName)
        {
            reqInfo.setUserName(userName);
            resp.setUserName(userName);
        }
        else
        {
            reqInfo.setUserName(user.str());
        }
        if (password && *password)
        {
            reqInfo.setPassword(password);
            resp.setPassword(password);
        }
        else
        {
            reqInfo.setPassword(pw.str());
        }

        RunMachineQuery(context, req.getAddresses(),reqInfo,machineArray,columnArray);
        resp.setColumns( columnArray );
        resp.setMachines(machineArray);

        char timeStamp[32];
        getTimeStamp(timeStamp);
        resp.setTimeStamp( timeStamp );
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
#ifdef DETECT_WS_MC_MEM_LEAKS
    DBGLOG("Allocated=%d", setAllocHook(false));
#endif //DETECT_WS_MC_MEM_LEAKS

    return true;
}


bool Cws_machineEx::onGetMachineInfoEx(IEspContext &context, IEspGetMachineInfoRequestEx & req,
                                             IEspGetMachineInfoResponseEx & resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Machine Information. Permission denied.");

        StringBuffer user;
        StringBuffer pw;
        context.getUserID(user);
        context.getPassword(pw);

        Owned<IEspRequestInfoStruct> reqInfo = new CRequestInfoStruct("","");
        reqInfo->setGetProcessorInfo(true);
        reqInfo->setGetStorageInfo(true);
        reqInfo->setGetSoftwareInfo(true);
        reqInfo->setUserName(user.str());
        reqInfo->setPassword(pw.str());

        reqInfo->setSortBy("Address");
        reqInfo->setClusterType( req.getClusterType() );

        IArrayOf<IEspMachineInfoEx> machineArray;
        StringArray columnArray;
        RunMachineQuery(context, req.getAddresses(),*reqInfo,machineArray,columnArray);
        resp.setMachines(machineArray);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;


}

void Cws_machineEx::RunMachineQuery(IEspContext &context, StringArray &addresses,IEspRequestInfoStruct&  reqInfo,
                                             IArrayOf<IEspMachineInfoEx>& machineArray, StringArray& columnArray)
{
   bool bMonitorDaliFileServer = m_bMonitorDaliFileServer;
    bool bFilterProcesses = reqInfo.getApplyProcessFilter();
    int ordinality= addresses.ordinality();
    int index = 0;

    if (!ordinality)
        return;

   StringArray additionalProcesses;
    if (bFilterProcesses)
   {
      const char* xpath = reqInfo.getPath();
      if (xpath && *xpath)
      {
         StringBuffer decodedPath;
          JBASE64_Decode(xpath, decodedPath);

          try
          {
              //xpath is the Path to parent node (normally a cluster)
            Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
            Owned<IConstEnvironment> constEnv = envFactory->openEnvironmentByFile();
            Owned<IPropertyTree> root0 = &constEnv->getPTree();
            if (!root0)
                throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

            char* xpath = (char*)decodedPath.str();
            if (!strnicmp(xpath, "/Environment/", 13))
                xpath += 13;

            IPropertyTree* root = root0->queryPropTree( xpath );
            if (!root)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Specified path '%s' is invalid!", decodedPath.str());

            bMonitorDaliFileServer = root->getPropBool("@monitorDaliFileServer", false);
          }
          catch(IException* e)
         {
            StringBuffer msg;
            e->errorMessage(msg);
              WARNLOG("%s", msg.str());
              e->Release();
          }
          catch(...)
         {
              WARNLOG("Unknown Exception caught within Cws_machineEx::RunMachineQuery");
          }
      }

      DelimToStringArray(reqInfo.getAddProcessesToFilter(), additionalProcesses, " ,\t");

      int len = additionalProcesses.length();
      for (int i=0; i<len; i++)
      {
         StringBuffer sProcessName = additionalProcesses.item(i);
         sProcessName.toLowerCase().replaceString(".exe", "");
         if (sProcessName.length()==0)
         {
            additionalProcesses.remove(i--, true);//decrement i so we process the next item (now at position i)
            len--;
         }
         else
            additionalProcesses.replace(sProcessName, i, true);
      }
   }

   typedef multimap<unsigned, string> AddressMap;
   AddressMap addressMap; //maps <numeric address> to <process>:<comp name>:<os>:<path>
    UnsignedArray threadHandles;
    set<string>   columnSet;
    IpAddress     ipAddr;

    for (index=0; index<ordinality; index++)
    {
        char *address = strdup( addresses.item(index) );
        char* props = strchr(address, ':');
        if (props)
            *props++ = '\0';
        else
            props = (char *)"";

        char* configAddress = NULL;
        char* props1 = strchr(address, '|');
        if (props1)
        {
            configAddress = props1+1;
            *props1 = '\0';
        }
        if (!configAddress || !*configAddress)
        {
            configAddress = address;
        }

        IpAddress ipAddr;
        unsigned numIps = ipAddr.ipsetrange(address);
        //address is like 192.168.1.4-6:ThorSlaveProcess:thor1:2:path1
        //so process each address in the range
        for (unsigned j=0;j<numIps;j++)
        {
            if (!ipAddr.isIp4())
                IPV6_NOT_IMPLEMENTED();
            unsigned numAddr;
            if (ipAddr.getNetAddress(sizeof(numAddr),&numAddr)!=sizeof(numAddr))
                IPV6_NOT_IMPLEMENTED(); // Not quite right exception, but will use when IPv4 hack sanity check fails

            //if no mapping exists for numAddr yet or if we are using filters and props are different then
            //insert in the map
            AddressMap::const_iterator i = addressMap.find(numAddr);
            bool bInsert = (i == addressMap.end()) ||
                (reqInfo.getGetSoftwareInfo() && 0 != strcmp((*i).second.c_str(), props));
            if (bInsert)
            {
                StringBuffer sBuf;
                if (configAddress && *configAddress)
                    sBuf.appendf("%s:%s", configAddress, props);
                addressMap.insert(pair<unsigned, string>(numAddr, sBuf.str()));
            }
            ipAddr.ipincrement(1);
        }
        free(address);
    }


   AddressMap::const_iterator iBeginAddr = addressMap.begin();
   AddressMap::const_iterator iEndAddr   = addressMap.end();
    for (AddressMap::const_iterator iAddr = iBeginAddr; iAddr != iEndAddr; iAddr++)
    {
        IpAddress ipAddr;

        unsigned numAddr =  (*iAddr).first;          // TBD IPv6
        ipAddr.setNetAddress(sizeof(numAddr),&numAddr);
        StringBuffer address;
        ipAddr.getIpText(address);

        StringBuffer sProcessType;
        StringBuffer sCompName;
        OpSysType    os = OS_Windows;
        StringBuffer sPath;

        const char *configAddress = (*iAddr).second.c_str();
        char* props = (char*) strchr(configAddress, ':');
        if (props)
            *props++ = '\0';
        else
            props = (char*) configAddress;

        if (props)
            parseProperties( props, sProcessType, sCompName, os, sPath);
        else
            bFilterProcesses = false;

      //OS is constant for a m/c so can be ignored if IP is already recorded
        if (*address.str())
        {
            bool bAgentExec = stricmp(sProcessType.str(), "EclAgentProcess")==0;
            Owned<IEspMachineInfoEx> pMachineInfo = static_cast<IEspMachineInfoEx*>(new CMachineInfoEx(""));
            pMachineInfo->setOS( os );
            if (!bAgentExec)
            {
               machineArray.append(*pMachineInfo.getLink());

                CMachineInfoThreadParam* pThreadReq =
                    new CMachineInfoThreadParam( address.str(), sProcessType.str(), sCompName.str(), reqInfo.getUserName(), reqInfo.getPassword(), sPath.str(),
                                                          columnSet, columnArray, reqInfo.getGetProcessorInfo(), reqInfo.getGetStorageInfo(), reqInfo.getGetSoftwareInfo(),
                                                          bFilterProcesses, bMonitorDaliFileServer, os, additionalProcesses, pMachineInfo, this, context, configAddress);

                PooledThreadHandle handle = m_threadPool->start( pThreadReq );
                threadHandles.append(handle);
            }
            else
            {
                Owned<IEspMachineInfoEx> pMachineInfo1 = static_cast<IEspMachineInfoEx*>(new CMachineInfoEx(""));
                pMachineInfo1->setOS( os );
                machineArray.append(*pMachineInfo1.getLink());

                CMachineInfoThreadParam* pThreadReq =
                    new CMachineInfoThreadParam( address.str(), sProcessType.str(), sCompName.str(), reqInfo.getUserName(), reqInfo.getPassword(), sPath.str(),
                                         columnSet, columnArray, reqInfo.getGetProcessorInfo(), reqInfo.getGetStorageInfo(), reqInfo.getGetSoftwareInfo(),
                                         bFilterProcesses, bMonitorDaliFileServer, os, additionalProcesses, pMachineInfo, pMachineInfo1, this, context, configAddress);

                PooledThreadHandle handle = m_threadPool->start( pThreadReq );
                threadHandles.append(handle);
            }
      }
    }
   //block for worker theads to finish, if necessary and then collect results
    PooledThreadHandle* pThreadHandle = threadHandles.getArray();
    unsigned i=threadHandles.ordinality();
    while (i--)
    {
        m_threadPool->join(*pThreadHandle);
        pThreadHandle++;
    }
}

// the following method is invoked on worker threads of
void Cws_machineEx::getUpTime(CMachineInfoThreadParam* pParam, StringBuffer& out)
{
    if (pParam->m_sAddress.length() < 1)
        return;

    SocketEndpoint ep(pParam->m_sAddress.str());
    MemoryBuffer outbuf;
    int ret = remoteExec(ep,"/bin/cat /proc/uptime",NULL,true,0,NULL,&outbuf);
    if (ret != 0)
        return;

    outbuf.append((byte)0);

    const char *pStr = outbuf.toByteArray();
    if (!pStr)
        return;

    char *pStr0 = (char *) strchr(pStr, ' ');
    if (!pStr0)
        return;

    pStr0[0] = 0;

    int days = 0, hours = 0, min = 0;
    double seconds = 0.0;
    double dSec = atof(pStr);

    if (dSec > 24*3600)
    {
        days = (int) dSec/(24*3600);
    }

    hours = (int) (dSec/3600 - days * 24);
    min = (int) (dSec/60 - (days * 24 + hours) * 60);
    seconds = (int) (dSec - (days * 24 * 3600 + hours * 3600 + min * 60));

    if (days > 0)
        out.appendf("%d days, %d:%d:%2.2f", days, hours, min, seconds);
    else
        out.appendf("%d:%d:%2.2f", hours, min, seconds);

    return;
}
void Cws_machineEx::readAString(const char *orig, const char *begin, const char *end, StringBuffer& strReturn, bool bTrim)
{
    char* pStr = (char*) strstr(orig, begin);
    if (pStr)
    {
        pStr += strlen(begin);
        if (pStr)
        {
            char buf[1024];

            char* pStr1 = (char*) strchr(pStr, 0x0a);
            if (pStr1)
            {
                strncpy(buf, pStr, pStr1 - pStr);
                buf[pStr1 - pStr] = 0;
            }
            else
                strcpy(buf, pStr);

            strReturn.append(buf);
            if (bTrim)
                strReturn.trim();
        }
    }
}

void Cws_machineEx::readTwoStrings(const char *orig, const char *begin, const char *middle, const char *end, StringBuffer& strReturn1, StringBuffer& strReturn2, bool bTrim)
{
    char* pStr = (char*) strstr(orig, begin);
    if (pStr)
    {
        pStr += strlen(begin);
        if (pStr)
        {
            char buf[1024];

            char* pStr1 = (char*) strchr(pStr, 0x0a);
            if (pStr1)
            {
                strncpy(buf, pStr, pStr1 - pStr);
                buf[pStr1 - pStr] = 0;
            }
            else
                strcpy(buf, pStr);

            strReturn1.append(buf);
            if (bTrim)
                strReturn1.trim();

            if (strReturn1.length() > 0)
            {
                pStr = (char*) strReturn1.str();
                if (pStr)
                {
                    char* pStr1 = (char*) strstr(pStr, middle);
                    if (pStr1)
                    {
                        int len0 = strReturn1.length();
                        int len1 = pStr1 - pStr;
                        int len2 = len0 - strlen(middle) - len1;
                        strReturn2.append(strReturn1);
                        strReturn1.remove(len1, len0-len1);
                        strReturn2.remove(0, len0-len2);
                    }
                }
            }
        }
    }
}

void Cws_machineEx::readSpace(const char *line, char* title, __int64& free, __int64& total, int& percentAvail)
{
    if (!line)
        return;

    __int64 used = 0;
    char free0[1024], used0[1024];

    char* pStr = (char*) line;
    char* pStr1 = (char*) strchr(pStr, ':');
    if (!pStr1)
        return;

    strncpy(title, pStr, pStr1 - pStr);
    title[pStr1 - pStr] = 0;
    pStr = pStr1 + 2;

    pStr1 = (char*) strchr(pStr, ' ');
    if (!pStr1)
        return;

    strncpy(used0, pStr, pStr1 - pStr);
    used0[pStr1 - pStr] = 0;
    pStr = pStr1 + 1;
    if (!pStr)
        return;

    strcpy(free0, pStr);

    __int64 factor1 = 1;
    if (strlen(free0) > 9)
    {
        free0[strlen(free0) - 6] = 0;
        factor1 = 1000000;
    }
    free = atol(free0)*factor1;

    __int64 factor2 = 1;
    if (strlen(used0) > 9)
    {
        used0[strlen(used0) - 6] = 0;
        factor2 = 1000000;
    }
    used = atol(used0)*factor2;

    total = free + used;
    if (total > 0)
        percentAvail = (int) ((free*100)/total);

    free = (__int64) free /1000; //MByte
    total = (__int64) total /1000; //MByte
    return;
}

int Cws_machineEx::readMachineInfo(const char *response, CMachineInfo& machineInfo)
{
    if (!response || !*response)
        return -1;

    StringBuffer computerUptime;
    readAString(response, "ProcessUpTime:", "\r", machineInfo.m_sProcessUptime, true);
    if (machineInfo.m_sProcessUptime.length() > 0)
        readAString(response, "ProcessID:", "\r", machineInfo.m_sID, true);
    readAString(response, "CPU-Idle:", "\r", machineInfo.m_sCPUIdle, true);
    readAString(response, "ComputerUpTime:", "\r", computerUptime, true);

    const char* spaceLine = "---SpaceUsedAndFree---";
    char* pStr = (char*) strstr(response, spaceLine);
    if (pStr)
    {
        pStr += strlen(spaceLine)+1;
        if (pStr)
            machineInfo.m_sSpace.append(pStr);
    }

    if (computerUptime.length() > 0)
    {
        machineInfo.m_sComputerUptime = computerUptime;
        char* pStr = (char*) computerUptime.str();
        char* ppStr = strchr(pStr, ' ');
        if (ppStr)
        {
            ppStr++;
            ppStr = strchr(ppStr, ' ');
            if (ppStr)
            {
                ppStr++;
                if (ppStr)
                    machineInfo.m_sComputerUptime.clear().append(ppStr);
            }
        }
    }

    return 0;
}

int Cws_machineEx::runCommand(IEspContext& context, const char* sAddress, const char* sConfigAddress, const char* sCommand, const char* sUserId,
                                         const char* sPassword, StringBuffer& sResponse)
{
    int iRet = 0;

   try
   {
      StringBuffer command(sCommand);
      StringBuffer cmdLine;
      StringBuffer userId;
      StringBuffer password;
      bool bLinux;
      int exitCode = -1;

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

#ifdef _WIN32
#ifndef CHECK_LINUX_COMMAND
#define popen  _popen
#define pclose _pclose

      // Use psexec as default remote control program
      if (bLinux)
      {
         if (!checkFileExists(".\\plink.exe"))
            throw MakeStringException(ECLWATCH_PLINK_NOT_INSTALLED, "Invalid ESP installation: missing plink.exe to execute the remote program!");

         command.replace('\\', '/');//replace all '\\' by '/'

         /*
         note that if we use plink (cmd line ssh client) for the first time with a computer,
         it generates the following message:

         The server's host key is not cached in the registry. You have no guarantee that the
         server is the computer you think it is.  The server's key fingerprint is:
         1024 aa:bb:cc:dd:ee:ff:gg:hh:ii:jj:kk:ll:mm:nn:oo:pp
         If you trust this host, enter "y" to add the key to
         PuTTY's cache and carry on connecting.  If you want to carry on connecting just once,
         without adding the key to the cache, enter "n".If you do not trust this host, press
         Return to abandon the connection.

         To get around this, we pipe "n" to plink without using its -batch parameter.  We need
         help from cmd.exe to do this though...
         */
         cmdLine.appendf("cmd /c \"echo y | .\\plink.exe -ssh -l %s -pw %s %s sudo bash -c '%s' 2>&1\"",
             environmentConfData.m_user.str(), password.str(), sAddress, command.str());
      }
      else
      {
         if (!checkFileExists(".\\psexec.exe"))
            throw MakeStringException(ECLWATCH_PSEXEC_NOT_INSTALLED, "Invalid ESP installation: missing psexec.exe to execute the remote program!");

         cmdLine.appendf(".\\psexec \\\\%s -u %s -p %s %s cmd /c %s 2>&1",
            sAddress, userId.str(), password.str(),
            "", command.str());
      }
#else
      if (bLinux)
      {
         command.replace('\\', '/');//replace all '\\' by '/'
         cmdLine.appendf("ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5");
         cmdLine.appendf(" %s '%s' 2>&1", sAddress, command.str());
      }
      else
      {
         sResponse.append("Remote execution from Linux to Windows is not supported!");
         exitCode = 1;
      }
#endif
#else
      if (bLinux)
      {
         command.replace('\\', '/');//replace all '\\' by '/'
         cmdLine.appendf("ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5");
         cmdLine.appendf(" %s '%s' 2>&1", sAddress, command.str());
      }
      else
      {
         sResponse.append("Remote execution from Linux to Windows is not supported!");
         exitCode = 1;
      }
#endif

      if (*cmdLine.str())
      {
            StringBuffer response, response1;
                exitCode = invokeProgram(cmdLine, response);
                if (exitCode < 0)
                    response1.append("Failed in executing a system command.\n");
                else
                    response1.append("System command(s) has been executed.\n");

            //remove \n at the end
            int len = response.length();
            if (len > 0 && response.charAt(--len) == '\n')
               response.setLength(len);

                if (response.length() > 0)
                    response1.appendf("Response: %s", response.str());
                else
                    response1.append("No response received.\n");

            sResponse.append(response1.str());
      }

      iRet = exitCode;
   }
   catch(IException* e)
   {
      StringBuffer buf;
      e->errorMessage(buf);
      sResponse.append(buf.str());
      iRet = e->errorCode();
   }
#ifndef NO_CATCHALL
   catch(...)
   {
      sResponse.append("An unknown exception occurred!");
        iRet = -1;
   }
#endif

    return iRet;
}

//---------------------------------------------------------------------------
//  createProcess
//---------------------------------------------------------------------------
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
   if( (fp = popen( command_line, "r" )) == NULL )
      return -1;

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
   return pclose( fp );
}

int Cws_machineEx::remoteGetMachineInfo(IEspContext& context, const char *address, const char *configAddress, const char *preflightCommand, const char* user, const char* password, StringBuffer& sResponse, CMachineInfo& machineInfo)
{
#ifdef DETECT_WS_MC_MEM_LEAKS
    static bool firstTime = true;
    if (firstTime)
    {
        firstTime = false;
        unsigned t = setAllocHook(true);
    }
#endif //DETECT_WS_MC_MEM_LEAKS

    int iRet = runCommand(context, address, configAddress, preflightCommand, user, password, sResponse);
    if (iRet == 0)
    {
        iRet = readMachineInfo(sResponse.str(), machineInfo);
    }

#ifdef DETECT_WS_MC_MEM_LEAKS
    DBGLOG("Allocated=%d", setAllocHook(false));
#endif //DETECT_WS_MC_MEM_LEAKS
    return iRet;
}

// the following method is invoked on worker threads of
void Cws_machineEx::doGetMachineInfo(IEspContext& context, CMachineInfoThreadParam* pParam)
{
   IEspMachineInfoEx* info = pParam->m_pMachineInfo;
   IEspMachineInfoEx* info1 = pParam->m_pMachineInfo1;
   StringBuffer& sSecurityString = pParam->m_sSecurityString;

   try
   {
       info->setAddress(pParam->m_sAddress.str());
       info->setConfigAddress(pParam->m_sConfigAddress.str());
        info->setProcessType(pParam->m_sProcessType.str());
        info->setComponentName( pParam->m_sCompName.str() );
        info->setComponentPath( pParam->m_sPath.str());

        char displayName[128];
        GetDisplayProcessName(pParam->m_sProcessType.str(), displayName);
       info->setDisplayType(displayName);


        if (pParam->m_bECLAgent)
        {
            info1->setAddress(pParam->m_sAddress.str());
            info1->setConfigAddress(pParam->m_sConfigAddress.str());
            info1->setProcessType("AgentExecProcess");
            info1->setComponentName( pParam->m_sCompName.str() );
            info1->setComponentPath( pParam->m_sPath.str());

            char displayName[128];
            GetDisplayProcessName("AgentExecProcess", displayName);
            info1->setDisplayType(displayName);
        }

        int iRet = 0;
        CMachineInfo machineInfo;

        StringBuffer preFlightCommand;
        preFlightCommand.appendf("/%s/sbin/%s %s", environmentConfData.m_executionPath.str(), m_machineInfoFile.str(), environmentConfData.m_pidPath.str());
        if (preFlightCommand.charAt(preFlightCommand.length() - 1) == '/')
            preFlightCommand.remove(preFlightCommand.length()-1, 1);
        preFlightCommand.appendf(" %s", pParam->m_sCompName.str());

        if (!stricmp(pParam->m_sProcessType.str(), "ThorMasterProcess"))
            preFlightCommand.append("_master");
        else if (!stricmp(pParam->m_sProcessType.str(), "ThorSlaveProcess"))
            preFlightCommand.append("_slave");

        StringBuffer sResponse;
        iRet = remoteGetMachineInfo(context, pParam->m_sAddress.str(), pParam->m_sConfigAddress.str(), preFlightCommand.str(), pParam->m_sUserName.str(), pParam->m_sPassword.str(), sResponse, machineInfo);
        if (iRet != 0)
        {
            info->setDescription(sResponse.str());
            if (pParam->m_bECLAgent)
                info1->setDescription(sResponse.str());
        }
        else
        {
            IArrayOf<IEspProcessInfo> runningProcesses;
            getRunningProcesses(context, pParam->m_sAddress.str(), pParam->m_sConfigAddress.str(), pParam->m_sUserName.str(), pParam->m_sPassword.str(), runningProcesses);

            if (pParam->m_bGetStorageInfo)
            {
                IArrayOf<IEspStorageInfo> storageArray;
                doGetStorageInfo(pParam, storageArray, machineInfo);
                info->setStorage(storageArray);
                if (pParam->m_bECLAgent)
                    info1->setStorage(storageArray);
                storageArray.kill();
            }
            if (pParam->m_bGetProcessorInfo)
            {
                IArrayOf<IEspProcessorInfo> processorArray;
                doGetProcessorInfo(pParam, processorArray, machineInfo);
                info->setProcessors(processorArray);
                if (pParam->m_bECLAgent)
                    info1->setProcessors(processorArray);
            }

            if (pParam->m_bGetSwInfo)
            {
                IArrayOf<IEspSWRunInfo> runArray;
                doGetSWRunInfo(context, pParam, runArray, machineInfo, runningProcesses,
                                    pParam->m_sProcessType.str(),
                                    pParam->m_bFilterProcesses,
                                    pParam->m_bMonitorDaliFileServer,
                                    pParam->m_additionalProcesses);
                info->setRunning(runArray);
                if (pParam->m_bECLAgent)
                    info1->setRunning(runArray);
            }

            if (machineInfo.m_sComputerUptime.length() > 0)
            {
                info->setUpTime(machineInfo.m_sComputerUptime.str());
                if (pParam->m_bECLAgent)
                    info1->setUpTime(machineInfo.m_sComputerUptime.str());
            }
            else
            {
                info->setUpTime("-");
                if (pParam->m_bECLAgent)
                    info1->setUpTime("-");
            }

            pParam->addColumn("Up Time");
        }
   }
   catch (IException* e)
   {
      StringBuffer sError;
      e->errorMessage(sError);
      e->Release();
        info->setDescription(sError.str());
   }
   catch (const char* e)
   {
        info->setDescription(e);
   }
   catch (...)
   {
        info->setDescription("Unknown exception!");
   }
}

void Cws_machineEx::doGetSecurityString(const char* address, StringBuffer& securityString)
{
   //another client (like configenv) may have updated the constant environment and we got notified
   //(thanks to our subscription) so reload it
   m_envFactory->validateCache();

   securityString.clear();
    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IConstMachineInfo> machine = constEnv->getMachineByAddress(address);
    if (machine)
   {
       Owned<IConstDomainInfo> domain = machine->getDomain();
       if (domain)
      {
         StringBufferAdaptor strval(securityString);
         domain->getSnmpSecurityString(strval);
      }
   }
}

IPropertyTree* Cws_machineEx::getComponent(const char* compType, const char* compName)
{
    StringBuffer xpath;
    xpath.append("Software/").append(compType).append("[@name='").append(compName).append("']");

   m_envFactory->validateCache();

    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IPropertyTree> pEnvRoot = &constEnv->getPTree();
    return pEnvRoot->getPropTree( xpath.str() );
}

void Cws_machineEx::getAccountAndPlatformInfo(const char* address, StringBuffer& userId,
                                              StringBuffer& password, bool& bLinux)
{
   m_envFactory->validateCache();

    Owned<IConstEnvironment> constEnv = getConstEnvironment();
    Owned<IConstMachineInfo> machine = constEnv->getMachineByAddress(address);
    if (!machine && !stricmp(address, "."))
    {
        machine.setown(constEnv->getMachineByAddress("127.0.0.1"));
    }

    if (!machine)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Machine %s is not defined in environment!", address);

   bLinux = machine->getOS() == MachineOsLinux;

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
}

void Cws_machineEx::determineRequredProcesses(CMachineInfoThreadParam* pParam,
                                                             const char* pszProcessType,
                                                             bool bMonitorDaliFileServer,
                                                             const StringArray& additionalProcesses,
                                                             set<string>& requiredProcesses)
{
   StringBuffer xpath;
   const char* pszOperatingSystem = pParam->m_operatingSystem == OS_Windows ? "Windows" : "Linux";
   xpath.appendf("Platform[@name='%s']", pszOperatingSystem);
   IPropertyTree* pOsSpecificFilters = m_processFilters->queryPropTree( xpath.str() );

   if (!pOsSpecificFilters)
      throw MakeStringException(ECLWATCH_INVALID_PROCESS_FILTER, "No process filters have been defined for %s!", pszOperatingSystem);

   //create a set (sorted list that is easy to search into) of processes expected to run
   //on this box
   StringBuffer sProcessName;

   //first collect all "common" processes that are required on any box irrespective of
   //its type
   StringBuffer xPath("ProcessFilter[@name='any']/Process");
   Owned<IPropertyTreeIterator> iProcesses = pOsSpecificFilters->getElements(xPath.str());
   ForEach (*iProcesses)
   {
      iProcesses->query().getProp("@name", sProcessName.clear());
      sProcessName.toLowerCase().replaceString(".exe", "");

      //if this process name is valid and either we are monitoring dali file server or
      //(we are not monitoring that and) it is not that process then add it to list of required processes
        if (m_useDefaultHPCCInit)
        {
            if (*sProcessName.str() && (bMonitorDaliFileServer || 0 != strcmp(sProcessName.str(), "dafilesrv")))
                requiredProcesses.insert(sProcessName.str());
        }
        else
        {
            if (*sProcessName.str() && (bMonitorDaliFileServer || 0 != strcmp(sProcessName.str(), "dafilesrv")))
                requiredProcesses.insert(sProcessName.str());
        }
   }

   //insert all additional processes that may have been specified with the request
   int len = additionalProcesses.length();
   for (int i=0; i<len; i++)
      requiredProcesses.insert(additionalProcesses.item(i));

   //now collect all "box-specific" processes that are required to be running
   //for eg. thorslave[.exe] on a thor slave box
   if (pszProcessType && *pszProcessType)
   {
      xPath.clear().appendf("ProcessFilter[@name='%s']", pszProcessType);
      IPropertyTree* pProcessFilterNode = pOsSpecificFilters->queryPropTree(xPath.str());

        if (pProcessFilterNode)
        {
            pParam->m_bMultipleInstances = pParam->m_operatingSystem == OS_Linux && pProcessFilterNode->getPropBool("@multipleInstances", false);

            StringBuffer strCompName(pParam->m_sCompName);
            strCompName.trim();

            Owned<IPropertyTreeIterator> iProcesses = pProcessFilterNode->getElements("Process");
            ForEach (*iProcesses)
            {
                IPropertyTree* pProcess = &iProcesses->query();
                bool bRemove = pProcess->getPropBool("@remove", false);
                const char* sName = pProcess->queryProp("@name");
                sProcessName.clear();
                if (!strcmp(sName, "."))
                    sProcessName = pParam->m_sCompName;
                else
                    sProcessName.append(sName);
                if (bRemove)
                    sProcessName.toLowerCase().replaceString(".exe", "");

                if (*sProcessName.str())
                {
                    if (bRemove)
                        requiredProcesses.erase(sProcessName.str());
                    else
                    {
                        //support multithor: if this is a linux system and processtype is either a thor master or slave
                        if (!strncmp(pszProcessType, "Thor", 4) && pParam->m_operatingSystem != OS_Windows)
                        {
                            //lookup port from environment
                            Owned<IPropertyTree> pComponent = getComponent("ThorCluster", strCompName.str());
                            const bool bMaster = !strcmp(pszProcessType, "ThorMasterProcess");
                            StringBuffer szPort;
                            if (pComponent)
                                szPort.append(pComponent->queryProp( bMaster ? "@masterport" : "@slaveport"));
                            if (szPort.length() == 0)
                                szPort.appendf("%s", bMaster ? "6500" : "6600");

                            StringBuffer sPort(szPort);
                            sProcessName.append('_').append(sPort.trim().str());
                        }

                        requiredProcesses.insert(sProcessName.str());
                    }
                }
            }
        }
    }
}

char* Cws_machineEx::skipChar(const char* sBuf, char c)
{
    char* pStr2 = (char*) strchr(sBuf, c);
    if (pStr2)
    {
        while (pStr2[0] == c)
        pStr2++;
    }

    return pStr2;
}

void Cws_machineEx::readRunningProcess(const char* lineBuf, IArrayOf<IEspProcessInfo>& runningProcesses)
{
    if (strlen(lineBuf) < 1)
        return;

    int pid = -1;
    char desc[256];
    char param[4096];

    char* pStr1 = (char*) lineBuf;

    //skip UID
    char* pStr2 = skipChar(pStr1, ' ');
    if (!pStr2)
        return;

    //read PID
    pStr1 = pStr2;
    pStr2 = (char*) strchr(pStr1, ' ');
    if (!pStr2)
        return;

    char id[32];
    strncpy(id, pStr1, pStr2 - pStr1);
    id[pStr2 - pStr1] = 0;

    for (unsigned i = 0; i < strlen(id); i++)
    {
        if (!isdigit(id[i]))
            return;
    }

    pid = atoi(id);

    while (pStr2[0] == ' ')
        pStr2++;

    //skip PPID
    pStr2 = skipChar(pStr2, ' ');
    if (!pStr2)
        return;

    //skip C
    pStr2 = skipChar(pStr2, ' ');
    if (!pStr2)
        return;

    //skip STIME
    pStr2 = skipChar(pStr2, ' ');
    if (!pStr2)
        return;

    //skip TTY
    pStr2 = skipChar(pStr2, ' ');
    if (!pStr2)
        return;

    //skip TIME
    pStr2 = skipChar(pStr2, ' ');
    if (!pStr2)
        return;

    //Read CMD
    bool bFound = false;
    if (!bFound)
    {
        strcpy(param, pStr2);

        pStr1 = pStr2;
        pStr2 = (char*) strchr(pStr1, ' ');
        if (!pStr2)
        {
            strcpy(desc, param);
        }
        else
        {
            if (pStr1[0] == '.' && pStr1[1] == '/')
                pStr1 += 2;
            strncpy(desc, pStr1, pStr2 - pStr1);
            desc[pStr2 - pStr1] = 0;
        }

        if (!strcmp(desc, "ps"))
            return;
    }

    //clean path, etc
    StringBuffer descStr(desc);
    if (descStr.charAt(0) == '[')
    {
        descStr.remove(0, 1);
        descStr = descStr.reverse();
        if (descStr.charAt(0) == ']')
            descStr.remove(0, 1);
        descStr = descStr.reverse();
    }
    else
    {
        descStr = descStr.reverse();
        pStr1 = (char*) descStr.str();
        pStr2 = (char*) strchr(pStr1, '/');
        if (pStr2)
        {
            strncpy(desc, pStr1, pStr2 - pStr1);
            desc[pStr2 - pStr1] = 0;
            descStr.clear().append(desc);
        }
        descStr = descStr.reverse();
    }

    Owned<IEspProcessInfo> info = createProcessInfo("","");
    info->setPID(pid);
    info->setParameter(param);
    info->setDescription(descStr.str());
    runningProcesses.append(*info.getClear());

    return;
}

void Cws_machineEx::getRunningProcesses(IEspContext& context, const char* address, const char* configAddress, const char* userId, const char* password, IArrayOf<IEspProcessInfo>& runningProcesses)
{
    StringBuffer sResponse;
    int iRet = runCommand(context, address, configAddress, "ps -ef", userId, password, sResponse);
    if (iRet == 0)
    {
        bool bStop = false;
        char* pStr = (char*) sResponse.str();
        while (!bStop && pStr)
        {
            char lineBuf[4096];

            //read a line
            char* pStr1 = (char*) strchr(pStr, 0x0a);
            if (!pStr1)
            {
                strcpy(lineBuf, pStr);
                bStop = true;
            }
            else
            {
                strncpy(lineBuf, pStr, pStr1 - pStr);
                lineBuf[pStr1 - pStr] = 0;
                pStr = pStr1+1;
            }

            if (strlen(lineBuf) > 0)
            {
                readRunningProcess(lineBuf, runningProcesses);
            }
        }
    }

    return;
}

void Cws_machineEx::checkRunningProcessesByPID(IEspContext& context, CMachineInfoThreadParam* pParam, set<string>* pRequiredProcesses)
{
    StringBuffer sCommand;
    StringBuffer sResponse;
    sCommand.appendf("ls %s", environmentConfData.m_pidPath.str());
    int iRet = runCommand(context, pParam->m_sAddress.str(), pParam->m_sConfigAddress.str(), sCommand.str(), pParam->m_sUserName.str(), pParam->m_sPassword.str(), sResponse);
    if (iRet == 0)
    {
        bool bStop = false;
        char* pStr = (char*) sResponse.str();
        while (!bStop)
        {
            char lineBuf[4096];

            //read a line
            char* pStr1 = (char*) strchr(pStr, 0x0a);
            if (!pStr1)
            {
                strcpy(lineBuf, pStr);
                bStop = true;
            }
            else
            {
                strncpy(lineBuf, pStr, pStr1 - pStr);
                lineBuf[pStr1 - pStr] = 0;
                pStr = pStr1+1;
            }

            if (strlen(lineBuf) > 0)
            {
                char* foundProcess = NULL;
                set<string>::const_iterator it   = pRequiredProcesses->begin();
              set<string>::const_iterator iEnd = pRequiredProcesses->end();
              for (; it != iEnd; it++) //add in sorted order simply by traversing the map
                {
                    StringBuffer sName;
                    if (strchr(lineBuf, ' '))
                    {
                        sName.appendf(" %s.pid", (*it).c_str());
                        const char* pStr = strstr(lineBuf, sName);
                        if (pStr)
                        {
                            foundProcess = (char*) ((*it).c_str());
                            break;
                        }
                    }
                    else
                    {
                        sName.appendf("%s.pid", (*it).c_str());
                        if (!stricmp(lineBuf, sName.str()))
                        {
                            foundProcess = (char*) ((*it).c_str());
                            break;
                        }
                    }
                }

                if (foundProcess)
                    pRequiredProcesses->erase(foundProcess);
            }
        }
    }

    return;
}

void Cws_machineEx::enumerateRunningProcesses(CMachineInfoThreadParam* pParam,
                                                             IArrayOf<IEspProcessInfo>& runningProcesses,
                                                             bool bLinuxInstance,
                                                             bool bFilterProcesses,
                                                             map<string, Linked<IEspSWRunInfo> >* processMap,
                                                             map<int, Linked<IEspSWRunInfo> >& pidMap,
                                                             set<string>* pRequiredProcesses)
{
    const bool bThorMasterOrSlave = !strncmp(pParam->m_sProcessType, "Thor", 4);
   ForEachItemIn(k, runningProcesses)
    {
        IEspProcessInfo& processInfo = runningProcesses.item(k);

        StringBuffer scmName = processInfo.getDescription();

        StringBuffer scmPath; //keep this in scope since pszName may point to it
        int pid = processInfo.getPID();

        const char* pszName = scmName.str();
        const char* pszPath = pszName;

        if (bLinuxInstance)
        {
            //dafilesrv would probably be running from a global directory
            //and not component's installation directory so ignore their paths
            if (//pParam->m_bMultipleInstances &&
                 0 != stricmp(pszName, "dafilesrv"))
            {
                scmPath.append(processInfo.getParameter());
                pszPath = scmPath.str();

                if (pszPath && *pszPath)
                {
                    if (!strncmp(pszPath, "bash ", 5))
                    {
                        pszPath = scmPath.remove(0, 5).str();
                        if (!pszPath || !*pszPath)
                            continue;
                    }
                    //params typically is like "/c$/esp_dir/esp [parameters...]"
                    //so just pick the full path
                    const char* pch = strchr(pszPath, ' ');
                    if (pch)
                    {
                        scmPath.setLength( pch - pszPath );
                        pszPath = scmPath.str();
                    }
                }
                else
                    pszPath = scmName.insert(0, '[').append(']').str();
            }

            if (pRequiredProcesses)
            {
                const char* pszProcessName;
                if (bThorMasterOrSlave && !strnicmp(pszName, "thor", 4))
                {
                    const char* pch = strrchr(pszPath, '/');
                    pszProcessName = pch ? pch+1 : pszName;
                }
                else
                {
                    const char* pszName0 = pParam->m_bMultipleInstances ? pszPath : pszName;
                    const char* pch = strrchr(pszName0, '/');
                    pszProcessName = pch ? pch+1 : pszName0;
                }
                pRequiredProcesses->erase(pszProcessName);
            }
            pszName = pszPath;
        }
        else
        {
            pszName = scmName.toLowerCase().replaceString(".exe", "");
            if (pRequiredProcesses)
                pRequiredProcesses->erase(pszName);
        }
        //skip processes starting with '[' character on linux unless we are not
        //applying filters and have to return a complete list of processes.
        //On windows, skip [system process] regardless.
        if ((bLinuxInstance && (!bFilterProcesses || *pszName != '[')) ||
             (!bLinuxInstance && *pszName != '['))
      {
         map<string, Linked<IEspSWRunInfo> >::iterator it;
            if (processMap)
                it = processMap->find(pszName);

            Linked<IEspSWRunInfo> lptr;
         if ( !processMap || it == processMap->end()) //not in the set
         {
              Owned<IEspSWRunInfo> info = static_cast<IEspSWRunInfo*>(new CSWRunInfo(""));
              info->setName(pszName);
            info->setInstances(1);
                lptr = info.get();

                if (processMap)
                    processMap->insert(pair<string, Linked<IEspSWRunInfo> >(pszName, lptr));
         }
         else
         {
                const Linked<IEspSWRunInfo>& linkedPtr = (*it).second;
              lptr = linkedPtr;
            lptr->setInstances( lptr->getInstances() + 1);
         }

            pidMap.insert(pair<int, Linked<IEspSWRunInfo> >(pid, lptr));
      }
    }
}

void Cws_machineEx::doGetStorageInfo(CMachineInfoThreadParam* pParam, IArrayOf<IEspStorageInfo> &output,
                                     CMachineInfo machineInfo)
{
    ///const int mbyte = !strcmp(temp.str(), "FixedDisk") ? 1000*1000 : 1024*1024;
    ///int units = storage->getUnits();
    if (machineInfo.m_sSpace.length() < 1)
        return;

    char* pStr = (char*) machineInfo.m_sSpace.str();
    while (pStr)
    {
        char buf[1024], title[1024];
        char* pStr1 = (char*) strchr(pStr, 0x0a);
        if (pStr1)
        {
            strncpy(buf, pStr, pStr1 - pStr);
            buf[pStr1 - pStr] = 0;
            pStr = pStr1+1;
        }
        else
        {
            strcpy(buf, pStr);
            pStr = NULL;
        }
        if (strlen(buf) < 1)
            continue;

        __int64 available = 0;
        __int64 total = 0;
        int percentAvail = 0;
        readSpace(buf, title, available, total, percentAvail);
        if ((strlen(title) < 1) || (total < 1))
            continue;

        if (!excludePartition(title))
        {
            Owned<IEspStorageInfo> info = static_cast<IEspStorageInfo*>(new CStorageInfo(""));
            pParam->addColumn( title );
            info->setDescription(title);
            info->setTotal(total);
            info->setAvailable(available);
            info->setPercentAvail(percentAvail);

            output.append(*info.getLink());
        }
    }
}

void Cws_machineEx::doGetProcessorInfo(CMachineInfoThreadParam* pParam, IArrayOf<IEspProcessorInfo> &output,
                                       CMachineInfo machineInfo)
{
   pParam->addColumn("CPU Load");

    int cpuLoad = 0;
    if (machineInfo.m_sCPUIdle.length() > 0)
    {
        char* cpuIdle = (char*) machineInfo.m_sCPUIdle.str();
        if (cpuIdle[strlen(cpuIdle) - 1] == '%')
            cpuIdle[strlen(cpuIdle) - 1] = 0;

        cpuLoad = 100-atoi(cpuIdle);
    }

    Owned<IEspProcessorInfo> info = static_cast<IEspProcessorInfo*>(new CProcessorInfo(""));
    info->setLoad(cpuLoad);

    output.append(*info.getLink());
}

void Cws_machineEx::doGetSWRunInfo(IEspContext& context, CMachineInfoThreadParam* pParam, IArrayOf<IEspSWRunInfo> &output,
                                   CMachineInfo machineInfo, IArrayOf<IEspProcessInfo>& runningProcesses,
                                   const char* pszProcessType, bool bFilterProcesses,
                                   bool bMonitorDaliFileServer,
                                   const StringArray& additionalProcesses)
{
    map<string, Linked<IEspSWRunInfo> > processMap; //save only one description of each process
   map<int, Linked<IEspSWRunInfo> > pidMap;
    bool bLinuxInstance = pParam->m_operatingSystem == OS_Linux;
    bool bAddColumn = false;

    bool bDafilesrvDown = false;

   if (bFilterProcesses)
   {
        bAddColumn = true;
        if (!m_useDefaultHPCCInit)
        {
            set<string> requiredProcesses;
            determineRequredProcesses( pParam, pszProcessType, bMonitorDaliFileServer, additionalProcesses,
                                                requiredProcesses);
            //now enumerate the processes running on this box and remove them from required processes
            //ignoring any non-required process
            if (runningProcesses.length() > 0)
            {
                enumerateRunningProcesses( pParam, runningProcesses, bLinuxInstance, true, NULL, pidMap, &requiredProcesses);
            }

            set<string>::const_iterator it   = requiredProcesses.begin();
            set<string>::const_iterator iEnd = requiredProcesses.end();

            for (; it != iEnd; it++) //add in sorted order simply by traversing the map
            {
                const char* procName = (*it).c_str();
                if (procName && *procName)
                {
                    IEspSWRunInfo* info = static_cast<IEspSWRunInfo*>(new CSWRunInfo(""));
                    if (!stricmp(procName, "dafilesrv"))
                        bDafilesrvDown = true;

                    info->setName(procName);
                    info->setInstances(0);

                    output.append(*info);
                }
            }
        }
    }
   else
   {
        if (pParam->m_operatingSystem == OS_Linux && pszProcessType && *pszProcessType)
        {
            StringBuffer xpath;
            xpath.appendf("Platform[@name='Linux']/ProcessFilter[@name='%s']/@multipleInstances", pszProcessType);
            pParam->m_bMultipleInstances = m_processFilters->getPropBool(xpath.str(), false);
        }

        if (runningProcesses.length() > 0)
        {
          bAddColumn = true;
            enumerateRunningProcesses( pParam, runningProcesses, bLinuxInstance,
                                            bFilterProcesses, &processMap, pidMap, NULL);
        }

      map<string, Linked<IEspSWRunInfo> >::const_iterator it;
      map<string, Linked<IEspSWRunInfo> >::const_iterator iEnd = processMap.end();

        if (!m_useDefaultHPCCInit)
        {
            set<string> requiredProcesses;
            determineRequredProcesses( pParam, pszProcessType, bMonitorDaliFileServer, additionalProcesses,
                                                requiredProcesses);

            set<string>::const_iterator it1   = requiredProcesses.begin();
            for (; it1 != requiredProcesses.end(); it1++) //add in sorted order simply by traversing the map
            {
                const char* procName = (*it1).c_str();
                if (procName && *procName && !stricmp(procName, "dafilesrv"))
                {
                    bDafilesrvDown = true;
                }
            }

            for (it=processMap.begin(); it != iEnd; it++) //add in sorted order simply by traversing the map
            {
                Linked<IEspSWRunInfo> info( (*it).second );

                const char* procName = info->getName();
                if (procName && *procName && !stricmp(procName, "dafilesrv"))
                {
                    bDafilesrvDown = false;
                }

                output.append( *info.getLink() );
            }
        }
        else
        {
            for (it=processMap.begin(); it != iEnd; it++) //add in sorted order simply by traversing the map
            {
                Linked<IEspSWRunInfo> info( (*it).second );
                output.append( *info.getLink() );
            }
        }
    }

    if (bAddColumn)
      pParam->addColumn("Processes");

   if (!bFilterProcesses)
   {
        if (pParam->m_operatingSystem == OS_Linux && pszProcessType && *pszProcessType)
        {
            StringBuffer xpath;
            xpath.appendf("Platform[@name='Linux']/ProcessFilter[@name='%s']/@multipleInstances", pszProcessType);
            pParam->m_bMultipleInstances = m_processFilters->getPropBool(xpath.str(), false);
        }
    }

    pParam->addColumn("Condition");
    pParam->addColumn("State");
    pParam->addColumn("UpTime");

    const char* procType = pParam->m_sProcessType.str();
    IEspComponentInfo* pComponentInfo = &pParam->m_pMachineInfo->updateComponentInfo();
    pComponentInfo->setCondition(-1);//failed to retrieve SNMP info - will get overwritten by walker callback, if successful

    //The bDafilesrvDown is set only when used
    if (!bDafilesrvDown && machineInfo.m_sID.length() > 0)
    {
        //conditions: unknown, normal, warning, minor, major, critical, fatal
        pComponentInfo->setCondition( 1 );
        pComponentInfo->setState(5);

        if (machineInfo.m_sProcessUptime.length() > 0)
        {
            char day[1024];
            char* pDay = (char*) machineInfo.m_sProcessUptime.str();
            char* pTime = strchr(pDay, '-');
            if (!pTime)
            {
                pComponentInfo->setUpTime( pDay );
            }
            else
            {
                strncpy(day, pDay, pTime - pDay);
                day[pTime - pDay] = 0;

                StringBuffer upTime;
                upTime.appendf("%s days %s", day, pTime+1);
                pComponentInfo->setUpTime( upTime.str() );
            }
        }
    }
    else
    {
        pComponentInfo->setCondition(2); //Warnning
        pComponentInfo->setState(0);

        if (bFilterProcesses && (machineInfo.m_sID.length() < 1))
        {
            IEspSWRunInfo* info = static_cast<IEspSWRunInfo*>(new CSWRunInfo(""));
            info->setName(pParam->m_sCompName.str());
            info->setInstances(0);
            output.append(*info);
        }
    }

    if (pParam->m_bECLAgent)
    {
        IEspComponentInfo* pComponentInfo1 = &pParam->m_pMachineInfo1->updateComponentInfo();
        pComponentInfo1->setCondition(pComponentInfo->getCondition()); //for AgentExec
        pComponentInfo1->setState(pComponentInfo->getState()); //for AgentExec
        pComponentInfo1->setUpTime(pComponentInfo->getUpTime()); //for AgentExec
        pComponentInfo->setUpTime("-"); //for ECL Agent
    }

}

//this method parses address info of the form "192.168.1.4-6:ThorSlaveProcess:thor1:2:path1"
//into respective components
void Cws_machineEx::parseProperties(const char* info, StringBuffer& processType, StringBuffer& sCompName,
                                                OpSysType& os, StringBuffer& path)
{
    StringArray sArray;
    DelimToStringArray(info, sArray, ":");
    unsigned int ordinality = sArray.ordinality();

    if (ordinality == 0)
        throw MakeStringException(ECLWATCH_MISSING_PARAMS, "Invalid address format '%s'.", info);

    processType.clear().append( sArray.item(0) );
    sCompName.clear();
    path.clear();
    os  = OS_Windows;

    if (ordinality > 1)
    {
        sCompName.append( sArray.item(1) );
        if (ordinality > 2)
        {
            os  = (OpSysType) atoi( sArray.item(2) );
            if (ordinality > 3)
            {
                path.append( sArray.item(3) );
                if (path.length())
                {
                    char pat1, pat2;
                    char rep1, rep2;

                    if (os == OS_Linux)
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

                    const char* pszPath = path.str();
                    if (os == OS_Linux && *pszPath != '/')
                    {
                        path.insert(0, '/');
                        pszPath = path.str();
                    }

                    if (*(pszPath + path.length()-1) != rep2)
                        path.append(rep2);
                }
            }
        }
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


int Cws_machineEx::lookupSnmpComponentIndex(const StringBuffer& sProcessType)
{
    map<string, int>::const_iterator it = s_processTypeToSnmpIdMap.find( sProcessType.str() );
    return (it != s_processTypeToSnmpIdMap.end()) ? (*it).second : -1;
}

const char* Cws_machineEx::lookupProcessname(const StringBuffer& sProcessType)
{
    map<string, const char*>::const_iterator it = s_processTypeToProcessMap.find( sProcessType.str() );
    return (it != s_processTypeToProcessMap.end()) ? (*it).second : "";
}
//---------------------------------------------------------------------------
//  GetDisplayProcessName
//---------------------------------------------------------------------------
const char* Cws_machineEx::GetDisplayProcessName(const char* processName, char* buf)
{
   //produces "LDAPServerProcess" as "LDAP Server" and "EspService" as "Esp Service", etc.
   const char* begin = buf;
   const char* end = strstr(processName, "Process");
   if (!end)
      end = processName + strlen(processName);

   *buf++ = *processName++;
   bool bLower = false;

   while (processName < end)
   {
      char ch = *processName;
      if (isupper(ch))
      {
         if (bLower || //last char was uppercase or the following character is lowercase?
            ((processName+1 < end) && islower(*(processName+1))))
         {
            *buf++ = ' ';
         }

         bLower = false;
      }
      else
         bLower = true;

      *buf++ = *processName++;
   }
   *buf = '\0';
   return begin;
}


bool Cws_machineEx::excludePartition(const char* partition) const
{
    //first see if this partition is meant to be excluded as is - for instance
    //if partition is /dev and /dev is one of the predefined partitions to be excluded
    set<string>::const_iterator it = m_excludePartitions.find( partition );
    set<string>::const_iterator itEnd = m_excludePartitions.end();
    bool bFound = false;

    if (it != itEnd)
        bFound = true;
    else
    {
        //now check if /dev* is one of the partitions to be excluded
        set<string>::const_iterator itBegin = m_excludePartitionPatterns.begin();
        itEnd = m_excludePartitionPatterns.end();
        unsigned int partitionLen = strlen(partition);

        for (it=itBegin; it != itEnd; it++)
        {
            const string& pattern = *it;
            if (bFound = ::WildMatch(partition, partitionLen, pattern.c_str(), pattern.length(), false))
                break;
        }
    }
    return bFound;
}

void Cws_machineEx::setAttPath(StringBuffer& Path,const char* PathToAppend,const char* AttName,const char* AttValue)
{
    Path.append("/");
    Path.append(PathToAppend);
    Path.append("[@");
    Path.append(AttName);
    Path.append("=\"");
    Path.append(AttValue);
    Path.append("\"]");
}

int Cws_machineEx::checkProcess(const char* type, const char* name, StringArray& typeArray, StringArray& nameArray)
{
    int pos = -1;

    if (!type || !*type || !name || !*name)
        return pos;

    int count = nameArray.ordinality();
    if (count < 1)
        return pos;

    int i = 0;
    while (i < count)
    {
        const char* name0 = nameArray.item(i);
        const char* type0 = typeArray.item(i);
        if (type0 && !strcmp(type, type0) && name0 && !strcmp(name, name0))
        {
            pos = i;
            break;
        }

        i++;
    }

    return pos;
}

void Cws_machineEx::getMachineList(IConstEnvironment* constEnv, IPropertyTree* envRoot, const char* machineName,
                                              const char* machineType, const char* status, const char* directory,
                                StringArray& processAddresses,
                                set<string>* pMachineNames/*=NULL*/)
{
    StringBuffer directoryStr = directory;
    Owned<IPropertyTreeIterator> machines= envRoot->getElements(machineType);
    if (machines->first()) 
    {
        do 
        {
            StringArray machineInstance;
            
            IPropertyTree &machine = machines->query();
            const char* computerName = machine.queryProp("@computer");
            if (!computerName || !*computerName)
            {
                Owned<IPropertyTreeIterator> instances= machine.getElements("Instance");
                if (instances->first()) 
                {
                    do 
                    {
                        IPropertyTree &instance = instances->query();
                        computerName = instance.queryProp("@computer");
                        if (!computerName || !*computerName)
                            continue;

                        if (directoryStr.length() < 1)
                            directoryStr.append(instance.queryProp("@directory"));

                        machineInstance.append(computerName);
                    } while (instances->next());
                }
            }
            else
            {
                machineInstance.append(computerName);
            }

            if (machineInstance.length() < 1)
                continue;

            for (unsigned i = 0; i < machineInstance.length(); i++)
            {
                const char* name0 = machineInstance.item(i);

                if (pMachineNames)//caller wishes us to avoid inserting duplicate entries for machines
                {
                    if (pMachineNames->find(name0) != pMachineNames->end())
                        continue;
                    pMachineNames->insert(name0);
                }

                StringBuffer processAddress, name, netAddress, configNetAddress, os;

                if (machineName && *machineName)
                    name.append(machineName);
                else
                    name.append(name0);
                
                Owned<IConstMachineInfo> pMachineInfo =  constEnv->getMachine(name0);
                if (pMachineInfo.get())
                {
                    SCMStringBuffer ep;

                    pMachineInfo->getNetAddress(ep);

                    const char* ip = ep.str();
                    if (!ip || stricmp(ip, "."))
                    {
                        netAddress.append(ep.str());
                        configNetAddress.append(ep.str());
                    }
                    else
                    {
                        StringBuffer ipStr;
                        IpAddress ipaddr = queryHostIP();
                        ipaddr.getIpText(ipStr);
                        if (ipStr.length() > 0)
                        {
                            netAddress.append(ipStr.str());
                            configNetAddress.append(".");
                        }
                    }
                    os.append(pMachineInfo->getOS());       
                }

                processAddress.appendf("%s|%s:%s:%s:%s:%s", netAddress.str(), configNetAddress.str(), machineType, name.str(), os.str(), directoryStr.str());

                processAddresses.append(processAddress);
            }
        } while (machines->next());
    }

    return;
}

const char* Cws_machineEx::getProcessTypeFromMachineType(const char* machineType)
{
    const char* processType = machineType;
    if (!stricmp(machineType, eqThorMasterProcess) || !stricmp(machineType, eqThorSlaveProcess) || !stricmp(machineType, eqThorSpareProcess))
    {
        processType = eqThorCluster;
    }
    else    if (!stricmp(machineType, "RoxieServerProcess") || !stricmp(machineType, "RoxieSlaveProcess"))
    {
        processType = eqRoxieCluster;
    }
    else    if (!stricmp(machineType, "AgentExecProcess"))
    {
        processType = eqEclAgent;
    }

    return processType;
}

void Cws_machineEx::setTargetClusterInfo(IPropertyTree* pTargetClusterTree, IArrayOf<IEspMachineInfoEx>& machineArray, IArrayOf<IEspTargetClusterInfo>& targetClusterInfoList)
{
    unsigned machineCount = machineArray.ordinality();
    if (machineCount < 1)
        return;

    if (!pTargetClusterTree)
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
                if (!name || !type || stricmp(name, processName.str()) || stricmp(getProcessTypeFromMachineType(type), processType.str()))
                    continue;

                Owned<IEspMachineInfoEx> pMachineInfo = static_cast<IEspMachineInfoEx*>(new CMachineInfoEx(""));
                pMachineInfo->copy(machineInfoEx);
                machineArrayNew.append(*pMachineInfo.getLink());
            }
        }

        if (machineArrayNew.ordinality())
            targetClusterInfo->setProcesses(machineArrayNew);

        targetClusterInfoList.append(*targetClusterInfo.getLink());
    }
}

void Cws_machineEx::getTargetClusterProcesses(StringArray& targetClusters, StringArray& processTypes, 
                StringArray& processNames, StringArray& processAddresses, IPropertyTree* pTargetClusterTree)
{
    unsigned ordinality= targetClusters.ordinality();
    if (ordinality < 1)
        return;

    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironmentByFile();
    Owned<IPropertyTree> pEnvironmentRoot = &constEnv->getPTree();
    if (!pEnvironmentRoot)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

    IPropertyTree* pEnvironmentSoftware = pEnvironmentRoot->queryPropTree("Software");
    if (!pEnvironmentSoftware)
        throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

    IPropertyTree* pEnvironmentDirectories = pEnvironmentSoftware->queryPropTree("Directories");
    for (unsigned index=0; index<ordinality; index++)
    {
        char* clusterName = strdup( targetClusters.item(index) );

        char type[1024];
        char* pClusterName = strchr(clusterName, ':');
        if (!pClusterName)
        {
            pClusterName = clusterName;
        }
        else
        {
            strncpy(type, clusterName, pClusterName - clusterName);
            type[pClusterName - clusterName] = 0;

            pClusterName++;
        }

        if (!pClusterName || !*pClusterName)
            continue;

        StringBuffer path;
        path.appendf("Software/Topology/Cluster[@name='%s']", pClusterName);

        IPropertyTree* pCluster = pEnvironmentRoot->queryPropTree(path.str());
        if (!pCluster)
            continue;

        Owned<IPropertyTreeIterator> thorClusters= pCluster->getElements(eqThorCluster);
        Owned<IPropertyTreeIterator> roxieClusters= pCluster->getElements(eqRoxieCluster);
        Owned<IPropertyTreeIterator> eclCCServerProcesses= pCluster->getElements(eqEclCCServer);
        Owned<IPropertyTreeIterator> eclAgentProcesses= pCluster->getElements(eqEclAgent);
        Owned<IPropertyTreeIterator> eclSchedulerProcesses= pCluster->getElements(eqEclScheduler);

        if (type && !stricmp(type, eqThorCluster) && !thorClusters->first())
            continue;

        if (type && !stricmp(type, eqRoxieCluster) && !roxieClusters->first())
            continue;

        if (type && !stricmp(type, eqHoleCluster) && (roxieClusters->first() || thorClusters->first()))
            continue;

        IPropertyTree *pTargetClusterInfo = pTargetClusterTree->addPropTree("TargetCluster", createPTree("TargetCluster"));
        if (!pTargetClusterInfo)
            throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in creating an XML tree");

        pTargetClusterInfo->setProp("@Name", pClusterName);
        if (type && *type)
            pTargetClusterInfo->setProp("@Type", type);

        //Read Cluster process
        if (thorClusters->first())
        {
            IArrayOf<IEspMachineInfoEx> machineArray;

            do 
            {
                IPropertyTree &thorCluster = thorClusters->query();     

                const char* process = thorCluster.queryProp("@process");
                if (process && *process)
                {
                    IPropertyTree *pThorClusterInfo = pTargetClusterInfo->addPropTree("Process", createPTree("Process"));
                    if (!pThorClusterInfo)
                        throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in creating an XML tree");

                    pThorClusterInfo->setProp("@Name", process);
                    pThorClusterInfo->setProp("@Type", eqThorCluster);
    
                    if (checkProcess(eqThorCluster, process, processTypes, processNames) < 0)
                    {
                        Owned<IEspMachineInfoEx> pMachineInfo = static_cast<IEspMachineInfoEx*>(new CMachineInfoEx(""));
                        pMachineInfo->setComponentName( process );
                        pMachineInfo->setProcessType(eqThorCluster);
                        machineArray.append(*pMachineInfo.getLink());

                        processTypes.append(eqThorCluster);
                        processNames.append(process);

                        StringBuffer dirStr;
                        if (pEnvironmentDirectories && !getConfigurationDirectory(pEnvironmentDirectories, "run", eqThorCluster, process, dirStr))
                        {
                            dirStr.clear().append(thorCluster.queryProp("@directory"));
                        }

                        path.clear().appendf("Software/%s[@name='%s']", eqThorCluster, process);
                        IPropertyTree* pClusterProcess = pEnvironmentRoot->queryPropTree(path.str());
                        if (pClusterProcess)
                        {
                            if (dirStr.length() < 1)
                                dirStr.append(pClusterProcess->queryProp("@directory"));
                            getMachineList(constEnv, pClusterProcess, process, eqThorMasterProcess, "", dirStr.str(), processAddresses);
                            getMachineList(constEnv, pClusterProcess, process, eqThorSlaveProcess, "", dirStr.str(), processAddresses);
                            getMachineList(constEnv, pClusterProcess, process, eqThorSpareProcess, "", dirStr.str(), processAddresses);
                        }
                    }
                }
            } while (thorClusters->next());
        }

        if (roxieClusters->first())
        {
            do {
                IPropertyTree &roxieCluster = roxieClusters->query();                   
                const char* process = roxieCluster.queryProp("@process");
                if (process && *process)
                {
                    IPropertyTree *pRoxieClusterInfo = pTargetClusterInfo->addPropTree("Process", createPTree("Process"));
                    if (!pRoxieClusterInfo)
                        throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in creating an XML tree");

                    pRoxieClusterInfo->setProp("@Name", process);
                    pRoxieClusterInfo->setProp("@Type", eqRoxieCluster);

                    if (checkProcess(eqRoxieCluster, process, processTypes, processNames) < 0)
                    {
                        processTypes.append(eqRoxieCluster);
                        processNames.append(process);

                        StringBuffer dirStr;
                        if (pEnvironmentDirectories && !getConfigurationDirectory(pEnvironmentDirectories, "run", eqRoxieCluster, process, dirStr))
                        {
                            dirStr.clear().append(roxieCluster.queryProp("@directory"));
                        }

                        path.clear().appendf("Software/%s[@name='%s']", eqRoxieCluster, process);
                        IPropertyTree* pClusterProcess = pEnvironmentRoot->queryPropTree(path.str());
                        if (pClusterProcess)
                        {
                            if (dirStr.length() < 1)
                                dirStr.append(pClusterProcess->queryProp("@directory"));
                            set<string> machineNames; //used for checking duplicates
                            getMachineList(constEnv, pClusterProcess, process, "RoxieServerProcess", "", dirStr.str(), processAddresses, &machineNames);
                            getMachineList(constEnv, pClusterProcess, process, "RoxieSlaveProcess", "", dirStr.str(), processAddresses, &machineNames);
                        }
                    }
                }
            } while (thorClusters->next());
        }

        //Read eclCCServer process
        if (eclCCServerProcesses->first())
        {
            IPropertyTree &eclCCServerProcess = eclCCServerProcesses->query();                  
            const char* process = eclCCServerProcess.queryProp("@process");
            if (process && *process)
            {
                IPropertyTree *pEclCCServerInfo = pTargetClusterInfo->addPropTree("Process", createPTree("Process"));
                if (!pEclCCServerInfo)
                    throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in creating an XML tree");

                pEclCCServerInfo->setProp("@Name", process);
                pEclCCServerInfo->setProp("@Type", eqEclCCServer);

                if (checkProcess(eqEclCCServer, process, processTypes, processNames) < 0)
                {
                    processTypes.append(eqEclCCServer);
                    processNames.append(process);

                    StringBuffer dirStr;
                    if (pEnvironmentDirectories && !getConfigurationDirectory(pEnvironmentDirectories, "run", eqEclCCServer, process, dirStr))
                    {
                        dirStr.clear().append(eclCCServerProcess.queryProp("@directory"));
                    }

                    getMachineList(constEnv, pEnvironmentSoftware, process, eqEclCCServer, "", dirStr.str(), processAddresses);
                }
            }
        }

        //Read eclAgent process
        if (eclAgentProcesses->first())
        {
            IPropertyTree &eclAgentProcess = eclAgentProcesses->query();                    
            const char* process = eclAgentProcess.queryProp("@process");
            if (process && *process)
            {
                IPropertyTree *pEclAgentInfo = pTargetClusterInfo->addPropTree("Process", createPTree("Process"));
                if (!pEclAgentInfo)
                    throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in creating an XML tree");

                pEclAgentInfo->setProp("@Name", process);
                pEclAgentInfo->setProp("@Type", eqEclAgent);

                if (checkProcess(eqEclAgent, process, processTypes, processNames) < 0)
                {
                    processTypes.append(eqEclAgent);
                    processNames.append(process);

                    StringBuffer dirStr;
                    if (pEnvironmentDirectories && !getConfigurationDirectory(pEnvironmentDirectories, "run", eqEclAgent, process, dirStr))
                    {
                        dirStr.clear().append(eclAgentProcess.queryProp("@directory"));
                    }

                    getMachineList(constEnv, pEnvironmentSoftware, process, eqEclAgent, "", dirStr.str(), processAddresses);
                }
            }
        }
        
        //Read eclScheduler process
        if (eclSchedulerProcesses->first())
        {
            IPropertyTree &eclSchedulerProcess = eclSchedulerProcesses->query();                    
            const char* process = eclSchedulerProcess.queryProp("@process");
            if (process && *process)
            {
                IPropertyTree *pEclSchedulerInfo = pTargetClusterInfo->addPropTree("Process", createPTree("Process"));
                if (!pEclSchedulerInfo)
                    throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed in creating an XML tree");

                pEclSchedulerInfo->setProp("@Name", process);
                pEclSchedulerInfo->setProp("@Type", eqEclScheduler);

                if (checkProcess(eqEclScheduler, process, processTypes, processNames) < 0)
                {
                    processTypes.append(eqEclScheduler);
                    processNames.append(process);

                    StringBuffer dirStr;
                    if (pEnvironmentDirectories && !getConfigurationDirectory(pEnvironmentDirectories, "run", eqEclScheduler, process, dirStr))
                    {
                        dirStr.clear().append(eclSchedulerProcess.queryProp("@directory"));
                    }

                    getMachineList(constEnv, pEnvironmentSoftware, process, eqEclScheduler, "", dirStr.str(), processAddresses);
                }
            }
        }

        free(clusterName);
    }

    return;
}

bool Cws_machineEx::onGetTargetClusterInfo(IEspContext &context, IEspGetTargetClusterInfoRequest & req,
                                             IEspGetTargetClusterInfoResponse & resp)
{
    try
    {
        if (!context.validateFeatureAccess(FEATURE_URL, SecAccess_Read, false))
            throw MakeStringException(ECLWATCH_MACHINE_INFO_ACCESS_DENIED, "Failed to Get Machine Information. Permission denied.");

        StringBuffer user;
        StringBuffer pw;
        context.getUserID(user);
        context.getPassword(pw);

        IEspRequestInfoStruct& reqInfo = resp.updateRequestInfo();
        reqInfo.setGetProcessorInfo(req.getGetProcessorInfo());
        reqInfo.setGetStorageInfo(req.getGetStorageInfo());
        reqInfo.setGetSoftwareInfo(req.getGetSoftwareInfo());
        reqInfo.setAutoRefresh( req.getAutoRefresh() );
        reqInfo.setMemThreshold(req.getMemThreshold());
        reqInfo.setDiskThreshold(req.getDiskThreshold());
        reqInfo.setCpuThreshold(req.getCpuThreshold());
        reqInfo.setMemThresholdType(req.getMemThresholdType());
        reqInfo.setDiskThresholdType(req.getDiskThresholdType());
        reqInfo.setApplyProcessFilter( req.getApplyProcessFilter() );
        reqInfo.setAddProcessesToFilter( req.getAddProcessesToFilter() );

        StringArray& targetClusters = req.getTargetClusters();
        StringArray processTypes, processNames, processAddresses;
        Owned<IPropertyTree> pTargetClusterTree = createPTreeFromXMLString("<Root/>");

        getTargetClusterProcesses(targetClusters, processTypes, processNames, processAddresses, pTargetClusterTree);

        if (processAddresses.ordinality())
        {
            IArrayOf<IEspMachineInfoEx> machineArray;
            StringArray columnArray;

            RunMachineQuery(context, processAddresses,reqInfo,machineArray,columnArray);

            resp.setColumns( columnArray );

            if (machineArray.ordinality())
            {
                IArrayOf<IEspTargetClusterInfo> targetClusterInfoList;
                setTargetClusterInfo(pTargetClusterTree, machineArray, targetClusterInfoList);
                resp.setTargetClusterInfoList(targetClusterInfoList);
            }           
        }

        char timeStamp[32];
        getTimeStamp(timeStamp);
        resp.setTimeStamp( timeStamp );
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}
