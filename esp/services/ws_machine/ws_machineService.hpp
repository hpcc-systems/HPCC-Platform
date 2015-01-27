/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef _ESPWIZ_ws_machine_HPP__
#define _ESPWIZ_ws_machine_HPP__

#pragma warning (disable : 4786)

#include "ws_machine_esp.ipp"
#include "environment.hpp"
#include <set>
#include <map>

class CMachineInfoThreadParam;
class CRoxieStateInfoThreadParam;
class CMetricsThreadParam;
class CRemoteExecThreadParam;

static const char *legacyFilterStrings[] = {"AttrServerProcess:attrserver", "DaliProcess:daserver",
"DfuServerProcess:dfuserver", "DKCSlaveProcess:dkcslave", "EclServerProcess:eclserver", "EclCCServerProcess:eclccserver",
"EspProcess:esp", "FTSlaveProcess:ftslave", "HoleControlProcess:hoctrl", "HoleSocketProcess:hoserver",
"HoleCollatorProcess:collator", "HoleProcessorProcess:processor", "JobServerProcess:jobserver",
"RoxieServerProcess:roxie", "RoxieSlaveProcess:roxie", "RoxieServerProcess:ccd", "RoxieFarmerProcess:ccd",
"RoxieSlaveProcess:ccd", "SchedulerProcess:scheduler","ThorMasterProcess:thormaster", "ThorSlaveProcess:thorslave",
"SashaServerProcess:saserver", NULL };

struct CEnvironmentConfData
{
    StringBuffer m_configsPath;
    StringBuffer m_executionPath;
    StringBuffer m_runtimePath;
    StringBuffer m_lockPath;
    StringBuffer m_pidPath;
    StringBuffer m_user;
};

static CEnvironmentConfData     environmentConfData;

struct CField
{
   double Value;
   bool  Warn;
   bool  Undefined;
   bool  Hide;

   CField()
      : Value(0), Warn(0), Undefined(0)
   {      
   }
    void serialize(StringBuffer& xml) const
    {
        xml.append("<Field>");
        xml.appendf("<Value>%f</Value>", Value);
        if (Warn)
            xml.append("<Warn>1</Warn>");
        if (Hide)
            xml.append("<Hide>1</Hide>");
        if (Undefined)
            xml.append("<Undefined>1</Undefined>");
        xml.append("</Field>");
    }
};

struct CFieldMap : public map<string, CField*>
{
    virtual ~CFieldMap()
    {
        const_iterator iEnd=end();
        for (const_iterator i=begin(); i!=iEnd; i++)
            delete (*i).second;
    }
    void serialize(StringBuffer& xml)
    {
        xml.append("<Fields>");
        const_iterator iEnd=end();
        for (const_iterator i=begin(); i!=iEnd; i++)
            (*i).second->serialize(xml);
        xml.append("</Fields>");
    }
};

struct CFieldInfo
{
   unsigned Count; //N
   double   SumSquaredDeviations; //SSD
   double Mean;
   double StandardDeviation;
   bool  Hide;

   CFieldInfo() 
      : Count(0),
        SumSquaredDeviations(0),
          Mean(0),
          StandardDeviation(0),
          Hide(true)
   {
   }
    void serialize(StringBuffer& xml, const char* fieldName) const
    {
        const char* fieldName0 = fieldName;
        if (!strncmp(fieldName, "ibyti", 5))
            fieldName += 5;

        xml.append("<FieldInfo>");
            xml.appendf("<Name>%s</Name>", fieldName0);
            xml.append("<Caption>");
            const char* pch = fieldName;
         if (!strncmp(pch, "lo", 2))
         {
            xml.append("Low");
            pch += 2;
         }
         else if (!strncmp(pch, "hi", 2))
         {
            xml.append("High");
            pch += 2;
         }
         else if (!strncmp(pch, "tot", 3))
         {
            xml.append("Total");
            pch += 3;
         }
         else xml.append( (char)toupper( *pch++) );

            while (*pch)
            {
                if (isupper(*pch))
                    xml.append(' ');
                xml.append(*pch++);     
            }
            xml.append("</Caption>");
            xml.appendf("<Mean>%f</Mean>", Mean);
            xml.appendf("<StandardDeviation>%f</StandardDeviation>", StandardDeviation);
            if (Hide)
                xml.appendf("<Hide>1</Hide>");
        xml.append("</FieldInfo>");
    }
};

struct CFieldInfoMap : public map<string, CFieldInfo*>
{
   Mutex    m_mutex;

    virtual ~CFieldInfoMap()
    {
        const_iterator iEnd=end();
        for (const_iterator i=begin(); i!=iEnd; i++)
            delete (*i).second;
    }

    void serialize(StringBuffer& xml) const
    {
        const_iterator iEnd=end();
        for (const_iterator i=begin(); i!=iEnd; i++)
        {
            const char* fieldName = (*i).first.c_str();
            (*i).second->serialize(xml, fieldName);
        }
    }
};

class CMetricsParam : public CInterface
{
public:
   IMPLEMENT_IINTERFACE;

   CMetricsParam( const char* pszAddress) : m_sAddress(pszAddress){}
   virtual ~CMetricsParam() {}

   StringBuffer       m_sAddress;
   CFieldMap          m_fieldMap; 
};

class CProcessData : public CInterface
{
    StringAttr    m_type;
    StringAttr    m_name;
    StringAttr    m_path;
    unsigned        m_processNumber;
    bool            m_multipleInstances;     //required from ProcessFilter in environment.xml

    StringAttr    m_pid;
    StringAttr    m_upTime;
    set<string>     m_dependencies;
public:
IMPLEMENT_IINTERFACE;

	CProcessData()
    {
        m_processNumber = 0;
        m_multipleInstances = false;
        m_dependencies.clear();
    }

    CProcessData(const char* name, const char* type, const char* path, unsigned processNumber):
        m_processNumber(processNumber)
    {
        m_name.set(name);
        m_type.set(type);
        m_path.set(path);
        m_multipleInstances = false;
        m_dependencies.clear();
    }
	virtual ~CProcessData(){}

    void setName(const char* name)
    {
        m_name.set(name);
    }

    const char* getName()
    {
        return m_name.str();
    }

    void setType(const char* type)
    {
        m_type.set(type);
    }

    const char* getType()
    {
        return m_type.str();
    }

    void setPath(const char* path)
    {
        m_path.set(path);
    }

    const char* getPath()
    {
        return m_path.str();
    }

    void setPID(const char* pid)
    {
        m_pid.set(pid);
    }

    const char* getPID()
    {
        return m_pid.str();
    }

    void setUpTime(const char* upTime)
    {
        m_upTime.set(upTime);
    }

    const char* getUpTime()
    {
        return m_upTime.str();
    }

    void setProcessNumber(unsigned processNumber)
    {
        m_processNumber = processNumber;
    }

    const unsigned getProcessNumber()
    {
        return m_processNumber;
    }

    void setMultipleInstances(bool multipleInstances)
    {
        m_multipleInstances = multipleInstances;
    }

    const bool getMultipleInstances()
    {
        return m_multipleInstances;
    }

    set<string>& getDependencies()
    {
        return m_dependencies;
    }
};

class CStorageData  : public CInterface
{
    StringBuffer    m_diskSpaceTitle;
    __int64         m_diskSpaceAvailable;
    __int64         m_diskSpaceTotal;
    int             m_diskSpacePercentAvail;

public:
    IMPLEMENT_IINTERFACE;

	CStorageData()
    {
        m_diskSpaceTitle.clear();
        m_diskSpaceAvailable = 0;
        m_diskSpaceTotal = 0;
        m_diskSpacePercentAvail = 0;
    }
	CStorageData(const char* diskSpaceTitle, __int64 diskSpaceAvailable, __int64 diskSpaceTotal, int diskSpacePercentAvail)
        : m_diskSpaceAvailable(diskSpaceAvailable), m_diskSpaceTotal(diskSpaceTotal), m_diskSpacePercentAvail(diskSpacePercentAvail)
    {
        m_diskSpaceTitle = diskSpaceTitle;
    }
	virtual ~CStorageData(){}

    void setDiskSpaceTitle(const char* title)
    {
        m_diskSpaceTitle.clear().append(title);
    }

    const char* getDiskSpaceTitle()
    {
        return m_diskSpaceTitle.str();
    }

    void setDiskSpaceAvailable(__int64 space)
    {
        m_diskSpaceAvailable = space;
    }

    const __int64 getDiskSpaceAvailable()
    {
        return m_diskSpaceAvailable;
    }

    void setDiskSpaceTotal(__int64 space)
    {
        m_diskSpaceTotal = space;
    }

    const __int64 getDiskSpaceTotal()
    {
        return m_diskSpaceTotal;
    }

    void setDiskSpacePercentAvail(int space)
    {
        m_diskSpacePercentAvail = space;
    }

    const int getDiskSpacePercentAvail()
    {
        return m_diskSpacePercentAvail;
    }
};

interface IStateHash : extends IInterface
{
    virtual unsigned queryID() = 0;
    virtual unsigned queryCount() = 0;
    virtual void incrementCount() = 0;
};

class CStateHash : public CInterface, implements IStateHash
{
    unsigned   id;
    unsigned   count;
public:
    IMPLEMENT_IINTERFACE;

    CStateHash(unsigned _id, unsigned _count) : id(_id), count(_count) { };

    virtual unsigned queryID() { return id; }
    virtual unsigned queryCount() { return count; }
    virtual void incrementCount() { count++; };
};

typedef MapStringToMyClass<IStateHash> StateHashes;

class CRoxieStateData : public CInterface
{
    BoolHash   ipAddress;
    StringAttr hash;
    unsigned   hashID; //the position inside cluster's state hash list - used to set the majorHash flag in updateMajorRoxieStateHash().
    bool       majorHash; //whether its state hash is the same as the most of other roxie cluster nodes or not.
    bool       ok;
    bool       attached;
    bool       detached;
public:
    IMPLEMENT_IINTERFACE;

    CRoxieStateData(const char* _ipAddress, unsigned _hashID) : hashID(_hashID), majorHash(true), ok(false), attached(false), detached(false)
    {
        ipAddress.setValue(_ipAddress, true);
    };

    bool matchIPAddress(const char* _ipAddress)
    {
        bool* match = ipAddress.getValue(_ipAddress);
        return (match && *match);
    }
    unsigned getHashID() { return hashID; }
    const char* getHash() { return hash.get(); }
    void setMajorHash(bool _majorHash) { majorHash = _majorHash; }

    void setState(bool _ok, bool _attached, bool _detached, const char* _hash)
    {
        ok = _ok;
        attached = _attached;
        detached = _detached;
        hash.set(_hash);
    }

    void reportState(StringBuffer& state, StringBuffer& stateDetails)
    {
        if (!ok)
            state.set("Node State: not ok ...");
        else if (!hash || !*hash)
            state.set("empty state hash ...");
        else if (!majorHash)
            state.set("State hash mismatch ...");
        else if (!attached)
            state.set("Not attached to DALI ...");
        else
            state.set("ok");

        if (ok)
            stateDetails.appendf("Node State: ok\n");
        else
            stateDetails.appendf("Node State: not ok\n");
        if (!hash || !*hash)
            stateDetails.appendf("This node had an empty hash\n");
        else
            stateDetails.appendf("State hash: %s\n", hash.get());
        if (!majorHash)
            stateDetails.appendf("State hash: mismatch\n");
        if (attached)
            stateDetails.appendf("This node attached to DALI\n");
        if (detached)
            stateDetails.appendf("This node detached from DALI\n");
    }
};

class CMachineData  : public CInterface
{
    char         m_pathSep;
    EnvMachineOS m_os;
    StringBuffer m_networkAddress;
    StringBuffer m_networkAddressInEnvSetting;  //Used for retrieving domainName/userId for MachineOsW2K

    int          m_CPULoad;
    StringBuffer m_computerUpTime;

    CIArrayOf<CStorageData>     m_storage;
    CIArrayOf<CProcessData>     m_processes;
    IArrayOf<IEspProcessInfo>   m_runningProcesses;
    set<string>                 m_dependencies; //from "any" process filter section in environment.xml
    set<string>                 m_additionalProcesses; //based on additionalProcessFilters in CGetMachineInfoUserOptions;
public:
    IMPLEMENT_IINTERFACE;

	CMachineData()
    {
        m_pathSep = '/';
        m_os = MachineOsLinux;
        m_CPULoad = 0;
        m_networkAddress.clear();
        m_networkAddressInEnvSetting.clear();
        m_computerUpTime.clear();
    }
    CMachineData(const char* networkAddress, const char* networkAddressInEnvSetting, EnvMachineOS os, char pathSep)
        : m_os(os), m_pathSep(pathSep)
    {
        m_networkAddress = networkAddress;
        m_networkAddressInEnvSetting = networkAddressInEnvSetting;
        m_CPULoad = 0;
        m_computerUpTime.clear();
    }
	virtual ~CMachineData(){}

    void setNetworkAddress(const char* networkAddress)
    {
        m_networkAddress.clear().append(networkAddress);
    }

    const char* getNetworkAddress()
    {
        return m_networkAddress.str();
    }

    void setNetworkAddressInEnvSetting(const char* networkAddress)
    {
        m_networkAddressInEnvSetting.clear().append(networkAddress);
    }

    const char* getNetworkAddressInEnvSetting()
    {
        return m_networkAddressInEnvSetting.str();
    }

    void setComputerUpTime(const char* computerUpTime)
    {
        m_computerUpTime.clear().append(computerUpTime);
    }

    const char* getComputerUpTime()
    {
        return m_computerUpTime.str();
    }

    void setOS(EnvMachineOS os)
    {
        m_os = os;
    }

    EnvMachineOS getOS()
    {
        return m_os;
    }

    void setPathSep(const char pathSep)
    {
        m_pathSep = pathSep;
    }

    const char getPathSep()
    {
        return m_pathSep;
    }

    void setCPULoad(int CPULoad)
    {
        m_CPULoad = CPULoad;
    }

    const int getCPULoad()
    {
        return m_CPULoad;
    }

    CIArrayOf<CStorageData>& getStorage()
    {
        return m_storage;
    }

    CIArrayOf<CProcessData>& getProcesses()
    {
        return m_processes;
    }

    IArrayOf<IEspProcessInfo>& getRunningProcesses()
    {
        return m_runningProcesses;
    }

    set<string>& getDependencies()
    {
        return m_dependencies; //from "any" process filter section in environment.xml
    }

    set<string>& getAdditinalProcessFilters()
    {
        return m_additionalProcesses; //based on additionalProcessFilters in CGetMachineInfoUserOptions;
    }
};

class CGetMachineInfoUserOptions : public CInterface
{
    StringBuffer m_userName;
    StringBuffer m_password;
    bool         m_getProcessorInfo;
    bool         m_getStorageInfo;
    bool         m_localFileSystemsOnly;
    bool         m_getSoftwareInfo;
    bool         m_applyProcessFilter;
    StringArray  m_additionalProcessFilters; //A user may add them using edit box 'Additional processes to filter:'.
public:
    IMPLEMENT_IINTERFACE;

	CGetMachineInfoUserOptions()
    {
        m_userName.clear();
        m_password.clear();
        m_getProcessorInfo = true;
        m_getStorageInfo = true;
        m_localFileSystemsOnly = true;
        m_getSoftwareInfo = true;
        m_applyProcessFilter = true;
    }
	virtual ~CGetMachineInfoUserOptions(){}

    void setUserName(const char* userName)
    {
        m_userName.clear().append(userName);
    }

    const char* getUserName()
    {
        return m_userName.str();
    }

    void setPassword(const char* password)
    {
        m_password.clear().append(password);
    }

    const char* getPassword()
    {
        return m_password.str();
    }

    void setGetProcessorInfo(bool getProcessorInfo)
    {
        m_getProcessorInfo = getProcessorInfo;
    }

    const bool getGetProcessorInfo()
    {
        return m_getProcessorInfo;
    }

    void setGetStorageInfo(bool getStorageInfo)
    {
        m_getStorageInfo = getStorageInfo;
    }

    const bool getGetStorageInfo()
    {
        return m_getStorageInfo;
    }

    void setLocalFileSystemsOnly(bool localFileSystemsOnly)
    {
        m_localFileSystemsOnly = localFileSystemsOnly;
    }

    const bool getLocalFileSystemsOnly()
    {
        return m_localFileSystemsOnly;
    }

    void setGetSoftwareInfo(bool getSoftwareInfo)
    {
        m_getSoftwareInfo = getSoftwareInfo;
    }

    const bool getGetSoftwareInfo()
    {
        return m_getSoftwareInfo;
    }

    void setApplyProcessFilter(bool applyProcessFilter)
    {
        m_applyProcessFilter = applyProcessFilter;
    }

    const bool getApplyProcessFilter()
    {
        return m_applyProcessFilter;
    }

    StringArray& getAdditionalProcessFilters()
    {
        return m_additionalProcessFilters;
    }
};

class CGetMachineInfoData
{
    //From request
    CIArrayOf<CMachineData>         m_machine;
    CGetMachineInfoUserOptions      m_options;

    //For response
    IArrayOf<IEspMachineInfoEx>     m_machineInfoTable;
    StringArray                     m_machineInfoColumns;

    StringArray                     roxieClusters;
    BoolHash                        uniqueRoxieClusters;

public:
    CGetMachineInfoUserOptions& getOptions()
    {
        return m_options;
    }

    CIArrayOf<CMachineData>& getMachineData()
    {
        return m_machine;
    }

    IArrayOf<IEspMachineInfoEx>& getMachineInfoTable()
    {
        return m_machineInfoTable;
    }

    StringArray& getMachineInfoColumns()
    {
        return m_machineInfoColumns;
    }

    StringArray& getRoxieClusters()
    {
        return roxieClusters;
    }

    void appendRoxieClusters(const char* clusterName)
    {
        bool* found = uniqueRoxieClusters.getValue(clusterName);
        if (found && *found)
            return;

        roxieClusters.append(clusterName);
        uniqueRoxieClusters.setValue(clusterName, true);
    }
};

//---------------------------------------------------------------------------------------------

class Cws_machineEx : public Cws_machine
{
public:
   IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    ~Cws_machineEx() {};

    bool onGetMachineInfo(IEspContext &context, IEspGetMachineInfoRequest &req, IEspGetMachineInfoResponse &resp);
    bool onGetTargetClusterInfo(IEspContext &context, IEspGetTargetClusterInfoRequest &req, IEspGetTargetClusterInfoResponse &resp);
    bool onGetMachineInfoEx(IEspContext &context, IEspGetMachineInfoRequestEx &req, IEspGetMachineInfoResponseEx &resp);
    bool onGetComponentStatus(IEspContext &context, IEspGetComponentStatusRequest &req, IEspGetComponentStatusResponse &resp);
    bool onUpdateComponentStatus(IEspContext &context, IEspUpdateComponentStatusRequest &req, IEspUpdateComponentStatusResponse &resp);

    bool onGetMetrics(IEspContext &context, IEspMetricsRequest &req, IEspMetricsResponse &resp);
    bool onStartStop( IEspContext &context, IEspStartStopRequest &req,  IEspStartStopResponse &resp);
    bool onStartStopBegin( IEspContext &context, IEspStartStopBeginRequest &req,  IEspStartStopBeginResponse &resp);
    bool onStartStopDone( IEspContext &context, IEspStartStopDoneRequest &req,  IEspStartStopResponse &resp);

    void getRoxieStateInfo(CRoxieStateInfoThreadParam* param);
    void doGetMachineInfo(IEspContext& context, CMachineInfoThreadParam* pReq);
    void doGetMetrics(CMetricsThreadParam* pParam);
    bool doStartStop(IEspContext &context, StringArray& addresses, char* userName, char* password, bool bStop, IEspStartStopResponse &resp);

    IConstEnvironment* getConstEnvironment();

    //Used in StartStop/Rexec
    void getAccountAndPlatformInfo(const char* address, StringBuffer& userId, StringBuffer& password, bool& bLinux);
    IPropertyTree* getComponent(const char* compType, const char* compName);
private:
    void setupLegacyFilters();
    bool isLegacyFilter(const char* processType, const char* dependency);
    bool excludePartition(const char* partition) const;
    void appendProcessInstance(const char* name, const char* directory1, const char* directory2, StringArray& machineInstances, StringArray& directories);
    void getProcesses(IConstEnvironment* constEnv, IPropertyTree* envRoot, const char* processName, const char* processType, const char* directory, CGetMachineInfoData& machineInfoData, bool isThorOrRoxieProcess, BoolHash& uniqueProcesses, BoolHash* uniqueRoxieProcesses = NULL);
    void getThorProcesses(IConstEnvironment* constEnv,  IPropertyTree* cluster, const char* processName, const char* processType, const char* directory, CGetMachineInfoData& machineInfoData, BoolHash& uniqueProcesses);
    const char* getProcessTypeFromMachineType(const char* machineType);
    void readSettingsForTargetClusters(IEspContext& context, StringArray& targetClusters, CGetMachineInfoData& machineInfoData, IPropertyTree* targetClustersOut);
    void readTargetClusterProcesses(IPropertyTree& targetClusters, const char* processType, BoolHash& uniqueProcesses, CGetMachineInfoData& machineInfoData, IPropertyTree* targetClustersOut);
    void setTargetClusterInfo(IPropertyTree* pTargetClusterTree, IArrayOf<IEspMachineInfoEx>& machineArray, IArrayOf<IEspTargetClusterInfo>& targetClusterInfoList);

    void buildPreflightCommand(IEspContext& context, CMachineInfoThreadParam* pParam, StringBuffer& preflightCommand);
    int runCommand(IEspContext& context, const char* sAddress, const char *configAddress, EnvMachineOS os, const char* sCommand, const char* sUserId, const char* sPassword, StringBuffer& sResponse);
    int invokeProgram(const char *command_line, StringBuffer& response);

    void getTimeStamp(char* timeStamp);
    void getProcessDisplayName(const char* processName, StringBuffer& displayName);
    void readALineFromResult(const char *result, const char *start, StringBuffer& value, bool bTrim = true);

    void readMachineInfoRequest(IEspContext& context, bool getProcessorInfo, bool getStorageInfo, bool localFileSystemsOnly, bool getSwInfo, bool applyProcessFilter, StringArray& addresses, const char* addProcessesToFilters, CGetMachineInfoData& machineInfoData);
    void readMachineInfoRequest(IEspContext& context, bool getProcessorInfo, bool getStorageInfo, bool localFileSystemsOnly, bool getSwInfo, bool applyProcessFilter, const char* addProcessesToFilters, StringArray& targetClustersIn, CGetMachineInfoData& machineInfoData, IPropertyTree* targetClustersOut);
    void getMachineInfo(IEspContext& context, bool getRoxieState, CGetMachineInfoData& machineInfoData);
    void getMachineInfo(IEspContext& context, CGetMachineInfoData& machineInfoData);
    void setMachineInfoResponse(IEspContext& context, IEspGetMachineInfoRequest& req, CGetMachineInfoData& machineInfoData, IEspGetMachineInfoResponse& resp);
    void setTargetClusterInfoResponse(IEspContext& context, IEspGetTargetClusterInfoRequest& req, CGetMachineInfoData& machineInfoData, IPropertyTree* targetClusterTree, IEspGetTargetClusterInfoResponse& resp);

    void setProcessRequest(CGetMachineInfoData& machineInfoData, BoolHash& uniqueProcesses, const char* address1, const char* address2, const char* processType, const char* compName,  const char* path, unsigned processNumber = 0);
    void addProcessRequestToMachineInfoData(CGetMachineInfoData& machineInfoData, const char* address1, const char* address2, const char* processType, const char* compName,  const char* path, unsigned processNumber);
    void parseProcessString(StringArray& process, StringBuffer& address1, StringBuffer& address2, StringBuffer& processType, StringBuffer& compName, StringBuffer& path, unsigned& processNumber);
    void parseAddresses(const char *address, StringBuffer& address1, StringBuffer& address2);
    void readPreflightResponse(IEspContext& context, CMachineInfoThreadParam* pParam, const char *response,int error);
    void readStorageData(const char* response, CMachineInfoThreadParam* pParam);
    void readProcessData(const char* response, CMachineInfoThreadParam* pParam);
    void readRunningProcesses(const char* response, CMachineInfoThreadParam* pParam);
    bool readStorageSpace(const char *line, StringBuffer& title, __int64& free, __int64& total, int& percentAvail);
    void addProcessData(CMachineData* machine, const char* processType, const char* compName, const char* path, unsigned processNumber);
    void setMachineInfo(IEspContext& context, CMachineInfoThreadParam* pParam, const char *response, int error);
    void setProcessInfo(IEspContext& context, CMachineInfoThreadParam* pParam, const char* response, int error, CProcessData& process, bool firstProcess, IEspMachineInfoEx* pMachineInfo);
    void setProcessComponent(IEspContext& context, CMachineInfoThreadParam* pParam, CProcessData& process, bool firstProcess, IArrayOf<IEspSWRunInfo>& processArray, IEspComponentInfo* pComponentInfo);
    void enumerateRunningProcesses(CMachineInfoThreadParam* pParam, CProcessData& process, map<string, Linked<IEspSWRunInfo> >* processMap, bool firstProcess);

    //Used in StartStop/Rexec
    void ConvertAddress( const char* originalAddress, StringBuffer& newAddress);
    void updatePathInAddress(const char* address, StringBuffer& addrStr);
    void getRoxieClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress, int& port);
    void doPostProcessing(CFieldInfoMap& myfieldInfoMap, CFieldMap&  myfieldMap);
    void processValue(const char *oid, const char *value, const bool bShow, CFieldInfoMap& myfieldInfoMap, CFieldMap&  myfieldMap);
    void addIpAddressesToBuffer( void** buffer, unsigned& count, const char* address);

    void readRoxieStatus(const Owned<IPropertyTree> controlResp, CIArrayOf<CRoxieStateData>& roxieStates);
    unsigned addRoxieStateHash(const char* hash, StateHashes& stateHashes, unsigned& totalUniqueHashes);
    void updateMajorRoxieStateHash(StateHashes& stateHashes, CIArrayOf<CRoxieStateData>& roxieStates);
    StringBuffer& getAcceptLanguage(IEspContext& context, StringBuffer& acceptLanguage);

    //Still used in StartStop/Rexec, so keep them for now.
    enum OpSysType { OS_Windows, OS_Solaris, OS_Linux };
    StringBuffer m_sTestStr1;
    StringBuffer m_sTestStr2;
    bool m_useDefaultHPCCInit;

    Owned<IEnvironmentFactory>  m_envFactory;
    Owned<IPropertyTree>        m_processFilters;
    Owned<IThreadPool>          m_threadPool;
    int                         m_threadPoolSize;
    int                         m_threadPoolStackSize;
    int                         m_SSHConnectTimeoutSeconds;
    bool                        m_bMonitorDaliFileServer;
    set<string>                 m_excludePartitions;
    set<string>                 m_excludePartitionPatterns;
    StringBuffer                m_machineInfoFile;
    BoolHash                    m_legacyFilters;
    Mutex                       mutex_machine_info_table;
};

//---------------------------------------------------------------------------------------------

class CWsMachineThreadParam : public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    virtual ~CWsMachineThreadParam() {}

    StringBuffer          m_sAddress;
    StringBuffer          m_sSecurityString; 
    StringBuffer          m_sUserName; 
    StringBuffer          m_sPassword;
    Linked<Cws_machineEx> m_pService;

    virtual void doWork() = 0;

protected:

    CWsMachineThreadParam(Cws_machineEx* pService) : m_pService(pService)
    {
    }
    CWsMachineThreadParam( const char* pszAddress,
                          const char* pszSecurityString, Cws_machineEx* pService)
        : m_sAddress(pszAddress), m_sSecurityString(pszSecurityString), m_pService(pService)
    {
    }
    CWsMachineThreadParam( const char* pszAddress,
                          const char* pszUserName, const char* pszPassword, Cws_machineEx* pService)
        : m_sAddress(pszAddress), m_sUserName(pszUserName), m_sPassword(pszPassword), m_pService(pService)
    {
    }
};

//---------------------------------------------------------------------------------------------

//the following class implements a worker thread
//
class CWsMachineThread : public CInterface, 
                         implements IPooledThread
{
public:
    IMPLEMENT_IINTERFACE;

    CWsMachineThread()
    {
    }
    virtual ~CWsMachineThread()
    {
    }

    void init(void *startInfo) 
    {
        m_pParam.setown((CWsMachineThreadParam*)startInfo);
    }
    void main()
    {
        m_pParam->doWork();
        m_pParam.clear();
    }

    bool canReuse()
    {
        return true;
    }
    bool stop()
    {
        return true;
    }
   
private:
    Owned<CWsMachineThreadParam> m_pParam;
};

//---------------------------------------------------------------------------------------------

class CWsMachineThreadFactory : public CInterface, public IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;
    IPooledThread *createNew()
    {
        return new CWsMachineThread();
    }
};

#endif //_ESPWIZ_ws_machine_HPP__

