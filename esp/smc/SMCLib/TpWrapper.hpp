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

// TpWrapper.hpp: interface for the CTpWrapper class.
//
//////////////////////////////////////////////////////////////////////

#ifndef _ESPWIZ_TpWrapper_HPP__
#define _ESPWIZ_TpWrapper_HPP__

#ifdef SMCLIB_EXPORTS
    #define TPWRAPPER_API DECL_EXPORT
#else
    #define TPWRAPPER_API DECL_IMPORT
#endif



#include "jlib.hpp"
#include "mpbase.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "environment.hpp"
#include "ws_topology.hpp"
#include "ws_resources.hpp"
#include <string>
#include <set>
#include "sacmd.hpp"
#include "jsmartsock.ipp"

enum OS_TYPE {
    OS_WINDOWS,
    OS_SOLARIS,
    OS_LINUX
};

using std::set;
using std::string;

#define eqClusters  "TpClusters"
#define eqCluster   "TpCluster"
#define eqComputers "TpComputers"
#define eqComputer  "TpComputer"

#define eqGroups    "TpGroups"
#define eqGroup "TpGroup"
#define eqGroupName  "Name"

#define eqPrefixNode "Node"
#define eqPrefixName "Prefix"

#define eqClusterName  "Name"
#define eqClusterDescription  "Desc"
#define eqClusterPath  "Path"
#define eqClusterType  "Type"
#define eqClusterDataDirectory  "Directory"


#define eqMachineName  "Name"
#define eqMachinePath  "Path"
#define eqMachineAddress  "Netaddress"
#define eqMachineType  "Type"

#define eqMachineUserID     "UserID"
#define eqMachinePassword   "Password"
#define eqMachineDomain     "Domain"
#define eqMachineOS         "OS"

#define eqMachineAvailablability    "Available"

#define eqTHORMACHINES          "THORMACHINES"
#define eqHOLEMACHINES          "HOLEMACHINES"
#define eqROXIEMACHINES       "ROXIEMACHINES"
#define eqMACHINES              "MACHINES"


#define eqDali                  "DaliServerProcess"
#define eqEclServer         "EclServerProcess"
#define eqEclCCServer       "EclCCServerProcess"
#define eqEclScheduler      "EclSchedulerProcess"
#define eqEclAgent          "EclAgentProcess"
#define eqAgentExec         "AgentExecProcess"
#define eqEsp                   "EspProcess"
#define eqDfu                   "DfuServerProcess"
#define eqSashaServer      "SashaServerProcess"
#define eqLdapServer       "LDAPServerProcess"
#define eqFTSlave          "FTSlaveProcess"
#define eqGenesisServer     "GenesisServerProcess"


#define eqDropZone              "DropZone"

#define eqAVAILABLEMACHINES     "AVAILABLEMACHINES"
#define eqRootNode  "ROOT"

#define eqAllClusters "ALLCLUSTERS"
#define eqAllNodes    "ALLNODES"
#define eqAllServices "ALLSERVICES"


#define eqHoleCluster  "HoleCluster"
#define eqThorCluster  "ThorCluster"
#define eqRoxieCluster "RoxieCluster"

#define eqThorMasterProcess     "ThorMasterProcess"
#define eqThorSlaveProcess      "ThorSlaveProcess"
#define eqThorSpareProcess      "ThorSpareProcess"
#define eqHoleSocketProcess     "HoleSocketProcess"
#define eqHoleProcessorProcess  "HoleProcessorProcess"
#define eqHoleControlProcess    "HoleControlProcess"
#define eqHoleCollatorProcess   "HoleCollatorProcess"
#define eqHoleStandbyProcess    "HoleStandbyProcess"
#define eqSparkThorProcess      "SparkThorProcess"
#define eqRoxieServerProcess    "RoxieServerProcess"

#define SDS_LOCK_TIMEOUT 30000

class TPWRAPPER_API CTpWrapper : public CInterface
{

private:
    void setAttPath(StringBuffer& Path,const char* PathToAppend,const char* AttName,const char* AttValue,StringBuffer& returnStr);
    void getAttPath(const char* Path,StringBuffer& returnStr);
    const char* getNodeNameTag(const char* MachineType);
    void fetchInstances(const char* ServiceType, IPropertyTree& service, IArrayOf<IEspTpMachine>& tpMachines);
    bool checkGroupReplicateOutputs(const char* groupName, const char* kind);
    void appendTpDropZone(double clientVersion, IConstEnvironment* constEnv, IConstDropZoneInfo& dropZoneInfo, IArrayOf<IConstTpDropZone>& list);
    void appendTpSparkThor(double clientVersion, IConstEnvironment* constEnv, IConstSparkThorInfo& sparkThorInfo, IArrayOf<IConstTpSparkThor>& list);
    void appendTpMachine(double clientVersion, IConstEnvironment* constEnv, IConstInstanceInfo& instanceInfo, IArrayOf<IConstTpMachine>& machines);
    void getThorMachineList(double clientVersion, const char* clusterName, const char* directory,
        bool slaveNode, IArrayOf<IEspTpMachine>& machineList);
    void appendThorMachineList(double clientVersion, IConstEnvironment* constEnv, INode& node, const char* clusterName,
         const char* machineType, unsigned& processNumber, unsigned channels, const char* directory, IArrayOf<IEspTpMachine>& machineList);
    void getRoxieServices(IPropertyTree* environmentRoot, const char* serviceName, IArrayOf<IConstHPCCService>& services);
    void getRoxieService(IPropertyTree* clusterTree, IArrayOf<IConstHPCCService>& services);
    void getESPServices(IPropertyTree* environmentRoot, const char* serviceType, const char* serviceName, IArrayOf<IConstHPCCService>& services);

#ifndef _CONTAINERIZED
    IPropertyTree* getEnvironment(const char* xpath);
#endif

public:
    CTpWrapper() {};
    virtual ~CTpWrapper() {};
    void getClusterInfo(const char* Cluster,StringBuffer& returnStr);
    void getClusterProcessList(const char* ClusterType, IArrayOf<IEspTpCluster>& clusters, bool ignoreduplicatqueues=false, bool ignoreduplicategroups=false);
    void getHthorClusterList(IArrayOf<IEspTpCluster>& clusterList);
    void getGroupList(double espVersion, const char* kindReq, IArrayOf<IEspTpGroup> &Groups);
    void getCluster(const char* ClusterType,IPropertyTree& returnRoot);
    void getClusterMachineList(double clientVersion, const char* ClusterType,const char* ClusterPath, const char* ClusterDirectory,
                                        IArrayOf<IEspTpMachine> &MachineList, bool& hasThorSpareProcess, const char* ClusterName = NULL);
    void getMachineList(double clientVersion, const char* MachineType, const char* MachinePath, const char* Status,
        const char* Directory, IArrayOf<IEspTpMachine>& MachineList, set<string>* pMachineNames=nullptr);
    void getThorSpareMachineList(double clientVersion, const char* clusterName, const char* directory, IArrayOf<IEspTpMachine>& machineList);
    void getThorSlaveMachineList(double clientVersion, const char* clusterName, const char* directory, IArrayOf<IEspTpMachine>& machineList);
    bool checkMultiSlavesFlag(const char* clusterName);
    void getDropZoneMachineList(double clientVersion, bool ECLWatchVisibleOnly, IArrayOf<IEspTpMachine> &MachineList);
    void setMachineInfo(const char* name,const char* type,IEspTpMachine& machine);
    void getMachineInfo(IEspTpMachine& machineInfo,IPropertyTree& machine,const char* ParentPath,const char* MachineType,const char* nodenametag);
    void getMachineInfo(double clientVersion, const char* name, const char* netAddress, IEspTpMachine& machineInfo);
    void setTpMachine(IConstMachineInfo* machine, IEspTpMachine& tpMachine);
    IEspTpMachine* createTpMachineEx(const char* name, const char* type, IConstMachineInfo* machineInfo);

    void getTpDaliServers(double clientVersion, IArrayOf<IConstTpDali>& list);
    void getTpEclServers(IArrayOf<IConstTpEclServer>& ServiceList, const char* name = NULL);
    void getTpEclCCServers(IArrayOf<IConstTpEclServer>& ServiceList, const char* name = NULL);
    void getTpEclCCServers(IPropertyTree* environmentSoftware, IArrayOf<IConstTpEclServer>& ServiceList, const char* name = NULL);
    void getTpEclAgents(IArrayOf<IConstTpEclAgent>& list, const char* name = NULL);
    void getTpEclSchedulers(IArrayOf<IConstTpEclScheduler>& ServiceList, const char* name = NULL);
    void getTpEspServers(IArrayOf<IConstTpEspServer>& list);
    void getTpDfuServers(IArrayOf<IConstTpDfuServer>& list);
    void getTpSashaServers(IArrayOf<IConstTpSashaServer>& list);
    void getTpLdapServers(IArrayOf<IConstTpLdapServer>& list);
    void getTpDropZones(double clientVersion, const char* name, bool ECLWatchVisibleOnly, IArrayOf<IConstTpDropZone>& list);
    void getTpFTSlaves(IArrayOf<IConstTpFTSlave>& list);
    void getTpGenesisServers(IArrayOf<IConstTpGenesisServer>& list);
    void getTpSparkThors(double clientVersion, const char* name, IArrayOf<IConstTpSparkThor>& list);

    void queryTargetClusters(double version, const char* clusterType, const char* clusterName, IArrayOf<IEspTpTargetCluster>& clusterList);
    void getTargetClusterList(IArrayOf<IEspTpLogicalCluster>& clusters, const char* clusterType = NULL, const char* clusterName = NULL);
    void queryTargetClusterProcess(double version, const char* processName, const char* clusterType, IArrayOf<IConstTpCluster>& list);
    void getServices(double version, const char* serviceType, const char* serviceName, IArrayOf<IConstHPCCService>& list);

};


class CCluster
{
public:
    CCluster(const char* cluster): conn(querySDS().connect(StringBuffer("/Status/Servers/Server[@thorname=\"").append(cluster).append("\"]").str(),myProcessSession(),RTM_SUB,SDS_LOCK_TIMEOUT))
    {
        if (!conn)
            throw MakeStringException(0,"Cannot connect to SDS cluster %s",cluster);
    }

    operator IRemoteConnection* () { return conn.get(); }
    IRemoteConnection* operator->() { return conn.get(); }

private:
    Owned<IRemoteConnection> conn;
};

extern TPWRAPPER_API ISashaCommand* archiveOrRestoreWorkunits(StringArray& wuids, IProperties* params, bool archive, bool dfu);

extern TPWRAPPER_API unsigned getContainerWUClusterInfo(CConstWUClusterInfoArray& clusters);
extern TPWRAPPER_API unsigned getWUClusterInfo(CConstWUClusterInfoArray& clusters);
extern TPWRAPPER_API IConstWUClusterInfo* getWUClusterInfoByName(const char* clustName);

extern TPWRAPPER_API IStringIterator *getContainerTargetClusters(const char* processType, const char* processName);
extern TPWRAPPER_API void initContainerRoxieTargets(MapStringToMyClass<ISmartSocketFactory>& connMap);
extern TPWRAPPER_API unsigned getThorClusterNames(StringArray& targetNames, StringArray& queueNames);
extern TPWRAPPER_API void validateTargetName(const char* target);
extern TPWRAPPER_API bool getSashaService(StringBuffer &serviceAddress, const char *service, bool failIfNotFound);
extern TPWRAPPER_API bool getSashaServiceEP(SocketEndpoint &serviceEndpoint, const char *service, bool failIfNotFound);

extern TPWRAPPER_API StringBuffer & getRoxieDefaultPlane(StringBuffer & plane, const char * roxieName);
extern TPWRAPPER_API StringArray & getRoxieDirectAccessPlanes(StringArray & planes, StringBuffer &defaultPlane, const char * roxieName, bool includeDefaultPlane);

extern TPWRAPPER_API bool validateDataPlaneName(const char *remoteDali, const char * name);
extern TPWRAPPER_API bool matchNetAddressRequest(const char* netAddressReg, bool ipReq, IConstTpMachine& tpMachine);

extern TPWRAPPER_API bool validateDropZonePath(const char* dropZoneName, const char* netAddr, const char* pathToCheck);
extern TPWRAPPER_API SecAccessFlags getDropZoneScopePermissions(IEspContext& context, const char * dropZoneName, const char * dropZonePath, const char * dropZoneHost);

#endif //_ESPWIZ_TpWrapper_HPP__
