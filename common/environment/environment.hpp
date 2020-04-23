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

#ifndef ENVIRONMENT_INCL
#define ENVIRONMENT_INCL

#include "jiface.hpp"

#include "dasubs.hpp"

#ifdef ENVIRONMENT_EXPORTS
    #define ENVIRONMENT_API DECL_EXPORT
#else
    #define ENVIRONMENT_API DECL_IMPORT
#endif

interface IPropertyTree;   // Forward reference
interface IEnvironment;    // Forward reference
interface ISDSSubscription;// Forward reference

interface IConstEnvBase : extends IInterface
{
    virtual IStringVal & getXML(IStringVal & str) const = 0;
    virtual IStringVal & getName(IStringVal & str) const = 0;
    virtual IPropertyTree & getPTree() const = 0;
};


interface IConstDomainInfo : extends IConstEnvBase
{
    virtual void getAccountInfo(IStringVal & name, IStringVal & pw) const = 0;
    virtual void getSnmpSecurityString(IStringVal & securityString) const = 0;
    virtual void getSSHAccountInfo(IStringVal & name, IStringVal & sshKeyFile, IStringVal& sshKeyPassphrase) const = 0;
};


enum EnvMachineState
{
    MachineStateAvailable = 0,
    MachineStateUnavailable = 1,
    MachineStateUnknown = 2
};



enum EnvMachineOS
{
    MachineOsW2K = 0,
    MachineOsSolaris = 1,
    MachineOsLinux = 2,
    MachineOsUnknown = 3,
    MachineOsSize = 4
};



interface IConstComputerTypeInfo : extends IConstEnvBase
{
    virtual EnvMachineOS getOS() const = 0;
    virtual unsigned getNicSpeedMbitSec() const = 0;
};



interface IConstMachineInfo : extends IConstEnvBase
{
    virtual IConstDomainInfo * getDomain() const = 0;
    virtual IStringVal & getNetAddress(IStringVal & str) const = 0;
    virtual unsigned getNicSpeedMbitSec() const = 0;
    virtual IStringVal & getDescription(IStringVal & str) const = 0;
    virtual EnvMachineOS getOS() const = 0;
    virtual EnvMachineState getState() const = 0;
};

interface  IConstMachineInfoIterator : extends IIteratorOf<IConstMachineInfo>
{
    virtual unsigned count() const = 0;
};


interface IConstInstanceInfo : extends IConstEnvBase
{
    virtual IConstMachineInfo * getMachine() const = 0;
    virtual IStringVal & getEndPoint(IStringVal & str) const = 0;
    virtual unsigned getPort() const = 0;
    virtual IStringVal & getDirectory(IStringVal & str) const = 0;
    virtual IStringVal & getExecutableDirectory(IStringVal & str) const = 0;
    virtual bool getRunInfo(IStringVal & progpath, IStringVal & workdir, const char * defaultprogname) const = 0;
};

interface IConstDropZoneServerInfo : extends IConstEnvBase
{
    virtual StringBuffer & getName(StringBuffer & name) const = 0;
    virtual StringBuffer & getServer(StringBuffer & server) const = 0;
};

interface IConstDropZoneServerInfoIterator : extends IIteratorOf<IConstDropZoneServerInfo>
{
    virtual unsigned count() const = 0;
};

interface IConstDropZoneInfo : extends IConstEnvBase
{
    virtual IStringVal & getComputerName(IStringVal & str) const = 0;
    virtual IStringVal & getDescription(IStringVal & str) const = 0;
    virtual IStringVal & getDirectory(IStringVal & str) const = 0;
    virtual IStringVal & getUMask(IStringVal & str) const = 0;
    virtual bool isECLWatchVisible() const = 0;
    virtual IConstDropZoneServerInfoIterator * getServers() const = 0;
};

interface  IConstDropZoneInfoIterator : extends IIteratorOf<IConstDropZoneInfo>
{
    virtual unsigned count() const = 0;
};

interface IConstDfuQueueInfo : extends IConstEnvBase
{
    virtual IStringVal & getDfuQueueName(IStringVal & str) const = 0;
};

interface IConstDfuQueueInfoIterator : extends IIteratorOf<IConstDfuQueueInfo>
{
    virtual unsigned count() const = 0;
};

interface IConstDaFileSrvInfo : extends IConstEnvBase
{
    virtual const char *getName() const = 0;
    virtual unsigned getPort() const = 0;
    virtual bool getSecure() const = 0;
};

interface IConstInstanceInfoIterator : extends IIteratorOf<IConstInstanceInfo>
{
    virtual unsigned count() const = 0;
};

interface IConstSparkThorInfo : extends IConstEnvBase
{
    virtual IStringVal & getName(IStringVal & str) const = 0;
    virtual IStringVal & getBuild(IStringVal & str) const = 0;
    virtual IStringVal & getThorClusterName(IStringVal & str) const = 0;
    virtual unsigned getSparkExecutorCores() const = 0;
    virtual unsigned __int64 getSparkExecutorMemory() const = 0;
    virtual unsigned getSparkMasterPort() const = 0;
    virtual unsigned getSparkMasterWebUIPort() const = 0;
    virtual unsigned getSparkWorkerCores() const = 0;
    virtual unsigned __int64 getSparkWorkerMemory() const = 0;
    virtual unsigned getSparkWorkerPort() const = 0;
    virtual IConstInstanceInfoIterator * getInstanceIterator() const = 0;
};

interface IConstSparkThorInfoIterator : extends IIteratorOf<IConstSparkThorInfo>
{
    virtual unsigned count() const = 0;
};

interface IConstEnvironment : extends IConstEnvBase
{
    virtual IConstDomainInfo * getDomain(const char * name) const = 0;
    virtual IConstMachineInfo * getMachine(const char * name) const = 0;
    virtual IConstMachineInfo * getMachineByAddress(const char * hostOrIP) const = 0;
    virtual IConstMachineInfo * getMachineForLocalHost() const = 0;
    virtual IConstDropZoneInfo * getDropZone(const char * name) const = 0;
    virtual IConstInstanceInfo * getInstance(const char * type, const char * version, const char * domain) const = 0;
    virtual IConstComputerTypeInfo * getComputerType(const char * name) const = 0;
    virtual bool getRunInfo(IStringVal & path, IStringVal & dir, const char * type, const char * version, const char * machineaddr, const char * defaultprogname) const = 0;
    virtual IEnvironment & lock() const = 0;
    virtual bool isConstEnvironment() const = 0;
    virtual void clearCache() = 0;

    virtual IConstMachineInfoIterator * getMachineIterator() const = 0;
    virtual IConstDropZoneInfoIterator * getDropZoneIteratorByAddress(const char * address) const = 0;
    // returns a drop zone that is defined on IP with the shortest path that's a parent of targetPath
    virtual IConstDropZoneInfo * getDropZoneByAddressPath(const char * netaddress, const char *targetPath) const = 0;
    virtual IConstDropZoneInfoIterator * getDropZoneIterator() const = 0;
    virtual bool isDropZoneRestrictionEnabled() const = 0;
    virtual const char *getClusterGroupKeyPairName(const char *cluster) const = 0;
    virtual const char *getPublicKeyPath(const char *keyPairName) const = 0;
    virtual const char *getPrivateKeyPath(const char *keyPairName) const = 0;
    virtual const char *getFileAccessUrl() const = 0;
    virtual IConstDaFileSrvInfo *getDaFileSrvGroupInfo(const char *name) const = 0;
    virtual IConstSparkThorInfo *getSparkThor(const char *name) const = 0;
    virtual IConstSparkThorInfoIterator *getSparkThorIterator() const = 0;

    virtual IConstDfuQueueInfoIterator * getDfuQueueIterator() const = 0;
    virtual bool isValidDfuQueueName(const char * queueName) const = 0;
};


interface IEnvironment : extends IConstEnvironment
{
    virtual void commit() = 0;
    virtual void rollback() = 0;
    virtual void setXML(const char * (null)) = 0;
    virtual void preload() = 0;
};


interface IEnvironmentFactory : extends IInterface
{
    virtual IConstEnvironment * openEnvironment() = 0;
    virtual IEnvironment * updateEnvironment() = 0;
    virtual IEnvironment * loadLocalEnvironmentFile(const char * filename) = 0;
    virtual IEnvironment * loadLocalEnvironment(const char * xml) = 0;
    virtual SubscriptionId subscribe(ISDSSubscription * pSubHandler) = 0;
    virtual void unsubscribe(SubscriptionId id) = 0;
    virtual void validateCache() = 0;
};

class StringBuffer;
extern "C" ENVIRONMENT_API IEnvironmentFactory * getEnvironmentFactory(bool update);
extern "C" ENVIRONMENT_API void closeEnvironment();

extern ENVIRONMENT_API unsigned getAccessibleServiceURLList(const char *serviceType, std::vector<std::string> &list);

//------------------- Moved from workunit.hpp -------------

// This enumeration is currently duplicated in workunit.hpp and environment.hpp.  They must stay in sync.
#ifndef ENGINE_CLUSTER_TYPE
#define ENGINE_CLUSTER_TYPE
enum ClusterType { NoCluster, HThorCluster, RoxieCluster, ThorLCRCluster };
#endif

interface IStringIterator;

//! IClusterInfo
interface IConstWUClusterInfo : extends IInterface
{
    virtual IStringVal & getName(IStringVal & str) const = 0;
    virtual IStringVal & getScope(IStringVal & str) const = 0;
    virtual IStringVal & getThorQueue(IStringVal & str) const = 0;
    virtual unsigned getSize() const = 0;
    virtual unsigned getNumberOfSlaveLogs() const = 0;
    virtual ClusterType getPlatform() const = 0;
    virtual IStringVal & getAgentQueue(IStringVal & str) const = 0;
    virtual IStringVal & getAgentName(IStringVal & str) const = 0;
    virtual IStringVal & getECLSchedulerName(IStringVal & str) const = 0;
    virtual const StringArray & getECLServerNames() const = 0;
    virtual bool isLegacyEclServer() const = 0;
    virtual IStringVal & getServerQueue(IStringVal & str) const = 0;
    virtual IStringVal & getRoxieProcess(IStringVal & str) const = 0;
    virtual const StringArray & getThorProcesses() const = 0;
    virtual const StringArray & getPrimaryThorProcesses() const = 0;
    virtual const SocketEndpointArray & getRoxieServers() const = 0;
    virtual const char *getLdapUser() const = 0;
    virtual const char *getLdapPassword() const = 0;
    virtual unsigned getRoxieRedundancy() const = 0;
    virtual unsigned getChannelsPerNode() const = 0;
    virtual int getRoxieReplicateOffset() const = 0;
    virtual const char *getAlias() const = 0;
};

extern ENVIRONMENT_API void getDFUServerQueueNames(StringArray &ret, const char *process);
extern ENVIRONMENT_API IStringVal &getEclCCServerQueueNames(IStringVal &ret, const char *process);
extern ENVIRONMENT_API IStringVal &getEclServerQueueNames(IStringVal &ret, const char *process);
extern ENVIRONMENT_API IStringVal &getEclSchedulerQueueNames(IStringVal &ret, const char *process);
extern ENVIRONMENT_API IStringVal &getAgentQueueNames(IStringVal &ret, const char *process);
extern ENVIRONMENT_API IStringVal &getRoxieQueueNames(IStringVal &ret, const char *process);
extern ENVIRONMENT_API IStringVal &getThorQueueNames(IStringVal &ret, const char *process);
extern ENVIRONMENT_API ClusterType getClusterTypeByClusterName(const char *cluster);
extern ENVIRONMENT_API StringBuffer &getClusterGroupName(StringBuffer &ret, const char *cluster);
extern ENVIRONMENT_API StringBuffer &getClusterThorGroupName(StringBuffer &ret, const char *cluster);
extern ENVIRONMENT_API IStringIterator *getTargetClusters(const char *processType, const char *processName);
extern ENVIRONMENT_API bool validateTargetClusterName(const char *clustname);
extern ENVIRONMENT_API IConstWUClusterInfo* getTargetClusterInfo(const char *clustname);
typedef IArrayOf<IConstWUClusterInfo> CConstWUClusterInfoArray;
extern ENVIRONMENT_API unsigned getEnvironmentClusterInfo(CConstWUClusterInfoArray &clusters);
extern ENVIRONMENT_API unsigned getEnvironmentClusterInfo(IPropertyTree* environmentRoot, CConstWUClusterInfoArray &clusters);
extern ENVIRONMENT_API void getRoxieProcessServers(const char *process, SocketEndpointArray &servers);
extern ENVIRONMENT_API bool isProcessCluster(const char *remoteDali, const char *process);
extern ENVIRONMENT_API bool isProcessCluster(const char *process);
extern ENVIRONMENT_API unsigned getEnvironmentThorClusterNames(StringArray &thorNames, StringArray &groupNames, StringArray &targetNames, StringArray &queueNames);

#endif // _ENVIRONMENT_INCL
//end
