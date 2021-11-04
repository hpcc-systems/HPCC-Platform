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

#include <unordered_map>
#include <string>

#include "platform.h"
#include "thirdparty.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jmetrics.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "mplog.hpp" //MORE: deprecated feature, not used by any new clients, remove once all deployed clients that depend on it are upgraded
#include "rmtfile.hpp"
#include "dacoven.hpp"
#include "dadfs.hpp"
#include "dasess.hpp"
#include "daaudit.hpp"
#include "dasds.hpp"
#include "daclient.hpp"
#include "dasubs.ipp"
#include "danqs.hpp"
#include "dadiags.hpp"

#ifdef _DEBUG
//#define DALI_MIN
#endif

#ifdef DALI_MIN
#define _NO_LDAP
#endif


#include "daserver.hpp"
#ifndef _NO_LDAP
#include "daldap.hpp"
#endif

Owned<IPropertyTree> serverConfig;
static IArrayOf<IDaliServer> servers;
static CriticalSection *stopServerCrit;
MODULE_INIT(INIT_PRIORITY_DALI_DASERVER)
{
    stopServerCrit = new CriticalSection;
    return true;
}
MODULE_EXIT()
{
    servers.kill(); // should already be clear when stopped
    serverConfig.clear();
    delete stopServerCrit;
}

#define DEFAULT_PERF_REPORT_DELAY 60
#define DEFAULT_MOUNT_POINT "/mnt/dalimirror/"

void setMsgLevel(ILogMsgHandler * fileMsgHandler, unsigned level)
{
    ILogMsgFilter *filter = getSwitchLogMsgFilterOwn(getComponentLogMsgFilter(3), getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, level, true), getDefaultLogMsgFilter());
    queryLogMsgManager()->changeMonitorFilter(queryStderrLogMsgHandler(), filter);
    queryLogMsgManager()->changeMonitorFilterOwn(fileMsgHandler, filter);
}

void AddServers(const char *auditdir)
{
    // order significant
    servers.append(*createDaliSessionServer());
    servers.append(*createDaliPublisherServer());
    servers.append(*createDaliSDSServer(serverConfig));
    servers.append(*createDaliNamedQueueServer());
    servers.append(*createDaliDFSServer(serverConfig));
    servers.append(*createDaliAuditServer(auditdir));
    servers.append(*createDaliDiagnosticsServer());
    // add new coven servers here
}

static bool serverStopped = false;
static void stopServer()
{
    CriticalBlock b(*stopServerCrit); // NB: will not protect against abort handler, which will interrupt thread and be on same TID.
    if (serverStopped) return;
    serverStopped = true;
    ForEachItemInRev(h,servers)
    {
        IDaliServer &server=servers.item(h);
        LOG(MCprogress, unknownJob, "Suspending %d",h);
        server.suspend();
    }
    ForEachItemInRev(i,servers)
    {
        IDaliServer &server=servers.item(i);
        LOG(MCprogress, unknownJob, "Stopping %d",i);
        server.stop();
    }
    closeCoven();
    ForEachItemInRev(j,servers)
    {
        servers.remove(j);      // ensure correct order for destruction
    }
    stopLogMsgReceivers(); //MORE: deprecated feature, not used by any new clients, remove once all deployed clients that depend on it are upgraded
    stopMPServer();
}

bool actionOnAbort()
{
    stopServer();
    return true;
}

USE_JLIB_ALLOC_HOOK;

void usage(void)
{
    printf("daserver (option)\n");
    printf("--rank|-r <value>\t: dali ranking value\n");
    printf("--server|-s <value>\t: server ip if not local host\n");
    printf("--port|-p <value>\t: server port only effective if --server set\n");
    printf("--daemon|-d <instanceName>\t: run daemon as instance\n");
}

static IPropertyTree *getSecMgrPluginPropTree(const IPropertyTree *configTree)
{
    Owned<IPropertyTree> foundTree;

#ifdef _CONTAINERIZED
    // TODO
#else
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment", 0, 0, INFINITE);

    if (conn)
    {
        const IPropertyTree *proptree = conn->queryRoot()->queryPropTree("Software/DaliServerProcess[1]");

        if (proptree)
        {
            const char* authMethod = proptree->queryProp("@authMethod");

            if (strisame(authMethod, "secmgrPlugin"))
            {
                const char* authPluginType = proptree->queryProp("@authPluginType");

                if (authPluginType)
                {
                    VStringBuffer xpath("SecurityManagers/SecurityManager[@name='%s']", authPluginType);

                    foundTree.setown(configTree->getPropTree(xpath));

                    if (!foundTree.get())
                    {
                        WARNLOG("secmgPlugin '%s' not defined in configuration", authPluginType);
                    }
                }
            }
        }
    }
#endif

    return foundTree.getClear();
}

/* NB: Ideally this belongs within common/environment,
 * however, that would introduce a circular dependency.
 */
static bool populateAllowListFromEnvironment(IAllowListWriter &writer)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment", 0, 0, INFINITE);
    assertex(conn);
    if (!conn->queryRoot()->hasProp("Software/DaliServerProcess"))
        return false;

    // only ever expecting 1 DaliServerProcess and 1 AllowList
    const IPropertyTree *allowListTree = conn->queryRoot()->queryPropTree("Software/DaliServerProcess[1]/AllowList[1]");
    if (!allowListTree)
    {
        // deprecated, but for backward compatibility..
        allowListTree = conn->queryRoot()->queryPropTree("Software/DaliServerProcess[1]/WhiteList[1]");
    }

    bool enabled = true;
    if (allowListTree)
    {
        enabled = allowListTree->getPropBool("@enabled", true); // on by default
        // Default for now is to allow clients that send no role (legacy) to connect if their IP is in allowlist.
        writer.setAllowAnonRoles(allowListTree->getPropBool("@allowAnonRoles", true));
    }

    std::unordered_map<std::string, std::string> machineMap;

    auto populateMachineMap = [&machineMap](const IPropertyTree &environment)
    {
        Owned<IPropertyTreeIterator> machineIter = environment.getElements("Hardware/Computer");
        ForEach(*machineIter)
        {
            const IPropertyTree &machine = machineIter->query();
            const char *name = machine.queryProp("@name");
            const char *host = machine.queryProp("@netAddress");
            machineMap.insert({name, host});
        }
    };
    auto resolveComputer = [&machineMap](const char *compName, const char *defaultValue, StringBuffer &result) -> const char *
    {
        const auto &it = machineMap.find(compName);
        if (it == machineMap.end())
            return defaultValue;
        IpAddress ip(it->second.c_str());
        if (ip.isNull())
            return defaultValue;
        return ip.getIpText(result);
    };
    auto addRoles = [&writer, &resolveComputer](const IPropertyTree &component, const std::initializer_list<unsigned __int64> &roles)
    {
        Owned<IPropertyTreeIterator> instanceIter = component.getElements("Instance");
        ForEach(*instanceIter)
        {
            const char *compName = instanceIter->query().queryProp("@computer");
            StringBuffer ipSB;
            const char *ip = resolveComputer(compName, component.queryProp("@netAddress"), ipSB);
            if (ip)
            {
                for (const auto &role: roles)
                    writer.add(ip, role);
            }
        }
    };

    populateMachineMap(*conn->queryRoot());
    enum SoftwareComponentType
    {
        RoxieCluster,
        ThorCluster,
        EclAgentProcess,
        DfuServerProcess,
        EclCCServerProcess,
        EspProcess,
        SashaServerProcess,
        EclSchedulerProcess,
        DaliServerProcess,
        BackupNodeProcess,
        EclServerProcess,
        SparkThorProcess,
    };
    std::unordered_map<std::string, SoftwareComponentType> softwareTypeRoleMap = {
            { "RoxieCluster", RoxieCluster },
            { "ThorCluster", ThorCluster },
            { "EclAgentProcess", EclAgentProcess },
            { "DfuServerProcess", DfuServerProcess },
            { "EclCCServerProcess", EclCCServerProcess },
            { "EspProcess", EspProcess },
            { "SashaServerProcess", SashaServerProcess },
            { "EclSchedulerProcess", EclSchedulerProcess },
            { "DaliServerProcess", DaliServerProcess },
            { "BackupNodeProcess", BackupNodeProcess },
            { "EclServerProcess", EclServerProcess },
            { "SparkThorProcess", SparkThorProcess },
    };

    Owned<IPropertyTreeIterator> softwareIter = conn->queryRoot()->getElements("Software/*");
    ForEach(*softwareIter)
    {
        const IPropertyTree &component = softwareIter->query();
        const char *compProcess = component.queryName();
        const auto &it = softwareTypeRoleMap.find(compProcess);
        if (it != softwareTypeRoleMap.end())
        {
            switch (it->second)
            {
                case RoxieCluster:
                {
                    Owned<IPropertyTreeIterator> serverIter = component.getElements("RoxieServerProcess");
                    ForEach(*serverIter)
                    {
                        const IPropertyTree &server = serverIter->query();
                        const char *serverCompName = server.queryProp("@computer");
                        StringBuffer ipSB;
                        const char *ip = resolveComputer(serverCompName, server.queryProp("@netAddress"), ipSB);
                        if (ip)
                            writer.add(ip, DCR_Roxie);
                    }
                    break;
                }
                case ThorCluster:
                {
                    const char *masterCompName = component.queryProp("ThorMasterProcess/@computer");
                    StringBuffer ipSB;
                    const char *ip = resolveComputer(masterCompName, nullptr, ipSB);
                    if (ip)
                    {
                        writer.add(ip, DCR_ThorMaster);
                        writer.add(ip, DCR_DaliAdmin);
                    }

                    // NB: slaves are currently seen as foreign clients and are only used by Std.File.GetUniqueInteger (which calls Dali v. occassionally)
                    Owned<IPropertyTreeIterator> slaveIter = component.getElements("ThorSlaveProcess");
                    ForEach(*slaveIter)
                    {
                        const char *slaveCompName = slaveIter->query().queryProp("@computer");
                        const char *ip = resolveComputer(slaveCompName, nullptr, ipSB.clear());
                        if (ip)
                            writer.add(ip, DCR_ThorSlave);
                    }
                    break;
                }
                case EclAgentProcess:
                    addRoles(component, { DCR_EclAgent, DCR_AgentExec });
                    break;
                case DfuServerProcess:
                    addRoles(component, { DCR_DfuServer });
                    break;
                case EclCCServerProcess:
                    addRoles(component, { DCR_EclCCServer, DCR_EclCC });
                    break;
                case EclServerProcess:
                    addRoles(component, { DCR_EclServer, DCR_EclCC });
                    break;
                case EspProcess:
                    addRoles(component, { DCR_EspServer });
                    break;
                case SashaServerProcess:
                    addRoles(component, { DCR_SashaServer, DCR_XRef });
                    break;
                case EclSchedulerProcess:
                    addRoles(component, { DCR_EclScheduler });
                    break;
                case BackupNodeProcess:
                    addRoles(component, { DCR_BackupGen, DCR_DaliAdmin });
                    break;
                case DaliServerProcess:
                    addRoles(component, { DCR_DaliServer, DCR_DaliDiag, DCR_SwapNode, DCR_UpdateEnv, DCR_DaliAdmin, DCR_TreeView, DCR_Testing, DCR_DaFsControl, DCR_XRef, DCR_Config, DCR_ScheduleAdmin, DCR_Monitoring, DCR_DaliStop });
                    break;
                case SparkThorProcess:
                    addRoles(component, { DCR_DaliAdmin });
                    break;
            }
        }
    }

    if (allowListTree)
    {
        Owned<IPropertyTreeIterator> allowListIter = allowListTree->getElements("Entry");
        ForEach(*allowListIter)
        {
            const IPropertyTree &entry = allowListIter->query();
            StringArray hosts, roles;
            hosts.appendListUniq(entry.queryProp("@hosts"), ",");
            roles.appendListUniq(entry.queryProp("@roles"), ",");
            ForEachItemIn(h, hosts)
            {
                IpAddress ip(hosts.item(h));
                if (!ip.isNull())
                {
                    StringBuffer ipStr;
                    ip.getIpText(ipStr);
                    ForEachItemIn(r, roles)
                    {
                        const char *roleStr = roles.item(r);

                        char *endPtr;
                        long numericRole = strtol(roleStr, &endPtr, 10);
                        if (endPtr != roleStr && (numericRole>0 && numericRole<DCR_Max)) // in case legacy role needs adding
                            writer.add(ipStr.str(), numericRole);
                        else
                            writer.add(ipStr.str(), queryRole(roleStr));
                    }
                }
            }
        }
    }
    return enabled;
}

static StringBuffer &formatDaliRole(StringBuffer &out, unsigned __int64 role)
{
    return out.append(queryRoleName((DaliClientRole)role));
}

#ifdef _CONTAINERIZED
static IPropertyTree * getContainerLDAPConfiguration(const IPropertyTree *appConfig)
{
    const char * authMethod = appConfig->queryProp("@auth");
    if (streq(authMethod, "none"))
    {
        WARNLOG("ECLWatch is unsafe, no security manager specified in configuration (auth: none)");
        return nullptr; //no security manager
    }

    if (!streq(authMethod, "ldap"))
    {
        throw makeStringExceptionV(-1, "Unrecognized auth method specified, (auth: %s)", authMethod);
    }

    //Get default LDAP attributes from ldap.yaml
    StringBuffer ldapDefaultsFile(hpccBuildInfo.componentDir);
    char sepchar = getPathSepChar(ldapDefaultsFile.str());
    addPathSepChar(ldapDefaultsFile, sepchar).append("applications").append(sepchar).append("common").append(sepchar).append("ldap").append(sepchar).append("ldap.yaml");
    Owned<IPropertyTree> defaults;
    if (!checkFileExists(ldapDefaultsFile))
    {
        throw makeStringExceptionV(-1, "Unable to locate LDAP defaults file '%s'", ldapDefaultsFile.str());
    }
    defaults.setown(createPTreeFromYAMLFile(ldapDefaultsFile.str()));

    //Build merged configuration
    Owned<IPropertyTree> mergedConfig = defaults->getPropTree("ldap");
    mergeConfiguration(*mergedConfig, *appConfig->queryPropTree("ldap"));

    return LINK(mergedConfig);
}
#endif

static constexpr const char * defaultYaml = R"!!(
version: 1.0
dali:
  name: dali
  dataPath: "/var/lib/HPCCSystems/dalistorage"
)!!";

//
// Initialize metrics
static void initializeMetrics(IPropertyTree *pConfig)
{
    //
    // Initialize metrics if present
    Owned<IPropertyTree> pMetricsTree = pConfig->getPropTree("metrics");
    if (pMetricsTree != nullptr)
    {
        PROGLOG("Metrics initializing...");
        hpccMetrics::MetricsManager &metricsManager = hpccMetrics::queryMetricsManager();
        metricsManager.init(pMetricsTree);
        metricsManager.startCollecting();
    }
}

int main(int argc, const char* argv[])
{
    rank_t myrank = 0;
    const char *server = nullptr;
    int port = 0;

    InitModuleObjects();
    NoQuickEditSection x;
    try
    {
        EnableSEHtoExceptionMapping();
#ifndef __64BIT__
        // Restrict stack sizes on 32-bit systems
        Thread::setDefaultStackSize(0x20000);
#endif
        setAllocHook(true);

        serverConfig.setown(loadConfiguration(defaultYaml, argv, "dali", "DALI", DALICONF, nullptr));
        Owned<IFile> sentinelFile = createSentinelTarget();
        removeSentinelFile(sentinelFile);
#ifndef _CONTAINERIZED
        if (!checkCreateDaemon(argc, argv))
            return EXIT_FAILURE;

        for (unsigned i=1;i<(unsigned)argc;i++) {
            if (streq(argv[i],"--daemon") || streq(argv[i],"-d")) {
                i++; // consumed within checkCreateDaemon(), bump up here
            }
            else if (streq(argv[i],"--server") || streq(argv[i],"-s"))
                server = argv[++i];
            else if (streq(argv[i],"--port") || streq(argv[i],"-p"))
                port = atoi(argv[++i]);
            else if (streq(argv[i],"--rank") || streq(argv[i],"-r"))
                myrank = atoi(argv[++i]);
            else if (!startsWith(argv[i],"--config"))
            {
                usage();
                return EXIT_FAILURE;
            }
        }
#endif

#ifndef _CONTAINERIZED
        ILogMsgHandler * fileMsgHandler;
        {
            Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(serverConfig, "dali");
            lf->setLogDirSubdir("server");//add to tail of config log dir
            lf->setName("DaServer");//override default filename
            fileMsgHandler = lf->beginLogging();
        }
#else
        setupContainerizedLogMsgHandler();
#endif
        PROGLOG("Build %s", hpccBuildInfo.buildTag);

        StringBuffer dataPath;
        StringBuffer mirrorPath;
#ifdef _CONTAINERIZED
        serverConfig->getProp("@dataPath", dataPath);
        /* NB: mirror settings are unlikely to be used in a container setup
           If detected, set in to legacy location under SDS/ for backward compatibility */
        serverConfig->getProp("@remoteBackupLocation", mirrorPath);
        if (mirrorPath.length())
            serverConfig->setProp("SDS/@remoteBackupLocation", mirrorPath);
#else
        if (getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"dali","dali",serverConfig->queryProp("@name"),dataPath))
            serverConfig->setProp("@dataPath",dataPath.str());
        else if (getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"data","dali",serverConfig->queryProp("@name"),dataPath))
            serverConfig->setProp("@dataPath",dataPath.str());
        else
            serverConfig->getProp("@dataPath",dataPath);
        if (dataPath.length())
        {
            RemoteFilename rfn;
            rfn.setRemotePath(dataPath);
            if (!rfn.isLocal())
            {
                OERRLOG("if a dataPath is specified, it must be on local machine");
                return 0;
            }
        }
        // JCSMORE remoteBackupLocation should not be a property of SDS section really.
        if (!getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"mirror","dali",serverConfig->queryProp("@name"),mirrorPath))
            serverConfig->getProp("SDS/@remoteBackupLocation",mirrorPath);

#endif
        if (dataPath.length())
        {
            addPathSepChar(dataPath); // ensures trailing path separator
            serverConfig->setProp("@dataPath", dataPath.str());
            recursiveCreateDirectory(dataPath.str());
        }

        if (mirrorPath.length())
        {
            try
            {
                addPathSepChar(mirrorPath);
                try
                {
                    StringBuffer backupURL;
                    if (mirrorPath.length()<=2 || !isPathSepChar(mirrorPath.charAt(0)) || !isPathSepChar(mirrorPath.charAt(1)))
                    { // local machine path, convert to url
                        const char *backupnode = serverConfig->queryProp("SDS/@backupComputer");
                        RemoteFilename rfn;
                        if (backupnode&&*backupnode) {
                            SocketEndpoint ep(backupnode);
                            rfn.setPath(ep,mirrorPath.str());
                        }
                        else {
                            OWARNLOG("Local path used for backup url: %s", mirrorPath.str());
                            rfn.setLocalPath(mirrorPath.str());
                        }
                        rfn.getRemotePath(backupURL);
                        mirrorPath.clear().append(backupURL);
                    }
                    else
                        backupURL.append(mirrorPath);
                    recursiveCreateDirectory(backupURL.str());
                    addPathSepChar(backupURL);
                    serverConfig->setProp("SDS/@remoteBackupLocation", backupURL.str());
                    PROGLOG("Backup URL = %s", backupURL.str());
                }
                catch (IException *e)
                {
                    EXCLOG(e, "Failed to create remote backup directory, disabling backups", MSGCLS_warning);
                    serverConfig->removeProp("SDS/@remoteBackupLocation");
                    mirrorPath.clear();
                    e->Release();
                }

                if (mirrorPath.length())
                {
                    PROGLOG("Checking backup location: %s", mirrorPath.str());
#if defined(__linux__)
                    if (serverConfig->getPropBool("@useNFSBackupMount", false))
                    {
                        RemoteFilename rfn;
                        if (mirrorPath.length()<=2 || !isPathSepChar(mirrorPath.charAt(0)) || !isPathSepChar(mirrorPath.charAt(1)))
                            rfn.setLocalPath(mirrorPath.str());
                        else
                            rfn.setRemotePath(mirrorPath.str());

                        if (!rfn.getPort() && !rfn.isLocal())
                        {
                            StringBuffer mountPoint;
                            serverConfig->getProp("@mountPoint", mountPoint);
                            if (!mountPoint.length())
                                mountPoint.append(DEFAULT_MOUNT_POINT);
                            addPathSepChar(mountPoint);
                            recursiveCreateDirectory(mountPoint.str());
                            PROGLOG("Mounting url \"%s\" on mount point \"%s\"", mirrorPath.str(), mountPoint.str());
                            bool ub = unmountDrive(mountPoint.str());
                            if (!mountDrive(mountPoint.str(), rfn))
                            {
                                if (!ub)
                                    PROGLOG("Failed to remount mount point \"%s\", possibly in use?", mountPoint.str());
                                else
                                    PROGLOG("Failed to mount \"%s\"", mountPoint.str());
                                return 0;
                            }
                            else
                                serverConfig->setProp("SDS/@remoteBackupLocation", mountPoint.str());
                            mirrorPath.clear().append(mountPoint);
                        }
                    }
#endif
                    StringBuffer backupCheck(dataPath);
                    backupCheck.append("bakchk.").append((unsigned)GetCurrentProcessId());
                    OwnedIFile iFileDataDir = createIFile(backupCheck.str());
                    OwnedIFileIO iFileIO = iFileDataDir->open(IFOcreate);
                    iFileIO.clear();
                    try
                    {
                        backupCheck.clear().append(mirrorPath).append("bakchk.").append((unsigned)GetCurrentProcessId());
                        OwnedIFile iFileBackup = createIFile(backupCheck.str());
                        if (iFileBackup->exists())
                        {
                            PROGLOG("remoteBackupLocation and dali data path point to same location! : %s", mirrorPath.str());
                            iFileDataDir->remove();
                            return 0;
                        }
                    }
                    catch (IException *)
                    {
                        try { iFileDataDir->remove(); } catch (IException *e) { EXCLOG(e, NULL); e->Release(); }
                        throw;
                    }
                    iFileDataDir->remove();

#ifndef _CONTAINERIZED
                    StringBuffer dest(mirrorPath.str());
                    dest.append(DALICONF);
                    copyFile(dest.str(), DALICONF);
                    StringBuffer covenPath(dataPath);
                    OwnedIFile ifile = createIFile(covenPath.append(DALICOVEN).str());
                    if (ifile->exists())
                    {
                        dest.clear().append(mirrorPath.str()).append(DALICOVEN);
                        copyFile(dest.str(), covenPath.str());
                    }
#endif
                }
                if (serverConfig->getPropBool("@daliServixCaching", true))
                    setDaliServixSocketCaching(true);
            }
            catch (IException *e)
            {
                StringBuffer s("Failure whilst preparing dali backup location: ");
                LOG(MCoperatorError, unknownJob, e, s.append(mirrorPath).append(". Backup disabled").str());
                serverConfig->removeProp("SDS/@remoteBackupLocation");
                e->Release();
            }
        }

#ifndef _CONTAINERIZED
        write_pidfile(serverConfig->queryProp("@name"));
        NamedMutex globalNamedMutex("DASERVER");
        if (!serverConfig->getPropBool("allowMultipleDalis"))
        {
            PROGLOG("Checking for existing daserver instances");
            if (!globalNamedMutex.lockWait(0))
            {
                OWARNLOG("Another DASERVER process is currently running");
                return 0;
            }
        }
#endif
        SocketEndpoint ep;
        SocketEndpointArray epa;
        if (!server)
        {
            ep.setLocalHost(DALI_SERVER_PORT);
            epa.append(ep);
        }
        else
        {
            if (!port)
                ep.set(server,DALI_SERVER_PORT);
            else
                ep.set(server,port);
            epa.append(ep);
        }

        unsigned short myport = epa.item(myrank).port;
        startMPServer(DCR_DaliServer, myport, true, true);
#ifndef _CONTAINERIZED
        Owned<IMPServer> mpServer = getMPServer();
        Owned<IAllowListHandler> allowListHandler = createAllowListHandler(populateAllowListFromEnvironment, formatDaliRole);
        mpServer->installAllowListCallback(allowListHandler);
        setMsgLevel(fileMsgHandler, serverConfig->getPropInt("SDS/@msgLevel", DebugMsgThreshold));
#endif
        startLogMsgChildReceiver();
        startLogMsgParentReceiver();

        IGroup *group = createIGroup(epa);
        initCoven(group,serverConfig);
        group->Release();
        epa.kill();

// Audit logging
        StringBuffer auditDir;
        {
            //MORE: Does this need to change in CONTAINERIZED mode?
            Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(serverConfig, "dali");
            lf->setLogDirSubdir("audit");//add to tail of config log dir
            lf->setName("DaAudit");//override default filename
            lf->setCreateAliasFile(false);
            lf->setMsgFields(MSGFIELD_timeDate | MSGFIELD_code | MSGFIELD_job);
            lf->setMsgAudiences(MSGAUD_audit);
            lf->setMaxDetail(TopDetail);
            lf->beginLogging();
            auditDir.set(lf->queryLogDir());
        }

// SNMP logging
        bool enableSNMP = serverConfig->getPropBool("SDS/@enableSNMP");
        if (serverConfig->getPropBool("SDS/@enableSysLog",true))
            UseSysLogForOperatorMessages();
        AddServers(auditDir.str());
        addAbortHandler(actionOnAbort);

#ifndef _CONTAINERIZED
        startPerformanceMonitor(serverConfig->getPropInt("Coven/@perfReportDelay", DEFAULT_PERF_REPORT_DELAY)*1000);
#endif
        StringBuffer absPath;
        makeAbsolutePath(dataPath.str(), absPath);
        setPerformanceMonitorPrimaryFileSystem(absPath.str());
        if (mirrorPath.length())
        {
            absPath.clear();
            makeAbsolutePath(mirrorPath.str(), absPath);
            setPerformanceMonitorSecondaryFileSystem(absPath.str());
        }

        try
        {
            ForEachItemIn(i1,servers)
            {
                IDaliServer &server=servers.item(i1);
                server.start();
            }
        }
        catch (IException *e)
        {
            EXCLOG(e, "Failed whilst starting servers");
            stopServer();
#ifndef _CONTAINERIZED
            stopPerformanceMonitor();
#endif
            throw;
        }
        try {
#ifndef _NO_LDAP
            Owned<IPropertyTree> secMgrPropTree = getSecMgrPluginPropTree(serverConfig);

            if (secMgrPropTree.get())
            {
                setLDAPconnection(createDaliSecMgrPluginConnection(secMgrPropTree));
            }
            else
            {
#ifdef _CONTAINERIZED
                setLDAPconnection(createDaliLdapConnection(getContainerLDAPConfiguration(serverConfig)));//container configuration
#else
                setLDAPconnection(createDaliLdapConnection(serverConfig->getPropTree("Coven/ldapSecurity")));//legacy configuration
#endif
            }
#endif
        }
        catch (IException *e) {
            EXCLOG(e, "LDAP initialization error");
            stopServer();
#ifndef _CONTAINERIZED
            stopPerformanceMonitor();
#endif
            throw;
        }
        PROGLOG("DASERVER[%d] starting - listening to port %d",myrank,queryMyNode()->endpoint().port);
        startMPServer(DCR_DaliServer, myport, false, true);
        bool ok = true;
        ForEachItemIn(i2,servers)
        {
            IDaliServer &server=servers.item(i2);
            try {
                server.ready();
            }
            catch (IException *e) {
                EXCLOG(e,"Exception starting Dali Server");
                ok = false;
            }

        }
        if (ok) {
            writeSentinelFile(sentinelFile);
            initializeMetrics(serverConfig);
            covenMain();
            removeAbortHandler(actionOnAbort);
        }
        stopServer();
#ifndef _CONTAINERIZED
        stopPerformanceMonitor();
#endif
    }
    catch (IException *e) {
        EXCLOG(e, "Exception");
    }
    UseSysLogForOperatorMessages(false);
    return 0;
}
