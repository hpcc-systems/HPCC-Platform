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

#include "build-config.h"
#include "platform.h"
#include "thirdparty.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"

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
        LOG(MCuserProgress, unknownJob, "Suspending %d",h);
        server.suspend();
    }
    ForEachItemInRev(i,servers)
    {
        IDaliServer &server=servers.item(i);
        LOG(MCuserProgress, unknownJob, "Stopping %d",i);
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

int main(int argc, char* argv[])
{
    rank_t myrank = 0;
    char *server = nullptr;
    int port = 0;
    for (unsigned i=1;i<(unsigned)argc;i++) {
        if (streq(argv[i],"--daemon") || streq(argv[i],"-d")) {
            if (daemon(1,0) || write_pidfile(argv[++i])) {
                perror("Failed to daemonize");
                return EXIT_FAILURE;
            }
        }
        else if (streq(argv[i],"--server") || streq(argv[i],"-s"))
            server = argv[++i];
        else if (streq(argv[i],"--port") || streq(argv[i],"-p"))
            port = atoi(argv[++i]);
        else if (streq(argv[i],"--rank") || streq(argv[i],"-r"))
            myrank = atoi(argv[++i]);
        else {
            usage();
            return EXIT_FAILURE;
        }
    }
    InitModuleObjects();
    NoQuickEditSection x;
    try {

        EnableSEHtoExceptionMapping();
#ifndef __64BIT__
        // Restrict stack sizes on 32-bit systems
        Thread::setDefaultStackSize(0x20000);
#endif
        setAllocHook(true);

        Owned<IFile> sentinelFile = createSentinelTarget();
        removeSentinelFile(sentinelFile);
	
        OwnedIFile confIFile = createIFile(DALICONF);
        if (confIFile->exists())
            serverConfig.setown(createPTreeFromXMLFile(DALICONF));

        ILogMsgHandler * fileMsgHandler;
        {
            Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(serverConfig, "dali");
            lf->setLogDirSubdir("server");//add to tail of config log dir
            lf->setName("DaServer");//override default filename
            fileMsgHandler = lf->beginLogging();
        }

        DBGLOG("Build %s", BUILD_TAG);

        if (serverConfig)
        {
            StringBuffer dataPath;
            if (getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"data","dali",serverConfig->queryProp("@name"),dataPath)) 
                serverConfig->setProp("@dataPath",dataPath.str());
            else
                serverConfig->getProp("@dataPath",dataPath);
            if (dataPath.length()) {
                RemoteFilename rfn;
                rfn.setRemotePath(dataPath);
                if (!rfn.isLocal()) {
                    ERRLOG("if a dataPath is specified, it must be on local machine");
                    return 0;
                }
                addPathSepChar(dataPath);
                serverConfig->setProp("@dataPath", dataPath.str());
                if (dataPath.length())
                    recursiveCreateDirectory(dataPath.str());
            }

            // JCSMORE remoteBackupLocation should not be a property of SDS section really.
            StringBuffer mirrorPath;
            if (!getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"mirror","dali",serverConfig->queryProp("@name"),mirrorPath)) 
                serverConfig->getProp("SDS/@remoteBackupLocation",mirrorPath);

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
                                WARNLOG("Local path used for backup url: %s", mirrorPath.str());
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
        }
        else
            serverConfig.setown(createPTree());

        write_pidfile(serverConfig->queryProp("@name"));
        NamedMutex globalNamedMutex("DASERVER");
        if (!serverConfig->getPropBool("allowMultipleDalis"))
        {
            PROGLOG("Checking for existing daserver instances");
            if (!globalNamedMutex.lockWait(0))
            {
                PrintLog("Another DASERVER process is currently running");
                return 0;
            }
        }

        SocketEndpoint ep;
        SocketEndpointArray epa;
        if (!server) {
            ep.setLocalHost(DALI_SERVER_PORT);
            epa.append(ep);
        }
        else {
                if (!port)
                    ep.set(server,DALI_SERVER_PORT);
                else
                    ep.set(server,port);
                epa.append(ep);
        }

        unsigned short myport = epa.item(myrank).port;
        startMPServer(myport,true);
        setMsgLevel(fileMsgHandler, serverConfig->getPropInt("SDS/@msgLevel", 100));
        startLogMsgChildReceiver(); 
        startLogMsgParentReceiver();

        IGroup *group = createIGroup(epa); 
        initCoven(group,serverConfig);
        group->Release();
        epa.kill();

// Audit logging
        StringBuffer auditDir;
        {
            Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(serverConfig, "dali");
            lf->setLogDirSubdir("audit");//add to tail of config log dir
            lf->setName("DaAudit");//override default filename
            lf->setCreateAliasFile(false);
            lf->setMsgFields(MSGFIELD_timeDate | MSGFIELD_code);
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
        startPerformanceMonitor(serverConfig->getPropInt("Coven/@perfReportDelay", DEFAULT_PERF_REPORT_DELAY)*1000);
        StringBuffer absPath;
        StringBuffer dataPath;
        serverConfig->getProp("@dataPath",dataPath);
        makeAbsolutePath(dataPath.str(), absPath);
        setPerformanceMonitorPrimaryFileSystem(absPath.str());
        if(serverConfig->hasProp("SDS/@remoteBackupLocation"))
        {
            absPath.clear();
            serverConfig->getProp("SDS/@remoteBackupLocation",dataPath.clear());
            makeAbsolutePath(dataPath.str(), absPath);
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
            stopPerformanceMonitor();
            throw;
        }
        try {
#ifndef _NO_LDAP
            setLDAPconnection(createDaliLdapConnection(serverConfig->getPropTree("Coven/ldapSecurity")));
#endif
        }
        catch (IException *e) {
            EXCLOG(e, "LDAP initialization error");
            stopServer();
            stopPerformanceMonitor();
            throw;
        }
        PrintLog("DASERVER[%d] starting - listening to port %d",myrank,queryMyNode()->endpoint().port);
        startMPServer(myport,false);
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
            covenMain();
            removeAbortHandler(actionOnAbort);
        }
        stopLogMsgListener();
        stopServer();
        stopPerformanceMonitor();
    }
    catch (IException *e) {
        EXCLOG(e, "Exception");
    }
    UseSysLogForOperatorMessages(false);
    return 0;
}
