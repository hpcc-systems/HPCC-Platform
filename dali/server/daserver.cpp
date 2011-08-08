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

#include "build-config.h"
#include "platform.h"
#include "thirdparty.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jfile.hpp"

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

ILogMsgHandler * fileMsgHandler;

#define DEFAULT_PERF_REPORT_DELAY 60
#define DEFAULT_MOUNT_POINT "/mnt/dalimirror/"

void setMsgLevel(unsigned level)
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
    servers.append(*createDaliDFSServer());
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

#ifdef _WIN32
class CReleaseMutex : public CInterface, public Mutex
{
public:
    CReleaseMutex(const char *name) : Mutex(name) { }
    ~CReleaseMutex() { if (owner) unlock(); }
};
#endif

USE_JLIB_ALLOC_HOOK;

int main(int argc, char* argv[])
{
    InitModuleObjects();
    NoQuickEditSection x;
    try {

        EnableSEHtoExceptionMapping();
#ifndef __64BIT__
        Thread::setDefaultStackSize(0x20000);
#endif
        setAllocHook(true);

        rank_t myrank = 0;
        if (argc==2) {
            printf("daserver <myrank> <server_ip:port>* \n");
            return 0;
        }
        else if (argc>=3) {
            myrank = atoi(argv[1]);
            if (myrank>=(unsigned)(argc-2)) {
                printf("incorrect rank\n");
                return 0;
            }
        }

        Owned<IFile> sentinelFile = createSentinelTarget(argv[0], "daserver");
        // We remove any existing sentinel until we have validated that we can successfully start (i.e. all options are valid...)
        sentinelFile->remove();

        OwnedIFile confIFile = createIFile(DALICONF);
        StringBuffer logName;
        StringBuffer auditDir;
        if (confIFile->exists())
        {
            serverConfig.setown(createPTreeFromXMLFile(DALICONF));
            if (getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"log","dali",serverConfig->queryProp("@name"),logName)) {
                addPathSepChar(auditDir.append(logName)).append("audit");
                addPathSepChar(logName).append("server");
            }
            else if (serverConfig->getProp("@log_dir", logName))
            {
                serverConfig->getProp("@auditlog_dir", auditDir);
            }
            if (logName.length()) {
                _mkdir(logName.str());
                addPathSepChar(logName);
            }
        }
        if (auditDir.length()==0)
            auditDir.append("AuditLogs");
        _mkdir(auditDir.str());
        addPathSepChar(auditDir);
        StringBuffer auditLogName(auditDir);
        auditLogName.append("DaAudit");
        logName.append("DaServer");
        StringBuffer aliasLogName(logName);
        aliasLogName.append(".log");

        fileMsgHandler = getRollingFileLogMsgHandler(logName.str(), ".log", MSGFIELD_STANDARD, false, true, NULL, aliasLogName.str());
        queryLogMsgManager()->addMonitorOwn(fileMsgHandler, getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, TopDetail));

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
                    serverConfig->setProp("SDS/@remoteBackupLocation", mirrorPath.str());
                    PROGLOG("Checking backup location: %s", mirrorPath.str());

                    try
                    {
                        recursiveCreateDirectory(mirrorPath);
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
                        }
                        else
                            backupURL.append(mirrorPath);
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
#ifdef _WIN32
        Owned<CReleaseMutex> globalNamedMutex;
        if (!serverConfig->getPropBool("allowMultipleDalis"))
        {
            PROGLOG("Checking for existing daserver instances");
            StringBuffer s("DASERVER");
            globalNamedMutex.setown(new CReleaseMutex(s.str()));
            if (!globalNamedMutex->lockWait(10*1000)) // wait for 10 secs
            {
                PrintLog("Another DASERVER process is currently running");
                return 0;
            }
        }
#endif

        SocketEndpoint ep;
        SocketEndpointArray epa;
        if (argc==1) {
            ep.setLocalHost(DALI_SERVER_PORT);
            epa.append(ep);
        }
        else {
            for (unsigned i=2;i<(unsigned)argc;i++) {
                ep.set(argv[i],DALI_SERVER_PORT);
                epa.append(ep);
            }
        }
        unsigned short myport = epa.item(myrank).port;
        startMPServer(myport,true);
        setMsgLevel(serverConfig->getPropInt("SDS/@msgLevel", 100));
        startLogMsgChildReceiver();
        startLogMsgParentReceiver();

        IGroup *group = createIGroup(epa);
        initCoven(group,serverConfig);
        group->Release();
        epa.kill();

// Audit logging
        ILogMsgHandler *msgh = getRollingFileLogMsgHandler(auditLogName, ".log", MSGFIELD_timeDate | MSGFIELD_code, false, true, NULL);
        queryLogMsgManager()->addMonitorOwn(msgh, getCategoryLogMsgFilter(MSGAUD_audit, MSGCLS_all, TopDetail, false));

// SNMP logging
        bool enableSNMP = serverConfig->getPropBool("SDS/@enableSNMP");
        if (serverConfig->getPropBool("SDS/@enableSysLog",true))
            UseSysLogForOperatorMessages();
        AddServers(auditDir.str());
        addAbortHandler(actionOnAbort);
        Owned<IPerfMonHook> perfMonHook;
        startPerformanceMonitor(serverConfig->getPropInt("Coven/@perfReportDelay", DEFAULT_PERF_REPORT_DELAY)*1000, PerfMonStandard, perfMonHook);
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
