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

// Entrypoint for ThorSlave.EXE

#include "platform.h"

#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h> 
#include <stdlib.h> 

#include "build-config.h"
#include "jlib.hpp"
#include "jdebug.hpp"
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jprop.hpp"
#include "thormisc.hpp"
#include "slavmain.hpp"
#include "thorport.hpp"
#include "thexception.hpp"
#include "thmem.hpp"
#include "thbuf.hpp"

#include "mpbase.hpp"
#include "mplog.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "slave.hpp"
#include "portlist.h"
#include "dafdesc.hpp"

#include "slavmain.hpp"

// #define USE_MP_LOG

static INode *masterNode = NULL;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    ::Release(masterNode);
}

#ifdef _DEBUG
USE_JLIB_ALLOC_HOOK;
#endif

static SocketEndpoint slfEp;

static char **cmdArgs;
void mergeCmdParams(IPropertyTree *props)
{
    while (*cmdArgs)
        loadCmdProp(props, *cmdArgs++);
}

static void replyError(const char *errorMsg)
{
    SocketEndpoint myEp = queryMyNode()->endpoint();
    myEp.port -= THOR_MP_INC;
    StringBuffer str("Node '");
    myEp.getUrlStr(str);
    str.append("' exception: ").append(errorMsg);
    Owned<IException> e = MakeStringException(0, "%s", str.str());
    CMessageBuffer msg;
    serializeException(e, msg);
    queryClusterComm().send(msg, 0, MPTAG_THORREGISTRATION);
}

static bool RegisterSelf(SocketEndpoint &masterEp)
{
    StringBuffer slfStr;
    StringBuffer masterStr;
    LOG(MCdebugProgress, thorJob, "registering %s - master %s",slfEp.getUrlStr(slfStr).toCharArray(),masterEp.getUrlStr(masterStr).toCharArray());
    try
    {
        SocketEndpoint ep = masterEp;
        ep.port = getFixedPort(getMasterPortBase(), TPORT_mp);
        Owned<INode> masterNode = createINode(ep);
        CMessageBuffer msg;
        if (!queryWorldCommunicator().recv(msg, masterNode, MPTAG_THORREGISTRATION))
            return false;
        PROGLOG("Initialization received");
        unsigned vmajor, vminor;
        msg.read(vmajor);
        msg.read(vminor);
        if (vmajor != THOR_VERSION_MAJOR || vminor != THOR_VERSION_MINOR)
        {
            replyError("Thor master/slave version mismatch");
            return false;
        }
        Owned<IGroup> group = deserializeIGroup(msg);
        setClusterGroup(group);

        SocketEndpoint myEp = queryMyNode()->endpoint();
        myEp.port -= THOR_MP_INC;
        if (RANK_NULL == group->rank(queryMyNode()))
        {
            replyError("Node not part of thorgroup");
            return false;
        }
        globals->Release();
        globals = createPTree(msg);
        mergeCmdParams(globals); // cmd line 
        msg.read((unsigned &)masterSlaveMpTag);
        msg.clear();
        msg.setReplyTag(MPTAG_THORREGISTRATION);
        if (!queryClusterComm().reply(msg))
            return false;

        PROGLOG("Registration confirmation sent");
        if (!queryClusterComm().recv(msg, 0, MPTAG_THORREGISTRATION)) // when all registered
            return false;
        PROGLOG("verifying mp connection to rest of cluster");
        if (!queryClusterComm().verifyAll())
            ERRLOG("Failed to connect to all nodes");
        else
            PROGLOG("verified mp connection to rest of cluster");
        ::masterNode = LINK(masterNode);
        LOG(MCdebugProgress, thorJob, "registered %s",slfStr.toCharArray());
    }
    catch (IException *e)
    {
        FLLOG(MCexception(e), thorJob, e,"slave registration error");
        e->Release();
        return false;
    }
    return true;
}

void UnregisterSelf()
{
    StringBuffer slfStr;
    slfEp.getUrlStr(slfStr);
    LOG(MCdebugProgress, thorJob, "Unregistering slave : %s", slfStr.toCharArray());
    try
    {
        CMessageBuffer msg;
        msg.append((int)rc_deregister);
        if (!queryWorldCommunicator().send(msg, masterNode, MPTAG_THORREGISTRATION, 60*1000))
        {
            LOG(MCerror, thorJob, "Failed to unregister slave : %s", slfStr.toCharArray());
            return;
        }
        LOG(MCdebugProgress, thorJob, "Unregistered slave : %s", slfStr.toCharArray());
    }
    catch (IException *e) {
        FLLOG(MCexception(e), thorJob, e,"slave unregistration error");
        e->Release();
    }
}

bool ControlHandler() 
{ 
    LOG(MCdebugProgress, thorJob, "CTRL-C pressed");
    if (masterNode) UnregisterSelf();
    abortSlave();
    return false; 
}

void usage()
{
    printf("usage: thorslave  MASTER=ip:port SLAVE=.:port DALISERVERS=ip:port\n");
    exit(1);
}

#ifdef _WIN32
class CReleaseMutex : public CSimpleInterface, public Mutex
{
public:
    CReleaseMutex(const char *name) : Mutex(name) { }
    ~CReleaseMutex() { if (owner) unlock(); }
}; 
#endif


void startSlaveLog(const char *dir)
{
    StringBuffer prefix("THORSLAVE.");
    SocketEndpoint ep;
    ep.setLocalHost(0);
    ep.getUrlStr(prefix);
    prefix.append("_").append(getMachinePortBase());
    StringBuffer logname;
    getLogDir(prefix.str(), dir, logname); 
    openLogFile(logname.toCharArray());
    StringBuffer url;
    createUNCFilename(logname.str(), url);
    LOG(MCdebugProgress, thorJob, "Opened log file %s", url.toCharArray());
    LOG(MCdebugProgress, thorJob, "Build %s", BUILD_TAG);
    globals->setProp("@logURL", url.str());
}


int main( int argc, char *argv[]  )
{
#if defined(WIN32) && defined(_DEBUG)
    int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
    tmpFlag |= _CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag( tmpFlag );
#endif

    InitModuleObjects();

    addAbortHandler(ControlHandler);
    EnableSEHtoExceptionMapping();

    dummyProc();
#ifndef __64BIT__
    Thread::setDefaultStackSize(0x10000);   // NB under windows requires linker setting (/stack:)
#endif

#ifdef _WIN32
    Owned<CReleaseMutex> globalNamedMutex;
#endif 

    if (globals)
        globals->Release();

    {
        Owned<IFile> iFile = createIFile("thor.xml");
        globals = iFile->exists() ? createPTree(*iFile, ipt_caseInsensitive) : createPTree("Thor", ipt_caseInsensitive);
    }
    unsigned multiThorMemoryThreshold = 0;
    try {
        if (argc==1)
        {
            usage();
            return 1;
        }
        cmdArgs = argv+1;
        mergeCmdParams(globals);
        cmdArgs = argv+1;

        const char *master = globals->queryProp("@MASTER");
        if (!master)
            usage();

        setIORetryCount(globals->getPropInt("Debug/@ioRetries")); // default == 0 == off
        const char *slave = globals->queryProp("@SLAVE");
        if (slave)
        {
            slfEp.set(slave);
            localHostToNIC(slfEp);
        }
        else 
            slfEp.setLocalHost(0); 

        setMachinePortBase(slfEp.port);
        slfEp.port = getMachinePortBase();

        const char *confLogDir = globals->queryProp("@logDir");
        StringBuffer logDir;
        if (getConfigurationDirectory(globals->queryPropTree("Directories"),"log","thor",globals->queryProp("@name"),logDir))
        {
            if (confLogDir && *confLogDir)
            {
                StringBuffer confLogTail;
                splitFilename(confLogDir, NULL, NULL, &confLogTail, &confLogTail);
                addPathSepChar(logDir);
                logDir.append(confLogTail);
            }
        }
        else
            logDir.append(confLogDir);
        startSlaveLog(logDir);

#define ISDALICLIENT // JCSMORE plugins *can* access dali - though I think we should probably prohibit somehow.
#ifdef ISDALICLIENT
        const char *daliServers = globals->queryProp("@DALISERVERS");
        if (!daliServers)
        {
            LOG(MCerror, thorJob, "No Dali server list specified\n");
            return 1;
        }
        Owned<IGroup> serverGroup = createIGroup(daliServers, DALI_SERVER_PORT);
        unsigned retry = 0;
        loop {
            try {
                LOG(MCdebugProgress, thorJob, "calling initClientProcess");
                initClientProcess(serverGroup,DCR_ThorSlave, getFixedPort(TPORT_mp));
                break;
            }
            catch (IJSOCK_Exception *e) { 
                if ((e->errorCode()!=JSOCKERR_port_in_use))
                    throw;
                FLLOG(MCexception(e), thorJob, e,"InitClientProcess");
                if (retry++>10) 
                    throw;
                e->Release();
                LOG(MCdebugProgress, thorJob, "Retrying");
                Sleep(retry*2000);  
            }
        }
        setPasswordsFromSDS();
#else
        startMPServer(getFixedPort(TPORT_mp));
#endif

#ifdef USE_MP_LOG
        startMPServer(getFixedPort(TPORT_mp));
        startLogMsgParentReceiver();
        LOG(MCdebugProgress, thorJob, "MPServer started on port %d", getFixedPort(TPORT_mp));
#endif

        SocketEndpoint masterEp(master);
        localHostToNIC(masterEp);
        setMasterPortBase(masterEp.port);
        markNodeCentral(masterEp);
        if (RegisterSelf(masterEp))
        {
            StringBuffer thorPath;
            globals->getProp("@thorPath", thorPath);
            recursiveCreateDirectory(thorPath.str());
            int err = _chdir(thorPath.str());
            if (err)
            {
                IException *e = MakeErrnoException(-1, "Failed to change dir to '%s'",thorPath.str());
                FLLOG(MCexception(e), thorJob, e);
                throw e;
            }

// Initialization from globals
            StringBuffer str;
            if (globals->getProp("@externalProgDir", str.clear()))
                _mkdir(str.str());
            else
                globals->setProp("@externalProgDir", thorPath);

            const char * overrideBaseDirectory = globals->queryProp("@thorDataDirectory");
            const char * overrideReplicateDirectory = globals->queryProp("@thorReplicateDirectory");
            StringBuffer datadir;
            StringBuffer repdir;
            if (getConfigurationDirectory(globals->queryPropTree("Directories"),"data","thor",globals->queryProp("@name"),datadir))
                overrideBaseDirectory = datadir.str();
            if (getConfigurationDirectory(globals->queryPropTree("Directories"),"mirror","thor",globals->queryProp("@name"),repdir))
                overrideReplicateDirectory = repdir.str();
            if (overrideBaseDirectory&&*overrideBaseDirectory)
                setBaseDirectory(overrideBaseDirectory, false);
            if (overrideReplicateDirectory&&*overrideBaseDirectory)
                setBaseDirectory(overrideReplicateDirectory, true);
            StringBuffer tempdirstr;
            const char *tempdir = globals->queryProp("@thorTempDirectory");
            if (getConfigurationDirectory(globals->queryPropTree("Directories"),"temp","thor",globals->queryProp("@name"),tempdirstr))
                tempdir = tempdirstr.str();
            SetTempDir(tempdir,true);

            unsigned gmemsize = globals->getPropInt("@globalMemorySize"); // in MB
            if (gmemsize==0) 
                gmemsize = 2048;
            initThorMemoryManager(gmemsize,globals->getPropInt("@memTraceLevel", 1),globals->getPropInt("@memoryStatsInterval", 60));
            useMemoryMappedRead(globals->getPropBool("@useMemoryMappedRead"));

            LOG(MCdebugProgress, thorJob, "ThorSlave Version LCR - %d.%d started",THOR_VERSION_MAJOR,THOR_VERSION_MINOR);
            StringBuffer url;
            LOG(MCdebugProgress, thorJob, "Slave %s - thor_tmp_dir set to : %s", slfEp.getUrlStr(url).toCharArray(), queryTempDir());
#ifdef _WIN32
            ULARGE_INTEGER userfree;
            ULARGE_INTEGER total;
            ULARGE_INTEGER free;
            if (GetDiskFreeSpaceEx("c:\\",&userfree,&total,&free)&&total.QuadPart) {
                unsigned pc = (unsigned)(free.QuadPart*100/total.QuadPart);
                LOG(MCdebugProgress, thorJob, "Total disk space = %"I64F"d k", total.QuadPart/1000);
                LOG(MCdebugProgress, thorJob, "Free  disk space = %"I64F"d k", free.QuadPart/1000);
                LOG(MCdebugProgress, thorJob, "%d%% disk free\n",pc);
            }
#endif
            if (getConfigurationDirectory(globals->queryPropTree("Directories"),"query","thor",globals->queryProp("@name"),str.clear()))
                globals->setProp("@query_so_dir", str.str());
            else
                globals->getProp("@query_so_dir", str.clear());
            if (str.length())
            {
                if (globals->getPropBool("Debug/@dllsToSlaves"))
                {
                    StringBuffer uniqSoPath;
                    if (PATHSEPCHAR == str.charAt(str.length()-1))
                        uniqSoPath.append(str.length()-1, str.str());
                    else
                        uniqSoPath.append(str);
                    uniqSoPath.append("_").append(getMachinePortBase());
                    str.swapWith(uniqSoPath);
                    globals->setProp("@query_so_dir", str.str());
                }
                PROGLOG("Using querySo directory: %s", str.str());
                recursiveCreateDirectory(str.str());
            }
     
            multiThorMemoryThreshold = globals->getPropInt("@multiThorMemoryThreshold")*0x100000;
            if (multiThorMemoryThreshold) {
                StringBuffer lgname;
                if (!globals->getProp("@multiThorResourceGroup",lgname))
                    globals->getProp("@nodeGroup",lgname);
                if (lgname.length()) {
                    Owned<ILargeMemLimitNotify> notify = createMultiThorResourceMutex(lgname.str());
                    setMultiThorMemoryNotify(multiThorMemoryThreshold,notify);
                    PROGLOG("Multi-Thor resource limit for %s set to %"I64F"d",lgname.str(),(__int64)multiThorMemoryThreshold);
                }   
                else
                    multiThorMemoryThreshold = 0;
            }
            slaveMain();
        }

        LOG(MCdebugProgress, thorJob, "ThorSlave terminated OK");
    }
    catch (IException *e) 
    {
        FLLOG(MCexception(e), thorJob, e,"ThorSlave");
        e->Release();
    }
    catch (CATCHALL)
    {
        FLLOG(MCerror, thorJob, "ThorSlave exiting because of uncaught exception");
    }
    ClearTempDirs();

    if (multiThorMemoryThreshold)
        setMultiThorMemoryNotify(0,NULL);

#ifdef ISDALICLIENT
    closeEnvironment();
    closedownClientProcess();   // dali client closedown
#endif

#ifdef USE_MP_LOG
    stopLogMsgReceivers();
    stopMPServer();
#endif
    ::Release(globals);
    releaseAtoms(); // don't know why we can't use a module_exit to destruct these...

    return 0;
}

