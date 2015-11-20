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
#include "rmtfile.hpp"

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
static unsigned mySlaveNum;

static char **cmdArgs;
void mergeCmdParams(IPropertyTree *props)
{
    while (*cmdArgs)
        loadCmdProp(props, *cmdArgs++);
}

static void replyError(unsigned errorCode, const char *errorMsg)
{
    SocketEndpoint myEp = queryMyNode()->endpoint();
    StringBuffer str("Node '");
    myEp.getUrlStr(str);
    str.append("' exception: ").append(errorMsg);
    Owned<IException> e = MakeStringException(errorCode, "%s", str.str());
    CMessageBuffer msg;
    serializeException(e, msg);
    queryWorldCommunicator().send(msg, 0, MPTAG_THORREGISTRATION);
}

static bool RegisterSelf(SocketEndpoint &masterEp)
{
    StringBuffer slfStr;
    StringBuffer masterStr;
    LOG(MCdebugProgress, thorJob, "registering %s - master %s",slfEp.getUrlStr(slfStr).str(),masterEp.getUrlStr(masterStr).str());
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
            replyError(TE_FailedToRegisterSlave, "Thor master/slave version mismatch");
            return false;
        }
        Owned<IGroup> rawGroup = deserializeIGroup(msg);
        globals->Release();
        globals = createPTree(msg);
        mergeCmdParams(globals); // cmd line

        unsigned slavesPerNode = globals->getPropInt("@slavesPerNode", 1);
        unsigned channelsPerSlave = globals->getPropInt("@channelsPerSlave", 1);
        unsigned localThorPortInc = globals->getPropInt("@localThorPortInc", DEFAULT_SLAVEPORTINC);
        unsigned slaveBasePort = globals->getPropInt("@slaveport", DEFAULT_THORSLAVEPORT);
        setClusterGroup(masterNode, rawGroup, slavesPerNode, channelsPerSlave, slaveBasePort, localThorPortInc);

        const char *_masterBuildTag = globals->queryProp("@masterBuildTag");
        const char *masterBuildTag = _masterBuildTag?_masterBuildTag:"no build tag";
        PROGLOG("Master build: %s", masterBuildTag);
        if (!_masterBuildTag || 0 != strcmp(BUILD_TAG, _masterBuildTag))
        {
            StringBuffer errStr("Thor master/slave build mismatch, master = ");
            errStr.append(masterBuildTag).append(", slave = ").append(BUILD_TAG);
            ERRLOG("%s", errStr.str());
#ifndef _DEBUG
            replyError(TE_FailedToRegisterSlave, errStr.str());
            return false;
#endif
        }
        msg.read((unsigned &)masterSlaveMpTag);
        msg.clear();
        msg.setReplyTag(MPTAG_THORREGISTRATION);
        if (!queryNodeComm().reply(msg))
            return false;

        PROGLOG("Registration confirmation sent");
        if (!queryNodeComm().recv(msg, 0, MPTAG_THORREGISTRATION)) // when all registered
            return false;

        ::masterNode = LINK(masterNode);

        PROGLOG("verifying mp connection to rest of cluster");
        if (!queryNodeComm().verifyAll())
            ERRLOG("Failed to connect to all nodes");
        else
            PROGLOG("verified mp connection to rest of cluster");
        LOG(MCdebugProgress, thorJob, "registered %s",slfStr.str());
    }
    catch (IException *e)
    {
        FLLOG(MCexception(e), thorJob, e,"slave registration error");
        e->Release();
        return false;
    }
    return true;
}

static bool jobListenerStopped = true;

void UnregisterSelf(IException *e)
{
    StringBuffer slfStr;
    slfEp.getUrlStr(slfStr);
    LOG(MCdebugProgress, thorJob, "Unregistering slave : %s", slfStr.str());
    try
    {
        CMessageBuffer msg;
        msg.append((int)rc_deregister);
        serializeException(e, msg); // NB: allows exception to be NULL
        if (!queryWorldCommunicator().send(msg, masterNode, MPTAG_THORREGISTRATION, 60*1000))
        {
            LOG(MCerror, thorJob, "Failed to unregister slave : %s", slfStr.str());
            return;
        }
        LOG(MCdebugProgress, thorJob, "Unregistered slave : %s", slfStr.str());
    }
    catch (IException *e) {
        if (!jobListenerStopped)
            FLLOG(MCexception(e), thorJob, e,"slave unregistration error");
        e->Release();
    }
}

bool ControlHandler(ahType type)
{
    if (ahInterrupt == type)
        LOG(MCdebugProgress, thorJob, "CTRL-C pressed");
    if (!jobListenerStopped)
    {
        if (masterNode)
            UnregisterSelf(NULL);
        abortSlave();
    }
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


void startSlaveLog()
{
    StringBuffer fileName("thorslave");
    Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(globals->queryProp("@logDir"), "thor");
    StringBuffer slaveNumStr;
    lf->setPostfix(slaveNumStr.append(mySlaveNum).str());
    lf->setCreateAliasFile(false);
    lf->setName(fileName.str());//override default filename
    lf->beginLogging();

    StringBuffer url;
    createUNCFilename(lf->queryLogFileSpec(), url);

    LOG(MCdebugProgress, thorJob, "Opened log file %s", url.str());
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
    // Restrict stack sizes on 32-bit systems
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

    Owned<IException> unregisterException;
    try
    {
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

        const char *slave = globals->queryProp("@SLAVE");
        if (slave)
        {
            slfEp.set(slave);
            localHostToNIC(slfEp);
        }
        else 
            slfEp.setLocalHost(0);
        mySlaveNum = globals->getPropInt("@SLAVENUM");

        setMachinePortBase(slfEp.port);
        slfEp.port = getMachinePortBase();
        startSlaveLog();

        startMPServer(getFixedPort(TPORT_mp));
#ifdef USE_MP_LOG
        startLogMsgParentReceiver();
        LOG(MCdebugProgress, thorJob, "MPServer started on port %d", getFixedPort(TPORT_mp));
#endif

        SocketEndpoint masterEp(master);
        localHostToNIC(masterEp);
        setMasterPortBase(masterEp.port);
        markNodeCentral(masterEp);
        if (RegisterSelf(masterEp))
        {
            if (globals->getPropBool("Debug/@slaveDaliClient"))
                enableThorSlaveAsDaliClient();

            IDaFileSrvHook *daFileSrvHook = queryDaFileSrvHook();
            if (daFileSrvHook) // probably always installed
                daFileSrvHook->addFilters(globals->queryPropTree("NAS"), &slfEp);

            StringBuffer thorPath;
            globals->getProp("@thorPath", thorPath);
            recursiveCreateDirectory(thorPath.str());
            int err = _chdir(thorPath.str());
            if (err)
            {
                IException *e = makeErrnoExceptionV(-1, "Failed to change dir to '%s'", thorPath.str());
                FLLOG(MCexception(e), thorJob, e);
                throw e;
            }

// Initialization from globals
            setIORetryCount(globals->getPropInt("Debug/@ioRetries")); // default == 0 == off

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
            StringBuffer tempDirStr;
            if (getConfigurationDirectory(globals->queryPropTree("Directories"),"temp","thor",globals->queryProp("@name"), tempDirStr))
                globals->setProp("@thorTempDirectory", tempDirStr.str());
            else
                tempDirStr.append(globals->queryProp("@thorTempDirectory"));
            addPathSepChar(tempDirStr).append(getMachinePortBase());

            logDiskSpace(); // Log before temp space is cleared
            SetTempDir(tempDirStr.str(), "thtmp", true);

            useMemoryMappedRead(globals->getPropBool("@useMemoryMappedRead"));

            LOG(MCdebugProgress, thorJob, "ThorSlave Version LCR - %d.%d started",THOR_VERSION_MAJOR,THOR_VERSION_MINOR);
            StringBuffer url;
            LOG(MCdebugProgress, thorJob, "Slave %s - temporary dir set to : %s", slfEp.getUrlStr(url).str(), queryTempDir());
#ifdef _WIN32
            ULARGE_INTEGER userfree;
            ULARGE_INTEGER total;
            ULARGE_INTEGER free;
            if (GetDiskFreeSpaceEx("c:\\",&userfree,&total,&free)&&total.QuadPart) {
                unsigned pc = (unsigned)(free.QuadPart*100/total.QuadPart);
                LOG(MCdebugProgress, thorJob, "Total disk space = %" I64F "d k", total.QuadPart/1000);
                LOG(MCdebugProgress, thorJob, "Free  disk space = %" I64F "d k", free.QuadPart/1000);
                LOG(MCdebugProgress, thorJob, "%d%% disk free\n",pc);
            }
#endif
            if (getConfigurationDirectory(globals->queryPropTree("Directories"),"query","thor",globals->queryProp("@name"),str.clear()))
                globals->setProp("@query_so_dir", str.str());
            else
                globals->getProp("@query_so_dir", str.clear());
            if (str.length())
            {
                if (globals->getPropBool("Debug/@dllsToSlaves", true))
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
                    PROGLOG("Multi-Thor resource limit for %s set to %" I64F "d",lgname.str(),(__int64)multiThorMemoryThreshold);
                }   
                else
                    multiThorMemoryThreshold = 0;
            }
            slaveMain(jobListenerStopped);
        }

        LOG(MCdebugProgress, thorJob, "ThorSlave terminated OK");
    }
    catch (IException *e) 
    {
        if (!jobListenerStopped)
            FLLOG(MCexception(e), thorJob, e,"ThorSlave");
        unregisterException.setown(e);
    }
    ClearTempDirs();

    if (multiThorMemoryThreshold)
        setMultiThorMemoryNotify(0,NULL);
    roxiemem::releaseRoxieHeap();

    if (unregisterException.get())
        UnregisterSelf(unregisterException);

    if (globals->getPropBool("Debug/@slaveDaliClient"))
        disableThorSlaveAsDaliClient();

#ifdef USE_MP_LOG
    stopLogMsgReceivers();
#endif
    stopMPServer();
    ::Release(globals);
    releaseAtoms(); // don't know why we can't use a module_exit to destruct these...

    ExitModuleObjects(); // not necessary, atexit will call, but good for leak checking
    return 0;
}

