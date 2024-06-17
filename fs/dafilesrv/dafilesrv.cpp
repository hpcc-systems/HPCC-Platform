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

#include "platform.h"
#include "portlist.h"

#include "jlib.hpp"
#include "jiface.hpp"
#include "jutil.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "jmisc.hpp"
#include "jsecrets.hpp"
#include "dalienv.hpp"
#include "dafdesc.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

static const bool defaultRowServiceOnStdPort = true;
static const bool defaultDedicatedRowServiceSSL = false;
static const char* defaultRowSericeConfiguration = "RowSvc";


#include "remoteerr.hpp"
#include "dafscommon.hpp"
#include "rmtclient.hpp"
#include "rmtfile.hpp"
#include "dafsserver.hpp"

void usage()
{
    printf("dafilesrv usage:\n");
    printf("    dafilesrv [--trace <n>] [...] [--port <port>] [--sbSize <send-buff-size-kb>] [--rbSize <recv-buff-size-kb>]\n");
    printf("                                                  -- run test local\n");
    printf("    dafilesrv --daemon [ --logDir <log-dir> ] [ -LOCAL ]      -- run as linux daemon\n");
    printf("    dafilesrv --remote                            -- run remote (linux daemon, windows standalone)\n");
#ifdef _WIN32
    printf("    dafilesrv --install                            -- install windows service\n");
    printf("    dafilesrv --remove                             -- remove windows service\n\n");
#endif
    printf("    add --name <instance name> to specify an instance name\n");
    printf("    add --noSSL to disable SSL sockets, even when specified in configuration\n\n");
    printf("    additional optional args:\n");
    printf("        [--port <port>] [--sslPort <ssl-port>] [--sbSize <send-buff-size-kb>] [--rbSize <recv-buff-size-kb>]\n");
    printf("        [--addr <ip>:<port>]\n\n");
    printf("    Standard port is %d\n",DAFILESRV_PORT);
    printf("    Standard SSL port is %d (certificate and key required in environment.conf)\n",SECURE_DAFILESRV_PORT);
    printf("    Version:  %s\n\n",remoteServerVersionString());
}

static Owned<IRemoteFileServer> server;


#ifdef _WIN32
// Service code

#define DAFS_SERVICE_NAME "DaFileSrv"
#define DAFS_SERVICE_DISPLAY_NAME "Dali File Server"


void LogError( const char *s,DWORD dwError ) 
{   
    LPTSTR lpBuffer = NULL;   
    FormatMessage( FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM, NULL, dwError,
            MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
            (LPTSTR) &lpBuffer, 0, NULL );   
    OERRLOG( "%s(%d): %s", s, dwError, lpBuffer );   
    LocalFree( lpBuffer );
}


// Service initialization

static class CService *THIS; // singleton instance

class CService
{

    SERVICE_STATUS ServiceStatus; 
    SERVICE_STATUS_HANDLE hStatus; 

    // Control handler function
    void doControlHandler(DWORD request) 
    { 
        switch(request) { 
            case SERVICE_CONTROL_STOP: 
                PROGLOG(DAFS_SERVICE_NAME " Control: stopped");

                ServiceStatus.dwWin32ExitCode = 0; 
                ServiceStatus.dwCurrentState  = SERVICE_STOPPED; 
                break;
            case SERVICE_CONTROL_SHUTDOWN: 
                PROGLOG(DAFS_SERVICE_NAME "Control: shutdown");

                ServiceStatus.dwWin32ExitCode = 0; 
                ServiceStatus.dwCurrentState  = SERVICE_STOPPED; 
                break;
            case SERVICE_CONTROL_INTERROGATE:        
                break;
            default:
                PROGLOG(DAFS_SERVICE_NAME " Control: %d",request);
                break;
        } 
 
        // Report current status
        SetServiceStatus (hStatus,  &ServiceStatus);
 
        return; 
    } 


    void doServiceMain(int argc, char** argv) 
    { 
 
        ServiceStatus.dwServiceType        = SERVICE_WIN32; 
        ServiceStatus.dwCurrentState       = SERVICE_START_PENDING; 
        ServiceStatus.dwControlsAccepted   = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
        ServiceStatus.dwWin32ExitCode      = 0; 
        ServiceStatus.dwServiceSpecificExitCode = 0; 
        ServiceStatus.dwCheckPoint         = 0; 
        ServiceStatus.dwWaitHint           = 0; 
 
        hStatus = RegisterServiceCtrlHandler(DAFS_SERVICE_NAME, (LPHANDLER_FUNCTION)ControlHandler); 
        if (hStatus == (SERVICE_STATUS_HANDLE)0)  { 
            // Registering Control Handler failed
            LogError("RegisterServiceCtrlHandler",GetLastError());
            return; 
        }  
        // Initialize Service 
        if (!init()) {
            // Initialization failed
            ServiceStatus.dwCurrentState       = SERVICE_STOPPED; 
            ServiceStatus.dwWin32ExitCode      = -1; 
            SetServiceStatus(hStatus, &ServiceStatus); 
            return; 
        } 
        // We report the running status to SCM. 
        ServiceStatus.dwCurrentState = SERVICE_RUNNING; 
        SetServiceStatus (hStatus, &ServiceStatus);
 
        // The worker loop of a service
        run();
        return; 
    }


public:
    CService() 
    { 
        THIS = this;    
    }


    void start()
    {
        SERVICE_TABLE_ENTRY ServiceTable[2];
        ServiceTable[0].lpServiceName = DAFS_SERVICE_NAME;
        ServiceTable[0].lpServiceProc = (LPSERVICE_MAIN_FUNCTION)ServiceMain;

        ServiceTable[1].lpServiceName = NULL;
        ServiceTable[1].lpServiceProc = NULL;
        // Start the control dispatcher thread for our service
        if (!StartServiceCtrlDispatcher(ServiceTable)) {
            LogError("StartServiceCtrlDispatcher",GetLastError());
        }
    }

    void stop()
    {

        ServiceStatus.dwCurrentState       = SERVICE_STOPPED; 
        ServiceStatus.dwCheckPoint         = 0; 
        ServiceStatus.dwWaitHint           = 0; 
        ServiceStatus.dwWin32ExitCode      = 0; 
        ServiceStatus.dwServiceSpecificExitCode = 0; 
        SetServiceStatus(hStatus, &ServiceStatus); 
    }

    // Control handler function
    static void ControlHandler(DWORD request) 
    { 
        THIS->doControlHandler(request) ;
    } 


    static void ServiceMain(int argc, char** argv) 
    { 
        THIS->doServiceMain(argc, argv);
    }

    bool running() { return ServiceStatus.dwCurrentState == SERVICE_RUNNING; }

    virtual bool init() = 0;
    virtual void run() = 0;
    



};



bool installService(const char *servicename,const char *servicedisplayname,const char *dependancies) 
{ 
    DWORD err = ERROR_SUCCESS;
    char path[512]; 
    if (GetModuleFileName( NULL, path, sizeof(path) )) {
        SC_HANDLE hSCM = OpenSCManager( NULL, NULL, SC_MANAGER_ALL_ACCESS);  // full access rights 
        if (hSCM) {
            SC_HANDLE hService = CreateService( 
                hSCM,                       // SCManager database 
                servicename,                // name of service 
                servicedisplayname,         // name to display 
                SERVICE_ALL_ACCESS,         // desired access 
                SERVICE_WIN32_OWN_PROCESS,//|SERVICE_INTERACTIVE_PROCESS ,  // service type 
                SERVICE_AUTO_START,         // start type 
                SERVICE_ERROR_NORMAL,       // error control type 
                path,                       // service's binary 
                NULL,                       // no load ordering group 
                NULL,                       // no tag identifier 
                dependancies,               // dependencies 
                NULL,                       // LocalSystem account 
                NULL);                      // no password 
            if (hService) {
                Sleep(1000); 
                StartService(hService,0,0);
                CloseServiceHandle(hService); 
            }
            else
                err = GetLastError();
        }
        else
            err = GetLastError();
        CloseServiceHandle(hSCM); 
    }
    else
        err = GetLastError();
    if (err!=ERROR_SUCCESS) {
        LogError("Install failed",err);
        return false;
    }
    return true;
} 



bool uninstallService(const char *servicename,const char *servicedisplayname) 
{ 
    DWORD err = ERROR_SUCCESS;
    SC_HANDLE hSCM = OpenSCManager( NULL, NULL, SC_MANAGER_ALL_ACCESS);  // full access rights 
    if (hSCM) {
        SC_HANDLE hService = OpenService(hSCM, servicename, SERVICE_STOP|SERVICE_QUERY_STATUS); 
        if (hService) {
            // try to stop the service 
            SERVICE_STATUS ss;
            if ( ControlService( hService, SERVICE_CONTROL_STOP, &ss ) ) { 
                PROGLOG("Stopping %s", servicedisplayname); 
                Sleep( 1000 ); 

                while( QueryServiceStatus( hService, &ss ) ) { 
                    if ( ss.dwCurrentState != SERVICE_STOP_PENDING ) 
                        break;
                    Sleep( 1000 ); 
                } 

                if ( ss.dwCurrentState == SERVICE_STOPPED ) 
                    PROGLOG("%s Service stopped",servicedisplayname); 
                else 
                    OERRLOG("%s failed to stop",servicedisplayname); 
            } 
            CloseServiceHandle(hService); 
        }
        hService = OpenService(hSCM, servicename, DELETE); 
        if (hService) {
            // now remove the service 
            if (!DeleteService(hService)) 
                err = GetLastError();
            CloseServiceHandle(hService); 
        } 
        else 
            err = GetLastError();
        CloseServiceHandle(hSCM); 
    } 
    else 
        err = GetLastError();
    if (err!=ERROR_SUCCESS) {
        LogError("Uninstall failed",err);
        return false;
    }
    return true;
} 

#else


void sighandler(int signum, siginfo_t *info, void *extra) 
{
    PROGLOG("Caught signal %d, %p", signum, info?info->si_addr:0);
    if (server)
        server->stop();
}

int initDaemon()
{
    int ret = daemon(1,0);
    if (ret)
        return ret;
    struct sigaction act;
    sigset_t blockset;
    sigemptyset(&blockset);
    act.sa_mask = blockset;
    act.sa_handler = SIG_IGN;
    act.sa_flags = 0;
    sigaction(SIGHUP, &act, NULL);

    act.sa_flags = SA_SIGINFO;
    act.sa_sigaction = &sighandler; 
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);
    return 0;
}

#endif

const char *getSSLMethodText(DAFSConnectCfg connectMethod)
{
    switch (connectMethod)
    {
        case SSLNone:
            return "SSLNone";
        case SSLOnly:
            return "SSLOnly";
        case SSLFirst:
            return "SSLFirst";
        case UnsecureFirst:
            return "UnsecureFirst";
        case UnsecureAndSSL:
            return "UnsecureAndSSL";
        default:
            throwUnexpected();
    }
}


static constexpr const char * defaultYaml = R"!!(
version: 1.0
dafilesrv:
  name: dafilesrv
  logging:
    detail: 100
)!!";


int main(int argc, const char* argv[])
{
    InitModuleObjects();

    EnableSEHtoExceptionMapping();
#ifndef __64BIT__
    // Restrict stack sizes on 32-bit systems
    Thread::setDefaultStackSize(0x10000);   // 64K stack (also set in windows DSP)
#endif
    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);

    SocketEndpoint listenep;
    unsigned sendbufsize = 0;
    unsigned recvbufsize = 0;
    bool locallisten = false;
    StringBuffer componentName;

    // NB: bare-metal dafilesrv does not have a component specific xml
    Owned<IPropertyTree> config = loadConfiguration(defaultYaml, argv, "dafilesrv", "DAFILESRV", nullptr, nullptr);

    Owned<IPropertyTree> keyPairInfo; // NB: not used in containerized mode
    // Get SSL Settings
    DAFSConnectCfg  connectMethod;
    unsigned short  port;
    unsigned short  sslport;
    unsigned dedicatedRowServicePort = DEFAULT_ROWSERVICE_PORT;
#ifdef _CONTAINERIZED
    // Use the "public" certificate issuer, unless it's visibility is "cluster" (meaning internal only)
    const char *visibility = getComponentConfigSP()->queryProp("service/@visibility");
    const char *certScope = strsame("cluster", visibility) ? "local" : "public";
    connectMethod = hasIssuerTlsConfig(certScope) ? SSLOnly : SSLNone;
    // NB: connectMethod will direct the CRemoteFileServer on accept to create a secure socket based on the same issuer certificates

    dedicatedRowServicePort = 0; // row service always runs on same secure ssl port in containerized mode
    port = 0;
    sslport = config->getPropInt("service/@port", SECURE_DAFILESRV_PORT);
    listenep.port = sslport;
#else
    // NB: certificates used by dafsserver when creating secure socket to listen to
    queryDafsSecSettings(&connectMethod, &port, &sslport, nullptr, nullptr, nullptr);
    // bit of a kludge for windows - if .exe not specified then not daemon
    bool isdaemon = (memicmp(argv[0]+strlen(argv[0])-4,".exe",4)==0);
    listenep.port = port;
    if (config->hasProp("@port"))
        listenep.port = config->getPropInt("@port");
#endif

    unsigned maxThreads = DEFAULT_THREADLIMIT;
    unsigned maxThreadsDelayMs = DEFAULT_THREADLIMITDELAYMS;
    unsigned maxAsyncCopy = DEFAULT_ASYNCCOPYMAX;
    unsigned parallelRequestLimit = DEFAULT_STDCMD_PARALLELREQUESTLIMIT;
    unsigned throttleDelayMs = DEFAULT_STDCMD_THROTTLEDELAYMS;
    unsigned throttleCPULimit = DEFAULT_STDCMD_THROTTLECPULIMIT;
    unsigned throttleQueueLimit = DEFAULT_STDCMD_THROTTLEQUEUELIMIT;
    unsigned parallelSlowRequestLimit = DEFAULT_SLOWCMD_PARALLELREQUESTLIMIT;
    unsigned throttleSlowDelayMs = DEFAULT_SLOWCMD_THROTTLEDELAYMS;
    unsigned throttleSlowCPULimit = DEFAULT_SLOWCMD_THROTTLECPULIMIT;
    unsigned throttleSlowQueueLimit = DEFAULT_SLOWCMD_THROTTLEQUEUELIMIT;

    StringAttr rowServiceConfiguration = defaultRowSericeConfiguration;
    bool dedicatedRowServiceSSL = defaultDedicatedRowServiceSSL;
    bool rowServiceOnStdPort = defaultRowServiceOnStdPort;

    if (config->hasProp("@help"))
    {
        usage();
        exit(0);
    }

#ifndef _CONTAINERIZED
    if (config->getPropBool("@daemon"))
        isdaemon = true;
    if (config->getPropBool("@remote"))
    {
#ifdef _WIN32
        isdaemon = false;
#else
        isdaemon = true;
#endif
    }
    StringBuffer logDir;
    config->getProp("@logDir", logDir);
#endif
    if (config->hasProp("@trace"))
    {
        setDaliServerTrace(config->getPropInt("@trace"));
#ifndef _CONTAINERIZED
        isdaemon = false;
#endif
    }
    config->getProp("@name", componentName);
    if (config->hasProp("@addr"))
    {
        unsigned portOnly = config->getPropInt("@addr");
        if (portOnly)
            listenep.port = portOnly;
        else
            listenep.set(config->queryProp("@addr"), listenep.port);
    }
    if (config->hasProp("@sslPort"))
        sslport = config->getPropInt("@sslPort");

    if (config->hasProp("@sbSize"))
        sendbufsize = config->getPropInt("@sbSize");

    if (config->hasProp("@rbSize"))
        recvbufsize = config->getPropInt("@rbSize");
    
    if (config->hasProp("@local"))
        locallisten = true;
    if (config->hasProp("@noSSL"))
    {
        if (connectMethod != SSLNone)
        {
            PROGLOG("DaFileSrv SSL specified in config but overridden by -NOSSL in command line");
            connectMethod = SSLNone;
        }
    }

#ifdef _WIN32
    if (config->hasProp("@install"))
    {
        if (installService(DAFS_SERVICE_NAME,DAFS_SERVICE_DISPLAY_NAME,NULL))
        {
            PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Installed");
            return 0;
        }
        return 1;
    }
    if (config->hasProp("@remove"))
    {
        if (uninstallService(DAFS_SERVICE_NAME,DAFS_SERVICE_DISPLAY_NAME))
        {
            PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Uninstalled");
            return 0;
        }
        return 1;
    }
#endif
    // NB: these are not used, but are in Solaris version to setsockopt SO_SNDBUF/SO_RCVBUF, perhaps should be
    sendbufsize = config->getPropInt("@sendBufSize", sendbufsize);
    recvbufsize = config->getPropInt("@recvBufSize", recvbufsize);

    IPropertyTree *dafileSrvInstance = nullptr;
#ifndef _CONTAINERIZED
    Owned<IPropertyTree> env = getHPCCEnvironment();
    Owned<IPropertyTree> _dafileSrvInstance;
    if (env)
    {
        Owned<IPropertyTree> newConfig = createPTreeFromIPT(config); // clone
        IPropertyTree *expert = ensurePTree(newConfig, "expert");

        StringBuffer dafilesrvPath("Software/DafilesrvProcess");
        if (componentName.length())
            dafilesrvPath.appendf("[@name=\"%s\"]", componentName.str());
        else
            dafilesrvPath.append("[1]"); // in absence of name, use 1st
        IPropertyTree *daFileSrv = env->queryPropTree(dafilesrvPath);
        Owned<IPropertyTree> _dafileSrv;

        // merge in bare-metal global expert settings
        IPropertyTree *globalExpert = nullptr;
        globalExpert = env->queryPropTree("Software/Globals");
        if (globalExpert)
            synchronizePTree(expert, globalExpert, false, false);

        if (daFileSrv)
        {
            const char *componentGroupName = daFileSrv->queryProp("@group");
            if (!isEmptyString(componentGroupName))
            {
                VStringBuffer dafilesrvGroupPath("Software/DafilesrvGroup[@name=\"%s\"]", componentGroupName);
                IPropertyTree *daFileSrvGroup = env->queryPropTree(dafilesrvGroupPath);
                if (daFileSrvGroup)
                {
                    // create a copy of the group settings and merge in (overwrite) with the component settings, i.e. any group settings become defaults
                    _dafileSrv.setown(createPTreeFromIPT(daFileSrvGroup));
                    synchronizePTree(_dafileSrv, daFileSrv, false, false);
                    daFileSrv = _dafileSrv;
                }
            }

            // Component level DaFileSrv settings:

            maxThreads = daFileSrv->getPropInt("@maxThreads", DEFAULT_THREADLIMIT);
            maxThreadsDelayMs = daFileSrv->getPropInt("@maxThreadsDelayMs", DEFAULT_THREADLIMITDELAYMS);
            maxAsyncCopy = daFileSrv->getPropInt("@maxAsyncCopy", DEFAULT_ASYNCCOPYMAX);

            parallelRequestLimit = daFileSrv->getPropInt("@parallelRequestLimit", DEFAULT_STDCMD_PARALLELREQUESTLIMIT);
            throttleDelayMs = daFileSrv->getPropInt("@throttleDelayMs", DEFAULT_STDCMD_THROTTLEDELAYMS);
            throttleCPULimit = daFileSrv->getPropInt("@throttleCPULimit", DEFAULT_STDCMD_THROTTLECPULIMIT);
            throttleQueueLimit = daFileSrv->getPropInt("@throttleQueueLimit", DEFAULT_STDCMD_THROTTLEQUEUELIMIT);

            parallelSlowRequestLimit = daFileSrv->getPropInt("@parallelSlowRequestLimit", DEFAULT_SLOWCMD_PARALLELREQUESTLIMIT);
            throttleSlowDelayMs = daFileSrv->getPropInt("@throttleSlowDelayMs", DEFAULT_SLOWCMD_THROTTLEDELAYMS);
            throttleSlowCPULimit = daFileSrv->getPropInt("@throttleSlowCPULimit", DEFAULT_SLOWCMD_THROTTLECPULIMIT);
            throttleSlowQueueLimit = daFileSrv->getPropInt("@throttleSlowQueueLimit", DEFAULT_SLOWCMD_THROTTLEQUEUELIMIT);

            dedicatedRowServicePort = daFileSrv->getPropInt("@rowServicePort", DEFAULT_ROWSERVICE_PORT);
            dedicatedRowServiceSSL = daFileSrv->getPropBool("@rowServiceSSL", defaultDedicatedRowServiceSSL);
            rowServiceOnStdPort = daFileSrv->getPropBool("@rowServiceOnStdPort", defaultRowServiceOnStdPort);
            if (daFileSrv->queryProp("@rowServiceConfiguration"))
                rowServiceConfiguration = daFileSrv->queryProp("@rowServiceConfiguration");

            // merge in bare-metal dafilesrv component expert settings
            IPropertyTree *componentExpert = nullptr;
            componentExpert = daFileSrv->queryPropTree("expert");
            if (componentExpert)
                synchronizePTree(expert, componentExpert, false, true);

            // any overrides by Instance definitions?
            Owned<IPropertyTreeIterator> iter = daFileSrv->getElements("Instance");
            ForEach(*iter)
            {
                IpAddress instanceIP(iter->query().queryProp("@netAddress"));
                if (instanceIP.ipequals(queryHostIP()))
                    dafileSrvInstance = &iter->query();
            }
            if (dafileSrvInstance)
            {
                // check if there's a DaFileSrvGroup
                const char *instanceGroupName = dafileSrvInstance->queryProp("@group");
                if (!isEmptyString(instanceGroupName) && (isEmptyString(componentGroupName) || !strsame(instanceGroupName, componentGroupName))) // i.e. only if different
                {
                    VStringBuffer dafilesrvGroupPath("Software/DafilesrvGroup[@name=\"%s\"]", instanceGroupName);
                    IPropertyTree *daFileSrvGroup = env->queryPropTree(dafilesrvGroupPath);
                    if (daFileSrvGroup)
                    {
                        // create a copy of the group settings and merge in (overwrite) with the instance settings, i.e. any group settings become defaults
                        _dafileSrvInstance.setown(createPTreeFromIPT(daFileSrvGroup));
                        synchronizePTree(_dafileSrvInstance, dafileSrvInstance, false, false);
                        dafileSrvInstance = _dafileSrvInstance;
                    }
                }
            }

            // merge in bare-metal dafilesrv instance expert settings
            IPropertyTree *instanceExpert = nullptr;
            instanceExpert = dafileSrvInstance->queryPropTree("expert");
            if (instanceExpert)
                synchronizePTree(expert, instanceExpert, false, true);
        }

        // update config and hook callback with dafilesrv expert PTree
        replaceComponentConfig(newConfig, getGlobalConfigSP());

        // bare-metal gets it's certificate info. from environment at the moment, 'keyPairInfo' not used in containerized mode
        keyPairInfo.set(env->queryPropTree("EnvSettings/Keys"));
    }

#ifndef _USE_OPENSSL
    if (dedicatedRowServicePort)
    {
        dedicatedRowServiceSSL = false;
    }
#endif

    if (0 == logDir.length())
    {
        getConfigurationDirectory(NULL,"log","dafilesrv",componentName.str(),logDir);
        if (0 == logDir.length())
            logDir.append(".");
    }
    if (componentName.length())
    {
        addPathSepChar(logDir);
        logDir.append(componentName.str());
    }

    if ( (connectMethod == SSLNone) && (listenep.port == 0) )
    {
        printf("\nError, port must not be 0\n");
        usage();
        exit(-1);
    }
    else if ( (connectMethod == SSLOnly) && (sslport == 0) )
    {
        printf("\nError, secure port must not be 0\n");
        usage();
        exit(-1);
    }
    else if ( ((connectMethod == SSLFirst) || (connectMethod == UnsecureFirst) || (connectMethod == UnsecureAndSSL)) && ((listenep.port == 0) || (sslport == 0)) )
    {
        printf("\nError, both port and secure port must not be 0\n");
        usage();
        exit(-1);
    }

    {
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(logDir.str(), "DAFILESRV");
        lf->setCreateAliasFile(false);
        lf->setMaxDetail(TopDetail);
        lf->beginLogging();
    }
    write_pidfile(componentName.str());
#else // _CONTAINERIZED
    setupContainerizedLogMsgHandler();

    dafileSrvInstance = config;

    // k8s defaults, a bit arbitrary, but allow more concurrency by default than legacy
    maxThreads = 400;
    maxThreadsDelayMs = (60*1000);
    maxAsyncCopy = 400;
    parallelRequestLimit = 400;
    throttleDelayMs = 1000;
    throttleCPULimit = 85;
    throttleQueueLimit = 1000;
    parallelSlowRequestLimit = parallelRequestLimit;
    throttleSlowDelayMs = throttleDelayMs;
    throttleSlowCPULimit = throttleCPULimit;
    throttleSlowQueueLimit = throttleQueueLimit;
#endif

    Owned<IPropertyTree> dummyDafileSrvInstance;
    if (nullptr == dafileSrvInstance)
    {
        PROGLOG("WARNING: no dafilesrv configuration, default settings will be used");
        dummyDafileSrvInstance.setown(createPTree());
        dafileSrvInstance = dummyDafileSrvInstance;
    }
    maxThreads = dafileSrvInstance->getPropInt("@maxThreads", maxThreads);
    maxThreadsDelayMs = dafileSrvInstance->getPropInt("@maxThreadsDelayMs", maxThreadsDelayMs);
    maxAsyncCopy = dafileSrvInstance->getPropInt("@maxAsyncCopy", maxAsyncCopy);

    parallelRequestLimit = dafileSrvInstance->getPropInt("@parallelRequestLimit", parallelRequestLimit);
    throttleDelayMs = dafileSrvInstance->getPropInt("@throttleDelayMs", throttleDelayMs);
    throttleCPULimit = dafileSrvInstance->getPropInt("@throttleCPULimit", throttleCPULimit);
    throttleQueueLimit = dafileSrvInstance->getPropInt("@throttleQueueLimit", throttleQueueLimit);

    parallelSlowRequestLimit = dafileSrvInstance->getPropInt("@parallelSlowRequestLimit", parallelSlowRequestLimit);
    throttleSlowDelayMs = dafileSrvInstance->getPropInt("@throttleSlowDelayMs", throttleSlowDelayMs);
    throttleSlowCPULimit = dafileSrvInstance->getPropInt("@throttleSlowCPULimit", throttleSlowCPULimit);
    throttleSlowQueueLimit = dafileSrvInstance->getPropInt("@throttleSlowQueueLimit", throttleSlowQueueLimit);

    dedicatedRowServicePort = dafileSrvInstance->getPropInt("@rowServicePort", dedicatedRowServicePort);
    dedicatedRowServiceSSL = dafileSrvInstance->getPropBool("@rowServiceSSL", dedicatedRowServiceSSL);
    rowServiceOnStdPort = dafileSrvInstance->getPropBool("@rowServiceOnStdPort", rowServiceOnStdPort);

    unsigned listenQueueLimit = dafileSrvInstance->getPropInt("@maxBacklogQueueSize", DEFAULT_LISTEN_QUEUE_SIZE);
    // NB: could check getComponentConfig()->getPropInt("expert/@maxBacklogQueueSize", DEFAULT_LISTEN_QUEUE_SIZE);
    // but many other components have their own explcit setting for this ...

    installDefaultFileHooks(dafileSrvInstance);

#ifndef _CONTAINERIZED
    if (isdaemon)
    {
#ifdef _WIN32
        class cserv: public CService
        {
            bool stopped;
            bool started;
            Linked<IPropertyTree> config;
            DAFSConnectCfg connectMethod;
            SocketEndpoint listenep;
            unsigned maxThreads;
            unsigned maxThreadsDelayMs;
            unsigned maxAsyncCopy;
            unsigned parallelRequestLimit;
            unsigned throttleDelayMs;
            unsigned throttleCPULimit;
            unsigned parallelSlowRequestLimit;
            unsigned throttleSlowDelayMs;
            unsigned throttleSlowCPULimit;
            unsigned sslport;
            unsigned listenQueueLimit;
            Linked<IPropertyTree> keyPairInfo;
            StringAttr rowServiceConfiguration;
            unsigned dedicatedRowServicePort;
            bool dedicatedRowServiceSSL;
            bool rowServiceOnStdPort;
            
            class cpollthread: public Thread
            {
                cserv *parent;
            public:
                cpollthread( cserv *_parent ) 
                : Thread("CService::cpollthread"), parent(_parent) 
                {
                }
                int run() 
                { 
                    while (parent->poll())
                        Sleep(1000);
                    return 1;
                }
            } pollthread;
            Owned<IRemoteFileServer> server;

        public:

            cserv(IPropertyTree *_config, DAFSConnectCfg _connectMethod, SocketEndpoint _listenep,
                        unsigned _maxThreads, unsigned _maxThreadsDelayMs, unsigned _maxAsyncCopy,
                        unsigned _parallelRequestLimit, unsigned _throttleDelayMs, unsigned _throttleCPULimit,
                        unsigned _parallelSlowRequestLimit, unsigned _throttleSlowDelayMs, unsigned _throttleSlowCPULimit,
                        unsigned _sslport, unsigned _listenQueueLimit,
                        IPropertyTree *_keyPairInfo,
                        const char *_rowServiceConfiguration,
                        unsigned _dedicatedRowServicePort, bool _dedicatedRowServiceSSL, bool _rowServiceOnStdPort)
                : config(_config), connectMethod(_connectMethod), listenep(_listenep), pollthread(this),
                  maxThreads(_maxThreads), maxThreadsDelayMs(_maxThreadsDelayMs), maxAsyncCopy(_maxAsyncCopy),
                  parallelRequestLimit(_parallelRequestLimit), throttleDelayMs(_throttleDelayMs), throttleCPULimit(_throttleCPULimit),
                  parallelSlowRequestLimit(_parallelSlowRequestLimit), throttleSlowDelayMs(_throttleSlowDelayMs), throttleSlowCPULimit(_throttleSlowCPULimit),
                  sslport(_sslport), listenQueueLimit(_listenQueueLimit),
                  keyPairInfo(_keyPairInfo),
                  rowServiceConfiguration(_rowServiceConfiguration), dedicatedRowServicePort(_dedicatedRowServicePort), dedicatedRowServiceSSL(_dedicatedRowServiceSSL), rowServiceOnStdPort(_rowServiceOnStdPort)
            {
                stopped = false;
                started = false;
            }

            virtual ~cserv()
            {
                stopped = true;
                if (started)
                    pollthread.join();
            }

            bool init()
            {
                PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Initialized");
                started = true;
                pollthread.start(false);
                return true;
            }

            bool poll()
            {
                if (stopped||!running()) {
                    PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Stopping");
                    if (server) {
                        server->stop();
                        server.clear();
                    }
                    return false;
                }
                return true;
            }

            void run()
            {
                // Get params from HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\DaFileSrv\Parameters
                
                StringBuffer eps;
                if (listenep.isNull())
                    eps.append(listenep.port);
                else
                    listenep.getEndpointHostText(eps);

                if (connectMethod != SSLOnly)
                    PROGLOG("Opening " DAFS_SERVICE_DISPLAY_NAME " on %s", eps.str());
                if (connectMethod != SSLNone)
                {
                    SocketEndpoint sslep(listenep);
                    sslep.port = sslport;
                    eps.kill();
                    if (sslep.isNull())
                        eps.append(sslep.port);
                    else
                        sslep.getEndpointHostText(eps);
                    PROGLOG("Opening " DAFS_SERVICE_DISPLAY_NAME " on SECURE %s", eps.str());
                }

                PROGLOG("Dali File Server socket security model: %s", getSSLMethodText(connectMethod));

                const char * verstring = remoteServerVersionString();
                PROGLOG("Version: %s", verstring);
                if (dedicatedRowServicePort)
                    PROGLOG("Row service(%s) port = %u", rowServiceConfiguration, dedicatedRowServicePort);
                PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Running");
                server.setown(createRemoteFileServer(maxThreads, maxThreadsDelayMs, maxAsyncCopy, keyPairInfo));
                server->setThrottle(ThrottleStd, parallelRequestLimit, throttleDelayMs, throttleCPULimit);
                server->setThrottle(ThrottleSlow, parallelSlowRequestLimit, throttleSlowDelayMs, throttleSlowCPULimit);
                try
                {
                    if (dedicatedRowServicePort)
                    {
                        SocketEndpoint rowServiceEp(listenep); // copy listenep, incase bound by -addr
                        rowServiceEp.port = dedicatedRowServicePort;
                        server->run(config, connectMethod, listenep, sslport, listenQueueLimit, &rowServiceEp, dedicatedRowServiceSSL, rowServiceOnStdPort);
                    }
                    else
                        server->run(config, connectMethod, listenep, sslport);
                }
                catch (IException *e)
                {
                    EXCLOG(e,DAFS_SERVICE_NAME);
                    e->Release();
                }
                PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Stopped");
                stopped = true;
            }
        } service(config, connectMethod, listenep,
                maxThreads, maxThreadsDelayMs, maxAsyncCopy,
                parallelRequestLimit, throttleDelayMs, throttleCPULimit,
                parallelSlowRequestLimit, throttleSlowDelayMs, throttleSlowCPULimit, sslport, listenQueueLimit,
                keyPairInfo, rowServiceConfiguration, dedicatedRowServicePort, dedicatedRowServiceSSL, rowServiceOnStdPort);
        service.start();
        return 0;
#else
        int ret = initDaemon();
        if (ret)
            return ret;
#endif
    }
#endif

    PROGLOG("Dafilesrv starting - Build %s", hpccBuildInfo.buildTag);
    PROGLOG("Parallel request limit = %d, throttleDelayMs = %d, throttleCPULimit = %d", parallelRequestLimit, throttleDelayMs, throttleCPULimit);

    const char * verstring = remoteServerVersionString();

    StringBuffer eps;
    if (listenep.isNull())
        eps.append(listenep.port);
    else
        listenep.getEndpointHostText(eps);
    if (connectMethod != SSLOnly)
        PROGLOG("Opening Dali File Server on %s", eps.str());
    if (connectMethod != SSLNone)
    {
        SocketEndpoint sslep(listenep);
        sslep.port = sslport;
        eps.kill();
        if (sslep.isNull())
            eps.append(sslep.port);
        else
            sslep.getEndpointHostText(eps);
        PROGLOG("Opening Dali File Server on SECURE %s", eps.str());
    }

    PROGLOG("Dali File Server socket security model: %s", getSSLMethodText(connectMethod));

    PROGLOG("Version: %s", verstring);
    if (dedicatedRowServicePort)
        PROGLOG("Row service port = %u%s", dedicatedRowServicePort, dedicatedRowServiceSSL ? " SECURE" : "");

    server.setown(createRemoteFileServer(maxThreads, maxThreadsDelayMs, maxAsyncCopy, keyPairInfo));
    server->setThrottle(ThrottleStd, parallelRequestLimit, throttleDelayMs, throttleCPULimit);
    server->setThrottle(ThrottleSlow, parallelSlowRequestLimit, throttleSlowDelayMs, throttleSlowCPULimit);

#ifndef _CONTAINERIZED
    class CPerfHook : public CSimpleInterfaceOf<IPerfMonHook>
    {
    public:
        virtual void processPerfStats(unsigned processorUsage, unsigned memoryUsage, unsigned memoryTotal, unsigned __int64 fistDiskUsage, unsigned __int64 firstDiskTotal, unsigned __int64 secondDiskUsage, unsigned __int64 secondDiskTotal, unsigned threadCount)
        {
        }
        virtual StringBuffer &extraLogging(StringBuffer &extra)
        {
            return server->getStats(extra.newline(), true);
        }
        virtual void log(int level, const char *msg)
        {
            PROGLOG("%s", msg);
        }
    } perfHook;
    startPerformanceMonitor(10*60*1000, PerfMonStandard, &perfHook);
#endif

    writeSentinelFile(sentinelFile);
    try
    {
        if (dedicatedRowServicePort)
        {
            SocketEndpoint rowServiceEp(listenep); // copy listenep, incase bound by -addr
            rowServiceEp.port = dedicatedRowServicePort;
            server->run(config, connectMethod, listenep, sslport, listenQueueLimit, &rowServiceEp, dedicatedRowServiceSSL, rowServiceOnStdPort);
        }
        else
            server->run(config, connectMethod, listenep, sslport);
    }
    catch (IException *e)
    {
        EXCLOG(e,"DAFILESRV");
        if (e->errorCode() == DAFSERR_serverinit_failed)
            removeSentinelFile(sentinelFile); // so init does not keep trying to start it ...
        e->Release();
    }

#ifndef _CONTAINERIZED
    stopPerformanceMonitor();
#endif

    if (server)
        server->stop();
    server.clear();
    PROGLOG("Stopped Dali File Server");

    return 0;
}

