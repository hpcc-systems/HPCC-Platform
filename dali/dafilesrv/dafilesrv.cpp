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

#include "build-config.h"
#include "jlib.hpp"
#include "jiface.hpp"
#include "jutil.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "jmisc.hpp"
#include "dalienv.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif


#include "sockfile.hpp"

void usage()
{
    printf("dafilesrv usage:\n");
    printf("    dafilesrv -T<n> <port> <-NOSSL> [<send-buff-size-kb> <recv-buff-size-kb>]\n");
    printf("                                                  -- run test local\n");
    printf("    dafilesrv -D [ -L <log-dir> ] [ -LOCAL ]      -- run as linux daemon\n");
    printf("    dafilesrv -R                                  -- run remote (linux daemon, windows standalone)\n");
    printf("    dafilesrv -install                            -- install windows service\n");
    printf("    dafilesrv -remove                             -- remove windows service\n\n");
    
    printf("add -A to enable authentication to the above \n\n");
    printf("add -I <instance name>  to specify an instance name\n\n");
    printf("add -NOSSL to disable SSL sockets, even when specified in configuration\n\n");
    printf("Standard port is %d\n",DAFILESRV_PORT);
    printf("Standard SSL port is %d (certificate specs required in environment.conf)\n",SECURE_DAFILESRV_PORT);
    printf("Version:  %s\n\n",remoteServerVersionString());
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
    ERRLOG( "%s(%d): %s", s, dwError, lpBuffer );   
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
                    ERRLOG("%s failed to stop",servicedisplayname); 
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
    int ret = make_daemon(true);
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

int main(int argc,char **argv) 
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
    int i = 1;
    bool isdaemon = (memicmp(argv[0]+strlen(argv[0])-4,".exe",4)==0);
    // bit of a kludge for windows - if .exe not specified then not daemon
    bool locallisten = false;
    const char *logdir=NULL;
    bool requireauthenticate = false;
    StringBuffer logDir;
    StringBuffer instanceName;

   //Get SSL Settings
    const char *    sslCertFile;
    bool            useSSL;
    unsigned short  dafsPort;//DAFILESRV_PORT or SECURE_DAFILESRV_PORT
    querySecuritySettings(&useSSL, &dafsPort, &sslCertFile, NULL);

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

    Owned<IPropertyTree> env = getHPCCEnvironment();
    if (env)
    {
        StringBuffer dafilesrvPath("Software/DafilesrvProcess");
        if (instanceName.length())
            dafilesrvPath.appendf("[@name=\"%s\"]", instanceName.str());
        IPropertyTree *daFileSrv = env->queryPropTree(dafilesrvPath);
        if (daFileSrv)
        {
            // global DaFileSrv settings:

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

            // any overrides by Instance definitions?
            // NB: This won't work if netAddress is "." or if we start supporting hostnames there
            StringBuffer ipStr;
            queryHostIP().getIpText(ipStr);
            VStringBuffer daFileSrvPath("Instance[@netAddress=\"%s\"]", ipStr.str());
            IPropertyTree *dafileSrvInstance = daFileSrv->queryPropTree(daFileSrvPath);
            if (dafileSrvInstance)
            {
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
            }
        }
    }

    while (argc>i) {
        if (stricmp(argv[i],"-D")==0) {
            i++;
            isdaemon = true;
        }
        else if (stricmp(argv[i],"-R")==0) { // for remote run
            i++;
#ifdef _WIN32
            isdaemon = false;
#else
            isdaemon = true;
#endif
        }
        else if (stricmp(argv[i],"-A")==0) { 
            i++;
            requireauthenticate = true;
        }
        else if ((argv[i][0]=='-')&&(toupper(argv[i][1])=='T')&&(!argv[i][2]||isdigit(argv[i][2]))) {
            if (argv[i][2])
                setDafsTrace(NULL,(byte)atoi(argv[i]+2));
            i++;
            isdaemon = false;
        }
        else if ((argc>i+1)&&(stricmp(argv[i],"-L")==0)) { 
            i++;
            logDir.clear().append(argv[i++]);
        }
        else if ((argc>i+1)&&(stricmp(argv[i],"-I")==0)) {
            i++;
            instanceName.clear().append(argv[i++]);
        }
        else if (stricmp(argv[i],"-LOCAL")==0) { 
            i++;
            locallisten = true;
        }
        else if (stricmp(argv[i],"-NOSSL")==0) {//overrides config setting
            i++;
            if (useSSL)
            {
                PROGLOG("DaFileSrv SSL specified in config but overridden by -NOSSL in command line");
                useSSL = false;
                dafsPort = DAFILESRV_PORT;
            }
        }
        else
            break;
    }

    if (useSSL && !sslCertFile)
    {
        ERRLOG("DaFileSrv SSL specified but certificate file information missing from environment.conf");
        exit(-1);
    }

    if (0 == logDir.length())
    {
        getConfigurationDirectory(NULL,"log","dafilesrv",instanceName.str(),logDir);
        if (0 == logDir.length())
            logDir.append(".");
    }
    if (instanceName.length())
    {
        addPathSepChar(logDir);
        logDir.append(instanceName.str());
    }

#ifdef _WIN32
    if ((argc>i)&&(stricmp(argv[i],"-install")==0)) {
        if (installService(DAFS_SERVICE_NAME,DAFS_SERVICE_DISPLAY_NAME,NULL)) {
            PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Installed");
            return 0;
        }
        return 1;
    }
    if ((argc>i)&&(stricmp(argv[i],"-remove")==0)) {
        if (uninstallService(DAFS_SERVICE_NAME,DAFS_SERVICE_DISPLAY_NAME)) {
            PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Uninstalled");
            return 0;
        }
        return 1;
    }
#endif
    if (argc == i)
        listenep.port = dafsPort;
    else {
        if (strchr(argv[i],'.')||!isdigit(argv[i][0]))
            listenep.set(argv[i], dafsPort);
        else
            listenep.port = atoi(argv[i]);
        if (listenep.port==0) {
            usage();
            exit(-1);
        }
        sendbufsize = (argc>i+1)?(atoi(argv[i+1])*1024):0;
        recvbufsize = (argc>i+2)?(atoi(argv[i+2])*1024):0;
    }
    if (isdaemon) {
#ifdef _WIN32
        class cserv: public CService
        {
            bool stopped;
            bool started;
            SocketEndpoint listenep;
            bool useSSL;
            bool requireauthenticate;
            unsigned maxThreads;
            unsigned maxThreadsDelayMs;
            unsigned maxAsyncCopy;
            unsigned parallelRequestLimit;
            unsigned throttleDelayMs;
            unsigned throttleCPULimit;
            unsigned parallelSlowRequestLimit;
            unsigned throttleSlowDelayMs;
            unsigned throttleSlowCPULimit;

            
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

            cserv(SocketEndpoint _listenep, bool _useSSL,
                        unsigned _maxThreads, unsigned _maxThreadsDelayMs, unsigned _maxAsyncCopy,
                        unsigned _parallelRequestLimit, unsigned _throttleDelayMs, unsigned _throttleCPULimit,
                        unsigned _parallelSlowRequestLimit, unsigned _throttleSlowDelayMs, unsigned _throttleSlowCPULimit)
            : listenep(_listenep),useSSL(_useSSL),pollthread(this),
                  maxThreads(_maxThreads), maxThreadsDelayMs(_maxThreadsDelayMs), maxAsyncCopy(_maxAsyncCopy),
                  parallelRequestLimit(_parallelRequestLimit), throttleDelayMs(_throttleDelayMs), throttleCPULimit(_throttleCPULimit),
                  parallelSlowRequestLimit(_parallelSlowRequestLimit), throttleSlowDelayMs(_throttleSlowDelayMs), throttleSlowCPULimit(_throttleSlowCPULimit)
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
                pollthread.start();
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
                
                int requireauthenticate=0;
                HKEY hkey;
                if (RegOpenKeyEx(HKEY_LOCAL_MACHINE,
                                 "SYSTEM\\CurrentControlSet\\Services\\DaFileSrv\\Parameters",
                                 0,
                                 KEY_QUERY_VALUE,
                                 &hkey) == ERROR_SUCCESS) {
                    DWORD dwType = 0;
                    DWORD dwSize = sizeof(requireauthenticate);
                    RegQueryValueEx(hkey,
                                    "RequireAuthentication",
                                    NULL,
                                    &dwType,
                                    (BYTE*)&requireauthenticate,
                                    &dwSize);
                    RegCloseKey(hkey);
                }
                StringBuffer eps;
                if (listenep.isNull())
                    eps.append(listenep.port);
                else
                    listenep.getUrlStr(eps);
                enableDafsAuthentication(requireauthenticate!=0);
                PROGLOG("Opening " DAFS_SERVICE_DISPLAY_NAME " on %s%s", useSSL?"SECURE ":"",eps.str());
                const char * verstring = remoteServerVersionString();
                PROGLOG("Version: %s", verstring);
                PROGLOG("Authentication:%s required",requireauthenticate?"":" not");
                PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Running");
                server.setown(createRemoteFileServer(maxThreads, maxThreadsDelayMs, maxAsyncCopy));
                server->setThrottle(ThrottleStd, parallelRequestLimit, throttleDelayMs, throttleCPULimit);
                server->setThrottle(ThrottleSlow, parallelSlowRequestLimit, throttleSlowDelayMs, throttleSlowCPULimit);
                try {
                    server->run(listenep, useSSL);
                }
                catch (IException *e) {
                    EXCLOG(e,DAFS_SERVICE_NAME);
                    e->Release();
                }
                PROGLOG(DAFS_SERVICE_DISPLAY_NAME " Stopped");
                stopped = true;
            }
        } service(listenep, useSSL,
                maxThreads, maxThreadsDelayMs, maxAsyncCopy,
                parallelRequestLimit, throttleDelayMs, throttleCPULimit,
                parallelSlowRequestLimit, throttleSlowDelayMs, throttleSlowCPULimit);
        service.start();
        return 0;
#else
        int ret = initDaemon();
        if (ret)
            return ret;
#endif
    }
    {
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(logDir.str(), "DAFILESRV");
        lf->setCreateAliasFile(false);
        lf->setMaxDetail(TopDetail);
        lf->beginLogging();
    }

    PROGLOG("Dafilesrv starting - Build %s", BUILD_TAG);
    PROGLOG("Parallel request limit = %d, throttleDelayMs = %d, throttleCPULimit = %d", parallelRequestLimit, throttleDelayMs, throttleCPULimit);

    const char * verstring = remoteServerVersionString();
    StringBuffer eps;
    if (listenep.isNull())
        eps.append(listenep.port);
    else
        listenep.getUrlStr(eps);
    enableDafsAuthentication(requireauthenticate);
    PROGLOG("Opening Dali File Server on %s%s", useSSL?"SECURE ":"",eps.str());
    PROGLOG("Version: %s", verstring);
    PROGLOG("Authentication:%s required",requireauthenticate?"":" not");
    server.setown(createRemoteFileServer(maxThreads, maxThreadsDelayMs, maxAsyncCopy));
    server->setThrottle(ThrottleStd, parallelRequestLimit, throttleDelayMs, throttleCPULimit);
    server->setThrottle(ThrottleSlow, parallelSlowRequestLimit, throttleSlowDelayMs, throttleSlowCPULimit);
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
    writeSentinelFile(sentinelFile);
    try
    {
        server->run(listenep, useSSL);
    }
    catch (IException *e)
    {
        EXCLOG(e,"DAFILESRV");
        e->Release();
    }
    stopPerformanceMonitor();
    if (server)
        server->stop();
    server.clear();
    PROGLOG("Stopped Dali File Server");

    return 0;
}

