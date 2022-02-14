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

#ifndef SOCKFILE_HPP
#define SOCKFILE_HPP

#include "jsocket.hpp"

#ifdef DAFSSERVER_EXPORTS
#define DAFSSERVER_API DECL_EXPORT
#else
#define DAFSSERVER_API DECL_IMPORT
#endif

#define DAFILESRV_METAINFOVERSION 2

#define DAFILESRV_STREAMREAD_MINVERSION 22
#define DAFILESRV_STREAMGENERAL_MINVERSION 25

// RemoteFileServer throttling defaults
#define DEFAULT_THREADLIMIT 100
#define DEFAULT_THREADLIMITDELAYMS (60*1000)
#define DEFAULT_ASYNCCOPYMAX 10

#define DEFAULT_STDCMD_PARALLELREQUESTLIMIT 80
#define DEFAULT_STDCMD_THROTTLEDELAYMS 1000
#define DEFAULT_STDCMD_THROTTLECPULIMIT 85
#define DEFAULT_STDCMD_THROTTLEQUEUELIMIT 1000

#define DEFAULT_SLOWCMD_PARALLELREQUESTLIMIT 20
#define DEFAULT_SLOWCMD_THROTTLEDELAYMS 5000
#define DEFAULT_SLOWCMD_THROTTLECPULIMIT 75
#define DEFAULT_SLOWCMD_THROTTLEQUEUELIMIT 1000


enum RowServiceCfg
{
    rs_off,     // No dedicated row service, allows row service commands on std. dafilesrv port.
    rs_on,      // Dedicated row service on own port accepting authorized signed connections only. Row service commands on std. dafilersv port will be refused.
    rs_both,    // Dedicated row service on own port accepting authorized signed connections only. Still accepts unsigned connection on std. dafilesrv port
    rs_onssl,   // Same as rs_on, but SSL
    rs_bothssl  // Same as rs_only, but SSL
};

interface IRemoteFileServer : extends IInterface
{
    virtual void run(IPropertyTree *componentConfig, DAFSConnectCfg connectMethod, const SocketEndpoint &listenep, unsigned sslPort=0, const SocketEndpoint *rowServiceEp=nullptr, bool rowServiceSSL=false, bool rowServiceOnStdPort=true) = 0;
    virtual void run(IPropertyTree *componentConfig, DAFSConnectCfg _connectMethod, ISocket *listenSocket, ISocket *secureSocket, ISocket *rowServiceSocket) = 0;
    virtual void stop() = 0;
    virtual unsigned idleTime() = 0; // in ms
    virtual void setThrottle(ThrottleClass throttleClass, unsigned limit, unsigned delayMs=DEFAULT_STDCMD_THROTTLEDELAYMS, unsigned cpuThreshold=DEFAULT_STDCMD_THROTTLECPULIMIT, unsigned queueLimit=DEFAULT_STDCMD_THROTTLEQUEUELIMIT) = 0;
    virtual StringBuffer &getStats(StringBuffer &stats, bool reset) = 0;
};

interface IRemoteRowServer : extends IInterface
{
    virtual void run(unsigned port=0) = 0;
    virtual void stop() = 0;
    virtual unsigned idleTime() = 0; // in ms
    virtual void setThrottle(ThrottleClass throttleClass, unsigned limit, unsigned delayMs=DEFAULT_STDCMD_THROTTLEDELAYMS, unsigned cpuThreshold=DEFAULT_STDCMD_THROTTLECPULIMIT, unsigned queueLimit=DEFAULT_STDCMD_THROTTLEQUEUELIMIT) = 0;
    virtual StringBuffer &getStats(StringBuffer &stats, bool reset) = 0;
};

interface IKeyManager;
interface IDelayedFile;
interface IDaFsConnection;

extern DAFSSERVER_API const char *remoteServerVersionString();
extern DAFSSERVER_API IRemoteFileServer * createRemoteFileServer(unsigned maxThreads=DEFAULT_THREADLIMIT, unsigned maxThreadsDelayMs=DEFAULT_THREADLIMITDELAYMS, unsigned maxAsyncCopy=DEFAULT_ASYNCCOPYMAX, IPropertyTree *keyPairInfo=nullptr);
extern DAFSSERVER_API int setDaliServerTrace(byte flags);
extern DAFSSERVER_API bool enableDaliServerAuthentication(bool on);


#endif
