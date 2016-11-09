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
#include "jfile.hpp"

#ifdef REMOTE_EXPORTS
#define REMOTE_API DECL_EXPORT
#else
#define REMOTE_API DECL_IMPORT
#endif

#define RFEnoerror      0

enum ThrottleClass
{
    ThrottleStd,
    ThrottleSlow,
    ThrottleClassMax
};

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

interface IRemoteFileServer : extends IInterface
{
    virtual void run(SocketEndpoint &listenep, bool useSSL = false) = 0;
    virtual void stop() = 0;
    virtual unsigned idleTime() = 0; // in ms
    virtual void setThrottle(ThrottleClass throttleClass, unsigned limit, unsigned delayMs=DEFAULT_STDCMD_THROTTLEDELAYMS, unsigned cpuThreshold=DEFAULT_STDCMD_THROTTLECPULIMIT, unsigned queueLimit=DEFAULT_STDCMD_THROTTLEQUEUELIMIT) = 0;
    virtual StringBuffer &getStats(StringBuffer &stats, bool reset) = 0;
};

#define FILESRV_VERSION 19 // don't forget VERSTRING in sockfile.cpp

extern REMOTE_API IFile * createRemoteFile(SocketEndpoint &ep,const char * _filename); // takes ownershop of socket
extern REMOTE_API unsigned getRemoteVersion(ISocket * _socket, StringBuffer &ver);
extern REMOTE_API unsigned stopRemoteServer(ISocket * _socket);
extern REMOTE_API const char *remoteServerVersionString();
extern REMOTE_API IRemoteFileServer * createRemoteFileServer(unsigned maxThreads=DEFAULT_THREADLIMIT, unsigned maxThreadsDelayMs=DEFAULT_THREADLIMITDELAYMS, unsigned maxAsyncCopy=DEFAULT_ASYNCCOPYMAX);
extern REMOTE_API int setDafsTrace(ISocket * socket,byte flags);
extern REMOTE_API int setDafsThrottleLimit(ISocket * socket, ThrottleClass throttleClass, unsigned throttleLimit, unsigned throttleDelayMs, unsigned throttleCPULimit, unsigned queueLimit, StringBuffer *errMsg=NULL);
extern REMOTE_API bool enableDafsAuthentication(bool on);
extern int remoteExec(ISocket * socket, const char *cmdline, const char *workdir,bool sync,
                size32_t insize, void *inbuf, MemoryBuffer *outbuf);
extern void remoteExtractBlobElements(const SocketEndpoint &ep, const char * prefix, const char * filename, ExtractedBlobArray & extracted);
extern int getDafsInfo(ISocket * socket, unsigned level, StringBuffer &retstr);
extern void setDafsEndpointPort(SocketEndpoint &ep);
extern void setDafsLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir);

// client only
extern void clientSetDaliServixSocketCaching(bool set);
extern void clientDisconnectRemoteFile(IFile *file);
extern void clientDisconnectRemoteIoOnExit(IFileIO *fileio,bool set);

extern bool clientResetFilename(IFile *file, const char *newname); // returns false if not remote

extern bool clientAsyncCopyFileSection(const char *uuid,    // from genUUID - must be same for subsequent calls
                        IFile *from,                        // expected to be remote
                        RemoteFilename &to,
                        offset_t toofs,                     // (offset_t)-1 created file and copies to start
                        offset_t fromofs,
                        offset_t size,                      // (offset_t)-1 for all file
                        ICopyFileProgress *progress,
                        unsigned timeout                    // 0 to start, non-zero to wait
                        ); // returns true when done

extern void clientSetRemoteFileTimeouts(unsigned maxconnecttime,unsigned maxreadtime);
extern void clientAddSocketToCache(SocketEndpoint &ep,ISocket *socket);

#endif
