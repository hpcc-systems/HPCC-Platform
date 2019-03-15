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

#define DAFILESRV_METAINFOVERSION 2

#define DAFILESRV_STREAMREAD_MINVERSION 22
#define DAFILESRV_STREAMGENERAL_MINVERSION 25

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
    virtual void run(DAFSConnectCfg connectMethod, const SocketEndpoint &listenep, unsigned sslPort=0, const SocketEndpoint *rowServiceEp=nullptr, bool rowServiceSSL=false, bool rowServiceOnStdPort=true) = 0;
    virtual void run(DAFSConnectCfg _connectMethod, ISocket *listenSocket, ISocket *secureSocket, ISocket *rowServiceSocket) = 0;
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

#define FILESRV_VERSION 25 // don't forget VERSTRING in sockfile.cpp

interface IKeyManager;
interface IDelayedFile;
interface IDaFsConnection;

extern REMOTE_API IFile * createRemoteFile(SocketEndpoint &ep,const char * _filename);
extern REMOTE_API unsigned getRemoteVersion(ISocket * _socket, StringBuffer &ver);
extern REMOTE_API unsigned getCachedRemoteVersion(IDaFsConnection &daFsConnection);
extern REMOTE_API unsigned getCachedRemoteVersion(const SocketEndpoint &ep, bool secure);
extern REMOTE_API unsigned stopRemoteServer(ISocket * _socket);
extern REMOTE_API const char *remoteServerVersionString();
extern REMOTE_API IRemoteFileServer * createRemoteFileServer(unsigned maxThreads=DEFAULT_THREADLIMIT, unsigned maxThreadsDelayMs=DEFAULT_THREADLIMITDELAYMS, unsigned maxAsyncCopy=DEFAULT_ASYNCCOPYMAX, IPropertyTree *keyPairInfo=nullptr);
extern REMOTE_API int setDafsTrace(ISocket * socket,byte flags);
extern REMOTE_API int setDafsThrottleLimit(ISocket * socket, ThrottleClass throttleClass, unsigned throttleLimit, unsigned throttleDelayMs, unsigned throttleCPULimit, unsigned queueLimit, StringBuffer *errMsg=NULL);
extern REMOTE_API bool enableDafsAuthentication(bool on);
extern REMOTE_API void remoteExtractBlobElements(const SocketEndpoint &ep, const char * prefix, const char * filename, ExtractedBlobArray & extracted);
extern REMOTE_API int getDafsInfo(ISocket * socket, unsigned level, StringBuffer &retstr);
extern REMOTE_API void setDafsEndpointPort(SocketEndpoint &ep);
extern REMOTE_API void setDafsLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir);
extern REMOTE_API ISocket *connectDafs(SocketEndpoint &ep, unsigned timeoutms); // NOTE: might alter ep.port if configured for multiple ports ...
extern REMOTE_API ISocket *checkSocketSecure(ISocket *socket);


extern REMOTE_API void setRemoteOutputCompressionDefault(const char *type);
extern REMOTE_API const char *queryOutputCompressionDefault();

interface IOutputMetaData;
class RowFilter;
interface IRemoteFileIO : extends IFileIO
{
    virtual void addVirtualFieldMapping(const char *fieldName, const char *fieldValue) = 0;
    virtual void ensureAvailable() = 0;
};
extern REMOTE_API IRemoteFileIO *createRemoteFilteredFile(SocketEndpoint &ep, const char * filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, bool compressed, bool grouped, unsigned __int64 chooseNLimit);

interface IIndexLookup;
extern REMOTE_API IIndexLookup *createRemoteFilteredKey(SocketEndpoint &ep, const char * filename, unsigned crc, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, unsigned __int64 chooseNLimit);

typedef unsigned char RemoteFileCommandType;
extern REMOTE_API RemoteFileCommandType queryRemoteStreamCmd(); // used by testsocket only

interface IFileDescriptor;
typedef IFileDescriptor *(*FileDescriptorFactoryType)(IPropertyTree *);
extern REMOTE_API void configureRemoteCreateFileDescriptorCB(FileDescriptorFactoryType cb);


// client only
extern FileDescriptorFactoryType queryRemoteCreateFileDescriptorCB();
interface IDaFsConnection : extends IInterface
{
    virtual void close(int handle) = 0;
    virtual void send(MemoryBuffer &sendMb, MemoryBuffer &reply) = 0;
    virtual unsigned getVersion(StringBuffer &ver) = 0;
    virtual const SocketEndpoint &queryEp() const = 0;
};

IDaFsConnection *createDaFsConnection(const SocketEndpoint &ep, DAFSConnectCfg connectMethod, const char *tracing);

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
