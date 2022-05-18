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

#ifndef RMTCLIENT_HPP
#define RMTCLIENT_HPP

#include "jsocket.hpp"
#include "jfile.hpp"

#include "remoteerr.hpp"
#include "dafscommon.hpp"

#ifdef DAFSCLIENT_EXPORTS
#define DAFSCLIENT_API DECL_EXPORT
#else
#define DAFSCLIENT_API DECL_IMPORT
#endif

#define DAFILESRV_METAINFOVERSION 2

#define DAFILESRV_STREAMREAD_MINVERSION 22
#define DAFILESRV_STREAMGENERAL_MINVERSION 25

typedef int RemoteFileIOHandle;
// backward compatible modes
typedef enum { compatIFSHnone, compatIFSHread, compatIFSHwrite, compatIFSHexec, compatIFSHall} compatIFSHmode;

enum DAFS_OS
{
    DAFS_OSunknown,
    DAFS_OSwindows,
    DAFS_OSlinux,
    DAFS_OSsolaris
};

extern DAFSCLIENT_API IFile * createRemoteFile(SocketEndpoint &ep,const char * _filename);


interface IDaFsConnection;
extern DAFSCLIENT_API unsigned short getDaliServixPort();  // assumed just the one for now
extern DAFSCLIENT_API void setCanAccessDirectly(RemoteFilename & file,bool set);
extern DAFSCLIENT_API unsigned getRemoteVersion(ISocket * _socket, StringBuffer &ver);
extern DAFSCLIENT_API unsigned getCachedRemoteVersion(IDaFsConnection &daFsConnection);
extern DAFSCLIENT_API unsigned getCachedRemoteVersion(const SocketEndpoint &ep, bool secure);
extern DAFSCLIENT_API int setDafsTrace(ISocket * socket,byte flags);
extern DAFSCLIENT_API int setDafsThrottleLimit(ISocket * socket, ThrottleClass throttleClass, unsigned throttleLimit, unsigned throttleDelayMs, unsigned throttleCPULimit, unsigned queueLimit, StringBuffer *errMsg=NULL);
extern DAFSCLIENT_API int getDafsInfo(ISocket * socket, unsigned level, StringBuffer &retstr);
extern DAFSCLIENT_API void setDafsEndpointPort(SocketEndpoint &ep);
extern DAFSCLIENT_API void setDafsLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir);
extern DAFSCLIENT_API ISocket *connectDafs(SocketEndpoint &ep, unsigned timeoutms); // NOTE: might alter ep.port if configured for multiple ports ...
extern DAFSCLIENT_API ISocket *checkSocketSecure(ISocket *socket);
extern DAFSCLIENT_API unsigned short getActiveDaliServixPort(const IpAddress &ip);
extern DAFSCLIENT_API unsigned getDaliServixVersion(const IpAddress &ip,StringBuffer &ver);
extern DAFSCLIENT_API unsigned getDaliServixVersion(const SocketEndpoint &ep,StringBuffer &ver);
extern DAFSCLIENT_API DAFS_OS getDaliServixOs(const SocketEndpoint &ep);
extern DAFSCLIENT_API bool testDaliServixPresent(const IpAddress &ip);
extern DAFSCLIENT_API int sendDaFsFtSlaveCmd(ISocket *socket, MemoryBuffer &cmdBuffer);



extern DAFSCLIENT_API unsigned stopRemoteServer(ISocket *socket);


extern DAFSCLIENT_API int setDafileSvrTraceFlags(const SocketEndpoint &ep,byte flags);
extern DAFSCLIENT_API int setDafileSvrThrottleLimit(const SocketEndpoint &_ep, ThrottleClass throttleClass, unsigned throttleLimit, unsigned throttleDelayMs, unsigned throttleCPULimit, unsigned queueLimit, StringBuffer *errMsg=NULL);
extern DAFSCLIENT_API int getDafileSvrInfo(const SocketEndpoint &ep, unsigned level, StringBuffer &retstr);


extern DAFSCLIENT_API RemoteFileCommandType queryRemoteStreamCmd(); // used by testsocket only

interface IDaFsConnection : extends IInterface
{
    virtual void close(int handle) = 0;
    virtual void send(MemoryBuffer &sendMb, MemoryBuffer &reply) = 0;
    virtual unsigned getVersion(StringBuffer &ver) = 0;
    virtual const SocketEndpoint &queryEp() const = 0;
};

extern DAFSCLIENT_API IDAFS_Exception *createDafsException(int code, const char *msg);
extern DAFSCLIENT_API IDAFS_Exception *createDafsExceptionVA(int code, const char *format, va_list args) __attribute__((format(printf,2,0)));
extern DAFSCLIENT_API IDAFS_Exception *createDafsExceptionV(int code, const char *format, ...) __attribute__((format(printf,2,3)));

extern DAFSCLIENT_API IDaFsConnection *createDaFsConnection(const SocketEndpoint &ep, DAFSConnectCfg connectMethod, const char *tracing);

extern DAFSCLIENT_API unsigned stopRemoteServer(ISocket * _socket);

#endif
