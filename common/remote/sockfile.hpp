/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#define REMOTE_API __declspec(dllexport)
#else
#define REMOTE_API __declspec(dllimport)
#endif

#define RFEnoerror      0

interface IRemoteFileServer : public IInterface
{
public:
    virtual void run(SocketEndpoint &listenep) = 0;
    virtual void stop() = 0;
    virtual unsigned idleTime() = 0; // in ms
};

#define FILESRV_VERSION 17 // don't forget VERSTRING in sockfile.cpp


extern REMOTE_API IFile * createRemoteFile(SocketEndpoint &ep,const char * _filename); // takes ownershop of socket
extern REMOTE_API unsigned getRemoteVersion(ISocket * _socket, StringBuffer &ver);
extern REMOTE_API unsigned stopRemoteServer(ISocket * _socket);
extern REMOTE_API const char *remoteServerVersionString();
extern REMOTE_API IRemoteFileServer * createRemoteFileServer();
extern REMOTE_API int setDafsTrace(ISocket * socket,byte flags);
extern REMOTE_API bool enableDafsAuthentication(bool on);
extern int remoteExec(ISocket * socket, const char *cmdline, const char *workdir,bool sync,
                size32_t insize, void *inbuf, MemoryBuffer *outbuf);
extern void remoteExtractBlobElements(const SocketEndpoint &ep, const char * prefix, const char * filename, ExtractedBlobArray & extracted);
extern int getDafsInfo(ISocket * socket,StringBuffer &retstr);
extern void setDafsEndpointPort(SocketEndpoint &ep);
extern void setDafsLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir);

// client only
extern void clientSetDaliServixSocketCaching(bool set);
extern void clientCacheFileConnect(SocketEndpoint &ep,unsigned timeout);
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
