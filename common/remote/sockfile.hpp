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
