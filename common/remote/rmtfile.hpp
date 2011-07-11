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

#ifndef RMTFILE_HPP
#define RMTFILE_HPP

#include "jsocket.hpp"
#include "jfile.hpp"

#ifdef DAFILESRV_LOCAL
#define REMOTE_API
#else
#ifdef REMOTE_EXPORTS
#define REMOTE_API __declspec(dllexport)
#else
#define REMOTE_API __declspec(dllimport)
#endif
#endif

enum DAFS_OS
{
    DAFS_OSunknown,
    DAFS_OSwindows,
    DAFS_OSlinux,
    DAFS_OSsolaris
};

extern REMOTE_API void filenameToUrl(StringBuffer & out, const char * filename);

extern REMOTE_API unsigned short getDaliServixPort();  // assumed just the one for now
extern REMOTE_API void setCanAccessDirectly(RemoteFilename & file,bool set);
extern REMOTE_API void setDaliServixSocketCaching(bool set);
extern REMOTE_API void cacheFileConnect(IFile *file,unsigned timeout);
extern REMOTE_API bool canAccessDirectly(const RemoteFilename & file);
extern REMOTE_API IFile *createDaliServixFile(const RemoteFilename & file);
extern REMOTE_API bool testDaliServixPresent(const IpAddress &ip);
extern REMOTE_API bool testDaliServixPresent(const SocketEndpoint &ep);
extern REMOTE_API unsigned getDaliServixVersion(const IpAddress &ip,StringBuffer &ver);
extern REMOTE_API unsigned getDaliServixVersion(const SocketEndpoint &ep,StringBuffer &ver);
extern REMOTE_API DAFS_OS getDaliServixOs(const SocketEndpoint &ep);

extern REMOTE_API void setLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir);
// redirects a daliservix file to a local mount. To remove redirect use NULL for mount dir or NULL for dir


extern REMOTE_API int remoteExec(const SocketEndpoint &ep,const char *cmdline, const char *workdir,bool sync,
                                 size32_t insize, void *inbuf, MemoryBuffer *outbuf);

extern REMOTE_API void remoteExtractBlobElements(const char * prefix, const RemoteFilename &file, ExtractedBlobArray & extracted);

extern REMOTE_API int setDafileSvrTraceFlags(const SocketEndpoint &ep,byte flags);
extern REMOTE_API int getDafileSvrInfo(const SocketEndpoint &ep,StringBuffer &retstr);

extern REMOTE_API void disconnectRemoteFile(IFile *file);
extern REMOTE_API void disconnectRemoteIoOnExit(IFileIO *fileio,bool set=true);

extern REMOTE_API bool resetRemoteFilename(IFile *file, const char *newname); // returns false if not remote


extern REMOTE_API void enableAuthentication(bool set=true); // default enabled for clients, disabled for server

extern REMOTE_API bool asyncCopyFileSection(const char *uuid,   // from genUUID - must be same for subsequent calls
                            IFile *from,                        // expected to be remote
                            RemoteFilename &to,
                            offset_t toofs,                     // (offset_t)-1 created file and copies to start
                            offset_t fromofs,
                            offset_t size,                      // (offset_t)-1 for all file
                            ICopyFileProgress *progress,
                            unsigned timeout                    // 0 to start, non-zero to wait
                        ); // returns true when done

extern REMOTE_API void setRemoteFileTimeouts(unsigned maxconnecttime,unsigned maxreadtime);

#define DAFS_VALIDATE_CONNECT_FAIL  (0x01)
#define DAFS_VALIDATE_BAD_VERSION   (0x02)
#define DAFS_VALIDATE_WRITE_FAIL_C  (0x12)
#define DAFS_VALIDATE_READ_FAIL_C   (0x14)
#define DAFS_VALIDATE_DISK_FULL_C   (0x18)
#define DAFS_VALIDATE_WRITE_FAIL_D  (0x22)
#define DAFS_VALIDATE_READ_FAIL_D   (0x24)
#define DAFS_VALIDATE_DISK_FULL_D   (0x28)
#define DAFS_SCRIPT_FAIL            (0x40)
                                
extern REMOTE_API unsigned validateNodes(const SocketEndpointArray &ep,bool chkc,bool chkd,bool chkver,const char *script,unsigned scripttimeout,SocketEndpointArray &failures,UnsignedArray &failedcodes, StringArray &failedmessages, const char *filename=NULL);


#endif
