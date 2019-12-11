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

#ifndef RMTFILE_HPP
#define RMTFILE_HPP

#include "jsocket.hpp"
#include "jfile.hpp"

#include "dafscommon.hpp"
#include "rmtclient.hpp"

#ifdef DAFSCLIENT_EXPORTS
#define DAFSCLIENT_API DECL_EXPORT
#else
#define DAFSCLIENT_API DECL_IMPORT
#endif

extern DAFSCLIENT_API void filenameToUrl(StringBuffer & out, const char * filename);

interface IDaFileSrvHook : extends IRemoteFileCreateHook
{
    virtual void forceRemote(const char *pattern) = 0; // NB: forces all local files matching pattern to be remote reads
    virtual void addSubnetFilter(const char *subnet, const char *mask, const char *dir, const char *sourceRange, bool trace) = 0;
    virtual void addRangeFilter(const char *range, const char *dir, const char *sourceRange, bool trace) = 0;
    virtual IPropertyTree *addFilters(IPropertyTree *filters, const SocketEndpoint *ipAddress) = 0;
    virtual IPropertyTree *addMyFilters(IPropertyTree *filters, SocketEndpoint *myEp=NULL) = 0;
    virtual void clearFilters() = 0;
};
extern DAFSCLIENT_API IDaFileSrvHook *queryDaFileSrvHook();

extern DAFSCLIENT_API void setDaliServixSocketCaching(bool set);
extern DAFSCLIENT_API bool canAccessDirectly(const RemoteFilename & file);
extern DAFSCLIENT_API IFile *createDaliServixFile(const RemoteFilename & file);
extern DAFSCLIENT_API void enableForceRemoteReads(); // forces file reads to be remote reads if they match environment setting 'forceRemotePattern' pattern.
extern DAFSCLIENT_API bool testForceRemote(const char *path); // return true if forceRemote setup/pattern will make this path a remote read.

extern DAFSCLIENT_API IDirectoryIterator *createRemoteDirectorIterator(const SocketEndpoint &ep, const char *name, MemoryBuffer &state);
extern DAFSCLIENT_API bool serializeRemoteDirectoryIterator(MemoryBuffer &tgt, IDirectoryIterator *iter, size32_t bufsize, bool first);
extern DAFSCLIENT_API void serializeRemoteDirectoryDiff(MemoryBuffer &tgt, IDirectoryDifferenceIterator *iter);

extern DAFSCLIENT_API void setLocalMountRedirect(const IpAddress &ip,const char *dir,const char *mountdir);
// redirects a daliservix file to a local mount. To remove redirect use NULL for mount dir or NULL for dir


extern DAFSCLIENT_API void remoteExtractBlobElements(const char * prefix, const RemoteFilename &file, ExtractedBlobArray & extracted);


extern DAFSCLIENT_API void disconnectRemoteFile(IFile *file);
extern DAFSCLIENT_API void disconnectRemoteIoOnExit(IFileIO *fileio,bool set=true);

extern DAFSCLIENT_API bool resetRemoteFilename(IFile *file, const char *newname); // returns false if not remote


extern DAFSCLIENT_API bool asyncCopyFileSection(const char *uuid,   // from genUUID - must be same for subsequent calls
                            IFile *from,                        // expected to be remote
                            RemoteFilename &to,
                            offset_t toofs,                     // (offset_t)-1 created file and copies to start
                            offset_t fromofs,
                            offset_t size,                      // (offset_t)-1 for all file
                            ICopyFileProgress *progress,
                            unsigned timeout                    // 0 to start, non-zero to wait
                        ); // returns true when done

extern DAFSCLIENT_API void setRemoteFileTimeouts(unsigned maxconnecttime,unsigned maxreadtime);

#define DAFS_VALIDATE_CONNECT_FAIL  (0x01)
#define DAFS_VALIDATE_BAD_VERSION   (0x02)
#define DAFS_VALIDATE_WRITE_FAIL_DATA  (0x12)
#define DAFS_VALIDATE_READ_FAIL_DATA   (0x14)
#define DAFS_VALIDATE_DISK_FULL_DATA   (0x18)
#define DAFS_VALIDATE_WRITE_FAIL_MIRROR  (0x22)
#define DAFS_VALIDATE_READ_FAIL_MIRROR   (0x24)
#define DAFS_VALIDATE_DISK_FULL_MIRROR   (0x28)
#define DAFS_SCRIPT_FAIL            (0x40)
                                
extern DAFSCLIENT_API unsigned validateNodes(const SocketEndpointArray &eps,const char *dataDir, const char *mirrorDir, bool chkver, SocketEndpointArray &failures, UnsignedArray &failedcodes, StringArray &failedmessages, const char *filename=NULL);

extern DAFSCLIENT_API void installFileHooks(const char *filespec);
extern DAFSCLIENT_API void installDefaultFileHooks(IPropertyTree * config);
extern DAFSCLIENT_API void removeFileHooks(); // Should be called before closedown

extern DAFSCLIENT_API void setRemoteOutputCompressionDefault(const char *type);
extern DAFSCLIENT_API const char *queryOutputCompressionDefault();

extern DAFSCLIENT_API void remoteExtractBlobElements(const SocketEndpoint &ep, const char * prefix, const char * filename, ExtractedBlobArray & extracted);

//// legacy implementations of the streaming support (to be replaced by dafsstream.*)

interface IOutputMetaData;
class RowFilter;
interface IRemoteFileIO : extends IFileIO
{
    virtual void addVirtualFieldMapping(const char *fieldName, const char *fieldValue) = 0;
    virtual void ensureAvailable() = 0;
};
extern DAFSCLIENT_API IRemoteFileIO *createRemoteFilteredFile(SocketEndpoint &ep, const char * filename, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, bool compressed, bool grouped, unsigned __int64 chooseNLimit);

interface IIndexLookup;
extern DAFSCLIENT_API IIndexLookup *createRemoteFilteredKey(SocketEndpoint &ep, const char * filename, unsigned crc, IOutputMetaData *actual, IOutputMetaData *projected, const RowFilter &fieldFilters, unsigned __int64 chooseNLimit);

////



#endif
