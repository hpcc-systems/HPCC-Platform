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

#ifndef REFFILE_LIST_HPP
#define REFFILE_LIST_HPP

#include "jlib.hpp"
#include "workunit.hpp"
#include "package.h"
#include "dfuutil.hpp"

#ifdef PKGFILES_EXPORTS
    #define REFFILES_API DECL_EXPORT
#else
    #define REFFILES_API DECL_IMPORT
#endif

#define RefFileNone           0x0000
#define RefFileIndex          0x0001
#define RefFileNotOnCluster   0x0002
#define RefFileNotFound       0x0004
#define RefFileResolvedForeign 0x0008
#define RefFileResolvedRemote 0x0010
#define RefFileLFNForeign     0x0020 //LFN was Foreign
#define RefFileLFNRemote      0x0040 //LFN was remote
#define RefFileSuper          0x0080
#define RefSubFile            0x0100
#define RefFileCopyInfoFailed 0x0200
#define RefFileCloned         0x0400
#define RefFileInPackage      0x0800
#define RefFileNotOnSource    0x1000
#define RefFileOptional       0x2000 //File referenced in more than one place can be both optional and not optional
#define RefFileNotOptional    0x4000


interface IReferencedFile : extends IInterface
{
    virtual const char *getLogicalName() const =0;
    virtual unsigned getFlags() const =0;
    virtual const SocketEndpoint &getForeignIP(SocketEndpoint &ep) const =0;
    virtual const char *queryPackageId() const =0;
    virtual __int64 getFileSize()=0;
    virtual unsigned getNumParts()=0;
    virtual const StringArray &getSubFileNames() const =0;
    virtual bool needsCopying(bool cloneForeign) const = 0;
};

interface IReferencedFileIterator : extends IIteratorOf<IReferencedFile> { };

interface IReferencedFileList : extends IInterface
{
    virtual void addFilesFromWorkUnit(IConstWorkUnit *cw)=0;
    virtual bool addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackageMap *pm, const char *queryid)=0;
    virtual bool addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackage *pkg)=0;
    virtual void addFilesFromPackageMap(IPropertyTree *pm)=0;

    virtual void addFile(const char *ln, const char *daliip=nullptr, const char *sourceProcessCluster=nullptr, const char *remotePrefix=nullptr, const char *remoteStorageName=nullptr)=0;
    virtual void addFiles(StringArray &files)=0;

    virtual IReferencedFileIterator *getFiles()=0;
    virtual void resolveFiles(const StringArray &locations, const char *remoteIP, const char * remotePrefix, const char *srcCluster, bool checkLocalFirst, bool addSubFiles, bool trackSubFiles, bool resolveLFNForeign, bool useRemoteStorage)=0;
    virtual void cloneAllInfo(StringBuffer &publisherWuid, const char *dstCluster, unsigned updateFlags, IDFUhelper *helper, bool cloneSuperInfo, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defRepFolder)=0;
    virtual void cloneFileInfo(StringBuffer &publisherWuid, const char *dstCluster, unsigned updateFlags, IDFUhelper *helper, bool cloneSuperInfo, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defRepFolder)=0;
    virtual void cloneRelationships()=0;
    virtual void setDfuQueue(const char *dfu_queue) = 0;
    virtual void setKeyCompression(const char * keyCompression) = 0;
};

extern REFFILES_API const char *skipForeignOrRemote(const char *name, StringBuffer *ip=nullptr, StringBuffer *remote=nullptr);

extern REFFILES_API IReferencedFileList *createReferencedFileList(const char *user, const char *pw, bool allowForeignFiles, bool allowFileSizeCalc, const char *jobname = nullptr);
extern REFFILES_API IReferencedFileList *createReferencedFileList(IUserDescriptor *userDesc, bool allowForeignFiles, bool allowFileSizeCalc, const char *jobname = nullptr);

extern REFFILES_API void splitDfsLocation(const char *address, StringBuffer &cluster, StringBuffer &ip, StringBuffer &prefix, const char *defaultCluster);
extern REFFILES_API void splitDerivedDfsLocation(const char *address, StringBuffer &cluster, StringBuffer &ip, StringBuffer &prefix, const char *defaultCluster, const char *baseCluster, const char *baseIP, const char *basePrefix);

#endif //REFFILE_LIST_HPP
