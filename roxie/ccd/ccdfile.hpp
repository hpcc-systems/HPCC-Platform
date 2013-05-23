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

#ifndef _CCDFILE_INCL
#define _CCDFILE_INCL
#include "jfile.hpp"
#include "eclhelper.hpp"
#include "ccddali.hpp"
#include "dautils.hpp"

enum RoxieFileStatus { FileSizeMismatch, FileDateMismatch, FileCRCMismatch, FileIsValid, FileNotFound };
enum RoxieFileType { ROXIE_KEY, ROXIE_FILE, ROXIE_PATCH, ROXIE_BASEINDEX };
interface IFileIOArray;
interface IRoxieFileCache;

interface ILazyFileIO : extends IFileIO
{
    virtual const char *queryFilename() = 0;
    virtual void checkOpen() = 0;
    virtual bool isAlive() const = 0;
    virtual void addSource(IFile *source) = 0;
    virtual bool isRemote() = 0;
    virtual offset_t getSize() = 0;
    virtual CDateTime *queryDateTime() = 0;

    virtual IFile *querySource() = 0;
    virtual IFile *queryTarget() = 0;
    virtual void copyComplete() = 0;
    virtual int getLinkCount() const = 0;
    virtual bool createHardFileLink() = 0;

    virtual void setBaseIndexFileName(const char *val) =0;
    virtual const char *queryBaseIndexFileName() = 0;
    virtual void setPatchFile(ILazyFileIO *val) = 0;
    virtual ILazyFileIO *queryPatchFile() = 0;

    virtual unsigned getLastAccessed() const = 0;
    virtual bool isOpen() const = 0;
    virtual void close() = 0;
    virtual RoxieFileType getFileType() = 0;
    virtual void setCopying(bool copying) = 0;
    virtual bool isCopying() const = 0;
    virtual IMemoryMappedFile *queryMappedFile() = 0;

    virtual void setCache(const IRoxieFileCache *) = 0;
    virtual void removeCache(const IRoxieFileCache *) = 0;
};

extern ILazyFileIO *createDynamicFile(const char *id, IPartDescriptor *pdesc, RoxieFileType fileType, int numParts, SocketEndpoint &cloneFrom);

interface IRoxieFileCache : extends IInterface
{
    virtual ILazyFileIO *lookupFile(const char *id, unsigned partNo, RoxieFileType fileType, const char *localLocation, const char *baseIndexFileName, ILazyFileIO *patchFile, const StringArray &peerRoxieCopiedLocationInfo, const StringArray &deployedLocationInfo, offset_t size, const CDateTime &modified, bool memFile, bool isRemote, bool startFileCopy, bool doForegroundCopy, unsigned crc, bool isCompressed, const char *lookupDali) = 0;
    virtual RoxieFileStatus fileUpToDate(IFile *f, RoxieFileType fileType, offset_t size, const CDateTime &modified, unsigned crc, const char* id, bool isCompressed) = 0;
    virtual int numFilesToCopy() = 0;
    virtual void closeExpired(bool remote) = 0;
    virtual StringAttrMapping *queryFileErrorList() = 0;  // returns list of files that could not be open
    virtual void flushUnusedDirectories(const char *origBaseDir, const char *directory, StringBuffer &info) = 0;
    virtual void start() = 0;
    virtual void removeCache(ILazyFileIO *file) const = 0;
};

interface IDiffFileInfoCache : extends IInterface
{
    virtual void saveDiffFileLocationInfo(const char *id, const StringArray &locations) = 0;
    virtual void saveDiffFileLocationInfo(const char *id, const char *location) = 0;
    virtual const char *queryDiffFileNames(StringBuffer &names) = 0;
    virtual void deleteDiffFiles(IPropertyTree *tree, IPropertyTree *goers) = 0;
};

interface IMemoryFile : extends IFileIO
{
    virtual const char *queryBase() = 0;
};

interface IKeyArray;
interface IDefRecordMeta;
interface IFilePartMap;
class TranslatorArray;
interface IInMemoryIndexManager ;
interface IRoxiePackage;

interface IResolvedFile : extends ISimpleSuperFileEnquiry
{
    virtual void serializePartial(MemoryBuffer &mb, unsigned channel, bool localInfoOnly) const = 0;

    virtual IFileIOArray *getIFileIOArray(bool isOpt, unsigned channel) const = 0;
    virtual IKeyArray *getKeyArray(IDefRecordMeta *activityMeta, TranslatorArray *translators, bool isOpt, unsigned channel, bool allowFieldTranslation) const = 0;
    virtual IFilePartMap *getFileMap() const = 0;
    virtual IInMemoryIndexManager *getIndexManager(bool isOpt, unsigned channel, IFileIOArray *files, IRecordSize *recs, bool preload, int numKeys) const = 0;
    virtual offset_t getFileSize() const = 0;

    virtual const CDateTime &queryTimeStamp() const = 0;
    virtual unsigned queryCheckSum() const = 0;

    virtual const char *queryPhysicalName() const = 0; // Returns NULL unless in local file mode.
    virtual const char *queryFileName() const = 0;
    virtual void setCache(const IRoxiePackage *cache) = 0;
    virtual bool isAlive() const = 0;
    virtual const IPropertyTree *queryProperties() const = 0;

    virtual void remove() = 0;
    virtual bool exists() const = 0;
};

interface IResolvedFileCreator : extends IResolvedFile
{
    virtual void addSubFile(const char *localFileName) = 0;
    virtual void addSubFile(const IResolvedFile *sub) = 0;
    virtual void addSubFile(IFileDescriptor *sub, IFileDescriptor *remoteSub) = 0;
};

extern IResolvedFileCreator *createResolvedFile(const char *lfn, const char *physical);
extern IResolvedFile *createResolvedFile(const char *lfn, const char *physical, IDistributedFile *dFile, IRoxieDaliHelper *daliHelper, bool cacheIt, bool writeAccess);

interface IRoxiePublishCallback
{
    virtual void setFileProperties(IFileDescriptor *) const = 0;
    virtual IUserDescriptor *queryUserDescriptor() const = 0;
};

interface IRoxieWriteHandler : public IInterface
{
    virtual IFile *queryFile() const = 0;
    virtual void finish(bool success, const IRoxiePublishCallback *activity) = 0;
    virtual void getClusters(StringArray &clusters) const = 0;
};

extern IRoxieWriteHandler *createRoxieWriteHandler(IRoxieDaliHelper *_daliHelper, ILocalOrDistributedFile *_dFile, const StringArray &_clusters);

extern IRoxieFileCache &queryFileCache();
extern IMemoryFile *createMemoryFile(const char *fileName);
extern IDiffFileInfoCache *queryDiffFileInfoCache();
extern void releaseDiffFileInfoCache();

#endif
