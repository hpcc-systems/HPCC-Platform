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

#ifndef _CCDFILE_INCL
#define _CCDFILE_INCL
#include "jfile.hpp"
#include "eclhelper.hpp"
#include "ccddali.hpp"
#include "dautils.hpp"

enum RoxieFileStatus { FileSizeMismatch, FileDateMismatch, FileCRCMismatch, FileIsValid, FileNotFound };
enum RoxieFileType { ROXIE_WU_DLL, ROXIE_PLUGIN_DLL, ROXIE_KEY, ROXIE_FILE, ROXIE_PATCH, ROXIE_BASEINDEX };
interface IFileIOArray;
interface ILazyFileIO : extends IFileIO
{
    virtual const char *queryFilename() = 0;
    virtual void checkOpen() = 0;
    virtual bool IsShared() const = 0;
    virtual void addSource(IFile *source) = 0;
    virtual bool isRemote() = 0;
    virtual offset_t getSize() = 0;
    virtual CDateTime *queryDateTime() = 0;

    virtual IFile *querySource() = 0;
    virtual IFile *queryTarget() = 0;
    virtual void copyComplete() = 0;
    virtual int getLinkCount() const = 0;
    virtual void setFileIsMemFile(bool val) = 0;
    virtual bool getFileIsMemFile() = 0;
    virtual void setCopyInForeground(bool val) = 0;
    virtual bool getCopyInForeground() = 0;
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
};

extern ILazyFileIO *createDynamicFile(const char *id, IPartDescriptor *pdesc, RoxieFileType fileType, int numParts);

interface IRoxieFileCache : extends IInterface
{
    virtual ILazyFileIO *lookupFile(const char *id, unsigned partNo, RoxieFileType fileType, const char *localLocation, const char *baseIndexFileName, ILazyFileIO *patchFile, const StringArray &peerRoxieCopiedLocationInfo, const StringArray &deployedLocationInfo, offset_t size, const CDateTime &modified, bool memFile, bool isRemote, bool startFileCopy, bool doForegroundCopy, unsigned crc, bool isCompressed, const char *lookupDali) = 0;
    virtual IFileIO *lookupDllFile(const char* dllname, const char *localLocation, const StringArray &remoteNames, unsigned crc, bool isRemote) = 0;
    virtual IFileIO *lookupPluginFile(const char* dllname, const char *localLocation) = 0;
    virtual void flushUnused(bool deleteFiles, bool cleanUpOneTimeQueries) = 0;
    virtual RoxieFileStatus fileUpToDate(IFile *f, RoxieFileType fileType, offset_t size, const CDateTime &modified, unsigned crc, const char* id, bool isCompressed) = 0;
    virtual int numFilesToCopy() = 0;
    virtual void closeExpired(bool remote) = 0;
    virtual StringAttrMapping *queryFileErrorList() = 0;  // returns list of files that could not be open
    virtual void flushUnusedDirectories(const char *origBaseDir, const char *directory, StringBuffer &info) = 0;
    virtual void start() = 0;
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

    virtual const char *queryFileName() const = 0;
    virtual void setCache(const IRoxiePackage *cache) = 0;
    virtual bool isAlive() const = 0;
    virtual const IPropertyTree *queryProperties() const = 0;

    virtual void remove() = 0;
};

interface IResolvedFileCreator : extends IResolvedFile
{
    virtual void addSubFile(const IResolvedFile *sub) = 0;
    virtual void addSubFile(IFileDescriptor *sub) = 0;
};

extern IResolvedFileCreator *createResolvedFile(const char *lfn);
extern IResolvedFile *createResolvedFile(const char *lfn, IDistributedFile *dFile);

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
