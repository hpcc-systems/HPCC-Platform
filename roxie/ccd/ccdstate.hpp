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

#ifndef _CCDSTATE_INCL
#define _CCDSTATE_INCL

#include "ccdactivities.hpp"
#include "ccdserver.hpp"
#include "ccdkey.hpp"
#include "ccdfile.hpp"
#include "jhtree.hpp"
#include "jisem.hpp"
#include "dllserver.hpp"
#include "thorcommon.hpp"
#include "ccddali.hpp"
#include "thorcommon.ipp"
#include "package.h"

interface IFilePartMap : public IInterface
{
    virtual bool IsShared() const = 0;
    virtual unsigned mapOffset(offset_t offset) const = 0;
    virtual unsigned getNumParts() const = 0;
    virtual offset_t getBase(unsigned part) const = 0;
    virtual offset_t getFileSize() const = 0;
    virtual offset_t getTotalSize() const = 0;
    virtual offset_t getRecordCount() const = 0;
};
extern IFilePartMap *createFilePartMap(const char *fileName, IFileDescriptor &fdesc);

interface IRoxiePackage;
interface IRoxiePackageMap : public IHpccPackageMap
{
    virtual const IRoxiePackage *queryRoxiePackage(const char *name) const = 0;
    virtual const IRoxiePackage *matchRoxiePackage(const char *name) const = 0;
};
extern const IRoxiePackage &queryRootRoxiePackage();
extern const IRoxiePackageMap &queryEmptyRoxiePackageMap();

interface IRoxiePackage : public IHpccPackage
{
    // Lookup information in package to resolve existing logical file name
    virtual const IResolvedFile *lookupFileName(const char *fileName, bool opt, bool useCache, bool cacheResults, IConstWorkUnit *wu, bool ignoreForeignPrefix, bool isPrivilegedUser) const = 0;
    // Lookup information in package to create new logical file name
    virtual IRoxieWriteHandler *createWriteHandler(const char *fileName, bool overwrite, bool extend, const StringArray &clusters, IConstWorkUnit *wu, bool isPrivilegedUser) const = 0;
    // Lookup information in package about what in-memory indexes should be built for file
    virtual IPropertyTreeIterator *getInMemoryIndexInfo(const IPropertyTree &graphNode) const = 0;
    // Set the hash to be used for this package
    virtual void setHash(hash64_t newhash)  = 0;
};

interface IResolvedFileCache
{
    // Add a filename and the corresponding IResolvedFile to the cache
    virtual void addCache(const char *filename, const IResolvedFile *file) = 0;
    // Lookup a filename in the cache
    virtual IResolvedFile *lookupCache(const char *filename) = 0;
    // Remove a resolved file from the cache
    virtual void removeCache(const IResolvedFile *goer) = 0;
};

extern IRoxiePackage *createRoxiePackage(IPropertyTree *p, IRoxiePackageMap *packages);

interface IAgentDynamicFileCache : extends IInterface
{
    virtual IResolvedFile *lookupDynamicFile(const IRoxieContextLogger &logctx, const char *lfn, CDateTime &cacheDate, unsigned checksum, RoxiePacketHeader *header, bool isOpt, bool isLocal) = 0;
    virtual void releaseAll() = 0;
};
extern IAgentDynamicFileCache *queryAgentDynamicFileCache();
extern void releaseAgentDynamicFileCache();

interface IDynamicTransform;

interface IFileIOArray : extends IInterface
{
    virtual bool IsShared() const = 0;
    virtual IFileIO *getFilePart(unsigned partNo, offset_t &base) const = 0;
    virtual unsigned length() const = 0;
    virtual unsigned numValid() const = 0;
    virtual bool isValid(unsigned partNo) const = 0;
    virtual unsigned __int64 size() const = 0;
    virtual unsigned __int64 partSize(unsigned partNo) const = 0;
    virtual StringBuffer &getId(StringBuffer &) const = 0;
    virtual const char *queryLogicalFilename(unsigned partNo) const = 0;
    virtual int queryActualFormatCrc() const = 0;    // Actual format on disk
    virtual bool allFormatsMatch() const = 0;  // i.e. not a superfile with mixed formats
    virtual unsigned getSubFile(unsigned partNo) const = 0;
};

interface ITranslatorSet : extends IInterface
{
    virtual const IDynamicTransform *queryTranslator(unsigned subFile) const = 0;
    virtual const IKeyTranslator *queryKeyTranslator(unsigned subFile) const = 0;
    virtual ISourceRowPrefetcher *getPrefetcher(unsigned subFile) const = 0;
    virtual IOutputMetaData *queryActualLayout(unsigned subFile) const = 0;
    virtual int queryTargetFormatCrc() const = 0;
    virtual const RtlRecord &queryTargetFormat() const = 0;
    virtual bool isTranslating() const = 0;
    virtual bool isTranslatingKeyed() const = 0;
    virtual bool hasConsistentTranslation() const = 0;
};

interface IRoxieQuerySetManagerSet : extends IInterface
{
    virtual void load(const IPropertyTree *querySets, const IRoxiePackageMap &packages, hash64_t &hash, bool forceRetry) = 0;
    virtual void getQueries(const char *id, IArrayOf<IQueryFactory> &queries, const IRoxieContextLogger &logctx) const = 0;
    virtual void preloadOnce() = 0;
};

interface IRoxieQuerySetManager : extends IInterface
{
    virtual bool isActive() const = 0;
    virtual IQueryFactory *getQuery(const char *id, StringBuffer *querySet, const IRoxieContextLogger &ctx) const = 0;
    virtual void load(const IPropertyTree *querySet, const IRoxiePackageMap &packages, hash64_t &hash, bool forceRetry) = 0;
    virtual void getStats(const char *queryName, const char *graphName, IConstWorkUnit *statsWu, unsigned channel, bool reset, const IRoxieContextLogger &logctx) const = 0;
    virtual void resetQueryTimings(const char *queryName, const IRoxieContextLogger &logctx) = 0;
    virtual void resetAllQueryTimings() = 0;
    virtual void getActivityMetrics(StringBuffer &reply) const = 0;
    virtual void getAllQueryInfo(StringBuffer &reply, bool full, const IRoxieQuerySetManagerSet *agents, const IRoxieContextLogger &logctx) const = 0;
    virtual void preloadOnce() const = 0;
};

interface IRoxieDebugSessionManager : extends IInterface
{
    virtual void registerDebugId(const char *id, IDebuggerContext *ctx) = 0;
    virtual void deregisterDebugId(const char *id) = 0;
    virtual IDebuggerContext *lookupDebuggerContext(const char *id) = 0;
};

interface IRoxieQueryPackageManagerSet : extends IInterface
{
    virtual void requestReload(bool waitUntilComplete, bool forceRetry, bool incremental) = 0;
    virtual void load() = 0;
    virtual void doControlMessage(IPropertyTree *xml, StringBuffer &reply, const IRoxieContextLogger &ctx) = 0;
    virtual IQueryFactory *getQuery(const char *id, StringBuffer *querySet, IArrayOf<IQueryFactory> *agents, const IRoxieContextLogger &logctx) const = 0;
    virtual IQueryFactory *lookupLibrary(const char *libraryName, unsigned expectedInterfaceHash, const IRoxieContextLogger &logctx) const = 0;
    virtual int getActivePackageCount() const = 0;
};

extern IRoxieDebugSessionManager &queryRoxieDebugSessionManager();

extern IRoxieQuerySetManager *createServerManager(const char *querySet);
extern IRoxieQuerySetManager *createAgentManager();
extern IRoxieQueryPackageManagerSet *createRoxiePackageSetManager(const IQueryDll *standAloneDll);
extern IRoxieQueryPackageManagerSet *globalPackageSetManager;

extern void loadPlugins();
extern void cleanupPlugins();

extern void mergeStats(IPropertyTree *s1, IPropertyTree *s2, unsigned level);
extern void mergeStats(IPropertyTree *s1, IPropertyTree *s2);
extern void mergeQueries(IPropertyTree *s1, IPropertyTree *s2);

extern const char *queryNodeFileName(const IPropertyTree &graphNode, ThorActivityKind kind);
extern const char *queryNodeIndexName(const IPropertyTree &graphNode, ThorActivityKind kind);

extern void createDelayedReleaser();
extern void stopDelayedReleaser();

struct EventRecordingSummary;
extern CCD_API bool startRoxieEventRecording(const char * optionsText, const char * filename);
extern CCD_API bool stopRoxieEventRecording(EventRecordingSummary * optSummary);

#endif
