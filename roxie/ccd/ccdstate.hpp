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

#ifndef _CCDSTATE_INCL
#define _CCDSTATE_INCL

#include "ccdactivities.hpp"
#include "ccdserver.hpp"
#include "ccdkey.hpp"
#include "ccdfile.hpp"
#include "jhtree.hpp"
#include "jisem.hpp"
#include "dllserver.hpp"
#include "layouttrans.hpp"
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
    virtual const IResolvedFile *lookupFileName(const char *fileName, bool opt, bool cacheDaliResults, IConstWorkUnit *wu) const = 0;
    // Lookup information in package to create new logical file name
    virtual IRoxieWriteHandler *createFileName(const char *fileName, bool overwrite, bool extend, const StringArray &clusters, IConstWorkUnit *wu) const = 0;
    // Lookup information in package about what in-memory indexes should be built for file
    virtual IPropertyTreeIterator *getInMemoryIndexInfo(const IPropertyTree &graphNode) const = 0;
    // Remove a resolved file from the cache 
    virtual void removeCache(const IResolvedFile *goer) const = 0; // note that this is const as cache is considered mutable
};

extern IRoxiePackage *createRoxiePackage(IPropertyTree *p, IRoxiePackageMap *packages);

interface ISlaveDynamicFileCache : extends IInterface
{
    virtual IResolvedFile *lookupDynamicFile(const IRoxieContextLogger &logctx, const char *lfn, CDateTime &cacheDate, unsigned checksum, RoxiePacketHeader *header, bool isOpt, bool isLocal) = 0;
};
extern ISlaveDynamicFileCache *querySlaveDynamicFileCache();
extern void releaseSlaveDynamicFileCache();

interface IFileIOArray : extends IInterface
{
    virtual bool IsShared() const = 0;
    virtual IFileIO *getFilePart(unsigned partNo, offset_t &base) = 0;
    virtual unsigned length() = 0;
    virtual unsigned numValid() = 0;
    virtual bool isValid(unsigned partNo) = 0;
    virtual unsigned __int64 size() = 0;
    virtual StringBuffer &getId(StringBuffer &) const = 0;
};

interface IRoxieQuerySetManager : extends IInterface
{
    virtual bool isActive() const = 0;
    virtual IQueryFactory *getQuery(const char *id, const IRoxieContextLogger &ctx) const = 0;
    virtual void load(const IPropertyTree *querySet, const IRoxiePackageMap &packages, hash64_t &hash) = 0;
    virtual void getStats(const char *queryName, const char *graphName, StringBuffer &reply, const IRoxieContextLogger &logctx) const = 0;
    virtual void resetQueryTimings(const char *queryName, const IRoxieContextLogger &logctx) = 0;
    virtual void resetAllQueryTimings() = 0;
    virtual void getActivityMetrics(StringBuffer &reply) const = 0;
    virtual void getAllQueryInfo(StringBuffer &reply, bool full, const IRoxieContextLogger &logctx) const = 0;
};

interface IRoxieDebugSessionManager : extends IInterface
{
    virtual void registerDebugId(const char *id, IDebuggerContext *ctx) = 0;
    virtual void deregisterDebugId(const char *id) = 0;
    virtual IDebuggerContext *lookupDebuggerContext(const char *id) = 0;
};

interface IRoxieQuerySetManagerSet : extends IInterface
{
    virtual void load(const IPropertyTree *querySets, const IRoxiePackageMap &packages, hash64_t &hash) = 0;
};

interface IRoxieQueryPackageManagerSet : extends IInterface
{
    virtual void load() = 0;
    virtual void doControlMessage(IPropertyTree *xml, StringBuffer &reply, const IRoxieContextLogger &ctx) = 0;
    virtual IQueryFactory *getQuery(const char *id, const IRoxieContextLogger &logctx) const = 0;
    virtual IQueryFactory *lookupLibrary(const IRoxiePackage &package, const char *libraryName, unsigned expectedInterfaceHash, const IRoxieContextLogger &logctx) const = 0;
    virtual int getActivePackageCount() const = 0;
};

extern IRoxieDebugSessionManager &queryRoxieDebugSessionManager();

extern IRoxieQuerySetManager *createServerManager(const char *querySet);
extern IRoxieQuerySetManager *createSlaveManager();
extern IRoxieQueryPackageManagerSet *createRoxiePackageSetManager(const IQueryDll *standAloneDll);
extern IRoxieQueryPackageManagerSet *globalPackageSetManager;

extern void loadPlugins();
extern void cleanupPlugins();

extern void mergeStats(IPropertyTree *s1, IPropertyTree *s2, unsigned level);

extern const char *queryNodeFileName(const IPropertyTree &graphNode);
extern const char *queryNodeIndexName(const IPropertyTree &graphNode);

extern IPropertyTreeIterator *getNodeSubFileNames(const IPropertyTree &graphNode);
extern IPropertyTreeIterator *getNodeSubIndexNames(const IPropertyTree &graphNode);

#endif
