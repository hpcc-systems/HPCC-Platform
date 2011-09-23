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

#ifdef _DEBUG
#define _CLEAR_ALLOCATED_ROW
#endif

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
interface IPackageMap : public IInterface
{
    virtual void addPackage(const char *name, IRoxiePackage *package) = 0;
    virtual const IRoxiePackage *queryPackage(const char *name) const = 0;
    virtual IPropertyTree *getQuerySets() const = 0;
    virtual const char *queryPackageId() const = 0;
};
extern const IRoxiePackage &queryRootPackage();
extern const IPackageMap &queryEmptyPackageMap();

interface IRoxiePackage : extends IInterface 
{
    // Complete the setup of a package by resolving base package references
    virtual void resolveBases(IPackageMap *packages) = 0;
    // Lookup package environment variable
    virtual const char *queryEnv(const char *varname) const = 0;
    // Lookup package environment variable controlling field translation
    virtual bool getEnableFieldTranslation() const = 0;
    // Return entire XML tree for package
    virtual const IPropertyTree *queryTree() const = 0;
    // Lookup information in package to resolve logical file name
    virtual const IResolvedFile *lookupFileName(const char *fileName, bool opt, bool cacheDaliResults) const = 0;
    // Lookup information in package about what in-memory indexes should be built for file
    virtual IPropertyTreeIterator *getInMemoryIndexInfo(const IPropertyTree &graphNode) const = 0;
    // Retrieve hash for the package
    virtual hash64_t queryHash() const = 0;
    // Lookup information in package about what in-memory indexes should be built for file
    virtual IPropertyTree *getQuerySets() const = 0;
    // Remove a resolved file from the cache 
    virtual void removeCache(const IResolvedFile *goer) const = 0; // note that this is const as cache is considered mutable
};

extern IRoxiePackage *createPackage(IPropertyTree *p);

interface ISlaveDynamicFileCache : extends IInterface
{
    virtual IResolvedFile *lookupDynamicFile(const IRoxieContextLogger &logctx, const char *lfn, CDateTime &cacheDate, RoxiePacketHeader *header, bool isOpt, bool isLocal) = 0; 
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

interface IRoxieResourceManager : extends IInterface
{
    virtual IQueryFactory *lookupLibrary(const char * libraryName, unsigned expectedInterfaceHash, const IRoxieContextLogger &logctx) const = 0;
    virtual IQueryFactory *getQuery(const char *id, const IRoxieContextLogger &ctx) const = 0;
    virtual void load(const IPropertyTree *querySets, const IPackageMap &packages) = 0;
    virtual void getStats(const char *queryName, const char *graphName, StringBuffer &reply, const IRoxieContextLogger &logctx) const = 0;
    virtual void resetQueryTimings(const char *queryName, const IRoxieContextLogger &logctx) = 0;
    virtual void getActivityMetrics(StringBuffer &reply) const = 0;
    virtual void resetActivityMetrics() = 0;
};

interface IRoxieDebugSessionManager : extends IInterface
{
    virtual void registerDebugId(const char *id, IDebuggerContext *ctx) = 0;
    virtual void deregisterDebugId(const char *id) = 0;
    virtual IDebuggerContext *lookupDebuggerContext(const char *id) = 0;
};

interface IRoxieResourceManagerSet : extends IInterface
{
    virtual void load(const IPropertyTree *querySets, const IPackageMap &packages) = 0;
};

class GlobalResourceManager;

extern IRoxieResourceManager *createServerManager();
extern IRoxieResourceManager *createSlaveManager();
extern const IRoxieResourceManager *getRoxieServerManager();
extern IRoxieDebugSessionManager *getRoxieDebugSessionManager();
extern void selectPackage(const char * packageId);
extern void deleteNonActiveGlobalResourceManager(const char * packageId);
extern unsigned findAllLoadedPackageIds(StringArray &packageIds);
extern void loadPackageSet(const char *packageId);
extern GlobalResourceManager *getGlobalResourceManager(const char *packageId);
extern GlobalResourceManager *getActiveGlobalResourceManager();

extern void createResourceManagers(const IQueryDll *standAloneDll, unsigned numChannels);
extern void cleanupResourceManagers();

extern void doControlMessage(IPropertyTree *xml, StringBuffer &reply, const IRoxieContextLogger &ctx);
extern InterruptableSemaphore controlSem;
extern void mergeStats(IPropertyTree *s1, IPropertyTree *s2, unsigned level);

extern const char *queryNodeFileName(const IPropertyTree &graphNode);
extern const char *queryNodeIndexName(const IPropertyTree &graphNode);

extern IPropertyTreeIterator *getNodeSubFileNames(const IPropertyTree &graphNode);
extern IPropertyTreeIterator *getNodeSubIndexNames(const IPropertyTree &graphNode);

#endif
