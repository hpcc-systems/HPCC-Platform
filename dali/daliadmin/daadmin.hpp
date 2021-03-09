/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#include "dautils.hpp"
#include "workunit.hpp"

static unsigned daliConnectTimeoutMs = 5000;

extern da_decl void xmlSize(const char *filename, double pc);
extern da_decl void translateToXpath(const char *logicalfile, DfsXmlBranchKind tailType = DXB_File);

extern da_decl void _export_(const char *path, const char *dst, bool safe = false);
extern da_decl void import(const char *path, const char *src, bool add);
extern da_decl void _delete_(const char *path, bool backup);
extern da_decl void set(const char *path, const char *val);
extern da_decl void get(const char *path);
extern da_decl void bget(const char *path, const char *outfn);
extern da_decl void wget(const char *path);
extern da_decl void add(const char *path, const char *val);
extern da_decl void delv(const char *path);
extern da_decl void count(const char *path);

extern da_decl void dfsfile(const char *lname, IUserDescriptor *userDesc, UnsignedArray *partslist = nullptr);
extern da_decl void dfspart(const char *lname,IUserDescriptor *userDesc, unsigned partnum);
extern da_decl void dfsmeta(const char *filename,IUserDescriptor *userDesc, bool includeStorage);
extern da_decl void setdfspartattr(const char *lname, unsigned partNum, const char *attr, const char *value, IUserDescriptor *userDesc);
extern da_decl void dfscsv(const char *dali, IUserDescriptor *udesc);
extern da_decl void dfsCheck();
extern da_decl void dfsGroup(const char *name, const char *outputFilename);
extern da_decl int clusterGroup(const char *name, const char *outputFilename);
extern da_decl void dfsLs(const char *name, const char *options, bool safe = false);
extern da_decl void dfsmap(const char *lname, IUserDescriptor *user);
extern da_decl int dfsexists(const char *lname, IUserDescriptor *user);
extern da_decl void dfsparents(const char *lname, IUserDescriptor *user);
extern da_decl void dfsunlink(const char *lname, IUserDescriptor *user);
extern da_decl int dfsverify(const char *name, CDateTime *cutoff, IUserDescriptor *user);
extern da_decl int dfsperm(const char *obj, IUserDescriptor *user);
extern da_decl void setprotect(const char *filename, const char *callerid, IUserDescriptor *user);
extern da_decl void unprotect(const char *filename, const char *callerid, IUserDescriptor *user);
extern da_decl void listprotect(const char *filename, const char *callerid);
extern da_decl void checksuperfile(const char *lfn, bool fix);
extern da_decl void checksubfile(const char *lfn);
extern da_decl void listexpires(const char * lfnmask, IUserDescriptor *user);
extern da_decl void listrelationships(const char *primary, const char *secondary);
extern da_decl void dfscompratio (const char *lname, IUserDescriptor *user);
extern da_decl void dfsscopes(const char *name, IUserDescriptor *user);
extern da_decl void cleanscopes(IUserDescriptor *user);
extern da_decl void normalizeFileNames(IUserDescriptor *user, const char *name);
extern da_decl void listmatches(const char *path, const char *match, const char *pval);
extern da_decl void dfsreplication(const char *clusterMask, const char *lfnMask, unsigned redundancy, bool dryRun);
extern da_decl void migrateFiles(const char *srcGroup, const char *tgtGroup, const char *filemask, const char *_options);
extern da_decl void getxref(const char *dst);

extern da_decl void listworkunits(const char *test, const char *min, const char *max);
extern da_decl void workunittimings(const char *wuid);
extern da_decl void dumpWorkunit(const char *wuid, bool includeProgress);
extern da_decl void dumpProgress(const char *wuid, const char *graph);
extern da_decl void wuidCompress(const char *match, const char *type, bool compress);
extern da_decl void dumpWorkunitAttr(IConstWorkUnit *workunit, const WuScopeFilter &filter);
extern da_decl void dumpWorkunitAttr(const char *wuid, const char *userFilter);

extern da_decl void dalilocks(const char *ipPattern, bool files);
extern da_decl void unlock(const char *pattern, bool files);
extern da_decl void holdlock(const char *logicalFile, const char *mode, IUserDescriptor *userDesc);

extern da_decl void serverlist(const char *mask);
extern da_decl void clusterlist(const char *mask);
extern da_decl void auditlog(const char *froms, const char *tos, const char *matchs);
extern da_decl void coalesce();
extern da_decl void mpping(const char *eps);
extern da_decl void daliping(const char *dalis, unsigned connecttime, unsigned n);

extern da_decl void validateStore(bool fix, bool deleteFiles, bool verbose);
extern da_decl void removeOrphanedGlobalVariables(bool dryrun, bool reconstruct);

