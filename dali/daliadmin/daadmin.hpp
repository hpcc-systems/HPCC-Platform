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

#ifdef DALIADMIN_API_EXPORTS
    #define DALIADMIN_API DECL_EXPORT
#else
    #define DALIADMIN_API DECL_IMPORT
#endif

#include "dautils.hpp"

namespace daadmin
{

extern DALIADMIN_API void setDaliConnectTimeoutMs(unsigned timeoutMs);
extern DALIADMIN_API void xmlSize(const char *filename, double pc);
extern DALIADMIN_API void translateToXpath(const char *logicalfile, DfsXmlBranchKind tailType = DXB_File);

extern DALIADMIN_API void exportToFile(const char *path, const char *filename, bool safe = false);
extern DALIADMIN_API bool exportToXML(const char *path, StringBuffer &out, bool safe = false);
extern DALIADMIN_API bool importFromFile(const char *path, const char *filename, bool add, StringBuffer &out);
extern DALIADMIN_API bool importFromXML(const char *path, const char *xml, bool add, StringBuffer &out);
extern DALIADMIN_API bool erase(const char *path, bool backup,StringBuffer &out);
extern DALIADMIN_API StringBuffer &setValue(const char *path, const char *val, StringBuffer &oldVal);
extern DALIADMIN_API void getValue(const char *path, StringBuffer& out);
extern DALIADMIN_API void bget(const char *path, const char *outfn);
extern DALIADMIN_API void wget(const char *path);
extern DALIADMIN_API bool add(const char *path, const char *val, StringBuffer &out);
extern DALIADMIN_API void delv(const char *path);
extern DALIADMIN_API unsigned count(const char *path);

extern DALIADMIN_API bool dfsfile(const char *lname, IUserDescriptor *userDesc, StringBuffer &out, UnsignedArray *partslist = nullptr);
extern DALIADMIN_API bool dfspart(const char *lname,IUserDescriptor *userDesc, unsigned partnum, StringBuffer &out);
extern DALIADMIN_API void dfsmeta(const char *filename,IUserDescriptor *userDesc, bool includeStorage);
extern DALIADMIN_API void setdfspartattr(const char *lname, unsigned partNum, const char *attr, const char *value, IUserDescriptor *userDesc, StringBuffer &out);
extern DALIADMIN_API void dfscsv(const char *dali, IUserDescriptor *udesc, StringBuffer &out);
extern DALIADMIN_API bool dfsCheck(StringBuffer &out);
extern DALIADMIN_API void dfsGroup(const char *name, const char *outputFilename);
extern DALIADMIN_API int clusterGroup(const char *name, const char *outputFilename);
extern DALIADMIN_API bool dfsLs(const char *name, const char *options, StringBuffer &out);
extern DALIADMIN_API bool dfsmap(const char *lname, IUserDescriptor *user, StringBuffer &out);
extern DALIADMIN_API int dfsexists(const char *lname, IUserDescriptor *user);
extern DALIADMIN_API void dfsparents(const char *lname, IUserDescriptor *user, StringBuffer &out);
extern DALIADMIN_API void dfsunlink(const char *lname, IUserDescriptor *user);
extern DALIADMIN_API int dfsverify(const char *name, CDateTime *cutoff, IUserDescriptor *user);
extern DALIADMIN_API int dfsperm(const char *obj, IUserDescriptor *user);
extern DALIADMIN_API void setprotect(const char *filename, const char *callerid, IUserDescriptor *user);
extern DALIADMIN_API void unprotect(const char *filename, const char *callerid, IUserDescriptor *user);
extern DALIADMIN_API void listprotect(const char *filename, const char *callerid);
extern DALIADMIN_API void checksuperfile(const char *lfn, bool fix);
extern DALIADMIN_API void checksubfile(const char *lfn);
extern DALIADMIN_API void listexpires(const char * lfnmask, IUserDescriptor *user);
extern DALIADMIN_API void listrelationships(const char *primary, const char *secondary);
extern DALIADMIN_API void dfscompratio (const char *lname, IUserDescriptor *user);
extern DALIADMIN_API void dfsscopes(const char *name, IUserDescriptor *user);
extern DALIADMIN_API void cleanscopes(IUserDescriptor *user);
extern DALIADMIN_API void normalizeFileNames(IUserDescriptor *user, const char *name);
extern DALIADMIN_API void listmatches(const char *path, const char *match, const char *pval);
extern DALIADMIN_API void dfsreplication(const char *clusterMask, const char *lfnMask, unsigned redundancy, bool dryRun);
extern DALIADMIN_API void migrateFiles(const char *srcGroup, const char *tgtGroup, const char *filemask, const char *_options);
extern DALIADMIN_API void getxref(const char *dst);

extern DALIADMIN_API void listworkunits(const char *test, const char *min, const char *max);
extern DALIADMIN_API void workunittimings(const char *wuid);
extern DALIADMIN_API void dumpWorkunit(const char *wuid, bool includeProgress);
extern DALIADMIN_API void dumpProgress(const char *wuid, const char *graph);
extern DALIADMIN_API void wuidCompress(const char *match, const char *type, bool compress);
extern DALIADMIN_API void dumpWorkunitAttr(const char *wuid, const char *userFilter);

extern DALIADMIN_API void dalilocks(const char *ipPattern, bool files);
extern DALIADMIN_API void unlock(const char *pattern, bool files);
extern DALIADMIN_API void holdlock(const char *logicalFile, const char *mode, IUserDescriptor *userDesc);

extern DALIADMIN_API void serverlist(const char *mask);
extern DALIADMIN_API void clusterlist(const char *mask);
extern DALIADMIN_API void auditlog(const char *froms, const char *tos, const char *matchs);
extern DALIADMIN_API void coalesce();
extern DALIADMIN_API void mpping(const char *eps);
extern DALIADMIN_API void daliping(const char *dalis, unsigned connecttime, unsigned n);

extern DALIADMIN_API void validateStore(bool fix, bool deleteFiles, bool verbose);
extern DALIADMIN_API void removeOrphanedGlobalVariables(bool dryrun, bool reconstruct);

} // namespace daadmin