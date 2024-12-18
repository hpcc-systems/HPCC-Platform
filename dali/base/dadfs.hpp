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

/**
 * Distributed File System: access to directories and files on the cluster.
 *
 * The distributed file system is based on sprayed files in distributed directories.
 * Each DistributedFile is spread across multiple DistributedFileParts, which can be
 * accessed from DistributedFileDirectory or DistributedSuperFile.
 */

#ifndef DADFS_HPP
#define DADFS_HPP

#ifdef DALI_EXPORTS
#define da_decl DECL_EXPORT
#else
#define da_decl DECL_IMPORT
#endif

#include "jiface.hpp"
#include "jiter.hpp"
#include "jtime.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "mpbase.hpp"
#include "dafdesc.hpp"
#include "seclib.hpp"
#include "errorlist.h"

#include <vector>
#include <string>

enum : unsigned {
    DALI_DUPLICATE_STORAGE_PLANE = DALI_ERROR_START
};

typedef __int64 DistributedLockID;
#define FOREIGN_DALI_TIMEOUT (1000*60*5)

interface IPropertyTree;
interface IUserDescriptor;

#define S_LINK_RELATIONSHIP_KIND "link"
#define S_VIEW_RELATIONSHIP_KIND "view"

#define ITERATE_FILTEREDFILES_LIMIT 100000

interface IDistributedSuperFile;
interface IDistributedFile;
class CDfsLogicalFileName;

interface IDistributedFileIterator: public IIteratorOf<IDistributedFile>
{
public:
    virtual StringBuffer &getName(StringBuffer &name) = 0;
};

typedef IIteratorOf<IPropertyTree> IDFAttributesIterator;


interface IDFScopeIterator: public IInterface
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual const char *query() = 0;
};

interface IDFProtectedIterator: public IInterface
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual const char *queryFilename() = 0;
    virtual const char *queryOwner() = 0;
    virtual bool isSuper() = 0;
};

enum DistributedFileCompareMode
{
    DFS_COMPARE_FILES_LOGICAL=1,                // compares date/size/crc only logical
    DFS_COMPARE_FILES_PHYSICAL=2,               // compares date/size only physical dates and sizes
    DFS_COMPARE_FILES_PHYSICAL_CRCS=3,          // compares physical crcs and sizes
};

enum DistributedFileCompareResult
{
    DFS_COMPARE_RESULT_FAILURE=-6,
    DFS_COMPARE_RESULT_DIFFER_OLDER=-2,
    DFS_COMPARE_RESULT_SAME_OLDER=-1,
    DFS_COMPARE_RESULT_SAME=0,
    DFS_COMPARE_RESULT_SAME_NEWER=1,
    DFS_COMPARE_RESULT_DIFFER_NEWER=2
};

enum GetFileClusterNamesType
{
    GFCN_NotFound=0,
    GFCN_Normal=1,
    GFCN_Super=2,
    GFCN_Foreign=3,
    GFCN_External=4
};


enum DFTransactionState {
    TAS_NONE,    // still working
    TAS_RETRY,   // retry (din-phil problems)
    TAS_SUCCESS, // committing
    TAS_FAILURE  // rolling back
};

// ==DISTRIBUTED FILES===================================================================================================

/**
 * Parts of a logic file, that is spread across the cluster. The same part can
 * be replicated in several machines of the cluster.
 */
interface IDistributedFilePart: implements IInterface
{
    virtual unsigned getPartIndex() = 0;                                        // Part index (0 based)

    virtual unsigned numCopies() = 0;                                           // number of copies of this part
                                                                                // note same as parent File numCopies

    virtual INode *getNode(unsigned copy=0) = 0;                                // Node containing specified part copy
    virtual INode *queryNode(unsigned copy=0) = 0;                              // Node containing specified part copy


    virtual RemoteFilename &getFilename(RemoteFilename &ret,unsigned copy = 0) = 0;     // get filename info
    virtual StringBuffer &getPartName(StringBuffer &name) = 0;                          // Tail Name (e.g. "test.d00._1_of_3")
    virtual StringBuffer &getPartDirectory(StringBuffer &name,unsigned copy = 0) = 0;   // get filename info

    virtual IPropertyTree &queryAttributes() = 0;                               // part attributes

    virtual bool lockProperties(unsigned timeoutms=INFINITE) = 0;               // must be called before updating
    virtual void unlockProperties(DFTransactionState state=TAS_NONE) = 0;         // must be called after updating

    virtual bool isHost(unsigned copy=0) = 0;                                   // file is located on this machine

    virtual offset_t getFileSize(bool allowphysical,bool forcephysical)=0; // gets the part filesize (NB this will be the *expanded* size)
    virtual offset_t getDiskSize(bool allowphysical,bool forcephysical)=0; // gets the part size on disk (NB this will be the compressed size)
    virtual bool    getModifiedTime(bool allowphysical,bool forcephysical,CDateTime &dt)=0; // gets the part date time

    virtual bool getCrc(unsigned &crc) = 0;             // block compressed returns false (not ~0)
    virtual unsigned getPhysicalCrc() = 0;              // can be used on compressed file (blocked and non-blocked)

    virtual unsigned bestCopyNum(const IpAddress &ip,unsigned rel=0) = 0;   // returns the (rel) 'closest' copy num to ip
                                                        // note does not affect cluster order

    virtual unsigned copyClusterNum(unsigned copy,unsigned *replicate=NULL)=0;      // map copy number to cluster (and optionally replicate number)
    virtual StringBuffer &getStorageFilePath(StringBuffer & path, unsigned copy)=0;
    virtual unsigned getStripeNum(unsigned copy)=0;
};


interface IDFPartFilter : public IInterface
{
    virtual bool includePart(unsigned part) = 0;
};

typedef IIteratorOf<IDistributedFilePart> IDistributedFilePartIterator;

class CDFAction ;

#define DFUQFilterSeparator '|' // | is used as a separator because it is not a valid character for logical file name

enum DFUQFilterType
{
    DFUQFTwildcardMatch,
    DFUQFThasProp,
    DFUQFTcontainString,
    DFUQFTbooleanMatch,
    DFUQFTstringRange,
    DFUQFTintegerRange,
    DFUQFTinteger64Range,
    DFUQFTspecial,
    DFUQFTincludeFileAttr,
    DFUQFTinverseWildcardMatch
};

enum DFUQSerializeFileAttrOption
{
    DFUQSFAOincludeSuperOwner = 1
    //May add more
};

enum DFUQSpecialFilter
{
    DFUQSFFileNameWithPrefix = 1,
    DFUQSFFileType = 2,
    DFUQSFMaxFiles = 3
};

enum DFUQFileTypeFilter
{
    DFUQFFTall = 1,
    DFUQFFTsuperfileonly = 2,
    DFUQFFTnonsuperfileonly = 3
};

enum DFUQFilterField
{
    DFUQFFfiletype = 0,
    DFUQFFdescription = 1,
    DFUQFFdirectory = 2,
    DFUQFFgroup = 3,
    DFUQFFtimemodified = 4,
    DFUQFFname = 5,
    DFUQFFnumclusters = 6,
    DFUQFFnumparts = 7,
    DFUQFFpartmask = 8,
    DFUQFForigname = 9,
    DFUQFFattr = 10,
    DFUQFFattrjob = 11,
    DFUQFFattrowner = 12,
    DFUQFFattrrecordcount = 13,
    DFUQFFattrrecordsize = 14,
    DFUQFFattrsize = 15,
    DFUQFFattrcompressedsize = 16,
    DFUQFFattrworkunit = 17,
    DFUQFFcluster = 18,
    DFUQFFclusterdefaultbasedir = 19,
    DFUQFFclusterdefaultrepldir = 20,
    DFUQFFclustermapflags = 21,
    DFUQFFclustername = 22,
    DFUQFFpart = 23,
    DFUQFFpartname = 24,
    DFUQFFpartnum = 25,
    DFUQFFpartsize = 26,
    DFUQFFsuperowner = 27,
    DFUQFFsuperownername = 28,
    DFUQFFsubfile = 29,
    DFUQFFsubfilename = 30,
    DFUQFFsubfilenum = 31,
    DFUQFFkind = 32,
    DFUQFFaccessed = 33,
    DFUQFFattrmaxskew = 34,
    DFUQFFattrminskew = 35,
    DFUQFFterm = 36,
    DFUQFFreverse = 256,
    DFUQFFnocase = 512,
    DFUQFFnumeric = 1024,
    DFUQFFwild = 2048
};

enum DFUQResultField
{
    DFUQRFname = 0,
    DFUQRFdescription = 1,
    DFUQRFnodegroups = 2,
    DFUQRFkind = 3,
    DFUQRFtimemodified = 4,
    DFUQRFjob = 5,
    DFUQRFowner = 6,
    DFUQRFrecordcount = 7,
    DFUQRForigrecordcount = 8,
    DFUQRFrecordsize = 9,
    DFUQRFsize = 10,
    DFUQRForigsize = 11,
    DFUQRFworkunit = 12,
    DFUQRFnodegroup = 13,
    DFUQRFnumsubfiles = 14,
    DFUQRFaccessed = 15,
    DFUQRFnumparts = 16,
    DFUQRFcompressedsize = 17,
    DFUQRFdirectory = 18,
    DFUQRFpartmask = 19,
    DFUQRFsuperowners = 20,
    DFUQRFpersistent = 21,
    DFUQRFprotect = 22,
    DFUQRFiscompressed = 23,
    DFUQRFcost = 24,
    DFUQRFnumDiskReads = 25,
    DFUQRFnumDiskWrites = 26,
    DFUQRFatRestCost = 27,
    DFUQRFaccessCost = 28,
    DFUQRFmaxSkew = 29,
    DFUQRFminSkew = 30,
    DFUQRFmaxSkewPart = 31,
    DFUQRFminSkewPart = 32,
    DFUQRFreadCost = 33,
    DFUQRFwriteCost = 34,
    DFUQRFterm = 35, // must be last in list
    DFUQRFreverse = 256,
    DFUQRFnocase = 512,
    DFUQRFnumeric = 1024,
    DFUQRFfloat = 2048
};

extern da_decl const char* getDFUQFilterFieldName(DFUQFilterField field);
extern da_decl const char* getDFUQResultFieldName(DFUQResultField field);

/**
 * File operations can be included in a transaction to ensure that multiple
 * updates are handled atomically. This is the interface to a transaction
 * instance. Auto-commit state when transaction is not active (ie. you
 * don't want the actions to be executed on commit).
 */
interface IDistributedFileTransaction: extends IInterface
{
    virtual void start()=0;
    virtual void commit()=0;
    virtual void rollback()=0;
    virtual bool active()=0;
// TBD: shouldn't really be necessary, lookups should auto-add to transaction
    virtual IDistributedFile *lookupFile(const char *lfn, AccessMode accessMode, unsigned timeout=INFINITE)=0;
    virtual IDistributedSuperFile *lookupSuperFile(const char *slfn, AccessMode accessMode, unsigned timeout=INFINITE)=0;
};

interface IDistributedSuperFileIterator: extends IIteratorOf<IDistributedSuperFile>
{
    virtual const char *queryName() = 0;
};

interface ICodeContext;
/**
 * A distributed file, composed of one or more DistributedFileParts.
 */
interface IDistributedFile: extends IInterface
{
    virtual unsigned numParts() = 0;
    virtual IDistributedFilePart &queryPart(unsigned idx) = 0;
    virtual IDistributedFilePart* getPart(unsigned idx) = 0;

    virtual StringBuffer &getLogicalName(StringBuffer &name) = 0;               // logical name of file (including scope e.g. scope::name )
    virtual const char *queryLogicalName() = 0;                                 // ditto

    virtual IDistributedFilePartIterator *getIterator(IDFPartFilter *filter=NULL) = 0;

    virtual void rename(const char *logicalname,IUserDescriptor *user) = 0;     // simple rename (no file copy)

    virtual IFileDescriptor *getFileDescriptor(const char *clustername=NULL) = 0;   // get file descriptor used for file system access
                                                                                // if clustername specified makes filedesc for only that cluster

    virtual const char *queryDefaultDir() = 0;                                  // default directory (note primary dir)
    virtual const char *queryPartMask() = 0;                                    // default part name mask

    virtual void attach(const char *logicalname,IUserDescriptor *user) = 0;     // attach to name in DFS
    virtual void detach(unsigned timeoutms=INFINITE, ICodeContext *ctx=NULL) = 0; // no longer attached to name in DFS

    virtual IPropertyTree &queryAttributes() = 0;                               // DFile attributes

    virtual bool lockProperties(unsigned timeoutms=INFINITE) = 0;               // must be called before updating properties (will discard uncommitted changes)
    virtual void unlockProperties(DFTransactionState state=TAS_NONE) = 0;         // must be called after updating properties

    virtual bool getModificationTime(CDateTime &dt) = 0;                        // get date and time last modified (returns false if not set)
    virtual void setModificationTime(const CDateTime &dt) = 0;                  // set date and time last modified
    virtual void setModified() = 0;                                             // set date and time last modified to now (local time)

    virtual bool getAccessedTime(CDateTime &dt) = 0;                            // get date and time last accessed (returns false if not set)
    virtual void setAccessedTime(const CDateTime &dt) = 0;                      // set date and time last accessed
    virtual void setAccessed() = 0;                                             // set date and time last accessed to now (local time)
    virtual void addAttrValue(const char *attr, unsigned __int64 value) = 0;    // atomic add to attribute value
    virtual unsigned numCopies(unsigned partno) = 0;                            // number of copies

    virtual bool existsPhysicalPartFiles(unsigned short port) = 0;              // returns true if physical patrs all exist (on primary OR secondary)

    virtual bool renamePhysicalPartFiles(const char *newlfn,const char *cluster=NULL,IMultiException *exceptions=NULL,const char *newbasedir=NULL) = 0;           // renames all physical part files
                                                                                // returns true if no major errors


    virtual __int64 getFileSize(bool allowphysical,bool forcephysical)=0;       // gets the total file size (forcephysical doesn't use cached value)
    virtual __int64 getDiskSize(bool allowphysical,bool forcephysical)=0;       // gets the part size on disk (NB this will be the compressed size)
    virtual bool getFileCheckSum(unsigned &checksum)=0;                         // gets a single checksum for the logical file, based on the part crc's
    virtual unsigned getPositionPart(offset_t pos,offset_t &base)=0;            // get the part for a given position and the base offset of that part

    virtual IDistributedSuperFile *querySuperFile()=0;                          // returns non NULL if superfile
    virtual IDistributedSuperFileIterator *getOwningSuperFiles(IDistributedFileTransaction *_transaction=NULL)=0;           // returns iterator for all parents
    virtual bool isCompressed(bool *blocked=NULL)=0;

    virtual StringBuffer &getClusterName(unsigned clusternum,StringBuffer &name) = 0;
    virtual unsigned getClusterNames(StringArray &clusters)=0;                  // returns ordinality
                                                                                      // (use findCluster)
    virtual unsigned numClusters()=0;
    virtual unsigned findCluster(const char *clustername)=0;

    virtual ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum)=0;
    virtual IGroup *queryClusterGroup(unsigned clusternum)=0;
    virtual StringBuffer &getClusterGroupName(unsigned clusternum, StringBuffer &name)=0;
    virtual StringBuffer &getECL(StringBuffer &buf) = 0;
    virtual void setECL(const char *ecl) = 0;

    virtual void addCluster(const char *clustername,const ClusterPartDiskMapSpec &mspec) = 0;
    virtual bool removeCluster(const char *clustername) = 0;    // doesn't delete parts
    virtual bool checkClusterCompatible(IFileDescriptor &fdesc, StringBuffer &err) = 0;
    virtual void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec)=0;

    virtual void setPreferredClusters(const char *clusters) = 0;
    virtual void setSingleClusterOnly() = 0;

    virtual bool getFormatCrc(unsigned &crc) =0;   // CRC for record format
    virtual bool getRecordSize(size32_t &rsz) =0;
    virtual bool getRecordLayout(MemoryBuffer &layout, const char *attrname) =0;


    virtual void enqueueReplicate()=0;

    virtual StringBuffer &getColumnMapping(StringBuffer &mapping) =0;
    virtual void setColumnMapping(const char *mapping) =0;

    virtual bool canModify(StringBuffer &reason) = 0;
    virtual bool canRemove(StringBuffer &reason,bool ignoresub=false) = 0;
    virtual void setProtect(const char *callerid, bool protect=true, unsigned timeoutms=INFINITE) = 0;
                                                                                            // sets or clears deletion protection
                                                                                            // returns true if locked (by anyone) after action
                                                                                            // if callerid NULL and protect=false, clears all
    virtual bool isRestrictedAccess() = 0;
    virtual void setRestrictedAccess(bool restricted) = 0;
    virtual unsigned setDefaultTimeout(unsigned timems) = 0;                                // sets default timeout for SDS connections and locking
                                                                                            // returns previous value

    virtual void validate() = 0;

    virtual IPropertyTree *queryHistory() const = 0;                         // DFile History records
    virtual void resetHistory() = 0;
    virtual bool isExternal() const = 0;
    virtual bool getSkewInfo(unsigned &maxSkew, unsigned &minSkew, unsigned &maxSkewPart, unsigned &minSkewPart, bool calculateIfMissing) = 0;
    virtual int  getExpire(StringBuffer *expirationDate) = 0;
    virtual void setExpire(int expireDays) = 0;
    virtual void getCost(const char * cluster, cost_type & atRestCost, cost_type & accessCost) = 0;
};


// ==SUPER FILES===================================================================================================

/**
 * A DistributedSuperFile is a collection of specific logical files (wildcard
 * expansion, lists, sequences of fileservice calls, etc), that can still be
 * treated as an IDistributedFile.
 *
 * All sub files must have the same record layout, format, etc.
 */
interface IDistributedSuperFile: extends IDistributedFile
{

    virtual IDistributedFileIterator *getSubFileIterator(bool supersub=false)=0;    // if supersub true recurse through sub-superfiles and only return normal files
    virtual void addSubFile(const char *subfile,
                            bool before=false,              // if true before other
                            const char *other=NULL,         // if NULL add at end (before=false) or start(before=true)
                            bool addcontents=false,         // if true add contents of subfile (assuming it is a superfile)
                            IDistributedFileTransaction *transaction=NULL
                         )=0;
    virtual bool removeSubFile(const char *subfile,         // if NULL removes all
                                bool remsub,                // if true removes subfiles from DFS
                                bool remcontents=false,     // if true removes contents of subfile (assuming it is a superfile)
                                IDistributedFileTransaction *transaction=NULL)=0;
                            // Note does not delete subfile
    virtual bool removeOwnedSubFiles(bool remsub,           // if true removes subfiles from DFS
                                     IDistributedFileTransaction *transaction=NULL)=0;
                            // Note does not delete subfile
    virtual bool swapSuperFile( IDistributedSuperFile *_file,               // swaps sub files
                                IDistributedFileTransaction *transaction)=0;

    virtual IDistributedFile &querySubFile(unsigned idx,bool sub=false)=0;
    virtual IDistributedFile *getSubFile(unsigned idx,bool sub=false)=0;
    virtual IDistributedFile *querySubFileNamed(const char *name,bool sub=true)=0;
    virtual unsigned numSubFiles(bool sub=false)=0;

    // useful part functions
    virtual bool isInterleaved()=0;
    virtual IDistributedFile *querySubPart(unsigned partidx,                // superfile part index
                                           unsigned &subfilepartidx)=0;     // part index in subfile
                                // returns file for part (not linked) NULL if not found
};

extern da_decl unsigned getSuperFileSubs(IDistributedSuperFile *super, IArrayOf<IDistributedFile> &subFiles, bool superSub=false);

interface ISimpleSuperFileEnquiry: extends IInterface // lightweight local
{
    virtual unsigned numSubFiles() const = 0;
    virtual bool getSubFileName(unsigned num, StringBuffer &name) const = 0;
    virtual unsigned findSubName(const char *subname) const = 0;
    virtual unsigned getContents(StringArray &contents) const = 0;
};


// ==DISTRIBUTED FILE PROPERTY LOCKS============================================================================
/*
 * Context-based file property locking mechanism. Allows early unlocking for special cases,
 * stores the reload flag and allows you to query the 'Attr' section of the file property,
 * which is the only part external consumers are allowed to change.
 *
 * Use this instead of locking/unlocking manually. Manual lock is deprecated and will
 * disappear soon.
 */
class DistributedFilePropertyLock {
protected:
    IDistributedFile *file;
    bool reload;
    bool unlocked;
public:
    DistributedFilePropertyLock(IDistributedFile *_file)
        : file(_file), reload(false), unlocked(false)
    {
        reload = file->lockProperties();
    }
    ~DistributedFilePropertyLock()
    {
        if (!unlocked)
            unlock();
    }
    void unlock()
    {
        file->unlockProperties(TAS_NONE);
        unlocked = true;
    }
    void commit()
    {
        file->unlockProperties(TAS_SUCCESS);
        unlocked = true;
    }
    // MORE: Implement rollback when necessary
    IPropertyTree &queryAttributes()
    {
        return file->queryAttributes();
    }
    IDistributedFile *queryFile()
    {
        return file;
    }
    bool needsReload()
    {
        return reload;
    }
};


// ==DISTRIBUTED FILE DIRECTORY=========================================================================================

interface IDfsLogicalFileNameIterator : extends IInterface
{
  virtual bool first() = 0;
  virtual bool next() = 0;
  virtual bool isValid() = 0;
  virtual CDfsLogicalFileName & query() = 0;
};


// redirection
interface IDFSredirection: extends IInterface
{
    virtual IDfsLogicalFileNameIterator *getMatch(const char *infn)=0;
    virtual void update(const char *targpat, const char *targrepl, unsigned idx)=0;
    virtual bool getEntry(unsigned idx,StringBuffer &pat,StringBuffer &repl)=0;
    virtual unsigned numEntries()=0;
};

interface IFileRelationship: extends IInterface
{
    virtual const char *queryKind()=0;
    virtual const char *queryPrimaryFilename()=0;
    virtual const char *querySecondaryFilename()=0;
    virtual const char *queryPrimaryFields()=0;
    virtual const char *querySecondaryFields()=0;
    virtual const char *queryCardinality()=0;
    virtual bool isPayload()=0;
    virtual const char *queryDescription()=0;
    virtual IPropertyTree *queryTree()=0;
};

typedef IIteratorOf<IFileRelationship> IFileRelationshipIterator;

/**
 * A distributed directory. Can created, access and delete files, super-files and logic-files.
 */

enum class GetFileTreeOpts
{
    none                          = 0x0,
    expandNodes                   = 0x1,
    appendForeign                 = 0x2,
    remapToService                = 0x4,
    suppressForeignRemapToService = 0x8
};
BITMASK_ENUM(GetFileTreeOpts);

interface IDistributedFileDirectory: extends IInterface
{
    virtual IDistributedFile *lookup(   const char *logicalname,
                                        IUserDescriptor *user,
                                        AccessMode accessMode,
                                        bool hold,
                                        bool lockSuperOwner,
                                        IDistributedFileTransaction *transaction, // transaction only used for looking up superfile sub file
                                        bool privilegedUser,
                                        unsigned timeout=INFINITE
                                    ) = 0;  // links, returns NULL if not found

    virtual IDistributedFile *lookup(   CDfsLogicalFileName &logicalname,
                                        IUserDescriptor *user,
                                        AccessMode accessMode,
                                        bool hold,
                                        bool lockSuperOwner,
                                        IDistributedFileTransaction *transaction, // transaction only used for looking up superfile sub files
                                        bool privilegedUser,
                                        unsigned timeout=INFINITE
                                    ) = 0;  // links, returns NULL if not found

    virtual IDistributedFile *createNew(IFileDescriptor *desc, const char *optName=nullptr) = 0;
    virtual IDistributedFile *createExternal(IFileDescriptor *desc, const char *name) = 0;

    virtual IDistributedFileIterator *getIterator(const char *wildname, bool includesuper, IUserDescriptor *user,bool isPrivilegedUser) = 0;
            // wildname is in form scope/name and may contain wild components for either
    virtual IDFAttributesIterator *getDFAttributesIterator(const char *wildname, IUserDescriptor *user, bool recursive=true, bool includesuper=false, INode *foreigndali=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT) = 0;
    virtual IPropertyTreeIterator *getDFAttributesTreeIterator(const char *filters, DFUQResultField* localFilters,
        const char *localFilterBuf, IUserDescriptor *user, bool recursive, bool& allMatchingFilesReceived, INode *foreigndali=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT) = 0;
    virtual IDFAttributesIterator *getForeignDFAttributesIterator(const char *wildname, IUserDescriptor *user, bool recursive=true, bool includesuper=false, const char *foreigndali="", unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT) = 0;

    virtual IDFScopeIterator *getScopeIterator(IUserDescriptor *user, const char *subscope=NULL,bool recursive=true,bool includeempty=false)=0;

    // Removes files and super-files with format: context/file@cluster
    virtual bool removeEntry(const char *name, IUserDescriptor *user, IDistributedFileTransaction *transaction=NULL, unsigned timeoutms=INFINITE, bool throwException=false) = 0;
    virtual void renamePhysical(const char *oldname,const char *newname,IUserDescriptor *user,IDistributedFileTransaction *transaction) = 0;                         // renames the physical parts as well as entry
    virtual void removeEmptyScope(const char *scope) = 0;   // does nothing if called on non-empty scope


    virtual bool exists(const char *logicalname,IUserDescriptor *user,bool notsuper=false,bool superonly=false) = 0;                           // logical name exists
    virtual bool existsPhysical(const char *logicalname,IUserDescriptor *user) = 0;                                                    // physical parts exists

    virtual IPropertyTree *getFileTree(const char *lname, IUserDescriptor *user, const INode *foreigndali=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT, GetFileTreeOpts opts = GetFileTreeOpts::expandNodes|GetFileTreeOpts::appendForeign) =0;
    virtual IFileDescriptor *getFileDescriptor(const char *lname, AccessMode accessMode, IUserDescriptor *user, const INode *foreigndali=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT) =0;

    virtual IDistributedSuperFile *createSuperFile(const char *logicalname,IUserDescriptor *user,bool interleaved,bool ifdoesnotexist=false,IDistributedFileTransaction *transaction=NULL) = 0;
    virtual IDistributedSuperFile *createNewSuperFile(IPropertyTree *tree, const char *optionamName=nullptr, IArrayOf<IDistributedFile> *subFiles=nullptr) = 0;
    virtual IDistributedSuperFile *lookupSuperFile(const char *logicalname, IUserDescriptor *user, AccessMode accessMode,
                                                    IDistributedFileTransaction *transaction=NULL, // transaction only used for looking up sub files
                                                    unsigned timeout=INFINITE) = 0;  // NB lookup will also return superfiles
    virtual void removeSuperFile(const char *_logicalname, bool delSubs=false, IUserDescriptor *user=NULL, IDistributedFileTransaction *transaction=NULL)=0;

    virtual SecAccessFlags getFilePermissions(const char *lname,IUserDescriptor *user,unsigned auditflags=0)=0; // see dasess for auditflags values
    virtual SecAccessFlags getFScopePermissions(const char *scope,IUserDescriptor *user,unsigned auditflags=0)=0; // see dasess for auditflags values
    virtual void setDefaultUser(IUserDescriptor *user)=0;
    virtual IUserDescriptor* queryDefaultUser()=0;
    virtual SecAccessFlags getFDescPermissions(IFileDescriptor *,IUserDescriptor *user,unsigned auditflags=0)=0;
    virtual SecAccessFlags getDLFNPermissions(CDfsLogicalFileName &dlfn,IUserDescriptor *user,unsigned auditflags=0)=0;
    virtual SecAccessFlags getDropZoneScopePermissions(const char *dropZoneName,const char *dropZonePath,IUserDescriptor *user,unsigned auditflags=0)=0;

    virtual DistributedFileCompareResult fileCompare(const char *lfn1,const char *lfn2,DistributedFileCompareMode mode,StringBuffer &errstr,IUserDescriptor *user)=0;
    virtual bool filePhysicalVerify(const char *lfn1,IUserDescriptor *user,bool includecrc,StringBuffer &errstr)=0;

    virtual void setDefaultPreferredClusters(const char *clusters)=0;   // comma separated list of clusters


    virtual GetFileClusterNamesType getFileClusterNames(const char *logicalname,StringArray &out)=0;

    virtual bool isSuperFile( const char *logicalname, IUserDescriptor *user, INode *foreigndali=NULL, unsigned timeout=INFINITE) = 0;

    // Local 'lightweight' routines
    virtual void promoteSuperFiles(unsigned numsf,const char **sfnames,const char *addsubnames,bool delsub,bool createonlyonesuperfile,IUserDescriptor *user,unsigned timeout, StringArray &outunlinked)=0;
    virtual bool getFileSuperOwners(const char *logicalname, StringArray &owners)=0; // local only
    virtual ISimpleSuperFileEnquiry * getSimpleSuperFileEnquiry(const char *logicalname,const char *dbgtitle,IUserDescriptor *udesc,unsigned timeout=INFINITE)=0; // NB must be local!

    virtual IDFSredirection & queryRedirection()=0;

    virtual void addFileRelationship(
        const char *primary,
        const char *secondary,
        const char *primflds,
        const char *secflds,
        const char *kind,
        const char *cardinality,
        bool payload,
        IUserDescriptor *user,
        const char *description=NULL
    )=0;

    virtual IFileRelationshipIterator *lookupFileRelationships(
        const char *primary,
        const char *secondary,
        const char *primflds=NULL,
        const char *secflds=NULL,
        const char *kind=S_LINK_RELATIONSHIP_KIND,
        const char *cardinality=NULL,
        const bool *payload=NULL,
        const char *foreigndali="",
        unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT
    )=0;


    virtual void removeFileRelationships(
        const char *primary,
        const char *secondary,
        const char *primflds=NULL,
        const char *secflds=NULL,
        const char *kind=S_LINK_RELATIONSHIP_KIND
    )=0;

    virtual void removeAllFileRelationships(const char *filename)=0;
    virtual IFileRelationshipIterator *lookupAllFileRelationships(const char *filename)=0; // either primary or secondary

    virtual bool loadScopeContents(const char *scopelfn,
                                   StringArray *scopes,
                                   StringArray *supers,
                                   StringArray *files,
                                   bool includeemptyscopes=false
                                ) = 0; // if scope returns subfiles and subdirs

    virtual bool publishMetaFileXML(const CDfsLogicalFileName &logicalname,
                                    IUserDescriptor *user) = 0;

    virtual bool isProtectedFile( // needs no lock on file
                                      const CDfsLogicalFileName &logicalname,
                                      unsigned timeout=0                // 0 = return state immediately, >0 waits until false or timed-out
                                     ) = 0;

    virtual IDFProtectedIterator *lookupProtectedFiles(const char *owner=NULL,bool notsuper=false,bool superonly=false)=0; // if owner = NULL then all
    virtual IDFAttributesIterator* getLogicalFilesSorted(IUserDescriptor* udesc, DFUQResultField *sortOrder, const void* filters, DFUQResultField *localFilters,
            const void *specialFilterBuf, unsigned startOffset, unsigned maxNum, __int64 *cacheHint, unsigned *total, bool *allMatchingFilesReceived) = 0;
    virtual IDFAttributesIterator* getLogicalFiles(IUserDescriptor* udesc, DFUQResultField *sortOrder, const void* filters, DFUQResultField *localFilters,
            const void *specialFilterBuf, unsigned startOffset, unsigned maxNum, __int64 *cacheHint, unsigned *total, bool *allMatchingFilesReceived, bool recursive, bool sorted) = 0;

    virtual unsigned setDefaultTimeout(unsigned timems) = 0;                                // sets default timeout for SDS connections and locking
                                                                                            // returns previous value

    // useful to clearup after temporary unpublished file.
    virtual bool removePhysicalPartFiles(const char *logicalName, IFileDescriptor *fileDesc, IMultiException *mexcept, unsigned numParallelDeletes=0) = 0;

    virtual void setFileAccessed(IUserDescriptor* udesc, const char *logicalName, const CDateTime &dt, const INode *foreigndali=nullptr, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT) = 0;
};



extern da_decl IDistributedFileDirectory &queryDistributedFileDirectory();



// ==GROUP STORE=================================================================================================

interface INamedGroupIterator: extends IInterface
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual StringBuffer &get(StringBuffer &name) = 0;
    virtual bool isCluster() = 0;
    virtual StringBuffer &getdir(StringBuffer &dir) = 0;
};

interface INamedGroupStore : extends IGroupResolver
{
    virtual IGroup *lookup(const char *logicalgroupname) = 0;
    virtual INamedGroupIterator *getIterator() = 0;
    virtual INamedGroupIterator *getIterator(IGroup *match, bool exact=false) = 0;
    virtual void add(const char *logicalgroupname, const std::vector<std::string> &hosts, bool cluster=false, const char *dir=NULL, GroupType groupType = grp_unknown) = 0;
    virtual void ensure(const char *logicalgroupname, const std::vector<std::string> &hosts, bool cluster=false, const char *dir=NULL, GroupType groupType = grp_unknown) = 0;
    virtual void ensureNasGroup(size32_t size) = 0;
    virtual StringBuffer &getNasGroupName(StringBuffer &groupName, size32_t size) const = 0;
    virtual unsigned removeNode(const char *logicalgroupname, const char *nodeToRemove) = 0;
    virtual void remove(const char *logicalgroupname) = 0;
    virtual void addUnique(IGroup *group,StringBuffer &lname,const char *dir=NULL) = 0;
    virtual void swapNode(const char *from, const char *to) = 0;
    virtual IGroup *lookup(const char *logicalgroupname, StringBuffer &dir, GroupType &groupType) = 0;
    virtual unsigned setDefaultTimeout(unsigned timems) = 0;     // sets default timeout for SDS connections and locking
    virtual unsigned setRemoteTimeout(unsigned timems) = 0;      // sets default timeout for remote SDS connections and locking
    virtual void resetCache() = 0;      // resets any cached lookups
};

extern da_decl INamedGroupStore  &queryNamedGroupStore();

extern da_decl bool decodeChildGroupName(const char *gname,StringBuffer &parentname, StringBuffer &range);
extern da_decl StringBuffer &encodeChildGroupRange(UnsignedArray &positions, StringBuffer &rangeText);


// ==MISC========================================================================================================

// Exceptions

interface da_decl IDFS_Exception: extends IException
{
};


enum DistributedFileSystemError
{
    DFSERR_ok,
    DFSERR_LogicalNameAlreadyExists,
    DFSERR_CannotFindPartFileSize,
    DFSERR_LookupAccessDenied,
    DFSERR_CreateAccessDenied,
    DFSERR_PhysicalPartAlreadyExists,
    DFSERR_PhysicalPartDoesntExist,
    DFSERR_ForeignDaliTimeout,
    DFSERR_CannotFindPartFileCrc,
    DFSERR_ClusterNotFound,
    DFSERR_ClusterAlreadyExists,
    DFSERR_LookupConnectionTimout,       // only raised if timeout specified on lookup etc.
    DFSERR_FailedToDeleteFile,
    DFSERR_PassIterateFilesLimit,
    DFSERR_RestrictedFileAccessDenied,
    DFSERR_EmptyStoragePlane,
    DFSERR_MissingStoragePlane,
    DFSERR_PhysicalCompressedPartInvalid,
    DFSERR_InvalidRemoteFileContext
};


// utility routines (used by xref and dfu)
extern da_decl RemoteFilename &constructPartFilename(IGroup *grp,unsigned partno,unsigned partmax,const char *name,const char *partmask,const char *partdir,unsigned copy,ClusterPartDiskMapSpec &mspec,RemoteFilename &rfn);
// legacy version
inline RemoteFilename &constructPartFilename(IGroup *grp,unsigned partno,unsigned partmax,const char *name,const char *partmask,const char *partdir,bool replicate,int replicateoffset,RemoteFilename &rfn,bool localmount=false)
{
    // local mount ignored!
    ClusterPartDiskMapSpec mspec;
    mspec.replicateOffset = replicateoffset;
    return constructPartFilename(grp,partno,partmax,name,partmask,partdir,replicate?1:0,mspec,rfn);
}

extern da_decl IDFPartFilter *createPartFilter(const char *filter);
/* format:
<part_filter> ::=  <part_range> [ ',' <part_filter> ]
<part_range>  ::=  <part_number>
              |    <part_number> '-' <part_number>
*/

// for server use
interface IDaliServer;
extern da_decl IDaliServer *createDaliDFSServer(IPropertyTree *config); // called for coven members

// to initialize clustergroups after clusters change in the environment
extern da_decl void initClusterGroups(bool force, StringBuffer &response, IPropertyTree *oldEnvironment, unsigned timems=INFINITE);
extern da_decl void initClusterAndStoragePlaneGroups(bool force, IPropertyTree *oldEnvironment, unsigned timems=INFINITE);
extern da_decl bool resetClusterGroup(const char *clusterName, const char *type, bool spares, StringBuffer &response, unsigned timems=INFINITE);
extern da_decl bool addClusterSpares(const char *clusterName, const char *type, const std::vector<std::string> &hosts, StringBuffer &response, unsigned timems=INFINITE);
extern da_decl bool removeClusterSpares(const char *clusterName, const char *type, const std::vector<std::string> &hosts, StringBuffer &response, unsigned timems=INFINITE);

// should poss. belong in lib workunit
extern da_decl StringBuffer &getClusterGroupName(const IPropertyTree &cluster, StringBuffer &groupName);
extern da_decl StringBuffer &getClusterSpareGroupName(const IPropertyTree &cluster, StringBuffer &groupName);
extern da_decl IGroup *getClusterNodeGroup(const char *clusterName, const char *type, unsigned timems=INFINITE); // returns the raw cluster group (as defined in the Cluster topology)
extern da_decl IGroup *getClusterProcessNodeGroup(const char *clusterName, const char *type, unsigned timems=INFINITE); // returns the group of all processes of cluster group (i.e. cluster group * slavesPerNode)

extern da_decl IDistributedFileTransaction *createDistributedFileTransaction(IUserDescriptor *user, ICodeContext *ctx=NULL);

extern da_decl const char *normalizeLFN(const char *s, StringBuffer &normalized);

extern da_decl IDFAttributesIterator *createSubFileFilter(IDFAttributesIterator *_iter,IUserDescriptor* _user, bool includesub, unsigned timems=INFINITE); // takes ownership of iter

extern da_decl GroupType translateGroupType(const char *groupType);

#define DFS_REPLICATE_QUEUE "dfs_replicate_queue"
#define DRQ_STOP 0
#define DRQ_REPLICATE 1


// Useful property query functions

inline bool isFileKey(const IPropertyTree &pt) { const char *kind = pt.queryProp("@kind"); return kind&&strieq(kind,"key"); }
inline bool isFileKey(IDistributedFile *f) { return isFileKey(f->queryAttributes()); }
inline bool isFileKey(IFileDescriptor *f) { return isFileKey(f->queryProperties()); }

inline bool isFilePartitionKey(IPropertyTree &pt) { return pt.hasProp("@partitionFieldMask"); }
inline bool isFilePartitionKey(IDistributedFile *f) { return isFilePartitionKey(f->queryAttributes()); }
inline bool isFilePartitionKey(IFileDescriptor *f) { return isFilePartitionKey(f->queryProperties()); }

inline bool isFileLocalKey(IPropertyTree &pt) { return pt.getPropBool("@local"); }
inline bool isFileLocalKey(IDistributedFile *f) { return isFileLocalKey(f->queryAttributes()); }
inline bool isFileLocalKey(IFileDescriptor *f) { return isFileLocalKey(f->queryProperties()); }

inline bool isPartTLK(IPropertyTree &pt) { const char *kind = pt.queryProp("@kind"); return kind&&strieq(kind,"topLevelKey"); }
inline bool isPartTLK(IDistributedFilePart *p) { return isPartTLK(p->queryAttributes()); }
inline bool isPartTLK(IPartDescriptor *p) { return isPartTLK(p->queryProperties()); }

da_decl bool hasTLK(IDistributedFile *f);

inline const char *queryFileKind(IPropertyTree &pt) { return pt.queryProp("@kind"); }
inline const char *queryFileKind(IDistributedFile *f) { return queryFileKind(f->queryAttributes()); }
inline const char *queryFileKind(IFileDescriptor *f) { return queryFileKind(f->queryProperties()); }

extern da_decl void ensureFileScope(const CDfsLogicalFileName &dlfn, unsigned timeoutms=INFINITE);

extern da_decl bool checkLogicalName(const char *lfn,IUserDescriptor *user,bool readreq,bool createreq,bool allowquery,const char *specialnotallowedmsg);

extern da_decl cost_type calcFileAtRestCost(const char * cluster, double sizeGB, double fileAgeDays);
extern da_decl cost_type calcFileAccessCost(const char * cluster, __int64 numDiskWrites, __int64 numDiskReads);
extern da_decl cost_type calcFileAccessCost(IDistributedFile *f, __int64 numDiskWrites, __int64 numDiskReads);
extern da_decl cost_type calcDiskWriteCost(const StringArray & clusters, stat_type numDiskWrites);
extern da_decl cost_type updateCostAndNumReads(IDistributedFile *file, stat_type numDiskReads, cost_type curReadCost=0); // Update readCost and numDiskReads - return calculated read cost
constexpr bool defaultPrivilegedUser = true;
constexpr bool defaultNonPrivilegedUser = false;

extern da_decl void configurePreferredPlanes();

// Get read cost from readCost field or calculate legacy read cost
// - migrateLegacyCost: if true, update readCost field with legacy read cost
template<typename Source>
inline cost_type getReadCost(IPropertyTree & fileAttr, Source source, bool migrateLegacyCost = false)
{
    if (fileAttr.hasProp(getDFUQResultFieldName(DFUQRFreadCost)))
        return fileAttr.getPropInt64(getDFUQResultFieldName(DFUQRFreadCost), 0);
    else
    {
        // Calculate legacy read cost from numDiskReads
        // (However, it is not possible to accurately calculate read cost for key
        // files, as the reads may have been from page cache and not from disk.)
        if (!isFileKey(fileAttr) && source)
        {
            stat_type numDiskReads = fileAttr.getPropInt64(getDFUQResultFieldName(DFUQRFnumDiskReads), 0);
            cost_type readCost = calcFileAccessCost(source, 0, numDiskReads);
            if (migrateLegacyCost)
                fileAttr.setPropInt64(getDFUQResultFieldName(DFUQRFreadCost), readCost);
            return readCost;
        }
    }
    return 0;
}

// Get read cost from readCost field or calculate legacy read cost
template<typename Source>
inline cost_type getReadCost(const IPropertyTree & fileAttr, Source source)
{
    if (fileAttr.hasProp(getDFUQResultFieldName(DFUQRFreadCost)))
        return fileAttr.getPropInt64(getDFUQResultFieldName(DFUQRFreadCost), 0);
    else
    {
        // Calculate legacy read cost from numDiskReads
        // (However, it is not possible to accurately calculate read cost for key
        // files, as the reads may have been from page cache and not from disk.)
        if (!isFileKey(fileAttr) && source)
        {
            stat_type numDiskReads = fileAttr.getPropInt64(getDFUQResultFieldName(DFUQRFnumDiskReads), 0);
            return calcFileAccessCost(source, 0, numDiskReads);
        }
    }
    return 0;
}

// Get write cost from writeCost field or calculate legacy write cost
// - migrateLegacyCost: if true, update writeCost field with legacy write cost
template<typename Source>
inline cost_type getWriteCost(IPropertyTree & fileAttr, Source source, bool migrateLegacyCost = false)
{
    if (fileAttr.hasProp(getDFUQResultFieldName(DFUQRFwriteCost)))
        return fileAttr.getPropInt64(getDFUQResultFieldName(DFUQRFwriteCost), 0);
    else
    {
        // Calculate legacy write cost from numDiskWrites
        if (source)
        {
            stat_type numDiskWrites = fileAttr.getPropInt64(getDFUQResultFieldName(DFUQRFnumDiskWrites), 0);
            cost_type writeCost = calcFileAccessCost(source, numDiskWrites, 0);
            if (migrateLegacyCost)
                fileAttr.setPropInt64(getDFUQResultFieldName(DFUQRFwriteCost), writeCost);
            return writeCost;
        }
    }
    return 0;
}

// Get write cost from writeCost field or calculate legacy write cost
template<typename Source>
inline cost_type getWriteCost(const IPropertyTree & fileAttr, Source source)
{
    if (fileAttr.hasProp(getDFUQResultFieldName(DFUQRFwriteCost)))
        return fileAttr.getPropInt64(getDFUQResultFieldName(DFUQRFwriteCost), 0);
    else
    {
        // Calculate legacy write cost from numDiskWrites
        if (source)
        {
            stat_type numDiskWrites = fileAttr.getPropInt64(getDFUQResultFieldName(DFUQRFnumDiskWrites), 0);
            return calcFileAccessCost(source, numDiskWrites, 0);
        }
    }
    return 0;
}

extern da_decl bool doesPhysicalMatchMeta(IPartDescriptor &partDesc, IFile &iFile, offset_t &expectedSize, offset_t &actualSize);
extern da_decl bool doesPhysicalMatchMeta(IDistributedFilePart &partDesc, IFile &iFile, offset_t &expectedSize, offset_t &actualSize);

#endif
