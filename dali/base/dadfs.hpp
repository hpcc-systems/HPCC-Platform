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

/**
 * Distributed File System: access to directories and files on the cluster.
 *
 * The distributed file system is based on sprayed files in distributed directories.
 * Each DistributedFile is spread across multiple DistributedFileParts, which can be
 * accessed from DistributedFileDirectory or DistributedSuperFile.
 */

#ifndef DADFS_HPP
#define DADFS_HPP

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

#include "jiface.hpp"
#include "jiter.hpp"
#include "jtime.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "mpbase.hpp"
#include "dafdesc.hpp"


typedef __int64 DistributedLockID;
#define FOREIGN_DALI_TIMEOUT (1000*60*5)

interface IPropertyTree;
interface IUserDescriptor;

#define S_LINK_RELATIONSHIP_KIND "link"
#define S_VIEW_RELATIONSHIP_KIND "view"



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
    virtual unsigned getCount() = 0;
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

    virtual IPropertyTree &queryProperties() = 0;                               // part properties

    virtual IPropertyTree &lockProperties(unsigned timeoutms=INFINITE) = 0;                             // must be called before updating
    virtual void unlockProperties() = 0;                                        // must be called after updating

    virtual bool isHost(unsigned copy=0) = 0;                                   // file is located on this machine

    virtual offset_t getFileSize(bool allowphysical,bool forcephysical)=0;                  // gets the part filesize (NB this will be the *expanded* size)
    virtual offset_t getDiskSize()=0;                                                       // gets the part size on disk (NB this will be the compressed size)
    virtual bool    getModifiedTime(bool allowphysical,bool forcephysical,CDateTime &dt)=0; // gets the part date time

    virtual bool getCrc(unsigned &crc) = 0;             // block compressed returns false (not ~0)
    virtual unsigned getPhysicalCrc() = 0;              // can be used on compressed file (blocked and non-blocked)

    virtual unsigned bestCopyNum(const IpAddress &ip,unsigned rel=0) = 0;   // returns the (rel) 'closest' copy num to ip
                                                        // note does not affect cluster order

    virtual unsigned copyClusterNum(unsigned copy,unsigned *replicate=NULL)=0;      // map copy number to cluster (and optionally replicate number)

};


interface IDFPartFilter : public IInterface
{
    virtual bool includePart(unsigned part) = 0;
};

typedef IIteratorOf<IDistributedFilePart> IDistributedFilePartIterator;

class CDFAction ;

/**
 * File operations can be included in a transaction to ensure that multiple
 * updates are handled atomically. This is the interface to a transaction
 * instance.
 */
interface IDistributedFileTransaction: extends IInterface
{
    virtual void start()=0;
    virtual void commit()=0;
    virtual void rollback()=0;
    virtual bool active()=0;
    virtual bool setActive(bool on)=0; // returns old value (used internally)
    virtual IDistributedFile *lookupFile(const char *lfn,unsigned timeout=INFINITE)=0;
    virtual IDistributedSuperFile *lookupSuperFile(const char *slfn,bool fixmissing=false,unsigned timeout=INFINITE)=0;
    virtual IUserDescriptor *queryUser()=0;
    virtual bool addDelayedDelete(const char *lfn,bool remphys,IUserDescriptor *user)=0; // used internally to delay deletes untill commit 
    virtual void addAction(CDFAction *action)=0; // internal
    virtual void clearFiles()=0; // internal
    virtual IDistributedFileTransaction *baseTransaction()=0;
};

interface IDistributedSuperFileIterator: extends IIteratorOf<IDistributedSuperFile>
{
    virtual const char *queryName() = 0;
};

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

    virtual void rename(const char *logicalname,IDistributedFileTransaction *transaction=NULL,IUserDescriptor *user=NULL) = 0;                          // simple rename (no file copy)

    virtual IFileDescriptor *getFileDescriptor(const char *clustername=NULL) = 0;   // get file descriptor used for file system access
                                                                                // if clustername specified makes filedesc for only that cluster

    virtual const char *queryDefaultDir() = 0;                                  // default directory (note primary dir)
    virtual const char *queryPartMask() = 0;                                    // default part name mask

    virtual void attach(const char *logicalname,IDistributedFileTransaction *transaction=NULL,IUserDescriptor *user=NULL) = 0;                          // attach to name in DFS
    virtual void detach(IDistributedFileTransaction *transaction=NULL) = 0;                                                 // no longer attached to name in DFS

    virtual IPropertyTree &queryProperties() = 0;                               // DFile properties

    virtual IPropertyTree &lockProperties(unsigned timeoutms=INFINITE) = 0;     // must be called before updating
    virtual void unlockProperties() = 0;                                        // must be called after updating

    virtual bool getModificationTime(CDateTime &dt) = 0;                        // get date and time last modified (returns false if not set)
    virtual void setModificationTime(const CDateTime &dt) = 0;                  // set date and time last modified
    virtual void setModified() = 0;                                             // set date and time last modified to now (local time)

    virtual bool getAccessedTime(CDateTime &dt) = 0;                            // get date and time last accessed (returns false if not set)
    virtual void setAccessedTime(const CDateTime &dt) = 0;                      // set date and time last accessed
    virtual void setAccessed() = 0;                                             // set date and time last accessed to now (local time)

    virtual unsigned numCopies(unsigned partno) = 0;                            // number of copies

    virtual bool removePhysicalPartFiles(unsigned short port=0,const char *cluster=NULL,IMultiException *exceptions=NULL) = 0;          // removes all physical part files
                                                                                // returns true if no major errors

    virtual bool existsPhysicalPartFiles(unsigned short port) = 0;              // returns true if physical patrs all exist (on primary OR secondary)

    virtual bool renamePhysicalPartFiles(const char *newlfn,const char *cluster=NULL,unsigned short port=0,IMultiException *exceptions=NULL,const char *newbasedir=NULL) = 0;           // renames all physical part files
                                                                                // returns true if no major errors


    virtual __int64 getFileSize(bool allowphysical,bool forcephysical)=0;       // gets the total file size (forcephysical doesn't use cached value)
    virtual bool getFileCheckSum(unsigned &checksum)=0;                         // gets a single checksum for the logical file, based on the part crc's
    virtual unsigned getPositionPart(offset_t pos,offset_t &base)=0;            // get the part for a given position and the base offset of that part

    virtual IDistributedSuperFile *querySuperFile()=0;                          // returns non NULL if superfile
    virtual bool isSubFile()=0;                                         // returns true if sub file of any SuperFile
    virtual IDistributedSuperFileIterator *getOwningSuperFiles(IDistributedFileTransaction *_transaction=NULL)=0;           // returns iterator for all parents
    virtual bool isCompressed(bool *blocked=NULL)=0;

    virtual bool lockTransaction(unsigned timeout)=0;                           // internal use
    virtual void unlockTransaction(bool _commit,bool _rollback)=0;              // internal use

    virtual StringBuffer &getClusterName(unsigned clusternum,StringBuffer &name) = 0;
    virtual unsigned getClusterNames(StringArray &clusters)=0;                  // returns ordinality
                                                                                      // (use findCluster)
    virtual unsigned numClusters()=0;
    virtual unsigned findCluster(const char *clustername)=0;

    virtual ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum)=0;
    virtual IGroup *queryClusterGroup(unsigned clusternum)=0;

    virtual StringBuffer &getECL(StringBuffer &buf) = 0;
    virtual void setECL(const char *ecl) = 0;

    virtual void addCluster(const char *clustername,ClusterPartDiskMapSpec &mspec) = 0; 
    virtual void removeCluster(const char *clustername) = 0;    // doesn't delete parts
    virtual bool checkClusterCompatible(IFileDescriptor &fdesc, StringBuffer &err) = 0;
    virtual void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec)=0;

    virtual void setPreferredClusters(const char *clusters) = 0;
    virtual void setSingleClusterOnly() = 0;

    virtual bool getFormatCrc(unsigned &crc) =0;   // CRC for record format 
    virtual bool getRecordSize(size32_t &rsz) =0;   
    virtual bool getRecordLayout(MemoryBuffer &layout) =0;   


    virtual void enqueueReplicate()=0;

    virtual StringBuffer &getColumnMapping(StringBuffer &mapping) =0;   
    virtual void setColumnMapping(const char *mapping) =0;   

    virtual bool cannotRemove(StringBuffer &reason) = 0;  // returns true if is query, external, foreign, subfile or protected
                                                          // if returns true then reason filled in
    virtual void setProtect(const char *callerid, bool protect=true, unsigned timeoutms=INFINITE) = 0;                  
                                                                                            // sets or clears deletion protection
                                                                                            // returns true if locked (by anyone) after action
                                                                                            // if callerid NULL and protect=false, clears all
    
    virtual unsigned setDefaultTimeout(unsigned timems) = 0;                                // sets default timeout for SDS connections and locking
                                                                                            // returns previous value
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
                                bool remphys,               // if true removes physical parts of sub file
                                bool remcontents=false,     // if true removes contents of subfile (assuming it is a superfile)
                                IDistributedFileTransaction *transaction=NULL,
                                bool delayed = false)=0;
                            // Note does not delete subfile
    virtual bool swapSuperFile( IDistributedSuperFile *_file,               // swaps sub files
                                IDistributedFileTransaction *transaction)=0;

    virtual IDistributedFile &querySubFile(unsigned idx,bool sub=false)=0;
    virtual IDistributedFile *getSubFile(unsigned idx,bool sub=false)=0;
    virtual IDistributedFile *querySubFileNamed(const char *name,bool sub=true)=0;
    virtual unsigned numSubFiles(bool sub=false)=0;

    // useful part funtions
    virtual bool isInterleaved()=0;
    virtual IDistributedFile *querySubPart(unsigned partidx,                // superfile part index
                                           unsigned &subfilepartidx)=0;     // part index in subfile
                                // returns file for part (not linked) NULL if not found

    virtual bool cannotModify(StringBuffer &reason) = 0;  // returns true if protected
};


interface ISimpleSuperFileEnquiry: extends IInterface // lightweight local
{
    virtual unsigned numSubFiles() const = 0;
    virtual bool getSubFileName(unsigned num, StringBuffer &name) const = 0;
    virtual unsigned findSubName(const char *subname) const = 0;
    virtual unsigned getContents(StringArray &contents) const = 0;
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
interface IDistributedFileDirectory: extends IInterface
{
    virtual IDistributedFile *lookup(   const char *logicalname,
                                        IUserDescriptor *user=NULL,
                                        bool writeaccess=false,
                                        IDistributedFileTransaction *transaction=NULL, // transaction only used for looking up superfile sub files
                                        unsigned timeout=INFINITE
                                    ) = 0;  // links, returns NULL if not found

    virtual IDistributedFile *lookup(   const CDfsLogicalFileName &logicalname,
                                        IUserDescriptor *user=NULL,
                                        bool writeaccess=false,
                                        IDistributedFileTransaction *transaction=NULL, // transaction only used for looking up superfile sub files
                                        unsigned timeout=INFINITE
                                    ) = 0;  // links, returns NULL if not found

    virtual IDistributedFile *createNew(IFileDescriptor *desc,bool includeports=false) = 0;

    virtual IDistributedFileIterator *getIterator(const char *wildname, bool includesuper, IUserDescriptor *user=NULL) = 0;
            // wildname is in form scope/name and may contain wild components for either
    virtual IDFAttributesIterator *getDFAttributesIterator(const char *wildname, bool recursive=true, bool includesuper=false, INode *foreigndali=NULL,IUserDescriptor *user=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT) = 0;
    virtual IDFAttributesIterator *getForeignDFAttributesIterator(const char *wildname, bool recursive=true, bool includesuper=false, const char *foreigndali="",IUserDescriptor *user=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT) = 0;

    virtual IDFScopeIterator *getScopeIterator(const char *subscope=NULL,bool recursive=true,bool includeempty=false, IUserDescriptor *user=NULL)=0;

    virtual bool removeEntry(const char *name,IUserDescriptor *user=NULL) = 0;  // equivalent to lookup/detach/release
    virtual bool removePhysical(const char *name,unsigned short port=0,const char *cluster=NULL,IMultiException *exceptions=NULL,IUserDescriptor *user=NULL) = 0;                           // removes the physical parts as well as entry
    virtual bool renamePhysical(const char *oldname,const char *newname,unsigned short port=0,IMultiException *exceptions=NULL,IUserDescriptor *user=NULL) = 0;                         // renames the physical parts as well as entry
    virtual void removeEmptyScope(const char *scope) = 0;   // does nothing if called on non-empty scope
    

    virtual bool exists(const char *logicalname,bool notsuper=false,bool superonly=false,IUserDescriptor *user=NULL) = 0;                           // logical name exists
    virtual bool existsPhysical(const char *logicalname,IUserDescriptor *user=NULL) = 0;                                                    // physical parts exists

    virtual IPropertyTree *getFileTree(const char *lname,const INode *foreigndali=NULL,IUserDescriptor *user=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT, bool expandnodes=true) =0;
    virtual IFileDescriptor *getFileDescriptor(const char *lname,const INode *foreigndali=NULL,IUserDescriptor *user=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT) =0;

    virtual IDistributedSuperFile *createSuperFile(const char *logicalname,bool interleaved,bool ifdoesnotexist=false,IUserDescriptor *user=NULL) = 0;
    virtual IDistributedSuperFile *lookupSuperFile(const char *logicalname,IUserDescriptor *user=NULL,
                                                    IDistributedFileTransaction *transaction=NULL, // transaction only used for looking up sub files
                                                    bool fixmissing=false,  // used when removing
                                                    unsigned timeout=INFINITE

                                                ) = 0;  // NB lookup will also return superfiles 

    virtual int getFilePermissions(const char *lname,IUserDescriptor *user=NULL,unsigned auditflags=0)=0; // see dasess for auditflags values
    virtual void setDefaultUser(IUserDescriptor *user)=0;
    virtual IUserDescriptor* queryDefaultUser()=0;
    virtual int getNodePermissions(const IpAddress &ip,IUserDescriptor *user=NULL,unsigned auditflags=0)=0;
    virtual int getFDescPermissions(IFileDescriptor *,IUserDescriptor *user=NULL,unsigned auditflags=0)=0;

    virtual DistributedFileCompareResult fileCompare(const char *lfn1,const char *lfn2,DistributedFileCompareMode mode,StringBuffer &errstr,IUserDescriptor *user=NULL)=0;
    virtual bool filePhysicalVerify(const char *lfn1,bool includecrc,StringBuffer &errstr,IUserDescriptor *user=NULL)=0;

    virtual void setDefaultPreferredClusters(const char *clusters)=0;   // comma separated list of clusters


    virtual GetFileClusterNamesType getFileClusterNames(const char *logicalname,StringArray &out)=0;

    virtual bool isSuperFile( const char *logicalname, INode *foreigndali=NULL, IUserDescriptor *user=NULL, unsigned timeout=INFINITE) = 0;

    // Local 'lightweight' routines
    virtual void promoteSuperFiles(unsigned numsf,const char **sfnames,const char *addsubnames,bool delsub,bool createonlyonesuperfile,IUserDescriptor *user,unsigned timeout, StringArray &outunlinked)=0;
    virtual bool getFileSuperOwners(const char *logicalname, StringArray &owners)=0; // local only
    virtual ISimpleSuperFileEnquiry * getSimpleSuperFileEnquiry(const char *logicalname,const char *dbgtitle,unsigned timeout=INFINITE)=0; // NB must be local!

    virtual IDFSredirection & queryRedirection()=0;

    virtual void addFileRelationship(
        const char *primary,
        const char *secondary,
        const char *primflds,
        const char *secflds,
        const char *kind,
        const char *cardinality,
        bool payload,
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
                                    IUserDescriptor *user=NULL) = 0;

    virtual bool isProtectedFile( // needs no lock on file
                                      const CDfsLogicalFileName &logicalname,
                                      unsigned timeout=0                // 0 = return state immediately, >0 waits until false or timed-out
                                     ) = 0;

    virtual unsigned queryProtectedCount(const CDfsLogicalFileName &logicalname,
                                  const char *callerid=NULL) = 0;                   // if NULL  then sums all

    virtual bool getProtectedInfo (const CDfsLogicalFileName &logicalname, 
                                           StringArray &names, UnsignedArray &counts) = 0;

    virtual IDFProtectedIterator *lookupProtectedFiles(const char *owner=NULL,bool notsuper=false,bool superonly=false)=0; // if owner = NULL then all

    virtual unsigned setDefaultTimeout(unsigned timems) = 0;                                // sets default timeout for SDS connections and locking
                                                                                            // returns previous value
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

interface INamedGroupStore: implements IGroupResolver
{
    
    virtual IGroup *lookup(const char *logicalgroupname) = 0;
    virtual INamedGroupIterator *getIterator() = 0;
    virtual INamedGroupIterator *getIterator(IGroup *match,bool exact=false) = 0;
    virtual void add(const char *logicalgroupname,IGroup *group,bool cluster=false, const char *dir=NULL) = 0;
    virtual void remove(const char *logicalgroupname) = 0;
    virtual bool find(IGroup *grp, StringBuffer &lname, bool add=false) = 0;
    virtual void addUnique(IGroup *group,StringBuffer &lname,const char *dir=NULL) = 0;
    virtual void swapNode(IpAddress &from, IpAddress &to) = 0;
    virtual IGroup *getRemoteGroup(const INode *foreigndali, const char *gname, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT, StringBuffer *dir=NULL) = 0;
    virtual IGroup *lookup(const char *logicalgroupname, StringBuffer &dir) = 0;
    virtual unsigned setDefaultTimeout(unsigned timems) = 0;                                    // sets default timeout for SDS connections and locking                                                                                         // returns previous value

};

extern da_decl INamedGroupStore  &queryNamedGroupStore();

extern da_decl bool decodeChildGroupName(const char *gname,StringBuffer &parentname, StringBuffer &range);


// ==MISC========================================================================================================

// Exceptions

interface IDFS_Exception: extends IException
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
    DFSERR_LookupConnectionTimout       // only raised if timeout specified on lookup etc.
};


// creation routines
inline IDistributedFile *createDistributedFile(const char *logicalname,IUserDescriptor *user,bool writeaccess,IDistributedFileTransaction *transaction) { return queryDistributedFileDirectory().lookup(logicalname,user,writeaccess,transaction); }

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

extern da_decl bool removePhysicalFiles(IGroup *grp,const char *_filemask,unsigned short port, ClusterPartDiskMapSpec &mspec,IMultiException *mexcept);
// for removing orphaned files

// for server use
interface IDaliServer;
extern da_decl IDaliServer *createDaliDFSServer(); // called for coven members

// to initialize clustergroups after clusters change in the environment
extern da_decl void initClusterGroups(unsigned timems=INFINITE);

extern da_decl IDistributedFileTransaction *createDistributedFileTransaction(IUserDescriptor *user=NULL);

extern da_decl const char *normalizeLFN(const char *s, StringBuffer &normalized);

extern da_decl IDFAttributesIterator *createSubFileFilter(IDFAttributesIterator *_iter,IUserDescriptor* _user, bool includesub, unsigned timems=INFINITE); // takes ownership of iter

#define DFS_REPLICATE_QUEUE "dfs_replicate_queue"
#define DRQ_STOP 0
#define DRQ_REPLICATE 1 


// Useful property query functions 

inline bool isFileKey(IPropertyTree &pt) { const char *kind = pt.queryProp("@kind"); return kind&&strieq(kind,"key"); }
inline bool isFileKey(IDistributedFile *f) { return isFileKey(f->queryProperties()); }
inline bool isFileKey(IFileDescriptor *f) { return isFileKey(f->queryProperties()); }

inline bool isPartTLK(IPropertyTree &pt) { const char *kind = pt.queryProp("@kind"); return kind&&strieq(kind,"topLevelKey"); }
inline bool isPartTLK(IDistributedFilePart *p) { return isPartTLK(p->queryProperties()); }
inline bool isPartTLK(IPartDescriptor *p) { return isPartTLK(p->queryProperties()); }

extern da_decl void ensureFileScope(const CDfsLogicalFileName &dlfn, unsigned timeoutms=INFINITE);


#endif


