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

#ifndef DAFDESC_HPP
#define DAFDESC_HPP

#ifdef DALI_EXPORTS
#define da_decl DECL_EXPORT
#else
#define da_decl DECL_IMPORT
#endif

#include <vector>
#include <string>

#include "jiface.hpp"
#include "mpbase.hpp"
class RemoteFilename;
class RemoteFilenameArray;
interface IGroup;
interface IPropertyTree;
interface IFileDescriptor;
interface IClusterInfo;
interface IReplicatedFile;
interface INamedGroupStore;

#define SUPPORTS_MULTI_CLUSTERS  // always now set

#define MAX_REPLICATION_LEVELS 4

#define DEFAULTXMLROWTAG "Row"
#define DEFAULTXMLHEADER "<Dataset>"
#define DEFAULTXMLFOOTER "</Dataset>"

enum DFD_OS
{
    DFD_OSdefault,
    DFD_OSwindows,
    DFD_OSunix
};

enum DFD_Replicate
{
    DFD_NoCopies      = 1,
#ifdef _CONTAINERIZED
    // NB: in containerized mode, each plane has only 1 copy.
    DFD_DefaultCopies = 1
#else
    DFD_DefaultCopies = 2
#endif
};

enum GroupType { grp_thor, grp_thorspares, grp_roxie, grp_hthor, grp_dropzone, grp_unknown, __grp_size };

enum class AccessMode : unsigned
{

    none            = 0x00000000,
    read            = 0x00000001,
    write           = 0x00000002,
    sequential      = 0x00000004,
    random          = 0x00000008,           // corresponds to "random" reason in alias reasons
    noMount         = 0x01000000,           // corresponds to "api" reason in alias reasons

    readRandom      = read | random,
    readSequential  = read | sequential,
    readNoMount     = read | noMount,
    writeSequential = write | sequential,

    readMeta        = read,                  // read access - may not actually read the contents
    writeMeta       = write,                 // write access - may also be used for delete

//The following are used for mechanical replacement of writeattr to update the function prototypes but not change
//the behaviour but allow all the calls to be revisited later to ensure the correct parameter is used.

    tbdRead          = read,                 // writeattr was false
    tbdWrite         = write,                // writeattr was true
};
BITMASK_ENUM(AccessMode);
inline bool isWrite(AccessMode mode) { return (mode & AccessMode::write) != AccessMode::none; }

extern da_decl AccessMode getAccessModeFromString(const char *access); // single access mode


// ==CLUSTER PART MAPPING ==============================================================================================

// repeatedPart flags
#define CPDMSRP_lastRepeated     (0x20000000) // repeat last part
#define CPDMSRP_onlyRepeated     (0x40000000) // or with repeatedPart for only repeated parts
#define CPDMSRP_allRepeated      (0x80000000) // or with repeatedPart for all parts repeated
#define CPDMSRP_notRepeated      ((unsigned)-1) // value of no repeated parts (i.e. only normal replication)
#define CPDMSRP_partMask         (0xffffff)

struct da_decl ClusterPartDiskMapSpec
{
    ClusterPartDiskMapSpec() { replicateOffset=1; defaultCopies=DFD_DefaultCopies; startDrv=0; maxDrvs=2; flags=0; interleave=0; repeatedPart=CPDMSRP_notRepeated; }
    int replicateOffset;    // offset to add to determine node
public:
    byte defaultCopies;     // default number of copies (i.e. redundancy+1)
    byte maxDrvs;           // normally 2 (c$, d$)
    byte startDrv;          // normally 0 (c$)
    byte flags;             // CPDMSF_*
    unsigned interleave;    // for superfiles (1..)
    unsigned repeatedPart;  // if part is repeated on every node (NB maxCopies does not include)
                            // ( | CPDMSRP_onlyRepeated for *only* repeats )
    StringAttr defaultBaseDir; // if set overrides *base* directory (i.e. /c$/dir/x/y becomes odir/x/y)
    StringAttr defaultReplicateDir;
    unsigned numStripedDevices = 1;

    void setRoxie (unsigned redundancy, unsigned channelsPerNode, int replicateOffset=1);
    void setRepeatedCopies(unsigned partnum,bool onlyrepeats);
    void setDefaultBaseDir(const char *dir);
    void setDefaultReplicateDir(const char *dir);
    void ensureReplicate();
    bool calcPartLocation (unsigned part, unsigned maxparts, unsigned copy, unsigned clusterWidth, unsigned &node, unsigned &drv);
    void toProp(IPropertyTree *tree);
    void fromProp(IPropertyTree *tree);
    void serialize(MemoryBuffer &mb);
    void deserialize(MemoryBuffer &mb);
    unsigned numCopies(unsigned part,unsigned clusterwidth,unsigned filewidth);

    ClusterPartDiskMapSpec & operator=(const ClusterPartDiskMapSpec &other);

    bool isReplicated() const;
};

#define CPDMSF_wrapToNextDrv       (0x01) // whether should wrap to next drv
#define CPDMSF_fillWidth           (0x02) // replicate copies fill cluster serially (when num parts < clusterwidth/2)
#define CPDMSF_packParts           (0x04) // whether to save parts as binary
#define CPDMSF_repeatedPart        (0x08) // if repeated parts included
#define CPDMSF_defaultBaseDir      (0x10) // set if defaultBaseDir present
#define CPDMSF_defaultReplicateDir (0x20) // set if defaultBaseDir present
#define CPDMSF_overloadedConfig    (0x40) // set if overloaded mode
#define CPDMSF_striped             (0x80) // set if parts striped over multiple devices


// ==PART DESCRIPTOR ==============================================================================================


interface IPartDescriptor: extends IInterface
{   // describes the location and contents of a part
    virtual unsigned queryPartIndex() = 0;                                      // part index (0 based)

    virtual unsigned numCopies() = 0;                                           // number of copies
    virtual INode *getNode(unsigned copy=0) = 0;                                // get machine node (link)
    virtual INode *queryNode(unsigned copy=0) = 0;                              // query machine node

    virtual IPropertyTree &queryProperties() = 0;                               // query part properties
    virtual IPropertyTree *getProperties() = 0;                                 // get part properties

    virtual RemoteFilename &getFilename(unsigned copy, RemoteFilename &rfn) = 0; // get filename info
    virtual StringBuffer &getTail(StringBuffer &name) = 0;                          // tail name - may be multi
    virtual StringBuffer &getDirectory(StringBuffer &name,unsigned copy = 0) = 0;   // get filename dir
    virtual StringBuffer &getPath(StringBuffer &name,unsigned copy = 0) = 0;        // full local name - may be multi

    virtual void serialize(MemoryBuffer &tgt) = 0;                              // deserialize with deserializeFilePartDescriptor

    virtual bool isMulti() = 0;                                                 // true if part is a multi-part file
    virtual RemoteMultiFilename &getMultiFilename(unsigned copy, RemoteMultiFilename &rfn) = 0; // get multi-filename info

    virtual bool getCrc(unsigned &crc) = 0;
    virtual IFileDescriptor &queryOwner() = 0;
    virtual const char *queryOverrideName() = 0;                                        // for non-standard files
    virtual unsigned copyClusterNum(unsigned copy,unsigned *replicate=NULL)=0;      // map copy number to cluster (and optionally replicate number)
    virtual IReplicatedFile *getReplicatedFile()=0;

    virtual offset_t getFileSize(bool allowphysical,bool forcephysical)=0; // gets the part filesize (NB this will be the *expanded* size)
    virtual offset_t getDiskSize(bool allowphysical,bool forcephysical)=0; // gets the part size on disk (NB this will be the compressed size)
};
typedef IArrayOf<IPartDescriptor> CPartDescriptorArray;
typedef IIteratorOf<IPartDescriptor> IPartDescriptorIterator;

#define IFDSF_EXCLUDE_PARTS   0x01
#define IFDSF_EXCLUDE_GROUPS  0x02
#define IFDSF_ATTR_ONLY       0x04
#define IFDSF_EXCLUDE_CLUSTERNAMES  0x08
#define IFDSF_FOREIGN_GROUP   0x10

enum class FileDescriptorFlags
{
    none          = 0x00,
    dirperpart    = 0x01,
    foreign       = 0x02
};
BITMASK_ENUM(FileDescriptorFlags);

// ==FILE DESCRIPTOR ==============================================================================================
interface ISuperFileDescriptor;

interface IFileDescriptor: extends IInterface
{   // describes the location and contents of a file

/* to setup a file descriptor call:

    setDefaultDir(localdirpath)
    setPartMask(partmask);
    setNumParts(numparts);
    for each cluster
        addCluster(grp,mapping) (or addCluster(grpname,mapping,resolver)

 or:

   for each cluster
        for each part
            setPart(partnum,file,attr);
        endCluster(mapping);

 or: (single cluster legacy)

    for each part
        setPart(partnum,file,attr);


note it is an error to set different filenames for the same part on different clusters
or to set the same part twice in the same cluster
if endCluster is not called it will assume only one cluster and not replicated

*/

    virtual void setDefaultDir(const char *dir) = 0;                            // set default directory
    virtual void setPartMask(const char *mask) = 0;
    virtual void setNumParts(unsigned numparts) = 0;
    virtual unsigned addCluster(IGroup *grp,const ClusterPartDiskMapSpec &map) = 0;
    virtual unsigned addCluster(const char *grpname,IGroup *grp,const ClusterPartDiskMapSpec &map) = 0; // alternative if name known

    virtual void setPart(unsigned idx, const RemoteFilename &name, IPropertyTree *pt=NULL) = 0; // using RemoteFilename
    virtual void setPart(unsigned idx, INode *node, const char *filename, IPropertyTree *pt=NULL) = 0;
    virtual void endCluster(ClusterPartDiskMapSpec &map)=0;

    virtual void setTraceName(const char *trc, bool normalize=true) = 0;        // name used for progress reports, errors etc

    virtual unsigned numParts() = 0;                                            // number of separate parts
    virtual unsigned numCopies(unsigned partidx) = 0;                           // number of copies
    virtual INode *getNode(unsigned partidx, unsigned copy=0) = 0;              // get machine node (link)
    virtual INode *queryNode(unsigned partidx, unsigned copy=0) = 0;            // query machine node

    virtual StringBuffer &getTraceName(StringBuffer &dir) = 0;                  // get trace name

    virtual IPropertyTree &queryProperties() = 0;                               // query file properties
    virtual IPropertyTree *getProperties() = 0;                                 // get file properties

    virtual RemoteFilename &getFilename(unsigned partidx,unsigned copy, RemoteFilename &rfn) = 0; // get filename info
    virtual RemoteMultiFilename &getMultiFilename(unsigned partidx,unsigned copy, RemoteMultiFilename &rfn) = 0; // get multi-filename info

    virtual bool isMulti(unsigned partidx=(unsigned)-1) = 0;                    // true if part is a multi-part file
                                                                                // if partidx -1 then true if any part multi
    virtual const char *queryPartMask() = 0;                                    // part mask if available (or NULL)
    virtual const char *queryDefaultDir() = 0;                                  // default dir if avail  (or NULL)

    virtual void serialize(MemoryBuffer &tgt) = 0;                              // use to serialize distributed file
                                                                                // deserialize with deserializeFileDescriptor

    virtual IPartDescriptor *queryPart(unsigned idx) = 0;
    virtual IPartDescriptor *getPart(unsigned idx) = 0;
    virtual IPartDescriptorIterator *getIterator() = 0;

    virtual bool isGrouped() = 0;                                               // misc properties
    virtual const char *queryKind() = 0;
    virtual bool isCompressed(bool *blocked=NULL) = 0;

    virtual IGroup *getGroup() = 0;                                             // will be for first cluster

    virtual unsigned numClusters() = 0;
    virtual IClusterInfo *queryCluster(const char *clusterName) = 0;
    virtual IClusterInfo *queryClusterNum(unsigned idx) = 0;
    virtual ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum) = 0;
    virtual IGroup *queryClusterGroup(unsigned clusternum) = 0;                     // returns group for cluster if known
    virtual void setClusterGroup(unsigned clusternum,IGroup *grp) = 0;              // sets group for cluster
    virtual StringBuffer &getClusterGroupName(unsigned clusternum,StringBuffer &ret,IGroupResolver *resolver=NULL) = 0;                 // returns group name of cluster (if set)
    virtual void setClusterGroupName(unsigned clusternum,const char *name) = 0;     // sets group name of cluster (if set)
    virtual void setClusterOrder(StringArray &names,bool exclusive) = 0;            // if exclusive set then other clusters deleted
    virtual void serializeTree(IPropertyTree &pt,unsigned flags=0) = 0;             // deserialize with deserializeFileDescriptorTree
    virtual IPropertyTree *getFileTree(unsigned flags=0) = 0;                       // flags IFDSF_*

    virtual void serializeParts(MemoryBuffer &mb,unsigned *parts, unsigned nparts) =0;  //for serializing 1 or more parts from a file
    virtual void serializeParts(MemoryBuffer &mb,UnsignedArray &parts) =0;              // alternative of above

    virtual ISuperFileDescriptor *querySuperFileDescriptor() = 0;   // returns NULL if not superfile descriptor (or if superfile contained <=1 subfiles)

    virtual StringBuffer &getClusterLabel(unsigned clusternum,StringBuffer &ret) = 0; // node group name

    virtual void ensureReplicate() = 0;                                             // make sure a file can be replicated

    virtual IPropertyTree *queryHistory() = 0;                                       // query file history records
    virtual void setFlags(FileDescriptorFlags flags) = 0;
    virtual FileDescriptorFlags getFlags() = 0;
};

interface ISuperFileDescriptor: extends IFileDescriptor
{
    // extension of IFileDescriptor for superfiles providing some part->file mapping
    virtual bool mapSubPart(unsigned superpartnum, unsigned &subfile, unsigned &subpartnum)=0;
    virtual void setSubMapping(UnsignedArray &subcounts, bool interleaved)=0;
    virtual unsigned querySubFiles() = 0;
};


// == CLUSTER INFO (currently not exposed outside dali base) =================================================================================

interface IStoragePlane;
interface IClusterInfo: extends IInterface  // used by IFileDescriptor and IDistributedFile
{
    virtual StringBuffer &getGroupName(StringBuffer &name,IGroupResolver *resolver=NULL)=0;
    virtual const char *queryGroupName()=0;     // may be NULL
    virtual IGroup *queryGroup()=0;           // may be NULL
    virtual ClusterPartDiskMapSpec  &queryPartDiskMapping()=0;
    virtual IGroup *queryGroup(IGroupResolver *resolver)=0;
    virtual INode *queryNode(unsigned idx,unsigned maxparts,unsigned copy)=0;
    virtual unsigned queryDrive(unsigned idx,unsigned maxparts,unsigned copy)=0;
    virtual void serializeTree(IPropertyTree * pt,unsigned flags=0)=0;
    virtual void serialize(MemoryBuffer &mb)=0;
    virtual void setGroup(IGroup *grp)=0;
    virtual void setGroupName(const char *name)=0;
    virtual void getBaseDir(StringBuffer &basedir, DFD_OS os)=0;
    virtual void getReplicateDir(StringBuffer &basedir, DFD_OS os)=0;
    virtual StringBuffer &getClusterLabel(StringBuffer &name)=0; // node group name
    virtual void applyPlane(IStoragePlane *plane) = 0;
};

interface IStoragePlaneAlias: extends IInterface
{
    virtual AccessMode queryModes() const = 0;
    virtual const char *queryPrefix() const = 0 ;
    virtual bool isAccessible() const = 0;
};

//I'm not sure if this should be used in place of an IGroup, probably as system gradually changes
interface IStorageApiInfo;
interface IStoragePlane: extends IInterface
{
    virtual const char * queryPrefix() const = 0;
    virtual unsigned numDevices() const = 0;
    virtual const std::vector<std::string> &queryHosts() const = 0;
    virtual unsigned numDefaultSprayParts() const = 0 ;
    virtual bool queryDirPerPart() const = 0;
    virtual IStoragePlaneAlias *getAliasMatch(AccessMode desiredModes) const = 0;
    virtual IStorageApiInfo *getStorageApiInfo() = 0;
    virtual bool isAccessible() const = 0;
};

IClusterInfo *createClusterInfo(const char *grpname,                  // NULL if roxie label set
                                IGroup *grp,
                                const ClusterPartDiskMapSpec &mspec,
                                INamedGroupStore *resolver=NULL,
                                unsigned flags=0
                                );
IClusterInfo *createRoxieClusterInfo(const char *label,
                                const ClusterPartDiskMapSpec &mspec
                                );
IClusterInfo *deserializeClusterInfo(MemoryBuffer &mb,
                                INamedGroupStore *resolver=NULL);
IClusterInfo *deserializeClusterInfo(IPropertyTree *pt,
                                INamedGroupStore *resolver=NULL,
                                unsigned flags=0);

void getClusterInfo(IPropertyTree &pt, INamedGroupStore *resolver, unsigned flags, IArrayOf<IClusterInfo> &clusters);



// ==MISC ==============================================================================================



// default logical to physical filename routines
extern da_decl StringBuffer &makePhysicalPartName(
                                const char *lname,                  // logical name
                                unsigned partno,                    // part number (1..)
                                unsigned partmax,                   // number of parts (1..)
                                StringBuffer &result,               // result filename (or directory name if part 0)
                                unsigned replicateLevel,            // uses replication directory
                                DFD_OS os,                          // os must be specified if no dir specified
                                const char *diroverride,            // override default directory
                                bool dirPerPart,                    // generate a subdirectory per part
                                unsigned stripeNum);                // stripe number
extern da_decl StringBuffer &makeSinglePhysicalPartName(const char *lname, // single part file
                                                        StringBuffer &result,
                                                        bool allowospath,   // allow an OS (absolute) file path
                                                        bool &wasdfs,       // not OS path
                                                        const char *diroverride=NULL
                                                        );
extern da_decl StringBuffer &makePhysicalDirectory(StringBuffer &result, const char *lname, unsigned replicateLevel, DFD_OS os,const char *diroverride);

// 
extern da_decl StringBuffer &getLFNDirectoryUsingBaseDir(StringBuffer &result, const char *lname, const char *baseDir);
extern da_decl StringBuffer &getLFNDirectoryUsingDefaultBaseDir(StringBuffer &result, const char *lname, DFD_OS os);

// set/get defaults
extern da_decl const char *queryBaseDirectory(GroupType groupType, unsigned replicateLevel=0, DFD_OS os=DFD_OSdefault);
extern da_decl void setBaseDirectory(const char * dir, unsigned replicateLevel=0, DFD_OS os=DFD_OSdefault);
extern da_decl const char *queryPartMask();
extern da_decl StringBuffer &getPartMask(StringBuffer &ret,const char *lname=NULL,unsigned partmax=0);
extern da_decl void setPartMask(const char * mask);
extern da_decl bool setReplicateDir(const char *name,StringBuffer &out, bool isrep=true,const char *baseDir=NULL,const char *repDir=NULL); // changes directory of name passed to backup directory

extern da_decl void initializeStoragePlanes(bool createPlanesFromGroups, bool threadSafe);  // threadSafe should be true if no other threads will be accessing the global config
extern da_decl void disableStoragePlanesDaliUpdates();

extern da_decl bool getDefaultStoragePlane(StringBuffer &ret);
extern da_decl bool getDefaultSpillPlane(StringBuffer &ret);
extern da_decl bool getDefaultIndexBuildStoragePlane(StringBuffer &ret);
extern da_decl bool getDefaultPersistPlane(StringBuffer &ret);
extern da_decl bool getDefaultJobTempPlane(StringBuffer &ret);
extern da_decl IStoragePlane * getDataStoragePlane(const char * name, bool required);
extern da_decl IStoragePlane * getRemoteStoragePlane(const char * name, bool required);
extern da_decl IStoragePlane * createStoragePlane(IPropertyTree *meta);

extern da_decl IFileDescriptor *createFileDescriptor();
extern da_decl IFileDescriptor *createFileDescriptor(IPropertyTree *attr);      // ownership of attr tree is taken
extern da_decl IFileDescriptor *createFileDescriptor(const char *lname, const char *clusterType, const char *groupName, IGroup *grp);
extern da_decl IFileDescriptor *createExternalFileDescriptor(const char *logicalname);
extern da_decl IFileDescriptor *getExternalFileDescriptor(const char *logicalname);
extern da_decl ISuperFileDescriptor *createSuperFileDescriptor(IPropertyTree *attr);        // ownership of attr tree is taken
extern da_decl IFileDescriptor *deserializeFileDescriptor(MemoryBuffer &mb);
extern da_decl IFileDescriptor *deserializeFileDescriptorTree(IPropertyTree *tree, INamedGroupStore *resolver=NULL, unsigned flags=0);  // flags IFDSF_*
extern da_decl IPartDescriptor *deserializePartFileDescriptor(MemoryBuffer &mb);
extern da_decl void deserializePartFileDescriptors(MemoryBuffer &mb,IArrayOf<IPartDescriptor> &parts);
extern da_decl IFileDescriptor *createFileDescriptor(const char *lname, const char *planeName, unsigned numParts);

extern da_decl IFileDescriptor *createMultiCopyFileDescriptor(IFileDescriptor *in,unsigned num);

extern da_decl IFileDescriptor *createFileDescriptorFromRoxieXML(IPropertyTree *tree, const char *clustername=NULL);


extern da_decl bool getCrcFromPartProps(IPropertyTree &fileattr,IPropertyTree &props, unsigned &crc);
extern da_decl bool isCompressed(IPropertyTree &fileattr, bool *blocked=NULL);

extern da_decl void removePartFiles(IFileDescriptor *desc,IMultiException *mexcept=NULL);   // remove part files

extern da_decl StringBuffer &setReplicateFilename(StringBuffer &filename,unsigned drvnum,const char *baseDir=NULL,const char *repDir=NULL);

// path separator utility routines (work for both host and non-host OS)

inline char OsSepChar(DFD_OS os)
{
    if (os==DFD_OSdefault)
#ifdef _WIN32
        os = DFD_OSwindows;
#else
        os = DFD_OSunix;
#endif
    if (os==DFD_OSwindows)
        return '\\';
    return '/';
}
inline DFD_OS SepCharBaseOs(char c)
{
    switch (c) {
    case '\\': return DFD_OSwindows;
    case '/': return DFD_OSunix;
    }
    return DFD_OSdefault;
}

extern da_decl void extractFilePartInfo(IPropertyTree &info, IFileDescriptor &file);

extern da_decl unsigned __int64 getPartPlaneAttr(IPartDescriptor &part, unsigned copy, PlaneAttributeType attr, size32_t defaultValue);

#endif
