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

#ifndef DFUWU_INCL
#define DFUWU_INCL

#ifndef dfuwu_decl
#define dfuwu_decl __declspec(dllimport)
#endif

#include "jtime.hpp"

interface IPropertyTree;
interface IRemoteConnection;
interface IDFUWorkUnit;
interface IFileDescriptor;
interface IDfuFileCopier;
interface IGroup;
class RemoteFilename;
class RemoteMultiFilename;
class SocketEndpoint;
struct ClusterPartDiskMapSpec;

enum DFUstate
{
    DFUstate_unknown,
    DFUstate_scheduled,
    DFUstate_queued,
    DFUstate_started,
    DFUstate_aborted,
    DFUstate_failed,
    DFUstate_finished,
    DFUstate_monitoring,
    DFUstate_aborting
};

enum DFUcmd
{
    DFUcmd_none, 
    DFUcmd_copy, 
    DFUcmd_remove, 
    DFUcmd_move, 
    DFUcmd_rename, 
    DFUcmd_replicate, 
    DFUcmd_import, 
    DFUcmd_export, 
    DFUcmd_add, 
    DFUcmd_transfer, 
    DFUcmd_savemap,             // Cmd line only currently
    DFUcmd_addgroup,            // Cmd line only currently
    DFUcmd_server,              // Cmd line only
    DFUcmd_monitor,
    DFUcmd_copymerge,
    DFUcmd_supercopy
};

enum DFUreplicateMode 
{
    DFURMprimary,
    DFURMsecondary,
    DFURMmissing
};

enum DFUservermode              // DFU server use only
{
    DFUservermode_none,
    DFUservermode_run,
    DFUservermode_stop,
    DFUservermode_cycle
};

enum DFUfileformat
{
    DFUff_fixed,
    DFUff_csv,
    DFUff_utf8,
    DFUff_utf8n,
    DFUff_utf16,
    DFUff_utf16le,
    DFUff_utf16be,
    DFUff_utf32,
    DFUff_utf32le,
    DFUff_utf32be,
    DFUff_variable,
    DFUff_recfmvb,
    DFUff_recfmv,
    DFUff_variablebigendian

};

#define RECFMVB_RECSIZE_ESCAPE (-1)
#define RECFMV_RECSIZE_ESCAPE (-2)
#define PREFIX_VARIABLE_RECSIZE_ESCAPE (-3)
#define PREFIX_VARIABLE_BIGENDIAN_RECSIZE_ESCAPE (-4)

enum DFUsortfield
{
    DFUsf_user = 1,
    DFUsf_cluster,
    DFUsf_state,
    DFUsf_command,
    DFUsf_job,
    DFUsf_wuid,             // if filter low of range
    DFUsf_pcdone,
    DFUsf_wuidhigh,         // only for filter high of range
    DFUsf_protected,
    DFUsf_wildwuid,
    DFUsf_term = 0,
    DFUsf_reverse = 0x100,  // for sort
    DFUsf_nocase  = 0x200,  // sort and filter
    DFUsf_numeric = 0x400,  // sort
    DFUsf_wild = 0x800      // filter
};

enum DFUclusterPartDiskMapping // legacy - should use ClusterPartDiskMapSpec instead
{
    DFUcpdm_c_replicated_by_d = 0,   // default 
    DFUcpdm_c_only,                 
    DFUcpdm_d_only,
    DFUcpdm_c_then_d
};


interface IConstDFUoptions : extends IInterface
{
    virtual bool getNoSplit() const = 0;
    virtual bool getReplicate() const = 0;
    virtual bool getRecover() const = 0;
    virtual __int64 getRecover_ID() const = 0;
    virtual unsigned getmaxConnections() const = 0;
    virtual bool getCrc() const = 0;
    virtual unsigned getRetry() const = 0;                              // not yet supported
    virtual bool getPush() const = 0;
    virtual bool getPull() const = 0;
    virtual unsigned getThrottle() const = 0;
    virtual size32_t getTransferBufferSize() const = 0;
    virtual bool getVerify() const = 0;
    virtual bool getOverwrite() const = 0;
    virtual DFUreplicateMode getReplicateMode(StringBuffer &cluster, bool &repeatlast,bool &onlyrepeated) const = 0;
    virtual const char *queryPartFilter() const = 0;
    virtual bool getKeepHeader() const = 0;
    virtual const char * queryFooter() const = 0;
    virtual const char * queryHeader() const = 0;
    virtual const char * queryGlue() const = 0;
    virtual const char * queryLengthPrefix() const = 0;
    virtual const char * querySplitPrefix() const = 0;
    virtual bool getCrcCheck() const = 0;
    virtual bool getNoRecover() const = 0;
    virtual bool getIfNewer() const = 0;    
    virtual bool getIfModified() const = 0;                             // for 'smart' super copy
    virtual bool getSlavePathOverride(StringBuffer &path) const = 0;
    virtual bool getSuppressNonKeyRepeats() const = 0;
    virtual bool getSubfileCopy() const = 0;                                // i.e. called by supercopy
    virtual bool getEncDec(StringAttr &enc,StringAttr &dec) = 0;
    virtual IPropertyTree *queryTree() const = 0;                   // used by DFU server
    virtual bool getFailIfNoSourceFile() const = 0;
    virtual bool getRecordStructurePresent() const = 0;
    virtual bool getQuotedTerminator() const = 0;
    virtual bool getPreserveCompression() const = 0;
    virtual StringBuffer &getUMask(StringBuffer &str)const =0;
};

interface IDFUoptions : extends IConstDFUoptions
{
    virtual void setNoDelete(bool val=true) = 0;
    virtual void setNoSplit(bool val=true) = 0;
    virtual void setReplicate(bool val=true) = 0;
    virtual void setRecover(bool val=true) = 0;
    virtual void setRecover_ID(__int64 val) = 0;
    virtual void setmaxConnections(unsigned val) = 0;
    virtual void setCrc(bool val=true) = 0;
    virtual void setRetry(unsigned val) = 0;                        // not yet supported
    virtual void setPush(bool val=true) = 0;
    virtual void setPull(bool val=true) = 0;
    virtual void setThrottle(unsigned val) = 0;
    virtual void setTransferBufferSize(size32_t val) = 0;
    virtual void setVerify(bool val=true) = 0;
    virtual void setOverwrite(bool val=true) = 0;
    virtual void setReplicateMode(DFUreplicateMode val,const char *cluster=NULL,bool repeatlast=false,bool onlyrepeated=false) = 0;
    virtual void setPartFilter(const char *filter) = 0;             // format n,n-n,n etc
    virtual void setKeepHeader(bool val=true) = 0;
    virtual void setHeader(const char *str) = 0;
    virtual void setGlue(const char *str) = 0;
    virtual void setFooter(const char *str) = 0;
    virtual void setLengthPrefix(const char *str) = 0;
    virtual void setSplitPrefix(const char *str) = 0;
    virtual void setCrcCheck(bool val=true) = 0;
    virtual void setNoRecover(bool val=true) = 0;
    virtual void setIfNewer(bool val=true) = 0;         
    virtual void setIfModified(bool val=true) = 0;                  // for 'smart' super copy
    virtual void setSlavePathOverride(const char *path) = 0;
    virtual void setSuppressNonKeyRepeats(bool val=true) = 0;
    virtual void setSubfileCopy(bool val=true) = 0;                             // i.e. called by supercopy
    virtual void setEncDec(const char *enc,const char *dec) = 0;
    virtual void setFailIfNoSourceFile(bool val=false) = 0;
    virtual void setRecordStructurePresent(bool val=false) = 0;
    virtual void setQuotedTerminator(bool val=true) = 0;
    virtual void setPreserveCompression(bool val=true) = 0;
    virtual void setUMask(const char *val) = 0;
};

interface IConstDFUfileSpec: extends IInterface
{
    virtual IFileDescriptor *getFileDescriptor(bool iskey=false,bool ignorerepeats=false) const = 0;
    virtual StringBuffer &getTitle(StringBuffer &str) const = 0;
    virtual StringBuffer &getLogicalName(StringBuffer &str)const  =0;
    virtual StringBuffer &getDirectory(StringBuffer &str) const = 0;
    virtual StringBuffer &getFileMask(StringBuffer &str) const = 0;
    virtual unsigned getNumParts(unsigned clustnum,bool iskey=false) const = 0;
    virtual StringBuffer &getGroupName(unsigned cluster,StringBuffer &str) const = 0;       // i.e. cluster name if known
    virtual IPropertyTree *queryProperties() const = 0;
    virtual size32_t getRecordSize() const = 0;
    virtual size32_t getMaxRecordSize() const = 0;  
    virtual DFUfileformat getFormat() const = 0; 
    virtual StringBuffer &getPartUrl(unsigned clustnum,unsigned partidx, StringBuffer &url,bool iskey=false) const = 0; // idx 0 based
    virtual RemoteFilename &getPartFilename(unsigned clustnum,unsigned partidx, RemoteFilename &rfn, bool iskey=false) const = 0; // idx 0 based
    virtual IPropertyTree *queryPartProperties(unsigned partidx) const = 0;
    virtual void getCsvOptions(StringBuffer &separate,StringBuffer &terminate,StringBuffer &quote,StringBuffer &escape,bool &quotedTerminator) const = 0;
    virtual StringBuffer &getRowTag(StringBuffer &str)const =0;
    virtual void setForeignDali(const SocketEndpoint &ep)=0; // only used for source of copy (for inter-dali copy)
    virtual bool getForeignDali(SocketEndpoint &ep) const =0;
    virtual void setForeignUser(const char *user,const char *password)=0; 
    virtual bool getForeignUser(StringBuffer &user,StringBuffer &password) const =0; // only if foreign dali set
    virtual bool isCompressed() const = 0;
    virtual bool getWrap() const = 0;
    virtual StringBuffer &getDiffKey(StringBuffer &keyname) const = 0;
    virtual bool getMultiCopy() const = 0;
    virtual void setNumPartsOverride(unsigned num)=0; // internal use for wrap
    virtual unsigned numClusters() const = 0;                 // minimum 1
    virtual bool getClusterPartDiskMapSpec(const char* clustername,ClusterPartDiskMapSpec &spec) const = 0;
    virtual StringBuffer &getClusterPartDefaultBaseDir(const char *clustername,StringBuffer &basedir) const = 0; // set by setClusterPartDiskMapping
    virtual IGroup *getGroup(unsigned clustnum) const = 0;
    virtual void setWindowsOS(bool iswin) =0;
    virtual bool getWindowsOS(bool &iswin) const =0;
    virtual StringBuffer &getRoxiePrefix(StringBuffer &str) const = 0;   // extra prefix to add to roxie file name
    virtual StringBuffer &getRawDirectory(StringBuffer &str) const = 0;
    virtual StringBuffer &getRawFileMask(StringBuffer &str) const = 0;
    virtual bool getRemoteGroupOverride() const = 0;
};

interface IDFUfileSpec: extends IConstDFUfileSpec
{
// only subsets of these are used for setup depending on src/dest type
// see testDFUWU in dfuwu.cpp for examples
    virtual void setLogicalName(const char *val) = 0;
    virtual void setDirectory(const char *val)  = 0;                
    virtual void setFileMask(const char *val) = 0;                  
    virtual void setGroupName(const char *val) = 0;                 // i.e. cluster name 
    virtual void setFromFileDescriptor(IFileDescriptor &fd) = 0;    // alter 
    virtual void setSingleFilename(RemoteFilename &rfn) = 0;
    virtual void setMultiFilename(RemoteMultiFilename &rmfn) = 0;
    virtual void setTitle(const char *val) = 0; //used for error messages etc (defaults to logical name)
    virtual void setRecordSize(size32_t size) = 0; // may need to be supplied for non 1-1 splits
    virtual void setMaxRecordSize(size32_t size) = 0; 
    virtual void setFormat(DFUfileformat format) = 0; 
    virtual void setCsvOptions(const char *separate=NULL,const char *terminate=NULL,const char *quote=NULL,const char *escape=NULL,bool quotedTerminator=true) = 0;  // NULL for default
    virtual void setRowTag(const char *str) = 0;
    virtual void setFromXML(const char *xml) = 0;
    virtual void setCompressed(bool set) = 0;
    virtual void setWrap(bool val) = 0;
    virtual void setReplicateOffset(int val) = 0;           // sets for all clusters
    virtual void setDiffKey(const char *keyname) = 0;
    virtual void setMultiCopy(bool val) = 0;
    virtual void setClusterPartDiskMapSpec(const char* clustername,ClusterPartDiskMapSpec &spec) = 0; 
    virtual void setClusterPartDefaultBaseDir(const char* clustername,const char *basedir) = 0; 
    virtual void setClusterPartDiskMapping(DFUclusterPartDiskMapping val,const char *basedir, const char *clustername, bool repeatlast=false, bool onlyrepeated=false) = 0; 
    virtual IPropertyTree *setProperties(IPropertyTree *val) = 0;   // takes ownershop of val
    virtual IPropertyTree *queryUpdateProperties() = 0;
    virtual IPropertyTree *queryUpdatePartProperties(unsigned partidx) = 0;
    virtual void setRoxiePrefix(const char *val) = 0;   // extra prefix to add to file name
    virtual void setRemoteGroupOverride(bool set) = 0;
};


interface IConstDFUprogress: extends IInterface
{
    virtual unsigned getPercentDone() const = 0;
    virtual unsigned getSecsLeft() const = 0;
    virtual StringBuffer &getTimeLeft(StringBuffer &str) const = 0;
    virtual unsigned __int64 getScaledDone() const = 0;
    virtual unsigned __int64 getScaledTotal() const = 0;
    virtual StringBuffer &getScale(StringBuffer &str) const = 0;
    virtual unsigned getKbPerSecAve() const = 0;
    virtual unsigned getKbPerSec() const = 0;
    virtual unsigned getSlavesDone() const = 0;
    virtual StringBuffer &getTimeTaken(StringBuffer &str) const = 0;
    virtual StringBuffer &formatProgressMessage(StringBuffer &str) const = 0; // default progress message (maybe add options)
    virtual StringBuffer &formatSummaryMessage(StringBuffer &str) const = 0;  // default summary message (maybe add options)
    virtual DFUstate getState() const = 0;
    virtual CDateTime &getTimeStarted(CDateTime &val) const = 0;
    virtual CDateTime &getTimeStopped(CDateTime &val) const = 0;
    virtual unsigned getTotalNodes() const = 0;
    virtual StringBuffer &getSubInProgress(StringBuffer &str) const = 0;    // sub-DFUWUs in progress
    virtual StringBuffer &getSubDone(StringBuffer &str) const = 0;          // sub-DFUWUs done (list)
};

interface IDFUprogress: extends IConstDFUprogress
{
    virtual void setProgress(unsigned percentDone, unsigned secsLeft, const char * timeLeft,
                             unsigned __int64 scaledDone, unsigned __int64 scaledTotal, const char * scale,
                             unsigned kbPerSecondAve, unsigned kbPerSecondRate,
                             unsigned slavesDone, bool replicating)=0;
    virtual void setDone(const char * timeTaken, unsigned kbPerSecond, bool set100pc) = 0;
    virtual void setState(DFUstate state) = 0;
    virtual void setTotalNodes(unsigned val) = 0;
    virtual void setPercentDone(unsigned pc)=0;
    virtual void setSubInProgress(const char *str) = 0;     // set sub-DFUWUs in progress
    virtual void setSubDone(const char *str) = 0;           // set sub-DFUWUs done
    virtual void clearProgress() = 0;
};

interface IDFUprogressSubscriber: extends IInterface
{
    virtual void notify(IConstDFUprogress *progress) = 0;
};

interface IDFUabortSubscriber: extends IInterface
{
    virtual void notifyAbort() = 0;
};


interface IConstDFUmonitor: extends IInterface
{
    virtual bool getHandlerEp(SocketEndpoint &ep) const = 0; // monitors only
    virtual unsigned getCycleCount() const = 0;
    virtual unsigned getShotCount() const = 0;
    virtual StringBuffer &getEventName(StringBuffer &str) const  =0;
    virtual bool getSub() const = 0;
    virtual unsigned getShotLimit() const = 0;
    virtual unsigned getTriggeredList(StringAttrArray &files) const = 0; // internal
};

interface IDFUmonitor: extends IConstDFUmonitor
{
    virtual void setHandlerEp(const SocketEndpoint &ep) = 0; // monitors only
    virtual void setCycleCount(unsigned val) = 0;
    virtual void setShotCount(unsigned val) = 0;
    virtual void setEventName(const char *val) = 0;
    virtual void setSub(bool sub) = 0;
    virtual void setShotLimit(unsigned limit) = 0;
    virtual void setTriggeredList(const StringAttrArray &files) = 0; // internal
};



typedef IIteratorOf<IException> IExceptionIterator;

interface IConstDFUWorkUnit : extends IInterface
{
    virtual const char *queryId() const = 0;
    virtual StringBuffer &getClusterName(StringBuffer &str) const = 0;
    virtual StringBuffer &getDFUServerName(StringBuffer &str) const = 0;
    virtual StringBuffer &getJobName(StringBuffer &str) const = 0;
    virtual StringBuffer &getQueue(StringBuffer &str) const = 0;
    virtual StringBuffer &getUser(StringBuffer &str) const = 0;
    virtual StringBuffer &getPassword(StringBuffer &str) const =0;
    virtual bool isProtected() const = 0;
    virtual IDFUWorkUnit *openUpdate(bool exclusive) = 0;       // this converts (note cannot be converted back so const interface should be released after)
    virtual void requestAbort() = 0;
    virtual DFUcmd getCommand() const = 0;
    virtual StringBuffer &getCommandName(StringBuffer &str) const = 0;
    virtual IConstDFUoptions *queryOptions() const = 0;
    virtual IConstDFUfileSpec *querySource() const = 0;
    virtual IConstDFUfileSpec *queryDestination() const = 0;
    virtual IConstDFUprogress *queryProgress(bool reload=true) = 0;
    virtual IConstDFUmonitor *queryMonitor(bool reload=true) = 0;
    virtual void subscribeProgress(IDFUprogressSubscriber *) = 0;
    virtual void subscribeAbort(IDFUabortSubscriber *) = 0;
    virtual DFUstate waitForCompletion(int timeout = -1) = 0;
    virtual IExceptionIterator *getExceptionIterator() = 0;
    virtual CDateTime &getTimeScheduled(CDateTime &val) const = 0;
    virtual StringBuffer &getApplicationValue(const char * application, const char * propname, StringBuffer & str) const = 0;
    virtual int getApplicationValueInt(const char * application, const char * propname, int defVal) const = 0;
    virtual StringBuffer &getDebugValue(const char *propname, StringBuffer &str) const = 0;
    virtual StringBuffer &toXML(StringBuffer &buf) = 0;
};


interface IDFUWorkUnit : extends IConstDFUWorkUnit
{
    virtual unsigned commit() = 0;                          // returns edition
    virtual void  rollback() = 0;                           // cancel changes
    virtual void protect(bool protectMode) = 0;
    virtual void setClusterName(const char * value) = 0;
    virtual void setDFUServerName(const char * value) = 0;
    virtual void setJobName(const char * value) = 0;
    virtual void setQueue(const char * value) = 0;
    virtual void setUser(const char * value) = 0;
    virtual void setPassword(const char * value) = 0;
    virtual void setCommand(DFUcmd cmd) = 0;
    virtual IDFUoptions *queryUpdateOptions() = 0;
    virtual IDFUfileSpec *queryUpdateSource() = 0;
    virtual IDFUfileSpec *queryUpdateDestination() = 0;
    virtual void addOptions(IPropertyTree *tree) = 0;       // used by DFU command line (for moment)
    virtual IDFUprogress *queryUpdateProgress() = 0;
    virtual IDFUmonitor *queryUpdateMonitor() = 0;
    virtual void closeUpdate() = 0;                     // called before WU obtained by openUpdate is released
    virtual void queryRecoveryStore(IRemoteConnection *& conn,IPropertyTree *&tree,StringBuffer &runningpath) = 0; // not nice - needed by daft 
    virtual void removeRecoveryStore() = 0;
    virtual void addException(IException *e)=0;
    virtual void setTimeScheduled(const CDateTime &val) = 0;
    virtual unsigned getEdition(bool local) = 0;            
    virtual void setApplicationValue(const char * application, const char * propname, const char * value, bool overwrite) = 0;
    virtual void setApplicationValueInt(const char * application, const char * propname, int value, bool overwrite) = 0;
    virtual void setDebugValue(const char *propname, const char *value, bool overwrite) = 0;
    virtual bool removeQueue() = 0;
    virtual void clearExceptions() = 0;
};


interface IConstDFUWorkUnitIterator : extends IInterface
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual StringBuffer &getId(StringBuffer &str) = 0;
    virtual IConstDFUWorkUnit * get() = 0;
};


interface IDFUWorkUnitFactory : extends IInterface
{
    virtual IDFUWorkUnit * createWorkUnit() = 0;    // opened in exclusive
    virtual bool deleteWorkUnit(const char * wuid) = 0;
    virtual IConstDFUWorkUnit * openWorkUnit(const char * wuid, bool lock) = 0;
    virtual IConstDFUWorkUnitIterator * getWorkUnitsByXPath(const char *xpath) = 0;
    virtual IConstDFUWorkUnitIterator * getWorkUnitsByOwner(const char * owner) = 0; 
    virtual IConstDFUWorkUnitIterator * getWorkUnitsByState(DFUstate state) = 0;
    // more get functions (e.g. by date) could be added
    virtual IDFUWorkUnit * updateWorkUnit(const char * wuid,bool exclusive=false) = 0;
    virtual IConstDFUWorkUnitIterator* getWorkUnitsSorted( DFUsortfield *sortorder, // list of fields to sort by (terminated by WUSFterm)
                                                        DFUsortfield * filters,     // list of fields to filter on (terminated by WUSFterm)
                                                        const void *filterbuf,      // list of 0 termintated strings for filter values
                                                        unsigned startoffset,
                                                        unsigned maxnum,
                                                        const char *queryowner, 
                                                        __int64 *cachehint,         // set to NULL if caching not required
                                                        unsigned *total) = 0;       // set to NULL if caching not required
    virtual unsigned numWorkUnits()=0;
    virtual __int64  subscribe(const char *xpath,void *iface) =0;       // internal use
};


extern dfuwu_decl IDFUWorkUnitFactory * getDFUWorkUnitFactory();
extern dfuwu_decl void submitDFUWorkUnit(IDFUWorkUnit *wu);
extern dfuwu_decl void submitDFUWorkUnit(const char *wuid);

extern dfuwu_decl DFUcmd decodeDFUcommand(const char * str);
extern dfuwu_decl StringBuffer &encodeDFUcommand(DFUcmd cmd,StringBuffer &str);
extern dfuwu_decl DFUstate decodeDFUstate(const char * str);
extern dfuwu_decl StringBuffer &encodeDFUstate(DFUstate state,StringBuffer &str);

class CDFUfileformat
{
#define DFF_MAP \
    DFF_MAP1(DFUff_csv,                 "csv") \
    DFF_MAP1(DFUff_csv,                 "ascii") \
    DFF_MAP1(DFUff_utf8,                "utf8") \
    DFF_MAP1(DFUff_utf8n,               "utf8n") \
    DFF_MAP1(DFUff_utf16,               "utf16") \
    DFF_MAP1(DFUff_utf16le,             "utf16le") \
    DFF_MAP1(DFUff_utf16be,             "utf16be") \
    DFF_MAP1(DFUff_utf32,               "utf32") \
    DFF_MAP1(DFUff_utf32le,             "utf32le") \
    DFF_MAP1(DFUff_utf32be,             "utf32be") \
    DFF_MAP1(DFUff_variable,            "variable") \
    DFF_MAP1(DFUff_recfmvb,             "recfmvb") \
    DFF_MAP1(DFUff_recfmv,              "recfmv") \
    DFF_MAP1(DFUff_variablebigendian,   "variablebigendian") \
    DFF_MAP1(DFUff_fixed,               "fixed") 
public:
    static inline DFUfileformat decode(const char * str)
    {
        if (str) {
#define DFF_MAP1(v,s) if (stricmp(str,s)==0) return v;
          DFF_MAP;
#undef DFF_MAP1
        }
        return DFUff_fixed;
    }

    static inline StringBuffer &encode(DFUfileformat fmt,StringBuffer &str)
    {
#define DFF_MAP1(v,s) if (v==fmt) return str.append(s);
        DFF_MAP;
#undef DFF_MAP1
        return str.append("fixed");
    }
#undef DFF_MAP
};

extern dfuwu_decl unsigned queuedJobs(const char *queuename,StringAttrArray &wulist); // returns number of *running* jobs
                                                                                      // (running jobs at head of list)

extern dfuwu_decl IDfuFileCopier *createRemoteFileCopier(const char *qname,const char *clustername, const char *jobname, bool replicate);


#endif 

