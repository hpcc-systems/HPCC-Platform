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

#ifndef DAUTILS_HPP
#define DAUTILS_HPP


#include "jiface.hpp"
#include "jstring.hpp"
#include "jsocket.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "mpbase.hpp"
#include "dasess.hpp"

#ifndef da_decl
#define da_decl __declspec(dllimport)
#endif

interface IRemoteConnection;
interface IProperties;

enum DfsXmlBranchKind
{
    DXB_File,
    DXB_SuperFile,
    DXB_Collection,
    DXB_Scope,
    DXB_Internal // intended as branch for engines to place per user transient files
};


class da_decl CMultiDLFN;

/**
 * Maps between file names and its properties, for
 * normal files, super files, logic files, etc.
 */
class da_decl CDfsLogicalFileName
{
    StringAttr lfn;
    unsigned tailpos;
    unsigned localpos; // if 0 not foreign
    StringAttr cluster; 
    CMultiDLFN *multi;   // for temp superfile
    bool external;
    bool allowospath;
    bool allowWild;

public:
    CDfsLogicalFileName();
    ~CDfsLogicalFileName();

    CDfsLogicalFileName & operator = (CDfsLogicalFileName const &from);
    void set(const char *lfn, bool removeForeign=false); // throws an exception on invalid filenames
    bool setValidate(const char *lfn, bool removeForeign=false); // returns false for invalid filenames
    void set(const CDfsLogicalFileName &lfn);
    void set(const char *scopes,const char *tail);
    bool setFromMask(const char *partmask,const char *rootdir=NULL);
    bool setFromXPath(const char *xpath);
    void clear();
    bool isSet() const;
    /*
     * Foreign files are distributed files whose meta data is stored on a foreign
     * Dali Server, so their names are resolved externally.
     */
    void setForeign(const SocketEndpoint &daliip,bool checklocal);
    void clearForeign();
    bool isForeign() const;
    /*
     * External files are non-distributed files. Most of the time
     * they refer to an input source, like files on a drop-zone
     * (either local or foreign).
     */
    void setExternal(const char *location,const char *path);
    void setExternal(const SocketEndpoint &dafsip,const char *path);
    void setExternal(const RemoteFilename &rfn);
    bool isExternal() const { return external; }
    /*
     * Multi files are temporary SuperFiles only. SuperFiles created
     * by the user do not fit into this category and are created
     * directly by an IDistributedFileDirectory object.
     */
    bool isMulti() const { return multi!=NULL; };
    /*
     * A query to a file, not a direct path. e.g. RIFS SQL
     */
    void setQuery(const char *location,const char *query);
    void setQuery(const SocketEndpoint &rfsep,const char *query);
    bool isQuery() const;

    void setCluster(const char *cname); 
    StringBuffer &getCluster(StringBuffer &cname) const { return cname.append(cluster); }

    const char *get(bool removeforeign=false) const;
    StringBuffer &get(StringBuffer &str,bool removeforeign=false, bool withCluster=false) const;
    const char *queryTail() const;
    StringBuffer &getTail(StringBuffer &buf) const;

    StringBuffer &getScopes(StringBuffer &buf,bool removeforeign=false) const;
    unsigned numScopes(bool removeforeign=false) const; // not including tail
    StringBuffer &getScope(StringBuffer &buf,unsigned idx,bool removeforeign=false) const;

    StringBuffer &makeScopeQuery(StringBuffer &query, bool absolute=true) const; // returns xpath for containing scope
    StringBuffer &makeFullnameQuery(StringBuffer &query, DfsXmlBranchKind kind, bool absolute=true) const; // return xpath for branch
    StringBuffer &makeXPathLName(StringBuffer &lfnNodeName) const; // return a mangled logical name compatible with a xpath node name

    bool getEp(SocketEndpoint &ep) const;       // foreign and external
    StringBuffer &getGroupName(StringBuffer &grp) const;    // external only
    bool getExternalPath(StringBuffer &dir, StringBuffer &tail,    // dir and tail can be same StringBuffer
#ifdef _WIN32
                        bool iswin = true,
#else
                        bool iswin = false,
#endif
                        IException **e=NULL) const;     // external only
    bool getExternalFilename(RemoteFilename &rfn) const;

    // Multi routines
    unsigned multiOrdinality() const;
    const CDfsLogicalFileName &multiItem(unsigned idx) const;
    const void resolveWild();  // only for multi
    IPropertyTree *createSuperTree() const;
    void allowOsPath(bool allow=true) { allowospath = allow; } // allow local OS path to be specified
    void setAllowWild(bool b=true) { allowWild = b; } // allow wildcards
    bool isExpanded() const;
    void expand(IUserDescriptor *user);
    void normalizeName(const char * name, StringAttr &res, bool strict);
};

// abstract class, define getCmdText to return tracing text of commands
class da_decl CTransactionLogTracker
{
    unsigned max;
    atomic_t *counts;
public:
    CTransactionLogTracker(int _max) : max(_max)
    {
        counts = new atomic_t[max+1]; // +1 reserve for unknown commands
        unsigned t=0;
        for (; t<=max; t++)
            atomic_set(&counts[t],0);
    }
    ~CTransactionLogTracker()
    {
        delete [] counts;
    }
    inline const unsigned &getMax() const { return max; }
    inline void startTransaction(unsigned cmd)
    {
        atomic_inc(&counts[cmd]);
    }
    inline void endTransaction(unsigned cmd)
    {
        atomic_dec(&counts[cmd]);
    }
    unsigned getTransactionCount(unsigned cmd) const
    {
        return (unsigned)atomic_read(&counts[cmd]);
    }
    virtual StringBuffer &getCmdText(unsigned cmd, StringBuffer &ret) const = 0;
};

extern da_decl const bool &queryTransactionLogging();
struct da_decl TransactionLog
{
    CTransactionLogTracker &owner;
    unsigned startMs, extraStartMs;
    StringBuffer msg;
    unsigned cmd;
    const SocketEndpoint &ep;
    inline TransactionLog(CTransactionLogTracker &_owner, const unsigned _cmd, const SocketEndpoint &_ep) : owner(_owner), cmd(_cmd), ep(_ep)
    {
        if (queryTransactionLogging())
        {
            if (cmd > owner.getMax())
                cmd = owner.getMax(); // unknown
            extraStartMs = 0;
            owner.startTransaction(cmd);
            owner.getCmdText(cmd, msg);
            msg.append(", endpoint=");
            ep.getUrlStr(msg);
            startMs = msTick();
        }
        else
            startMs = 0;
    }
    inline ~TransactionLog()
    {
        if (startMs)
        {
            unsigned now = msTick();
            unsigned transCount = owner.getTransactionCount(cmd);
            owner.endTransaction(cmd);
            if (extraStartMs)
                PROGLOG("<<<[%d] (Timing: total=%d, from mark=%d) %s", transCount, now-startMs, extraStartMs-startMs, msg.str());
            else
                PROGLOG("<<<[%d] (Timing: total=%d) %s", transCount, now-startMs, msg.str());
        }
    }
    inline void log()
    {
        unsigned transCount = owner.getTransactionCount(cmd);
        PROGLOG(">>>[%d] %s", transCount, msg.str());
    }
    inline void log(const char *formatMsg, ...) __attribute__((format(printf, 2, 3)))
    {
        va_list args;
        va_start(args, formatMsg);
        msg.append(" ");
        msg.valist_appendf(formatMsg, args);
        va_end(args);
        log();
    }
    inline void markExtra()
    {
        if (!extraStartMs)
            extraStartMs = msTick();
    }
    inline void extra(const char *formatMsg, ...) __attribute__((format(printf, 2, 3)))
    {
        va_list args;
        va_start(args, formatMsg);
        msg.valist_appendf(formatMsg, args);
        va_end(args);
        markExtra();
    }
};



extern da_decl const char * skipScope(const char *lname,const char *scope); // skips a sspecified scope (returns NULL if scope doesn't match)
extern da_decl const char * querySdsFilesRoot();
extern da_decl const char * querySdsRelationshipsRoot();


extern da_decl IPropertyTreeIterator *deserializePartAttrIterator(MemoryBuffer &mb);    // clears mb
extern da_decl MemoryBuffer &serializePartAttr(MemoryBuffer &mb,IPropertyTree *tree); 
extern da_decl IPropertyTree *deserializePartAttr(MemoryBuffer &mb);

extern da_decl void expandFileTree(IPropertyTree *file,bool expandnodes,const char *cluster=NULL); 
     // expands Parts blob in file as well as optionally filling in node IPs
     // if expand nodes set then removes all clusters > 1 (for backward compatibility)
extern da_decl bool shrinkFileTree(IPropertyTree *file); // compresses parts into Parts blob
extern da_decl void filterParts(IPropertyTree *file,UnsignedArray &partslist); // only include parts in list (in expanded tree)

IRemoteConnection *getSortedElements( const char *basexpath, 
                                     const char *xpath, 
                                     const char *sortorder, 
                                     const char *namefilterlo, // if non null filter less than this value
                                     const char *namefilterhi, // if non null filter greater than this value
                                     StringArray& unknownAttributes,
                                     IArrayOf<IPropertyTree> &results);
interface ISortedElementsTreeFilter : extends IInterface
{
    virtual bool isOK(IPropertyTree &tree) = 0;
};
interface IElementsPager : extends IInterface
{
    virtual IRemoteConnection *getElements(IArrayOf<IPropertyTree> &elements) = 0;
    virtual bool allMatchingElementsReceived() = 0;
};
extern da_decl void sortElements( IPropertyTreeIterator* elementsIter,
                                     const char *sortorder, 
                                     const char *namefilterlo, // if non null filter less than this value
                                     const char *namefilterhi, // if non null filter greater than this value
                                     StringArray& unknownAttributes, //the attribute not exist or empty
                                     IArrayOf<IPropertyTree> &sortedElements);

extern da_decl IRemoteConnection *getElementsPaged(IElementsPager *elementsPager,
                                     unsigned startoffset, 
                                     unsigned pagesize, 
                                     ISortedElementsTreeFilter *postfilter, // if non-NULL filters before adding to page
                                     const char *owner,
                                     __int64 *hint,                         // if non null points to in/out cache hint
                                     IArrayOf<IPropertyTree> &results,
                                     unsigned *total,
                                     bool *allMatchingElementsReceived,
                                     bool checkConn = true); // total possible filtered matches, i.e. irrespective of startoffset and pagesize

extern da_decl void clearPagedElementsCache();

class da_decl CSDSFileScanner // NB should use dadfs iterators in preference to this unless good reason not to
{
    void processScopes(IRemoteConnection *conn,IPropertyTree &root,StringBuffer &name);
    void processFiles(IRemoteConnection *conn,IPropertyTree &root,StringBuffer &name);

protected:
    bool includefiles;
    bool includesuper;
public:

    virtual void processFile(IPropertyTree &file,StringBuffer &name) {}
    virtual void processSuperFile(IPropertyTree &superfile,StringBuffer &name) {}
    virtual bool checkFileOk(IPropertyTree &file,const char *filename)
    {
        return true;
    }
    virtual bool checkSuperFileOk(IPropertyTree &file,const char *filename)
    {
        return true;
    }
    virtual bool checkScopeOk(const char *scopename)
    {
        return true;
    }
    void scan(IRemoteConnection *conn,  // conn is connection to Files
              bool includefiles=true,
              bool includesuper=false);
              
    bool singlefile(IRemoteConnection *conn,CDfsLogicalFileName &lfn);  // useful if just want to process 1 file using same code
};

extern da_decl const char *queryDfsXmlBranchName(DfsXmlBranchKind kind);
extern da_decl unsigned getFileGroups(IPropertyTree *pt,StringArray &groups,bool checkclusters=false);
extern da_decl unsigned getFileGroups(const char *grplist,StringArray &groups); // actually returns labels not groups
extern da_decl bool isAnonCluster(const char *grp);

interface IClusterFileScanIterator: extends IPropertyTreeIterator
{
public:
    virtual const char *queryName()=0;
};


extern da_decl IClusterFileScanIterator *getClusterFileScanIterator(
                      IRemoteConnection *conn,  // conn is connection to Files
                      IGroup *group,        // only scans file with nodes in specified group
                      bool exactmatch,          // only files that match group exactly (if not true includes base subset or wrapped superset)
                      bool anymatch,            // any nodes match (overrides exactmatch)
                      bool loadbranch,         // whether to load entire branch
                      IUserDescriptor *user);


class da_decl CheckTime
{
    unsigned start;
    StringBuffer msg;
public:
    CheckTime(const char *s)
    {
        msg.append(s);
        start = msTick();
    }
    ~CheckTime()
    {
        unsigned e=msTick()-start;
        if (e>1000) 
            DBGLOG("TIME: %s took %d", msg.str(), e);
    }
    bool slow() { return ((msTick()-start) > 1000); }
    StringBuffer &appendMsg(const char *s)
    {
        msg.append(s);
        return msg;
    }
};

extern da_decl void getLogicalFileSuperSubList(MemoryBuffer &mb, IUserDescriptor *user);


interface IDaliMutexNotifyWaiting
{
    virtual void startWait()=0;         // gets notified when starts waiting (after minimum period (1min))
    virtual void cycleWait()=0;
    virtual void stopWait(bool got)=0;  // and when stops
};


interface IDaliMutex: implements IInterface
{
    virtual bool enter(unsigned timeout=(unsigned)-1,IDaliMutexNotifyWaiting *notify=NULL)=0;
    virtual void leave()=0;
    virtual void kill()=0;
};
extern da_decl IDaliMutex  *createDaliMutex(const char *name);

interface IDFSredirection;
extern da_decl IDFSredirection *createDFSredirection(); // only called by dadfs.cpp

extern da_decl void safeChangeModeWrite(IRemoteConnection *conn,const char *name,bool &lockreleased,unsigned timems=INFINITE);

// Local/distributed file wrapper
//===============================

interface IDistributedFile;
interface IFileDescriptor;
interface IUserDescriptor;

interface ILocalOrDistributedFile: extends IInterface
{
    virtual const char *queryLogicalName()=0;
    virtual IDistributedFile * queryDistributedFile()=0;     // NULL for local file
    virtual IFileDescriptor *getFileDescriptor()=0;
    virtual bool getModificationTime(CDateTime &dt) = 0;                        // get date and time last modified (returns false if not set)
    virtual offset_t getFileSize() = 0;

    virtual unsigned numParts() = 0;
    virtual unsigned numPartCopies(unsigned partnum) = 0;
    virtual IFile *getPartFile(unsigned partnum,unsigned copy=0) = 0;
    virtual RemoteFilename &getPartFilename(RemoteFilename &rfn, unsigned partnum,unsigned copy=0) = 0;
    virtual offset_t getPartFileSize(unsigned partnum)=0;   // NB expanded size             
    virtual bool getPartCrc(unsigned partnum, unsigned &crc) = 0;
    virtual bool exists() const = 0;   // if created for writing, this may be false
    virtual bool isExternal() const = 0;
};

extern da_decl ILocalOrDistributedFile* createLocalOrDistributedFile(const char *fname,IUserDescriptor *user,bool onlylocal,bool onlydfs,bool iswrite=false);

typedef __int64 ConnectionId;

struct LockData
{
    unsigned mode;
    SessionId sessId;
    unsigned timeLockObtained;

    LockData(unsigned _mode, SessionId _sessId, unsigned _timeLockObtained) : mode(_mode), sessId(_sessId), timeLockObtained(_timeLockObtained)
    {
    }
    LockData(const LockData &other)
    {
        mode = other.mode;
        sessId = other.sessId;
        timeLockObtained = other.timeLockObtained;
    }
    LockData(MemoryBuffer &mb)
    {
        mb.read(mode).read(sessId).read(timeLockObtained);
    }
    void serialize(MemoryBuffer &mb) const
    {
        mb.append(mode).append(sessId).append(timeLockObtained);
    }
};

typedef MapBetween<ConnectionId, ConnectionId, LockData, LockData> ConnectionInfoMap;

class CLockMetaData : public LockData
{
    mutable StringAttr ep;
    mutable bool epResolved;
public:
    ConnectionId connectionId;

    CLockMetaData(LockData &lD, ConnectionId _connectionId) : LockData(lD), connectionId(_connectionId)
    {
        epResolved = false;
    }
    CLockMetaData(MemoryBuffer &mb) : LockData(mb)
    {
        mb.read(connectionId).read(ep);
        epResolved = true;
    }
    void serialize(MemoryBuffer &mb) const
    {
        LockData::serialize(mb);
        mb.append(connectionId).append(queryEp());
    }
    const char *queryEp() const
    {
        if (!epResolved)
        {
            epResolved = true;
            StringBuffer sessionEpStr;
            querySessionManager().getClientProcessEndpoint(sessId, sessionEpStr);
            ep.set(sessionEpStr);
        }
        return ep;
    }
};

interface ILockInfo : extends IInterface
{
    virtual const char *queryXPath() const = 0;
    virtual unsigned queryConnections() const = 0;
    virtual CLockMetaData &queryLockData(unsigned lock) const = 0;
    virtual void prune(const char *ipPattern) = 0;
    virtual void serialize(MemoryBuffer &mb) const = 0;
    virtual StringBuffer &toString(StringBuffer &out, unsigned format, bool header, const char *altText=NULL) const = 0;
};

extern da_decl ILockInfo *createLockInfo(const char *xpath, const ConnectionInfoMap &map);
extern da_decl ILockInfo *deserializeLockInfo(MemoryBuffer &mb);

typedef IArrayOf<ILockInfo> CLockInfoArray;
typedef IIteratorOf<ILockInfo> ILockInfoIterator;

interface ILockInfoCollection : extends IInterface
{
    virtual unsigned queryLocks() const = 0;
    virtual ILockInfo &queryLock(unsigned lock) const = 0;
    virtual void serialize(MemoryBuffer &mb) const = 0;
    virtual StringBuffer &toString(StringBuffer &out) const = 0;
    virtual void add(ILockInfo &lock) = 0;
};
extern da_decl ILockInfoCollection *createLockInfoCollection();
extern da_decl ILockInfoCollection *deserializeLockInfoCollection(MemoryBuffer &mb);

#endif
