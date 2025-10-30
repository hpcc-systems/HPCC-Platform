/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#ifndef XREF_HPP
#define XREF_HPP

#include <memory>
#include <unordered_map>
#include <atomic>

#include "jlib.hpp"
#include "saserver.hpp"
#include "jmisc.hpp"
#include "jset.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "dasds.hpp"
#include "rmtfile.hpp"

// Constants
#define DEFAULT_MAXDIRTHREADS 500
#define DEFAULT_MAXMEMORY 4096

#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite
#define SDS_LOCK_TIMEOUT 300000

#define DEFAULT_XREF_INTERVAL       48 // hours
#define DEFAULT_EXPIRY_INTERVAL     24 // hours

#define DEFAULT_EXPIRYDAYS 14
#define DEFAULT_PERSISTEXPIRYDAYS 7

#define LOGPFX "XREF: "
#define LOGPFX2 "FILEEXPIRY: "

#define DEFAULT_RECENT_CUTOFF_DAYS 1

constexpr int64_t oneSecondNS = 1000 * 1000 * 1000; // 1 second in nanoseconds
constexpr int64_t oneHourNS = 60 * 60 * oneSecondNS; // 1 hour in nanoseconds

// A simple allocator to track memory usage and throw an exception if the
// requested size exceeds @memoryLimit or resources/@memory in containerized
class XRefAllocator
{
public:
    XRefAllocator(unsigned _maxMB);
    ~XRefAllocator();

    void *alloc(unsigned sz);
    void dealloc(void *ptr, unsigned sz);

private:
    size_t maxBytes = 0;
    std::atomic<size_t> usedBytes = 0;
};

struct cMisplacedRec
{
private:
    cMisplacedRec(XRefAllocator *_allocator);

public:
    static cMisplacedRec *create(XRefAllocator *allocator);

    static void *operator new(size_t baseSize, XRefAllocator *allocator);
    static void operator delete(void* p, XRefAllocator* allocator) noexcept;
    static void operator delete(void *ptr) noexcept;

    XRefAllocator *allocator = nullptr;
    cMisplacedRec *next = nullptr;
    unsigned short nn = 0;  // node on (+N*drv)
    unsigned short pn = 0;  // part number
    bool marked = false;

    void init(unsigned drv, unsigned pf, unsigned xn, unsigned tn);
    bool eq(unsigned drv, unsigned pf, unsigned xn, unsigned tn);
    unsigned getDrv(unsigned tn);
    unsigned getNode(unsigned tn);
};

struct cFileDesc
{
private:
    cFileDesc(const char *_name, unsigned nameLen, unsigned mapLen, unsigned numParts, bool d, unsigned fnLen, XRefAllocator *_allocator);

public:
    static cFileDesc *create(const char *name, unsigned numParts, bool isDirPerPart, unsigned fnLen, XRefAllocator *allocator);

    static void *operator new(size_t baseSize, unsigned nameLen, unsigned mapLen, XRefAllocator *allocator);
    static void operator delete(void *ptr, XRefAllocator* allocator) noexcept;
    static void operator delete(void *ptr) noexcept;

    ~cFileDesc();

    XRefAllocator *allocator = nullptr;
    unsigned hash;
    unsigned short N;                     // num parts
    bool isDirPerPart;                    // directory-per-part number present in physical path
    byte filenameLen;                     // length of file name excluding extension i.e. ._$P$_of_$N$
    const char *owningfile = nullptr;     // for crosslinked
    cMisplacedRec *misplaced = nullptr;   // for files on the wrong node
    byte name[1];                         // first byte length

    unsigned getSize() const;
    inline byte *map() const;

    bool setpresent(unsigned d, unsigned i);
    bool testpresent(unsigned d, unsigned i);
    bool setmarked(unsigned d, unsigned i);
    bool testmarked(unsigned d, unsigned i);
    bool eq(const char *key);
    bool isHPCCFile() const;
    bool getName(StringBuffer &buf) const;
    StringBuffer &getNameMask(StringBuffer &buf) const;
    StringBuffer &getPartName(StringBuffer &buf, unsigned p) const;

    static cFileDesc * create(const char *);
    static unsigned getHash(const char *key);
};

struct cDirDesc
{
private:
    cDirDesc(const char *_name, size32_t sl, XRefAllocator *_allocator);

public:
    static cDirDesc *create(const char * name, XRefAllocator *allocator);

    static void *operator new(size_t baseSize, unsigned nameLen, XRefAllocator *allocator);
    static void operator delete(void *ptr, XRefAllocator* allocator) noexcept;
    static void operator delete(void *ptr) noexcept;

    XRefAllocator *allocator = nullptr;
    unsigned hash;
    std::unordered_map<std::string, std::unique_ptr<cDirDesc>> dirs;
    std::unordered_map<std::string, std::unique_ptr<cFileDesc>> files;
    CriticalSection dirsCrit;
    CriticalSection filesCrit;
    CriticalSection dirDescCrit;
    offset_t totalsize[2];              //  across all nodes
    offset_t minsize[2];                //  smallest node size
    offset_t maxsize[2];                //  largest node size
    unsigned short minnode[2];          //  smallest node (1..)
    unsigned short maxnode[2];          //  largest node (1..)

    byte name[1];                     // first byte length  NB this is the tail name

    unsigned getSize() const;
    bool eq(const char *key);
    static unsigned getHash(const char *key);
    StringBuffer &getName(StringBuffer &buf);

    cDirDesc *lookupDirNonThreadSafe(const char *name, XRefAllocator *allocator);
    cDirDesc *lookupDir(const char *name, XRefAllocator *allocator);

    const char *decodeName(unsigned drv, const char *name, unsigned node, unsigned numnodes,
                    StringAttr &mask, unsigned &pf, unsigned &nf, unsigned &filenameLen);

    bool isMisplaced(unsigned partNum, unsigned numParts, const SocketEndpoint &ep, IGroup &grp,
                     const char *fullPath, unsigned filePathOffset, unsigned stripeNum, unsigned numStripedDevices);

    cFileDesc *addFile(unsigned drv, const char *name, const char *filePath, unsigned filePathOffset,
                       unsigned node, const SocketEndpoint &ep, IGroup &grp, unsigned numnodes,
                       unsigned stripeNum, unsigned numStripedDevices, XRefAllocator *allocator);

    bool markFile(unsigned drv, const char *name, unsigned node, const SocketEndpoint &ep, IGroup &grp, unsigned numnodes);
    void addNodeStats(unsigned node, unsigned drv, offset_t sz);
    bool empty(unsigned drv);
};

struct cMessage: public CInterface
{
    StringAttr lname;
    StringAttr msg;
    cMessage(const char *_lname, const char *_msg);
};

class XRefPeriodicTimer : public PeriodicTimer
{
public:
    XRefPeriodicTimer() = default;
    XRefPeriodicTimer(unsigned seconds, bool suppressFirst, const char *_clustname);

    unsigned calcElapsedMinutes() const;
    bool hasElapsed();
    void reset(unsigned seconds, bool suppressFirst, const char *_clustname);
    void updatePeriod();

private:
    CriticalSection timerSect;
    const char *clustname = nullptr; // Cluster name for logging
    cycle_t startCycles = 0;
};

class CNewXRefManagerBase
{
public:
    CNewXRefManagerBase();
    virtual ~CNewXRefManagerBase() = default;

    CriticalSection logsect;
    Owned<IRemoteConnection> logconn;
    StringAttr logcache;
    StringAttr clustname;
    CIArrayOf<cMessage> errors;
    CIArrayOf<cMessage> warnings;
    StringAttr rootdir;
    unsigned lastlog;
    unsigned sfnum;
    unsigned fnum;
    std::atomic<uint64_t> processedDirs{0};
    std::atomic<uint64_t> processedFiles{0};
    XRefPeriodicTimer heartbeatTimer;

    bool saveToDebugPlane = false; // If true, before updating ECL Watch, save scan data to sasha debug plane
    Owned<IPropertyTree> foundbranch;
    Owned<IPropertyTree> lostbranch;
    Owned<IPropertyTree> orphansbranch;
    Owned<IPropertyTree> dirbranch;

    void log(bool forceStatusUpdate, const char * format, ...) __attribute__((format(printf, 3, 4)));
    void error(const char *lname, const char * format, ...) __attribute__((format(printf, 3, 4)));
    void warn(const char *lname, const char * format, ...) __attribute__((format(printf, 3, 4)));
    void updateStatus(bool uncond);
    void startHeartbeat(const char * op);
    void checkHeartbeat(const char * op);
    void doSaveToDebugPlane(const char *name, StringBuffer &datastr);
    void addBranch(IPropertyTree *root, const char *name, IPropertyTree *branch);
    void start(bool updateeclwatch, const char *clname);
    void finish(bool aborted);
    void addErrorsWarnings(IPropertyTree *croot);
    void saveToEclWatch(bool &abort, bool byscheduler);
};

class CNewXRefManager: public CNewXRefManagerBase
{
    XRefAllocator allocator;
    std::unique_ptr<cDirDesc> root;
    bool iswin;                     // set by scanDirectories
    IpAddress *iphash;
    unsigned *ipnum;
    unsigned iphashsz;
    IArrayOf<IPropertyTree> sorteddirs;

public:
    Owned<IGroup> grp, rawgrp;
    StringArray clusters;           // list of matching cluster (used in xref)
    StringBuffer clusterscsl;       // comma separated list of cluster (used in xref)
    unsigned numnodes;
    StringArray lostfiles;
    bool verbose;
    unsigned numuniqnodes = 0;
    Owned<IUserDescriptor> udesc;
    Linked<IPropertyTree> storagePlane;
    Linked<IPropertyTree> serverConfig;
    bool isPlaneStriped = false;
    unsigned numStripedDevices = 1;

    CNewXRefManager(IPropertyTree *plane, unsigned maxMb, IPropertyTree *_serverConfig);
    ~CNewXRefManager();

    void start(bool updateeclwatch);
    void addIpHash(const IpAddress &ip, unsigned n);
    unsigned checkIpHash(const IpAddress &ip);
    bool setGroup(const char *_clustname, const char *_grpname, IArrayOf<IGroup> &done, StringArray &donedir);
    void clear();
    cDirDesc *findDirectory(const char *name);
    bool dirFiltered(const char *filename);
    bool fileFiltered(const char *filename, const CDateTime &dt);
    bool scanDirectory(unsigned node, const SocketEndpoint &ep, StringBuffer &path, unsigned drv,
                       cDirDesc *pdir, IFile *cachefile, unsigned level, unsigned filePathOffset, unsigned stripeNum);
    bool scanDirectories(bool &abort, unsigned numThreads);
    void scanLogicalFiles(bool &abort);
    bool checkOrphanPhysicalFile(RemoteFilename &rfn, offset_t &sz, CDateTime &dt);
    void addOrphanPartNode(Owned<IPropertyTree> &branch, const SocketEndpoint &ep, unsigned i, bool rep);
    void addExternalFoundFile(cFileDesc *file, const char *currentPath, unsigned int recentCutoffDays);
    void listOrphans(cFileDesc *f, const char *currentPath, const char *currentScope, bool &abort, unsigned int recentCutoffDays);
    void listDirectory(cDirDesc *d, const char *name, bool &abort);
    void listOrphans(cDirDesc *d, StringBuffer &basedir, StringBuffer &scope, bool &abort, unsigned int recentCutoffDays);
    static int compareDirs(IInterface * const *t1, IInterface * const *t2);
    void listOrphans(bool &abort, unsigned int recentCutoffDays);
    void listLost(bool &abort, bool ignorelazylost, unsigned int recentCutoffDays);
    void save();
};

class CSuperfileCheckManager: public CNewXRefManagerBase
{
public:
    CSuperfileCheckManager();
    void start(bool updateeclwatch);
    void errornotrecent(const char *lname, const char * format, ...) __attribute__((format(printf, 3, 4)));
    void checkSuperFileLinkage();
};

#endif // XREF_HPP
