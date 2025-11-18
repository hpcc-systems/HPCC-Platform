//TBD check min time from when *finished*

#include "platform.h"

#include "jlib.hpp"
#include "jiface.hpp"
#include "jstring.hpp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jregexp.hpp"
#include "jset.hpp"
#include "jfile.hpp"
#include "jplane.hpp"
#include "jutil.hpp"
#include "jsocket.hpp"

#include <memory>
#include <unordered_map>

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "dasds.hpp"
#include "dalienv.hpp"
#include "rmtfile.hpp"

#include "saserver.hpp"
#include "sautil.hpp"
#include "sacoalescer.hpp"
#include "sacmd.hpp"
#include "salds.hpp"

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

inline bool nextCsvToken(const char *&s,StringBuffer &tok)
{
    if (!s)
        return false;
    for (;;) {
        if (!*s)
            return false;
        if (*s!=',')
            break;
        s++;
    }
    const char *e = s;
    for (;;) {
        if (!*e||(*e==','))
            break;
        e++;
    }
    if (e==s)
        return false;
    tok.append(e-s,s);
    s = e;  // leave comma for next time
    return true;
}


// A simple allocator to track memory usage and throw an exception if the
// requested size exceeds @memoryLimit or resources/@memory in containerized
class XRefAllocator
{
public:
    XRefAllocator(unsigned _maxMB)
    {
        maxBytes = ((size_t)_maxMB) * 0x100000;
    }

    ~XRefAllocator()
    {
        if (usedBytes != 0)
            OWARNLOG(LOGPFX "XRefAllocator::~XRefAllocator : Memory leak detected: %zu bytes", usedBytes.load());
    }

    void *alloc(unsigned sz)
    {
        size_t oldUsed = usedBytes.fetch_add(sz);
        if ((sz+oldUsed)>maxBytes)
        {
            usedBytes.fetch_sub(sz); // Roll back the increment
            throw makeStringExceptionV(0, "XRefAllocator::alloc : Requested size too large: req: %d, used: %zu, max: %zu", sz, oldUsed, maxBytes);
        }

        void *ret = malloc(sz);
        if (ret == nullptr) throw std::bad_alloc();

        return ret;
    }

    void dealloc(void *ptr, unsigned sz)
    {
        free(ptr);
        usedBytes.fetch_sub(sz);
    }

private:
    size_t maxBytes = 0;
    std::atomic<size_t> usedBytes = 0;
};


struct cMisplacedRec
{
private:
    cMisplacedRec(XRefAllocator *_allocator)
        : allocator(_allocator)
    {}
public:
    static cMisplacedRec *create(XRefAllocator *allocator)
    {
        return new(allocator) cMisplacedRec(allocator);
    }

    static void *operator new(size_t baseSize, XRefAllocator *allocator)
    {
        return allocator->alloc(baseSize);
    }

    static void operator delete(void* p, XRefAllocator* allocator) noexcept
    {
        // Called if constructor throws
        allocator->dealloc(p, sizeof(cMisplacedRec));
    }

    static void operator delete(void *ptr) noexcept
    {
        cMisplacedRec *desc = static_cast<cMisplacedRec *>(ptr);
        desc->allocator->dealloc(ptr, sizeof(cMisplacedRec));
    }

    XRefAllocator *allocator = nullptr;
    cMisplacedRec *next = nullptr;
    unsigned short nn = 0;  // node on (+N*drv)
    unsigned short pn = 0;  // part number
    bool marked = false;

    void init(unsigned drv,
              unsigned pf,      // part
              unsigned xn,      // node located on
              unsigned tn)      // total nodes
    {
        nn = (unsigned short)(xn+tn*drv);
        pn = (unsigned short)pf;
        marked = false;
        next = nullptr;
    }

    bool eq(unsigned drv,
              unsigned pf,      // part
              unsigned xn,      // node located on
              unsigned tn)      // total nodes
    {
        if (pn != (unsigned short)pf)
            return false;
        return (nn == (unsigned short)(xn+tn*drv));
    }

    unsigned getDrv(unsigned tn)
    {
        return nn/tn;
    }

    unsigned getNode(unsigned tn)
    {
        return nn%tn;
    }
};


struct cFileDesc // no virtuals
{
private:
    cFileDesc(const char *_name, unsigned nameLen, unsigned mapLen, unsigned numParts, bool d, unsigned fnLen, XRefAllocator *_allocator)
        : allocator(_allocator)
    {
        N = (unsigned short)numParts;
        isDirPerPart = d;
        filenameLen = (byte)fnLen;

        name[0] = (byte)nameLen;
        memcpy(&name[1],_name,nameLen);
        memset(map(),0,mapLen);
        hash = hashc((const byte *)_name,nameLen,17);
    }

public:
    XRefAllocator *allocator = nullptr;
    unsigned hash;
    unsigned short N;                     // num parts
    bool isDirPerPart;                    // directory-per-part number present in physical path
    byte filenameLen;                     // length of file name excluding extension i.e. ._$P$_of_$N$
    const char *owningfile = nullptr;     // for crosslinked
    cMisplacedRec *misplaced = nullptr;   // for files on the wrong node
    byte name[1];                         // first byte length
    // char namestr[name[1]]
    // bitset presentc[N];
    // bitset presentd[N];
    // bitset markedc[N];
    // bitset markedd[N];


    static cFileDesc *create(const char *name, unsigned numParts, bool isDirPerPart, unsigned fnLen, XRefAllocator *allocator)
    {
        unsigned mapLen;
        if (numParts==NotFound)
        {
            // numParts==NotFound is used for files without a part mask. Treat them as single files
            mapLen = 1;
        }
        else if (numParts<=0xfff)
            mapLen = (numParts*4+7)/8;
        else
            throw makeStringExceptionV(0, "cFileDesc::create : numParts too large: %d (max 4096)", numParts);

        size_t nameLen = strlen(name);
        if (nameLen>255)
        {
            OWARNLOG(LOGPFX "File name %s longer than 255 chars, truncating",name);
            nameLen = 255;
        }
        return new(nameLen, mapLen, allocator) cFileDesc(name, nameLen, mapLen, numParts, isDirPerPart, fnLen, allocator);
    }

    static void *operator new(size_t baseSize, unsigned nameLen, unsigned mapLen, XRefAllocator *allocator)
    {
        return allocator->alloc(baseSize+nameLen+mapLen);
    }

    static void operator delete(void *ptr, XRefAllocator* allocator) noexcept
    {
        // Called if constructor throws
        cFileDesc *desc = static_cast<cFileDesc *>(ptr);
        allocator->dealloc(ptr, desc->getSize());
    }

    static void operator delete(void *ptr) noexcept
    {
        cFileDesc *desc = static_cast<cFileDesc *>(ptr);
        desc->allocator->dealloc(ptr, desc->getSize());
    }

    ~cFileDesc()
    {
        // Clean up misplaced records
        while (misplaced)
        {
            cMisplacedRec *next = misplaced->next;
            delete misplaced;
            misplaced = next;
        }
    }

    unsigned getSize() const
    {
        size32_t nameLen = name[0];
        size32_t mapLen = N==(unsigned short)NotFound ? 1 : (N*4+7)/8; // N==(unsigned short)NotFound is used for files without a part mask. Treat them as single files
        return sizeof(cFileDesc)+nameLen+mapLen;
    }
    inline byte *map() const
    {
        return (byte *)&name+1+name[0];
    }

    bool setpresent(unsigned d,unsigned i) // returns old value
    {
        if (d)
            i += N*d;
        byte v = 1<<(i%8);
        byte &m = map()[i/8];
        bool ret = (m&v)!=0;
        m |= v;
        return ret;
    }

    bool testpresent(unsigned d,unsigned i)
    {
        if (d)
            i += N*d;
        return (map()[i/8]&(1<<(i%8)))!=0;
    }

    bool setmarked(unsigned d,unsigned i) // returns old value
    {
        return setpresent(d+2,i);
    }

    bool testmarked(unsigned d,unsigned i)
    {
        return testpresent(d+2,i);
    }

    bool eq(const char *key)
    {
        size32_t sl = strlen(key);
        if (sl>255)
            sl = 255;
        if (sl!=(byte)name[0])
            return false;
        return memcmp(key,name+1,sl)==0;
    }

    bool isHPCCFile() const
    {
        return filenameLen > 0;
    }

    bool getName(StringBuffer &buf) const
    {
        // Check mask exists
        if (filenameLen == 0)
            return false;
        buf.append((size32_t)filenameLen, (const char *)(name+1));
        return true;
    }

    StringBuffer &getNameMask(StringBuffer &buf) const
    {
        return buf.append((size32_t)name[0],(const char *)(name+1));
    }

    StringBuffer &getPartName(StringBuffer &buf,unsigned p) const
    {
        StringBuffer mask;
        getNameMask(mask);
        return expandMask(buf, mask, p, N);
    }

    static cFileDesc * create(const char *)
    {
        assertex(false);
        return NULL;
        // not used
    }

    static unsigned getHash(const char *key)
    {
        size32_t sl = strlen(key);
        if (sl>255)
            sl = 255;
        return  hashc((const byte *)key,sl,17);
    }



};


struct cDirDesc
{
private:
    cDirDesc(const char *_name, size32_t sl, XRefAllocator *_allocator)
        : allocator(_allocator)
    {
        name[0] = (byte)sl;
        memcpy(name+1,_name,sl);
        hash = hashc((const byte *)_name,sl,17);
        for (unsigned drv=0;drv<2;drv++) {
            totalsize[drv] = 0;
            minnode[drv] = 0;
            minsize[drv] = 0;
            maxnode[drv] = 0;
            maxsize[drv] = 0;
        }
        dirPerPartNum = readDigits(_name, sl, false);
    }

public:
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
    unsigned dirPerPartNum = 0;         //  only >0 if the directory *looks* like a dir-per-part dir

    byte name[1];                     // first byte length  NB this is the tail name
    // char namestr[*name]

    static cDirDesc *create(const char * name, XRefAllocator *allocator)
    {
        size32_t nameLen = strlen(name);
        if (nameLen>255)
        {
            OWARNLOG(LOGPFX "Directory name %s longer than 255 chars, truncating",name);
            nameLen = 255;
        }
        return new(nameLen, allocator) cDirDesc(name, nameLen, allocator);
    }

    static void *operator new(size_t baseSize, unsigned nameLen, XRefAllocator *allocator)
    {
        return allocator->alloc(baseSize+nameLen);
    }

    static void operator delete(void *ptr, XRefAllocator* allocator) noexcept
    {
        // Called if constructor throws
        cDirDesc *desc = static_cast<cDirDesc *>(ptr);
        allocator->dealloc(ptr, desc->getSize());
    }

    static void operator delete(void *ptr) noexcept
    {
        cDirDesc *desc = static_cast<cDirDesc *>(ptr);
        desc->allocator->dealloc(ptr, desc->getSize());
    }

    unsigned getSize() const
    {
        size32_t nameLen = name[0];
        return sizeof(cDirDesc)+nameLen;
    }


    bool eq(const char *key)
    {
        size32_t sl = strlen(key);
        if (sl>255)
            sl = 255;
        if (sl!=(byte)name[0])
            return false;
        return memcmp(key,name+1,sl)==0;
    }

    static unsigned getHash(const char *key)
    {
        size32_t sl = strlen(key);
        if (sl>255)
            sl = 255;
        return  hashc((const byte *)key,sl,17);
    }

    StringBuffer &getName(StringBuffer &buf)
    {
        return buf.append((size32_t)name[0],(const char *)name+1);
    }

    const char *queryName() const
    {
        return (const char *)name+1;
    }

    size32_t getNameLen() const
    {
        return (size32_t)name[0];
    }

    bool isDirPerPartCandidate() const
    {
        return dirPerPartNum > 0; // I look like a dir-per-dir directory
    }

    bool isDirPerPartMatch(unsigned partNum) const
    {
        assertex(isDirPerPartCandidate());
        return partNum==(dirPerPartNum-1);
    }

    cFileDesc *getFile(const char *filename)
    {
        CriticalBlock block(filesCrit);
        auto it = files.find(filename);
        if (it != files.end())
            return it->second.get();
        return nullptr;
    }

    cFileDesc *ensureFile(const char *filename, unsigned numParts, bool isDirPerPart, unsigned filenameLen, XRefAllocator *allocator)
    {
        CriticalBlock block(filesCrit);
        auto it = files.find(filename);
        cFileDesc *file = nullptr;
        if (it != files.end())
            file = it->second.get();
        else
        {
            file = cFileDesc::create(filename, numParts, isDirPerPart, filenameLen, allocator);
            files.emplace(filename, file);
        }
        return file;
    }

    void addExistingFile(const char *filename, cFileDesc *file)
    {
        CriticalBlock block(filesCrit);
        files.emplace(filename, file);
    }

    cDirDesc *lookupDirNonThreadSafe(const char *name, XRefAllocator *allocator)
    {
        auto it = dirs.find(name);
        if (it != dirs.end())
            return it->second.get();

        if (!allocator)
            return nullptr;

        // NB: Creation only happens during scanDirectories, this function should be called from [thread-safe] lookupDir()
        // lookupDirNonThreadSafe is also called in findDirectory during scanLogicalFiles. It will not reach here, and does not need to be thread safe )
        cDirDesc *ret = cDirDesc::create(name, allocator);
        dirs.emplace(name, ret);
        return ret;
    }

    cDirDesc *lookupDir(const char *name, XRefAllocator *allocator)
    {
        CriticalBlock block(dirsCrit);
        return lookupDirNonThreadSafe(name, allocator);
    }

    static const char *decodeName(unsigned drv,const char *name,unsigned node, unsigned numnodes,
                    StringAttr &mask,       // decoded mask
                    unsigned &pf,           // part node
                    unsigned &nf,           // num parts
                    unsigned &filenameLen)  // length of file name excluding extension i.e. ._$P$_of_$N$
    {
        const char *fn = name;
        // first see if tail fits a mask
        if (deduceMask(fn, true, mask, pf, nf, filenameLen))
            fn = mask.get();
        else {  // didn't match mask so use straight name
            //PROGLOG("**unmatched(%d,%d,%d) %s",drv,node,numnodes,name);
            pf = (node+numnodes-drv)%numnodes;
            nf = NotFound;
        }
        return fn;
    }

    static bool isMisplaced(unsigned partNum, unsigned numParts, const SocketEndpoint &ep, IGroup &grp, const char *fullPath, unsigned filePathOffset, unsigned stripeNum, unsigned numStripedDevices)
    {
        // External files (i.e. no ._n_of_m suffix) are considered misplaced so we can get
        // the node where the external file was found for addExternalFoundFile later
        if (numParts==NotFound)
            return true;

        if (isContainerized())
        {
            // MORE: How can we check hosted planes?
            // Checking against group info would still make sense in containerized if hosted plane
            // if (hostedPlane)
            //     return numParts!=grp.ordinality() || partNum>=grp.ordinality() || !grp.queryNode(partNum).endpoint().equals(ep);

            if ((numStripedDevices>1)&&((stripeNum>numStripedDevices)||(stripeNum<1)))
                return true;

            // Get pointer to filename
            filePathOffset++; // fullPath+filePathOffset will always be a slash, so skip it
            const char *filePath = fullPath+filePathOffset;
            unsigned filePathLen = strlen(filePath);
            const char *filenameEndPtr = filePath+filePathLen;
            const char *filename = filenameEndPtr-1;
            while (filename>filePath)
            {
                if (*filename=='/')
                    break;
                filename--;
            }

            unsigned dirPerPartNum = 0;
            const char *dirPerPartPtr = nullptr; // If dirPerPartNum != 0, this points to the slash before the dir name of the dir-per-part directory
            if (filename==filePath)
            {
                // If filename starts at filePathOffset, the file was found in the root directory
                // Skip checking for a dir-per-part number because there is no directory name to check
            }
            else
            {
                // Calculate dir-per-part number from the file path
                // A file path can contain numeric directories that are not dir-per-part directories,
                // so we only check maxDirPerPartDigits to avoid false positives
                // Scan the filename backwards checking for digits until maxDirPerPartDigits is reached
                // or a non-digit character is found or a '/' is found
                constexpr unsigned maxDirPerPartDigits = 6;
                constexpr unsigned pow10[maxDirPerPartDigits] = {1, 10, 100, 1000, 10000, 100000};
                const char *tailDirEndPtr = filename-1; // Points to last character in dir name then if dir-per-part is found, it points to the slash before the dir name
                dirPerPartPtr = tailDirEndPtr;
                unsigned dirPerPartDigits = 0;
                while (true)
                {
                    if (isdigit(*dirPerPartPtr))
                    {
                        if (dirPerPartDigits>=maxDirPerPartDigits)
                        {
                            // Too many digits to be a dir-per-part directory
                            dirPerPartNum = 0;
                            break;
                        }
                        dirPerPartNum = dirPerPartNum + ((*dirPerPartPtr - '0') * pow10[dirPerPartDigits]);
                        dirPerPartPtr--;
                        dirPerPartDigits++;
                    }
                    else if (*dirPerPartPtr=='/')
                    {
                        if (dirPerPartPtr==tailDirEndPtr)
                            throw makeStringExceptionV(-1, LOGPFX "isMisplaced: Invalid directory name in file path: %s", fullPath);

                        // Reached end of directory name and found only digits, likely a dir-per-part directory
                        break;
                    }
                    else
                    {
                        // Not a digit or '/' means this is not a dir-per-part directory
                        dirPerPartNum = 0;
                        break;
                    }
                }

                // Check dirPerPartNum against partNum if dirPerPartNum is not zero and is less than the number of file parts
                // to avoid directories like /2025 or /092325 which are clearly dates and not dir-per-part numbers
                if ((dirPerPartNum>0)&&(dirPerPartNum<=numParts)&&(partNum!=(dirPerPartNum-1)))
                    return true;
            }

            // No more checks for non-striped containerized planes
            if (numStripedDevices==1)
                return false;

            // Get pointer to extension in filename to exclude for hashing
            const char *ext = filenameEndPtr-1;
            while (true)
            {
                if (ext==filename)
                {
                    // No extension found, reset to end ptr to use full filename in code below
                    ext = filenameEndPtr;
                    break;
                }
                else if (*ext=='.')
                    break;
                ext--;
            }

            // Calculate hash from the file path
            unsigned lfnHash = 0;
            if (dirPerPartNum)
            {
                // Catch dir-per-part files found in the root directory e.g. /prefix/1/afile._1_of_1
                if (dirPerPartPtr < filePath)
                    lfnHash = 0; // Nothing to hash
                else
                    lfnHash = getLfnHashFromPath(dirPerPartPtr-filePath,filePath);
                filename++; // Skip past leading slash
                lfnHash = appendLfnHashFromPath(ext-filename, filename, lfnHash);
            }
            else
                lfnHash = getLfnHashFromPath(ext-filePath, filePath, lfnHash);

            return calcStripeNumber(partNum, lfnHash, numStripedDevices)!=stripeNum;
        }
        else
            return numParts!=grp.ordinality() || partNum>=grp.ordinality() || !grp.queryNode(partNum).endpoint().equals(ep);
    }

    void setMisplacedAndPresent(cFileDesc *file, bool misplaced, unsigned partNum, unsigned drv, const char *filePath, unsigned node, unsigned numnodes)
    {
        CriticalBlock block(filesCrit);

        if (misplaced) {
            cMisplacedRec *mp = file->misplaced;
            while (mp) {
                if (mp->eq(drv,partNum,node,numnodes)) {
                    OERRLOG(LOGPFX "Duplicate file with mismatched tail (%d,%d) %s",partNum,node,filePath);
                    return;
                }
                mp = mp->next;
            }
            mp = cMisplacedRec::create(allocator);
            mp->init(drv,partNum,node,numnodes);
            mp->next = file->misplaced;
            file->misplaced = mp;
            // NB: still perform setpresent() below, so that later 'orphan' and 'found' scanning can spot the part as orphaned or part of a found file.
        }

        if (file->setpresent(drv,partNum))
            OERRLOG(LOGPFX "Duplicate file with mismatched tail (%d) %s",partNum,filePath);
    }

    bool markFile(unsigned drv,const char *name, unsigned node, const SocketEndpoint &ep, IGroup &grp, unsigned numnodes)
    {
        unsigned nf;
        unsigned pf;
        unsigned filenameLen;
        StringAttr mask;
        const char *fn = decodeName(drv,name,node,numnodes,mask,pf,nf,filenameLen);
        // NB: markFile is only called on files found from logical file metadata. Cannot check if file was striped
        // to correct location since processFiles only checks the expected location.
        bool misplaced = !isContainerized() && (nf!=grp.ordinality() || pf>=grp.ordinality() || !grp.queryNode(pf).endpoint().equals(ep));

        CriticalBlock block(filesCrit); // NB: currently, markFile is only called from scanOrphans, which is single-threaded
        auto it = files.find(fn);
        if (it != files.end()) {
            cFileDesc *file = it->second.get();
            if (misplaced) {
                cMisplacedRec *mp = file->misplaced;
                while (mp) {
                    if (mp->eq(drv,pf,node,numnodes)) {
                        mp->marked = true;
                        return true;
                    }
                    mp = mp->next;
                }
            }
            else if (file->testpresent(drv,pf)) {
                file->setmarked(drv,pf);
                return true;
            }
        }
        return false;
    }

    void addNodeStats(unsigned node,unsigned drv,offset_t sz)
    {
        if (drv>1)
            drv = 1;

        CriticalBlock block(dirDescCrit);
        totalsize[drv] += sz;
        if (!minnode[drv]||(minsize[drv]>sz)) {
            minnode[drv] = node+1;
            minsize[drv] = sz;
        }
        if (!maxnode[drv]||(maxsize[drv]<sz)) {
            maxnode[drv] = node+1;
            maxsize[drv] = sz;
        }
    }

    bool empty(unsigned drv)
    {
        // NB: Thread-safety not required because called after directory scan
        // completes, when structure is read-only and no longer being modified.
        // empty if no files, and all subdirs are empty
        if ((!files.empty())||(totalsize[drv]!=0))
            return false;
        if (dirs.empty())
            return true;
        for (const auto& dirPair : dirs) {
            if (!dirPair.second->empty(drv))
                return false;
        }
        return true;
    }

};


struct cMessage: public CInterface
{
    StringAttr lname;
    StringAttr msg;
    cMessage(const char *_lname,const char *_msg)
        : lname(_lname), msg(_msg)
    {
    }
};


constexpr int64_t oneSecondNS = 1000 * 1000 * 1000; // 1 second in nanoseconds
constexpr int64_t oneHourNS = 60 * 60 * oneSecondNS; // 1 hour in nanoseconds
class XRefPeriodicTimer : public PeriodicTimer
{
public:
    XRefPeriodicTimer() = default;
    XRefPeriodicTimer(unsigned seconds, bool suppressFirst, const char *_clustname)
    : clustname(_clustname) { reset(seconds, suppressFirst, clustname); }

    unsigned calcElapsedMinutes() const
    {
        int64_t elapsedNS = cycle_to_nanosec(lastElapsedCycles - startCycles);
        return elapsedNS / oneSecondNS / 60;
    }

    bool hasElapsed()
    {
        // MORE: Could make PeriodicTimer::hasElapsed thread safe and remove CriticalBlock
        CriticalBlock block(timerSect);
        return PeriodicTimer::hasElapsed();
    }

    int64_t queryElapsedNS() const
    {
        return cycle_to_nanosec(get_cycles_now() - startCycles);
    }

    void reset(unsigned seconds, bool suppressFirst, const char *_clustname)
    {
        clustname = _clustname;
        PeriodicTimer::reset(seconds*1000, suppressFirst);
        startCycles = lastElapsedCycles;
    }

    // Double the time period until it reaches 1 hour
    void updatePeriod()
    {
        int64_t timePeriodNS = cycle_to_nanosec(timePeriodCycles) + 1; // 1 second is lost converting to nanoseconds
        if (timePeriodNS < oneHourNS)
        {
            int64_t newTimePeriodNS = timePeriodNS >= oneHourNS / 2 ? oneHourNS : timePeriodNS * 2;
            timePeriodCycles = nanosec_to_cycle(newTimePeriodNS);

            unsigned intervalMinutes = newTimePeriodNS / oneSecondNS / 60;
            if (clustname)
                DBGLOG(LOGPFX "[%s] Heartbeat interval increased to %u minutes", clustname, intervalMinutes);
            else
                DBGLOG(LOGPFX "Heartbeat interval increased to %u minutes", intervalMinutes);
        }
    }

private:
    CriticalSection timerSect;
    const char *clustname = nullptr; // Cluster name for logging
    cycle_t startCycles = 0;
};

class CNewXRefManagerBase
{
public:
    CriticalSection logsect;
    Owned<IRemoteConnection> logconn;
    StringAttr logcache;
    StringAttr clustname;
    CIArrayOf<cMessage> errors;
    CIArrayOf<cMessage> warnings;
    StringAttr rootdir;
    unsigned lastlog = 0;
    unsigned sfnum = 0;
    unsigned fnum = 0;
    std::atomic<uint64_t> processedDirs{0};
    std::atomic<uint64_t> processedFiles{0};
    XRefPeriodicTimer heartbeatTimer;

    Owned<IPropertyTree> foundbranch;
    Owned<IPropertyTree> lostbranch;
    Owned<IPropertyTree> orphansbranch;
    Owned<IPropertyTree> dirbranch;
    bool saveToPlane = false;

    void log(bool forceStatusUpdate, const char * format, ...) __attribute__((format(printf, 3, 4)))
    {
        StringBuffer line;
        va_list args;
        va_start(args, format);
        line.valist_appendf(format, args);
        va_end(args);

        if (clustname.get())
            PROGLOG(LOGPFX "[%s] %s",clustname.get(),line.str());
        else
            PROGLOG(LOGPFX "%s",line.str());

        if (logconn) {
            CriticalBlock block(logsect);
            logcache.set(line.str());
            updateStatus(forceStatusUpdate);
        }
    }

    void error(const char *lname,const char * format, ...) __attribute__((format(printf, 3, 4)))
    {
        CriticalBlock block(logsect);
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        if (errors.ordinality()<1000) {
            errors.append(*new cMessage(lname,line.str()));
            if (errors.ordinality()==1000)
                errors.append(*new cMessage("","error limit exceeded (1000), truncating"));
        }

        OERRLOG("%s: %s",lname,line.str());
    }

    void warn(const char *lname,const char * format, ...) __attribute__((format(printf, 3, 4)))
    {
        CriticalBlock block(logsect);
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        if (warnings.ordinality()<1000) {
            warnings.append(*new cMessage(lname,line.str()));
            if (warnings.ordinality()==1000)
                warnings.append(*new cMessage("","warning limit (1000) exceeded, truncating"));
        }
        OWARNLOG("%s: %s",lname,line.str());
    }


    void updateStatus(bool uncond)
    {
        CriticalBlock block(logsect);

        if (logcache.length()&&logconn) {
            if (uncond||(msTick()-lastlog>10000))  {
                logconn->queryRoot()->setProp("@status",logcache.get());
                logconn->commit();
                lastlog = msTick();
            }
        }

    }

    void startHeartbeat(const char * op)
    {
        processedDirs = 0;
        processedFiles = 0;
        heartbeatTimer.reset(60, true, clustname.get()); // 1 minute interval
        log(true, "%s heartbeat started (interval: 1 minute)", op);
    }

    void finishHeartbeat(const char * op)
    {
        StringBuffer time;
        formatTime(time, heartbeatTimer.queryElapsedNS());
        log(true, "%s complete. Total time: %s, Total dirs: %lu, Total files: %lu", op, time.str(), processedDirs.load(), processedFiles.load());
    }

    void checkHeartbeat(const char * op)
    {
        time_t now = time(NULL);
        if (!heartbeatTimer.hasElapsed())
            return;

        unsigned elapsedMinutes = heartbeatTimer.calcElapsedMinutes();
        unsigned elapsedHours = elapsedMinutes / 60;
        unsigned elapsedDays = elapsedHours / 24;
        unsigned remainingHours = elapsedHours % 24;
        unsigned remainingMinutes = elapsedMinutes % 60;

        struct tm *utc_tm = gmtime(&now);
        char timestamp[32];
        strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", utc_tm);

        if (elapsedDays > 0)
            log(true, "%s - elapsed: %ud %uh processed: %lu dirs, %lu files (%s UTC)", op, elapsedDays, remainingHours, processedDirs.load(), processedFiles.load(), timestamp);
        else if (elapsedHours > 0)
            log(true, "%s - elapsed: %uh %um processed: %lu dirs, %lu files (%s UTC)", op, elapsedHours, remainingMinutes, processedDirs.load(), processedFiles.load(), timestamp);
        else
            log(true, "%s - elapsed: %um processed: %lu dirs, %lu files (%s UTC)", op, elapsedMinutes, processedDirs.load(), processedFiles.load(), timestamp);

        heartbeatTimer.updatePeriod();
    }

    void saveBranchToSashaPlane(const char *sashaDir, const char *name, IPropertyTree *branch)
    {
        if (!branch)
            return;
        try
        {
            branch->setProp("Cluster",clustname);
            StringBuffer filepath(sashaDir);
            addPathSepChar(filepath).append(name).append(".xml");

            StringBuffer datastr;
            toXML(branch,datastr);

            Owned<IFile> file = createIFile(filepath.str());
            Owned<IFileIO> fileIO = file->open(IFOcreate);
            if (!fileIO)
            {
                warn(filepath.str(), "Failed to create file");
                return;
            }
            fileIO->write(0, datastr.length(), datastr.str());
            fileIO->close();
            PROGLOG(LOGPFX "Saved branch %s to %s", name, filepath.str());
        }
        catch (IException *e)
        {
            StringBuffer errMsg;
            warn(name, "Error saving branch to Sasha plane: %s", e->errorMessage(errMsg).str());
            e->Release();
        }
    }

    void addBranch(IPropertyTree *root,const char *name,IPropertyTree *branch)
    {
        if (!branch)
            return;

        branch->setProp("Cluster",clustname);
        StringBuffer datastr;
        toXML(branch,datastr);

        root->addPropTree(name,createPTree(name))->setPropBin("data",datastr.length(),datastr.str());
    }

    CNewXRefManagerBase(bool _saveToPlane) : saveToPlane(_saveToPlane)
    {
        saveToPlane = getExpertOptBool("saveToDebugPlane", isContainerized());
    }

    void start(bool updateeclwatch,const char *clname)
    {
        StringBuffer xpath;
        {   // remove old tree
            Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE ,INFINITE);
            if (!conn)
                return;
            IPropertyTree *xrefroot = conn->queryRoot();
            xpath.appendf("Cluster[@name=\"%s\"]", clname);
        }
        if (updateeclwatch) {
            xpath.insert(0,"/DFU/XREF/");
            logconn.setown(querySDS().connect(xpath.str(),myProcessSession(),0 ,INFINITE));
        }
        log(false, "Starting");
    }

    void finish(bool aborted)
    {
        if (aborted)
            log(false, "Aborted");
        logconn.clear(); // final message done by save to eclwatch
    }

    IPropertyTree *createErrorWarningMessageTree()
    {
        Owned<IPropertyTree> message = createPTree("Messages");
        ForEachItemIn(i1,errors) {
            cMessage &item = errors.item(i1);
            IPropertyTree *t = message->addPropTree("Error",createPTree("Error"));
            t->addProp("File",item.lname.get());
            t->addProp("Text",item.msg.get());
        }
        ForEachItemIn(i2,warnings) {
            cMessage &item = warnings.item(i2);
            IPropertyTree *t = message->addPropTree("Warning",createPTree("Warning"));
            t->addProp("File",item.lname.get());
            t->addProp("Text",item.msg.get());
        }
        return message.getClear();
    }

    void addErrorsWarnings(IPropertyTree *croot)
    {
        Owned<IPropertyTree> message = createErrorWarningMessageTree();
        addBranch(croot,"Messages",message);
    }

    void saveToEclWatch(bool &abort,bool byscheduler)
    {
        if (abort)
            return;
        log(false,"Saving information");
        Owned<IPropertyTree> croot = createPTree("Cluster");
        croot->setProp("@name",clustname);
        if (!rootdir.isEmpty())
            croot->setProp("@rootdir",rootdir);
        CDateTime dt;
        dt.setNow();
        StringBuffer dts;
        dt.getString(dts);
        croot->setProp("@modified",dts.str());
        StringBuffer ss("Generated");
        if (byscheduler)
            ss.append(" by sasha scheduler");
        if (sfnum)
            ss.appendf("  [%d superfiles, %d subfiles]",sfnum,fnum);
        else if (fnum)
            ss.appendf("  [%d files]",fnum);
        croot->setProp("@status",ss.str());

        // Check if we should use Sasha plane for storage (configurable, default to containerized)

        // Read configuration - make this an expert option
        if (saveToPlane)
        {
            // Create directory structure: <prefix>/xref/<cluster>/<datestamp>/
            StringBuffer sashaDir;
            getLdsPath("xref", sashaDir);
            addPathSepChar(sashaDir).append(clustname);

            // Create date+time based subdirectory
            StringBuffer dateTimeDir;
            dt.getString(dateTimeDir, false);  // YYYY-MM-DDTHH:MM:SS format
            addPathSepChar(sashaDir).append(dateTimeDir);

            if (!recursiveCreateDirectory(sashaDir))
                throw makeStringExceptionV(0, LOGPFX "Failed to create directory: %s", sashaDir.str());
            PROGLOG(LOGPFX "Using Sasha storage at: %s", sashaDir.str());

            // Save branches to Sasha plane files
            saveBranchToSashaPlane(sashaDir.str(), "Orphans", orphansbranch);
            saveBranchToSashaPlane(sashaDir.str(), "Lost", lostbranch);
            saveBranchToSashaPlane(sashaDir.str(), "Found", foundbranch);
            saveBranchToSashaPlane(sashaDir.str(), "Directories", dirbranch);

            // Save Messages
            Owned<IPropertyTree> message = createErrorWarningMessageTree();
            saveBranchToSashaPlane(sashaDir.str(), "Messages", message);

            StringBuffer savePath(sashaDir);
            // Store path reference in Dali
            if (!isContainerized() && isAbsolutePath(sashaDir)) // convert to UNC path
            {
                RemoteFilename rfn;
                rfn.setLocalPath(sashaDir.str());
                rfn.getRemotePath(savePath.clear());
            }
            croot->setProp("@xrefPath", savePath);
            PROGLOG(LOGPFX "Saved XREF data to Sasha plane with path: %s", savePath.str());
        }
        else
        {
            // Use traditional Dali storage
            addBranch(croot,"Orphans",orphansbranch);
            addBranch(croot,"Lost",lostbranch);
            addBranch(croot,"Found",foundbranch);
            addBranch(croot,"Directories",dirbranch);
            addErrorsWarnings(croot);
        }

        if (abort)
            return;
        logconn.clear();
        Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE ,INFINITE);
        if (abort)
            return;
        IPropertyTree *xrefroot = conn->queryRoot();
        StringBuffer xpath;
        xpath.appendf("Cluster[@name=\"%s\"]", clustname.get());
        xrefroot->removeProp(xpath.str());
        xrefroot->addPropTree("Cluster",croot.getClear());
    }
};


class CNewXRefManager: public CNewXRefManagerBase
{
    XRefAllocator allocator;
    std::unique_ptr<cDirDesc> root;
    bool iswin = false;             // set by scanDirectories
    IpAddress *iphash = nullptr;
    unsigned *ipnum = nullptr;
    unsigned iphashsz = 0;          // set by setGroup
    IArrayOf<IPropertyTree> sorteddirs;

public:
    Owned<IGroup> grp, rawgrp;
    StringArray clusters;           // list of matching cluster (used in xref)
    StringBuffer clusterscsl;       // comma separated list of cluster (used in xref)
    unsigned numnodes = 0;          // set by setGroup
    StringArray lostfiles;
    bool verbose = true;
    unsigned numuniqnodes = 0;
    Owned<IUserDescriptor> udesc;
    Linked<IPropertyTree> storagePlane;
    bool isPlaneStriped = false;
    unsigned numStripedDevices = 1;
    bool filterScopesEnabled = false;
    StringArray scopeFilters;

    CNewXRefManager(IPropertyTree *plane, unsigned maxMb, bool saveToPlane, const char *_scopeFilters)
        : CNewXRefManagerBase(saveToPlane), allocator(maxMb)
    {
        root.reset(cDirDesc::create("", &allocator));
        foundbranch.setown(createPTree("Found"));
        lostbranch.setown(createPTree("Lost"));
        orphansbranch.setown(createPTree("Orphans"));
        dirbranch.setown(createPTree("Directories"));
        log(false, "Max memory = %d MB", maxMb);

        StringBuffer userName;
        serverConfig->getProp("@user", userName);
        if (userName.isEmpty()) // for backward compatibility
            serverConfig->getProp("@sashaUser", userName);
        udesc.setown(createUserDescriptor());
        udesc->set(userName.str(), nullptr);

        if (plane)
        {
            storagePlane.set(plane);
            unsigned numDevices = storagePlane->getPropInt("@numDevices", 1);
            isPlaneStriped = !storagePlane->hasProp("@hostGroup") && (numDevices>1);
            numStripedDevices = isPlaneStriped ? numDevices : 1;
        }

        if (!isEmptyString(_scopeFilters))
        {
            log(false, "Filter Scopes Enabled: searching for files in: %s", _scopeFilters);
            filterScopesEnabled = true;
            scopeFilters.appendList(_scopeFilters, ",");
        }
    }

    ~CNewXRefManager()
    {
        if (iphash)
            delete [] iphash;
        delete [] ipnum;
    }


    void start(bool updateeclwatch)
    {
        CNewXRefManagerBase::start(updateeclwatch,clustname);
    }



    void addIpHash(const IpAddress &ip,unsigned n)
    {
        unsigned r;
        _cpyrev4(&r,&ip);
        unsigned h = hashc((const byte *)&r,sizeof(r),0)%iphashsz;
        while (!iphash[h].isNull())
            if (++h==iphashsz)
                h = 0;
        iphash[h] = ip;
        ipnum[h] = n;
    }

    unsigned checkIpHash(const IpAddress &ip)
    {
        unsigned r;
        _cpyrev4(&r,&ip);
        unsigned h = hashc((const byte *)&r,sizeof(r),0)%iphashsz;
        while (!iphash[h].isNull()) {
            if (iphash[h].ipequals(ip))
                return ipnum[h];
            if (++h==iphashsz)
                h = 0;
        }
        return NotFound;
    }


    bool setGroup(const char *_clustname,const char *_grpname, IArrayOf<IGroup> &done, StringArray &donedir)
    {
        StringBuffer cluststr(_clustname);
        //cluststr.toLowerCase(); do not lower case
        clustname.set(cluststr);
        StringBuffer grpstr;
        StringBuffer range;
        if (!decodeChildGroupName(_grpname,grpstr, range))
            grpstr.append(_grpname);
        grpstr.toLowerCase();
        StringAttr grpname(grpstr.str());
        StringBuffer basedir;
        GroupType groupType;
        grp.setown(queryNamedGroupStore().lookup(grpstr.str(), basedir, groupType));
        if (!grp) {
            OERRLOG(LOGPFX "Cluster %s node group %s not found",clustname.get(),grpstr.str());
            return false;
        }
        // Group overlap doesn't apply to containerized. Should be cleaned up once planes are fully integrated
        if (!isContainerized()) {
            ForEachItemIn(i1,done) {
                GroupRelation gr = done.item(i1).compare(grp);
                if ((gr==GRidentical)||(gr==GRsubset)) {
                    if (strcmp(basedir.str(),donedir.item(i1))==0) {
                        OWARNLOG(LOGPFX "Node group %s already done",grpstr.str());
                        return false;
                    }
                }
            }
        }
        done.append(*LINK(grp));
        donedir.append(basedir.str());
        numnodes = grp->ordinality();
        // lets add HT for grp
        delete [] iphash;
        iphash = NULL;
        delete [] ipnum;
        iphashsz = numnodes*2;
        iphash = new IpAddress[iphashsz];
        ipnum = new unsigned[iphashsz];
        SocketEndpointArray deduppedEps;
        ForEachNodeInGroup(i,*grp) {
            const SocketEndpoint &ep = grp->queryNode(i).endpoint();
            if (ep.port!=0)
                OWARNLOG(LOGPFX "Group has ports!");
            // check port 0 TBD
            if (NotFound == checkIpHash(ep)) {
                addIpHash(ep,i);
                deduppedEps.append(ep);
            }
        }
        rawgrp.setown(createIGroup(deduppedEps));
        numuniqnodes = rawgrp->ordinality();
        clusters.kill();
        clusterscsl.clear().append(grpstr);
        clusters.append(grpstr.str());
        if (!isContainerized()) {
            // This code is locating all other groups that are a subset of the group being scanned.
            // and builds this list up into 'clustercsl', which is used when XREF identifies a lost
            // file, tagging them with this list, as possible candidate clusters it may be part of.
            Owned<INamedGroupIterator> giter = queryNamedGroupStore().getIterator(rawgrp,false);
            StringBuffer gname;
            ForEach(*giter) {
                giter->get(gname.clear());
                if (strcmp(grpname,gname.str())!=0) {
                    clusters.append(gname.str());
                    clusterscsl.append(',').append(gname.str());
                }
            }
            // add the first IP also
            rawgrp->queryNode(0).endpoint().getHostText(gname.clear());
            clusters.append(gname.str());
            clusterscsl.append(',').append(gname.str());
        }
        if (isContainerized()) {
            Owned<const IPropertyTree> plane = getStoragePlaneConfig(_clustname, true);
            rootdir.set(plane->queryProp("@prefix"));
        }
        else if (basedir.length()==0) {
            const char *ddir = "thor";
            const char *rdir = "thor";
            StringBuffer datadir;
            StringBuffer repdir;
            if (getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"data","thor",_clustname,datadir))
                ddir = datadir.str();
            if (getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"mirror","thor",_clustname,repdir))
                rdir = repdir.str();
            iswin = grp->ordinality()?(getDaliServixOs(grp->queryNode(0).endpoint())==DAFS_OSwindows):false;
            setBaseDirectory(ddir,0,iswin?DFD_OSwindows:DFD_OSunix);
            setBaseDirectory(rdir,1,iswin?DFD_OSwindows:DFD_OSunix);
            rootdir.set(queryBaseDirectory(grp_unknown, 0, iswin?DFD_OSwindows:DFD_OSunix));
        }
        else {
            rootdir.set(basedir);
            iswin = getPathSepChar(rootdir.get())=='\\';
        }
        assertex(!rootdir.isEmpty());
        return true;
    }


    void clear()
    {
        root.reset(cDirDesc::create("", &allocator));
    }

    cDirDesc *findDirectory(const char *name)
    {
        if (stricmp(name,rootdir)==0)
            return root.get();
        if (!*name)
            return NULL;
        StringBuffer pdir;
        const char *tail = splitDirTail(name,pdir);
        size32_t dl = pdir.length();
        if (dl&&isPathSepChar(pdir.charAt(dl-1)))
            pdir.setLength(--dl);
        cDirDesc *p = findDirectory(pdir.str());
        if (!p)
            return NULL;
        // When the cDirDesc hierarchy is built, stripe directories are excluded from the path (see scanDirectories' casyncfor::Do),
        // so prevent incorrect traversal into striped directory structures and return root instead
        if (isPlaneStriped&&p==root.get()&&tail[0]=='d'&&readDigits(tail+1)!=0)
            return p;
        return p->lookupDirNonThreadSafe(tail, nullptr);
    }

    bool dirFiltered(const char *filename)
    {
        // TBD (e.g. collections)
        return false;
    }

    bool fileFiltered(const char *filename,const CDateTime &dt)
    {
        if (!filename||!*filename)
            return true;
        const char *tail=pathTail(filename);
        if (tail&&(memicmp(tail,"backup",6)==0)) {
            size32_t sz = strlen(tail);
            if (sz>10) {
                if (strcmp(tail+sz-4,".lst")==0)
                    return true;
                if (strcmp(tail+sz-4,".log")==0)
                    return true;
            }
        }
        return false;
    }

    // Helper function to check if a scope matches a filter
    // matchType: 0 = partial match (for directories), 1 = full match (for files)
    bool checkScopeMatchesFilter(const char *path, bool allowPartialMatch)
    {
        if (!filterScopesEnabled)
            return true;

        ForEachItemIn(i, scopeFilters)
        {
            const char *filter = scopeFilters.item(i);
            const char *scope = path;

            while (true)
            {
                const char *filterSep = strstr(filter, "::");
                const char *scopeSep = strstr(scope, "/");

                if (filterSep && scopeSep)
                {
                    // Common Case: Both filter and scope have more subscopes, check that they match so far
                    size_t filterLen = filterSep - filter;
                    if (filterLen != (scopeSep - scope) || strncmp(filter, scope, filterLen) != 0)
                        break;

                    filter = filterSep + 2;
                    scope = scopeSep + 1;
                }
                else if (filterSep)
                {
                    // Filter has more subscopes than scope
                    if (allowPartialMatch)
                    {
                        if (strncmp(filter, scope, filterSep - filter) == 0)
                            return true;
                    }
                    // For full match: filter is longer, no match
                    break;
                }
                else if (scopeSep)
                {
                    // Scope has more subscopes than filter
                    if (!isEmptyString(filter) && strncmp(filter, scope, scopeSep - scope) != 0)
                        break;

                    // Check if remaining scope is a dir-per-part directory
                    scope = scopeSep + 1;
                    if (isContainerized() && strchr(scope, '/') == nullptr && readDigits(scope) != 0)
                        return true;

                    break;
                }
                else
                {
                    // Both filter and scope are out of subscopes, check final match
                    if (isEmptyString(filter))
                    {
                        if (isEmptyString(scope))
                            return true;

                        // Check if remaining scope is a dir-per-part directory
                        if (isContainerized() && readDigits(scope) != 0)
                            return true;
                    }
                    else if (streq(filter, scope))
                        return true;

                    break;
                }
            }
        }

        return false;
    }

    // Checks that a scope matches the beginning of a filter
    // Returns true if a partial scope matches any filter, false otherwise
    bool partialScopeMatchesFilter(const char *path)
    {
        return checkScopeMatchesFilter(path, true);
    }

    // Checks that a logical file scope matches the filter (scope does not include filename)
    // Returns true if the full scope matches any filter, false otherwise
    bool fullScopeMatchesFilter(const char *path)
    {
        return checkScopeMatchesFilter(path, false);
    }

    bool scanDirectory(unsigned node, const SocketEndpoint &ep, StringBuffer &path, unsigned drv, cDirDesc *pdir, IFile *cachefile, unsigned filePathOffset, unsigned stripeNum, cDirDesc *parent, offset_t &parentScopeSz)
    {
        checkHeartbeat("Directory scan");
        size32_t dsz = path.length();
        if (pdir==NULL)
            pdir = root.get();
        RemoteFilename rfn;
        rfn.setPath(ep,path.str());
        Owned<IFile> file;
        if (cachefile)
            file.set(cachefile);
        else
            file.setown(createIFile(rfn));
        Owned<IDirectoryIterator> iter;
        Owned<IException> e;
        try {
            iter.setown(file->directoryFiles(NULL,false,true));
        }
        catch (IException *_e) {
            e.setown(_e);
        }
        if (e) {
            StringBuffer tmp(LOGPFX "scanDirectory ");
            rfn.getRemotePath(tmp);
            EXCLOG(e,tmp.str());
            return false;
        }
        StringBuffer fname;
        offset_t scopeSz = 0;
        StringArray dirs;
        bool scanFiles = fullScopeMatchesFilter(path.str()+filePathOffset+1);
        ForEach(*iter) {
            iter->getName(fname.clear());
            if (iswin)
                fname.toLowerCase();
            addPathSepChar(path).append(fname);
            if (iter->isDir())
            {
                if (partialScopeMatchesFilter(path.str()+filePathOffset+1))
                    dirs.append(fname.str());
            }
            else if (scanFiles) {
                CDateTime dt;
                offset_t filesz = iter->getFileSize();
                iter->getModifiedTime(dt);
                if (!fileFiltered(path.str(),dt)) {
                    try {
                        unsigned numParts;    // num parts
                        unsigned partNum;     // part num
                        unsigned filenameLen; // length of file name excluding extension i.e. ._$P$_of_$N$
                        StringAttr mask;
                        const char *fn = cDirDesc::decodeName(drv,fname,node,numnodes,mask,partNum,numParts,filenameLen);
                        bool misplaced = cDirDesc::isMisplaced(partNum,numParts,ep,*grp,path,filePathOffset,stripeNum,numStripedDevices);

                        cFileDesc *file = nullptr;
                        bool addToParent = false;
                        if (isContainerized()&&parent&&!misplaced) // misplaced files should not be candidates for dir-per-part logic
                        {
                            if (pdir->isDirPerPartCandidate())
                            {
                                if (pdir->isDirPerPartMatch(partNum))
                                {
                                    // In a containerized deployment, a dir-per-part is on by default, and this branch
                                    // is expected to be the normal/common case, all other branches are exceptions.
                                    file = pdir->getFile(fn);
                                    if (file && !file->isDirPerPart)
                                    {
                                        // If the current file part matched the directory name and a non-dir-per-part file
                                        // was found in the current directory, likely this whole file is not a dir-per-part
                                        // file, and this part is the only match.
                                        scopeSz += filesz;
                                    }
                                    else
                                    {
                                        // This is still in a directory that looks like a dir-per-part, and the file was not found
                                        // so create it in the parent (above the dir-per-part directory), i.e. effectively ignoring
                                        // the dir-per-part dir itself.
                                        file = parent->ensureFile(fn, numParts, true, filenameLen, &allocator);
                                        parentScopeSz += filesz;
                                        addToParent = true;
                                    }
                                }
                                else // we are in a dir-per-part directory, but part doesn't look like it belongs in a dir-per-part structure
                                {
                                    // Not a dir-per-part file, check for previously moved file
                                    CLeavableCriticalBlock parentBlock(parent->filesCrit);
                                    auto it = parent->files.find(fn);
                                    if (it != parent->files.end() && it->second->isDirPerPart)
                                    {
                                        // Move file to current directory
                                        it->second->isDirPerPart = false;
                                        file = it->second.release();
                                        parent->files.erase(it);
                                        parentScopeSz -= filesz;

                                        parentBlock.leave();

                                        pdir->addExistingFile(fn, file);
                                        scopeSz += filesz; // moved file - MORE this isn't really correct
                                    }
                                    else
                                    {
                                        // No previously moved file and no dir-per-part. Create in current directory
                                        parentBlock.leave();

                                        file = pdir->ensureFile(fn, numParts, false, filenameLen, &allocator);

                                    }
                                    scopeSz += filesz;
                                }
                            }
                            else
                            {
                                file = pdir->ensureFile(fn, numParts, false, filenameLen, &allocator);
                                scopeSz += filesz;
                            }
                        }
                        else
                        {
                            file = pdir->ensureFile(fn, numParts, false, filenameLen, &allocator);
                            scopeSz += filesz;
                        }

                        cDirDesc *currentOrParent = addToParent ? parent : pdir;
                        currentOrParent->setMisplacedAndPresent(file, misplaced, partNum, drv, path, node, numnodes);

                        processedFiles++;
                    }
                    catch (IException *e) {
                        StringBuffer filepath, errMsg;
                        addPathSepChar(rfn.getRemotePath(filepath)).append(fname);
                        error(filepath.str(), "scanDirectory Error adding file : %s", e->errorMessage(errMsg).str());
                        e->Release();
                    }
                }
            }
            path.setLength(dsz);
        }
        iter.clear();
        ForEachItemIn(i,dirs) {
            addPathSepChar(path).append(dirs.item(i));
            if (file.get()&&!resetRemoteFilename(file,path.str())) // sneaky way of avoiding cache
                file.clear();
            if (!scanDirectory(node,ep,path,drv,pdir->lookupDir(dirs.item(i),&allocator),file,filePathOffset,stripeNum,pdir,scopeSz))
                return false;
            path.setLength(dsz);
        }

        pdir->addNodeStats(node,drv,scopeSz);
        processedDirs++;

        return true;
    }

    bool scanDirectories(bool &abort, unsigned numThreads)
    {
        class casyncfor: public CAsyncFor
        {
            CNewXRefManager &parent;
            const char *rootdir;
            unsigned n;
            unsigned r;
            bool &abort;
        public:
            bool ok;
            casyncfor(CNewXRefManager &_parent,const char *_rootdir,bool &_abort)
                : parent(_parent), abort(_abort)
            {
                rootdir = _rootdir;
                n = parent.numuniqnodes;
                r = (n+1)/2;
                ok = true;
            }
            void Do(unsigned i)
            {
                /* NB: Threading behavior depends on deployment type and cluster configuration:
                *
                * Bare-metal deployments:
                *   - Multi-threaded: Each thread scans a different node (i = node number)
                *   - Single-threaded: When only one node is available
                *
                * Containerized deployments:
                *   - Striped planes: Multi-threaded, each thread scans a different stripe directory (i = stripe number)
                *   - Non-striped planes: Single-threaded scan
                *
                * @param i Thread index - represents either node number (bare-metal) or stripe number (containerized)
                */
                if (!ok||abort)
                    return;

                StringBuffer path(rootdir);
                if (parent.isPlaneStriped)
                {
                    assertex(!parent.storagePlane->hasProp("@hostGroup"));

                    // A hosted plane will never be striped, so for striped planes, use local host
                    SocketEndpoint localEP;
                    localEP.setLocalHost(0);
                    // Add stripe directory to path so each thread scans a different stripe directory
                    addPathSepChar(path).append('d').append(i+1);

                    parent.log(false,"Scanning %s directory %s",parent.storagePlane->queryProp("@name"),path.str());
                    offset_t rootsz = 0;
                    if (!parent.scanDirectory(0,localEP,path,0,parent.root.get(),NULL,path.length(),i+1,nullptr,rootsz))
                    {
                        ok = false;
                        return;
                    }
                }
                else
                {
                    StringBuffer hostStr;
                    SocketEndpoint ep = parent.rawgrp->queryNode(i).endpoint();
                    parent.log(false,"Scanning %s directory %s",ep.getEndpointHostText(hostStr).str(),path.str());
                    offset_t rootsz = 0;
                    if (!parent.scanDirectory(i,ep,path,0,NULL,NULL,path.length(),0,nullptr,rootsz)) {
                        ok = false;
                        return;
                    }
                    if (!isContainerized()) {
                        // MORE: If containerized, a hosted plane may still have replication
                        i = (i+r)%n;
                        setReplicateFilename(path,1);
                        ep = parent.rawgrp->queryNode(i).endpoint();
                        rootsz = 0;
                        parent.log(false,"Scanning %s directory %s",ep.getEndpointHostText(hostStr.clear()).str(),path.str());
                        if (!parent.scanDirectory(i,ep,path,1,NULL,NULL,path.length(),0,nullptr,rootsz)) {
                            ok = false;
                        }
                    }
                }
    //             PROGLOG("Done %i - %d used",i,parent.mem.maxallocated());
            }
        } afor(*this,rootdir,abort);
        unsigned numMaxThreads = 0;
        if (isPlaneStriped)
            numMaxThreads = numStripedDevices;
        else
            numMaxThreads = numuniqnodes;
        if (numThreads > numMaxThreads)
            numThreads = numMaxThreads;
        startHeartbeat("Directory scan"); // Initialize heartbeat mechanism
        afor.For(numMaxThreads,numThreads,true,numThreads>1);
        if (afor.ok)
            finishHeartbeat("Directory scan");
        else
            log(true,"Errors occurred during scan");
        return afor.ok;
    }

    void scanLogicalFiles(bool &abort)
    {
        if (!grp||abort)
            return;
        class cfilescan1 : public CSDSFileScanner
        {
            Owned<IRemoteConnection> conn;
            CNewXRefManager &parent;
            bool &abort;

            bool checkFileOk(IPropertyTree &file,const char *filename)
            {
                if (abort)
                    return false;
                StringArray groups;
                getFileGroups(&file,groups);
                if (groups.ordinality()==0) {
                    parent.error(filename,"File has no group defined");
                    return false;
                }
                ForEachItemIn(i,groups) {
                    ForEachItemIn(j,parent.clusters) {
                        if (strcmp(parent.clusters.item(j),groups.item(i))==0) {
//                          if (j!=0)
//                              OWARNLOG("DANXREF(scanFiles):  %s has alt group %s",filename,parent.clusters.item(i));
                            return true;
                        }
                    }
                }
                return false;
            }

            bool checkScopeOk(const char *scopename)
            {
                return !abort;
            }

            bool logicalFileFiltered(StringBuffer &logicalFileName)
            {
                if (!parent.filterScopesEnabled)
                    return false;

                ForEachItemIn(i,parent.scopeFilters)
                {
                    const char *fileScope = logicalFileName.str();
                    const char *fileScopeEnd = fileScope + logicalFileName.length();
                    while (true)
                    {
                        if (fileScopeEnd == fileScope)
                        {
                            for (;i<numItemsi;i++)
                            {
                                if (isEmptyString(parent.scopeFilters.item(i)))
                                    return false;
                            }
                            return true;
                        }
                        else if (fileScopeEnd[0] == ':' && fileScopeEnd[1] == ':')
                            break;
                        else
                            fileScopeEnd--;
                    }
                    if (strncmp(fileScope, parent.scopeFilters.item(i), fileScopeEnd - fileScope) == 0)
                        return false;
                }
                return true;
            }

            void processFile(IPropertyTree &file,StringBuffer &name)
            {
                if (abort)
                    return;
                if (logicalFileFiltered(name))
                    return;
                parent.log(false,"Process file %s",name.str());
                parent.fnum++;

                Owned<IFileDescriptor> fdesc;
                try {
                    fdesc.setown(deserializeFileDescriptorTree(&file,&queryNamedGroupStore()));
                }
                catch (IException *e) {
                    EXCLOG(e,"processFile");
                    e->Release();
                }
                if (fdesc) {
                    unsigned np = fdesc->numParts();
                    if (np==0) {
                        parent.error(name.str(),"File has no parts");
                        return;
                    }
                    bool checkzport = true;
                    StringBuffer fn;
                    StringBuffer dir;
                    StringBuffer lastdir;
                    cDirDesc *pdir = NULL;
                    bool islost = false;
                    bool incluster = true;
                    for (unsigned p=0;p<np;p++) {
                        if (abort)
                            return;
                        unsigned matched = 0;
                        unsigned nc = fdesc->numCopies(p);
                        if (nc==0)
                            continue;   // ignore if no parts
                        for (unsigned c=0;c<nc;c++) {
                            RemoteFilename rfn;
                            fdesc->getFilename(p,c,rfn);
                            const SocketEndpoint &ep = rfn.queryEndpoint();
                            if (checkzport&&ep.port) {
                                parent.error(name.str(),"File has non-zero port");
                                checkzport = false;
                            }
                            unsigned nn = parent.checkIpHash(ep);
                            if (nn!=NotFound) {
                                rfn.getLocalPath(fn.clear());
                                const char *tail = splitDirTail(fn.str(),dir.clear());
                                if (dir.length()&&isPathSepChar(dir.charAt(dir.length()-1)))
                                    dir.setLength(dir.length()-1);
                                unsigned drv = isContainerized() ? 0 : getPathDrive(dir.str()); // should match c
                                if (drv)
                                    setReplicateFilename(dir,0);
                                if ((lastdir.length()==0)||(strcmp(lastdir.str(),dir.str())!=0)) {
                                    pdir = parent.findDirectory(dir.str());
                                    lastdir.clear().append(dir);
                                }
                                if (pdir&&pdir->markFile(drv,tail,nn,ep,*parent.grp,parent.numnodes)) {
                                    matched++;
                                }
                            }
                            else if (p==0) { // skip file
                                if (parent.verbose)
                                    PROGLOG(LOGPFX "ignoring file %s",name.str());
                                p = np;
                                incluster = false;
                                break;
                            }
                        }
                        if (!matched&&incluster)
                            islost = true;
                    }
                    if (islost) {
                        if (parent.verbose)
                            PROGLOG(LOGPFX "Potential lost file: %s",name.str());
                        parent.lostfiles.append(name.str());
                    }
                }
                else {
                    parent.error(name.str(),"cannot create file descriptor");
                }
            }
        public:

            cfilescan1(CNewXRefManager &_parent,bool &_abort)
                : parent(_parent), abort(_abort)
            {
            }

            ~cfilescan1()
            {
            }

            void scan()
            {
                if (abort)
                    return;
                conn.setown(querySDS().connect("/Files", myProcessSession(), 0, 100000));
                if (!conn||abort)
                    return;
                CSDSFileScanner::scan(conn);
            }

        } filescan(*this,abort);

        filescan.scan();
        log(true,"File scan complete");

    }

    bool checkOrphanPhysicalFile(RemoteFilename &rfn,offset_t &sz,CDateTime &dt)
    {
        try {
            Owned<IFile> file = createIFile(rfn);
            bool isdir;
            bool ret = false;
            if (file->getInfo(isdir,sz,dt)&&!isdir)
                ret = true;
#ifdef _DEBUG
            StringBuffer dbgname;
            rfn.getPath(dbgname);
            PROGLOG("checkOrphanPhysicalFile(%s) = %s",dbgname.str(),ret?"true":"false");
#endif
            return ret;
        }
        catch (IException *e) {
            StringBuffer tmp(LOGPFX "listOrphans reading ");
            rfn.getRemotePath(tmp);
            EXCLOG(e,tmp.str());
            e->Release();
        }
        return false;
    }

    void addOrphanPartNode(Owned<IPropertyTree> &branch,const SocketEndpoint &ep,unsigned i,bool rep)
    {
        if (!branch)
            branch.setown(createPTree("File"));
        i++;
        StringBuffer tmp;
        tmp.appendf("Part[Num=\"%d\"]",i);
        IPropertyTree* pb = branch->queryPropTree(tmp.str());
        if (!pb) {
            pb = createPTree("Part");
            pb->setPropInt("Num",i);
            pb = branch->addPropTree("Part",pb);
        }
        pb->setProp(rep?"RNode":"Node",ep.getEndpointHostText(tmp.clear()).str());
    }

    void addExternalFoundFile(cFileDesc *file, const char *currentPath, unsigned int recentCutoffDays)
    {
        // Treat external files as a single file because without a partmask there is no way
        // to determine the number of parts or the part number
        StringBuffer filePath(currentPath);
        file->getNameMask(addPathSepChar(filePath));

        // Files that are non-conforming will be marked as misplaced in the directory scan
        unsigned node = 0;
        assertex(file->misplaced);
        node = file->misplaced->nn;

        SocketEndpoint ep = grp->queryNode(node).endpoint();
        RemoteFilename rfn;
        rfn.setPath(ep, filePath.str());
        offset_t sz;
        CDateTime dt;
        bool found;
        {
            CheckTime ct("checkOrphanPhysicalFile ");
            found = checkOrphanPhysicalFile(rfn,sz,dt);
            if (ct.slow())
                ct.appendMsg(filePath.str());
        }
        if (found)
        {
            CDateTime now;
            now.setNow();
            CDateTime co(dt);
            co.adjustTime(recentCutoffDays*60*24);
            if (co.compare(now)>=0) {
                warn(filePath.str(),"Recent external file ignored");
                return;
            }

            StringBuffer tmp;
            Owned<IPropertyTree> branch;
            addOrphanPartNode(branch,rfn.queryEndpoint(),0,false);
            branch->setPropInt64("Size",sz);
            branch->setProp("Partmask",filePath.str());
            branch->setProp("Modified",dt.getString(tmp.clear()));
            branch->setPropInt("Numparts",1);
            branch->setPropInt("Partsfound",1);
            foundbranch->addPropTree("File",branch.getClear());
        }
    }

    void listOrphans(cFileDesc *f,const char *currentPath,const char *currentScope,bool &abort,unsigned int recentCutoffDays)
    {
        if (abort)
            return;
        if (!f)
            return;
        // first check if any orhans at all (maybe could do this faster)
#ifdef _DEBUG
        StringBuffer dbgname;
        f->getNameMask(dbgname);
        PROGLOG("listOrphans TEST FILE(%s)",dbgname.str());
#endif

        // Non-conforming files won't have a part mask and should be treated as single files
        if (!f->isHPCCFile()) {
            addExternalFoundFile(f,currentPath,recentCutoffDays);
            return;
        }

        unsigned drv;
        unsigned drvs = isContainerized() ? 1 : 2;
        for (drv=0;drv<drvs;drv++) {
            unsigned i0;
            for (i0=0;i0<f->N;i0++)
                if (f->testpresent(drv,i0)&&!f->testmarked(drv,i0))
                    break;
            if (i0<f->N)
                break;
        }
        if (drv==drvs)
            return; // no orphans
        StringBuffer mask;
        StringBuffer scopeBuf(currentScope);
        scopeBuf.append("::");
        f->getNameMask(mask);
        assertex(f->getName(scopeBuf)); // Should always return true for HPCC files
        // orphans are only orphans if there doesn't exist a valid file
        try {
            if (queryDistributedFileDirectory().exists(scopeBuf.str(),udesc,true,false)) {
                warn(mask.str(),"Orphans ignored as %s exists",scopeBuf.str());
                return;
            }
        }
        catch (IException *e) {
            EXCLOG(e,"CNewXRefManager::listOrphans");
            e->Release();
            return;
        }
        // treat drive differently for orphans (bit silly but bward compatible
        MemoryAttr buf;
        bool *completed = (bool *)buf.allocate(f->N);
        memset(completed,0,f->N);
        Owned<IPropertyTree> branch[2];
        CDateTime mostrecent[2];
        offset_t totsize[2];
        totsize[0] = 0;
        totsize[1] = 0;
        unsigned ndone[2];
        ndone[0] = 0;
        ndone[1] = 0;
        unsigned fnameHash = getFilenameHash(scopeBuf.str());
        const char * prefix = storagePlane->queryProp("@prefix");
        for (drv=0;drv<drvs;drv++) {
            if (abort)
                return;
            bool warnnotexists = true;
            StringBuffer path;
            if (drv)
                setReplicateFilename(addPathSepChar(path.append(currentPath)),drv);
            size32_t psz = path.length();
            StringBuffer tmp;
            for (unsigned pn=0;pn<f->N;pn++) {
                if (f->testpresent(drv,pn)&&!f->testmarked(drv,pn)) {
                    RemoteFilename rfn;
                    constructPartFilename(grp, pn+1, drv, f->N, fnameHash, drv, f->isDirPerPart, scopeBuf.str(), prefix, mask.str(), numStripedDevices, rfn);
                    offset_t sz;
                    CDateTime dt;
                    bool found;
                    {
                        CheckTime ct("checkOrphanPhysicalFile ");
                        found = checkOrphanPhysicalFile(rfn,sz,dt);
                        if (ct.slow()) {
                            rfn.getPath(tmp.clear());
                            ct.appendMsg(tmp.str());
                        }
                    }
                    if (found) {
                        if (mostrecent[drv].isNull()||(dt.compare(mostrecent[drv],false)>0))
                            mostrecent[drv].set(dt);
                        completed[pn] = true;
                        totsize[drv] += sz;
                        ndone[drv]++;
                        addOrphanPartNode(branch[drv],rfn.queryEndpoint(),pn,drv!=0);
                    }
                    else if (warnnotexists) {
                        rfn.getRemotePath(tmp.clear());
                        tmp.append(" no longer exists");
                        warn(tmp.str(),"Orphan file no longer exists");
                        warnnotexists = false;
                    }
                }
                path.setLength(psz);
            }
        }
        if (abort)
            return;
        // check if complete here
        unsigned ncomplete = 0;
        for (unsigned i=0;i<f->N;i++)
            if (completed[i])
                ncomplete++;
        if (ncomplete!=f->N) {  // if a found file ignore misplaces
            cMisplacedRec *mp = f->misplaced;
            bool warnnotexists = true;
            while (mp) {
                if (abort)
                    return;
                if (!mp->marked) {
                    unsigned drv = mp->getDrv(numnodes);
                    RemoteFilename rfn;
                    constructPartFilename(grp, mp->pn, drv, f->N, fnameHash, drv, f->isDirPerPart, scopeBuf.str(), prefix, mask.str(), numStripedDevices, rfn);
                    offset_t sz;
                    CDateTime dt;
                    if (checkOrphanPhysicalFile(rfn,sz,dt)) {
                        if (mostrecent[drv].isNull()||(dt.compare(mostrecent[drv],false)>0))
                            mostrecent[drv].set(dt);
                        totsize[drv] += sz;
                        ndone[drv]++;
                        addOrphanPartNode(branch[drv],rfn.queryEndpoint(),mp->pn,drv!=0);
                    }
                    else if (verbose&&warnnotexists) {
                        StringBuffer tmp;
                        rfn.getRemotePath(tmp);
                        warn(tmp.str(),"Orphan file no longer exists");
                        warnnotexists = false;
                    }
                }
                mp = mp->next;
            }
        }
        CDateTime now;
        now.setNow();
        StringBuffer tmp;
        for (drv=0;drv<drvs;drv++) {
            if (abort)
                return;
            if (branch[drv]) {
                addPathSepChar(tmp.clear().append(currentPath)).append(mask.str());
                if (drv)
                    setReplicateFilename(tmp,1);
                CDateTime co(mostrecent[drv]);
                co.adjustTime(recentCutoffDays*60*24);
                if (co.compare(now)>=0) {
                    warn(tmp.str(),"Recent orphans ignored");
                    branch[drv].clear();
                    continue;
                }
                branch[drv]->setProp("Partmask",tmp.str());
                branch[drv]->setPropInt64("Size",totsize[drv]);
                branch[drv]->setProp("Modified",mostrecent[drv].getString(tmp.clear()));
                branch[drv]->setPropInt("Numparts",f->N);
                branch[drv]->setPropBool("IsDirPerPart", f->isDirPerPart);
            }
            if (ncomplete!=f->N) {
                if (branch[drv])
                    branch[drv]->setPropInt("Partsfound",ndone[drv]);
            }
        }
        if (ncomplete!=f->N) {
            if (branch[0])
                orphansbranch->addPropTree("File",branch[0].getClear());
            if (branch[1])
                orphansbranch->addPropTree("File",branch[1].getClear());
        }
        else {
            if (branch[0]) {
                if (branch[1]) {
                    StringBuffer xpath;
                    StringBuffer ips;
                    for (unsigned i=0;i<f->N;i++) {
                        xpath.clear().appendf("Part[Num=\"%d\"]/RNode[1]",i+1);
                        if (branch[1]->getProp(xpath.str(),tmp.clear())) {
                            SocketEndpoint ep(tmp.str());
                            addOrphanPartNode(branch[0],ep,i,true);
                        }
                    }
                }
                foundbranch->addPropTree("File",branch[0].getClear());
            }
            else if (branch[1])
                foundbranch->addPropTree("File",branch[1].getClear());
        }
    }

    void listDirectory(cDirDesc *d,const char *name,bool &abort)
    {
        unsigned drvs = isContainerized() ? 1 : 2;
        for (unsigned drv=0;drv<drvs;drv++) {
            if (abort)
                return;
            if (!d->empty(drv)) {
                Owned<IPropertyTree> dt = createPTree("Directory");
                if (drv) {
                    StringBuffer tmp(name);
                    setReplicateFilename(tmp,drv);
                    dt->addProp("Name",tmp.str());
                }
                else
                    dt->addProp("Name",name);
                dt->addPropInt("Num",d->files.size());
                dt->addPropInt64("Size",d->totalsize[drv]);
                if (d->totalsize[drv]) {
                    StringBuffer s1;
                    if (d->maxnode[drv]) {
                        dt->addPropInt64("MaxSize",d->maxsize[drv]);
                        grp->queryNode(d->maxnode[drv]-1).endpoint().getHostText(s1);
                        dt->addProp("MaxIP",s1.str());
                    }
                    if (d->minnode[drv]) {
                        dt->addPropInt64("MinSize",d->minsize[drv]);
                        grp->queryNode(d->minnode[drv]-1).endpoint().getHostText(s1.clear());
                        dt->addProp("MinIP",s1.str());
                    }
                    if (d->minsize[drv]<d->maxsize[drv]) {
                        __int64 av = d->totalsize[drv]/(__int64)rawgrp->ordinality();
                        if (av) {
                            unsigned pcp = (unsigned)(d->maxsize[drv]*100/av);
                            unsigned pcn = (unsigned)(d->minsize[drv]*100/av);
                            if ((pcp>100)||(pcn<100)) {
                                s1.clear().appendf("+%d%%/-%d%%",pcp-100,100-pcn);
                                dt->addProp("Skew",s1.str());
                            }
                        }
                    }
                }
                sorteddirs.append(*dt.getClear());
            }
        }
    }

    void listOrphans(cDirDesc *d,StringBuffer &basedir,StringBuffer &scope,bool &abort,unsigned int recentCutoffDays)
    {
        checkHeartbeat("Orphan scan");
        if (abort)
            return;
        if (!d) {
            d = root.get();
            if (!d)
                return;
            basedir.append(rootdir);
        }
#ifdef _DEBUG
        StringBuffer dbgname;
        d->getName(dbgname);
        PROGLOG("listOrphans TEST DIR(%s)",dbgname.str());
#endif
        size32_t bds = basedir.length();
        size32_t scopeLen = scope.length();
        if (bds!=0)
            addPathSepChar(basedir);
        if (scopeLen!=0)
            scope.append("::");
        d->getName(basedir);
        d->getName(scope);
        listDirectory(d,basedir.str(),abort);

        for (auto& dirPair : d->dirs) {
            cDirDesc *dir = dirPair.second.get();
            listOrphans(dir,basedir,scope,abort,recentCutoffDays);
            if (abort)
                return;
        }

        for (auto& filePair : d->files) {
            cFileDesc *file = filePair.second.get();
            try {
                listOrphans(file,basedir,scope,abort,recentCutoffDays);
            }
            catch (IException *e) {
                StringBuffer filepath, errMsg;
                file->getNameMask(addPathSepChar(filepath.append(basedir)));
                error(filepath.str(),"listOrphans Error processing file : %s",e->errorMessage(errMsg).str());
                e->Release();
            }
            processedFiles++;
            if (abort)
                return;
        }
        basedir.setLength(bds);
        scope.setLength(scopeLen);
        processedDirs++;
    }

    static int compareDirs(IInterface * const *t1,IInterface * const *t2)
    {
        IPropertyTree *pt1 = *(IPropertyTree **)t1;
        IPropertyTree *pt2 = *(IPropertyTree **)t2;
        offset_t sz1 = pt1->getPropInt64("Size");
        offset_t sz2 = pt2->getPropInt64("Size");
        if (sz1<sz2)
            return -1;
        if (sz1>sz2)
            return 1;
        return stricmp(pt2->queryProp("Name"),pt1->queryProp("Name")); // rev
    }

    void listOrphans(bool &abort,unsigned int recentCutoffDays)
    {
        // also does directories
        log(true,"Scanning for orphans");
        startHeartbeat("Orphan scan");
        StringBuffer basedir;
        StringBuffer scope;
        listOrphans(NULL,basedir,scope,abort,recentCutoffDays);
        if (abort)
            return;
        finishHeartbeat("Orphan scan");
        sorteddirs.sort(compareDirs);   // NB sort reverse
        while (!abort&&sorteddirs.ordinality())
            dirbranch->addPropTree("Directory",&sorteddirs.popGet());
        log(true,"Directories sorted");
    }


    void listLost(bool &abort,bool ignorelazylost,unsigned int recentCutoffDays)
    {
        log(true,"Scanning for lost files");
        StringBuffer tmp;
        ForEachItemIn(i0,lostfiles) {
            if (abort)
                return;
            CDfsLogicalFileName lfn;
            if (!lfn.setValidate(lostfiles.item(i0))) {
                error(lostfiles.item(i0), "Invalid filename detected");
                continue;
            }
            Owned<IDistributedFile> file;
            try {
                file.setown(queryDistributedFileDirectory().lookup(lfn,udesc,AccessMode::tbdRead,false,false,nullptr,defaultPrivilegedUser));
            }
            catch (IException *e) {
                EXCLOG(e,"CNewXRefManager::listLost");
                e->Release();
            }
            if (!file) {
                error(lfn.get(),"could not lookup possible lost file");
                continue;
            }
            file->setPreferredClusters(clusterscsl);
            StringBuffer tmpname;
            file->setSingleClusterOnly();
            file->getClusterName(0,tmpname);
            if ((tmpname.length()==0)||(clusters.find(tmpname.str())==NotFound)) {
                StringBuffer tmp;
                error(lfn.get(),"could not set preferred cluster (set to %s)",tmpname.str());
                continue;
            }
            if (ignorelazylost&&(file->queryAttributes().getPropInt("@lazy")!=0)) {
                warn(lfn.get(),"Lazy file ignored");
                continue;
            }
            CDateTime dt;
            Owned<IPropertyTree> ft = createPTree("File");
            if (file->getModificationTime(dt)) {
                CDateTime now;
                now.setNow();
                CDateTime co(dt);
                co.adjustTime(recentCutoffDays*60*24);
                if (co.compare(now)>=0) {
                    warn(lfn.get(),"Recent file ignored");
                    continue;
                }
                dt.getString(tmp.clear());
                ft->setProp("Modified",tmp.str());
            }
            unsigned np = file->numParts();
            unsigned cn = 0;
            ft->setProp("Name",lfn.get());
            tmp.clear().append(file->queryPartMask()).toLowerCase();
            ft->setProp("Partmask",tmp.str());
            ft->setPropInt("Numparts",np);
            file->getClusterName(cn,tmp.clear());
            ft->setProp("Cluster",tmp.str());
            bool *primlost = new bool[np];
            bool *replost = new bool[np];
            for (unsigned i0=0;i0<np;i0++) {
                primlost[i0] = true;
                replost[i0] = true;
            }
            bool ok = true;
            if (abort) {
                delete [] primlost;
                delete [] replost;
                return;
            }
            Owned<IDistributedFilePartIterator> piter = file->getIterator();
            ForEach(*piter) {
                if (abort) {
                    delete [] primlost;
                    delete [] replost;
                    return;
                }
                IDistributedFilePart &part = piter->query();
                unsigned pn = part.getPartIndex();
                unsigned nc = part.numCopies();
                for (unsigned copy = 0; copy < nc; copy++) {
                    RemoteFilename rfn;
                    part.getFilename(rfn,copy);
                    Owned<IFile> partfile = createIFile(rfn);
                    StringBuffer eps;
                    bool lost = true;
                    try {
                        if (partfile->exists()) {
                            if (copy>0)
                                replost[pn] = false;
                            else
                                primlost[pn] = false;
                            lost = false;
                        }
                    }
                    catch (IException *e)
                    {
                        StringBuffer tmp(LOGPFX "Checking file ");
                        rfn.getRemotePath(tmp);
                        EXCLOG(e, tmp.str());
                        e->Release();
                        ok = false;
                    }
                    if (!ok)
                        break;
                    if (lost) {
                        Owned<IPropertyTree> pt = createPTree("Part");
                        StringBuffer tmp;
                        rfn.queryEndpoint().getHostText(tmp);
                        pt->setProp("Node",tmp.str());
                        pt->setPropInt("Num",pn+1);
                        if (copy>0)
                            pt->setPropInt("Replicate",copy);
                        ft->addPropTree("Part",pt.getClear());
                    }
                }
            }
            unsigned pc = 0;
            unsigned rc = 0;
            unsigned c = 0;
            for (unsigned i1=0;i1<np;i1++) {
                if (primlost[i1]&&replost[i1]) {
                    pc++;
                    rc++;
                    c++;
                }
                else if (primlost[i1])
                    pc++;
                else if (replost[i1])
                    rc++;
            }
            delete [] primlost;
            delete [] replost;
            if (c) {
                PROGLOG(LOGPFX "Adding %s to lost files",lfn.get());
                ft->addPropInt("Partslost",c);
                ft->addPropInt("Primarylost",pc);
                ft->addPropInt("Replicatedlost",rc);
                lostbranch->addPropTree("File",ft.getClear());
            }
        }
        log(true,"Lost scan complete");
    }


    void save()
    {
        if (!rootdir.isEmpty()) {
            orphansbranch->setProp("@rootdir",rootdir);
            lostbranch->setProp("@rootdir",rootdir);
            foundbranch->setProp("@rootdir",rootdir);
        }
        saveXML("orphans.xml",orphansbranch);
        saveXML("found.xml",foundbranch);
        saveXML("lost.xml",lostbranch);
        saveXML("directories.xml",dirbranch);
    }






};

class CSuperfileCheckManager: public CNewXRefManagerBase
{
public:

    CSuperfileCheckManager(bool saveToPlane) : CNewXRefManagerBase(saveToPlane)
    {
        clustname.set("SuperFiles");
    }

    void start(bool updateeclwatch)
    {
        CNewXRefManagerBase::start(updateeclwatch,"SuperFiles");
    }

    void errornotrecent(const char *lname,const char * format, ...) __attribute__((format(printf, 3, 4)))
    {
        // checks can lock LFN and not recently changed
        CriticalBlock block(logsect);
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        CDfsLogicalFileName lfn;
        if (lfn.setValidate(lname)&&!lfn.isExternal()&&!lfn.isForeign()) {
            StringBuffer xpath;
            lfn.makeFullnameQuery(xpath,DXB_SuperFile,true);
            bool ignore = true;
            try {
                Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(),myProcessSession(),RTM_LOCK_READ ,1000*10);
                if (!conn) {
                    lfn.makeFullnameQuery(xpath.clear(),DXB_File,true);
                    conn.setown(querySDS().connect(xpath.str(),myProcessSession(),RTM_LOCK_READ ,1000*10));
                }
                StringBuffer str;
                if (conn->queryRoot()->getProp("@modified",str)) {
                    CDateTime dt;
                    dt.setString(str.str());
                    dt.adjustTime(60*24);
                    CDateTime now;
                    now.setNow();
                    if (now.compareDate(dt)>0)
                        ignore = false;
                }
            }
            catch (IException *e) {
                e->Release();
            }
            if (ignore) {
                PROGLOG(LOGPFX "Ignoring %s: %s",lname,line.str());
                return;
            }
        }
        if (errors.ordinality()<1000) {
            errors.append(*new cMessage(lname,line.str()));
            if (errors.ordinality()==1000)
                errors.append(*new cMessage("","error limit exceeded (1000), truncating"));
        }

        OERRLOG("%s: %s",lname,line.str());
    }

    void checkSuperFileLinkage()
    {
        StringArray superowner;
        StringArray superowned;
        StringArray fileowned;
        StringArray fileowner;
        class cfilescan1: public CSDSFileScanner
        {
            CSuperfileCheckManager &parent;
            StringArray &superowner;
            StringArray &superowned;
            StringArray &fileowned;
            StringArray &fileowner;

            void processFile(IPropertyTree &file,StringBuffer &name)
            {
                //parent.statlog("Scanning File %s",name.str());
                Owned<IPropertyTreeIterator> iter = file.getElements("SuperOwner");
                StringArray superowner;
                bool owned = false;
                ForEach(*iter) {
                    IPropertyTree &sfile = iter->query();
                    const char *owner = sfile.queryProp("@name");
                    if (!owner||!*owner) {
                        parent.errornotrecent(name.str(),"FAILED nullsuperownername");
                    }
                    else {
                        bool ok = true;
                        ForEachItemIn(i,superowner) {
                            if (strcmp(owner,superowner.item(i))==0) {
                                parent.errornotrecent(name.str(),"FAILED dupsuperownername(%s)",owner);
                                ok = false;
                                break;
                            }
                        }
                        if (ok) {
                            superowner.append(owner);
                            fileowned.append(name.str());
                            fileowner.append(owner);
                            owned = true;
                        }
                    }
                }
                if (owned)
                    parent.fnum++;
            }
            void processSuperFile(IPropertyTree &file,StringBuffer &name)
            {
                parent.sfnum++;
                parent.log(false,"Scanning SuperFile %s",name.str());
                unsigned numsub = file.getPropInt("@numsubfiles");
                unsigned n = 0;
                Owned<IPropertyTreeIterator> iter = file.getElements("SubFile");
                Owned<IBitSet> parts = createThreadSafeBitSet();
                StringArray subname;
                ForEach(*iter) {
                    IPropertyTree &sfile = iter->query();
                    const char *owned = sfile.queryProp("@name");
                    if (!owned||!*owned) {
                        parent.errornotrecent(name.str(),"FAILED nullsubfilename");
                    }
                    else {
                        bool ok = true;
                        ForEachItemIn(i,subname) {
                            if (strcmp(owned,subname.item(i))==0) {
                                parent.errornotrecent(name.str(),"FAILED dupsubfilename(%s)",owned);
                                ok = false;
                                break;
                            }
                        }
                        if (ok) {
                            unsigned num = sfile.getPropInt("@num");
                            if (!num)
                                parent.errornotrecent(name.str(),"FAILED missingsubnum(%s)",sfile.queryProp("@name"));
                            else if (num>numsub)
                                parent.errornotrecent(name.str(),"FAILED toobigsubnum(%s,%d,%d)",sfile.queryProp("@name"),num,numsub);
                            else if (parts->testSet(num-1))
                                parent.errornotrecent(name.str(),"FAILED dupsubnum(%s,%d)",sfile.queryProp("@name"),num);
                            else {
                                subname.append(owned);
                                superowner.append(name.str());
                                superowned.append(owned);
                            }
                        }
                    }
                    n++;
                }
                if (n!=numsub)
                    parent.errornotrecent(name.str(),"FAILED mismatchednumsubs(%d,%d)",numsub,n);
                processFile(file,name); // superfile is a file too!
            }
        public:

            cfilescan1(CSuperfileCheckManager &_parent, StringArray &_superowner,StringArray &_superowned,StringArray &_fileowned,StringArray &_fileowner)
                : parent(_parent), superowner(_superowner),superowned(_superowned),fileowned(_fileowned),fileowner(_fileowner)
            {
            }

        } filescan(*this, superowner,superowned,fileowned,fileowner);

        Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(),0, 100000);
        filescan.scan(conn,true,true);

        bool fix = false;

        log(false,"Crossreferencing %d SuperFiles",superowner.ordinality());
        ForEachItemIn(i1,superowner) {
            const char *owner = superowner.item(i1);
            const char *owned = superowned.item(i1);
            bool ok = false;
            if (*owned=='{')
                ok = true;
            else {
                ForEachItemIn(i2,fileowned) {
                    if ((stricmp(owner,fileowner.item(i2))==0)&&(stricmp(owned,fileowned.item(i2))==0)) {
                        ok = true;
                        break;
                    }
                }
            }
            if (!ok) {
                CDfsLogicalFileName lfn;
                if (!lfn.setValidate(owned)) {
                    errornotrecent(owner,"FAILED badsubpath %s\n",owned);
                }
                else {
                    StringBuffer lfnpath;
                    lfn.makeFullnameQuery(lfnpath,DXB_File);
                    if (!lfn.isExternal()&&!lfn.isForeign()) {
                        Owned<IRemoteConnection> conn = querySDS().connect(lfnpath.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, INFINITE);
                        if (conn) {
                            StringBuffer  path;
                            path.appendf("SuperOwner[@name=\"%s\"]",owner);
                            if (!conn->queryRoot()->hasProp(path.str())) {
                                if (fix) {
                                    Owned<IPropertyTree> t = createPTree("SuperOwner");
                                    t->setProp("@name",owner);
                                    conn->queryRoot()->addPropTree("SuperOwner",t.getClear());
                                }
                                errornotrecent(owned,"%s nosuperlink to %s",fix?"FIXED":"FAILED",owner);
                            }

                        }
                        else
                            errornotrecent(owner,"FAILED subnotexist %s",owned);
                    }
                }
            }
        }
        log(false,"Crossreferencing %d Files",fileowned.ordinality());
        ForEachItemIn(i3,fileowned) {
            const char *fowner = fileowner.item(i3);
            const char *fowned = fileowned.item(i3);
            bool ok = false;
            ForEachItemIn(i4,superowner) {
                if ((stricmp(superowner.item(i4),fowner)==0)&&(stricmp(superowned.item(i4),fowned)==0)) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                CDfsLogicalFileName lfn;
                if (!lfn.setValidate(fowner)) {
                    errornotrecent(fowned,"FAILED badsuperpath %s",fowner);
                }
                else {
                    StringBuffer lfnpath;
                    lfn.makeFullnameQuery(lfnpath,DXB_SuperFile);
                    Owned<IRemoteConnection> conn = querySDS().connect(lfnpath.str(),myProcessSession(),0, INFINITE);
                    if (conn)
                        errornotrecent(fowner,"FAILED nosublink to %s",fowned);
                    else {
                        bool fixed = false;
                        if (fix) {
                            CDfsLogicalFileName lfn2;
                            if (lfn2.setValidate(fowned)) {
                                lfn2.makeFullnameQuery(lfnpath.clear(),DXB_File);
                                Owned<IRemoteConnection> conn2 = querySDS().connect(lfnpath.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, INFINITE);
                                if (conn2) {
                                    StringBuffer xpath;
                                    xpath.appendf("SuperOwner[@name=\"%s\"]",fowner);
                                    if (conn2->queryRoot()->hasProp(xpath.str())) {
                                        conn2->queryRoot()->removeProp(xpath.str());
                                        fixed = true;
                                    }
                                }

                            }
                        }
                        warn(fowned,"%s supernotexist %s",fixed?"FIXED":"FAILED",fowner);
                    }
                }
            }
        }

    }



};

#if 0


void usage(const char *pname)
{
    // TBD
}

void testScan(const char *grpname)
{
    CNewXRefManager manager;
    if (!manager.setGroup(grpname)) {
        return;
    }
    if (manager.scanDirectories()) {
        manager.scanLogicalFiles();
        manager.listLost();
        manager.listOrphans();
        manager.save();
//      manager.saveToEclWatch();
    }
}


int main(int argc, char* argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    setNodeCaching(true);
    StringBuffer logName("danxref");
    StringBuffer aliasLogName(logName);
    aliasLogName.append(".log");

    ILogMsgHandler *fileMsgHandler = getRollingFileLogMsgHandler(logName.str(), ".log", MSGFIELD_STANDARD, false, true, NULL, aliasLogName.str());
    queryLogMsgManager()->addMonitorOwn(fileMsgHandler, getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, TopDetail));
    queryStderrLogMsgHandler()->setMessageFields(0);
    StringBuffer cmdline;
    unsigned i;
    for (i=0;i<argc;i++)
      cmdline.append(' ').append(argv[i]);


    if (argc<3) {
        usage(argv[0]);
        return 0;
    }

    SocketEndpoint ep;
    SocketEndpointArray epa;
    ep.set(argv[1],DALI_SERVER_PORT);
    epa.append(ep);
    Owned<IGroup> group = createIGroup(epa);
    try {
        initClientProcess(group,DCR_Dfu);
        setPasswordsFromSDS();
        testScan(argv[2]);


    }
    catch (IException *e) {
        pexception("Exception",e);
        e->Release();
    }

    closeEnvironment();
    closedownClientProcess();

    releaseAtoms();
    return 0;
}

#endif


static constexpr float maxMemPercentage = 0.9; // In containerized, leave some headroom for the pod
class CSashaXRefServer: public ISashaServer, public Thread
{
    bool stopped;
    Semaphore stopsem;
    Mutex runmutex;
    bool ignorelazylost, suspendCoalescer;
    Owned<IPropertyTree> props;
    std::unordered_map<std::string, Linked<IPropertyTree>> storagePlanes;

    class cRunThread: public Thread
    {
        CSashaXRefServer &parent;
        StringAttr servers;
        StringAttr filterScopes;
    public:
        cRunThread(CSashaXRefServer &_parent,const char *_servers, const char *_filterScopes)
            : parent(_parent), servers(_servers), filterScopes(_filterScopes)
        {
        }
        int run()
        {
            parent.runXRef(servers,false,false,filterScopes);
            return 0;
        }
    };


public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaXRefServer()
        : Thread("CSashaXRefServer")
    {
        if (!isContainerized())
            suspendCoalescer = true; // can be overridden by configuration setting
        else
            suspendCoalescer = false;
        stopped = false;
    }

    ~CSashaXRefServer()
    {
    }

    void start()
    {
        Thread::start(false);
    }

    void ready()
    {
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            stopsem.signal();
        }
        synchronized block(runmutex);   // hopefully stopped should stop
        if (!join(1000*60*3))
            OERRLOG("CSashaXRefServer aborted");
    }

    void runXRef(const char *clustcsl,bool updateeclwatch,bool byscheduler, const char *filterScopes)
    {
        if (stopped||!clustcsl||!*clustcsl)
            return;
        class CSuspendResume : public CSimpleInterface
        {
        public:
            CSuspendResume()
            {
                PROGLOG(LOGPFX "suspending coalesce");
                suspendCoalescingServer();
            }
            ~CSuspendResume()
            {
                PROGLOG(LOGPFX "resuming coalesce");
                resumeCoalescingServer();
            }
        };
        if (!isContainerized()) {
            Owned<CSimpleInterface> suspendresume;
            if (suspendCoalescer)
                suspendresume.setown(new CSuspendResume());
        }
        synchronized block(runmutex);
        if (stopped)
            return;
        CSuspendAutoStop suspendstop;
        PROGLOG(LOGPFX "Started %s",clustcsl);
        StringArray list;
        getFileGroups(clustcsl, list);
        bool checksuperfiles=false;
        ForEachItemInRev(i0,list) {
            if (strcmp(list.item(i0),"SuperFiles")==0) {
                checksuperfiles = true;
                list.remove(i0);
            }
        }
        // Revisit: XREF should really be plane centric only
        StringArray groups;
        StringArray cnames;
        // NB: must be a list of planes only
        ForEachItemIn(i1, list) {
            const char *planeName = list.item(i1);
            Owned<IPropertyTreeIterator> planesIter = getPlanesIterator("data", planeName);
            ForEach(*planesIter) {
                IPropertyTree &plane = planesIter->query();
                bool isNotCopy = !plane.getPropBool("@copy", false);
                bool isNotHthorPlane = !plane.getPropBool("@hthorplane", false);
                if (isNotCopy && isNotHthorPlane) {
                    planeName = plane.queryProp("@name");
                    if (isContainerized()) {
                        groups.append(planeName);
                        cnames.append(planeName);
                    }
                    storagePlanes[planeName].set(&plane);
                }
            }
        }
        if (!isContainerized()) {
            Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
            if (!conn) {
                OERRLOG("Could not connect to /Environment/Software");
                return;
            }
            clustersToGroups(conn->queryRoot(),list,cnames,groups,NULL);
        }
        IArrayOf<IGroup> groupsdone;
        StringArray dirsdone;
        bool saveToPlane = false;
        if (isContainerized())
            saveToPlane = getComponentConfigSP()->getPropBool("expert/@saveToPlane", true);
        else
        {
            // default off in BM
            // NB: BM does not have a per component expert section atm (only at the global level)
            saveToPlane = getGlobalConfigSP()->getPropBool("expert/@xrefSaveToPlane", false);
        }

        ForEachItemIn(i,groups) {
#ifdef TESTINGSUPERFILELINKAGE
            continue;
#endif
            const char *gname = groups.item(i);
            unsigned maxMb;
            if (isContainerized()) {
                const char *resourcedMemory = props->queryProp("resources/@memory");
                if (!isEmptyString(resourcedMemory)) {
                    offset_t sizeBytes = friendlyStringToSize(resourcedMemory);
                    maxMb = (unsigned)(sizeBytes / 0x100000);
                }
                else
                    maxMb = DEFAULT_MAXMEMORY;
                maxMb *= maxMemPercentage;
            }
            else
                maxMb = props->getPropInt("@memoryLimit", DEFAULT_MAXMEMORY);
            CNewXRefManager manager(storagePlanes[gname],maxMb,saveToPlane,filterScopes);
            if (!manager.setGroup(cnames.item(i),gname,groupsdone,dirsdone))
                continue;
            manager.start(updateeclwatch);
            manager.updateStatus(true);
            if (stopped)
                break;
            unsigned numThreads = props->getPropInt("@numThreads", DEFAULT_MAXDIRTHREADS);
            unsigned int recentCutoffDays = props->getPropInt("@cutoff", DEFAULT_RECENT_CUTOFF_DAYS);
            if (manager.scanDirectories(stopped,numThreads)) {
                manager.updateStatus(true);
                manager.scanLogicalFiles(stopped);
                manager.updateStatus(true);
                manager.listLost(stopped,ignorelazylost,recentCutoffDays);
                manager.updateStatus(true);
                manager.listOrphans(stopped,recentCutoffDays);
                manager.updateStatus(true);
                manager.saveToEclWatch(stopped,byscheduler);
                manager.updateStatus(true);
            }
            manager.finish(stopped);
            manager.updateStatus(true);
            if (stopped)
                break;
        }
        if (checksuperfiles&&!stopped) {
            CSuperfileCheckManager scmanager(saveToPlane);
            scmanager.start(updateeclwatch);
            scmanager.updateStatus(true);
            if (stopped)
                return;
            scmanager.checkSuperFileLinkage();
            scmanager.updateStatus(true);
            scmanager.saveToEclWatch(stopped,byscheduler);
            scmanager.updateStatus(true);
        }
        PROGLOG(LOGPFX "%s %s",clustcsl,stopped?"Stopped":"Done");
    }

    void xrefRequest(const char *servers, const char *filterScopes)
    {
        //MORE: This could still be running when the server terminates which will likely cause the thread to core
        cRunThread *thread = new cRunThread(*this,servers,filterScopes);
        thread->startRelease();
    }

    bool checkClusterSubmitted(StringBuffer &cname, StringBuffer &filterScopes)
    {
        cname.clear();
        filterScopes.clear();
        Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_LOCK_WRITE ,INFINITE);
        Owned<IPropertyTreeIterator> clusters= conn->queryRoot()->getElements("Cluster");
        ForEach(*clusters) {
            IPropertyTree &cluster = clusters->query();
            const char *status = cluster.queryProp("@status");
            if (status&&(stricmp(status,"Submitted")==0)) {
                cluster.setProp("@status","Not Found"); // prevent cycling
                const char *name = cluster.queryProp("@name");
                if (name) {
                    if (cname.length())
                        cname.append(',');
                    cname.append(name);
                    // Get filterScopes for this cluster
                    const char *scopes = cluster.queryProp("@filterScopes");
                    if (scopes && *scopes)
                        filterScopes.set(scopes).toLowerCase();
                }
            }
        }
        return cname.length()!=0;
    }

    void setSubmittedOk(bool on)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE ,INFINITE);
        if (conn->queryRoot()->getPropBool("@useSasha")!=on)
            conn->queryRoot()->setPropBool("@useSasha",on);
    }

    int run()
    {
        if (isContainerized())
            props.setown(getComponentConfig());
        else
        {
            props.setown(serverConfig->getPropTree("DfuXRef"));
            if (!props)
                props.setown(createPTree("DfuXRef"));
        }

        bool eclwatchprovider = true;
        if (!isContainerized()) // NB: containerized does not support xref any other way.
        {
            // eclwatchProvider sets useSasha in call to setSubmittedOk
            eclwatchprovider = props->getPropBool("@eclwatchProvider");
        }

        unsigned interval = props->getPropInt("@interval",DEFAULT_XREF_INTERVAL);
        const char *clusters = props->queryProp(isContainerized() ? "@planes" : "@clusterlist");
        StringBuffer clusttmp;
        // TODO: xref should support checking superfiles in containerized
        if (props->getPropBool("@checkSuperFiles",isContainerized()?false:true))
        {
            if (!clusters||!*clusters)
                clusters = "SuperFiles";
            else
                clusters = clusttmp.append(clusters).append(',').append("SuperFiles").str();
        }
        if (!interval)
            stopped = !eclwatchprovider;
        setSubmittedOk(eclwatchprovider);
        if (!isContainerized())
            suspendCoalescer = props->getPropBool("@suspendCoalescerDuringXref", true);
        ignorelazylost = props->getPropBool("@ignoreLazyLost",true);
        PROGLOG(LOGPFX "min interval = %d hr", interval);
        unsigned initinterval = (interval-1)/2+1;  // wait a bit til dali has started
        CSashaSchedule schedule;
        if (interval)
            schedule.init(props,interval,initinterval);
        initinterval *= 60*60*1000; // ms
        unsigned started = msTick();
        while (!stopped)
        {
            stopsem.wait(1000*60);
            if (stopped)
                break;
            StringBuffer cname;
            StringBuffer filterScopes;
            bool byscheduler=false;
            if (!eclwatchprovider||!checkClusterSubmitted(cname.clear(), filterScopes.clear()))
            {
                if (!interval||((started!=(unsigned)-1)&&(msTick()-started<initinterval)))
                    continue;
                started = (unsigned)-1;
                if (!schedule.ready())
                    continue;
                byscheduler = true;
            }
            try
            {
                runXRef(cname.length()?cname.str():clusters,true,byscheduler,filterScopes.length()?filterScopes.str():nullptr);
                cname.clear();
                filterScopes.clear();
            }
            catch (IException *e)
            {
                StringBuffer s;
                EXCLOG(e, LOGPFX);
                e->Release();
            }
        }
        PROGLOG(LOGPFX "Exit");
        return 0;
    }


} *sashaXRefServer = NULL;


ISashaServer *createSashaXrefServer()
{
    assertex(!sashaXRefServer); // initialization problem
    sashaXRefServer = new CSashaXRefServer();
    return sashaXRefServer;
}

void processXRefRequest(ISashaCommand *cmd)
{
    if (sashaXRefServer) {
        StringBuffer clusterlist(cmd->queryCluster());
        StringBuffer filterScopes(cmd->queryFilterScopes());
        // only support single cluster for the moment
        if (clusterlist.length())
            sashaXRefServer->xrefRequest(clusterlist, filterScopes);
    }
}



// File Expiry monitor

class CSashaExpiryServer: public ISashaServer, public Thread
{
    bool stopped;
    Semaphore stopsem;
    Mutex runmutex;
    Owned<IUserDescriptor> udesc;
    Linked<IPropertyTree> props;
    bool dryRun = false;

public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaExpiryServer(IPropertyTree *_config)
        : Thread("CSashaExpiryServer"), props(_config)
    {
        stopped = false;

        StringBuffer userName;
#ifdef _CONTAINERIZED
        props->getProp("@user", userName);
#else
        serverConfig->getProp("@sashaUser", userName);
#endif
        udesc.setown(createUserDescriptor());
        udesc->set(userName.str(), nullptr);
    }

    ~CSashaExpiryServer()
    {
    }

    void start()
    {
        Thread::start(false);
    }

    void ready()
    {
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            stopsem.signal();
        }
        synchronized block(runmutex);   // hopefully stopped should stop
        if (!join(1000*60*3))
            OERRLOG("CSashaExpiryServer aborted");
    }

    void runExpiry()
    {
        synchronized block(runmutex);
        if (stopped)
            return;
        PROGLOG(LOGPFX2 "Started");

        dryRun = getComponentConfigSP()->getPropBool("@dryRun");

        constexpr unsigned minWindowDays = 3; // Minimum time window size
        constexpr unsigned maxWindowDays = 365 * 6; // Maximum time window size (6 years). If it dynamically grows to this size, the window is unbounded at the start.

        unsigned defaultExpireDays = props->getPropInt("@expiryDefault", DEFAULT_EXPIRYDAYS);
        unsigned defaultPersistExpireDays = props->getPropInt("@persistExpiryDefault", DEFAULT_PERSISTEXPIRYDAYS);
        unsigned maxFileLimit = props->getPropInt("@maxFileLimit", UINT_MAX);

        // Determine the effective limit for adaptive windowing
        unsigned effectiveLimit = (UINT_MAX != maxFileLimit) ? maxFileLimit : 100000; // 100k is server default
        unsigned lowWaterMark = effectiveLimit / 5; // 20% threshold for expanding window

        std::vector<DFUQResultField> selectiveFields = {DFUQResultField::expireDays, DFUQResultField::accessed, DFUQResultField::persistent, DFUQResultField::term};

        unsigned totalProcessed = 0;
        unsigned totalDeleted = 0;

        // Try fetching all files first (no time filter)
        bool allMatchingFilesReceived;
        unsigned fetchCount = 0;

        StringBuffer baseFilterBuf;
        // all non-superfiles
        baseFilterBuf.append(DFUQFTspecial).append(DFUQFilterSeparator).append(DFUQSFFileType).append(DFUQFilterSeparator).append(DFUQFFTnonsuperfileonly).append(DFUQFilterSeparator);
        // hasProp,SuperOwner,"false" - meaning not owned by a superfile
        baseFilterBuf.append(DFUQFThasProp).append(DFUQFilterSeparator).append(getDFUQFilterFieldName(DFUQFFsuperowner)).append(DFUQFilterSeparator).append("false").append(DFUQFilterSeparator);
        // hasProp,Attr/@expireDays,"true" - meaning file has @expireDays attribute
        baseFilterBuf.append(DFUQFThasProp).append(DFUQFilterSeparator).append(getDFUQFilterFieldName(DFUQFFexpiredays)).append(DFUQFilterSeparator).append("true").append(DFUQFilterSeparator);
        if (UINT_MAX != maxFileLimit)
            baseFilterBuf.append(DFUQFTspecial).append(DFUQFilterSeparator).append(DFUQSFMaxFiles).append(DFUQFilterSeparator).append(maxFileLimit);

        StringBuffer filterBuf(baseFilterBuf);
        Owned<IPropertyTreeIterator> iter = queryDistributedFileDirectory().getDFAttributesFilteredIterator(filterBuf,
            nullptr, selectiveFields.data(), udesc, true, allMatchingFilesReceived, &fetchCount);

        CDateTime now;
        now.setNow();

        // Always process the initial batch
        PROGLOG(LOGPFX2 "Fetched %u files in initial query", fetchCount);
        if (fetchCount > 0)
        {
            unsigned deleted = processExpiryBatch(iter, now, defaultExpireDays, defaultPersistExpireDays);
            totalProcessed += fetchCount;
            totalDeleted += deleted;
        }

        if (!allMatchingFilesReceived)
        {
            // Need to use time-windowing approach for remaining files
            // Use time windows to partition the file space
            // Start from 'now' (a known reference point) and work backwards in time, since we cannot
            // easily determine the oldest file date without an additional query that might also hit limits.
            OWARNLOG(LOGPFX2 "Exceeded maximum retrievable files (fetched: %u), using time-windowed approach for remaining files", fetchCount);

            unsigned windowDays = 365;
            CDateTime windowEnd = now;
            unsigned windowIteration = 0;

            // Note: The initial batch (processed above) contained an arbitrary set of files in semi-deterministic order,
            // so there will inevitably be some overlap between those files and the files in the first time window.
            // This is acceptable - files already deleted won't be found again, and files already checked will simply
            // be re-checked.
            while (!stopped)
            {
                windowIteration++;
                CDateTime windowStart = windowEnd;
                windowStart.adjustTime(-60*24*windowDays);

                StringBuffer startStr, endStr;
                windowStart.getString(startStr);
                windowEnd.getString(endStr);

                // Build filter with time range
                // If window is at maximum, use unbounded start (modified < windowEnd)
                // Otherwise use bounded range (windowStart <= modified < windowEnd)
                filterBuf.clear().append(baseFilterBuf).append(DFUQFilterSeparator);
                filterBuf.append(DFUQFTstringRange).append(DFUQFilterSeparator);
                filterBuf.append(getDFUQFilterFieldName(DFUQFFtimemodified)).append(DFUQFilterSeparator);
                if (windowDays < maxWindowDays) // else - empty 'from' - unbounded start
                    filterBuf.append(startStr);
                filterBuf.append(DFUQFilterSeparator);
                filterBuf.append(endStr).append(DFUQFilterSeparator);

                fetchCount = 0;
                try
                {
                    iter.setown(queryDistributedFileDirectory().getDFAttributesFilteredIterator(filterBuf,
                        nullptr, selectiveFields.data(), udesc, true, allMatchingFilesReceived, &fetchCount));

                    if (windowDays >= maxWindowDays)
                        PROGLOG(LOGPFX2 "Window iteration %u: Fetched %u files (modified < %s)",
                            windowIteration, fetchCount, endStr.str());
                    else
                        PROGLOG(LOGPFX2 "Window iteration %u: Fetched %u files (%s <= modified < %s)",
                            windowIteration, fetchCount, startStr.str(), endStr.str());

                    if (fetchCount > 0)
                    {
                        unsigned deleted = processExpiryBatch(iter, now, defaultExpireDays, defaultPersistExpireDays);
                        totalProcessed += fetchCount;
                        totalDeleted += deleted;
                    }

                    if (allMatchingFilesReceived)
                    {
                        if (windowDays >= maxWindowDays)
                        {
                            // Unbounded query with complete coverage - we're done
                            PROGLOG(LOGPFX2 "Unbounded query with complete coverage, all remaining files processed");
                            break;
                        }
                    }
                    else
                    {
                        // Exceeded limit within this time window - reduce window and retry same time period
                        if (windowDays > minWindowDays)
                        {
                            windowDays /= 2;
                            if (windowDays < minWindowDays)
                                windowDays = minWindowDays;
                            PROGLOG(LOGPFX2 "Exceeded limit, reducing window to %u days and retrying time period", windowDays);
                            continue;
                        }
                        else
                        {
                            // Window at minimum but still exceeding limit
                            // Process what we got and move to next window (accept incomplete coverage of this period).
                            // As expired files are deleted over successive runs, the total file count should decrease,
                            // eventually allowing complete coverage in future expiry cycles.
                            OWARNLOG(LOGPFX2 "Window at or below minimum %u days but still exceeding limit (%u files), some files in this period may not be processed", minWindowDays, fetchCount);
                        }
                    }

                    // Successfully processed this window (or accepted partial coverage)
                    // Optimize window size for next iteration based on file count
                    if (fetchCount < lowWaterMark)
                    {
                        if (fetchCount == 0)
                        {
                            // No files found in window
                            if (windowDays >= maxWindowDays)
                            {
                                // Already using unbounded query with no files - we're done
                                PROGLOG(LOGPFX2 "No files found with unbounded query, all remaining files processed");
                                break;
                            }
                            else
                            {
                                // Jump to unbounded query and retry
                                windowDays = maxWindowDays;
                                PROGLOG(LOGPFX2 "No files found in window, switching to unbounded query");
                                continue;
                            }
                        }
                        else if (windowDays >= maxWindowDays)
                        {
                            // Unbounded query with low file count - we're done
                            PROGLOG(LOGPFX2 "Unbounded query returned low file count (%u), all remaining files processed", fetchCount);
                            break;
                        }
                        else
                        {
                            // Some files found - expand window cautiously
                            windowDays = (windowDays * 2 > maxWindowDays) ? maxWindowDays : windowDays * 2;
                            PROGLOG(LOGPFX2 "Low file count (%u < %u), expanding next window to %u days",
                                fetchCount, lowWaterMark, windowDays);
                        }
                    }
                }
                catch (IException *e)
                {
                    OWARNLOG(e, LOGPFX2 "Window iteration failed, moving to next window");
                    e->Release();
                }
                // Move to next window (going backwards in time)
                windowEnd = windowStart;
            }
        }

        PROGLOG(LOGPFX2 "%s - Processed %u files, deleted %u",
            stopped ? "Stopped" : "Done", totalProcessed, totalDeleted);
    }

    unsigned processExpiryBatch(IPropertyTreeIterator *iter, const CDateTime &now,
        unsigned defaultExpireDays, unsigned defaultPersistExpireDays)
    {
        StringArray expirylist;

        ForEach(*iter)
        {
            if (stopped)
                break;

            IPropertyTree &attr = iter->query();
            if (attr.hasProp("@expireDays"))
            {
                unsigned expireDays = attr.getPropInt("@expireDays");
                const char *name = attr.queryProp("@name");
                const char *lastAccessed = attr.queryProp("@accessed");
                if (lastAccessed && name && *name)
                {
                    if (0 == expireDays)
                    {
                        bool isPersist = attr.getPropBool("@persistent");
                        expireDays = isPersist ? defaultPersistExpireDays : defaultExpireDays;
                    }
                    CDateTime expires;
                    try
                    {
                        expires.setString(lastAccessed);
                        expires.adjustTime(60*24*expireDays);
                        if (now.compare(expires, false) > 0)
                        {
                            expirylist.append(name);
                            StringBuffer expiresStr;
                            expires.getString(expiresStr);
                            PROGLOG(LOGPFX2 "%s expired on %s", name, expiresStr.str());
                        }
                    }
                    catch (IException *e)
                    {
                        EXCLOG(e, LOGPFX2 "setdate");
                        e->Release();
                    }
                }
            }
        }

        unsigned deleted = 0;
        ForEachItemIn(i, expirylist)
        {
            if (stopped)
                break;
            const char *lfn = expirylist.item(i);
            try
            {
                /* NB: 0 timeout, meaning fail and skip, if there is any locking contention.
                 * If the file is locked, it implies it is being accessed.
                 */
                if (dryRun)
                    PROGLOG(LOGPFX2 "Dry run - would delete %s", lfn);
                else
                {
                    queryDistributedFileDirectory().removeEntry(lfn, udesc, NULL, 0, true);
                    PROGLOG(LOGPFX2 "Deleted %s", lfn);
                }
                deleted++;
            }
            catch (IException *e) // may want to just detach if fails
            {
                OWARNLOG(e, LOGPFX2 "remove");
                e->Release();
            }
        }

        return deleted;
    }

    int run()
    {
        unsigned interval = props->getPropInt("@interval",DEFAULT_EXPIRY_INTERVAL);
        if (!interval)
            stopped = true;
        PROGLOG(LOGPFX2 "min interval = %d hr", interval);
        unsigned initinterval = (interval-1)/2;  // wait a bit til dali has started
        CSashaSchedule schedule;
        if (interval)
            schedule.init(props,interval,initinterval);
        initinterval *= 60*60*1000; // ms
        unsigned started = msTick();
        while (!stopped)
        {
            stopsem.wait(1000*60);
            if (stopped)
                break;
            if (!interval||((started!=(unsigned)-1)&&(msTick()-started<initinterval)))
                continue;
            started = (unsigned)-1;
            if (!schedule.ready())
                continue;
            try {
                runExpiry();
            }
            catch (IException *e) {
                StringBuffer s;
                EXCLOG(e, LOGPFX2);
                e->Release();
            }
        }
        PROGLOG(LOGPFX2 "Exit");
        return 0;
    }


} *sashaExpiryServer = NULL;


ISashaServer *createSashaFileExpiryServer()
{
    assertex(!sashaExpiryServer); // initialization problem
#ifdef _CONTAINERIZED
    Linked<IPropertyTree> config = serverConfig;
#else
    Owned<IPropertyTree> config = serverConfig->getPropTree("DfuExpiry");
    if (!config)
        config.setown(createPTree("DfuExpiry"));
#endif
    sashaExpiryServer = new CSashaExpiryServer(config);
    return sashaExpiryServer;
}

void runExpiryCLI()
{
    Owned<IPropertyTree> config = serverConfig->getPropTree("DfuExpiry");
    if (!config)
        config.setown(createPTree("DfuExpiry"));
    Owned<CSashaExpiryServer> sashaExpiryServer = new CSashaExpiryServer(config);
    sashaExpiryServer->runExpiry();
}
