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

//****************************************************************************
// Name:         jhtree.cpp
//
// Purpose:      
//
// Description:    
//
// Notes:        Supports only static (non-changing) files
//
//                Initially I was holding on to the root nodes, but came to find
//                that they could potentially fill the cache by themselves...
//
//                Things to play with:
//                    - try not unpacking the entire node when it is read in.
//                        break it out as needed later.
//
// History:        31-Aug-99   crs  original
//              08-Jan-00    nh  added LZW compression of nodes
//                14-feb-00     nh     added GetORDKey
//                15-feb-00     nh     fixed isolatenode and nextNode
//                12-Apr-00    jcs moved over to jhtree.dll etc.
//****************************************************************************

#include "platform.h"
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <limits.h>
#ifdef __linux__
#include <alloca.h>
#endif

#include "hlzw.h"

#include "jmutex.hpp"
#include "jhutil.hpp"
#include "jmisc.hpp"
#include "jstats.h"
#include "ctfile.hpp"

#include "jhtree.ipp"
#include "keybuild.hpp"
#include "eclhelper_dyn.hpp"
#include "layouttrans.hpp"
#include "rtlrecord.hpp"

static std::atomic<CKeyStore *> keyStore(nullptr);
static unsigned defaultKeyIndexLimit = 200;
static CNodeCache *nodeCache = NULL;
static CriticalSection *initCrit = NULL;

bool useMemoryMappedIndexes = false;
bool logExcessiveSeeks = false;
bool linuxYield = false;
bool traceSmartStepping = false;
bool flushJHtreeCacheOnOOM = true;

MODULE_INIT(INIT_PRIORITY_JHTREE_JHTREE)
{
    initCrit = new CriticalSection;
    return 1;
}

MODULE_EXIT()
{
    delete initCrit;
    delete keyStore.load(std::memory_order_relaxed);
    ::Release((CInterface*)nodeCache);
}

//#define DUMP_NODES

SegMonitorList::SegMonitorList(const RtlRecord &_recInfo, bool _needWild) : recInfo(_recInfo), needWild(_needWild)
{
    keySegCount = recInfo.getNumKeyedFields();
    reset();
}

unsigned SegMonitorList::ordinality() const
{
    return segMonitors.length();
}

IKeySegmentMonitor *SegMonitorList::item(unsigned idx) const
{
    return &segMonitors.item(idx);
}

size32_t SegMonitorList::getSize() const
{
    unsigned lim = segMonitors.length();
    if (lim)
    {
        IKeySegmentMonitor &lastItem = segMonitors.item(lim-1);
        return lastItem.getOffset() + lastItem.getSize();
    }
    else
        return 0;
}

void SegMonitorList::checkSize(size32_t keyedSize, char const * keyname)
{
    size32_t segSize = getSize();
    if (segSize != keyedSize)
    {
        StringBuffer err;
        err.appendf("Key size mismatch on key %s - key size is %u, expected %u", keyname, keyedSize, getSize());
        IException *e = MakeStringExceptionDirect(1000, err.str());
        EXCLOG(e, err.str());
        throw e;
    }
}

void SegMonitorList::setLow(unsigned segno, void *keyBuffer) const
{
    unsigned lim = segMonitors.length();
    while (segno < lim)
        segMonitors.item(segno++).setLow(keyBuffer);
}

unsigned SegMonitorList::setLowAfter(size32_t offset, void *keyBuffer) const
{
    unsigned lim = segMonitors.length();
    unsigned segno = 0;
    unsigned skipped = 0;
    while (segno < lim)
    {
        IKeySegmentMonitor &seg = segMonitors.item(segno++);
        if (seg.getOffset() >= offset)
            seg.setLow(keyBuffer);
        else if (seg.getSize()+seg.getOffset() <= offset)
            skipped++;
        else
        {
            byte *temp = (byte *) alloca(seg.getSize() + seg.getOffset());
            seg.setLow(temp);
            memcpy((byte *)keyBuffer+offset, temp+offset, seg.getSize() - (offset - seg.getOffset()));
        }
    }
    return skipped;
}

void SegMonitorList::endRange(unsigned segno, void *keyBuffer) const
{
    unsigned lim = segMonitors.length();
    if (segno < lim)
        segMonitors.item(segno++).endRange(keyBuffer);
    while (segno < lim)
        segMonitors.item(segno++).setHigh(keyBuffer);
}

bool SegMonitorList::incrementKey(unsigned segno, void *keyBuffer) const
{
    // Increment the key buffer to next acceptable value
    for(;;)
    {
        if (segMonitors.item(segno).increment(keyBuffer))
        {
            setLow(segno+1, keyBuffer);
            return true;
        }
        if (!segno)
            return false;
        segno--;
    }
}

unsigned SegMonitorList::_lastRealSeg() const
{
    unsigned seg = segMonitors.length();
    for (;;)
    {
        if (!seg)
            return 0;
        seg--;
        if (!segMonitors.item(seg).isWild())
            return seg;
    }
}

unsigned SegMonitorList::lastFullSeg() const
{
    // This is used to determine what part of the segmonitor list to use for a pre-count to determine if atmost/limit have been hit
    // We include everything up to the last of i) the last keyed element or ii) the last keyed,opt element that has no wild between it and a keyed element
    // NOTE - can return (unsigned) -1 if there are no full segments
    unsigned len = segMonitors.length();
    unsigned seg = 0;
    unsigned ret = (unsigned) -1;
    bool wildSeen = false;
    while (seg < len)
    {
        if (segMonitors.item(seg).isWild())
            wildSeen = true;
        else
        {
            if (!wildSeen || !segMonitors.item(seg).isOptional())
            {
                ret = seg;
                wildSeen = false;
            }
        }
        seg++;
    }
    return ret;
}

void SegMonitorList::finish(unsigned keyedSize)
{
    if (modified)
    {
        while (segMonitors.length() < keySegCount)
        {
            unsigned idx = segMonitors.length();
            size32_t offset = recInfo.getFixedOffset(idx);
            size32_t size = recInfo.getFixedOffset(idx+1) - offset;
            segMonitors.append(*createWildKeySegmentMonitor(idx, offset, size));
        }
        size32_t segSize = getSize();
        assertex(segSize == keyedSize);
        recalculateCache();
        modified = false;
    }
}

void SegMonitorList::recalculateCache()
{
    cachedLRS = _lastRealSeg();
}

void SegMonitorList::reset()
{
    segMonitors.kill();
    modified = true;
}

void SegMonitorList::swapWith(SegMonitorList &other)
{
    reset();
    other.segMonitors.swapWith(segMonitors);
}

void SegMonitorList::deserialize(MemoryBuffer &mb)
{
    unsigned num;
    mb.read(num);
    while (num--)
        append(deserializeKeySegmentMonitor(mb));
}

void SegMonitorList::serialize(MemoryBuffer &mb) const
{
    mb.append((unsigned) ordinality());
    ForEachItemIn(idx, segMonitors)
        segMonitors.item(idx).serialize(mb);
}

// interface IIndexReadContext
void SegMonitorList::append(IKeySegmentMonitor *segment)
{
    modified = true;
    unsigned fieldIdx = segment->getFieldIdx();
    unsigned offset = segment->getOffset();
    unsigned size = segment->getSize();
    while (segMonitors.length() < fieldIdx)
    {
        unsigned idx = segMonitors.length();
        size32_t offset = recInfo.getFixedOffset(idx);
        size32_t size = recInfo.getFixedOffset(idx+1) - offset;
        segMonitors.append(*createWildKeySegmentMonitor(idx, offset, size));
    }
    segMonitors.append(*segment);
}

void SegMonitorList::append(FFoption option, IFieldFilter * filter)
{
    UNIMPLEMENTED;
}

bool SegMonitorList::matched(void *keyBuffer, unsigned &lastMatch) const
{
    lastMatch = 0;
    for (; lastMatch < segMonitors.length(); lastMatch++)
    {
        if (!segMonitors.item(lastMatch).matchesBuffer(keyBuffer))
            return false;
    }
    return true;
}


///

class jhtree_decl CKeyLevelManager : implements IKeyManager, public CInterface
{
protected:
    IContextLogger *ctx;
    SegMonitorList segs;
    IKeyCursor *keyCursor;
    char *keyBuffer;
    unsigned keySize;       // size of key record including payload
    unsigned keyedSize;     // size of non-payload part of key
    unsigned numsegs;
    bool matched;
    bool eof;
    bool started;
    StringAttr keyName;
    unsigned seeks;
    unsigned scans;
    unsigned skips;
    unsigned nullSkips;
    unsigned wildseeks;

    Owned<IRecordLayoutTranslator> layoutTrans;
    bool transformSegs;
    IIndexReadContext * activitySegs;
    CMemoryBlock layoutTransBuff;
    size32_t layoutSize = 0;
    Owned<IRecordLayoutTranslator::SegmentMonitorContext> layoutTransSegCtx;
    Owned<IRecordLayoutTranslator::RowTransformContext> layoutTransRowCtx;

    inline void setLow(unsigned segNo) 
    {
        segs.setLow(segNo, keyBuffer);
    }

    inline unsigned setLowAfter(size32_t offset) 
    {
        return segs.setLowAfter(offset, keyBuffer);
    }

    inline bool incrementKey(unsigned segno) const
    {
        return segs.incrementKey(segno, keyBuffer);
    }

    inline void endRange(unsigned segno)
    {
        segs.endRange(segno, keyBuffer);
    }

    byte const * doRowLayoutTransform()
    {
        layoutSize = layoutTrans->transformRow(layoutTransRowCtx, reinterpret_cast<byte const *>(keyBuffer), keySize, layoutTransBuff);
        return layoutTransBuff.get();
    }

    bool skipTo(const void *_seek, size32_t seekOffset, size32_t seeklen)
    {
        // Modify the current key contents buffer as follows
        // Take bytes up to seekoffset from current buffer (i.e. leave them alone)
        // Take up to seeklen bytes from seek comparing them as I go. If I see a lower one before I see a higher one, stop.
        // If I didn't see any higher ones, return (at which point the skipto was a no-op
        // If I saw higher ones, call setLowAfter for all remaining segmonitors
        // If the current contents of buffer could not match, call incremementKey at the appropriate monitor so that it can
        // Clear the matched flag
        const byte *seek = (const byte *) _seek;
        while (seeklen)
        {
            int c = *seek - (byte) (keyBuffer[seekOffset]);
            if (c < 0)
                return false;
            else if (c>0)
            {
                memcpy(keyBuffer+seekOffset, seek, seeklen);
                break;
            }
            seek++;
            seekOffset++;
            seeklen--;
        }
#ifdef _DEBUG
        if (traceSmartStepping)
        {
            StringBuffer recstr;
            unsigned i;
            for (i = 0; i < keySize; i++)
            {
                unsigned char c = ((unsigned char *) keyBuffer)[i];
                recstr.appendf("%c", isprint(c) ? c : '.');
            }
            recstr.append ("    ");
            for (i = 0; i < keySize; i++)
            {
                recstr.appendf("%02x ", ((unsigned char *) keyBuffer)[i]);
            }
            DBGLOG("SKIP: IN  skips=%02d nullskips=%02d seeks=%02d scans=%02d : %s", skips, nullSkips, seeks, scans, recstr.str());
        }
#endif
        if (!seeklen) return false;

        unsigned j = setLowAfter(seekOffset + seeklen); 
        bool canmatch = true;
        unsigned lastSeg = segs.lastRealSeg();
        for (; j <= lastSeg; j++)
        {
            canmatch = segs.segMonitors.item(j).matchesBuffer(keyBuffer);
            if (!canmatch)
            {
                eof = !incrementKey(j);
                break;
            }
        }
        matched = false;
        return true;
    }

    void noteSeeks(unsigned lseeks, unsigned lscans, unsigned lwildseeks)
    {
        seeks += lseeks;
        scans += lscans;
        wildseeks += lwildseeks;
        if (ctx)
        {
            if (lseeks) ctx->noteStatistic(StNumIndexSeeks, lseeks);
            if (lscans) ctx->noteStatistic(StNumIndexScans, lscans);
            if (lwildseeks) ctx->noteStatistic(StNumIndexWildSeeks, lwildseeks);
        }
    }

    void noteSkips(unsigned lskips, unsigned lnullSkips)
    {
        skips += lskips;
        nullSkips += lnullSkips;
        if (ctx)
        {
            if (lskips) ctx->noteStatistic(StNumIndexSkips, lskips);
            if (lnullSkips) ctx->noteStatistic(StNumIndexNullSkips, lnullSkips);
        }
    }

    void reportExcessiveSeeks(unsigned numSeeks, unsigned lastSeg)
    {
        StringBuffer recstr;
        unsigned i;
        for (i = 0; i < keySize; i++)
        {
            unsigned char c = ((unsigned char *) keyBuffer)[i];
            recstr.appendf("%c", isprint(c) ? c : '.');
        }
        recstr.append ("\n");
        for (i = 0; i < keySize; i++)
        {
            recstr.appendf("%02x ", ((unsigned char *) keyBuffer)[i]);
        }
        recstr.append ("\nusing segmonitors:\n");
        for (i=0; i <= lastSeg; i++)
        {
            unsigned size = segs.segMonitors.item(i).getSize();
            while (size--)
                recstr.append( segs.segMonitors.item(i).isWild() ? '?' : '#');
        }
        if (ctx)
            ctx->CTXLOG("%d seeks to lookup record \n%s\n in key %s", numSeeks, recstr.str(), keyName.get());
        else
            DBGLOG("%d seeks to lookup record \n%s\n in key %s", numSeeks, recstr.str(), keyName.get());
    }

public:
    IMPLEMENT_IINTERFACE;

    CKeyLevelManager(const RtlRecord &_recInfo, IKeyIndex * _key, IContextLogger *_ctx) : segs(_recInfo, true)
    {
        ctx = _ctx;
        numsegs = 0;
        keyBuffer = NULL;
        keyCursor = NULL;
        keySize = 0;
        keyedSize = 0;
        started = false;
        setKey(_key);
        seeks = 0;
        scans = 0;
        skips = 0;
        eof = false;
        nullSkips = 0;
        wildseeks = 0;
        transformSegs = false;
        activitySegs = &segs;
    }

    ~CKeyLevelManager()
    {
        free (keyBuffer);
        ::Release(keyCursor);
    }

    virtual unsigned querySeeks() const
    {
        return seeks;
    }

    virtual unsigned queryScans() const
    {
        return scans;
    }

    virtual unsigned querySkips() const
    {
        return skips;
    }

    virtual unsigned queryNullSkips() const
    {
        return nullSkips;
    }

    virtual void resetCounts()
    {
        scans = 0;
        seeks = 0;
        skips = 0;
        nullSkips = 0;
        wildseeks = 0;
    }

    void setKey(IKeyIndexBase * _key)
    {
        ::Release(keyCursor);
        keyCursor = NULL;
        if (_key)
        {
            assertex(_key->numParts()==1);
            IKeyIndex *ki = _key->queryPart(0);
            keyCursor = ki->getCursor(ctx);
            keyName.set(ki->queryFileName());
            if (!keyBuffer)
            {
                keySize = ki->keySize();
                keyedSize = ki->keyedSize();
                if (!keySize)
                {
                    StringBuffer err;
                    err.appendf("Key appears corrupt - key file (%s) indicates record size is zero", keyName.get());
                    IException *e = MakeStringExceptionDirect(1000, err.str());
                    EXCLOG(e, err.str());
                    throw e;
                }
                keyBuffer = (char *) malloc(keySize);

            }
            else
            {
                assertex(keyedSize==ki->keyedSize());
                assertex(keySize==ki->keySize());
            }
        }
    }

    virtual void setChooseNLimit(unsigned __int64 _rowLimit) override
    {
        // TODO ?
    }

    virtual void reset(bool crappyHack)
    {
        if (keyCursor)
        {
            if (!started)
            {
                started = true;
                numsegs = segs.ordinality();
                segs.checkSize(keyedSize, keyName.get());
            }
            if (!crappyHack)
            {
                matched = false;
                eof = false;
                setLow(0);
                keyCursor->reset();
            }
        }
    }

    virtual void releaseSegmentMonitors()
    {
        segs.reset();
        started = false;
        if(layoutTransSegCtx)
            layoutTransSegCtx->reset();
    }

    virtual void append(IKeySegmentMonitor *segment) 
    { 
        assertex(!started);
        activitySegs->append(segment);
    }


    virtual void append(FFoption option, IFieldFilter * filter)
    {
        UNIMPLEMENTED;
    }

    virtual unsigned ordinality() const 
    {
        return activitySegs->ordinality();
    }

    virtual IKeySegmentMonitor *item(unsigned idx) const
    {
        return activitySegs->item(idx);
    }

    inline const byte *queryKeyBuffer()
    {
        if(layoutTrans)
            return doRowLayoutTransform();
        else
            return reinterpret_cast<byte const *>(keyBuffer);
    }

    inline size32_t queryRowSize()
    {
        if (layoutTrans)
            return layoutSize;
        else
            return keyCursor ? keyCursor->getSize() : 0;
    }

    inline unsigned __int64 querySequence()
    {
        return keyCursor ? keyCursor->getSequence() : 0;
    }

    inline unsigned queryRecordSize() { return keySize; }

    bool _lookup(bool exact, unsigned lastSeg)
    {
        bool ret = false;
        unsigned lwildseeks = 0;
        unsigned lseeks = 0;
        unsigned lscans = 0;
        while (!eof)
        {
            bool ok;
            if (matched)
            {
                ok = keyCursor->next(keyBuffer);
                lscans++;
            }
            else
            {
                ok = keyCursor->gtEqual(keyBuffer, keyBuffer, true);
                lseeks++;
            }
            if (ok)
            {
                unsigned i = 0;
                matched = true;
                if (segs.segMonitors.length())
                {
                    for (; i <= lastSeg; i++)
                    {
                        matched = segs.segMonitors.item(i).matchesBuffer(keyBuffer);
                        if (!matched)
                            break;
                    }
                }
                if (matched)
                {
                    ret = true;
                    break;
                }
#ifdef  __linux__
                if (linuxYield)
                    sched_yield();
#endif
                eof = !incrementKey(i);
                if (!exact)
                {
                    ret = true;
                    break;
                }
                lwildseeks++;
            }
            else
                eof = true;
        }
        if (logExcessiveSeeks && lwildseeks > 1000)
            reportExcessiveSeeks(lwildseeks, lastSeg);
        noteSeeks(lseeks, lscans, lwildseeks);
        return ret;
    }

    virtual bool lookup(bool exact)
    {
        if (keyCursor)
            return _lookup(exact, segs.lastRealSeg());
        else
            return false;
    }

    virtual bool lookupSkip(const void *seek, size32_t seekOffset, size32_t seeklen)
    {
        if (keyCursor)
        {
            if (skipTo(seek, seekOffset, seeklen))
                noteSkips(1, 0);
            else
                noteSkips(0, 1);
            bool ret = _lookup(true, segs.lastRealSeg());
#ifdef _DEBUG
            if (traceSmartStepping)
            {
                StringBuffer recstr;
                unsigned i;
                for (i = 0; i < keySize; i++)
                {
                    unsigned char c = ((unsigned char *) keyBuffer)[i];
                    recstr.appendf("%c", isprint(c) ? c : '.');
                }
                recstr.append ("    ");
                for (i = 0; i < keySize; i++)
                {
                    recstr.appendf("%02x ", ((unsigned char *) keyBuffer)[i]);
                }
                DBGLOG("SKIP: Got skips=%02d nullSkips=%02d seeks=%02d scans=%02d : %s", skips, nullSkips, seeks, scans, recstr.str());
            }
#endif
            return ret;
        }
        else
            return false;
    }

    unsigned __int64 getCount()
    {
        matched = false;
        eof = false;
        setLow(0);
        keyCursor->reset();
        unsigned __int64 result = 0;
        unsigned lseeks = 0;
        if (keyCursor)
        {
            unsigned lastRealSeg = segs.lastRealSeg();
            for (;;)
            {
                if (_lookup(true, lastRealSeg))
                {
                    unsigned __int64 locount = keyCursor->getSequence();
                    endRange(lastRealSeg);
                    keyCursor->ltEqual(keyBuffer, NULL, true);
                    lseeks++;
                    result += keyCursor->getSequence()-locount+1;
                    if (!incrementKey(lastRealSeg))
                        break;
                    matched = false;
                }
                else
                    break;
            }
        }
        noteSeeks(lseeks, 0, 0);
        return result;
    }

    unsigned __int64 getCurrentRangeCount(unsigned groupSegCount)
    {
        unsigned __int64 result = 0;
        if (keyCursor)
        {
            unsigned __int64 locount = keyCursor->getSequence();
            endRange(groupSegCount);
            keyCursor->ltEqual(keyBuffer, NULL, true);
            result = keyCursor->getSequence()-locount+1;
            noteSeeks(1, 0, 0);
        }
        return result;
    }

    bool nextRange(unsigned groupSegCount)
    {
        if (!incrementKey(groupSegCount-1))
            return false;
        matched = false;
        return true;
    }

    unsigned __int64 checkCount(unsigned __int64 max)
    {
        matched = false;
        eof = false;
        setLow(0);
        keyCursor->reset();
        unsigned __int64 result = 0;
        unsigned lseeks = 0;
        if (keyCursor)
        {
            unsigned lastFullSeg = segs.lastFullSeg();
            if (lastFullSeg == (unsigned) -1)
            {
                noteSeeks(1, 0, 0);
                if (keyCursor->last(NULL))
                    return keyCursor->getSequence()+1;
                else
                    return 0;
            }
            for (;;)
            {
                if (_lookup(true, lastFullSeg))
                {
                    unsigned __int64 locount = keyCursor->getSequence();
                    endRange(lastFullSeg);
                    keyCursor->ltEqual(keyBuffer, NULL, true);
                    lseeks++;
                    result += keyCursor->getSequence()-locount+1;
                    if (max && (result > max))
                        break;
                    if (!incrementKey(lastFullSeg))
                        break;
                    matched = false;
                }
                else
                    break;
            }
        }
        noteSeeks(lseeks, 0, 0);
        return result;
    }

    virtual void serializeCursorPos(MemoryBuffer &mb)
    {
        mb.append(eof);
        if (!eof)
        {
            keyCursor->serializeCursorPos(mb);
            mb.append(matched);
        }
    }

    virtual void deserializeCursorPos(MemoryBuffer &mb)
    {
        mb.read(eof);
        if (!eof)
        {
            assertex(keyBuffer);
            keyCursor->deserializeCursorPos(mb, keyBuffer);
            mb.read(matched);
        }
    }

    virtual const byte *loadBlob(unsigned __int64 blobid, size32_t &blobsize)
    {
        return keyCursor->loadBlob(blobid, blobsize);
    }

    virtual void releaseBlobs()
    {
        if (keyCursor)
            keyCursor->releaseBlobs();
    }

    virtual void setLayoutTranslator(IRecordLayoutTranslator * trans)
    {
        layoutTrans.set(trans);
        if(layoutTrans && layoutTrans->queryKeysTransformed())
        {
            transformSegs = true;
            layoutTransSegCtx.setown(layoutTrans->getSegmentMonitorContext());
            activitySegs = layoutTransSegCtx;
        }
        else
        {
            transformSegs = false;
            layoutTransSegCtx.clear();
            activitySegs = &segs;
        }
        if(layoutTrans)
        {
            layoutTransRowCtx.setown(layoutTrans->getRowTransformContext());
        }
        else
        {
            layoutTransRowCtx.clear();
        }
    }

    virtual void setSegmentMonitors(SegMonitorList &segmentMonitors) override
    {
        segs.swapWith(segmentMonitors);
    }

    virtual void deserializeSegmentMonitors(MemoryBuffer &mb) override
    {
        segs.deserialize(mb);
    }

    virtual void finishSegmentMonitors()
    {
        segs.finish(keyedSize);
        if(transformSegs)
        {
            layoutTrans->createDiskSegmentMonitors(*layoutTransSegCtx, segs);
            if(!layoutTrans->querySuccess())
            {
                StringBuffer err;
                err.append("Could not translate index read filters during layout translation of index ").append(keyName.get()).append(": ");
                layoutTrans->queryFailure().getDetail(err);
                throw MakeStringExceptionDirect(0, err.str());
            }
        }
    }
};


///////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////

// For some reason #pragma pack does not seem to work here. Force all elements to 8 bytes
class CKeyIdAndPos
{
public:
    unsigned __int64 keyId;
    offset_t pos;

    CKeyIdAndPos(unsigned __int64 _keyId, offset_t _pos) { keyId = _keyId; pos = _pos; }

    bool operator==(const CKeyIdAndPos &other) { return keyId == other.keyId && pos == other.pos; }
};

class CNodeMapping : public HTMapping<CJHTreeNode, CKeyIdAndPos>
{
public:
    CNodeMapping(CKeyIdAndPos &fp, CJHTreeNode &et) : HTMapping<CJHTreeNode, CKeyIdAndPos>(et, fp) { }
    ~CNodeMapping() { this->et.Release(); }
    CJHTreeNode &query() { return queryElement(); }
};
typedef OwningSimpleHashTableOf<CNodeMapping, CKeyIdAndPos> CNodeTable;
#define FIXED_NODE_OVERHEAD (sizeof(CJHTreeNode))
class CNodeMRUCache : public CMRUCacheOf<CKeyIdAndPos, CJHTreeNode, CNodeMapping, CNodeTable>
{
    size32_t sizeInMem, memLimit;
public:
    CNodeMRUCache(size32_t _memLimit) : memLimit(0)
    {
        sizeInMem = 0;
        setMemLimit(_memLimit);
    }
    size32_t setMemLimit(size32_t _memLimit)
    {
        size32_t oldMemLimit = memLimit;
        memLimit = _memLimit;
        if (full())
            makeSpace();
        return oldMemLimit;
    }
    virtual void makeSpace()
    {
        // remove LRU until !full
        do
        {
            clear(1);
        }
        while (full());
    }
    virtual bool full()
    {
        if (((size32_t)-1) == memLimit) return false;
        return sizeInMem > memLimit;
    }
    virtual void elementAdded(CNodeMapping *mapping)
    {
        CJHTreeNode &node = mapping->queryElement();
        sizeInMem += (FIXED_NODE_OVERHEAD+node.getMemSize());
    }
    virtual void elementRemoved(CNodeMapping *mapping)
    {
        CJHTreeNode &node = mapping->queryElement();
        sizeInMem -= (FIXED_NODE_OVERHEAD+node.getMemSize());
    }
};

class CNodeCache : public CInterface
{
private:
    mutable CriticalSection lock;
    CNodeMRUCache nodeCache;
    CNodeMRUCache leafCache;
    CNodeMRUCache blobCache;
    CNodeMRUCache preloadCache;
    bool cacheNodes; 
    bool cacheLeaves;
    bool cacheBlobs;
    bool preloadNodes;
public:
    CNodeCache(size32_t maxNodeMem, size32_t maxLeaveMem, size32_t maxBlobMem)
        : nodeCache(maxNodeMem), leafCache(maxLeaveMem), blobCache(maxBlobMem), preloadCache((unsigned) -1)
    {
        cacheNodes = maxNodeMem != 0;
        cacheLeaves = maxLeaveMem != 0;;
        cacheBlobs = maxBlobMem != 0;
        preloadNodes = false;
        // note that each index caches the last blob it unpacked so that sequential blobfetches are still ok
    }
    CJHTreeNode *getNode(INodeLoader *key, int keyID, offset_t pos, IContextLogger *ctx, bool isTLK);
    void preload(CJHTreeNode *node, int keyID, offset_t pos, IContextLogger *ctx);

    bool isPreloaded(int keyID, offset_t pos);

    inline bool getNodeCachePreload() 
    {
        return preloadNodes;
    }

    inline bool setNodeCachePreload(bool _preload)
    {
        bool oldPreloadNodes = preloadNodes;
        preloadNodes = _preload;
        return oldPreloadNodes;
    }

    inline size32_t setNodeCacheMem(size32_t newSize)
    {
        CriticalBlock block(lock);
        unsigned oldV = nodeCache.setMemLimit(newSize);
        cacheNodes = (newSize != 0);
        return oldV;
    }
    inline size32_t setLeafCacheMem(size32_t newSize)
    {
        CriticalBlock block(lock);
        unsigned oldV = leafCache.setMemLimit(newSize);
        cacheLeaves = (newSize != 0); 
        return oldV;
    }
    inline size32_t setBlobCacheMem(size32_t newSize)
    {
        CriticalBlock block(lock);
        unsigned oldV = blobCache.setMemLimit(newSize);
        cacheBlobs = (newSize != 0); 
        return oldV;
    }
    void clear()
    {
        CriticalBlock block(lock);
        nodeCache.kill();
        leafCache.kill();
        blobCache.kill();
    }
};

static inline CNodeCache *queryNodeCache()
{
    if (nodeCache) return nodeCache; // avoid crit
    CriticalBlock b(*initCrit);
    if (!nodeCache) nodeCache = new CNodeCache(100*0x100000, 50*0x100000, 0);
    return nodeCache;
}

void clearNodeCache()
{
    queryNodeCache()->clear();
}


inline CKeyStore *queryKeyStore()
{
    CKeyStore * value = keyStore.load(std::memory_order_acquire);
    if (value) return value; // avoid crit
    CriticalBlock b(*initCrit);
    if (!keyStore.load(std::memory_order_acquire)) keyStore = new CKeyStore;
    return keyStore;
}

unsigned setKeyIndexCacheSize(unsigned limit)
{
    return queryKeyStore()->setKeyCacheLimit(limit);
}

CKeyStore::CKeyStore() : keyIndexCache(defaultKeyIndexLimit)
{
    nextId = 0;
#if 0
    mm.setown(createSharedMemoryManager("RichardsSharedMemManager", 0x100000));
    try
    {
        if (mm)
            sharedCache.setown(mm->share());
    }
    catch (IException *E)
    {
        E->Release();
    }
#endif
}

CKeyStore::~CKeyStore()
{
}

unsigned CKeyStore::setKeyCacheLimit(unsigned limit)
{
    return keyIndexCache.setCacheLimit(limit);
}

IKeyIndex *CKeyStore::doload(const char *fileName, unsigned crc, IReplicatedFile *part, IFileIO *iFileIO, IMemoryMappedFile *iMappedFile, bool isTLK, bool allowPreload)
{
    // isTLK provided by caller since flags in key header unreliable. If either say it's a TLK, I believe it.
    {
        MTIME_SECTION(queryActiveTimer(), "CKeyStore_load");
        IKeyIndex *keyIndex;

        // MORE - holds onto the mutex way too long
        synchronized block(mutex);
        StringBuffer fname;
        fname.append(fileName).append('/').append(crc);
        keyIndex = keyIndexCache.query(fname);
        if (NULL == keyIndex)
        {
            if (iMappedFile)
            {
                assert(!iFileIO && !part);
                keyIndex = new CMemKeyIndex(getUniqId(), LINK(iMappedFile), fname, isTLK);
            }
            else if (iFileIO)
            {
                assert(!part);
                keyIndex = new CDiskKeyIndex(getUniqId(), LINK(iFileIO), fname, isTLK, allowPreload);
            }
            else
            {
                Owned<IFile> iFile;
                if (part)
                {
                    iFile.setown(part->open());
                    if (NULL == iFile.get())
                        throw MakeStringException(0, "Failed to open index file %s", fileName);
                }
                else
                    iFile.setown(createIFile(fileName));
                IFileIO *fio = iFile->open(IFOread);
                if (fio)
                    keyIndex = new CDiskKeyIndex(getUniqId(), fio, fname, isTLK, allowPreload);
                else
                    throw MakeStringException(0, "Failed to open index file %s", fileName);
            }
            keyIndexCache.add(fname, *LINK(keyIndex));
        }
        else
        {
            LINK(keyIndex);
        }
        assertex(NULL != keyIndex);
        return keyIndex;
    }
}

IKeyIndex *CKeyStore::load(const char *fileName, unsigned crc, IFileIO *iFileIO, bool isTLK, bool allowPreload)
{
    return doload(fileName, crc, NULL, iFileIO, NULL, isTLK, allowPreload);
}

IKeyIndex *CKeyStore::load(const char *fileName, unsigned crc, IMemoryMappedFile *iMappedFile, bool isTLK, bool allowPreload)
{
    return doload(fileName, crc, NULL, NULL, iMappedFile, isTLK, allowPreload);
}

// fileName+crc used only as key for cache
IKeyIndex *CKeyStore::load(const char *fileName, unsigned crc, IReplicatedFile &part, bool isTLK, bool allowPreload)
{
    return doload(fileName, crc, &part, NULL, NULL, isTLK, allowPreload);
}

IKeyIndex *CKeyStore::load(const char *fileName, unsigned crc, bool isTLK, bool allowPreload)
{
    return doload(fileName, crc, NULL, NULL, NULL, isTLK, allowPreload);
}

StringBuffer &CKeyStore::getMetrics(StringBuffer &xml)
{
    xml.append(" <IndexMetrics>\n");

    Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();
    ForEach(*iter)
    {           
        CKeyIndexMapping &mapping = iter->query();
        IKeyIndex &index = mapping.query();
        const char *name = mapping.queryFindString();
        xml.appendf(" <Index name=\"%s\" scans=\"%d\" seeks=\"%d\"/>\n", name, index.queryScans(), index.querySeeks());
    }
    xml.append(" </IndexMetrics>\n");
    return xml;
}


void CKeyStore::resetMetrics()
{
    synchronized block(mutex);

    Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();
    ForEach(*iter)
    {           
        CKeyIndexMapping &mapping = iter->query();
        IKeyIndex &index = mapping.query();
        index.resetCounts();
    }
}

void CKeyStore::clearCache(bool killAll)
{
    synchronized block(mutex);

    if (killAll)
    {
        clearNodeCache(); // no point in keeping old nodes cached if key store cache has been cleared
        keyIndexCache.kill();
    }
    else
    {
        StringArray goers;
        Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();
        ForEach(*iter)
        {           
            CKeyIndexMapping &mapping = iter->query();
            IKeyIndex &index = mapping.query();
            if (!index.IsShared())
            {
                const char *name = mapping.queryFindString();
                goers.append(name);
            }
        }
        ForEachItemIn(idx, goers)
        {
            keyIndexCache.remove(goers.item(idx));
        }
    }
}

void CKeyStore::clearCacheEntry(const char *keyName)
{
    if (!keyName || !*keyName)
        return;  // nothing to do

    synchronized block(mutex);
    Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();

    StringArray goers;
    ForEach(*iter)
    {           
        CKeyIndexMapping &mapping = iter->query();
        IKeyIndex &index = mapping.query();
        if (!index.IsShared())
        {
            const char *name = mapping.queryFindString();
            if (strstr(name, keyName) != 0)  // keyName doesn't have drive or part number associated with it
                goers.append(name);
        }
    }
    ForEachItemIn(idx, goers)
    {
        keyIndexCache.remove(goers.item(idx));
    }
}

void CKeyStore::clearCacheEntry(const IFileIO *io)
{
    synchronized block(mutex);
    Owned<CKeyIndexMRUCache::CMRUIterator> iter = keyIndexCache.getIterator();

    StringArray goers;
    ForEach(*iter)
    {
        CKeyIndexMapping &mapping = iter->query();
        IKeyIndex &index = mapping.query();
        if (!index.IsShared())
        {
            if (index.queryFileIO()==io)
                goers.append(mapping.queryFindString());
        }
    }
    ForEachItemIn(idx, goers)
    {
        keyIndexCache.remove(goers.item(idx));
    }
}

// CKeyIndex impl.

CKeyIndex::CKeyIndex(int _iD, const char *_name) : name(_name)
{
    iD = _iD;
    cache = queryNodeCache(); // use one node cache for all key indexes;
    cache->Link();
    keyHdr = NULL;
    rootNode = NULL;
    cachedBlobNodePos = 0;
    keySeeks.store(0);
    keyScans.store(0);
    latestGetNodeOffset = 0;
}

void CKeyIndex::cacheNodes(CNodeCache *cache, offset_t nodePos, bool isTLK)
{
    bool first = true;
    while (nodePos)
    {
        Owned<CJHTreeNode> node = loadNode(nodePos);
        if (node->isLeaf())
        {
            if (!isTLK)
                return;
        }
        else if (first)
        {
            cacheNodes(cache, node->getFPosAt(0), isTLK);
            first = false;
        }
        cache->preload(node, iD, nodePos, NULL);
        nodePos = node->getRightSib();
    }
}

void CKeyIndex::init(KeyHdr &hdr, bool isTLK, bool allowPreload)
{
    if (isTLK)
        hdr.ktype |= HTREE_TOPLEVEL_KEY; // thor does not set
    keyHdr = new CKeyHdr();
    try
    {
        keyHdr->load(hdr);
    }
    catch (IKeyException *ke)
    {
        if (!name.get()) throw;
        StringBuffer msg;
        IKeyException *ke2 = MakeKeyException(ke->errorCode(), "%s. In key '%s'.", ke->errorMessage(msg).str(), name.get());
        ke->Release();
        throw ke2;
    }
    offset_t rootPos = keyHdr->getRootFPos();
    Linked<CNodeCache> nodeCache = queryNodeCache();
    if (allowPreload)
    {
        if (nodeCache->getNodeCachePreload() && !nodeCache->isPreloaded(iD, rootPos))
        {
            cacheNodes(nodeCache, rootPos, isTLK);
        }
    }
    rootNode = nodeCache->getNode(this, iD, rootPos, NULL, isTLK);
}

CKeyIndex::~CKeyIndex()
{
    ::Release(keyHdr);
    ::Release(cache);
    ::Release(rootNode);
}

CMemKeyIndex::CMemKeyIndex(int _iD, IMemoryMappedFile *_io, const char *_name, bool isTLK)
    : CKeyIndex(_iD, _name)
{
    io.setown(_io);
    assertex(io->offset()==0);                  // mapped whole file
    assertex(io->length()==io->fileSize());     // mapped whole file
    KeyHdr hdr;
    if (io->length() < sizeof(hdr))
        throw MakeStringException(0, "Failed to read key header: file too small, could not read %u bytes", (unsigned) sizeof(hdr));
    memcpy(&hdr, io->base(), sizeof(hdr));
    init(hdr, isTLK, false);
}

CJHTreeNode *CMemKeyIndex::loadNode(offset_t pos)
{
    nodesLoaded++;
    if (pos + keyHdr->getNodeSize() > io->fileSize())
    {
        IException *E = MakeStringException(errno, "Error reading node at position %" I64F "x past EOF", pos); 
        StringBuffer m;
        m.appendf("In key %s, position 0x%" I64F "x", name.get(), pos);
        EXCLOG(E, m.str());
        throw E;
    }
    char *nodeData = (char *) (io->base() + pos);
    MTIME_SECTION(queryActiveTimer(), "JHTREE read node");
    return CKeyIndex::loadNode(nodeData, pos, false);
}

CDiskKeyIndex::CDiskKeyIndex(int _iD, IFileIO *_io, const char *_name, bool isTLK, bool allowPreload)
    : CKeyIndex(_iD, _name)
{
    io.setown(_io);
    KeyHdr hdr;
    if (io->read(0, sizeof(hdr), &hdr) != sizeof(hdr))
        throw MakeStringException(0, "Failed to read key header: file too small, could not read %u bytes", (unsigned) sizeof(hdr));
    init(hdr, isTLK, allowPreload);
}

CJHTreeNode *CDiskKeyIndex::loadNode(offset_t pos) 
{
    nodesLoaded++;
    unsigned nodeSize = keyHdr->getNodeSize();
    MemoryAttr ma;
    char *nodeData = (char *) ma.allocate(nodeSize);
    MTIME_SECTION(queryActiveTimer(), "JHTREE read node");
    if (io->read(pos, nodeSize, nodeData) != nodeSize)
    {
        IException *E = MakeStringException(errno, "Error %d reading node at position %" I64F "x", errno, pos); 
        StringBuffer m;
        m.appendf("In key %s, position 0x%" I64F "x", name.get(), pos);
        EXCLOG(E, m.str());
        throw E;
    }
    return CKeyIndex::loadNode(nodeData, pos, true);
}

CJHTreeNode *CKeyIndex::loadNode(char *nodeData, offset_t pos, bool needsCopy) 
{
    try
    {
        Owned<CJHTreeNode> ret;
        char leafFlag = ((NodeHdr *) nodeData)->leafFlag;
        switch(leafFlag)
        {
        case 0:
            ret.setown(new CJHTreeNode());
            break;
        case 1:
        	if (keyHdr->isVariable())
        		ret.setown(new CJHVarTreeNode());
        	else
        		ret.setown(new CJHTreeNode());
            break;
        case 2:
            ret.setown(new CJHTreeBlobNode());
            break;
        case 3:
            ret.setown(new CJHTreeMetadataNode());
            break;
        default:
            throwUnexpected();
        }
        {
            MTIME_SECTION(queryActiveTimer(), "JHTREE load node");
            ret->load(keyHdr, nodeData, pos, true);
        }
        return ret.getClear();
    }
    catch (IException *E)
    {
        StringBuffer m;
        m.appendf("In key %s, position 0x%" I64F "x", name.get(), pos);
        EXCLOG(E, m.str());
        throw;
    }
    catch (...)
    {
        DBGLOG("Unknown exception in key %s, position 0x%" I64F "x", name.get(), pos);
        throw;
    }
}

bool CKeyIndex::isTopLevelKey()
{
    return (keyHdr->getKeyType() & HTREE_TOPLEVEL_KEY) != 0;
}

bool CKeyIndex::isFullySorted()
{
    return (keyHdr->getKeyType() & HTREE_FULLSORT_KEY) != 0;
}

IKeyCursor *CKeyIndex::getCursor(IContextLogger *ctx)
{
    return new CKeyCursor(*this, ctx);      // MORE - pool them?
}

CJHTreeNode *CKeyIndex::getNode(offset_t offset, IContextLogger *ctx) 
{ 
    latestGetNodeOffset = offset;
    return cache->getNode(this, iD, offset, ctx, isTopLevelKey()); 
}

void dumpNode(FILE *out, CJHTreeNode *node, int length, unsigned rowCount, bool raw)
{
    if (!raw)
        fprintf(out, "Node dump: fpos(%" I64F "d) leaf(%d)\n", node->getFpos(), node->isLeaf());
    if (rowCount==0 || rowCount > node->getNumKeys())
        rowCount = node->getNumKeys();
    for (unsigned int i=0; i<rowCount; i++)
    {
        char *dst = (char *) alloca(node->getKeyLen()+50);
        node->getValueAt(i, dst);
        if (raw)
        {
            fwrite(dst, 1, length, out);
        }
        else
        {
            offset_t pos = node->getFPosAt(i);
            StringBuffer s;
            appendURL(&s, dst, length, true);
            fprintf(out, "keyVal %d [%" I64F "d] = %s\n", i, pos, s.str());
        }
    }
    if (!raw)
        fprintf(out, "==========\n");
}

void CKeyIndex::dumpNode(FILE *out, offset_t pos, unsigned count, bool isRaw)
{
    Owned<CJHTreeNode> node = loadNode(pos);
    ::dumpNode(out, node, keySize(), count, isRaw);
}

bool CKeyIndex::hasSpecialFileposition() const
{
    return keyHdr->hasSpecialFileposition();
}

size32_t CKeyIndex::keySize()
{
    size32_t fileposSize = keyHdr->hasSpecialFileposition() ? sizeof(offset_t) : 0;
    return keyHdr->getMaxKeyLength() + fileposSize;
}

size32_t CKeyIndex::keyedSize()
{
    return keyHdr->getNodeKeyLength();
}

bool CKeyIndex::hasPayload()
{
    return keyHdr->hasPayload();
}

CJHTreeBlobNode *CKeyIndex::getBlobNode(offset_t nodepos)
{
    CriticalBlock b(blobCacheCrit);
    if (nodepos != cachedBlobNodePos)
    {
        cachedBlobNode.setown(QUERYINTERFACE(loadNode(nodepos), CJHTreeBlobNode)); // note - don't use the cache
        cachedBlobNodePos = nodepos;
    }
    return cachedBlobNode.getLink();
}

const byte *CKeyIndex::loadBlob(unsigned __int64 blobid, size32_t &blobSize)
{
    offset_t nodepos = blobid & I64C(0xffffffffffff);
    size32_t offset = (size32_t) ((blobid & I64C(0xffff000000000000)) >> 44);

    Owned<CJHTreeBlobNode> blobNode = getBlobNode(nodepos);
    size32_t sizeRemaining = blobNode->getTotalBlobSize(offset);
    blobSize = sizeRemaining;
    byte *ret = (byte *) malloc(sizeRemaining);
    byte *finger = ret;
    for (;;)
    {
        size32_t gotHere = blobNode->getBlobData(offset, finger);
        assertex(gotHere <= sizeRemaining);
        sizeRemaining -= gotHere;
        finger += gotHere;
        if (!sizeRemaining)
            break;
        blobNode.setown(getBlobNode(blobNode->getRightSib()));
        offset = 0;
    }
    return ret;
}

offset_t CKeyIndex::queryMetadataHead()
{
    offset_t ret = keyHdr->getHdrStruct()->metadataHead;
    if(ret == static_cast<offset_t>(-1)) ret = 0; // index created before introduction of metadata would have FFFF... in this space
    return ret;
}

IPropertyTree * CKeyIndex::getMetadata()
{
    offset_t nodepos = queryMetadataHead();
    if(!nodepos)
        return NULL;
    Owned<CJHTreeMetadataNode> node;
    StringBuffer xml;
    while(nodepos)
    {
        node.setown(QUERYINTERFACE(loadNode(nodepos), CJHTreeMetadataNode));
        node->get(xml);
        nodepos = node->getRightSib();
    }
    IPropertyTree * ret;
    try
    {
        ret = createPTreeFromXMLString(xml.str());
    }
    catch(IPTreeReadException * e)
    {
        StringBuffer emsg;
        IException * wrapped = MakeStringException(e->errorAudience(), e->errorCode(), "Error retrieving XML metadata: %s", e->errorMessage(emsg).str());
        e->Release();
        throw wrapped;
    }
    return ret;
}

CKeyCursor::CKeyCursor(CKeyIndex &_key, IContextLogger *_ctx)
    : key(_key), ctx(_ctx)
{
    key.Link();
    nodeKey = 0;
}

CKeyCursor::~CKeyCursor()
{
    key.Release();
    releaseBlobs();
}

void CKeyCursor::reset()
{
    node.clear();
}

CJHTreeNode *CKeyCursor::locateFirstNode()
{
    CJHTreeNode * n = 0;
    CJHTreeNode * p = LINK(key.rootNode);
    while (p != 0)
    {
        n = p;
        p = key.getNode(n->prevNodeFpos(), ctx);
        if (p != 0)
            n->Release();
    }
    return n;
}

CJHTreeNode *CKeyCursor::locateLastNode()
{
    CJHTreeNode * n = 0;
    CJHTreeNode * p = LINK(key.rootNode);

    while (p != 0)
    {
        n = p;
        p = key.getNode(n->nextNodeFpos(), ctx);
        if (p != 0)
            n->Release();
    }

    return n;
}

bool CKeyCursor::next(char *dst)
{
    if (!node)
        return first(dst);
    else
    {
        key.keyScans++;
        if (!node->getValueAt( ++nodeKey, dst))
        {
            offset_t rsib = node->getRightSib();
            node.clear();
            if (rsib != 0)
            {
                node.setown(key.getNode(rsib, ctx));
                if (node != NULL)
                {
                    nodeKey = 0;
                    return node->getValueAt(0, dst);
                }
            }
            return false;
        }
        else
            return true;
    }
}

bool CKeyCursor::prev(char *dst)
{
    if (!node)
        return last(dst); // Note - this used to say First - surely a typo
    else
    {
        key.keyScans++;
        if (!nodeKey)
        {
            offset_t lsib = node->getLeftSib();
            node.clear();
            
            if (lsib != 0)
            {
                node.setown(key.getNode(lsib, ctx));
                if (node)
                {
                    nodeKey = node->getNumKeys()-1;
                    return node->getValueAt(nodeKey, dst );
                }
            }
            return false;
        }
        else
            return node->getValueAt(--nodeKey, dst);
    }
}

size32_t CKeyCursor::getSize()
{
    assertex(node);
    return node->getSizeAt(nodeKey);
}

offset_t CKeyCursor::getFPos()
{
    assertex(node);
    return node->getFPosAt(nodeKey);
}

unsigned __int64 CKeyCursor::getSequence()
{
    assertex(node);
    return node->getSequence(nodeKey);
}

bool CKeyCursor::first(char *dst)
{
    key.keySeeks++;
    node.setown(locateFirstNode());
    nodeKey = 0;
    return node->getValueAt(nodeKey, dst);
}

bool CKeyCursor::last(char *dst)
{
    key.keySeeks++;
    node.setown(locateLastNode());
    nodeKey = node->getNumKeys()-1;
    return node->getValueAt( nodeKey, dst );
}

bool CKeyCursor::gtEqual(const char *src, char *dst, bool seekForward)
{
    key.keySeeks++;
    unsigned lwm = 0;
    if (seekForward && node)
    {
        // When seeking forward, there are two cases worth optimizing:
        // 1. the next record is actually the one we want
        // 2. The record we want is on the current page
        unsigned numKeys = node->getNumKeys();
        if (nodeKey < numKeys-1)
        {   
            int rc = node->compareValueAt(src, ++nodeKey);
            if (rc <= 0)
            {
                node->getValueAt(nodeKey, dst);
                return true; 
            }
            if (nodeKey < numKeys-1)
            {
                rc = node->compareValueAt(src, numKeys-1);
                if (rc <= 0)
                    lwm = nodeKey+1;
            }
        }
    }
    if (!lwm)
        node.set(key.rootNode);
    for (;;)
    {
        unsigned int a = lwm;
        int b = node->getNumKeys();
        // first search for first GTE entry (result in b(<),a(>=))
        while ((int)a<b)
        {
            int i = a+(b-a)/2;
            int rc = node->compareValueAt(src, i);
            if (rc>0)
                a = i+1;
            else
                b = i;
        }
        if (node->isLeaf())
        {
            if (a<node->getNumKeys())
                nodeKey = a;
            else
            {
                offset_t nextPos = node->nextNodeFpos();  // This can happen at eof because of key peculiarity where level above reports ffff as last
                node.setown(key.getNode(nextPos, ctx));
                nodeKey = 0;
            }
            if (node)
            {
                node->getValueAt(nodeKey, dst);
                return true; 
            }
            else
                return false;
        }
        else
        {
            if (a<node->getNumKeys())
            {
                offset_t npos = node->getFPosAt(a);
                node.setown(key.getNode(npos, ctx));
            }
            else
                return false;
        }
    }
}

bool CKeyCursor::ltEqual(const char *src, char *dst, bool seekForward)
{
    key.keySeeks++;
    unsigned lwm = 0;
    if (seekForward && node)
    {
        // When seeking forward, there are two cases worth optimizing:
        // 1. next record is > src, so we return current
        // 2. The record we want is on the current page
        unsigned numKeys = node->getNumKeys();
        if (nodeKey < numKeys-1)
        {   
            int rc = node->compareValueAt(src, ++nodeKey);
            if (rc < 0)
            {
                node->getValueAt(--nodeKey, dst);
                return true; 
            }
            if (nodeKey < numKeys-1)
            {
                rc = node->compareValueAt(src, numKeys-1);
                if (rc < 0)
                    lwm = nodeKey;
            }
        }
    }
    if (!lwm)
        node.set(key.rootNode);
    for (;;)
    {
        unsigned int a = lwm;
        int b = node->getNumKeys();
        // Locate first record greater than src
        while ((int)a<b)
        {
            int i = a+(b+1-a)/2;
            int rc = node->compareValueAt(src, i-1);
            if (rc>=0)
                a = i;
            else
                b = i-1;
        }
        if (node->isLeaf())
        {
            // record we want is the one before first record greater than src.
            if (a>0)
                nodeKey = a-1;
            else
            {
                offset_t prevPos = node->prevNodeFpos();
                node.setown(key.getNode(prevPos, ctx));
                if (node)
                    nodeKey = node->getNumKeys()-1;
            }
            if (node)
            {
                node->getValueAt(nodeKey, dst);
                return true; 
            }
            else
                return false;
        }
        else
        {
            // Node to look in is the first one one that ended greater than src.
            if (a==node->getNumKeys())
                a--;   // value being looked for is off the end of the index.
            offset_t npos = node->getFPosAt(a);
            node.setown(key.getNode(npos, ctx));
            if (!node)
                throw MakeStringException(0, "Invalid key %s: child node pointer should never be NULL", key.name.get());
        }
    }
}

void CKeyCursor::serializeCursorPos(MemoryBuffer &mb)
{
    if (node)
    {
        mb.append(node->getFpos());
        mb.append(nodeKey);
    }
    else
    {
        offset_t zero = 0;
        unsigned zero2 = 0;
        mb.append(zero);
        mb.append(zero2);
    }
}

void CKeyCursor::deserializeCursorPos(MemoryBuffer &mb, char *keyBuffer)
{
    offset_t nodeAddress;
    mb.read(nodeAddress);
    mb.read(nodeKey);
    if (nodeAddress)
    {
        node.setown(key.getNode(nodeAddress, ctx));
        if (node && keyBuffer)
            node->getValueAt(nodeKey, keyBuffer);
    }
    else
        node.clear();
}

const byte *CKeyCursor::loadBlob(unsigned __int64 blobid, size32_t &blobsize)
{
    const byte *ret = key.loadBlob(blobid, blobsize);
    activeBlobs.append(ret);
    return ret;
}

void CKeyCursor::releaseBlobs()
{
    ForEachItemIn(idx, activeBlobs)
    {
        free((void *) activeBlobs.item(idx));
    }
    activeBlobs.kill();
}

class CLazyKeyIndex : implements IKeyIndex, public CInterface
{
    StringAttr keyfile;
    unsigned crc; 
    Linked<IDelayedFile> delayedFile;
    Owned<IFileIO> iFileIO;
    Owned<IKeyIndex> realKey;
    CriticalSection c;
    bool isTLK;
    bool preloadAllowed;

    inline IKeyIndex &checkOpen()
    {
        CriticalBlock b(c);
        if (!realKey)
        {
            Owned<IMemoryMappedFile> mapped = useMemoryMappedIndexes ? delayedFile->getMappedFile() : nullptr;
            if (mapped)
                realKey.setown(queryKeyStore()->load(keyfile, crc, mapped, isTLK, preloadAllowed));
            else
            {
                iFileIO.setown(delayedFile->getFileIO());
                realKey.setown(queryKeyStore()->load(keyfile, crc, iFileIO, isTLK, preloadAllowed));
            }
            if (!realKey)
            {
                DBGLOG("Lazy key file %s could not be opened", keyfile.get());
                throw MakeStringException(0, "Lazy key file %s could not be opened", keyfile.get());
            }
        }
        return *realKey;
    }

public:
    IMPLEMENT_IINTERFACE;
    CLazyKeyIndex(const char *_keyfile, unsigned _crc, IDelayedFile *_delayedFile, bool _isTLK, bool _preloadAllowed)
        : keyfile(_keyfile), crc(_crc), delayedFile(_delayedFile), isTLK(_isTLK), preloadAllowed(_preloadAllowed)
    {}

    virtual bool IsShared() const { return CInterface::IsShared(); }

    virtual IKeyCursor *getCursor(IContextLogger *ctx) { return checkOpen().getCursor(ctx); }
    virtual size32_t keySize() { return checkOpen().keySize(); }
    virtual size32_t keyedSize() { return checkOpen().keyedSize(); }
    virtual bool hasPayload() { return checkOpen().hasPayload(); }
    virtual bool isTopLevelKey() { return checkOpen().isTopLevelKey(); }
    virtual bool isFullySorted() { return checkOpen().isFullySorted(); }
    virtual unsigned getFlags() { return checkOpen().getFlags(); }
    virtual void dumpNode(FILE *out, offset_t pos, unsigned count, bool isRaw) { checkOpen().dumpNode(out, pos, count, isRaw); }
    virtual unsigned numParts() { return 1; }
    virtual IKeyIndex *queryPart(unsigned idx) { return idx ? NULL : this; }
    virtual unsigned queryScans() { return realKey ? realKey->queryScans() : 0; }
    virtual unsigned querySeeks() { return realKey ? realKey->querySeeks() : 0; }
    virtual const char *queryFileName() { return keyfile.get(); }
    virtual offset_t queryBlobHead() { return checkOpen().queryBlobHead(); }
    virtual void resetCounts() { if (realKey) realKey->resetCounts(); }
    virtual offset_t queryLatestGetNodeOffset() const { return realKey ? realKey->queryLatestGetNodeOffset() : 0; }
    virtual offset_t queryMetadataHead() { return checkOpen().queryMetadataHead(); }
    virtual IPropertyTree * getMetadata() { return checkOpen().getMetadata(); }
    virtual unsigned getNodeSize() { return checkOpen().getNodeSize(); }
    virtual const IFileIO *queryFileIO() const override { return iFileIO; } // NB: if not yet opened, will be null
    virtual bool hasSpecialFileposition() const { return realKey ? realKey->hasSpecialFileposition() : false; }
};

extern jhtree_decl IKeyIndex *createKeyIndex(const char *keyfile, unsigned crc, IFileIO &iFileIO, bool isTLK, bool preloadAllowed)
{
    return queryKeyStore()->load(keyfile, crc, &iFileIO, isTLK, preloadAllowed);
}

extern jhtree_decl IKeyIndex *createKeyIndex(const char *keyfile, unsigned crc, bool isTLK, bool preloadAllowed)
{
    return queryKeyStore()->load(keyfile, crc, isTLK, preloadAllowed);
}

extern jhtree_decl IKeyIndex *createKeyIndex(IReplicatedFile &part, unsigned crc, bool isTLK, bool preloadAllowed)
{
    StringBuffer filePath;
    const RemoteFilename &rfn = part.queryCopies().item(0);
    rfn.getPath(filePath);
    return queryKeyStore()->load(filePath.str(), crc, part, isTLK, preloadAllowed);
}

extern jhtree_decl IKeyIndex *createKeyIndex(const char *keyfile, unsigned crc, IDelayedFile &iFileIO, bool isTLK, bool preloadAllowed)
{
    return new CLazyKeyIndex(keyfile, crc, &iFileIO, isTLK, preloadAllowed);
}

extern jhtree_decl void clearKeyStoreCache(bool killAll)
{
    queryKeyStore()->clearCache(killAll);
}

extern jhtree_decl void clearKeyStoreCacheEntry(const char *name)
{
    queryKeyStore()->clearCacheEntry(name);
}

extern jhtree_decl void clearKeyStoreCacheEntry(const IFileIO *io)
{
    queryKeyStore()->clearCacheEntry(io);
}

extern jhtree_decl StringBuffer &getIndexMetrics(StringBuffer &ret)
{
    return queryKeyStore()->getMetrics(ret);
}

extern jhtree_decl void resetIndexMetrics()
{
    queryKeyStore()->resetMetrics();
}

extern jhtree_decl bool isKeyFile(const char *filename)
{
    OwnedIFile file = createIFile(filename);
    OwnedIFileIO io = file->open(IFOread);
    unsigned __int64 size = file->size();
    if (size)
    {
        KeyHdr hdr;
        if (io->read(0, sizeof(hdr), &hdr) == sizeof(hdr))
        {
            _WINREV(hdr.phyrec);
            _WINREV(hdr.root);
            _WINREV(hdr.nodeSize);
            if (size % hdr.nodeSize == 0 && hdr.phyrec == size-1 && hdr.root && hdr.root % hdr.nodeSize == 0)
            {
                NodeHdr root;
                if (io->read(hdr.root, sizeof(root), &root) == sizeof(root))
                {
                    _WINREV(root.rightSib);
                    _WINREV(root.leftSib);
                    return root.leftSib==0 && root.rightSib==0;
                }
            }
        }
    }
    return false;
}

extern jhtree_decl bool setNodeCachePreload(bool preload)
{
    return queryNodeCache()->setNodeCachePreload(preload);
}

extern jhtree_decl size32_t setNodeCacheMem(size32_t cacheSize)
{
    return queryNodeCache()->setNodeCacheMem(cacheSize);
}

extern jhtree_decl size32_t setLeafCacheMem(size32_t cacheSize)
{
    return queryNodeCache()->setLeafCacheMem(cacheSize);
}

extern jhtree_decl size32_t setBlobCacheMem(size32_t cacheSize)
{
    return queryNodeCache()->setBlobCacheMem(cacheSize);
}


///////////////////////////////////////////////////////////////////////////////
// CNodeCache impl.
///////////////////////////////////////////////////////////////////////////////

CJHTreeNode *CNodeCache::getNode(INodeLoader *keyIndex, int iD, offset_t pos, IContextLogger *ctx, bool isTLK)
{
    // MORE - could probably be improved - I think having the cache template separate is not helping us here
    // Also one cache per key would surely be faster, and could still use a global total
    if (!pos)
        return NULL;
    { 
        // It's a shame that we don't know the type before we read it. But probably not that big a deal
        CriticalBlock block(lock);
        CKeyIdAndPos key(iD, pos);
        if (preloadNodes)
        {
            CJHTreeNode *cacheNode = preloadCache.query(key);
            if (cacheNode)
            {
                cacheHits++;
                if (ctx) ctx->noteStatistic(StNumPreloadCacheHits, 1);
                preloadCacheHits++;
                return LINK(cacheNode);
            }
        }
        if (cacheNodes)
        {
            CJHTreeNode *cacheNode = nodeCache.query(key);
            if (cacheNode)
            {
                cacheHits++;
                if (ctx) ctx->noteStatistic(StNumNodeCacheHits, 1);
                nodeCacheHits++;
                return LINK(cacheNode);
            }
        }
        if (cacheLeaves)
        {
            CJHTreeNode *cacheNode = leafCache.query(key);
            if (cacheNode)
            {
                cacheHits++;
                if (ctx) ctx->noteStatistic(StNumLeafCacheHits, 1);
                leafCacheHits++;
                return LINK(cacheNode);
            }
        }
        if (cacheBlobs)
        {
            CJHTreeNode *cacheNode = blobCache.query(key);
            if (cacheNode)
            {
                cacheHits++;
                if (ctx) ctx->noteStatistic(StNumBlobCacheHits, 1);
                blobCacheHits++;
                return LINK(cacheNode);
            }
        }
        CJHTreeNode *node;
        {
            CriticalUnblock block(lock);
            node = keyIndex->loadNode(pos);  // NOTE - don't want cache locked while we load!
        }
        cacheAdds++;
        if (node->isBlob())
        {
            if (cacheBlobs)
            {
                CJHTreeNode *cacheNode = blobCache.query(key); // check if added to cache while we were reading
                if (cacheNode)
                {
                    ::Release(node);
                    cacheHits++;
                    if (ctx) ctx->noteStatistic(StNumBlobCacheHits, 1);
                    blobCacheHits++;
                    return LINK(cacheNode);
                }
                if (ctx) ctx->noteStatistic(StNumBlobCacheAdds, 1);
                blobCacheAdds++;
                blobCache.add(key, *LINK(node));
            }
        }
        else if (node->isLeaf() && !isTLK) // leaves in TLK are cached as if they were nodes
        {
            if (cacheLeaves)
            {
                CJHTreeNode *cacheNode = leafCache.query(key); // check if added to cache while we were reading
                if (cacheNode)
                {
                    ::Release(node);
                    cacheHits++;
                    if (ctx) ctx->noteStatistic(StNumLeafCacheHits, 1);
                    leafCacheHits++;
                    return LINK(cacheNode);
                }
                if (ctx) ctx->noteStatistic(StNumLeafCacheAdds, 1);
                leafCacheAdds++;
                leafCache.add(key, *LINK(node));
            }
        }
        else
        {
            if (cacheNodes)
            {
                CJHTreeNode *cacheNode = nodeCache.query(key); // check if added to cache while we were reading
                if (cacheNode)
                {
                    ::Release(node);
                    cacheHits++;
                    if (ctx) ctx->noteStatistic(StNumNodeCacheHits, 1);
                    nodeCacheHits++;
                    return LINK(cacheNode);
                }
                if (ctx) ctx->noteStatistic(StNumNodeCacheAdds, 1);
                nodeCacheAdds++;
                nodeCache.add(key, *LINK(node));
            }
        }
        return node;
    }
}

void CNodeCache::preload(CJHTreeNode *node, int iD, offset_t pos, IContextLogger *ctx)
{
    assertex(pos);
    assertex(preloadNodes);
    CriticalBlock block(lock);
    CKeyIdAndPos key(iD, pos);
    CJHTreeNode *cacheNode = preloadCache.query(key);
    if (!cacheNode)
    {
        cacheAdds++;
        if (ctx) ctx->noteStatistic(StNumPreloadCacheAdds, 1);
        preloadCacheAdds++;
        preloadCache.add(key, *LINK(node));
    }
}

bool CNodeCache::isPreloaded(int iD, offset_t pos)
{
    CriticalBlock block(lock);
    CKeyIdAndPos key(iD, pos);
    return NULL != preloadCache.query(key);
}

RelaxedAtomic<unsigned> cacheAdds;
RelaxedAtomic<unsigned> cacheHits;
RelaxedAtomic<unsigned> nodesLoaded;
RelaxedAtomic<unsigned> blobCacheHits;
RelaxedAtomic<unsigned> blobCacheAdds;
RelaxedAtomic<unsigned> leafCacheHits;
RelaxedAtomic<unsigned> leafCacheAdds;
RelaxedAtomic<unsigned> nodeCacheHits;
RelaxedAtomic<unsigned> nodeCacheAdds;
RelaxedAtomic<unsigned> preloadCacheHits;
RelaxedAtomic<unsigned> preloadCacheAdds;

void clearNodeStats()
{
    cacheAdds.store(0);
    cacheHits.store(0);
    nodesLoaded.store(0);
    blobCacheHits.store(0);
    blobCacheAdds.store(0);
    leafCacheHits.store(0);
    leafCacheAdds.store(0);
    nodeCacheHits.store(0);
    nodeCacheAdds.store(0);
    preloadCacheHits.store(0);
    preloadCacheAdds.store(0);
}

//------------------------------------------------------------------------------------------------

class CKeyMerger : public CKeyLevelManager
{
    unsigned *mergeheap;
    unsigned numkeys;
    unsigned activekeys;
    unsigned compareSize = 0;
    IArrayOf<IKeyCursor> cursorArray;
    PointerArray bufferArray;
    PointerArray fixedArray;
    BoolArray matchedArray; 
    UnsignedArray mergeHeapArray;
    UnsignedArray keyNoArray;

    IKeyCursor **cursors;
    char **buffers;
    void **fixeds;
    bool *matcheds;
    unsigned sortFieldOffset;
    unsigned sortFromSeg;

    bool resetPending;

//  #define BuffCompare(a, b) memcmp(buffers[mergeheap[a]]+sortFieldOffset, buffers[mergeheap[b]]+sortFieldOffset, keySize-sortFieldOffset) // NOTE - compare whole key not just keyed part.
    inline int BuffCompare(unsigned a, unsigned b)
    {
        const char *c1 = buffers[mergeheap[a]];
        const char *c2 = buffers[mergeheap[b]];
        //Backwards compatibility - do not compare the fileposition field, even if it would be significant.
        //int ret = memcmp(c1+sortFieldOffset, c2+sortFieldOffset, keySize-sortFieldOffset); // NOTE - compare whole key not just keyed part.
        int ret = memcmp(c1+sortFieldOffset, c2+sortFieldOffset, compareSize-sortFieldOffset); // NOTE - compare whole key not just keyed part.
        if (!ret && sortFieldOffset)
            ret = memcmp(c1, c2, sortFieldOffset);
        return ret;
    }

    Linked<IKeyIndexBase> keyset;

    void resetKey(unsigned i)
    {
        matcheds[i] = false;
        segs.setLow(sortFromSeg, buffers[i]);
        IKeyCursor *cursor = cursors[i];
        if (cursor)
            cursor->reset();
    }

    void replicateForTrailingSort()
    {
        // For each key that is present, we need to repeat it for each distinct value of leading segmonitor fields that is present in the index.
        // We also need to make sure that sortFromSeg is properly set
        sortFromSeg = (unsigned) -1;
        ForEachItemIn(idx, segs.segMonitors)
        {
            IKeySegmentMonitor &seg = segs.segMonitors.item(idx);
            unsigned offset = seg.getOffset();
            if (offset == sortFieldOffset)
            {
                sortFromSeg = idx;
                break;
            }
            IKeySegmentMonitor *override = createOverrideableKeySegmentMonitor(LINK(&seg));
            segs.segMonitors.replace(*override, idx);
        }
        if (sortFromSeg == -1)
            assertex(!"Attempting to sort from offset that is not on a segment boundary"); // MORE - can use the information that we have earlier to make sure not merged
        assertex(resetPending == true); // we do the actual replication in reset
    }

    void dumpMergeHeap()
    {
        DBGLOG("---------");
        for (unsigned i = 0; i < activekeys; i++)
        {
            int key = mergeheap[i];
            DBGLOG("%d %.*s %d", key, keySize, buffers[key], matcheds[key]);
        }

    }

    inline void setSegOverrides(unsigned key)
    {
        keyBuffer = buffers[key];
        keyCursor = cursors[key];
        matched = matcheds[key];
        for (unsigned segno = 0; segno < sortFromSeg; segno++)
        {
            IOverrideableKeySegmentMonitor *sm = QUERYINTERFACE(&segs.segMonitors.item(segno), IOverrideableKeySegmentMonitor);
            sm->setOverrideBuffer(fixeds[key]);
        }
        segs.recalculateCache();
    }

public:
    CKeyMerger(const RtlRecord &_recInfo, IKeyIndexSet *_keyset, unsigned _sortFieldOffset, IContextLogger *_ctx) : CKeyLevelManager(_recInfo, NULL, _ctx), sortFieldOffset(_sortFieldOffset)
    {
        init();
        setKey(_keyset);
    }

    CKeyMerger(const RtlRecord &_recInfo, IKeyIndex *_onekey, unsigned _sortFieldOffset, IContextLogger *_ctx) : CKeyLevelManager(_recInfo, NULL, _ctx), sortFieldOffset(_sortFieldOffset)
    {
        init();
        setKey(_onekey);
    }

    ~CKeyMerger()
    {
        killBuffers();
        keyCursor = NULL; // so parent class doesn't delete it!
        keyBuffer = NULL; // ditto
    }

    void killBuffers()
    {
        ForEachItemIn(idx, bufferArray)
        {
            free(bufferArray.item(idx));
        }
        ForEachItemIn(idx1, fixedArray)
        {
            free(fixedArray.item(idx1));
        }
        cursorArray.kill();
        keyCursor = NULL; // cursorArray owns cursors
        matchedArray.kill();
        mergeHeapArray.kill();
        bufferArray.kill();
        fixedArray.kill();
        keyNoArray.kill();

        cursors = NULL;
        matcheds = NULL;
        mergeheap = NULL;
        buffers = NULL; 
        fixeds = NULL;
    }

    void init()
    {
        numkeys = 0;
        activekeys = 0;
        resetPending = true;
        sortFromSeg = 0;
    }

    virtual bool lookupSkip(const void *seek, size32_t seekOffset, size32_t seeklen)
    {
        // Rather like a lookup, except that no records below the value indicated by seek* should be returned.
        if (resetPending)
        {
            resetSort(seek, seekOffset, seeklen);
            if (!activekeys)
                return false;
#ifdef _DEBUG
            if (traceSmartStepping)
                DBGLOG("SKIP: init key = %d", mergeheap[0]);
#endif
            return true;
        }
        else
        {
            if (!activekeys)
            {
#ifdef _DEBUG
                if (traceSmartStepping)
                    DBGLOG("SKIP: merge done");
#endif
                return false;
            }
            unsigned key = mergeheap[0];
#ifdef _DEBUG
            if (traceSmartStepping)
                DBGLOG("SKIP: merging key = %d", key);
#endif
            unsigned compares = 0;
            for (;;)
            {
                if (!CKeyLevelManager::lookupSkip(seek, seekOffset, seeklen) )
                {
                    activekeys--;
                    if (!activekeys)
                    {
                        if (ctx)
                            ctx->noteStatistic(StNumIndexMergeCompares, compares);
                        return false;
                    }
                    eof = false;
                    mergeheap[0] = mergeheap[activekeys];
                }
                /* The key associated with mergeheap[0] will have changed
                   This code restores the heap property
                */
                unsigned p = 0; /* parent */
                while (1) 
                {
                    unsigned c = p*2 + 1; /* child */
                    if ( c >= activekeys ) 
                        break;
                    /* Select smaller child */
                    if ( c+1 < activekeys && BuffCompare( c+1, c ) < 0 ) c += 1;
                    /* If child is greater or equal than parent then we are done */
                    if ( BuffCompare( c, p ) >= 0 ) 
                        break;
                    /* Swap parent and child */
                    int r = mergeheap[c];
                    mergeheap[c] = mergeheap[p];
                    mergeheap[p] = r;
                    /* child becomes parent */
                    p = c;
                }
                if (key != mergeheap[0])
                {
                    key = mergeheap[0];
                    setSegOverrides(key);
                }
                if (memcmp(seek, keyBuffer+seekOffset, seeklen) <= 0)
                {
#ifdef _DEBUG
                    if (traceSmartStepping)
                    {
                        DBGLOG("SKIP: merged key = %d", key);
                        StringBuffer recstr;
                        unsigned i;
                        for (i = 0; i < keySize; i++)
                        {
                            unsigned char c = ((unsigned char *) keyBuffer)[i];
                            recstr.appendf("%c", isprint(c) ? c : '.');
                        }
                        recstr.append ("    ");
                        for (i = 0; i < keySize; i++)
                        {
                            recstr.appendf("%02x ", ((unsigned char *) keyBuffer)[i]);
                        }
                        DBGLOG("SKIP: Out skips=%02d nullSkips=%02d seeks=%02d scans=%02d : %s", skips, nullSkips, seeks, scans, recstr.str());
                    }
#endif
                    if (ctx)
                        ctx->noteStatistic(StNumIndexMergeCompares, compares);
                    return true;
                }
                else
                {
                    compares++;
                    if (ctx && (compares == 100))
                    {
                        ctx->noteStatistic(StNumIndexMergeCompares, compares); // also checks for abort...
                        compares = 0;
                    }
                }
            }
        }
    }

    virtual void setLayoutTranslator(IRecordLayoutTranslator * trans) 
    { 
        if (trans)
            throw MakeStringException(0, "Layout translation not supported when merging key parts, as it may change sort order"); 
        // it MIGHT be possible to support translation still if all keyCursors have the same translation 
        // would have to translate AFTER the merge, but that's ok
        // HOWEVER the result won't be guaranteed to be in sorted order afterwards so is there any point?
    }

    virtual void setKey(IKeyIndexBase *_keyset)
    {
        keyset.set(_keyset);
        if (_keyset && _keyset->numParts())
        {
            IKeyIndex *ki = _keyset->queryPart(0);
            keySize = ki->keySize();
            if (!keySize)
                throw MakeStringException(0, "Invalid key size 0 in key %s", ki->queryFileName());
            keyedSize = ki->keyedSize();
            numkeys = _keyset->numParts();
            compareSize = keySize - (ki->hasSpecialFileposition() ? sizeof(offset_t) : 0);
        }
        else
            numkeys = 0;
        killBuffers();
    }

    void resetSort(const void *seek, size32_t seekOffset, size32_t seeklen)
    {
        activekeys = 0;
        keyBuffer = NULL;
        void *fixedValue = NULL;
        unsigned segno;
        for (segno = 0; segno < sortFromSeg; segno++)
        {
            IOverrideableKeySegmentMonitor *sm = QUERYINTERFACE(&segs.segMonitors.item(segno), IOverrideableKeySegmentMonitor);
            sm->setOverrideBuffer(NULL);
        }
        segs.recalculateCache();
        unsigned i;
        for (i = 0; i < numkeys; i++)
        {
            assertex(keySize);
            if (!keyBuffer) keyBuffer = (char *) malloc(keySize);
            segs.setLow(0, keyBuffer);
            for (;;)
            {
                keyCursor = keyset->queryPart(i)->getCursor(ctx);
                matched = false;
                eof = false;
                keyCursor->reset();
                bool found;
                unsigned lskips = 0;
                unsigned lnullSkips = 0;
                for (;;)
                {
                    if (seek)
                    {
                        if (skipTo(seek, seekOffset, seeklen))
                            lskips++;
                        else
                            lnullSkips++;
                    }
                    found = _lookup(true, segs.lastRealSeg());
                    if (!found || !seek || memcmp(keyBuffer + seekOffset, seek, seeklen) >= 0)
                        break;
                }
                noteSkips(lskips, lnullSkips);
                if (found)
                {
                    keyNoArray.append(i);
                    cursorArray.append(*keyCursor);
                    bufferArray.append(keyBuffer);
                    matchedArray.append(matched);
                    mergeHeapArray.append(activekeys++);
                    if (!sortFromSeg)
                    {
                        keyBuffer = NULL;
                        fixedValue = NULL;
                        break;
                    }
                    assertex(sortFieldOffset);
                    void *fixedValue = malloc(sortFieldOffset);
                    memcpy(fixedValue, keyBuffer, sortFieldOffset);
                    fixedArray.append(fixedValue);
#ifdef _DEBUG
                    if (traceSmartStepping)
                    {
                        StringBuffer recstr;
                        unsigned i;
                        for (i = 0; i < sortFieldOffset; i++)
                        {
                            unsigned char c = ((unsigned char *) keyBuffer)[i];
                            recstr.appendf("%c", isprint(c) ? c : '.');
                        }
                        recstr.append ("    ");
                        for (i = 0; i < keySize; i++)
                        {
                            recstr.appendf("%02x ", ((unsigned char *) keyBuffer)[i]);
                        }
                        DBGLOG("Adding key cursor info for %s", recstr.str());
                    }
#endif
                    // Now advance segments 0 through sortFromSeg-1 to next legal value...
                    assertex(keySize);
                    char *nextBuffer = (char *) malloc(keySize);
                    memcpy(nextBuffer, keyBuffer, keySize);
                    keyBuffer = nextBuffer;
#ifdef _DEBUG
                    assertex(segs.segMonitors.item(sortFromSeg-1).matchesBuffer(keyBuffer));
#endif
                    if (!segs.incrementKey(sortFromSeg-1, keyBuffer))
                        break;
                }
                else
                {
                    keyCursor->Release();
                    break;
                }
            }
        }
        free (keyBuffer);
        keyBuffer = NULL;
        if (activekeys>0) 
        {
            if (ctx)
                ctx->noteStatistic(StNumIndexMerges, activekeys);
            cursors = cursorArray.getArray();
            matcheds = (bool *) matchedArray.getArray();  // For some reason BoolArray is typedef'd to CharArray on linux...
            buffers = (char **) bufferArray.getArray();
            mergeheap = mergeHeapArray.getArray();
            fixeds = fixedArray.getArray();
            /* Permute mergeheap to establish the heap property
               For each element p, the children are p*2+1 and p*2+2 (provided these are in range)
               The children of p must both be greater than or equal to p
               The parent of a child c is given by p = (c-1)/2
            */
            for (i=1; i<activekeys; i++)
            {
                int r = mergeheap[i];
                int c = i; /* child */
                while (c > 0) 
                {
                    int p = (c-1)/2; /* parent */
                    if ( BuffCompare( c, p ) >= 0 ) 
                        break;
                    mergeheap[c] = mergeheap[p];
                    mergeheap[p] = r;
                    c = p;
                }
            }
            setSegOverrides(mergeheap[0]);
            eof = false;
        }
        else
        {
            keyBuffer = NULL;
            keyCursor = NULL;
            matched = false;
            eof = true;
        }
        resetPending = false;

    }

    virtual void reset(bool crappyHack)
    {
        if (!started)
        {
            started = true;
            if (keyedSize)
                segs.checkSize(keyedSize, "[merger]"); //PG: not sure what keyname to use here
        }
        if (!crappyHack)
        {
            killBuffers();
            resetPending = true;
        }
        else
        {
            setSegOverrides(mergeheap[0]);
            resetPending = false;
        }
    }

    virtual bool lookup(bool exact)
    {
        assertex(exact);
        if (resetPending)
        {
            resetSort(NULL, 0, 0);
            if (!activekeys)
                return false;
        }
        else
        {
            if (!activekeys)
                return false;
            unsigned key = mergeheap[0];
            if (!CKeyLevelManager::lookup(exact)) 
            {
                activekeys--;
                if (!activekeys)
                    return false; // MORE - does this lose a record?
                eof = false;
                mergeheap[0] = mergeheap[activekeys];
            }

            /* The key associated with mergeheap[0] will have changed
               This code restores the heap property
            */
            unsigned p = 0; /* parent */
            while (1) 
            {
                unsigned c = p*2 + 1; /* child */
                if ( c >= activekeys ) 
                    break;
                /* Select smaller child */
                if ( c+1 < activekeys && BuffCompare( c+1, c ) < 0 ) c += 1;
                /* If child is greater or equal than parent then we are done */
                if ( BuffCompare( c, p ) >= 0 ) 
                    break;
                /* Swap parent and child */
                int r = mergeheap[c];
                mergeheap[c] = mergeheap[p];
                mergeheap[p] = r;
                /* child becomes parent */
                p = c;
            }
//          dumpMergeHeap();
            if (mergeheap[0] != key)
                setSegOverrides(mergeheap[0]);
        }
        return true;
    }

    virtual unsigned __int64 getCount()
    {
        assertex (!sortFieldOffset);  // we should have avoided using a stepping merger for precheck of limits, both for efficiency and because this code won't work
                                      // as the sequence numbers are not in sequence
        unsigned __int64 ret = 0;
        if (resetPending)
            resetSort(NULL, 0, 0); // This is slightly suboptimal
        for (unsigned i = 0; i < activekeys; i++)
        {
            unsigned key = mergeheap[i];
            keyBuffer = buffers[key];
            keyCursor = cursors[key];
            ret += CKeyLevelManager::getCount();
        }
        return ret;
    }

    virtual unsigned __int64 checkCount(unsigned __int64 max)
    {
        assertex (!sortFieldOffset);  // we should have avoided using a stepping merger for precheck of limits, both for efficiency and because this code won't work
                                      // as the sequence numbers are not in sequence
        unsigned __int64 ret = 0;
        if (resetPending)
            resetSort(NULL, 0, 0); // this is a little suboptimal as we will not bail out early
        for (unsigned i = 0; i < activekeys; i++)
        {
            unsigned key = mergeheap[i];
            keyBuffer = buffers[key];
            keyCursor = cursors[key];
            unsigned __int64 thisKeyCount = CKeyLevelManager::checkCount(max);
            ret += thisKeyCount;
            if (thisKeyCount > max)
                return ret;
            max -= thisKeyCount;
        }
        return ret;
    }

    virtual void serializeCursorPos(MemoryBuffer &mb)
    {
//      dumpMergeHeap();
        mb.append(eof);
        mb.append(activekeys);
        for (unsigned i = 0; i < activekeys; i++)
        {
            unsigned key = mergeheap[i];
            mb.append(keyNoArray.item(key));
            cursors[key]->serializeCursorPos(mb);
            mb.append(matcheds[key]);
        }
    }

    virtual void deserializeCursorPos(MemoryBuffer &mb)
    {
        mb.read(eof);
        mb.read(activekeys);
        for (unsigned i = 0; i < activekeys; i++)
        {
            unsigned keyno;
            mb.read(keyno);
            keyNoArray.append(keyno);
            keyCursor = keyset->queryPart(keyno)->getCursor(ctx);
            assertex(keySize);
            keyBuffer = (char *) malloc(keySize);
            cursorArray.append(*keyCursor);
            keyCursor->deserializeCursorPos(mb, keyBuffer);
            mb.read(matched);
            matchedArray.append(matched);
            bufferArray.append(keyBuffer);
            void *fixedValue = (char *) malloc(sortFieldOffset);
            memcpy(fixedValue, keyBuffer, sortFieldOffset); // If it's not at EOF then it must match
            fixedArray.append(fixedValue);
            mergeHeapArray.append(i);
        }
        cursors = cursorArray.getArray();
        matcheds = (bool *) matchedArray.getArray();  // For some reason BoolArray is typedef'd to CharArray on linux...
        buffers = (char **) bufferArray.getArray();
        mergeheap = mergeHeapArray.getArray();
        fixeds = fixedArray.getArray();
    }

    virtual void finishSegmentMonitors()
    {
        CKeyLevelManager::finishSegmentMonitors();
        if (sortFieldOffset)
        {
            if (keyedSize)
                segs.checkSize(keyedSize, "[merger]"); // Ensures trailing KSM is setup
            replicateForTrailingSort();
        }
    }
};

extern jhtree_decl IKeyManager *createKeyMerger(const RtlRecord &_recInfo, IKeyIndexSet * _keys, unsigned _sortFieldOffset, IContextLogger *_ctx)
{
    return new CKeyMerger(_recInfo, _keys, _sortFieldOffset, _ctx);
}

extern jhtree_decl IKeyManager *createSingleKeyMerger(const RtlRecord &_recInfo, IKeyIndex * _onekey, unsigned _sortFieldOffset, IContextLogger *_ctx)
{
    return new CKeyMerger(_recInfo, _onekey, _sortFieldOffset, _ctx);
}

class CKeyIndexSet : implements IKeyIndexSet, public CInterface
{
    IPointerArrayOf<IKeyIndex> indexes;
    offset_t recordCount;
    offset_t totalSize;
    StringAttr origFileName;

public:
    IMPLEMENT_IINTERFACE;
    
    virtual bool IsShared() const { return CInterface::IsShared(); }
    void addIndex(IKeyIndex *i) { indexes.append(i); }
    virtual unsigned numParts() { return indexes.length(); }
    virtual IKeyIndex *queryPart(unsigned partNo) { return indexes.item(partNo); }
    virtual void setRecordCount(offset_t count) { recordCount = count; }
    virtual void setTotalSize(offset_t size) { totalSize = size; }
    virtual offset_t getRecordCount() { return recordCount; }
    virtual offset_t getTotalSize() { return totalSize; }
};

extern jhtree_decl IKeyIndexSet *createKeyIndexSet()
{
    return new CKeyIndexSet;
}

extern jhtree_decl IKeyManager *createLocalKeyManager(const RtlRecord &_recInfo, IKeyIndex *_key, IContextLogger *_ctx)
{
    return new CKeyLevelManager(_recInfo, _key, _ctx);
}

class CKeyArray : implements IKeyArray, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    virtual bool IsShared() const { return CInterface::IsShared(); }
    IPointerArrayOf<IKeyIndexBase> keys;
    virtual IKeyIndexBase *queryKeyPart(unsigned partNo)
    {
        if (!keys.isItem(partNo))
        {
            return NULL;
            DBGLOG("getKeyPart requested invalid part %d", partNo);
            throw MakeStringException(0, "queryKeyPart requested invalid part %d", partNo);
        }
        IKeyIndexBase *key = keys.item(partNo);
        return key;
    }

    virtual unsigned length() { return keys.length(); }
    void addKey(IKeyIndexBase *f) { keys.append(f); }
};

extern jhtree_decl IKeyArray *createKeyArray()
{
    return new CKeyArray;
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class IKeyManagerTest : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE( IKeyManagerTest  );
        CPPUNIT_TEST(testStepping);
        CPPUNIT_TEST(testKeys);
    CPPUNIT_TEST_SUITE_END();

    void testStepping()
    {
        buildTestKeys(false);
        {
            // We are going to treat as a 7-byte field then a 3-byte field, and request the datasorted by the 3-byte...
            Owned <IKeyIndex> index1 = createKeyIndex("keyfile1.$$$", 0, false, false);
            Owned <IKeyIndex> index2 = createKeyIndex("keyfile2.$$$", 0, false, false);
            Owned<IKeyIndexSet> keyset = createKeyIndexSet();
            keyset->addIndex(index1.getClear());
            keyset->addIndex(index2.getClear());
            const char *json = "{ \"ty1\": { \"fieldType\": 4, \"length\": 7 }, "
                               "  \"ty2\": { \"fieldType\": 4, \"length\": 3 }, "
                               " \"fieldType\": 13, \"length\": 10, "
                               " \"fields\": [ "
                               " { \"name\": \"f1\", \"type\": \"ty1\", \"flags\": 4 }, "
                               " { \"name\": \"f2\", \"type\": \"ty2\", \"flags\": 4 } ] "
                               "}";
            Owned<IOutputMetaData> meta = createTypeInfoOutputMetaData(json, nullptr);
            Owned <IKeyManager> tlk1 = createKeyMerger(meta->queryRecordAccessor(true), keyset, 7, NULL);
            Owned<IStringSet> sset1 = createStringSet(7);
            sset1->addRange("0000003", "0000003");
            sset1->addRange("0000005", "0000006");
            tlk1->append(createKeySegmentMonitor(false, sset1.getLink(), 0, 0, 7));
            Owned<IStringSet> sset2 = createStringSet(3);
            sset2->addRange("010", "010");
            sset2->addRange("030", "033");
            Owned<IStringSet> sset3 = createStringSet(3);
            sset3->addRange("999", "XXX");
            sset3->addRange("000", "002");
            tlk1->append(createKeySegmentMonitor(false, sset2.getLink(), 1, 7, 3));
            tlk1->finishSegmentMonitors();

            tlk1->reset();

            offset_t fpos;
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003010", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005010", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006010", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003030", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005030", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006030", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003031", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005031", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006031", 10)==0);
            MemoryBuffer mb;
            tlk1->serializeCursorPos(mb);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003032", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005032", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006032", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000003033", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000005033", 10)==0);
            ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000006033", 10)==0);
            ASSERT(!tlk1->lookup(true)); 
            ASSERT(!tlk1->lookup(true)); 

            Owned <IKeyManager> tlk2 = createKeyMerger(meta->queryRecordAccessor(true), NULL, 7, NULL);
            tlk2->setKey(keyset);
            tlk2->deserializeCursorPos(mb);
            tlk2->append(createKeySegmentMonitor(false, sset1.getLink(), 0, 0, 7));
            tlk2->append(createKeySegmentMonitor(false, sset2.getLink(), 1, 7, 3));
            tlk2->finishSegmentMonitors();
            tlk2->reset(true);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000003032", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000005032", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000006032", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000003033", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000005033", 10)==0);
            ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000006033", 10)==0);
            ASSERT(!tlk2->lookup(true)); 
            ASSERT(!tlk2->lookup(true)); 

            Owned <IKeyManager> tlk3 = createKeyMerger(meta->queryRecordAccessor(true), NULL, 7, NULL);
            tlk3->setKey(keyset);
            tlk3->append(createKeySegmentMonitor(false, sset1.getLink(), 0, 0, 7));
            tlk3->append(createKeySegmentMonitor(false, sset2.getLink(), 1, 7, 3));
            tlk3->finishSegmentMonitors();
            tlk3->reset(false);
            ASSERT(tlk3->lookup(true)); ASSERT(memcmp(tlk3->queryKeyBuffer(), "0000003010", 10)==0);
            ASSERT(tlk3->lookupSkip("031", 7, 3)); ASSERT(memcmp(tlk3->queryKeyBuffer(), "0000003031", 10)==0);
            ASSERT(tlk3->lookup(true)); ASSERT(memcmp(tlk3->queryKeyBuffer(), "0000005031", 10)==0);
            ASSERT(tlk3->lookup(true)); ASSERT(memcmp(tlk3->queryKeyBuffer(), "0000006031", 10)==0);
            ASSERT(!tlk3->lookupSkip("081", 7, 3)); 
            ASSERT(!tlk3->lookup(true)); 

            Owned <IKeyManager> tlk4 = createKeyMerger(meta->queryRecordAccessor(true), NULL, 7, NULL);
            tlk4->setKey(keyset);
            tlk4->append(createKeySegmentMonitor(false, sset1.getLink(), 0, 0, 7));
            tlk4->append(createKeySegmentMonitor(false, sset3.getLink(), 1, 7, 3));
            tlk4->finishSegmentMonitors();
            tlk4->reset(false);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000003000", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000005000", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000006000", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000003001", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000005001", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000006001", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000003002", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000005002", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000006002", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000003999", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000005999", 10)==0);
            ASSERT(tlk4->lookup(true)); ASSERT(memcmp(tlk4->queryKeyBuffer(), "0000006999", 10)==0);
            ASSERT(!tlk4->lookup(true)); 
            ASSERT(!tlk4->lookup(true)); 

        }
        clearKeyStoreCache(true);
        removeTestKeys();
    }

    void buildTestKeys(bool variable)
    {
        buildTestKey("keyfile1.$$$", false, variable);
        buildTestKey("keyfile2.$$$", true, variable);
    }

    void buildTestKey(const char *filename, bool skip, bool variable)
    {
        OwnedIFile file = createIFile(filename);
        OwnedIFileIO io = file->openShared(IFOcreate, IFSHfull);
        Owned<IFileIOStream> out = createIOStream(io);
        unsigned maxRecSize = variable ? 18 : 10;
        unsigned keyedSize = 10;
        Owned<IKeyBuilder> builder = createKeyBuilder(out, COL_PREFIX | HTREE_FULLSORT_KEY | HTREE_COMPRESSED_KEY |  (variable ? HTREE_VARSIZE : 0), maxRecSize, NODESIZE, keyedSize, 0);

        char keybuf[18];
        memset(keybuf, '0', 18);
        for (unsigned count = 0; count < 10000; count++)
        {
            unsigned datasize = 10;
            if (variable && (count % 10)==0)
            {
                char *blob = new char[count+100000];
                byte seed = count;
                for (unsigned i = 0; i < count+100000; i++)
                {
                    blob[i] = seed;
                    seed = seed * 13 + i;
                }
                offset_t blobid = builder->createBlob(count+100000, blob);
                memcpy(keybuf + 10, &blobid, sizeof(blobid));
                delete [] blob;
                datasize += sizeof(blobid);
            }
            bool skipme = (count % 4 == 0) != skip;
            if (!skipme)
            {
                builder->processKeyData(keybuf, count*10, datasize);
                if (count==48 || count==49)
                    builder->processKeyData(keybuf, count*10, datasize);
            }
            unsigned idx = 9;
            for (;;)
            {
                if (keybuf[idx]=='9')
                    keybuf[idx--]='0';
                else
                {
                    keybuf[idx]++;
                    break;
                }
            }
        }
        builder->finish(nullptr, nullptr);
        out->flush();
    }

    void removeTestKeys()
    {
        ASSERT(remove("keyfile1.$$$")==0);
        ASSERT(remove("keyfile2.$$$")==0);
    }

    void checkBlob(IKeyManager *key, unsigned size)
    {
        unsigned __int64 blobid;
        memcpy(&blobid, key->queryKeyBuffer()+10, sizeof(blobid));
        ASSERT(blobid != 0);
        size32_t blobsize;
        const byte *blob = key->loadBlob(blobid, blobsize);
        ASSERT(blob != NULL);
        ASSERT(blobsize == size);
        byte seed = size-100000;
        for (unsigned i = 0; i < size; i++)
        {
            ASSERT(blob[i] == seed);
            seed = seed * 13 + i;
        }
        key->releaseBlobs();
    }
protected:
    void testKeys(bool variable)
    {
        const char *json = variable ?
                "{ \"ty1\": { \"fieldType\": 4, \"length\": 10 }, "
                "  \"ty2\": { \"fieldType\": 15, \"length\": 8 }, "
                " \"fieldType\": 13, \"length\": 10, "
                " \"fields\": [ "
                " { \"name\": \"f1\", \"type\": \"ty1\", \"flags\": 4 }, "
                " { \"name\": \"f3\", \"type\": \"ty2\", \"flags\": 65551 } "  // 0x01000f i.e. payload and blob
                " ]"
                "}"
                :
                "{ \"ty1\": { \"fieldType\": 4, \"length\": 10 }, "
                " \"fieldType\": 13, \"length\": 10, "
                " \"fields\": [ "
                " { \"name\": \"f1\", \"type\": \"ty1\", \"flags\": 4 }, "
                " ] "
                "}";
        Owned<IOutputMetaData> meta = createTypeInfoOutputMetaData(json, nullptr);
        const RtlRecord &recInfo = meta->queryRecordAccessor(true);
        buildTestKeys(variable);
        {
            Owned <IKeyIndex> index1 = createKeyIndex("keyfile1.$$$", 0, false, false);
            Owned <IKeyManager> tlk1 = createLocalKeyManager(recInfo, index1, NULL);
            Owned<IStringSet> sset1 = createStringSet(10);
            sset1->addRange("0000000001", "0000000100");
            tlk1->append(createKeySegmentMonitor(false, sset1.getClear(), 0, 0, 10));
            tlk1->finishSegmentMonitors();
            tlk1->reset();

            Owned <IKeyManager> tlk1a = createLocalKeyManager(recInfo, index1, NULL);
            Owned<IStringSet> sset1a = createStringSet(8);
            sset1a->addRange("00000000", "00000001");
            tlk1a->append(createKeySegmentMonitor(false, sset1a.getClear(), 0, 0, 8));
            tlk1a->append(createKeySegmentMonitor(false, NULL, 1, 8, 1));
            sset1a.setown(createStringSet(1));
            sset1a->addRange("0", "1");
            tlk1a->append(createKeySegmentMonitor(false, sset1a.getClear(), 2, 9, 1));
            tlk1a->finishSegmentMonitors();
            tlk1a->reset();


            Owned<IStringSet> ssetx = createStringSet(10);
            ssetx->addRange("0000000001", "0000000002");
            ASSERT(ssetx->numValues() == 2);
            ssetx->addRange("00000000AK", "00000000AL");
            ASSERT(ssetx->numValues() == 4);
            ssetx->addRange("0000000100", "0010000000");
            ASSERT(ssetx->numValues() == (unsigned) -1);
            ssetx->addRange("0000000001", "0010000000");
            ASSERT(ssetx->numValues() == (unsigned) -1);


            Owned <IKeyIndex> index2 = createKeyIndex("keyfile2.$$$", 0, false, false);
            Owned <IKeyManager> tlk2 = createLocalKeyManager(recInfo, index2, NULL);
            Owned<IStringSet> sset2 = createStringSet(10);
            sset2->addRange("0000000001", "0000000100");
            ASSERT(sset2->numValues() == 65536);
            tlk2->append(createKeySegmentMonitor(false, sset2.getClear(), 0, 0, 10));
            tlk2->finishSegmentMonitors();
            tlk2->reset();

            Owned <IKeyManager> tlk3;
            if (!variable)
            {
                Owned<IKeyIndexSet> both = createKeyIndexSet();
                both->addIndex(index1.getLink());
                both->addIndex(index2.getLink());
                Owned<IStringSet> sset3 = createStringSet(10);
                tlk3.setown(createKeyMerger(recInfo, NULL, 0, NULL));
                tlk3->setKey(both);
                sset3->addRange("0000000001", "0000000100");
                tlk3->append(createKeySegmentMonitor(false, sset3.getClear(), 0, 0, 10));
                tlk3->finishSegmentMonitors();
                tlk3->reset();
            }

            Owned <IKeyManager> tlk2a = createLocalKeyManager(recInfo, index2, NULL);
            Owned<IStringSet> sset2a = createStringSet(10);
            sset2a->addRange("0000000048", "0000000048");
            ASSERT(sset2a->numValues() == 1);
            tlk2a->append(createKeySegmentMonitor(false, sset2a.getClear(), 0, 0, 10));
            tlk2a->finishSegmentMonitors();
            tlk2a->reset();

            Owned <IKeyManager> tlk2b = createLocalKeyManager(recInfo, index2, NULL);
            Owned<IStringSet> sset2b = createStringSet(10);
            sset2b->addRange("0000000047", "0000000049");
            ASSERT(sset2b->numValues() == 3);
            tlk2b->append(createKeySegmentMonitor(false, sset2b.getClear(), 0, 0, 10));
            tlk2b->finishSegmentMonitors();
            tlk2b->reset();

            Owned <IKeyManager> tlk2c = createLocalKeyManager(recInfo, index2, NULL);
            Owned<IStringSet> sset2c = createStringSet(10);
            sset2c->addRange("0000000047", "0000000047");
            tlk2c->append(createKeySegmentMonitor(false, sset2c.getClear(), 0, 0, 10));
            tlk2c->finishSegmentMonitors();
            tlk2c->reset();

            ASSERT(tlk1->getCount() == 76);
            ASSERT(tlk1->getCount() == 76);
            ASSERT(tlk1a->getCount() == 30);
            ASSERT(tlk2->getCount() == 26);
            ASSERT(tlk2a->getCount() == 2);
            ASSERT(tlk2b->getCount() == 2);
            ASSERT(tlk2c->getCount() == 0);
            if (tlk3)
                ASSERT(tlk3->getCount() == 102);

// MORE -           PUT SOME TESTS IN FOR WILD SEEK STUFF

            unsigned pass;
            char buf[11];
                unsigned i;
            for (pass = 0; pass < 2; pass++)
            {
                offset_t fpos;
                tlk1->reset();
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000001", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000002", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000003", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000005", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000006", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000007", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000009", 10)==0);
                ASSERT(tlk1->lookup(true)); ASSERT(memcmp(tlk1->queryKeyBuffer(), "0000000010", 10)==0);
                if (variable)
                    checkBlob(tlk1, 10+100000);

                tlk1a->reset();
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000001", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000010", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000011", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000021", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000030", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000031", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000041", 10)==0);
                ASSERT(tlk1a->lookup(true)); ASSERT(memcmp(tlk1a->queryKeyBuffer(), "0000000050", 10)==0);

                tlk2->reset();
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000004", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000008", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000012", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000016", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000020", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000024", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000028", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000032", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000036", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000040", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000044", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000048", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000048", 10)==0);
                ASSERT(tlk2->lookup(true)); ASSERT(memcmp(tlk2->queryKeyBuffer(), "0000000052", 10)==0);

                if (tlk3)
                {
                    tlk3->reset();
                    for (i = 1; i <= 100; i++)
                    {
                        ASSERT(tlk3->lookup(true)); 
                        sprintf(buf, "%010d", i);
                        ASSERT(memcmp(tlk3->queryKeyBuffer(), buf, 10)==0);
                        if (i==48 || i==49)
                        {
                            ASSERT(tlk3->lookup(true)); 
                            ASSERT(memcmp(tlk3->queryKeyBuffer(), buf, 10)==0);
                        }
                    }
                    ASSERT(!tlk3->lookup(true)); 
                    ASSERT(!tlk3->lookup(true));    
                }
            }
            tlk1->releaseSegmentMonitors();
            tlk2->releaseSegmentMonitors();
            if (tlk3)
                tlk3->releaseSegmentMonitors();
        }
        clearKeyStoreCache(true);
        removeTestKeys();
    }

    void testKeys()
    {
        ASSERT(sizeof(CKeyIdAndPos) == sizeof(unsigned __int64) + sizeof(offset_t));
        testKeys(false);
        testKeys(true);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( IKeyManagerTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( IKeyManagerTest, "IKeyManagerTest" );

#endif
