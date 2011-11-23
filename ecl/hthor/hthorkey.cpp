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
#include "hthor.ipp"
#include "rtlkey.hpp"
#include "jhtree.hpp"
#include "eclhelper.hpp"
#include "jthread.hpp"
#include "jqueue.tpp"
#include "dasess.hpp"
#include "thorxmlwrite.hpp"
#include "layouttrans.hpp"
#include "thorstep.ipp"
#include "roxiedebug.hpp"

#define MAX_FETCH_LOOKAHEAD 1000
#define IGNORE_FORMAT_CRC_MISMATCH_WHEN_NO_METADATA
#define DEFAULT_KJ_PRESERVES_ORDER 1

using roxiemem::IRowManager;
using roxiemem::OwnedRoxieRow;
using roxiemem::OwnedConstRoxieRow;

static IKeyIndex *openKeyFile(IDistributedFilePart & keyFile)
{
    unsigned numCopies = keyFile.numCopies();
    assertex(numCopies);
    Owned<IException> exc;
    for (unsigned copy=0; copy < numCopies; copy++)
    {
        RemoteFilename rfn;
        try
        {
            OwnedIFile ifile = createIFile(keyFile.getFilename(rfn,copy));
            unsigned __int64 thissize = ifile->size();
            if (thissize != -1)
            {
                StringBuffer remotePath;
                rfn.getRemotePath(remotePath);
                unsigned crc;
                keyFile.getCrc(crc);
                return createKeyIndex(remotePath.str(), crc, false, false);
            }
        }
        catch (IException *E)
        {
            EXCLOG(E, "While opening index file");
            if (exc)
                E->Release();
            else
                exc.setown(E);
        }
    }
    if (exc)
        throw exc.getClear();
    StringBuffer url;
    RemoteFilename rfn;
    keyFile.getFilename(rfn).getRemotePath(url);
    throw MakeStringException(1001, "Could not open key file at %s%s", url.str(), (numCopies > 1) ? " or any alternate location." : ".");
}

void enterSingletonSuperfiles(Shared<IDistributedFile> & file)
{
    IDistributedSuperFile * super = file->querySuperFile();
    while(super && (super->numSubFiles() == 1))
    {
        file.setown(super->getSubFile(0));
        super = file->querySuperFile();
    }
}

bool rltEnabled(IConstWorkUnit const * wu)
{
    if(wu->hasDebugValue("layoutTranslationEnabled"))
        return wu->getDebugValueBool("layoutTranslationEnabled", false);
    else
        return wu->getDebugValueBool("hthorLayoutTranslationEnabled", false);
}

IRecordLayoutTranslator * getRecordLayoutTranslator(IDefRecordMeta const * activityMeta, size32_t activityMetaSize, void const * activityMetaBuff, IDistributedFile * df, IRecordLayoutTranslatorCache * cache)
{
    IPropertyTree const & props = df->queryProperties();
    MemoryBuffer diskMetaBuff;
    if(!props.getPropBin("_record_layout", diskMetaBuff))
#ifdef IGNORE_FORMAT_CRC_MISMATCH_WHEN_NO_METADATA
    {
        WARNLOG("On reading index %s, formatCRC mismatch ignored because file had no record layout metadata and so assumed old", df->queryLogicalName());
        return NULL;
    }
#else
        throw MakeStringException(0, "Unable to recover from record layout mismatch for index %s: no record layout metadata in file", df->queryLogicalName());
#endif

    try
    {
        if(cache)
            return cache->get(diskMetaBuff.length(), diskMetaBuff.bufferBase(), activityMetaSize, activityMetaBuff, activityMeta);
        else
            return createRecordLayoutTranslator(diskMetaBuff.length(), diskMetaBuff.bufferBase(), activityMetaSize, activityMetaBuff);
    }
    catch (IException *E)
    {
        StringBuffer m;
        m.appendf("In index %s: ", df->queryLogicalName());
        E->errorMessage(m);
        E->Release();
        Owned<IDefRecordMeta> diskMeta = deserializeRecordMeta(diskMetaBuff, true);
        StringBuffer diskMetaDesc;
        getRecordMetaAsString(diskMetaDesc, diskMeta);
        StringBuffer activityMetaDesc;
        getRecordMetaAsString(activityMetaDesc, activityMeta);
        ERRLOG("RecordLayoutTranslator error: %s\nDisk meta: %s\nActivity meta: %s", m.str(), diskMetaDesc.str(), activityMetaDesc.str());
        throw MakeStringException(0, "%s", m.str());
    }
}

static void setProgress(IPropertyTree &node, const char *name, const char *value)
{
    StringBuffer attr("@");
    node.setProp(attr.append(name).str(), value);
}

static void setProgress(IPropertyTree &node, const char *name, unsigned __int64 value)
{
    StringBuffer attr("@");
    node.setPropInt64(attr.append(name).str(), value);
}


class TransformCallback : public CInterface, implements IThorIndexCallback 
{
public:
    TransformCallback() { keyManager = NULL; };
    IMPLEMENT_IINTERFACE

//IThorIndexCallback
    virtual unsigned __int64 getFilePosition(const void * row)
    {
        return filepos;
    }
    virtual byte * lookupBlob(unsigned __int64 id) 
    { 
        size32_t dummy; 
        return (byte *) keyManager->loadBlob(id, dummy); 
    }


public:
    offset_t & getFPosRef()                                 { return filepos; }
    void setManager(IKeyManager * _manager)
    {
        finishedRow();
        keyManager = _manager;
    }

    void finishedRow()
    {
        if (keyManager)
            keyManager->releaseBlobs(); 
    }

protected:
    IKeyManager * keyManager;
    offset_t filepos;
};

//-------------------------------------------------------------------------------------------------------------

class CHThorNullAggregateActivity : public CHThorNullActivity
{
public:
    CHThorNullAggregateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _arg, IHThorCompoundAggregateExtra &_extra, ThorActivityKind _kind) : CHThorNullActivity(agent, _activityId, _subgraphId, _arg, _kind), helper(_extra) {}

    //interface IHThorInput
    virtual void ready();
    virtual const void *nextInGroup();
    virtual bool needsAllocator() const { return true; }

protected:
    IHThorCompoundAggregateExtra &helper;
    bool finished;
};


void CHThorNullAggregateActivity::ready()
{
    CHThorNullActivity::ready();
    finished = false;
}

const void *CHThorNullAggregateActivity::nextInGroup()
{
    if (finished) return NULL;

    processed++;
    finished = true;
    RtlDynamicRowBuilder rowBuilder(rowAllocator);
    try
    {
        size32_t newSize = helper.clearAggregate(rowBuilder);
        return rowBuilder.finalizeRowClear(newSize);
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
}

//-------------------------------------------------------------------------------------------------------------

class CHThorNullCountActivity : public CHThorNullActivity
{
public:
    CHThorNullCountActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorArg & _arg, ThorActivityKind _kind) : CHThorNullActivity(agent, _activityId, _subgraphId, _arg, _kind) {}

    //interface IHThorInput
    virtual void ready();
    virtual const void *nextInGroup();
    virtual bool needsAllocator() const { return true; }

protected:
    bool finished;
};


void CHThorNullCountActivity::ready()
{
    CHThorNullActivity::ready();
    finished = false;
}

const void *CHThorNullCountActivity::nextInGroup()
{
    if (finished) return NULL;

    processed++;
    finished = true;

    size32_t outSize = outputMeta.getFixedSize();
    void * ret = rowAllocator->createRow(); //meta: outputMeta
    if (outSize == 1)
        *(byte *)ret = 0;
    else
        *(unsigned __int64 *)ret = 0;
    return rowAllocator->finalizeRow(outSize, ret, outSize);
}


//-------------------------------------------------------------------------------------------------------------


class CHThorIndexReadActivityBase : public CHThorActivityBase
{

public:
    CHThorIndexReadActivityBase(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexReadBaseArg &_arg, ThorActivityKind _kind, IDistributedFile * df);
    ~CHThorIndexReadActivityBase();

    virtual void ready();
    virtual void done();
    IHThorInput *queryOutput(unsigned index) { return this; }
    virtual bool needsAllocator() const { return true; }

    //interface IHThorInput
    virtual bool isGrouped()                { return false; }
    virtual const char *getFileName()       { return NULL; }
    virtual bool outputToFile(const char *) { return false; } 
    virtual IOutputMetaData * queryOutputMeta() const { return outputMeta; }

    virtual void updateProgress(IWUGraphProgress &progress) const
    {
        CHThorActivityBase::updateProgress(progress);
        IPropertyTree &node = progress.updateNode(subgraphId, activityId);
        setProgress(node, "postfiltered", queryPostFiltered());
        setProgress(node, "seeks", querySeeks());
        setProgress(node, "scans", queryScans());
    }

    virtual unsigned querySeeks() const
    {
        return seeks + (klManager ? klManager->querySeeks() : 0);
    }
    virtual unsigned queryScans() const
    {
        return scans + (klManager ? klManager->queryScans() : 0);
    }
    virtual unsigned queryPostFiltered() const
    {
        return postFiltered;
    }

    virtual void fail(char const * msg)
    {
        throw MakeStringException(0, "%s", msg);
    }

protected:
    bool doPreopenLimit(unsigned __int64 limit);
    bool doPreopenLimitFile(unsigned __int64 & count, unsigned __int64 limit);
    IKeyIndex * doPreopenLimitPart(unsigned __int64 & count, unsigned __int64 limit, unsigned part);
    const void * createKeyedLimitOnFailRow();
    void getLayoutTranslators();
    IRecordLayoutTranslator * getLayoutTranslator(IDistributedFile * f);
    void verifyIndex(IKeyIndex * idx);
    void initManager(IKeyManager *manager);
    bool firstPart();
    virtual bool nextPart();
    virtual void initPart();

private:
    bool firstMultiPart();
    bool nextMultiPart();
    void killPart();
    bool setCurrentPart(unsigned whichPart);
    void clearTlk()                                         { tlk.clear(); tlManager.clear(); }
    void openTlk();
    bool doNextSuper();

protected:
    IHThorIndexReadBaseArg &helper;
    IHThorSourceLimitTransformExtra * limitTransformExtra;
    CachedOutputMetaData eclKeySize;
    size32_t keySize;
    void * activityRecordMetaBuff;
    size32_t activityRecordMetaSize;
    Owned<IDefRecordMeta> activityRecordMeta;

// current part
    Owned<IDistributedFilePart> curPart;
    Owned<IKeyManager> klManager;
    Owned<IKeyIndex> keyIndex;
    unsigned nextPartNumber;

//multi files
    Owned<IDistributedFile> df;
    Owned<IKeyIndex> tlk;
    Owned<IKeyManager> tlManager;

//super files:
    Owned<IDistributedFileIterator> superIterator;
    unsigned superIndex;
    unsigned superCount;
    StringBuffer superName;

    TransformCallback callback;

//for preopening (when need counts for keyed skip limit):
    Owned<IKeyIndexSet> keyIndexCache;
    UnsignedArray superIndexCache;
    unsigned keyIndexCacheIdx;

    unsigned seeks;
    unsigned scans;
    unsigned postFiltered;
    bool singlePart;                // a single part index, not part of a super file - optimize so never reload the part.
    bool localSortKey;

//for layout translation
    Owned<IRecordLayoutTranslator> layoutTrans;
    PointerIArrayOf<IRecordLayoutTranslator> layoutTransArray;
    bool gotLayoutTrans;
};

CHThorIndexReadActivityBase::CHThorIndexReadActivityBase(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexReadBaseArg &_arg, ThorActivityKind _kind, IDistributedFile * _df)
    : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), helper(_arg), df(LINK(_df)), activityRecordMetaBuff(NULL)
{
    singlePart = false;
    localSortKey = (df->queryProperties().hasProp("@local"));
    IDistributedSuperFile *super = df->querySuperFile();
    superCount = 1;
    superIndex = 0;
    nextPartNumber = 0;
    if (super)
    {
        superIterator.setown(super->getSubFileIterator(true));
        superCount = super->numSubFiles(true);
        if (helper.getFlags() & TIRsorted)
            throw MakeStringException(1000, "SORTED attribute is not supported when reading from superkey");
        superName.append(df->queryLogicalName());
        df.clear();
    }
    else if (df->numParts() == 1)
    {
        singlePart = true;
    }

    eclKeySize.set(helper.queryDiskRecordSize());

    postFiltered = 0;
    seeks = 0;
    scans = 0;
    helper.setCallback(&callback);
    limitTransformExtra = static_cast<IHThorSourceLimitTransformExtra *>(helper.selectInterface(TAIsourcelimittransformextra_1));
    gotLayoutTrans = false;
}

CHThorIndexReadActivityBase::~CHThorIndexReadActivityBase()
{
//  releaseHThorRow(recBuffer);
    rtlFree(activityRecordMetaBuff);
}

void CHThorIndexReadActivityBase::ready()
{
    CHThorActivityBase::ready();
    if(!gotLayoutTrans)
    {
        getLayoutTranslators();
        gotLayoutTrans = true;
    }
    firstPart();
}

void CHThorIndexReadActivityBase::done()
{ 
    killPart(); 
    CHThorActivityBase::done(); 
}

bool CHThorIndexReadActivityBase::doPreopenLimit(unsigned __int64 limit)
{
    if(!helper.canMatchAny())
        return false;
    keyIndexCache.setown(createKeyIndexSet());
    unsigned __int64 count = 0;
    if(superIterator)
    {
        superIterator->first();
        do
        {
            df.set(&superIterator->query());
            if(doPreopenLimitFile(count, limit))
                return true;
            ++superIndex;
        } while(superIterator->next());
        return false;
    }
    else
    {
        return doPreopenLimitFile(count, limit);
    }
}

bool CHThorIndexReadActivityBase::doPreopenLimitFile(unsigned __int64 & count, unsigned __int64 limit)
{
    unsigned num = df->numParts()-1;
    if(num)
    {
        if(localSortKey)
        {
            Owned<IKeyIndex> tlk = openKeyFile(df->queryPart(num));
            verifyIndex(tlk);
            for(unsigned idx = 0; idx < num; ++idx)
            {
                keyIndexCache->addIndex(doPreopenLimitPart(count, limit, idx));
                if(superIterator)
                    superIndexCache.append(superIndex);
            }
        }
        else
        {
            Owned<IKeyIndex> tlk = openKeyFile(df->queryPart(num));
            verifyIndex(tlk);
            Owned<IKeyManager> tlman = createKeyManager(tlk, keySize, NULL);
            initManager(tlman);
            while(tlman->lookup(false) && (count<=limit))
            {
                unsigned slavePart = (unsigned)tlman->queryFpos();
                if (slavePart)
                {
                    keyIndexCache->addIndex(doPreopenLimitPart(count, limit, slavePart-1));
                    if(superIterator)
                        superIndexCache.append(superIndex);
                }
            }
            if (count>limit)
            {
                if ( agent.queryCodeContext()->queryDebugContext())
                    agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            }
        }
    }
    else
    {
        keyIndexCache->addIndex(doPreopenLimitPart(count, limit, 0));
        if(superIterator)
            superIndexCache.append(superIndex);
    }
    return (count>limit);
}

IKeyIndex * CHThorIndexReadActivityBase::doPreopenLimitPart(unsigned __int64 & result, unsigned __int64 limit, unsigned part)
{
    Owned<IKeyIndex> kidx;
    kidx.setown(openKeyFile(df->queryPart(part)));
    if(df->numParts() == 1)
        verifyIndex(kidx);
    if (limit != (unsigned) -1)
    {
        Owned<IKeyManager> kman = createKeyManager(kidx, keySize, NULL);
        initManager(kman);
        result += kman->checkCount(limit-result);
    }
    return kidx.getClear();
}

void CHThorIndexReadActivityBase::openTlk()
{
    tlk.setown(openKeyFile(df->queryPart(df->numParts()-1)));
}


const void * CHThorIndexReadActivityBase::createKeyedLimitOnFailRow()
{
    RtlDynamicRowBuilder rowBuilder(rowAllocator);
    size32_t newSize = limitTransformExtra->transformOnKeyedLimitExceeded(rowBuilder);
    if (newSize)
        return rowBuilder.finalizeRowClear(newSize);
    return NULL;
}

bool CHThorIndexReadActivityBase::firstPart()
{
    killPart();
    if (helper.canMatchAny())
    {
        if(keyIndexCache)
        {
            keyIndexCacheIdx = 0;
            return nextPart();
        }

        if (singlePart)
        {
            //part is cached and not reloaded - for efficiency in subqueries.
            if (!keyIndex)
                return setCurrentPart(0);
            initPart();
            return true;
        }

        if (superIterator)
        {
            superIterator->first();
            superIndex = 0;
            return doNextSuper();
        }
        else
            return firstMultiPart();
    }
    return false;
}


bool CHThorIndexReadActivityBase::nextPart()
{
    killPart();
    if(keyIndexCache)
    {
        if(keyIndexCacheIdx >= keyIndexCache->numParts())
            return false;
        keyIndex.set(keyIndexCache->queryPart(keyIndexCacheIdx));
        if(superIterator)
        {
            superIndex = superIndexCache.item(keyIndexCacheIdx);
            layoutTrans.set(layoutTransArray.item(superIndex));
            keySize = keyIndex->keySize();
        }
        ++keyIndexCacheIdx;
        initPart();
        return true;
    }

    if (singlePart)
        return false;

    if (nextMultiPart())
        return true;

    if (superIterator && superIterator->next())
    {
        ++superIndex;
        return doNextSuper();
    }

    return false;
}


void CHThorIndexReadActivityBase::initManager(IKeyManager *manager)
{
    if(layoutTrans)
        manager->setLayoutTranslator(layoutTrans);
    helper.createSegmentMonitors(manager);
    manager->finishSegmentMonitors();
    manager->reset();
}

void CHThorIndexReadActivityBase::initPart()                                    
{ 
    klManager.setown(createKeyManager(keyIndex, keySize, NULL));
    initManager(klManager);     
    callback.setManager(klManager);
}

void CHThorIndexReadActivityBase::killPart()
{
    callback.setManager(NULL);
    if (klManager)
    {
        seeks += klManager->querySeeks();
        scans += klManager->queryScans();
        klManager.clear();
    }
}

bool CHThorIndexReadActivityBase::setCurrentPart(unsigned whichPart)
{
    keyIndex.setown(openKeyFile(df->queryPart(whichPart)));
    if(df->numParts() == 1)
        verifyIndex(keyIndex);
    initPart();
    return true;
}

bool CHThorIndexReadActivityBase::firstMultiPart()
{
    if(!tlk)
        openTlk();
    verifyIndex(tlk);
    tlManager.setown(createKeyManager(tlk, keySize, NULL));
    initManager(tlManager);
    nextPartNumber = 0;
    return nextMultiPart();
}

bool CHThorIndexReadActivityBase::nextMultiPart()
{
    //tlManager may be null for a single part index within a superfile.
    if (tlManager)
    {
        if (localSortKey)
        {
            if (nextPartNumber<(df->numParts()-1))
                return setCurrentPart(nextPartNumber++);
        }
        else
        {
            while (tlManager->lookup(false))
            {
                if (tlManager->queryFpos())
                    return setCurrentPart((unsigned)tlManager->queryFpos()-1);
            }
        }
    }
    return false;
}

bool CHThorIndexReadActivityBase::doNextSuper()
{
    do
    {
        clearTlk(); 
        df.set(&superIterator->query());
        unsigned numParts = df->numParts();
        if (numParts==1)
            return setCurrentPart(0);

        if (firstMultiPart())
            return true;
        ++superIndex;
    } while (superIterator->next());
    return false;
}

void CHThorIndexReadActivityBase::getLayoutTranslators()
{
    if(superIterator)
    {
        superIterator->first();
        do
        {
            IDistributedFile & f = superIterator->query();
            layoutTrans.setown(getLayoutTranslator(&f));
            if(layoutTrans)
            {
                StringBuffer buff;
                buff.append("Using record layout translation to correct layout mismatch on reading index ").append(f.queryLogicalName());
                WARNLOG("%s", buff.str());
                agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
            }
            layoutTransArray.append(layoutTrans.getClear());
        } while(superIterator->next());
    }
    else
    {
        layoutTrans.setown(getLayoutTranslator(df));
        if(layoutTrans)
        {
            StringBuffer buff;
            buff.append("Using record layout translation to correct layout mismatch on reading index ").append(df->queryLogicalName());
            WARNLOG("%s", buff.str());
            agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
        }
    }
}

IRecordLayoutTranslator * CHThorIndexReadActivityBase::getLayoutTranslator(IDistributedFile * f)
{
    if(agent.queryWorkUnit()->getDebugValueBool("skipFileFormatCrcCheck", false))
        return NULL;

    if(!rltEnabled(agent.queryWorkUnit()))
    {
        verifyFormatCrc(helper.getFormatCrc(), f, (superIterator ? superName.str() : NULL) , true, true);
        return NULL;
    }

    if(verifyFormatCrc(helper.getFormatCrc(), f, (superIterator ? superName.str() : NULL) , true, false))
        return NULL;
        
    if(!activityRecordMeta)
    {
        if(!helper.getIndexLayout(activityRecordMetaSize, activityRecordMetaBuff))
            throw MakeStringException(0, "Unable to recover from record layout mismatch for index %s: no record layout metadata in activity", f->queryLogicalName());
        MemoryBuffer buff;
        buff.setBuffer(activityRecordMetaSize, activityRecordMetaBuff, false);
        activityRecordMeta.setown(deserializeRecordMeta(buff, true));
    }

    return getRecordLayoutTranslator(activityRecordMeta, activityRecordMetaSize, activityRecordMetaBuff, f, agent.queryRecordLayoutTranslatorCache());
}

void CHThorIndexReadActivityBase::verifyIndex(IKeyIndex * idx)
{
    if(superIterator)
        layoutTrans.set(layoutTransArray.item(superIndex));
    keySize = idx->keySize();
    if (eclKeySize.isFixedSize())
    {
        if(layoutTrans)
            layoutTrans->checkSizes(df->queryLogicalName(), eclKeySize.getFixedSize(), keySize);
        else
            if (keySize != eclKeySize.getFixedSize())
                throw MakeStringException(0, "Key size mismatch reading index %s: index indicates size %u, ECL indicates size %u", df->queryLogicalName(), keySize, eclKeySize.getFixedSize());
    }
}

//-------------------------------------------------------------------------------------------------------------

class CHThorIndexReadActivity : public CHThorIndexReadActivityBase
{

public:
    CHThorIndexReadActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexReadArg &_arg, ThorActivityKind _kind, IDistributedFile * df);
    ~CHThorIndexReadActivity();

    //interface IHThorInput
    virtual void ready();
    virtual const void *nextInGroup();
    virtual const void * nextGE(const void * seek, unsigned numFields);

    virtual IInputSteppingMeta * querySteppingMeta();

protected:
    virtual bool nextPart();
    virtual void initPart();

protected:
    IHThorIndexReadArg &helper;
    IHThorSteppedSourceExtra * steppedExtra;
    unsigned __int64 keyedProcessed;
    unsigned __int64 keyedLimit;
    unsigned __int64 rowLimit;
    unsigned __int64 stopAfter;
    ISteppingMeta * rawMeta;
    ISteppingMeta * projectedMeta;
    size32_t seekGEOffset;
    unsigned * seekSizes;
    CSteppingMeta steppingMeta;
    bool needTransform;
    bool keyedLimitReached;
    bool keyedLimitSkips;
    bool keyedLimitCreates;
    bool keyedLimitRowCreated;
};

CHThorIndexReadActivity::CHThorIndexReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexReadArg &_arg, ThorActivityKind _kind, IDistributedFile * _df) 
    : CHThorIndexReadActivityBase(_agent, _activityId, _subgraphId, _arg, _kind, _df), helper(_arg)
{
    steppedExtra = static_cast<IHThorSteppedSourceExtra *>(helper.selectInterface(TAIsteppedsourceextra_1));
    needTransform = helper.needTransform();
    keyedLimit = helper.getKeyedLimit();
    rowLimit = helper.getRowLimit();
    if (helper.getFlags() & TIRlimitskips)
        rowLimit = (unsigned __int64) -1;
    stopAfter = helper.getChooseNLimit();
    keyedLimitReached = false;
    keyedLimitSkips = ((helper.getFlags() & TIRkeyedlimitskips) != 0);
    keyedLimitCreates = ((helper.getFlags() & TIRkeyedlimitcreates) != 0);
    keyedLimitRowCreated = false;
    keyedProcessed = 0;
    rawMeta = helper.queryRawSteppingMeta();
    projectedMeta = helper.queryProjectedSteppingMeta();
    seekGEOffset = 0;
    seekSizes = 0;
    if (rawMeta)
    {
        //should check that no translation, also should check all keys in maxFields list can actually be keyed.
        const CFieldOffsetSize * fields = rawMeta->queryFields();
        unsigned maxFields = rawMeta->getNumFields();
        seekGEOffset = fields[0].offset;
        seekSizes = new unsigned[maxFields];
        seekSizes[0] = fields[0].size;
        for (unsigned i=1; i < maxFields; i++)
            seekSizes[i] = seekSizes[i-1] + fields[i].size;
        if (projectedMeta)
            steppingMeta.init(projectedMeta, false);
        else
            steppingMeta.init(rawMeta, false);
    }
}

CHThorIndexReadActivity::~CHThorIndexReadActivity()
{
    delete [] seekSizes;
}

void CHThorIndexReadActivity::ready()
{
    keyedLimitReached = false;
    keyedLimitRowCreated = false;
    keyedProcessed = 0;
    if(!gotLayoutTrans)
    {
        getLayoutTranslators();
        gotLayoutTrans = true;
    }
    if (seekGEOffset || localSortKey || ((keyedLimit != (unsigned __int64) -1) && ((helper.getFlags() & TIRcountkeyedlimit) != 0) && !singlePart))
        keyedLimitReached = doPreopenLimit(keyedLimit);
    CHThorIndexReadActivityBase::ready();
    if (steppedExtra)
        steppingMeta.setExtra(steppedExtra);
    if(klManager && (keyedLimit != (unsigned __int64) -1) && ((helper.getFlags() & TIRcountkeyedlimit) != 0) && singlePart && !seekGEOffset)
    {
        unsigned __int64 result = klManager->checkCount(keyedLimit);
        keyedLimitReached = (result > keyedLimit);
        klManager->reset();
    }
}

bool CHThorIndexReadActivity::nextPart()
{
    if(keyIndexCache && (seekGEOffset || localSortKey))
    {
        klManager.setown(createKeyMerger(keyIndexCache, keySize, seekGEOffset, NULL));
        keyIndexCache.clear();
        initManager(klManager);
        return true;
    }
    else if (seekGEOffset || localSortKey)
        return false;
    else
        return CHThorIndexReadActivityBase::nextPart();
}

void CHThorIndexReadActivity::initPart()
{ 
    CHThorIndexReadActivityBase::initPart();
}

const void *CHThorIndexReadActivity::nextInGroup()
{
    if(keyedLimitReached)
    {
        if (keyedLimitSkips)
            return NULL;
        if (keyedLimitCreates)
        {
            if (!keyedLimitRowCreated)
            {
                keyedLimitRowCreated = true;
                return createKeyedLimitOnFailRow();
            }
            return NULL;
        }
        helper.onKeyedLimitExceeded(); // should throw exception
    }

    if((stopAfter && (processed-initialProcessed)==stopAfter) || !klManager)
        return NULL;

    loop
    {
        agent.reportProgress(NULL);

        if (klManager->lookup(true))
        {
            keyedProcessed++;
            if ((keyedLimit != (unsigned __int64) -1) && keyedProcessed > keyedLimit)
                helper.onKeyedLimitExceeded();
            byte const * keyRow = klManager->queryKeyBuffer(callback.getFPosRef());
            if (needTransform)
            {
                try
                {
                    size32_t recSize;
                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                    recSize = helper.transform(rowBuilder, keyRow);
                    callback.finishedRow();
                    if (recSize)
                    {
                        processed++;
                        if ((processed-initialProcessed) > rowLimit)
                        {
                            helper.onLimitExceeded();
                            if ( agent.queryCodeContext()->queryDebugContext())
                                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                        }
                        return rowBuilder.finalizeRowClear(recSize);
                    }
                    else
                    {
                        postFiltered++;
                    }
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
            }
            else
            {
                processed++;
                if ((processed-initialProcessed) > rowLimit)
                {
                    helper.onLimitExceeded();
                    if ( agent.queryCodeContext()->queryDebugContext())
                        agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                }
                try
                {
                    return cloneRow(agent.queryCodeContext(), rowAllocator, keyRow);
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
            }
        }
        else if (!nextPart())
            return NULL;
    }
}


const void *CHThorIndexReadActivity::nextGE(const void * seek, unsigned numFields)
{
    if(keyedLimitReached && !keyedLimitSkips)
        helper.onKeyedLimitExceeded(); // should throw exception

    if(keyedLimitReached || (stopAfter && (processed-initialProcessed)==stopAfter) || !klManager)
        return NULL;

    const byte * rawSeek = (const byte *)seek + seekGEOffset;
    unsigned seekSize = seekSizes[numFields-1];
    if (projectedMeta)
    {
        byte *temp = (byte *) alloca(seekSize);
        RtlStaticRowBuilder tempBuilder(temp - seekGEOffset, seekSize + seekGEOffset);
        helper.mapOutputToInput(tempBuilder, seek, numFields); // NOTE - weird interface to mapOutputToInput means that it STARTS writing at seekGEOffset...
        rawSeek = (byte *)temp;
    }
    loop
    {
        agent.reportProgress(NULL);

        if (klManager->lookupSkip(rawSeek, seekGEOffset, seekSize))
        {
            const byte * row = klManager->queryKeyBuffer(callback.getFPosRef());
#ifdef _DEBUG
            if (memcmp(row + seekGEOffset, rawSeek, seekSize) < 0)
                assertex("smart seek failure");
#endif

            keyedProcessed++;
            if ((keyedLimit != (unsigned __int64) -1) && keyedProcessed > keyedLimit)
                helper.onKeyedLimitExceeded();
            if (needTransform)
            {
                try
                {
                    size32_t recSize;
                    RtlDynamicRowBuilder rowBuilder(rowAllocator);
                    recSize = helper.transform(rowBuilder, row);
                    callback.finishedRow();
                    if (recSize)
                    {
                        processed++;
                        if ((processed-initialProcessed) > rowLimit)
                        {
                            helper.onLimitExceeded();
                            if ( agent.queryCodeContext()->queryDebugContext())
                                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                        }
                        return rowBuilder.finalizeRowClear(recSize);
                    }
                    else
                    {
                        postFiltered++;
                    }
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
            }
            else
            {
                processed++;
                if ((processed-initialProcessed) > rowLimit)
                {
                    helper.onLimitExceeded();
                    if ( agent.queryCodeContext()->queryDebugContext())
                        agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                }
                try
                {
                    return cloneRow(agent.queryCodeContext(), rowAllocator, row);
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
            }
        }
        else if (!nextPart())
            return NULL;
    }
}


IInputSteppingMeta * CHThorIndexReadActivity::querySteppingMeta()
{
    if (rawMeta)
        return &steppingMeta;
    return NULL;
}

extern HTHOR_API IHThorActivity *createIndexReadActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexReadArg &arg, ThorActivityKind _kind)
{
    // A logical filename for the key should refer to a single physical file - either the TLK or a monolithic key
    const char *lfn = arg.getFileName();
    Owned<ILocalOrDistributedFile> ldFile = _agent.resolveLFN(lfn, "IndexRead", 0 != (arg.getFlags() & TIRoptional));
    Linked<IDistributedFile> dFile = ldFile ? ldFile->queryDistributedFile() : NULL;
    if (!dFile)
    {
        StringBuffer buff;
        buff.append("Skipping OPT index read of nonexistent file ").append(lfn);
        WARNLOG("%s", buff.str());
        _agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
        return new CHThorNullActivity(_agent, _activityId, _subgraphId, arg, _kind);
    }
    _agent.logFileAccess(dFile, "HThor", "READ");
    enterSingletonSuperfiles(dFile);
    return new CHThorIndexReadActivity(_agent, _activityId, _subgraphId, arg, _kind, dFile);
}

//-------------------------------------------------------------------------------------------------------------


class CHThorIndexNormalizeActivity : public CHThorIndexReadActivityBase
{

public:
    CHThorIndexNormalizeActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexNormalizeArg &_arg, ThorActivityKind _kind, IDistributedFile * df);
    ~CHThorIndexNormalizeActivity();

    virtual void ready();
    virtual void done();
    virtual const void *nextInGroup();
    virtual bool needsAllocator() const { return true; }

protected:
    const void * createNextRow();

protected:
    IHThorIndexNormalizeArg &helper;
    unsigned __int64 rowLimit;
    unsigned __int64 stopAfter;
    RtlDynamicRowBuilder outBuilder;
    unsigned __int64 keyedProcessed;
    unsigned __int64 keyedLimit;
    bool skipLimitReached;
    bool expanding;
};


CHThorIndexNormalizeActivity::CHThorIndexNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexNormalizeArg &_arg, ThorActivityKind _kind, IDistributedFile * _df) : CHThorIndexReadActivityBase(_agent, _activityId, _subgraphId, _arg, _kind, _df), helper(_arg), outBuilder(NULL)
{
    keyedLimit = helper.getKeyedLimit();
    skipLimitReached = false;
    keyedProcessed = 0;
    rowLimit = helper.getRowLimit();
    if (helper.getFlags() & TIRlimitskips)
        rowLimit = (unsigned __int64) -1;
    stopAfter = helper.getChooseNLimit();
}

CHThorIndexNormalizeActivity::~CHThorIndexNormalizeActivity()
{
}

void CHThorIndexNormalizeActivity::ready()
{
    skipLimitReached = false;
    keyedProcessed = 0;
    expanding = false;
    CHThorIndexReadActivityBase::ready();
    outBuilder.setAllocator(rowAllocator);
}

void CHThorIndexNormalizeActivity::done()
{
    outBuilder.clear();
    CHThorIndexReadActivityBase::done();
}

const void *CHThorIndexNormalizeActivity::nextInGroup()
{
    if ((stopAfter && (processed-initialProcessed)==stopAfter) || !klManager)
        return NULL;

    if (skipLimitReached || (stopAfter && (processed-initialProcessed)==stopAfter) || !klManager)
        return NULL;

    if ((keyedLimit != (unsigned __int64) -1) && (helper.getFlags() & TIRcountkeyedlimit) != 0)
    {
        unsigned __int64 result = klManager->checkCount(keyedLimit);
        if (result > keyedLimit)
        {
            if((helper.getFlags() & TIRkeyedlimitskips) != 0)
                skipLimitReached = true;
            else if((helper.getFlags() & TIRkeyedlimitcreates) != 0)
            {
                skipLimitReached = true;
                return createKeyedLimitOnFailRow();
            }
            else
                helper.onKeyedLimitExceeded(); // should throw exception
            return NULL;
        }
        klManager->reset();
        keyedLimit = (unsigned __int64) -1; // to avoid checking it again
    }
    assertex(!((keyedLimit != (unsigned __int64) -1) && ((helper.getFlags() & TIRkeyedlimitskips) != 0)));

    loop
    {
        loop
        {
            if (expanding)
            {
                loop
                {
                    expanding = helper.next();
                    if (!expanding)
                        break;

                    const void * ret = createNextRow();
                    if (ret)
                        return ret;
                }
            }

            callback.finishedRow();
            while (!klManager->lookup(true))
            {
                keyedProcessed++;
                if ((keyedLimit != (unsigned __int64) -1) && keyedProcessed > keyedLimit)
                    helper.onKeyedLimitExceeded();

                if (!nextPart())
                    return NULL;
            }

            agent.reportProgress(NULL);
            expanding = helper.first(klManager->queryKeyBuffer(callback.getFPosRef()));
            if (expanding)
            {
                const void * ret = createNextRow();
                if (ret)
                    return ret;
            }
        }
    }
}

const void * CHThorIndexNormalizeActivity::createNextRow()
{
    try
    {
        outBuilder.ensureRow();
        size32_t thisSize = helper.transform(outBuilder);
        if (thisSize == 0)
        {
            return NULL;
        }

        OwnedConstRoxieRow ret = outBuilder.finalizeRowClear(thisSize);
        if ((processed - initialProcessed) >=rowLimit)
        {
            helper.onLimitExceeded();
            if ( agent.queryCodeContext()->queryDebugContext())
                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            return NULL;
        }
        processed++;
        return ret.getClear();
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }

}

extern HTHOR_API IHThorActivity *createIndexNormalizeActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexNormalizeArg &arg, ThorActivityKind _kind)
{
    // A logical filename for the key should refer to a single physical file - either the TLK or a monolithic key
    const char *lfn = arg.getFileName();
    Owned<ILocalOrDistributedFile> ldFile = _agent.resolveLFN(lfn, "IndexNormalize", 0 != (arg.getFlags() & TIRoptional),true,true);
    Linked<IDistributedFile> dFile = ldFile ? ldFile->queryDistributedFile() : NULL;
    if (!dFile)
    {
        StringBuffer buff;
        buff.append("Skipping OPT index normalize of nonexistent file ").append(lfn);
        WARNLOG("%s", buff.str());
        _agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
        return new CHThorNullActivity(_agent, _activityId, _subgraphId, arg, _kind);
    }
    _agent.logFileAccess(dFile, "HThor", "READ");
    enterSingletonSuperfiles(dFile);
    return new CHThorIndexNormalizeActivity(_agent, _activityId, _subgraphId, arg, _kind, dFile);
}

//-------------------------------------------------------------------------------------------------------------


class CHThorIndexAggregateActivity : public CHThorIndexReadActivityBase
{

public:
    CHThorIndexAggregateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexAggregateArg &_arg, ThorActivityKind _kind, IDistributedFile * df);
    ~CHThorIndexAggregateActivity();

    //interface IHThorInput
    virtual void done();
    virtual void ready();
    virtual const void *nextInGroup();
    virtual bool needsAllocator() const { return true; }

protected:
    void * createNextRow();
    void gather();

protected:
    IHThorIndexAggregateArg &helper;
    RtlDynamicRowBuilder outBuilder;
    bool finished;
};


CHThorIndexAggregateActivity::CHThorIndexAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexAggregateArg &_arg, ThorActivityKind _kind, IDistributedFile * _df) 
    : CHThorIndexReadActivityBase(_agent, _activityId, _subgraphId, _arg, _kind, _df), helper(_arg), outBuilder(NULL)
{
}

CHThorIndexAggregateActivity::~CHThorIndexAggregateActivity()
{
}

void CHThorIndexAggregateActivity::ready()
{
    CHThorIndexReadActivityBase::ready();
    outBuilder.setAllocator(rowAllocator);
    finished = false;
}

void CHThorIndexAggregateActivity::done()
{
    outBuilder.clear();
    CHThorIndexReadActivityBase::done();
}



void CHThorIndexAggregateActivity::gather()
{
    outBuilder.ensureRow();

    try
    {
        helper.clearAggregate(outBuilder);
    }
    catch(IException * e)
    {
        throw makeWrappedException(e);
    }
    if(!klManager)
        return;

    loop
    {
        while (!klManager->lookup(true))
        {
            if (!nextPart())
                return;
        }

        agent.reportProgress(NULL);
        try
        {
            helper.processRow(outBuilder, klManager->queryKeyBuffer(callback.getFPosRef()));
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
        callback.finishedRow();
    }
}

const void *CHThorIndexAggregateActivity::nextInGroup()
{
    if (finished) return NULL;
    gather();

    processed++;
    finished = true;
    size32_t size = outputMeta.getRecordSize(outBuilder.getSelf());
    return outBuilder.finalizeRowClear(size);
}


extern HTHOR_API IHThorActivity *createIndexAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexAggregateArg &arg, ThorActivityKind _kind)
{
    // A logical filename for the key should refer to a single physical file - either the TLK or a monolithic key
    const char *lfn = arg.getFileName();
    Owned<ILocalOrDistributedFile> ldFile = _agent.resolveLFN(lfn, "IndexAggregate", 0 != (arg.getFlags() & TIRoptional));
    Linked<IDistributedFile> dFile = ldFile ? ldFile->queryDistributedFile() : NULL;
    if (!dFile)
    {
        StringBuffer buff;
        buff.append("Skipping OPT index aggregate of nonexistent file ").append(lfn);
        WARNLOG("%s", buff.str());
        _agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
        return new CHThorNullAggregateActivity(_agent, _activityId, _subgraphId, arg, arg, _kind);
    }
    _agent.logFileAccess(dFile, "HThor", "READ");
    enterSingletonSuperfiles(dFile);
    return new CHThorIndexAggregateActivity(_agent, _activityId, _subgraphId, arg, _kind, dFile);
}

//-------------------------------------------------------------------------------------------------------------

class CHThorIndexCountActivity : public CHThorIndexReadActivityBase
{

public:
    CHThorIndexCountActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexCountArg &_arg, ThorActivityKind _kind, IDistributedFile * df);

    //interface IHThorInput
    virtual void ready();
    virtual const void *nextInGroup();

protected:
    void * createNextRow();

protected:
    IHThorIndexCountArg &helper;
    unsigned __int64 choosenLimit;
    bool finished;
};


CHThorIndexCountActivity::CHThorIndexCountActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexCountArg &_arg, ThorActivityKind _kind, IDistributedFile * _df) 
    : CHThorIndexReadActivityBase(_agent, _activityId, _subgraphId, _arg, _kind, _df), helper(_arg)
{
}

void CHThorIndexCountActivity::ready()
{
    CHThorIndexReadActivityBase::ready();
    finished = false;
    choosenLimit = helper.getChooseNLimit();
}

const void *CHThorIndexCountActivity::nextInGroup()
{
    if (finished) return NULL;

    unsigned __int64 totalCount = 0;
    if(klManager)
    {
        loop
        {
            if (helper.hasFilter())
            {
                loop
                {
                    agent.reportProgress(NULL);
                    if (!klManager->lookup(true))
                        break;
                    totalCount += helper.numValid(klManager->queryKeyBuffer(callback.getFPosRef()));
                    callback.finishedRow();
                    if ((totalCount > choosenLimit))
                        break;
                }
            }
            else
                totalCount += klManager->getCount();

            if ((totalCount > choosenLimit) || !nextPart())
                break;
        }
    }

    finished = true;
    processed++;
    if (totalCount > choosenLimit)
        totalCount = choosenLimit;

    size32_t outSize = outputMeta.getFixedSize();
    void * ret = rowAllocator->createRow(); //meta: outputMeta
    if (outSize == 1)
    {
        assertex(choosenLimit == 1);
        *(byte *)ret = (byte)totalCount;
    }
    else
    {
        assertex(outSize == sizeof(unsigned __int64));
        *(unsigned __int64 *)ret = totalCount;
    }
    return ret = rowAllocator->finalizeRow(outSize, ret, outSize);
}


extern HTHOR_API IHThorActivity *createIndexCountActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexCountArg &arg, ThorActivityKind _kind)
{
    // A logical filename for the key should refer to a single physical file - either the TLK or a monolithic key
    const char *lfn = arg.getFileName();
    Owned<ILocalOrDistributedFile> ldFile = _agent.resolveLFN(lfn, "IndexCount", 0 != (arg.getFlags() & TIRoptional));
    Linked<IDistributedFile> dFile = ldFile ? ldFile->queryDistributedFile() : NULL;
    if (!dFile)
    {
        StringBuffer buff;
        buff.append("Skipping OPT index count of nonexistent file ").append(lfn);
        WARNLOG("%s", buff.str());
        _agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
        return new CHThorNullCountActivity(_agent, _activityId, _subgraphId, arg, _kind);
    }
    _agent.logFileAccess(dFile, "HThor", "READ");
    enterSingletonSuperfiles(dFile);
    return new CHThorIndexCountActivity(_agent, _activityId, _subgraphId, arg, _kind, dFile);
}

//-------------------------------------------------------------------------------------------------------------

class CHThorIndexGroupAggregateActivity : public CHThorIndexReadActivityBase, implements IHThorGroupAggregateCallback
{

public:
    CHThorIndexGroupAggregateActivity(IAgentContext &agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexGroupAggregateArg &_arg, ThorActivityKind _kind, IDistributedFile * df);
    IMPLEMENT_IINTERFACE

    //interface IHThorInput
    virtual void ready();
    virtual const void *nextInGroup();
    virtual bool needsAllocator() const { return true; }        
    virtual void processRow(const void * next);

protected:
    void * createNextRow();
    void gather();

protected:
    IHThorIndexGroupAggregateArg &helper;
    RowAggregator aggregated;
    bool eof;
    bool gathered;
};


CHThorIndexGroupAggregateActivity::CHThorIndexGroupAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexGroupAggregateArg &_arg, ThorActivityKind _kind, IDistributedFile * _df) : CHThorIndexReadActivityBase(_agent, _activityId, _subgraphId, _arg, _kind, _df), helper(_arg), aggregated(_arg, _arg)
{
}

void CHThorIndexGroupAggregateActivity::ready()
{
    CHThorIndexReadActivityBase::ready();
    eof = false;
    gathered = false;
    aggregated.reset();
    aggregated.start(rowAllocator);
}

void CHThorIndexGroupAggregateActivity::processRow(const void * next)
{
    aggregated.addRow(next);
}


void CHThorIndexGroupAggregateActivity::gather()
{
    gathered = true;
    if(!klManager)
        return;
    loop
    {
        while (!klManager->lookup(true))
        {
            if (!nextPart())
                return;
        }
                
        agent.reportProgress(NULL);
        try
        {
            helper.processRow(klManager->queryKeyBuffer(callback.getFPosRef()), this);
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
        callback.finishedRow();
    }
}

const void *CHThorIndexGroupAggregateActivity::nextInGroup()
{
    if (eof)
        return NULL;

    if (!gathered)
        gather();

    Owned<AggregateRowBuilder> next = aggregated.nextResult();
    if (next)
    {
        processed++;
        return next->finalizeRowClear();
    }
    eof = true;
    return NULL;
}


extern HTHOR_API IHThorActivity *createIndexGroupAggregateActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorIndexGroupAggregateArg &arg, ThorActivityKind _kind)
{
    // A logical filename for the key should refer to a single physical file - either the TLK or a monolithic key
    const char *lfn = arg.getFileName();
    Owned<ILocalOrDistributedFile> ldFile = _agent.resolveLFN(lfn, "IndexGroupAggregate", 0 != (arg.getFlags() & TIRoptional));
    Linked<IDistributedFile> dFile = ldFile ? ldFile->queryDistributedFile() : NULL;
    if (!dFile)
    {
        StringBuffer buff;
        buff.append("Skipping OPT index group aggregate of nonexistent file ").append(lfn);
        WARNLOG("%s", buff.str());
        _agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
        return new CHThorNullActivity(_agent, _activityId, _subgraphId, arg, _kind);
    }
    _agent.logFileAccess(dFile, "HThor", "READ");
    enterSingletonSuperfiles(dFile);
    return new CHThorIndexGroupAggregateActivity(_agent, _activityId, _subgraphId, arg, _kind, dFile);
}

//-------------------------------------------------------------------------------------------------------------

interface IThreadedExceptionHandler
{
    virtual void noteException(IException *E) = 0;
};

template <class ROW, class OWNER>
class PartHandlerThread : public CInterface, implements IPooledThread
{
public:
    typedef PartHandlerThread<ROW, OWNER> SELF;
    IMPLEMENT_IINTERFACE;
    PartHandlerThread() : owner(0)
    {
    }
    virtual void init(void * _owner) { owner = (OWNER *)_owner; }
    virtual void main()
    {
        try
        {
            owner->openPart();
            loop
            {
                ROW * row = owner->getRow();
                if (!row)
                    break;
                owner->doRequest(row);
            }
        }
        catch (IException *E)
        {
            owner->noteException(E);
        }
    }

    bool stop()
    {
        owner->stop();
        return true;
    }

    virtual bool canReuse() { return true; }
private:
    OWNER * owner;
};

template <class ROW>
class ThreadedPartHandler : public CInterface
{
protected:
    Linked<IThreadPool> threadPool;
    PooledThreadHandle threadHandle;
    QueueOf<ROW, true> pending;
    CriticalSection crit;
    Semaphore limit;
    bool started;
    Owned<IDistributedFilePart> part;
    IThreadedExceptionHandler *handler;

public:
    typedef ThreadedPartHandler<ROW> SELF;
    ThreadedPartHandler(IDistributedFilePart *_part, IThreadedExceptionHandler *_handler, IThreadPool * _threadPool)
        : limit(MAX_FETCH_LOOKAHEAD), part(_part), handler(_handler), threadHandle(0), threadPool(_threadPool)
    {
        started = false;
    }

    ~ThreadedPartHandler()
    {
        //is it the responsibility of the derived class to clean up the list on destruction --- can do nothing but assert here, since implementations different and VMTs gone by now
        assertex(pending.ordinality() == 0);
    }

    void addRow(ROW * row)
    {
        limit.wait();
        CriticalBlock procedure(crit);
        pending.enqueue(row);
        if (!started)
        {
            started = true;
            start();
        }
    }

    void stop()
    {
    }

    void start()
    {
        threadHandle = threadPool->start(this);
    }

    void join()
    {
        threadPool->join(threadHandle);
        started = false;
    }

    ROW * getRow()
    {
        CriticalBlock procedure(crit);
        if(pending.ordinality())
        {
            limit.signal();
            return pending.dequeue();
        }
        else
        {
            started = false; //because returning NULL will cause thread to terminate (has to be within this CriticalBlock to avoid race cond.)
            return NULL;
        }
    }

    void noteException(IException * e)
    {
        handler->noteException(e);
    }

private:
    friend class PartHandlerThread<ROW, SELF>;
    virtual void doRequest(ROW * row) = 0; // Must be implemented by derived class
    virtual void openPart() = 0;         // Must be implemented by derived class
};

template <class ROW>
class PartHandlerThreadFactory : public CInterface, implements IThreadFactory
{
    IMPLEMENT_IINTERFACE;
    typedef ThreadedPartHandler<ROW> OWNER;
    IPooledThread * createNew() { return new PartHandlerThread<ROW, OWNER>(); }
};

class FetchRequest : public CInterface
{
public:
    const void * left;
    offset_t pos;
    offset_t seq;
    FetchRequest(const void * _left, offset_t _pos, offset_t _seq) : left(_left), pos(_pos), seq(_seq) {}
    ~FetchRequest() { releaseHThorRow(left); }
};

class IFlatFetchHandlerCallback
{
public:
    virtual void processFetch(FetchRequest const * fetch, offset_t pos, ISerialStream *rawStream) = 0;
};

class IXmlFetchHandlerCallback
{
public:
    virtual void processFetched(FetchRequest const * fetch, IColumnProvider * lastMatch) = 0;
    virtual IException * makeWrappedException(IException * e, char const * extra) const = 0;
};

// this class base for all three fetch activities and keyed join
class FetchPartHandlerBase
{
protected:
    Owned<IFileIO> rawFile;
    Owned<ISerialStream> rawStream;
    offset_t base;
    offset_t top;
    bool blockcompressed;
    MemoryAttr encryptionkey;
    unsigned activityId;
    CachedOutputMetaData const & outputMeta;
    IEngineRowAllocator * rowAllocator;
    IOutputRowDeserializer * rowDeserializer;
public:
    FetchPartHandlerBase(offset_t _base, offset_t _size, bool _blockcompressed, MemoryAttr &_encryptionkey, unsigned _activityId, CachedOutputMetaData const & _outputMeta, IOutputRowDeserializer * _rowDeserializer, IEngineRowAllocator *_rowAllocator) 
        : blockcompressed(_blockcompressed), 
          encryptionkey(_encryptionkey), 
          activityId(_activityId), 
          outputMeta(_outputMeta),
          rowDeserializer(_rowDeserializer), 
          rowAllocator(_rowAllocator)
    {
        base = _base;
        top = _base + _size;
    }

    int compare(offset_t offset)
    {
        if (offset < base)
            return -1;
        else if (offset >= top)
            return 1;
        else
            return 0;
    }

    offset_t translateFPos(offset_t rp)
    {
        if(isLocalFpos(rp))
            return getLocalFposOffset(rp);
        else
            return rp-base;
    }

    virtual void openPart()
    {
        // MORE - cached file handles?
        if(rawFile)
            return;
        IDistributedFilePart * part = queryPart();
        unsigned numCopies = part->numCopies();
        for (unsigned copy=0; copy < numCopies; copy++)
        {
            RemoteFilename rfn;
            try
            {
                OwnedIFile ifile = createIFile(part->getFilename(rfn,copy));
                unsigned __int64 thissize = ifile->size();
                if (thissize != -1)
                {
                    IPropertyTree & props = part->queryProperties();
                    unsigned __int64 expectedSize;
                    Owned<IExpander> eexp;
                    if (encryptionkey.length()!=0) {
                        eexp.setown(createAESExpander256(encryptionkey.length(),encryptionkey.get()));
                        blockcompressed = true;
                    }
                    if(blockcompressed)
                        expectedSize = props.getPropInt64("@compressedSize", -1);
                    else
                        expectedSize = props.getPropInt64("@size", -1);
                    if(thissize != expectedSize && expectedSize != -1)
                        throw MakeStringException(0, "File size mismatch: file %s was supposed to be %"I64F"d bytes but appears to be %"I64F"d bytes", ifile->queryFilename(), expectedSize, thissize); 
                    if(blockcompressed)
                        rawFile.setown(createCompressedFileReader(ifile,eexp));
                    else
                        rawFile.setown(ifile->open(IFOread));
                    break;
                }
            }
            catch (IException *E)
            {
                EXCLOG(E, "Opening key part");
                E->Release();
            }
        }
        if(!rawFile)
        {
            RemoteFilename rfn;
            StringBuffer rmtPath;
            part->getFilename(rfn).getRemotePath(rmtPath);
            throw MakeStringException(1001, "Could not open file part at %s%s", rmtPath.str(), (numCopies > 1) ? " or any alternate location." : ".");
        }
        rawStream.setown(createFileSerialStream(rawFile, 0, -1, 0));
    }

    virtual IDistributedFilePart * queryPart() = 0;
};

// this class base for all three fetch activities, but not keyed join
class SimpleFetchPartHandlerBase : public FetchPartHandlerBase, public ThreadedPartHandler<FetchRequest>
{
public:
    SimpleFetchPartHandlerBase(IDistributedFilePart *_part, offset_t _base, offset_t _size, IThreadedExceptionHandler *_handler, IThreadPool * _threadPool, bool _blockcompressed, MemoryAttr &_encryptionkey, unsigned _activityId, CachedOutputMetaData const & _outputMeta, IOutputRowDeserializer * _rowDeserializer, IEngineRowAllocator *_rowAllocator) 
        : FetchPartHandlerBase(_base, _size, _blockcompressed, _encryptionkey, _activityId, _outputMeta, _rowDeserializer, _rowAllocator), 
          ThreadedPartHandler<FetchRequest>(_part, _handler, _threadPool)
    {
    }

    ~SimpleFetchPartHandlerBase()
    {
        while(FetchRequest * fetch = pending.dequeue())
            fetch->Release();
    }

    IMPLEMENT_IINTERFACE;

    virtual IDistributedFilePart * queryPart() { return part; }

private:
    virtual void openPart() { FetchPartHandlerBase::openPart(); }
};

// this class used for flat and CSV fetch activities, but not XML fetch or keyed join
class FlatFetchPartHandler : public SimpleFetchPartHandlerBase
{
public:
    FlatFetchPartHandler(IFlatFetchHandlerCallback & _owner, IDistributedFilePart * _part, offset_t _base, offset_t _size, IThreadedExceptionHandler *_handler, IThreadPool * _threadPool, bool _blockcompressed, MemoryAttr &_encryptionkey, unsigned _activityId, CachedOutputMetaData const & _outputMeta, IOutputRowDeserializer * _rowDeserializer, IEngineRowAllocator *_rowAllocator)
        : SimpleFetchPartHandlerBase(_part, _base, _size, _handler, _threadPool, _blockcompressed, _encryptionkey, _activityId, _outputMeta, _rowDeserializer, _rowAllocator), 
          owner(_owner)
    {
    }

    virtual void doRequest(FetchRequest * _fetch)
    {
        Owned<FetchRequest> fetch(_fetch);
        offset_t pos = translateFPos(fetch->pos);
        if(pos >= rawFile->size())
            throw MakeStringException(0, "Attempted to fetch at invalid filepos");
        owner.processFetch(fetch, pos, rawStream);
    }

private:
    IFlatFetchHandlerCallback & owner;
};

class DistributedFileFetchHandlerBase : public CInterface, implements IInterface, implements IThreadedExceptionHandler
{
public:
    IMPLEMENT_IINTERFACE;
    DistributedFileFetchHandlerBase() {}
    virtual ~DistributedFileFetchHandlerBase() {}

    virtual void noteException(IException *E)
    {
        CriticalBlock procedure(exceptionCrit);
        if (exception)
            E->Release();
        else
            exception = E;
    }

protected:
    static offset_t getPartSize(IDistributedFilePart *part)
    {
        offset_t partsize = part->queryProperties().getPropInt64("@size", -1);
        if (partsize==-1)
        {
            MTIME_SECTION(timer, "Fetch remote file size");
            unsigned numCopies = part->numCopies();
            for (unsigned copy=0; copy < numCopies; copy++)
            {
                RemoteFilename rfn;
                try
                {
                    OwnedIFile ifile = createIFile(part->getFilename(rfn,copy));
                    partsize = ifile->size();
                    if (partsize != -1)
                    {
                        IPropertyTree &tree = part->lockProperties();
                        tree.setPropInt64("@size", partsize);
                        part->unlockProperties();
                        break;
                    }
                }
                catch(IException *E)
                {
                    EXCLOG(E, "Open remote file");
                    E->Release();
                }
            }
        }
        if (partsize==-1)
            throw MakeStringException(0, "Unable to determine size of filepart"); 
        return partsize;
    }

protected:
    CriticalSection exceptionCrit;
    IException * exception;
};

template <class PARTHANDLER>
class IFetchHandlerFactory
{
public:
    virtual PARTHANDLER * createFetchPartHandler(IDistributedFilePart * part, offset_t base, offset_t size, IThreadedExceptionHandler * handler, bool blockcompressed, MemoryAttr &encryptionkey, IOutputRowDeserializer * rowDeserializer, IEngineRowAllocator *rowAllocator) = 0;
};

template <class PARTHANDLER, class LEFTPTR, class REQUEST>
class DistributedFileFetchHandler : public DistributedFileFetchHandlerBase
{
public:
    typedef DistributedFileFetchHandler<PARTHANDLER, LEFTPTR, REQUEST> SELF;

    DistributedFileFetchHandler(IDistributedFile * f, IFetchHandlerFactory<PARTHANDLER> & factory, MemoryAttr &encryptionkey, IOutputRowDeserializer * rowDeserializer, IEngineRowAllocator *rowAllocator) : file(f)
    {
        numParts = f->numParts();
        parts = new PARTHANDLER *[numParts];
        Owned<IFileDescriptor> fdesc = f->getFileDescriptor();
        bool blockcompressed = fdesc->isCompressed(); //assume new compression, old compression was never handled on fetch
        offset_t base = 0;
        unsigned idx;
        for (idx = 0; idx < numParts; idx++)
        {
            IDistributedFilePart *part = f->getPart(idx);
            offset_t size = getPartSize(part);
            parts[idx] = factory.createFetchPartHandler(part, base, size, this, blockcompressed, encryptionkey, rowDeserializer, rowAllocator);
            base += size;
        }
        exception = NULL;
    }

    ~DistributedFileFetchHandler()
    {
        unsigned idx;
        for (idx = 0; idx < numParts; idx++)
        {
            delete parts[idx];
        }
        delete [] parts;
    }

    int compare(offset_t l, PARTHANDLER * r)
    {
        return r->compare(l);
    }

    void addRow(LEFTPTR left, offset_t rp, offset_t seq)
    {
        PARTHANDLER * part = binsearch(rp, parts, numParts, this);
        if(!part)
            throw MakeStringException(1002, "FETCH: file position %"I64F"d out of range", rp);
        part->addRow(new REQUEST(left, rp, seq));
    }

    void stop()
    {
        unsigned idx;
        for (idx = 0; idx < numParts; idx++)
        {
            parts[idx]->stop();
            parts[idx]->join();
        }
        if (exception)
            throw (exception);
    }

private:
    Linked<IDistributedFile> file;
    unsigned numParts;
    PARTHANDLER * * parts;
};

//-------------------------------------------------------------------------------------------------------------

class CHThorThreadedActivityBase : public CHThorActivityBase, implements IThreadedExceptionHandler
{
    class InputHandler : extends Thread
    {
        CHThorThreadedActivityBase *parent;

    public:
        InputHandler(CHThorThreadedActivityBase  *_parent) : parent(_parent)
        {
        }

        virtual int run()
        {
            try
            {
                parent->fetchAll();
            }
            catch (IException *E)
            {
                parent->noteException(E);
            }
            catch (...)
            {
                parent->noteException(MakeStringException(0, "Unknown exception caught in Fetch::InputHandler"));
            }
            return 0;
        }

    };

public:
    CHThorThreadedActivityBase (IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, IHThorFetchContext &_fetch, ThorActivityKind _kind, IRecordSize *diskSize)
        : CHThorActivityBase(_agent, _activityId, _subgraphId, _arg, _kind), fetch(_fetch)
    {
        exception = NULL;
        rowLimit = 0;
    }

    virtual ~CHThorThreadedActivityBase ()
    {
    }

    virtual void waitForThreads()
    {
        aborting = true;
        if (inputThread)
            inputThread->join();
        inputThread.clear();
        threadPool.clear();
    }

    virtual void fetchAll() = 0;

    virtual void ready()        
    { 
        CHThorActivityBase::ready(); 
        started = false;
        stopped = false;
        aborting = false;
        initializeThreadPool();
    }

    virtual void initializeThreadPool() = 0;

    virtual void done()
    {
        aborting = true;
        stop();
        if (inputThread)
            inputThread->join();

        while (!stopped)
        {
            const void * row = getRow();
            if(row)
                releaseHThorRow(row);
        }
        clearQueue();
        waitForThreads();
        avail.reinit(0);
        CHThorActivityBase::done(); 
    }

    virtual const void * getRow() = 0;
    virtual void clearQueue() = 0;

    IHThorInput *queryOutput(unsigned index) { return this; }

    //interface IHThorInput
    virtual bool isGrouped()                { return false; }
    virtual const char *getFileName()       { return NULL; }
    virtual bool outputToFile(const char *) { return false; } 
    virtual IOutputMetaData * queryOutputMeta() const { return CHThorActivityBase::outputMeta; }

protected:
    Semaphore avail;
    bool stopped;
    bool started;
    bool aborting;
    IHThorFetchContext &fetch;
    Owned<InputHandler> inputThread;
    unsigned numParts;
    unsigned __int64 rowLimit;
    Owned<IThreadPool> threadPool;
    CriticalSection pendingCrit;
    IException *exception;

public:
    virtual void noteException(IException *E)
    {
        CriticalBlock procedure(pendingCrit);
        if (exception)
            E->Release();
        else
            exception = E;
        avail.signal();
    }

    void stop()
    {
        avail.signal();
    }

    virtual const void *nextInGroup()
    {
        if (!started)
        {
            started = true;
            start();
        }
        try
        {
            const void *ret = getRow();
            if (ret)
            {
                processed++;
                if ((processed-initialProcessed) > rowLimit)
                {
                    onLimitExceeded();
                    if ( agent.queryCodeContext()->queryDebugContext())
                        agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
                }
            }
            return ret;
        }
        catch(...)
        {
            stopParts();
            throw;
        }
    }

    virtual void initParts(IDistributedFile * f) = 0;
    
    virtual void stopParts() = 0;

    virtual void onLimitExceeded() = 0;

    virtual void start()
    {
        const char *lfn = fetch.getFileName();
        if (lfn)
        {
            Owned <ILocalOrDistributedFile> ldFile = agent.resolveLFN(lfn, "Fetch", 0 != (fetch.getFetchFlags() & FFdatafileoptional));
            IDistributedFile * dFile = ldFile ? ldFile->queryDistributedFile() : NULL;
            if(dFile)
            {
                verifyFetchFormatCrc(dFile);
                agent.logFileAccess(dFile, "HThor", "READ");
                initParts(dFile);
            }
            else
            {
                StringBuffer buff;
                buff.append("Skipping OPT fetch of nonexistent file ").append(lfn);
                WARNLOG("%s", buff.str());
                agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
            }
        }
        inputThread.setown(new InputHandler(this));
        inputThread->start();
    }

protected:
    virtual void verifyFetchFormatCrc(IDistributedFile * f) {} // do nothing here as (currently, and probably by design) not available for CSV and XML, so only implement for binary
};

class CHThorFetchActivityBase : public CHThorThreadedActivityBase, public IFetchHandlerFactory<SimpleFetchPartHandlerBase>
{
public:
    CHThorFetchActivityBase(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorArg &_arg, IHThorFetchContext &_fetch, ThorActivityKind _kind, IRecordSize *diskSize)
      : CHThorThreadedActivityBase (_agent, _activityId, _subgraphId, _arg, _fetch, _kind, diskSize)
    {
        pendingSeq = 0;
        signalSeq = 0;
        dequeuedSeq = 0;
    }

    ~CHThorFetchActivityBase()
    {
        clearQueue();
    }

    virtual void initializeThreadPool()
    {
        threadPool.setown(createThreadPool("hthor fetch activity thread pool", &threadFactory));
    }

    virtual void initParts(IDistributedFile * f)
    {
        size32_t kl;
        void *k;
        fetch.getFileEncryptKey(kl,k);
        MemoryAttr encryptionkey;
        encryptionkey.setOwn(kl,k);
        parts.setown(new DistributedFileFetchHandler<SimpleFetchPartHandlerBase, const void *, FetchRequest>(f, *this, encryptionkey, rowDeserializer, rowAllocator));
    }

    virtual void stopParts()
    {
        if(parts)
            parts->stop();
    }

    virtual void fetchAll()
    {
        if(parts)
        {
            loop
            {
                if (aborting)
                    break;
                const void *row = input->nextInGroup();
                if (!row)
                {
                    row = input->nextInGroup();
                    if (!row)
                        break;
                }
                offset_t rp = fetch.extractPosition(row);
                offset_t seq = addRowPlaceholder();
                parts->addRow(row, rp, seq);
            }
            parts->stop();
        }
        stop();
    }

    // to preserve order, we enqueue NULLs onto the queue and issue sequence numbers, and we only signal avail when rows in correct sequence are available
    // pendingSeq gives the next sequence number to issue; signalSeq gives the next sequence number to signal for; and dequeuedSeq gives the number actually dequeued

    offset_t addRowPlaceholder()
    {
        CriticalBlock procedure(pendingCrit);
        pending.enqueue(NULL);
        return pendingSeq++;
    }

    void setRow(const void *row, offset_t seq)
    {
        CriticalBlock procedure(pendingCrit);
        //GH->?  Why does this append allocated nulls instead of having a queue of const void??
        pending.set((unsigned)(seq-dequeuedSeq), new const void*(row));
        if(seq!=signalSeq)
            return;
        do
        {
            avail.signal();
            ++signalSeq;
        } while((signalSeq < pendingSeq) && (pending.query((unsigned)(signalSeq-dequeuedSeq)) != NULL));
    }

    const void * getRow()
    {
        while(!stopped)
        {
            avail.wait();
            CriticalBlock procedure(pendingCrit);
            if (exception)
            {
                IException *E = exception;
                exception = NULL;
                throw E;
            }
            if(pending.ordinality() == 0)
            {
                stopped = true;
                break;
            }
            const void * * ptr = pending.dequeue();
            ++dequeuedSeq;
            const void * ret = *ptr;
            delete ptr;
            if(ret)
                return ret;
        }
        return NULL;
    }

    virtual void clearQueue()
    {
        while(pending.ordinality())
        {
            const void * * ptr = pending.dequeue();
            if(ptr)
            {
                releaseHThorRow(*ptr);
                delete ptr;
            }
        }
        pendingSeq = 0;
        signalSeq = 0;
        dequeuedSeq = 0;
    }
protected:
    Owned<IOutputRowDeserializer> rowDeserializer;
private:
    PartHandlerThreadFactory<FetchRequest> threadFactory;   
    Owned<DistributedFileFetchHandler<SimpleFetchPartHandlerBase, const void *, FetchRequest> > parts;
    offset_t pendingSeq, signalSeq, dequeuedSeq;
    QueueOf<const void *, true> pending;
};

class CHThorFlatFetchActivity : public CHThorFetchActivityBase, public IFlatFetchHandlerCallback
{
public:
    CHThorFlatFetchActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorFetchArg &_arg, IHThorFetchContext &_fetch, ThorActivityKind _kind, IRecordSize *diskSize, MemoryAttr &encryptionkey)
        : CHThorFetchActivityBase (_agent, _activityId, _subgraphId, _arg, _fetch, _kind, diskSize), helper(_arg)
    {}

    ~CHThorFlatFetchActivity()
    {
        waitForThreads();
    }

    virtual void ready()
    {
        CHThorFetchActivityBase::ready();
        rowLimit = helper.getRowLimit();
        rowDeserializer.setown(helper.queryDiskRecordSize()->createRowDeserializer(agent.queryCodeContext(), activityId));
        diskAllocator.setown(agent.queryCodeContext()->getRowAllocator(helper.queryDiskRecordSize(), activityId));
    }

    virtual bool needsAllocator() const { return true; }

    virtual void processFetch(FetchRequest const * fetch, offset_t pos, ISerialStream *rawStream)
    {
        CThorStreamDeserializerSource deserializeSource;
        deserializeSource.setStream(rawStream);
        deserializeSource.reset(pos);
        RtlDynamicRowBuilder rowBuilder(diskAllocator);
        unsigned sizeRead = rowDeserializer->deserialize(rowBuilder.ensureRow(), deserializeSource);
        OwnedConstRoxieRow rawBuffer(rowBuilder.finalizeRowClear(sizeRead));

        CriticalBlock procedure(transformCrit);
        size32_t thisSize;
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            thisSize = helper.transform(rowBuilder, rawBuffer, fetch->left, fetch->pos);
            if(thisSize)
            {
                setRow(rowBuilder.finalizeRowClear(thisSize), fetch->seq);
            }
            else
            {
                setRow(NULL, fetch->seq);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }

    virtual void onLimitExceeded()
    {
        helper.onLimitExceeded();
    }

    virtual SimpleFetchPartHandlerBase * createFetchPartHandler(IDistributedFilePart * part, offset_t base, offset_t size, IThreadedExceptionHandler * handler, bool blockcompressed, MemoryAttr &encryptionkey, IOutputRowDeserializer * rowDeserializer, IEngineRowAllocator *rowAllocator)
    {
        return new FlatFetchPartHandler(*this, part, base, size, handler, threadPool, blockcompressed, encryptionkey, activityId, outputMeta, rowDeserializer, rowAllocator);
    }

protected:
    virtual void verifyFetchFormatCrc(IDistributedFile * f)
    {
        if(!agent.queryWorkUnit()->getDebugValueBool("skipFileFormatCrcCheck", false))
            ::verifyFormatCrcSuper(helper.getDiskFormatCrc(), f, false, true);
    }

protected:
    CriticalSection transformCrit;
    IHThorFetchArg & helper;
    Owned<IEngineRowAllocator> diskAllocator;
};

extern HTHOR_API IHThorActivity *createFetchActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorFetchArg &arg, ThorActivityKind _kind)
{
    size32_t kl;
    void *k;
    arg.getFileEncryptKey(kl,k);
    MemoryAttr encryptionkey;
    encryptionkey.setOwn(kl,k);
    return new CHThorFlatFetchActivity(_agent, _activityId, _subgraphId, arg, arg, _kind, arg.queryDiskRecordSize(),encryptionkey);
}

//------------------------------------------------------------------------------------------

class CHThorCsvFetchActivity : public CHThorFetchActivityBase, public IFlatFetchHandlerCallback
{
public:
    CHThorCsvFetchActivity (IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCsvFetchArg &_arg, ThorActivityKind _kind)
        : CHThorFetchActivityBase(_agent, _activityId, _subgraphId, _arg, _arg, _kind, NULL), helper(_arg)
    {
        //MORE: I have no idea what should be passed for recordSize in the line above, either something that reads a fixed size, or
        //reads a record based on the csv information
        ICsvParameters * csvInfo = _arg.queryCsvParameters();

        char const * lfn = fetch.getFileName();
        Owned<ILocalOrDistributedFile> ldFile = agent.resolveLFN(lfn, "CsvFetch", 0 != (_arg.getFetchFlags() & FFdatafileoptional));
        IDistributedFile * dFile = ldFile ? ldFile->queryDistributedFile() : NULL;
        const char * quotes = NULL;
        const char * separators = NULL;
        const char * terminators = NULL;
        if (dFile)
        {
            IPropertyTree & options = dFile->queryProperties();
            quotes = options.queryProp("@csvQuote");
            separators = options.queryProp("@csvSeparate");
            terminators = options.queryProp("@csvTerminate");
            agent.logFileAccess(dFile, "HThor", "READ");
        }
        else
        {
            StringBuffer buff;
            buff.append("Skipping OPT fetch of nonexistent file ").append(lfn);
            WARNLOG("%s", buff.str());
            agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
        }
            
        csvSplitter.init(_arg.getMaxColumns(), csvInfo, quotes, separators, terminators);
    }

    ~CHThorCsvFetchActivity()
    {
        waitForThreads();
    }

    virtual bool needsAllocator() const { return true; }

    virtual void processFetch(FetchRequest const * fetch, offset_t pos, ISerialStream *rawStream)
    {
        rawStream->reset(pos);
        CriticalBlock procedure(transformCrit);
        size32_t rowSize = 4096; // MORE - make configurable
        size32_t maxRowSize = 10*1024*1024; // MORE - make configurable
        loop
        {
            size32_t avail;
            const void *peek = rawStream->peek(rowSize, avail);
            if (csvSplitter.splitLine(avail, (const byte *)peek) < rowSize || avail < rowSize)
                break;
            if (rowSize == maxRowSize)
                throw MakeStringException(0, "Row too big");
            if (rowSize >= maxRowSize/2)
                rowSize = maxRowSize;
            else
                rowSize += rowSize;
        }

        size32_t thisSize;
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            thisSize = helper.transform(rowBuilder, csvSplitter.queryLengths(), (const char * *)csvSplitter.queryData(), fetch->left, fetch->pos);
            if(thisSize)
            {
                setRow(rowBuilder.finalizeRowClear(thisSize), fetch->seq);
            }
            else
            {
                setRow(NULL, fetch->seq);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }

    virtual void ready()
    {
        CHThorFetchActivityBase::ready();
        rowLimit = helper.getRowLimit();
        rowDeserializer.setown(helper.queryDiskRecordSize()->createRowDeserializer(agent.queryCodeContext(), activityId));
    }

    virtual void onLimitExceeded()
    {
        helper.onLimitExceeded();
    }

    virtual SimpleFetchPartHandlerBase * createFetchPartHandler(IDistributedFilePart * part, offset_t base, offset_t size, IThreadedExceptionHandler * handler, bool blockcompressed, MemoryAttr &encryptionkey, IOutputRowDeserializer * rowDeserializer, IEngineRowAllocator *rowAllocator)
    {
        return new FlatFetchPartHandler(*this, part, base, size, handler, threadPool, blockcompressed, encryptionkey, activityId, outputMeta, rowDeserializer, rowAllocator);
    }

protected:
    CSVSplitter csvSplitter;    
    CriticalSection transformCrit;
    IHThorCsvFetchArg & helper;
};

extern HTHOR_API IHThorActivity *createCsvFetchActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorCsvFetchArg &arg, ThorActivityKind _kind)
{
    return new CHThorCsvFetchActivity(_agent, _activityId, _subgraphId, arg, _kind);
}

//------------------------------------------------------------------------------------------

class XmlFetchPartHandler : public SimpleFetchPartHandlerBase, public IXMLSelect
{
public:
    IMPLEMENT_IINTERFACE;

    XmlFetchPartHandler(IXmlFetchHandlerCallback & _owner, IDistributedFilePart * _part, offset_t _base, offset_t _size, IThreadedExceptionHandler * _handler, unsigned _streamBufferSize, IThreadPool * _threadPool, bool _blockcompressed, MemoryAttr &_encryptionkey, unsigned _activityId, CachedOutputMetaData const & _outputMeta)
        : SimpleFetchPartHandlerBase(_part, _base, _size, _handler, _threadPool, _blockcompressed, _encryptionkey, _activityId, _outputMeta, NULL, NULL),
          owner(_owner),
          streamBufferSize(_streamBufferSize)
    {
    }

    virtual void doRequest(FetchRequest * _fetch)
    {
        Owned<FetchRequest> fetch(_fetch);
        offset_t pos = translateFPos(fetch->pos);
        rawStream->seek(pos, IFSbegin);
        while(!lastMatch)
        {
            bool gotNext = false;
            try
            {
                gotNext = parser->next();
            }
            catch(IException * e)
            {
                StringBuffer fname;
                RemoteFilename rfn;
                part->getFilename(rfn).getPath(fname);
                throw owner.makeWrappedException(e, fname.str());
            }
            if(!gotNext)
            {
                StringBuffer fname;
                RemoteFilename rfn;
                part->getFilename(rfn).getPath(fname);
                throw MakeStringException(0, "Fetch fpos at EOF of %s", fname.str());
            }
        }
        owner.processFetched(fetch, lastMatch);
        parser->reset();
    }

    virtual void openPart()
    {
        if(parser)
            return;
        FetchPartHandlerBase::openPart();
        rawStream.setown(createBufferedIOStream(rawFile, streamBufferSize));
        parser.setown(createXMLParse(*rawStream, "/", *this));
    }

    //iface IXMLSelect
    void match(IColumnProvider & entry, offset_t startOffset, offset_t endOffset)
    {
        lastMatch.set(&entry);
    }

protected:
    IXmlFetchHandlerCallback & owner;
    Owned<IFileIOStream> rawStream;
    Owned<IXMLParse> parser;
    Owned<IColumnProvider> lastMatch;
    unsigned streamBufferSize;
};

class CHThorXmlFetchActivity : public CHThorFetchActivityBase, public IXmlFetchHandlerCallback
{
public:
    CHThorXmlFetchActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorXmlFetchArg & _arg, ThorActivityKind _kind)
        : CHThorFetchActivityBase(_agent, _activityId, _subgraphId, _arg, _arg, _kind, NULL), helper(_arg)
    {
    }

    ~CHThorXmlFetchActivity()
    {
        waitForThreads();
    }

    virtual bool needsAllocator() const { return true; }

    virtual void processFetched(FetchRequest const * fetch, IColumnProvider * lastMatch)
    {
        CriticalBlock procedure(transformCrit);
        size32_t thisSize;
        try
        {
            RtlDynamicRowBuilder rowBuilder(rowAllocator);
            thisSize = helper.transform(rowBuilder, lastMatch, fetch->left, fetch->pos);

            if(thisSize)
            {
                setRow(rowBuilder.finalizeRowClear(thisSize), fetch->seq);
            }
            else
            {   
                setRow(NULL, fetch->seq);
            }
        }
        catch(IException * e)
        {
            throw makeWrappedException(e);
        }
    }

    IException * makeWrappedException(IException * e) const { return CHThorActivityBase::makeWrappedException(e); }
    virtual IException * makeWrappedException(IException * e, char const * extra) const { return CHThorActivityBase::makeWrappedException(e, extra); }

    virtual void ready()
    {
        CHThorFetchActivityBase::ready();
        rowLimit = helper.getRowLimit();
    }

    virtual void onLimitExceeded()
    {
        helper.onLimitExceeded();
    }

    virtual SimpleFetchPartHandlerBase * createFetchPartHandler(IDistributedFilePart * part, offset_t base, offset_t size, IThreadedExceptionHandler * handler, bool blockcompressed, MemoryAttr &encryptionkey, IOutputRowDeserializer * rowDeserializer, IEngineRowAllocator *rowAllocator)
    {
        return new XmlFetchPartHandler(*this, part, base, size, handler, 4096, threadPool, blockcompressed, encryptionkey, activityId, outputMeta); //MORE: need to put correct stream buffer size here, when Gavin provides it
    }

protected:
    CriticalSection transformCrit;
    IHThorXmlFetchArg & helper;
};

extern HTHOR_API IHThorActivity *createXmlFetchActivity(IAgentContext & _agent, unsigned _activityId, unsigned _subgraphId, IHThorXmlFetchArg & arg, ThorActivityKind _kind)
{
    return new CHThorXmlFetchActivity(_agent, _activityId, _subgraphId, arg, _kind);
}

//------------------------------------------------------------------------------------------

class CJoinGroup;

class MatchSet : public CInterface
{
public:
    MatchSet(CJoinGroup * _jg) : jg(_jg)
    {
    }

    ~MatchSet()
    {
        ForEachItemIn(idx, rows)
            releaseHThorRow(rows.item(idx));
    }

    void addRightMatch(void * right, offset_t fpos);
    offset_t addRightPending();
    void setPendingRightMatch(offset_t seq, void * right, offset_t fpos);
    void incRightMatchCount();

    unsigned count() const { return rows.ordinality(); }
    CJoinGroup * queryJoinGroup() const { return jg; }
    void * queryRow(unsigned idx) const { return rows.item(idx); }
    offset_t queryOffset(unsigned idx) const { return offsets.item(idx); }

private:
    CJoinGroup * jg;
    PointerArray rows;
    Int64Array offsets;
};

interface IJoinProcessor
{
    virtual CJoinGroup *createJoinGroup(const void *row) = 0;
    virtual void readyManager(IKeyManager * manager, const void * row) = 0;
    virtual void doneManager(IKeyManager * manager) = 0;
    virtual bool addMatch(MatchSet * ms, IKeyManager * manager) = 0;
    virtual void onComplete(CJoinGroup * jg) = 0;
    virtual bool leftCanMatch(const void *_left) = 0;
    virtual IRecordLayoutTranslator * getLayoutTranslator(IDistributedFile * f) = 0;
    virtual void verifyIndex(IDistributedFile * f, IKeyIndex * idx, IRecordLayoutTranslator * trans) = 0;
};

class CJoinGroup : public CInterface, implements IInterface
{
public:
    class MatchIterator
    {
    public:
        // Single threaded by now
        void const * queryRow() const { return owner.matchsets.item(ms).queryRow(idx); }
        offset_t queryOffset() const { return owner.matchsets.item(ms).queryOffset(idx); }
        bool start()
        {
            idx = 0;
            for(ms = 0; owner.matchsets.isItem(ms); ++ms)
                if(owner.matchsets.item(ms).count())
                    return true;
            return false;
        }
        bool next()
        {
            if(++idx < owner.matchsets.item(ms).count())
                return true;
            idx = 0;
            while(owner.matchsets.isItem(++ms))
                if(owner.matchsets.item(ms).count())
                    return true;
            return false;
        }

    private:
        friend class CJoinGroup;
        MatchIterator(CJoinGroup const & _owner) : owner(_owner) {}
        CJoinGroup const & owner;
        unsigned ms;
        unsigned idx;
    } matches;
    
    CJoinGroup *prev;  // Doubly-linked list to allow us to keep track of ones that are still in use
    CJoinGroup *next;

    CJoinGroup() : matches(*this)
    {
        // Used for head object only
        left = NULL;
        prev = NULL;
        next = NULL;
        atomic_set(&endMarkersPending,0);
        groupStart = NULL;
        matchcount = 0;
    }

    IMPLEMENT_IINTERFACE;

    CJoinGroup(const void *_left, IJoinProcessor *_join, CJoinGroup *_groupStart) : join(_join), matches(*this)
    {
        candidates = 0;
        left = _left;
        if (_groupStart)
        {
            groupStart = _groupStart;
            atomic_inc(&_groupStart->endMarkersPending);
        }
        else
        {
            groupStart = this;
            atomic_set(&endMarkersPending, 1);
        }
        matchcount = 0;
    }

    ~CJoinGroup()
    {
        releaseHThorRow(left);
    }

    MatchSet * getMatchSet()
    {
        CriticalBlock b(crit);
        MatchSet * ms = new MatchSet(this);
        matchsets.append(*ms);
        return ms;
    }

    inline void notePending()
    {
//      assertex(!complete());
        atomic_inc(&groupStart->endMarkersPending);
    }

    inline bool complete() const
    {
        return atomic_read(&groupStart->endMarkersPending) == 0;
    }

    inline bool inGroup(CJoinGroup *leader) const
    {
        return groupStart==leader;
    }

    inline void noteEnd()
    {
        assertex(!complete());
        if (atomic_dec_and_test(&groupStart->endMarkersPending))
        {
            join->onComplete(groupStart);
        }
    }

    inline unsigned noteCandidate()
    {
        CriticalBlock b(crit);
        return ++candidates;
    }

    inline const void *queryLeft() const
    {
        return left;
    }

    inline unsigned rowsSeen() const
    {
        CriticalBlock b(crit);
        return matchcount;
    }

    inline unsigned candidateCount() const
    {
        CriticalBlock b(crit);
        return candidates;
    }

protected:
    friend class MatchSet;
    friend class MatchIterator;
    const void *left;
    unsigned matchcount;
    CIArrayOf<MatchSet> matchsets;
    atomic_t endMarkersPending;
    IJoinProcessor *join;
    mutable CriticalSection crit;
    CJoinGroup *groupStart;
    unsigned candidates;
};

void MatchSet::addRightMatch(void * right, offset_t fpos)
{
    assertex(!jg->complete());
    CriticalBlock b(jg->crit);
    rows.append(right);
    offsets.append(fpos);
    jg->matchcount++;
}

offset_t MatchSet::addRightPending()
{
    assertex(!jg->complete());
    CriticalBlock b(jg->crit);
    offset_t seq = rows.ordinality();
    rows.append(NULL);
    offsets.append(0);
    return seq;
}

void MatchSet::setPendingRightMatch(offset_t seq, void * right, offset_t fpos)
{
    assertex(!jg->complete());
    CriticalBlock b(jg->crit);
    rows.replace(right, (aindex_t)seq);
    offsets.replace(fpos, (aindex_t)seq);
    jg->matchcount++;
}

void MatchSet::incRightMatchCount()
{
    assertex(!jg->complete());
    CriticalBlock b(jg->crit);
    jg->matchcount++;
}

class JoinGroupPool : public CInterface
{
    CJoinGroup *groupStart;
public:
    CJoinGroup head;
    CriticalSection crit;
    bool preserveGroups;

    JoinGroupPool(bool _preserveGroups)
    {
        head.next = &head;
        head.prev = &head;
        preserveGroups = _preserveGroups;
        groupStart = NULL;
    }

    ~JoinGroupPool()
    {
        CJoinGroup *finger = head.next;
        while (finger != &head)
        {
            CJoinGroup *next = finger->next;
            finger->Release();
            finger = next;
        }
    }

    CJoinGroup *createJoinGroup(const void *row, IJoinProcessor *join)
    {
        CJoinGroup *jg = new CJoinGroup(row, join, groupStart);
        if (preserveGroups && !groupStart)
        {
            jg->notePending(); // Make sure we wait for the group end
            groupStart = jg;
        }
        CriticalBlock c(crit);
        jg->next = &head;
        jg->prev = head.prev;
        head.prev->next = jg;
        head.prev = jg;
        return jg;
    }

    void endGroup()
    {
        if (groupStart)
            groupStart->noteEnd();
        groupStart = NULL;
    }

    void releaseJoinGroup(CJoinGroup *goer)
    {
        CriticalBlock c(crit);
        goer->next->prev = goer->prev;
        goer->prev->next = goer->next;
        goer->Release(); // MORE - could put onto another list to reuse....
    }
};

//=============================================================================================

class DistributedKeyLookupHandler;

class KeyedLookupPartHandler : extends ThreadedPartHandler<MatchSet>, implements IInterface
{
    IJoinProcessor &owner;
    Owned<IKeyManager> manager;
    IAgentContext &agent;
    DistributedKeyLookupHandler * tlk;
    unsigned subno;

public:
    IMPLEMENT_IINTERFACE;

    KeyedLookupPartHandler(IJoinProcessor &_owner, IDistributedFilePart *_part, DistributedKeyLookupHandler * _tlk, unsigned _subno, IThreadPool * _threadPool, IAgentContext &_agent);

    ~KeyedLookupPartHandler() { while(pending.dequeue()) 0; } //do nothing but dequeue as don't own MatchSets

private:
    virtual void doRequest(MatchSet * ms)
    {
        agent.reportProgress(NULL);
        CJoinGroup * jg = ms->queryJoinGroup();
        owner.readyManager(manager, jg->queryLeft());
        while(manager->lookup(true))
        {
            if(owner.addMatch(ms, manager))
                break;
        }
        jg->noteEnd();
        owner.doneManager(manager);
    }

    virtual void openPart();
};

interface IKeyLookupHandler : extends IInterface
{
    virtual void addRow(const void *row) = 0;
    virtual void stop() = 0;
};

class DistributedKeyLookupHandler : public CInterface, implements IThreadedExceptionHandler, implements IKeyLookupHandler
{
    bool opened;
    IArrayOf<IKeyManager> managers;
    Owned<IRecordLayoutTranslator> trans;
    unsigned subStart;
    UnsignedArray keyNumParts;

    IArrayOf<KeyedLookupPartHandler> parts;
    IArrayOf<IDistributedFile> keyFiles;
    IArrayOf<IDistributedFilePart> tlks;
    IJoinProcessor &owner;
    CriticalSection exceptionCrit;
    IException *exception;
    Linked<IDistributedFile> file;
    PartHandlerThreadFactory<MatchSet> threadFactory;
    Owned<IThreadPool> threadPool;
    IntArray subSizes;
    IAgentContext &agent;

    void addFile(IDistributedFile &f)
    {
        if((f.numParts() == 1) || (f.queryProperties().hasProp("@local")))
            throw MakeStringException(0, "Superfile %s contained mixed monolithic/local/noroot and regular distributed keys --- not supported", file->queryLogicalName());
        subSizes.append(parts.length());
        unsigned numParts = f.numParts()-1;
        for (unsigned idx = 0; idx < numParts; idx++)
        {
            IDistributedFilePart *part = f.getPart(idx);
            parts.append(*new KeyedLookupPartHandler(owner, part, this, tlks.ordinality(), threadPool, agent));
        }
        keyFiles.append(OLINK(f));
        tlks.append(*f.getPart(numParts));
        keyNumParts.append(numParts);
    }

public:
    IMPLEMENT_IINTERFACE;

    DistributedKeyLookupHandler(IDistributedFile *f, IJoinProcessor &_owner, IAgentContext &_agent)
        : file(f), owner(_owner), agent(_agent)
    {
        threadPool.setown(createThreadPool("hthor keyed join lookup thread pool", &threadFactory));
        IDistributedSuperFile *super = f->querySuperFile();
        if (super)
        {
            Owned<IDistributedFileIterator> it = super->getSubFileIterator(true);
            ForEach(*it)
                addFile(it->query());
        }
        else
            addFile(*f);

        opened = false;
        exception = NULL;
    }

    ~DistributedKeyLookupHandler()
    {
        threadPool.clear();
    }

    void addRow(const void *row)
    {
        if (owner.leftCanMatch(row))
        {
            if(!opened)
                openTLK();
            CJoinGroup *jg = owner.createJoinGroup(row);
            ForEachItemIn(subno, managers)
            {
                agent.reportProgress(NULL);
                subStart = subSizes.item(subno);
                IKeyManager & manager = managers.item(subno);
                owner.readyManager(&manager, row);
                while(manager.lookup(false))
                {
                    unsigned recptr = (unsigned)manager.queryFpos();
                    if (recptr)
                    {
                        jg->notePending();
                        parts.item(recptr+subStart-1).addRow(jg->getMatchSet());
                    }
                }
                owner.doneManager(&manager);
            }
            jg->noteEnd();
        }
        else
        {
            CJoinGroup *jg = owner.createJoinGroup(row);
            jg->noteEnd();
        }
    }

    void openTLK()
    {
        ForEachItemIn(idx, tlks)
        {
            IDistributedFile & f = keyFiles.item(idx);
            IDistributedFilePart &tlk = tlks.item(idx);
            Owned<IKeyIndex> index = openKeyFile(tlk);
            //Owned<IRecordLayoutTranslator> 
            trans.setown(owner.getLayoutTranslator(&f));
            owner.verifyIndex(&f, index, trans);
            Owned<IKeyManager> manager = createKeyManager(index, index->keySize(), NULL);
            if(trans)
                manager->setLayoutTranslator(trans);
            managers.append(*manager.getLink());
        }
        opened = true;
    }

    void stop()
    {
        ForEachItemIn(idx, parts)
        {
            parts.item(idx).stop();
            parts.item(idx).join();
        }
        if (exception)
            throw exception;
    }

    virtual void noteException(IException *E)
    {
        CriticalBlock procedure(exceptionCrit);
        if (exception)
            E->Release();
        else
            exception = E;
    }

    IRecordLayoutTranslator * queryRecordLayoutTranslator() const { return trans; }
};

KeyedLookupPartHandler::KeyedLookupPartHandler(IJoinProcessor &_owner, IDistributedFilePart *_part, DistributedKeyLookupHandler * _tlk, unsigned _subno, IThreadPool * _threadPool, IAgentContext &_agent)
    : owner(_owner), ThreadedPartHandler<MatchSet>(_part, _tlk, _threadPool), agent(_agent), tlk(_tlk), subno(_subno)
{
}

void KeyedLookupPartHandler::openPart()
{
    if(manager)
        return;
    Owned<IKeyIndex> index = openKeyFile(*part);
    manager.setown(createKeyManager(index, index->keySize(), NULL));
    IRecordLayoutTranslator * trans = tlk->queryRecordLayoutTranslator();
    if(trans)
        manager->setLayoutTranslator(trans);
}

class MonolithicKeyLookupHandler : public CInterface, implements IKeyLookupHandler
{
    IArrayOf<IKeyManager> managers;
    Linked<IDistributedFile> file;
    IDistributedSuperFile * super;
    IArrayOf<IDistributedFile> keyFiles;
    IJoinProcessor &owner;
    IAgentContext &agent;
    bool opened;

public:
    IMPLEMENT_IINTERFACE;


    MonolithicKeyLookupHandler(IDistributedFile *f, IJoinProcessor &_owner, IAgentContext &_agent)
        : file(f), owner(_owner), agent(_agent), opened(false)
    {
        super = f->querySuperFile();
        if (super)
        {
            Owned<IDistributedFileIterator> it = super->getSubFileIterator(true);
            ForEach(*it)
                addFile(it->query());
        }
        else
            addFile(*f);
    }

    void addFile(IDistributedFile &f)
    {
        if((f.numParts() != 1) && (!f.queryProperties().hasProp("@local")))
            throw MakeStringException(0, "Superfile %s contained mixed monolithic/local/noroot and regular distributed keys --- not supported", file->queryLogicalName());
        keyFiles.append(OLINK(f));
    }

    void addRow(const void *row)
    {
        if (owner.leftCanMatch(row))
        {
            if(!opened)
                openKey();
            CJoinGroup *jg = owner.createJoinGroup(row);
            ForEachItemIn(idx, managers)
            {
                agent.reportProgress(NULL);
                IKeyManager & manager = managers.item(idx);
                owner.readyManager(&manager, row);
                while(manager.lookup(true))
                {
                    if(owner.addMatch(jg->getMatchSet(), &manager))
                        break;
                }
                owner.doneManager(&manager);
            }
            jg->noteEnd();
        }
        else
        {
            CJoinGroup *jg = owner.createJoinGroup(row);
            jg->noteEnd();
        }
    }

    void openKey()
    {
        ForEachItemIn(idx, keyFiles)
        {
            IDistributedFile & f = keyFiles.item(idx);
            Owned<IRecordLayoutTranslator> trans = owner.getLayoutTranslator(&f);
            Owned<IKeyManager> manager;
            if(f.numParts() == 1)
            {
                Owned<IKeyIndex> index = openKeyFile(f.queryPart(0));
                owner.verifyIndex(&f, index, trans);
                manager.setown(createKeyManager(index, index->keySize(), NULL));
            }
            else
            {
                unsigned num = f.numParts()-1;
                Owned<IKeyIndexSet> parts = createKeyIndexSet();
                Owned<IKeyIndex> index;
                for(unsigned i=0; i<num; ++i)
                {
                    index.setown(openKeyFile(f.queryPart(i)));
                    parts->addIndex(index.getLink());
                }
                owner.verifyIndex(&f, index, trans);
                manager.setown(createKeyMerger(parts, index->keySize(), 0, NULL));
            }
            if(trans)
                manager->setLayoutTranslator(trans);
            managers.append(*manager.getLink());
        }
        opened = true;
    }

    void stop()
    {
    }
};


//------------------------------------------------------------------------------------------

class KeyedJoinFetchRequest : public CInterface
{
public:
    MatchSet * ms;
    offset_t pos;
    offset_t seq;
    KeyedJoinFetchRequest(MatchSet * _ms, offset_t _pos, offset_t _seq) : ms(_ms), pos(_pos), seq(_seq) {}
};

class IKeyedJoinFetchHandlerCallback
{
public:
    virtual void processFetch(KeyedJoinFetchRequest const * fetch, offset_t pos, ISerialStream *rawStream) = 0;
};

class KeyedJoinFetchPartHandler : public FetchPartHandlerBase, public ThreadedPartHandler<KeyedJoinFetchRequest>
{
public:
    KeyedJoinFetchPartHandler(IKeyedJoinFetchHandlerCallback & _owner, IDistributedFilePart *_part, offset_t _base, offset_t _size, IThreadedExceptionHandler *_handler, IThreadPool * _threadPool, bool _blockcompressed, MemoryAttr &_encryptionkey, unsigned _activityId, CachedOutputMetaData const & _outputMeta, IOutputRowDeserializer * _rowDeserializer, IEngineRowAllocator *_rowAllocator)
        : FetchPartHandlerBase(_base, _size, _blockcompressed, _encryptionkey, _activityId, _outputMeta, _rowDeserializer, _rowAllocator),
          ThreadedPartHandler<KeyedJoinFetchRequest>(_part, _handler, _threadPool),
          owner(_owner)
    {
    }

    virtual ~KeyedJoinFetchPartHandler()
    {
        while(KeyedJoinFetchRequest * fetch = pending.dequeue())
            fetch->Release();
    }

    IMPLEMENT_IINTERFACE;

    virtual IDistributedFilePart * queryPart() { return part; }

private:
    virtual void openPart() 
    { 
        FetchPartHandlerBase::openPart(); 
    }
    
    virtual void doRequest(KeyedJoinFetchRequest * _fetch)
    {
        Owned<KeyedJoinFetchRequest> fetch(_fetch);
        offset_t pos = translateFPos(fetch->pos);
        if(pos >= rawFile->size())
            throw MakeStringException(0, "Attempted to fetch at invalid filepos");
        owner.processFetch(fetch, pos, rawStream);
    }

private:
    IKeyedJoinFetchHandlerCallback & owner;
};

class CHThorKeyedJoinActivity  : public CHThorThreadedActivityBase, implements IJoinProcessor, public IKeyedJoinFetchHandlerCallback, public IFetchHandlerFactory<KeyedJoinFetchPartHandler>
{
    PartHandlerThreadFactory<FetchRequest> threadFactory;   
    Owned<DistributedFileFetchHandler<KeyedJoinFetchPartHandler, MatchSet *, KeyedJoinFetchRequest> > parts;
    IHThorKeyedJoinArg &helper;
    Owned<IKeyLookupHandler> lookup;
    Owned<IEngineRowAllocator> defaultRightAllocator;
    OwnedConstRoxieRow defaultRight;
    bool leftOuter;
    bool exclude;
    bool extractJoinFields;
    bool limitFail;
    bool limitOnFail;
    bool needsDiskRead;
    unsigned atMost;
    unsigned abortLimit;
    unsigned keepLimit;
    bool preserveOrder;
    bool preserveGroups;
    Owned<JoinGroupPool> pool;
    QueueOf<const void, true> pending;
    CriticalSection statsCrit, imatchCrit, fmatchCrit;
    atomic_t prefiltered;
    atomic_t postfiltered;
    atomic_t skips;
    unsigned seeks;
    unsigned scans;
    OwnedRowArray extractedRows;
    Owned <ILocalOrDistributedFile> ldFile;
    IDistributedFile * dFile;
    IDistributedSuperFile * super;
    CachedOutputMetaData eclKeySize;
    Owned<IOutputRowDeserializer> rowDeserializer;
    Owned<IEngineRowAllocator> diskAllocator;

public:
    CHThorKeyedJoinActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorKeyedJoinArg &_arg, ThorActivityKind _kind)
        : CHThorThreadedActivityBase(_agent, _activityId, _subgraphId, _arg, _arg, _kind, _arg.queryDiskRecordSize()), helper(_arg), activityRecordMetaBuff(NULL)
    {
        atomic_set(&prefiltered, 0);
        atomic_set(&postfiltered, 0);
        atomic_set(&skips, 0);
        seeks = 0;
        scans = 0;
        eclKeySize.set(helper.queryIndexRecordSize());
    }

    ~CHThorKeyedJoinActivity()
    {
        clearQueue();
        waitForThreads();
        if (defaultRight)
            releaseHThorRow(defaultRight);
        defaultRight.getClear();
        rtlFree(activityRecordMetaBuff);
    }

    virtual bool needsAllocator() const { return true; }

    virtual void ready()        
    { 
        CHThorThreadedActivityBase::ready(); 

        preserveOrder = ((helper.getJoinFlags() & JFkeepsorted) != 0) || agent.queryWorkUnit()->getDebugValueBool("__hthor_kj_always_preserve_order", DEFAULT_KJ_PRESERVES_ORDER);
        preserveGroups = helper.queryOutputMeta()->isGrouped();
        needsDiskRead = helper.diskAccessRequired();
        extractJoinFields = ((helper.getJoinFlags() & JFextractjoinfields) != 0);
        atMost = helper.getJoinLimit();
        if (atMost == 0) atMost = (unsigned)-1;
        abortLimit = helper.getMatchAbortLimit();
        if (abortLimit == 0) abortLimit = (unsigned)-1;
        leftOuter = ((helper.getJoinFlags() & JFleftouter) != 0);
        exclude = ((helper.getJoinFlags() & JFexclude) != 0);
        keepLimit = helper.getKeepLimit();
        if (keepLimit == 0) keepLimit = (unsigned)-1;
        rowLimit = helper.getRowLimit();
        pool.setown(new JoinGroupPool(preserveGroups));
        limitOnFail = ((helper.getJoinFlags() & JFonfail) != 0);
        limitFail = !limitOnFail && ((helper.getJoinFlags() & JFmatchAbortLimitSkips) == 0);
        if(leftOuter || limitOnFail)
        {
            if (!defaultRight)
            {
                RtlDynamicRowBuilder rowBuilder(queryRightRowAllocator());
                size32_t thisSize = helper.createDefaultRight(rowBuilder);
                defaultRight.setown(rowBuilder.finalizeRowClear(thisSize));
            }
        }
        if (needsDiskRead)
        {
            rowDeserializer.setown(helper.queryDiskRecordSize()->createRowDeserializer(agent.queryCodeContext(), activityId));
            diskAllocator.setown(agent.queryCodeContext()->getRowAllocator(helper.queryDiskRecordSize(), activityId));
        }
    }

    virtual void initializeThreadPool()
    {
        threadPool.setown(createThreadPool("hthor keyed join fetch thread pool", &threadFactory));
    }

    virtual void initParts(IDistributedFile * f)
    {
        size32_t kl;
        void *k;
        fetch.getFileEncryptKey(kl,k);
        MemoryAttr encryptionkey;
        encryptionkey.setOwn(kl,k);
        Owned<IEngineRowAllocator> inputRowAllocator;   
        if (needsDiskRead)
        {
            inputRowAllocator.setown(agent.queryCodeContext()->getRowAllocator(helper.queryDiskRecordSize(), activityId));
            parts.setown(new DistributedFileFetchHandler<KeyedJoinFetchPartHandler, MatchSet *, KeyedJoinFetchRequest>(f, *this, encryptionkey, rowDeserializer, inputRowAllocator));
        }
    }

    virtual void stopParts()
    {
        if(parts)
            parts->stop();
    }

    virtual bool isGrouped() { return preserveGroups; } 

    virtual void waitForThreads()
    {
        aborting = true;
        if (inputThread)
            inputThread->join();
        lookup.clear();
        threadPool.clear();
    }

    virtual void clearQueue()
    {
        while (pending.ordinality())
            releaseHThorRow(pending.dequeue());
    }

    void addRow(const void *row)
    {
        CriticalBlock procedure(pendingCrit);
        pending.enqueue(row);
        avail.signal();
    }

    const void * getRow()
    {
        if (stopped)
            return NULL;
        avail.wait();
        CriticalBlock procedure(pendingCrit);
        if (exception)
        {
            IException *E = exception;
            exception = NULL;
            throw E;
        }
        if (pending.ordinality())
            return pending.dequeue();
        else
        {
            stopped = true;
            return NULL;
        }
    }

    virtual void fetchAll()
    {
        bool eogSeen = false;  // arguably true makes more sense
        loop
        {
            if (aborting)
                break;
            const void *row = input->nextInGroup();
            if (!row)
            {
                if (eogSeen)
                    break;
                else 
                    eogSeen = true;
                pool->endGroup();
            }
            else
            {
                eogSeen = false;
                if(lookup)
                {
                    lookup->addRow(row);
                }
                else
                {
                    CJoinGroup *jg = createJoinGroup(row);
                    jg->noteEnd();
                }
            }
        }
        if(lookup)
            lookup->stop();
        if (parts)
            parts->stop();
        stop();
    }

    virtual KeyedJoinFetchPartHandler * createFetchPartHandler(IDistributedFilePart * part, offset_t base, offset_t size, IThreadedExceptionHandler * handler, bool blockcompressed, MemoryAttr &encryptionkey, IOutputRowDeserializer * rowDeserializer, IEngineRowAllocator *rowAllocator)
    {
        return new KeyedJoinFetchPartHandler(*this, part, base, size, handler, threadPool, blockcompressed, encryptionkey, activityId, outputMeta, rowDeserializer, rowAllocator);
    }

    virtual void processFetch(KeyedJoinFetchRequest const * fetch, offset_t pos, ISerialStream *rawStream)
    {
        CThorStreamDeserializerSource deserializeSource;
        deserializeSource.setStream(rawStream);
        deserializeSource.reset(pos);
        RtlDynamicRowBuilder rowBuilder(diskAllocator);
        unsigned sizeRead = rowDeserializer->deserialize(rowBuilder.ensureRow(), deserializeSource);
        OwnedConstRoxieRow row = rowBuilder.finalizeRowClear(sizeRead);

        if(match(fetch->ms, row))
        {
            if(exclude)
            {
                fetch->ms->incRightMatchCount();
            }
            else
            {
                RtlDynamicRowBuilder extractBuilder(queryRightRowAllocator()); 
                size32_t size = helper.extractJoinFields(extractBuilder, row, fetch->pos, NULL);
                void * ret = (void *) extractBuilder.finalizeRowClear(size);
                fetch->ms->setPendingRightMatch(fetch->seq, ret, fetch->pos);
            }
        }
        fetch->ms->queryJoinGroup()->noteEnd();
    }

    bool match(MatchSet * ms, const void * right)
    {
        CriticalBlock proc(fmatchCrit);
        bool ret = helper.fetchMatch(ms->queryJoinGroup()->queryLeft(), right);
        if (!ret)
            atomic_inc(&postfiltered);
        return ret;
    }

    virtual bool leftCanMatch(const void * _left)
    {
        bool ret = helper.leftCanMatch(_left);
        if (!ret)
            atomic_inc(&prefiltered);
        return ret;
    }

    virtual CJoinGroup *createJoinGroup(const void *row)
    {
        // NOTE - single threaded
        return pool->createJoinGroup(row, this);
    }

    virtual void onComplete(CJoinGroup *jg)
    {
        CriticalBlock c(pool->crit);
        if (preserveOrder)
        {
            CJoinGroup *finger = pool->head.next;
            if(preserveGroups)
            {
                unsigned joinGroupSize = 0;
                Linked<CJoinGroup> firstInGroup = finger;
                while(finger != &pool->head)
                {
                    CJoinGroup *next = finger->next;
                    if(finger->complete())
                        joinGroupSize += doJoinGroup(finger);
                    else
                        break;
                    finger = next;
                    if(!finger->inGroup(firstInGroup))
                    {
                        if(joinGroupSize)
                            addRow(NULL);
                        joinGroupSize = 0;
                        firstInGroup.set(finger);
                    }
                }
                assertex(finger == firstInGroup.get());
            }
            else
            {
                while(finger != &pool->head)
                {
                    CJoinGroup *next = finger->next;
                    if(finger->complete())
                        doJoinGroup(finger);
                    else
                        break;
                    finger = next;
                }
            }
        }
        else if (preserveGroups)
        {
            Linked<CJoinGroup> head = jg;  // Must avoid releasing head until the end, or while loop can overrun if head is reused
            assertex(jg->inGroup(jg));
            CJoinGroup *finger = jg;
            unsigned joinGroupSize = 0;
            while (finger->inGroup(jg))
            {
                CJoinGroup *next = finger->next;
                joinGroupSize += doJoinGroup(finger);
                finger = next;
            }
            if (joinGroupSize)
                addRow(NULL);
        }
        else
            doJoinGroup(jg);
    }

    void failLimit(const void * left)
    {
        helper.onMatchAbortLimitExceeded();
        CommonXmlWriter xmlwrite(0);
        if (input && input->queryOutputMeta() && input->queryOutputMeta()->hasXML())
        {
            input->queryOutputMeta()->toXML((byte *) left, xmlwrite);
        }
        throw MakeStringException(0, "More than %d match candidates in keyed join for row %s", abortLimit, xmlwrite.str());
    }

    unsigned doJoinGroup(CJoinGroup *jg)
    {
        unsigned matched = jg->rowsSeen();
        unsigned added = 0;
        const void *left = jg->queryLeft();
        if (jg->candidateCount() > abortLimit)
        {
            if(limitFail)
                failLimit(left);
            if(limitOnFail)
            {
                Owned<IException> except;
                try
                {
                    failLimit(left);
                }
                catch(IException * e)
                {
                    except.setown(e);
                }
                assertex(except);
                size32_t transformedSize;
                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                try
                {
                    transformedSize = helper.onFailTransform(rowBuilder, left, defaultRight, 0, except);
                }
                catch(IException * e)
                {
                    throw makeWrappedException(e);
                }
                if(transformedSize)
                {
                    const void * shrunk = rowBuilder.finalizeRowClear(transformedSize);
                    addRow(shrunk);
                    added++;
                }
                else
                {
                    atomic_inc(&skips);
                }
            }
            else
                return 0;
        }
        else if(!matched || jg->candidateCount() > atMost)
        {
            if(leftOuter)
            {
                switch(kind)
                {
                case TAKkeyedjoin:
                case TAKkeyeddenormalizegroup:
                    {
                        size32_t transformedSize;
                        try
                        {
                            RtlDynamicRowBuilder rowBuilder(rowAllocator);
                            if (kind == TAKkeyedjoin)
                                transformedSize = helper.transform(rowBuilder, left, defaultRight, 0);
                            else if (kind == TAKkeyeddenormalizegroup)
                                transformedSize = helper.transform(rowBuilder, left, defaultRight, 0, (const void * *)NULL);
                            if (transformedSize)
                            {
                                const void * shrunk = rowBuilder.finalizeRowClear(transformedSize);
                                addRow(shrunk);
                                added++;
                            }
                            else
                            {
                                atomic_inc(&skips);
                            }
                        }
                        catch(IException * e)
                        {
                            throw makeWrappedException(e);
                        }
                        break;
                    }
                case TAKkeyeddenormalize:
                    {
                        LinkRoxieRow(left);     
                        addRow((void *) left ); 
                        added++;
                        break;
                    }
                default:
                    throwUnexpected();
                }
            }
        }
        else if(!exclude)
        {
            switch(kind)
            {
            case TAKkeyedjoin:
                {
                    if(jg->matches.start())
                    {
                        do
                        {
                            try
                            {
                                RtlDynamicRowBuilder rowBuilder(rowAllocator);
                                void const * row = jg->matches.queryRow();
                                if(!row) continue;
                                offset_t fpos = jg->matches.queryOffset();
                                size32_t transformedSize;
                                transformedSize = helper.transform(rowBuilder, left, row, fpos);
                                if (transformedSize)
                                {
                                    const void * shrunk = rowBuilder.finalizeRowClear(transformedSize);
                                    addRow(shrunk);
                                    added++;
                                    if (added==keepLimit)
                                        break;
                                }
                                else
                                {
                                    atomic_inc(&skips);
                                }
                            }
                            catch(IException * e)
                            {
                                throw makeWrappedException(e);
                            }

                        } while(jg->matches.next());
                    }
                    break;
                }
            case TAKkeyeddenormalize:
                {
                    OwnedConstHThorRow newLeft;
                    newLeft.set(left);
                    unsigned rowSize = 0;
                    unsigned count = 0;
                    unsigned rightAdded = 0;
                    if(jg->matches.start())
                    {
                        do
                        {
                            void const * row = jg->matches.queryRow();
                            if(!row) continue;
                            ++count;
                            offset_t fpos = jg->matches.queryOffset();
                            size32_t transformedSize;
                            try
                            {
                                RtlDynamicRowBuilder rowBuilder(rowAllocator);

                                transformedSize = helper.transform(rowBuilder, newLeft, row, fpos, count);
                                if (transformedSize)
                                {
                                    newLeft.setown(rowBuilder.finalizeRowClear(transformedSize));
                                    rowSize = transformedSize;
                                    rightAdded++;
                                    if (rightAdded==keepLimit)
                                        break;
                                }
                                else
                                {
                                    atomic_inc(&skips);
                                }
                            }
                            catch(IException * e)
                            {
                                throw makeWrappedException(e);
                            }

                        } while(jg->matches.next());
                    }
                    if (rowSize)
                    {
                        addRow(newLeft.getClear());
                        releaseHThorRow(newLeft);
                        added++;
                    }
                    break;
                }
            case TAKkeyeddenormalizegroup:
                {
                    extractedRows.clear();
                    unsigned count = 0;
                    if(jg->matches.start())
                        do
                        {
                            const void * row = jg->matches.queryRow();
                            if(!row) continue;
                            if(++count > keepLimit)
                                break;
                            LinkRoxieRow(row);
                            extractedRows.append(row);
                        } while(jg->matches.next());
                    
                    size32_t transformedSize;
                    try
                    {
                        RtlDynamicRowBuilder rowBuilder(rowAllocator);
                        transformedSize = helper.transform(rowBuilder, left, extractedRows.item(0), extractedRows.ordinality(), (const void * *)extractedRows.getArray());
                        extractedRows.clear();
                        if (transformedSize)
                        {
                            const void * shrunk = rowBuilder.finalizeRowClear(transformedSize);
                            addRow(shrunk);
                            added++;
                        }
                        else
                        {
                            atomic_inc(&skips);
                        }
                    }
                    catch(IException * e)
                    {
                        throw makeWrappedException(e);
                    }

                    break;
                }
            default:
                throwUnexpected();
            }
        }
        pool->releaseJoinGroup(jg); // releases link to gotten row
        return added;
    }

    static bool useMonolithic(IDistributedFile & f)
    {
        return ((f.numParts() == 1) || (f.queryProperties().hasProp("@local")));
    }

    virtual void start()
    {
        const char *lfn = helper.getIndexFileName();
        ldFile.setown(agent.resolveLFN(lfn, "KeyedJoin", 0 != (helper.getJoinFlags() & JFindexoptional)));
        dFile = ldFile ? ldFile->queryDistributedFile() : NULL;
        if (dFile)
        {
            Owned<IDistributedFile> odFile;
            odFile.setown(dFile);
            LINK(odFile);
            enterSingletonSuperfiles(odFile);
            bool mono;
            super = dFile->querySuperFile();
            if(super)
            {
                if(super->numSubFiles()==0)
                    throw MakeStringException(0, "Superkey %s empty", super->queryLogicalName());
                mono = useMonolithic(super->querySubFile(0));
            }
            else
            {
                mono = useMonolithic(*dFile);
            }
            if (mono)
                lookup.setown(new MonolithicKeyLookupHandler(dFile, *this, agent));
            else
                lookup.setown(new DistributedKeyLookupHandler(dFile, *this, agent));
            agent.logFileAccess(dFile, "HThor", "READ");
        }
        else
        {
            StringBuffer buff;
            buff.append("Skipping OPT keyed join against nonexistent file ").append(lfn);
            WARNLOG("%s", buff.str());
            agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
        }
        CHThorThreadedActivityBase::start();
    }

    virtual void readyManager(IKeyManager * manager, const void * row)
    {
        helper.createSegmentMonitors(manager, row);
        manager->finishSegmentMonitors();
        manager->reset();
        manager->resetCounts();
    }

    virtual void doneManager(IKeyManager * manager)
    {
        manager->releaseSegmentMonitors();
        CriticalBlock b(statsCrit);
        seeks += manager->querySeeks();
        scans += manager->queryScans();
    }

    virtual bool addMatch(MatchSet * ms, IKeyManager * manager)
    {
        CJoinGroup * jg = ms->queryJoinGroup();
        unsigned candTotal = jg->noteCandidate();
        if (candTotal > atMost || candTotal > abortLimit)
        {
            if ( agent.queryCodeContext()->queryDebugContext())
                agent.queryCodeContext()->queryDebugContext()->checkBreakpoint(DebugStateLimit, NULL, static_cast<IActivityBase *>(this));
            return true;
        }
        KLBlobProviderAdapter adapter(manager);
        offset_t recptr;
        byte const * rhs = manager->queryKeyBuffer(recptr);
        if(indexReadMatch(jg->queryLeft(), rhs, recptr, &adapter))
        {
            if(needsDiskRead)
            {
                jg->notePending();
                offset_t seq = ms->addRightPending();
                parts->addRow(ms, recptr, seq);
            }
            else
            {
                if(exclude)
                    ms->incRightMatchCount();
                else
                {
                    RtlDynamicRowBuilder rowBuilder(queryRightRowAllocator()); 
                    size32_t size = helper.extractJoinFields(rowBuilder, rhs, recptr, &adapter);
                    void * ret = (void *)rowBuilder.finalizeRowClear(size);
                    ms->addRightMatch(ret, recptr);
                }
            }
        }
        else
        {
            atomic_inc(&postfiltered);
        }
        return false;
    }

    bool indexReadMatch(const void * indexRow, const void * inputRow, unsigned __int64 keyedFpos, IBlobProvider * blobs)
    {
        CriticalBlock proc(imatchCrit);
        return helper.indexReadMatch(indexRow, inputRow, keyedFpos, blobs);
    }

    IEngineRowAllocator * queryRightRowAllocator()
    {
        if (!defaultRightAllocator)
            defaultRightAllocator.setown(agent.queryCodeContext()->getRowAllocator(helper.queryJoinFieldsRecordSize(), activityId));
        return defaultRightAllocator;
    }

    virtual void onLimitExceeded()
    {
        helper.onLimitExceeded();
    }

    virtual void updateProgress(IWUGraphProgress &progress) const
    {
        CHThorThreadedActivityBase::updateProgress(progress);
        IPropertyTree &node = progress.updateNode(subgraphId, activityId);
        setProgress(node, "prefiltered", atomic_read(&prefiltered));
        setProgress(node, "postfiltered", atomic_read(&postfiltered));
        setProgress(node, "skips", atomic_read(&skips));
        setProgress(node, "seeks", seeks);
        setProgress(node, "scans", scans);
    }

protected:
    void * activityRecordMetaBuff;
    size32_t activityRecordMetaSize;
    Owned<IDefRecordMeta> activityRecordMeta;

    virtual IRecordLayoutTranslator * getLayoutTranslator(IDistributedFile * f)
    {
        if(agent.queryWorkUnit()->getDebugValueBool("skipFileFormatCrcCheck", false))
        {
            return NULL;
        }

        if(!rltEnabled(agent.queryWorkUnit()))
        {
            verifyFormatCrc(helper.getIndexFormatCrc(), f, super ? super->queryLogicalName() : NULL, true, true);
            return NULL;
        }

        if(verifyFormatCrc(helper.getIndexFormatCrc(), f, super ? super->queryLogicalName() : NULL, true, false))
        {
            return NULL;
        }

        if(!activityRecordMeta)
        {
            if(!helper.getIndexLayout(activityRecordMetaSize, activityRecordMetaBuff))
                throw MakeStringException(0, "Unable to recover from record layout mismatch for index %s: no record layout metadata in activity", f->queryLogicalName());
            MemoryBuffer buff;
            buff.setBuffer(activityRecordMetaSize, activityRecordMetaBuff, false);
            activityRecordMeta.setown(deserializeRecordMeta(buff, true));
        }

        return getRecordLayoutTranslator(activityRecordMeta, activityRecordMetaSize, activityRecordMetaBuff, f, agent.queryRecordLayoutTranslatorCache());
    }

    virtual void verifyIndex(IDistributedFile * f, IKeyIndex * idx, IRecordLayoutTranslator * trans)
    {
        if (eclKeySize.isFixedSize())
        {
            if(trans)
                trans->checkSizes(f->queryLogicalName(), eclKeySize.getFixedSize(), idx->keySize());
            else
                if(idx->keySize() != eclKeySize.getFixedSize())
                    throw MakeStringException(1002, "Key size mismatch on key %s: key file indicates record size should be %u, but ECL declaration was %u", f->queryLogicalName(), idx->keySize(), eclKeySize.getFixedSize());
        }
    }

    virtual void verifyFetchFormatCrc(IDistributedFile * f)
    {
        if(!agent.queryWorkUnit()->getDebugValueBool("skipFileFormatCrcCheck", false))
            ::verifyFormatCrcSuper(helper.getDiskFormatCrc(), f, false, true);
    }

    virtual void warn(char const * msg)
    {
        StringBuffer buff;
        buff.append(msg).append(" for index ").append(dFile->queryLogicalName());
        WARNLOG("%s", buff.str());
        agent.addWuException(buff.str(), 0, ExceptionSeverityWarning, "hthor");
    }

    virtual void fail(char const * msg)
    {
        throw MakeStringException(0, "%s", msg);
    }
};

extern HTHOR_API IHThorActivity *createKeyedJoinActivity(IAgentContext &_agent, unsigned _activityId, unsigned _subgraphId, IHThorKeyedJoinArg &arg, ThorActivityKind _kind)
{
    return new CHThorKeyedJoinActivity(_agent, _activityId, _subgraphId, arg, _kind);
}
