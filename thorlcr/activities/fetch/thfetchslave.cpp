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

#include "platform.h"
#include "limits.h"
#include "slave.ipp"

#include "thorport.hpp"
#include "jio.hpp"
#include "jlzw.hpp"
#include "jsort.hpp"
#include "jdebug.hpp"
#include "jhtree.hpp"
#include "thsortu.hpp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"
#include "thbufdef.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "csvsplitter.hpp"
#include "thorxmlread.hpp"

#include "../activities/fetch/thfetchcommon.hpp"
#include "../hashdistrib/thhashdistribslave.ipp"
#include "thfetchslave.ipp"

#define NUMSLAVEPORTS       2

struct FPosTableEntryIFileIO : public FPosTableEntry
{
    FPosTableEntryIFileIO() { file = NULL; }
    ~FPosTableEntryIFileIO() { ::Release(file); }
    unsigned location;
    IDelayedFile *file;
};

class CFetchStream : public CSimpleInterface, public IRowStream, implements IStopInput, implements IFetchStream
{
    Owned<IRowStream> keyIn;
    IFetchHandler *iFetchHandler;
    bool inputStopped;
    Linked<IExpander> eexp;

    FPosTableEntryIFileIO *fPosMultiPartTable;
    unsigned tableSize;
    unsigned files, offsetCount;
    CriticalSection stopsect;
    CPartDescriptorArray parts;
    FPosTableEntry *offsetTable;

    static int partLookup(const void *_key, const void *e)
    {
        FPosTableEntryIFileIO &entry = *(FPosTableEntryIFileIO *)e;
        offset_t keyFpos = *(offset_t *)_key;
        if (keyFpos < entry.base)
            return -1;
        else if (keyFpos >= entry.top)
            return 1;
        else
            return 0;
    }

protected:
    IHashDistributor *distributor;
    bool abortSoon;
    mptag_t tag;
    Owned<IRowStream> keyOutStream;
    CActivityBase &owner;
    Linked<IRowInterfaces> keyRowIf, fetchRowIf;

    class CFPosHandler : public CSimpleInterface, implements IHash
    {
        IFetchHandler &iFetchHandler;
        unsigned count;
        FPosTableEntry *offsetTable;

        static int slaveLookup(const void *_key, const void *e)
        {
            offset_t key = *(offset_t *)_key;
            FPosTableEntry &entry = *(FPosTableEntry *)e;
            if (key < entry.base)
                return -1;
            else if (key >= entry.top)
                return 1;
            else
                return 0;
        }

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        CFPosHandler(IFetchHandler &_iFetchHandler, unsigned _count, FPosTableEntry *_offsetTable) 
            : iFetchHandler(_iFetchHandler), count(_count), offsetTable(_offsetTable)
        {
        }
        virtual unsigned hash(const void *data)
        {
            if (1 == count)
                return offsetTable[0].index;
            offset_t fpos = iFetchHandler.extractFpos(data);
            if (isLocalFpos(fpos))
                return getLocalFposPart(fpos);
            const void *result = bsearch(&fpos, offsetTable, count, sizeof(FPosTableEntry), slaveLookup);
            if (!result)
                throw MakeThorException(TE_FetchOutOfRange, "FETCH: Offset not found in offset table; fpos=%" I64F "d", fpos);
            return ((FPosTableEntry *)result)->index;
        }
    } *fposHash;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFetchStream(CActivityBase &_owner, IRowInterfaces *_keyRowIf, IRowInterfaces *_fetchRowIf, bool &_abortSoon, CPartDescriptorArray &_parts, unsigned _offsetCount, size32_t offsetMapSz, const void *offsetMap, IFetchHandler *_iFetchHandler, mptag_t _tag, IExpander *_eexp)
        : owner(_owner), keyRowIf(_keyRowIf), fetchRowIf(_fetchRowIf), abortSoon(_abortSoon), iFetchHandler(_iFetchHandler), offsetCount(_offsetCount), tag(_tag), eexp(_eexp)
    {
        distributor = NULL;
        fposHash = NULL;
        inputStopped = false;
        fPosMultiPartTable = NULL;

        ForEachItemIn(f, _parts)
            parts.append(*LINK(&_parts.item(f)));

        assertex(offsetMapSz == sizeof(FPosTableEntry) * offsetCount);
        offsetTable = new FPosTableEntry[offsetCount];
        memcpy(offsetTable, offsetMap, offsetMapSz);
        unsigned c;
        for (c=0; c<offsetCount; c++)
        {
            FPosTableEntry &e = offsetTable[c];
            ActPrintLog(&owner, "Table[%d] : base=%" I64F "d, top=%" I64F "d, slave=%d", c, e.base, e.top, e.index);
        }
        files = parts.ordinality();
        if (files)
        {
            fPosMultiPartTable = new FPosTableEntryIFileIO[files];
            unsigned f;
            FPosTableEntryIFileIO *e;
            for (f=0, e=&fPosMultiPartTable[0]; f<files; f++, e++)
            {
                IPartDescriptor &part = parts.item(f);
                e->base = part.queryProperties().getPropInt64("@offset");
                e->top = e->base + part.queryProperties().getPropInt64("@size");
                e->index = f;

                Owned<IDelayedFile> lfile = queryThor().queryFileCache().lookup(owner, part);
                e->file = lfile.getClear();
            }
        }
    }
    ~CFetchStream()
    {
        if (fPosMultiPartTable)
            delete [] fPosMultiPartTable;
        ::Release(fposHash);
        ::Release(distributor);
        delete [] offsetTable;
    }

    // IFetchStream
    virtual void start(IRowStream *_keyIn)
    {
        fposHash = new CFPosHandler(*iFetchHandler, offsetCount, offsetTable);
        keyIn.set(_keyIn);
        distributor = createHashDistributor(&owner, owner.queryContainer().queryJobChannel().queryJobComm(), tag, false, this, "FetchStream");
        keyOutStream.setown(distributor->connect(keyRowIf, keyIn, fposHash, NULL));
    }
    virtual IRowStream *queryOutput() { return this; }
    virtual IFileIO *queryPartIO(unsigned part) { assertex(part<files); return fPosMultiPartTable[part].file->queryFileIO(); }
    virtual StringBuffer &getPartName(unsigned part, StringBuffer &out) { return getPartFilename(parts.item(part), fPosMultiPartTable[part].location, out, true); }
    virtual void abort()
    {
        if (distributor)
            distributor->abort();
    }

    // IStopInput
    virtual void stopInput()
    {
        CriticalBlock block(stopsect);  // can be called async by distribute
        if (!inputStopped)
        {
            inputStopped = true;
            keyIn->stop();
        }
    }
    virtual void stop()
    {
        if (keyOutStream)
        {
            keyOutStream->stop();
            keyOutStream.clear();
        }
        distributor->disconnect(true);  
        distributor->join();
        stopInput();
    }
    const void *nextRow()
    {
        if (abortSoon)
            return NULL;

        loop
        {
            OwnedConstThorRow keyRec = keyOutStream->nextRow(); // is this right?
            if (!keyRec)
                break;

            offset_t fpos = iFetchHandler->extractFpos(keyRec);
            switch (files)
            {
                case 0:
                    assertex(false);
                case 1:
                {
                    unsigned __int64 localFpos;
                    if (isLocalFpos(fpos))
                        localFpos = getLocalFposOffset(fpos);
                    else
                        localFpos = fpos-fPosMultiPartTable[0].base;
                    RtlDynamicRowBuilder row(fetchRowIf->queryRowAllocator());
                    size32_t sz = iFetchHandler->fetch(row, keyRec, 0, localFpos, fpos);
                    if (sz)
                        return row.finalizeRowClear(sz);
                    break;
                }
                default:
                {
                    // which of multiple parts this slave is dealing with.
                    FPosTableEntryIFileIO *result = (FPosTableEntryIFileIO *)bsearch(&fpos, fPosMultiPartTable, files, sizeof(FPosTableEntryIFileIO), partLookup);
                    unsigned __int64 localFpos;
                    if (isLocalFpos(fpos))
                        localFpos = getLocalFposOffset(fpos);
                    else
                        localFpos = fpos-result->base;
                    RtlDynamicRowBuilder row(fetchRowIf->queryRowAllocator());
                    size32_t sz = iFetchHandler->fetch(row, keyRec, result->index, localFpos, fpos);
                    if (sz)
                        return row.finalizeRowClear(sz);
                    break;
                }
            }

        }
        return NULL;
    }
};


IFetchStream *createFetchStream(CSlaveActivity &owner, IRowInterfaces *keyRowIf, IRowInterfaces *fetchRowIf, bool &abortSoon, CPartDescriptorArray &parts, unsigned offsetCount, size32_t offsetMapSz, const void *offsetMap, IFetchHandler *iFetchHandler, mptag_t tag, IExpander *eexp)
{
    return new CFetchStream(owner, keyRowIf, fetchRowIf, abortSoon, parts, offsetCount, offsetMapSz, offsetMap, iFetchHandler, tag, eexp);
}

class CFetchSlaveBase : public CSlaveActivity, public CThorDataLink, implements IFetchHandler
{
    IRowStream *fetchStreamOut;
    unsigned maxKeyRecSize;
    rowcount_t limit;
    unsigned offsetCount;
    unsigned offsetMapSz;
    MemoryBuffer offsetMapBytes;
    Owned<IExpander> eexp;
    Owned<IEngineRowAllocator> keyRowAllocator;

protected:
    Owned<IRowInterfaces> fetchDiskRowIf;
    IFetchStream *fetchStream;
    IHThorFetchBaseArg *fetchBaseHelper;
    IHThorFetchContext *fetchContext;
    unsigned files;
    CPartDescriptorArray parts;
    IRowStream *keyIn;
    bool indexRowExtractNeeded;
    mptag_t mptag;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFetchSlaveBase(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        fetchStream = NULL;
        keyIn = NULL;
        fetchStreamOut = NULL;
        fetchBaseHelper = (IHThorFetchBaseArg *)queryHelper();
        fetchContext = static_cast<IHThorFetchContext *>(fetchBaseHelper->selectInterface(TAIfetchcontext_1));
        reInit = 0 != (fetchContext->getFetchFlags() & (FFvarfilename|FFdynamicfilename));
    }
    ~CFetchSlaveBase()
    {
        ::Release(keyIn);
        ::Release(fetchStream);
    }

    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        unsigned numParts;
        data.read(numParts);
        offsetCount = 0;
        offsetMapSz = 0;
        if (numParts)
        {
            parts.ensure(numParts);
            deserializePartFileDescriptors(data, parts);
        }
        data.read(offsetCount);
        if (offsetCount)
        {
            data.read(offsetMapSz);
            offsetMapBytes.append(offsetMapSz, data.readDirect(offsetMapSz));
        }
        if (!container.queryLocalOrGrouped())
            mptag = container.queryJobChannel().deserializeMPTag(data);

        indexRowExtractNeeded = fetchBaseHelper->transformNeedsRhs();

        files = parts.ordinality();

        limit = (rowcount_t)fetchBaseHelper->getRowLimit(); // MORE - if no filtering going on could keyspan to get count

        unsigned encryptedKeyLen;
        void *encryptedKey;
        fetchContext->getFileEncryptKey(encryptedKeyLen,encryptedKey);
        if (0 != encryptedKeyLen)
        {
            bool dfsEncrypted = files?parts.item(0).queryOwner().queryProperties().getPropBool("@encrypted"):false;
            if (dfsEncrypted) // otherwise ignore (warning issued by master)
                eexp.setown(createAESExpander256(encryptedKeyLen, encryptedKey));
            memset(encryptedKey, 0, encryptedKeyLen);
            free(encryptedKey);
        }
        fetchDiskRowIf.setown(createRowInterfaces(fetchContext->queryDiskRecordSize(),queryActivityId(),queryCodeContext()));
        if (fetchBaseHelper->extractAllJoinFields())
        {
            IOutputMetaData *keyRowMeta = QUERYINTERFACE(fetchBaseHelper->queryExtractedSize(), IOutputMetaData);
            assertex(keyRowMeta);
            keyRowAllocator.setown(queryJob().getRowAllocator(keyRowMeta, queryActivityId()));
        }
        appendOutputLinked(this);
    }

    virtual void initializeFileParts()
    {
    }

// IThorDataLink impl.
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        class CKeyFieldExtractBase : public CSimpleInterface, implements IRowStream
        {
        protected:
            CFetchSlaveBase *activity;
            IRowStream &in;
            unsigned maxInSize;
            IHThorFetchBaseArg &fetchBaseHelper;
            IHThorFetchContext &fetchContext;

        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CKeyFieldExtractBase(CFetchSlaveBase *_activity, IRowStream &_in, IHThorFetchBaseArg &_fetchBaseHelper, IHThorFetchContext & _fetchContext) : activity(_activity), in(_in), fetchBaseHelper(_fetchBaseHelper), fetchContext(_fetchContext)
            {
            }
            virtual ~CKeyFieldExtractBase() {}

            // virtual const void *nextRow() = 0;  in IRowStream
            virtual void stop() { in.stop(); }
        };

        startInput(inputs.item(0));
        dataLinkStart();

        IThorDataLink *in = inputs.item(0);
        Owned<IRowInterfaces> keyInIf;
        if (indexRowExtractNeeded)
        {
            Linked<IOutputMetaData> keyInMeta;
            class CKeyFieldExtract : public CKeyFieldExtractBase
            {
            public:
                CKeyFieldExtract(CFetchSlaveBase *activity, IRowStream &in, IHThorFetchBaseArg &fetchBaseHelper, IHThorFetchContext & fetchContext)
                    : CKeyFieldExtractBase(activity, in, fetchBaseHelper, fetchContext)
                {
                }
                virtual ~CKeyFieldExtract() {}

                const void *nextRow()
                {
                    OwnedConstThorRow inRow = in.ungroupedNextRow();
                    if (inRow)
                    {
                        RtlDynamicRowBuilder row(activity->keyRowAllocator);
                        size32_t sz = fetchBaseHelper.extractJoinFields(row, inRow);
                        return row.finalizeRowClear(sz);
                    }
                    return NULL;
                }
            };

            if (fetchBaseHelper->extractAllJoinFields())
            {
                keyIn = LINK(in);
                keyInMeta.set(in->queryFromActivity()->queryRowMetaData());
            }
            else
            {
                keyIn = new CKeyFieldExtract(this, *in, *fetchBaseHelper, *fetchContext);
                keyInMeta.set(QUERYINTERFACE(fetchBaseHelper->queryExtractedSize(), IOutputMetaData));
            }
            keyInIf.setown(createRowInterfaces(keyInMeta,queryActivityId(),queryCodeContext()));
        }
        else
        {
            class CKeyFPosExtract : public CKeyFieldExtractBase
            {
                Linked<IRowInterfaces> rowif;
            public:
                CKeyFPosExtract(IRowInterfaces *_rowif, CFetchSlaveBase *activity, IRowStream &in, IHThorFetchBaseArg &fetchBaseHelper, IHThorFetchContext & fetchContext)
                    : CKeyFieldExtractBase(activity, in, fetchBaseHelper, fetchContext), rowif(_rowif)
                {
                }
            
                virtual ~CKeyFPosExtract() {}

                const void *nextRow()
                {
                    OwnedConstThorRow inRow(in.ungroupedNextRow());
                    if (inRow)
                    {
                        OwnedConstThorRow row;
                        unsigned __int64 fpos = fetchContext.extractPosition(inRow.get());
                        row.deserialize(rowif, sizeof(fpos), &fpos);
                        return row.getClear();
                    }
                    return NULL;
                }
            };
            Owned<IOutputMetaData> fmeta = createFixedSizeMetaData(sizeof(offset_t)); // should be provided by Gavin?
            keyInIf.setown(createRowInterfaces(fmeta,queryActivityId(),queryCodeContext()));
            keyIn = new CKeyFPosExtract(keyInIf, this, *in, *fetchBaseHelper, *fetchContext);
        }

        Owned<IRowInterfaces> rowIf = createRowInterfaces(queryRowMetaData(), queryActivityId(), queryCodeContext());
        fetchStream = createFetchStream(*this, keyInIf, rowIf, abortSoon, parts, offsetCount, offsetMapSz, offsetMapBytes.toByteArray(), this, mptag, eexp);
        fetchStreamOut = fetchStream->queryOutput();
        fetchStream->start(keyIn);
        initializeFileParts();
    }
    virtual void stop()
    {
        fetchStreamOut->stop();
        dataLinkStop();
    }
    virtual void abort()
    {
        if (fetchStream)
            fetchStream->abort();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (abortSoon)
            return NULL;

        OwnedConstThorRow row = fetchStreamOut->nextRow();
        if (row)
        {
            // JCSMORE - not used afaik, and not implemented correctly, i.e. not global, should use a global limit act in thor at least.
            if (getDataLinkCount() >= limit)
                onLimitExceeded();
            dataLinkIncrement();
            return row.getClear();
        }
        return NULL;
    }
    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canStall = true;
        info.unknownRowsOutput = true;
    }

// IFetchHandler
    virtual offset_t extractFpos(const void *key)
    {
        if (indexRowExtractNeeded)
            return fetchContext->extractPosition(key);
        else
        {
            offset_t fpos;
            memcpy(&fpos, key, sizeof(fpos));
            return fpos;
        }
    }
    virtual void onLimitExceeded() = 0;
};

class CFetchSlaveActivity : public CFetchSlaveBase
{
public:
    CFetchSlaveActivity(CGraphElementBase *container) : CFetchSlaveBase(container) { }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CFetchSlaveBase::init(data, slaveData);
    }
    virtual size32_t fetch(ARowBuilder & rowBuilder, const void *keyRow, unsigned filePartIndex, unsigned __int64 localFpos, unsigned __int64 fpos)
    {
        Owned<ISerialStream> stream = createFileSerialStream(fetchStream->queryPartIO(filePartIndex), localFpos);
        CThorStreamDeserializerSource ds(stream);
        RtlDynamicRowBuilder fetchedRowBuilder(fetchDiskRowIf->queryRowAllocator());
        size32_t fetchedLen = fetchDiskRowIf->queryRowDeserializer()->deserialize(fetchedRowBuilder, ds);
        OwnedConstThorRow diskFetchRow = fetchedRowBuilder.finalizeRowClear(fetchedLen);
        return ((IHThorFetchArg *)fetchBaseHelper)->transform(rowBuilder, diskFetchRow, keyRow, fpos);
    }
    virtual void onLimitExceeded()
    {
        ((IHThorFetchArg *)fetchBaseHelper)->onLimitExceeded();
    }
};

class CCsvFetchSlaveActivity : public CFetchSlaveBase
{
    CSVSplitter csvSplitter;

public:
    CCsvFetchSlaveActivity(CGraphElementBase *container) : CFetchSlaveBase(container) { }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CFetchSlaveBase::init(data, slaveData);

        IHThorCsvFetchArg *helper = (IHThorCsvFetchArg *)fetchBaseHelper;
        ICsvParameters *csvInfo = helper->queryCsvParameters();
        assertex(!csvInfo->queryEBCDIC());

        Owned<IPropertyTree> lFProps = createPTree(data);

        const char * quotes = lFProps->hasProp("@csvQuote")?lFProps->queryProp("@csvQuote"):NULL;
        const char * separators = lFProps->hasProp("@csvSeparate")?lFProps->queryProp("@csvSeparate"):NULL;
        const char * terminators = lFProps->hasProp("@csvTerminate")?lFProps->queryProp("@csvTerminate"):NULL;      
        const char * escapes = lFProps->hasProp("@csvEscape")?lFProps->queryProp("@csvEscape"):NULL;
        csvSplitter.init(helper->getMaxColumns(), csvInfo, quotes, separators, terminators, escapes);
    }
    virtual size32_t fetch(ARowBuilder & rowBuilder, const void *keyRow, unsigned filePartIndex, unsigned __int64 localFpos, unsigned __int64 fpos)
    {
        Owned<ISerialStream> inputStream = createFileSerialStream(fetchStream->queryPartIO(filePartIndex), localFpos);
        if (inputStream->eos())
            return 0;
        size32_t minRequired = 4096; // MORE - make configurable
        size32_t maxRowSize = 10*1024*1024; // MORE - make configurable
        size32_t thisLineLength;
        loop
        {
            size32_t avail;
            const void *peek = inputStream->peek(minRequired, avail);
            thisLineLength = csvSplitter.splitLine(avail, (const byte *)peek);
            if (thisLineLength < minRequired || avail < minRequired)
                break;
            if (minRequired == maxRowSize)
                throw MakeActivityException(this, 0, "CSV fetch line of length greater than %d bytes.", minRequired);
            if (minRequired >= maxRowSize/2)
                minRequired = maxRowSize;
            else
                minRequired += minRequired;
        }
        return ((IHThorCsvFetchArg *)fetchBaseHelper)->transform(rowBuilder, csvSplitter.queryLengths(), (const char * *)csvSplitter.queryData(), keyRow, localFpos);
    }
    virtual void onLimitExceeded()
    {
        ((IHThorCsvFetchArg *)fetchBaseHelper)->onLimitExceeded();
    }
};

class CXmlFetchSlaveActivity : public CFetchSlaveBase
{
    Owned<IXMLParse> *parsers;
    Owned<IColumnProvider> *lastMatches;
    Owned<IFileIOStream> *streams;
    Owned<IColumnProvider> *lastMatch;

    class CXMLSelect : public CSimpleInterface, implements IXMLSelect
    {
        CXmlFetchSlaveActivity &owner;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        CXMLSelect(CXmlFetchSlaveActivity &_owner) : owner(_owner) { }

        //IXMLSelect impl.
        void match(IColumnProvider & entry, offset_t startOffset, offset_t endOffset)
        {
            owner.lastMatch->set(&entry);
        }
    } *xmlSelect;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CXmlFetchSlaveActivity(CGraphElementBase *container) : CFetchSlaveBase(container)
    {
        parsers = NULL;
        lastMatches = NULL;
        lastMatch = NULL;
        streams = NULL;
        xmlSelect = new CXMLSelect(*this);
    }
    ~CXmlFetchSlaveActivity()
    {
        delete [] lastMatches;
        delete [] parsers;
        delete [] streams;
        ::Release(xmlSelect);
    }
    virtual void initializeFileParts()
    {
        CFetchSlaveBase::initializeFileParts();
        unsigned f;
        streams = new Owned<IFileIOStream>[files];
        parsers = new Owned<IXMLParse>[files];
        lastMatches = new Owned<IColumnProvider>[files];
        for (f=0; f<files; f++)
        {
            streams[f].setown(createBufferedIOStream(fetchStream->queryPartIO(f)));
            // NB: the index is based on path iteration matches, so on lookup the elements start at positioned stream
            // i.e. getXmlIteratorPath not used (or supplied) here.
            parsers[f].setown(createXMLParse(*streams[f], "/", *xmlSelect, ptr_none, ((IHThorXmlFetchArg *)fetchBaseHelper)->requiresContents()));
        }
    }
    virtual size32_t fetch(ARowBuilder & rowBuilder, const void *keyRow, unsigned filePartIndex, unsigned __int64 localFpos, unsigned __int64 fpos)
    {
        streams[filePartIndex]->seek(localFpos, IFSbegin);
        IXMLParse *parser = parsers[filePartIndex].get();
        lastMatch = &lastMatches[filePartIndex];
        while (!lastMatch->get())
        {
            if (!parser->next())
            {
                StringBuffer tmpStr;
                throw MakeActivityException(this, 0, "%s", fetchStream->getPartName(filePartIndex, tmpStr).str());
            }
        }
        size32_t retSz = ((IHThorXmlFetchArg *)fetchBaseHelper)->transform(rowBuilder, lastMatch->get(), keyRow, fpos);
        lastMatch->clear();
        parser->reset();
        return retSz;
    }
    virtual void onLimitExceeded()
    {
        ((IHThorXmlFetchArg *)fetchBaseHelper)->onLimitExceeded();
    }

friend class CXMLSelect;
};

CActivityBase *createFetchSlave(CGraphElementBase *container)
{
    return new CFetchSlaveActivity(container);
}


CActivityBase *createCsvFetchSlave(CGraphElementBase *container)
{
    return new CCsvFetchSlaveActivity(container);
}

CActivityBase *createXmlFetchSlave(CGraphElementBase *container)
{
    return new CXmlFetchSlaveActivity(container);
}
