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

#include <platform.h>

#include <type_traits>
#include <unordered_map>

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jthread.hpp"
#include "jprop.hpp"
#include "jiter.ipp"
#include "jlzw.hpp"
#include "jflz.hpp"

#include "jhtree.hpp"
#include "mpcomm.hpp"

#include "portlist.h"
#include "rmtfile.hpp"
#include "daclient.hpp"
#include "dafdesc.hpp"

#include "slwatchdog.hpp"
#include "thbuf.hpp"
#include "thmem.hpp"
#include "thexception.hpp"

#include "backup.hpp"
#include "slave.hpp"
#include "thormisc.hpp"
#include "thorport.hpp"
#include "thgraphslave.hpp"
#include "slave.ipp"
#include "thcompressutil.hpp"
#include "dalienv.hpp"
#include "eclhelper_dyn.hpp"
#include "rtlcommon.hpp"
#include "../activities/keyedjoin/thkeyedjoincommon.hpp"

//---------------------------------------------------------------------------

//---------------------------------------------------------------------------

#define ISDALICLIENT // JCSMORE plugins *can* access dali - though I think we should probably prohibit somehow.
void enableThorSlaveAsDaliClient()
{
#ifdef ISDALICLIENT
    PROGLOG("Slave activated as a Dali client");
    const char *daliServers = globals->queryProp("@DALISERVERS");
    if (!daliServers)
        throw MakeStringException(0, "No Dali server list specified");
    Owned<IGroup> serverGroup = createIGroup(daliServers, DALI_SERVER_PORT);
    unsigned retry = 0;
    for (;;)
    {
        try
        {
            LOG(MCdebugProgress, thorJob, "calling initClientProcess");
            initClientProcess(serverGroup,DCR_ThorSlave, getFixedPort(TPORT_mp));
            break;
        }
        catch (IJSOCK_Exception *e)
        {
            if ((e->errorCode()!=JSOCKERR_port_in_use))
                throw;
            FLLOG(MCexception(e), thorJob, e,"InitClientProcess");
            if (retry++>10)
                throw;
            e->Release();
            LOG(MCdebugProgress, thorJob, "Retrying");
            Sleep(retry*2000);
        }
    }
    setPasswordsFromSDS();
#endif
}

void disableThorSlaveAsDaliClient()
{
#ifdef ISDALICLIENT
    closeEnvironment();
    closedownClientProcess();   // dali client closedown
    PROGLOG("Slave deactivated as a Dali client");
#endif
}

class CKJService : public CSimpleInterfaceOf<IKJService>, implements IThreaded, implements IExceptionHandler
{
    const unsigned defaultMaxCachedKJManagers = 1000;
    const unsigned defaultKeyLookupMaxProcessThreads = 16;

    unsigned uniqueId = 0;
    CThreadedPersistent threaded;
    mptag_t keyLookupMpTag = TAG_NULL;
    bool aborted = false;
    unsigned numCached = 0;
    CJobBase *currentJob = nullptr;
    unsigned maxCachedKJManagers = defaultMaxCachedKJManagers;
    unsigned keyLookupMaxProcessThreads = defaultKeyLookupMaxProcessThreads;

    class CLookupKey
    {
    protected:
        unsigned hashv;

        void calcHash()
        {
            unsigned h = hashvalue(id, crc);
            hashv = hashc((const unsigned char *)&id, sizeof(unsigned), h);
        }
    public:
        activity_id id;
        StringAttr fname;
        unsigned crc;

        CLookupKey(MemoryBuffer &mb)
        {
            mb.read(id);
            mb.read(fname);
            mb.read(crc);
            calcHash();
        }
        CLookupKey(const CLookupKey &other)
        {
            id = other.id;
            fname.set(other.fname);
            crc = other.crc;
            hashv = other.hashv;
        }
        unsigned queryHash() const { return hashv; }
        const char *queryFilename() const { return fname; }
        bool operator==(CLookupKey const &other) const
        {
            return (id == other.id) && (crc == other.crc) && strsame(fname, other.fname);
        }
        const char *getTracing(StringBuffer &tracing) const
        {
            return tracing.append(fname);
        }
    };
    struct FetchKey
    {
        activity_id id;
        unsigned partNo;
        unsigned slave;
        FetchKey(MemoryBuffer &mb, unsigned _slave) : slave(_slave)
        {
            mb.read(id);
            mb.read(partNo);
        }

        bool operator==(FetchKey const &other) const { return id==other.id && partNo==other.partNo && slave==other.slave; }
        const char *getTracing(StringBuffer &tracing) const
        {
            return tracing.appendf("actId=%u, partNo=%u, slave=%u", id, partNo, slave);
        }
    };
    class CActivityContext : public CInterface
    {
        CKJService &service;
        activity_id id;
        Owned<IHThorKeyedJoinArg> helper;
        Owned<IOutputRowDeserializer> lookupInputDeserializer;
        Owned<IOutputRowSerializer> joinFieldsSerializer;
        Owned<IEngineRowAllocator> lookupInputAllocator, joinFieldsAllocator;

        Owned<IEngineRowAllocator> fetchInputAllocator;
        Owned<IEngineRowAllocator> fetchOutputAllocator;
        Owned<IOutputRowDeserializer> fetchInputDeserializer;
        Owned<IOutputRowSerializer> fetchOutputSerializer;

        Owned<IEngineRowAllocator> fetchDiskAllocator;
        Owned<IOutputRowDeserializer> fetchDiskDeserializer;

        ICodeContext *codeCtx;

        CriticalSection crit;
        StringArray fetchFilenames;
        IPointerArrayOf<IFileIO> openFetchFiles;
        size32_t fetchInMinSz = 0;
        bool encrypted = false;
        bool compressed = false;
        bool messageCompression = false;
    public:
        CActivityContext(CKJService &_service, activity_id _id, IHThorKeyedJoinArg *_helper, ICodeContext *_codeCtx)
            : service(_service), id(_id), helper(_helper), codeCtx(_codeCtx)
        {
            Owned<IOutputMetaData> lookupInputMeta = new CPrefixedOutputMeta(sizeof(KeyLookupHeader), helper->queryIndexReadInputRecordSize());
            lookupInputDeserializer.setown(lookupInputMeta->createDiskDeserializer(codeCtx, id));
            lookupInputAllocator.setown(codeCtx->getRowAllocatorEx(lookupInputMeta,id, (roxiemem::RoxieHeapFlags)roxiemem::RHFpacked|roxiemem::RHFunique));
            joinFieldsAllocator.setown(codeCtx->getRowAllocatorEx(helper->queryJoinFieldsRecordSize(), id, roxiemem::RHFnone));
            joinFieldsSerializer.setown(helper->queryJoinFieldsRecordSize()->createDiskSerializer(codeCtx, id));

            if (helper->diskAccessRequired())
            {
                Owned<IOutputMetaData> fetchInputMeta = new CPrefixedOutputMeta(sizeof(FetchRequestHeader), helper->queryFetchInputRecordSize());
                fetchInputAllocator.setown(codeCtx->getRowAllocatorEx(fetchInputMeta, id, (roxiemem::RoxieHeapFlags)roxiemem::RHFpacked|roxiemem::RHFunique));
                fetchInputDeserializer.setown(fetchInputMeta->createDiskDeserializer(codeCtx, id));

                Owned<IOutputMetaData> fetchOutputMeta = createOutputMetaDataWithChildRow(joinFieldsAllocator, sizeof(FetchReplyHeader));
                fetchOutputAllocator.setown(codeCtx->getRowAllocatorEx(fetchOutputMeta, id, (roxiemem::RoxieHeapFlags)roxiemem::RHFpacked|roxiemem::RHFunique));
                fetchOutputSerializer.setown(fetchOutputMeta->createDiskSerializer(codeCtx, id));

                fetchDiskAllocator.setown(codeCtx->getRowAllocatorEx(helper->queryDiskRecordSize(), id, (roxiemem::RoxieHeapFlags)roxiemem::RHFpacked|roxiemem::RHFunique));
                fetchDiskDeserializer.setown(helper->queryDiskRecordSize()->createDiskDeserializer(codeCtx, id));
                fetchInMinSz = helper->queryFetchInputRecordSize()->getMinRecordSize();
            }
        }
        ~CActivityContext()
        {
            // should already be removed by last Key or Fetch context
            service.removeActivityContext(this);
        }
        activity_id queryId() const { return id; }
        const void *queryFindParam() const { return &id; } // for SimpleHashTableOf

        IEngineRowAllocator *queryLookupInputAllocator() const { return lookupInputAllocator; }
        IOutputRowDeserializer *queryLookupInputDeserializer() const { return lookupInputDeserializer; }
        IEngineRowAllocator *queryJoinFieldsAllocator() const { return joinFieldsAllocator; }
        IOutputRowSerializer *queryJoinFieldsSerializer() const { return joinFieldsSerializer; }

        IEngineRowAllocator *queryFetchInputAllocator() const { return fetchInputAllocator; }
        IOutputRowDeserializer *queryFetchInputDeserializer() const { return fetchInputDeserializer; }
        IEngineRowAllocator *queryFetchOutputAllocator() const { return fetchOutputAllocator; }
        IOutputRowSerializer *queryFetchOutputSerializer() const { return fetchOutputSerializer; }

        IEngineRowAllocator *queryFetchDiskAllocator() const { return fetchDiskAllocator; }
        IOutputRowDeserializer *queryFetchDiskDeserializer() const { return fetchDiskDeserializer; }

        inline IHThorKeyedJoinArg *queryHelper() const { return helper; }

        void addFetchFile(byte _flags, unsigned _partNo, const char *_fname)
        {
            CriticalBlock b(crit);
            if (_partNo<fetchFilenames.ordinality() && !isEmptyString(fetchFilenames.item(_partNo)))
                return;
            while (_partNo>=fetchFilenames.ordinality())
                fetchFilenames.append("");
            fetchFilenames.replace(_fname, _partNo);
            compressed = _flags & kjf_compressed;
            encrypted = _flags & kjf_encrypted;
        }
        void setMessageCompression(bool _messageCompression) { messageCompression = _messageCompression; }
        inline bool useMessageCompression() const { return messageCompression; }
        IFileIO *getFetchFileIO(unsigned part)
        {
            CriticalBlock b(crit);
            if (part>=openFetchFiles.ordinality())
            {
                do
                {
                    openFetchFiles.append(nullptr);
                }
                while (part>=openFetchFiles.ordinality());
            }
            else
            {
                IFileIO *fileIO = openFetchFiles.item(part);
                if (fileIO)
                    return LINK(fileIO);
            }
            const char *fname = fetchFilenames.item(part);
            Owned<IFile> iFile = createIFile(fname);

            unsigned encryptedKeyLen;
            void *encryptedKey;
            helper->getFileEncryptKey(encryptedKeyLen,encryptedKey);
            Owned<IExpander> eexp;
            if (0 != encryptedKeyLen)
            {
                if (encrypted)
                    eexp.setown(createAESExpander256(encryptedKeyLen, encryptedKey));
                memset(encryptedKey, 0, encryptedKeyLen);
                free(encryptedKey);
            }
            IFileIO *fileIO;
            if (nullptr != eexp.get())
                fileIO = createCompressedFileReader(iFile, eexp);
            else if (compressed)
                fileIO = createCompressedFileReader(iFile);
            else
                fileIO = iFile->open(IFOread);
            if (!fileIO)
                throw MakeStringException(0, "Failed to open fetch file part %u: %s", part, fname);
            openFetchFiles.replace(fileIO, part);
            return LINK(fileIO);
        }
        size32_t queryFetchInMinSize() const { return fetchInMinSz; }
    };
    class CContext : public CInterface
    {
    protected:
        CKJService &service;
        Linked<CActivityContext> activityCtx;
        RecordTranslationMode translationMode = RecordTranslationMode::None;
        Owned<IOutputMetaData> publishedFormat, projectedFormat, expectedFormat;
        unsigned publishedFormatCrc = 0, expectedFormatCrc = 0;
        Owned<const IDynamicTransform> translator;
        Owned<ISourceRowPrefetcher> prefetcher;
    public:
        CContext(CKJService &_service, CActivityContext *_activityCtx) : service(_service), activityCtx(_activityCtx)
        {
        }
        virtual void beforeDispose() override
        {
            service.freeActivityContext(activityCtx.getClear());
        }
        CActivityContext *queryActivityCtx() const { return activityCtx; }
        void setTranslation(RecordTranslationMode _translationMode, IOutputMetaData *_publishedFormat, unsigned _publishedFormatCrc, IOutputMetaData *_projectedFormat)
        {
            dbgassertex(expectedFormatCrc); // translation mode wouldn't have been set unless available
            translationMode = _translationMode;
            publishedFormat.set(_publishedFormat);
            publishedFormatCrc = _publishedFormatCrc;
            projectedFormat.set(_projectedFormat);
        }
        const IDynamicTransform *queryTranslator(const char *tracing)
        {
            if (RecordTranslationMode::None == translationMode)
            {
                //Check if the file requires translation, but translation is disabled
                if (publishedFormatCrc && expectedFormatCrc && (publishedFormatCrc != expectedFormatCrc))
                    throwTranslationError(publishedFormat->queryRecordAccessor(true), expectedFormat->queryRecordAccessor(true), tracing);
                return nullptr;
            }
            else if (!translator)
            {
                if (RecordTranslationMode::AlwaysDisk == translationMode)
                    translator.setown(createRecordTranslator(projectedFormat->queryRecordAccessor(true), publishedFormat->queryRecordAccessor(true)));
                else if (RecordTranslationMode::AlwaysECL == translationMode)
                    translator.setown(createRecordTranslator(projectedFormat->queryRecordAccessor(true), expectedFormat->queryRecordAccessor(true)));
                else if (publishedFormatCrc && publishedFormatCrc != expectedFormatCrc)
                {
                    if (!projectedFormat)
                        throw MakeStringException(0, "Record layout mismatch for: %s", tracing);
                    translator.setown(createRecordTranslator(projectedFormat->queryRecordAccessor(true), publishedFormat->queryRecordAccessor(true)));
                    if (!translator->canTranslate())
                        throw MakeStringException(0, "Untranslatable record layout mismatch detected for: %s", tracing);
                }
                dbgassertex(translator->canTranslate());
            }
            return translator;
        }
        ISourceRowPrefetcher *queryPrefetcher()
        {
            if (!prefetcher)
            {
                if (translator)
                    prefetcher.setown(publishedFormat->createDiskPrefetcher());
                else
                    prefetcher.setown(expectedFormat->createDiskPrefetcher());
            }
            return prefetcher;
        }
        RecordTranslationMode queryTranslationMode() const { return translationMode; }
        IOutputMetaData *queryPublishedMeta() const { return publishedFormat; }
        unsigned queryPublishedMetaCrc() const { return publishedFormatCrc; }
        IOutputMetaData *queryProjectedMeta() const { return projectedFormat; }
    };
    class CKeyLookupContext : public CContext
    {
        CLookupKey key;
        Owned<IKeyIndex> keyIndex;
    public:
        CKeyLookupContext(CKJService &_service, CActivityContext *_activityCtx, const CLookupKey &_key)
            : CContext(_service, _activityCtx), key(_key)
        {
            keyIndex.setown(createKeyIndex(key.fname, key.crc, false, false));
            expectedFormat.set(activityCtx->queryHelper()->queryIndexRecordSize());
            expectedFormatCrc = activityCtx->queryHelper()->getIndexFormatCrc();
        }
        unsigned queryHash() const { return key.queryHash(); }
        const CLookupKey &queryKey() const { return key; }

        inline const char *queryFileName() const { return key.fname; }
        IEngineRowAllocator *queryLookupInputAllocator() const { return activityCtx->queryLookupInputAllocator(); }
        IOutputRowDeserializer *queryLookupInputDeserializer() const { return activityCtx->queryLookupInputDeserializer(); }
        IEngineRowAllocator *queryJoinFieldsAllocator() const { return activityCtx->queryJoinFieldsAllocator(); }
        IOutputRowSerializer *queryJoinFieldsSerializer() const { return activityCtx->queryJoinFieldsSerializer(); }

        IEngineRowAllocator *queryFetchInputAllocator() const { return activityCtx->queryFetchInputAllocator(); }
        IOutputRowDeserializer *queryFetchInputDeserializer() const { return activityCtx->queryFetchInputDeserializer(); }
        IEngineRowAllocator *queryFetchOutputAllocator() const { return activityCtx->queryFetchOutputAllocator(); }
        IOutputRowSerializer *queryFetchOutputSerializer() const { return activityCtx->queryFetchOutputSerializer(); }

        IEngineRowAllocator *queryFetchDiskAllocator() const { return activityCtx->queryFetchDiskAllocator(); }
        IOutputRowDeserializer *queryFetchDiskDeserializer() const { return activityCtx->queryFetchDiskDeserializer(); }

        IKeyManager *createKeyManager()
        {
            return createLocalKeyManager(queryHelper()->queryIndexRecordSize()->queryRecordAccessor(true), keyIndex, nullptr, queryHelper()->hasNewSegmentMonitors());
        }
        inline IHThorKeyedJoinArg *queryHelper() const { return activityCtx->queryHelper(); }
    };
    class CFetchContext : public CContext
    {
        FetchKey key;
        unsigned handle = 0;
        Owned<const IDynamicTransform> translator;
        Owned<ISourceRowPrefetcher> prefetcher;
        Owned<ISerialStream> ioStream;
        CThorContiguousRowBuffer prefetchSource;
        bool initialized = false;

    public:
        CFetchContext(CKJService &_service, CActivityContext *_activityCtx, const FetchKey &_key) : CContext(_service, _activityCtx), key(_key)
        {
            handle = service.getUniqId();
            expectedFormat.set(activityCtx->queryHelper()->queryDiskRecordSize());
            expectedFormatCrc = activityCtx->queryHelper()->getDiskFormatCrc();
        }
        const void *queryFindParam() const { return &key; } // for SimpleHashTableOf
        unsigned queryHandle() const { return handle; }
        const FetchKey &queryKey() const { return key; }
        CThorContiguousRowBuffer &queryPrefetchSource()
        {
            if (!initialized)
            {
                initialized = true;
                Owned<IFileIO> iFileIO = activityCtx->getFetchFileIO(key.partNo);
                ioStream.setown(createFileSerialStream(iFileIO, 0, (offset_t)-1, 0));
                prefetchSource.setStream(ioStream);
            }
            return prefetchSource;
        }
    };
    class CKMContainer : public CInterface
    {
        CKJService &service;
        Linked<CKeyLookupContext> ctx;
        Owned<IKeyManager> keyManager;
        unsigned handle = 0;
    public:
        CKMContainer(CKJService &_service, CKeyLookupContext *_ctx)
            : service(_service), ctx(_ctx)
        {
            keyManager.setown(ctx->createKeyManager());
            StringBuffer tracing;
            const IDynamicTransform *translator = ctx->queryTranslator(ctx->queryKey().getTracing(tracing));
            if (translator)
                keyManager->setLayoutTranslator(translator);
            handle = service.getUniqId();
        }
        ~CKMContainer()
        {
            service.freeLookupContext(ctx.getClear());
        }
        CKeyLookupContext &queryCtx() const { return *ctx; }
        IKeyManager *queryKeyManager() const { return keyManager; }
        unsigned queryHandle() const { return handle; }
        const void *queryFindParam() const { return &handle; } // for SimpleHashTableOf
    };
    class CKMKeyEntry : public CInterface
    {
        CLookupKey key;
        CIArrayOf<CKMContainer> kmcs;
    public:
        CKMKeyEntry(const CLookupKey &_key) : key(_key)
        {
        }
        inline CKMContainer *pop()
        {
            return &kmcs.popGet();
        }
        inline void push(CKMContainer *kmc)
        {
            kmcs.append(*kmc);
        }
        inline unsigned count() { return kmcs.ordinality(); }
        bool remove(CKMContainer *kmc)
        {
            return kmcs.zap(*kmc);
        }
        unsigned queryHash() const { return key.queryHash(); }
        const CLookupKey &queryKey() const { return key; }
    };
    class CLookupRequest : public CSimpleInterface
    {
    protected:
        Linked<CActivityContext> activityCtx;
        IHThorKeyedJoinArg *helper;
        std::vector<const void *> rows;
        rank_t sender;
        mptag_t replyTag;
        bool replyAttempt = false;
        IEngineRowAllocator *allocator = nullptr;
        IOutputRowDeserializer *deserializer = nullptr;
    public:
        CLookupRequest(CActivityContext *_activityCtx, rank_t _sender, mptag_t _replyTag)
            : activityCtx(_activityCtx), sender(_sender), replyTag(_replyTag)
        {
            helper = activityCtx->queryHelper();
        }
        ~CLookupRequest()
        {
            for (auto &r : rows)
                ReleaseThorRow(r);
        }
        inline void addRow(const void *row)
        {
            rows.push_back(row);
        }
        inline const void *getRowClear(unsigned r)
        {
            const void *row = rows[r];
            rows[r] = nullptr;
            return row;
        }
        inline unsigned getRowCount() const { return rows.size(); }
        inline CActivityContext &queryCtx() const { return *activityCtx; }
        void deserialize(size32_t sz, const void *_requestData)
        {
            MemoryBuffer requestData;
            if (activityCtx->useMessageCompression())
                fastLZDecompressToBuffer(requestData, _requestData);
            else
                requestData.setBuffer(sz, (void *)_requestData, false);
            unsigned count;
            requestData.read(count);
            size32_t rowDataSz = requestData.remaining();
            CThorStreamDeserializerSource d(rowDataSz, requestData.readDirect(rowDataSz));
            for (unsigned r=0; r<count; r++)
            {
                assertex(!d.eos());
                RtlDynamicRowBuilder rowBuilder(allocator);
                size32_t sz = deserializer->deserialize(rowBuilder, d);
                addRow(rowBuilder.finalizeRowClear(sz));
            }
        }
        void reply(CMessageBuffer &msg)
        {
            replyAttempt = true;
            if (!queryNodeComm().send(msg, sender, replyTag, LONGTIMEOUT))
                throw MakeStringException(0, "Failed to reply to lookup request");
        }
        void replyError(IException *e)
        {
            EXCLOG(e, "CLookupRequest");
            if (replyAttempt)
                return;
            byte errorCode = 1;
            CMessageBuffer msg;
            msg.append(errorCode);
            serializeException(e, msg);
            if (!queryNodeComm().send(msg, sender, replyTag, LONGTIMEOUT))
                throw MakeStringException(0, "Failed to reply to lookup request");
        }
        virtual void process(bool &abortSoon) = 0;
    };
    class CLookupResult
    {
    protected:
        CActivityContext &activityCtx;
        std::vector<const void *> rows;
        IOutputRowSerializer *serializer = nullptr;

        void clearRows()
        {
            for (auto &r : rows)
                ReleaseThorRow(r);
            rows.clear();
        }
    public:
        CLookupResult(CActivityContext &_activityCtx) : activityCtx(_activityCtx)
        {
        }
        ~CLookupResult()
        {
            clearRows();
        }
        void serializeRows(MemoryBuffer &mb) const
        {
            if (rows.size()) // will be 0 if fetch needed
            {
                DelayedSizeMarker sizeMark(mb);
                CMemoryRowSerializer s(mb);
                for (auto &row : rows)
                    serializer->serialize(s, (const byte *)row);
                sizeMark.write();
            }
        }
    };
    class CKeyLookupResult : public CLookupResult
    {
        typedef CLookupResult PARENT;

        std::vector<unsigned __int64> fposs;
        GroupFlags groupFlag = gf_null;
    public:
        CKeyLookupResult(CActivityContext &_activityCtx) : PARENT(_activityCtx)
        {
            serializer = activityCtx.queryJoinFieldsSerializer();
        }
        void addRow(const void *row, offset_t fpos)
        {
            if (row)
                rows.push_back(row);
            fposs.push_back(fpos);
        }
        void clear()
        {
            groupFlag = gf_null;
            clearRows();
            fposs.clear();
        }
        inline unsigned getCount() const { return fposs.size(); }
        inline GroupFlags queryFlag() const { return groupFlag; }
        void setFlag(GroupFlags gf)
        {
            clear();
            groupFlag = gf;
        }
        void serialize(MemoryBuffer &mb) const
        {
            mb.append(groupFlag);
            if (gf_null != groupFlag)
                return;
            unsigned candidates = fposs.size();
            mb.append(candidates);
            if (candidates)
            {
                serializeRows(mb);
                // JCSMORE - even in half-keyed join case, fpos' may be used by transform (would be good to have tip from codegen to say if used or not)
                mb.append(candidates * sizeof(unsigned __int64), &fposs[0]);
            }
        }
    };
    class CKeyLookupRequest : public CLookupRequest
    {
        CKJService &service;
        Linked<CKMContainer> kmc;

        rowcount_t abortLimit = 0;
        rowcount_t atMost = 0;
        bool fetchRequired = false;
        IEngineRowAllocator *joinFieldsAllocator = nullptr;

        template <class HeaderStruct>
        void getHeaderFromRow(const void *row, HeaderStruct &header)
        {
            memcpy(&header, row, sizeof(HeaderStruct));
        }
        void processRow(const void *row, IKeyManager *keyManager, CKeyLookupResult &reply)
        {
            KeyLookupHeader lookupKeyHeader;
            getHeaderFromRow(row, lookupKeyHeader);
            const void *keyedFieldsRow = (byte *)row + sizeof(KeyLookupHeader);

            helper->createSegmentMonitors(keyManager, keyedFieldsRow);
            keyManager->finishSegmentMonitors();
            keyManager->reset();

            unsigned candidates = 0;
            // NB: keepLimit is not on hard matches and can only be applied later, since other filtering (e.g. in transform) may keep below keepLimit
            while (keyManager->lookup(true))
            {
                ++candidates;
                if (candidates > abortLimit)
                {
                    reply.setFlag(gf_limitabort);
                    break;
                }
                else if (candidates > atMost) // atMost - filter out group if > max hard matches
                {
                    reply.setFlag(gf_limitatmost);
                    break;
                }
                KLBlobProviderAdapter adapter(keyManager);
                byte const * keyRow = keyManager->queryKeyBuffer();
                size_t fposOffset = keyManager->queryRowSize() - sizeof(offset_t);
                offset_t fpos = rtlReadBigUInt8(keyRow + fposOffset);
                if (helper->indexReadMatch(keyedFieldsRow, keyRow,  &adapter))
                {
                    if (fetchRequired)
                        reply.addRow(nullptr, fpos);
                    else
                    {
                        RtlDynamicRowBuilder joinFieldsRowBuilder(joinFieldsAllocator);
                        size32_t sz = helper->extractJoinFields(joinFieldsRowBuilder, keyRow, &adapter);
                        /* NB: Each row lookup could in theory == lots of keyed results. If needed to break into smaller replies
                         * Would have to create/keep a keyManager per sender, in those circumstances.
                         * As it stands, each lookup will be processed and all rows (below limits) will be returned, but I think that's okay.
                         * There are other reasons why might want a keyManager per sender, e.g. for concurrency.
                         */
                        reply.addRow(joinFieldsRowBuilder.finalizeRowClear(sz), fpos);
                    }
                }
            }
            keyManager->releaseSegmentMonitors();
        }
        const unsigned DEFAULT_KEYLOOKUP_MAXREPLYSZ = 0x100000;
    public:
        CKeyLookupRequest(CKJService &_service, CKeyLookupContext *_ctx, CKMContainer *_kmc, rank_t _sender, mptag_t _replyTag)
            : CLookupRequest(_ctx->queryActivityCtx(), _sender, _replyTag), kmc(_kmc), service(_service)
        {
            allocator = activityCtx->queryLookupInputAllocator();
            deserializer = activityCtx->queryLookupInputDeserializer();
            joinFieldsAllocator = activityCtx->queryJoinFieldsAllocator();

            atMost = helper->getJoinLimit();
            if (atMost == 0)
                atMost = (unsigned)-1;
            abortLimit = helper->getMatchAbortLimit();
            if (abortLimit == 0)
                abortLimit = (unsigned)-1;
            if (abortLimit < atMost)
                atMost = abortLimit;
            fetchRequired = helper->diskAccessRequired();
        }
        virtual void process(bool &abortSoon) override
        {
            Owned<IException> exception;
            try
            {
                CKeyLookupResult lookupResult(*activityCtx); // reply for 1 request row

                byte errorCode = 0;
                CMessageBuffer replyMsg;
                replyMsg.append(errorCode);
                unsigned startPos = replyMsg.length();
                MemoryBuffer tmpMB;
                MemoryBuffer &replyMb = activityCtx->useMessageCompression() ? tmpMB : replyMsg;

                replyMb.append(kmc->queryHandle()); // NB: not resent if multiple packets, see below
                DelayedMarker<unsigned> countMarker(replyMb);
                unsigned rowCount = getRowCount();
                unsigned rowNum = 0;
                unsigned rowStart = 0;
                while (!abortSoon)
                {
                    OwnedConstThorRow row = getRowClear(rowNum++);
                    processRow(row, kmc->queryKeyManager(), lookupResult);
                    lookupResult.serialize(replyMb);
                    bool last = rowNum == rowCount;
                    if (last || (replyMb.length() >= DEFAULT_KEYLOOKUP_MAXREPLYSZ))
                    {
                        countMarker.write(rowNum-rowStart);
                        if (activityCtx->useMessageCompression())
                        {
                            fastLZCompressToBuffer(replyMsg, tmpMB.length(), tmpMB.toByteArray());
                            tmpMB.clear();
                        }
                        reply(replyMsg);
                        if (last)
                            break;
                        replyMsg.setLength(startPos);
                        countMarker.restart();
                        // NB: handle not resent, 1st packet was { errorCode, handle, key-row-count, key-row-data.. }, subsequent packets are { errorCode, key-row-count, key-row-data.. }
                        rowStart = rowNum;
                    }
                    lookupResult.clear();
                }
                service.addToKeyManagerCache(kmc.getClear());
            }
            catch (IException *e)
            {
                exception.setown(e);
            }
            if (exception)
                replyError(exception);
        }
    };
    class CFetchLookupResult : public CLookupResult
    {
        typedef CLookupResult PARENT;

        unsigned accepted = 0;
        unsigned rejected = 0;
    public:
        CFetchLookupResult(CActivityContext &_activityCtx) : PARENT(_activityCtx)
        {
            serializer = activityCtx.queryFetchOutputSerializer();
        }
        inline void incAccepted() { ++accepted; }
        inline void incRejected() { ++rejected; }
        void addRow(const void *row)
        {
            rows.push_back(row);
        }
        void clear()
        {
            clearRows();
        }
        void serialize(MemoryBuffer &mb) const
        {
            unsigned numRows = rows.size();
            mb.append(numRows);
            if (numRows)
                serializeRows(mb);
            mb.append(accepted);
            mb.append(rejected);
        }
    };
    class CFetchLookupRequest : public CLookupRequest
    {
        CFetchContext *fetchContext = nullptr;
        const unsigned defaultMaxFetchLookupReplySz = 0x100000;
        const IDynamicTransform *translator = nullptr;
        ISourceRowPrefetcher *prefetcher = nullptr;
        CThorContiguousRowBuffer &prefetchSource;

        void processRow(const void *row, CFetchLookupResult &reply)
        {
            FetchRequestHeader &requestHeader = *(FetchRequestHeader *)row;

            const void *fetchKey = nullptr;
            if (0 != activityCtx->queryFetchInMinSize())
                fetchKey = (const byte *)row + sizeof(FetchRequestHeader);

            prefetchSource.reset(requestHeader.fpos);
            prefetcher->readAhead(prefetchSource);
            const byte *diskFetchRow = prefetchSource.queryRow();

            RtlDynamicRowBuilder fetchReplyBuilder(activityCtx->queryFetchOutputAllocator());
            FetchReplyHeader &replyHeader = *(FetchReplyHeader *)fetchReplyBuilder.getUnfinalized();
            replyHeader.sequence = requestHeader.sequence;
            const void * &childRow = *(const void **)((byte *)fetchReplyBuilder.getUnfinalized() + sizeof(FetchReplyHeader));

            MemoryBuffer diskFetchRowMb;
            if (translator)
            {
                MemoryBufferBuilder aBuilder(diskFetchRowMb, 0);
                LocalVirtualFieldCallback fieldCallback("<MORE>", requestHeader.fpos, 0);
                translator->translate(aBuilder, fieldCallback, diskFetchRow);
                diskFetchRow = reinterpret_cast<const byte *>(diskFetchRowMb.toByteArray());
            }
            size32_t fetchReplySz = sizeof(FetchReplyHeader);
            if (helper->fetchMatch(fetchKey, diskFetchRow))
            {
                replyHeader.sequence |= FetchReplyHeader::fetchMatchedMask;

                RtlDynamicRowBuilder joinFieldsRow(activityCtx->queryJoinFieldsAllocator());
                size32_t joinFieldsSz = helper->extractJoinFields(joinFieldsRow, diskFetchRow, (IBlobProvider*)nullptr); // JCSMORE is it right that passing NULL IBlobProvider here??
                fetchReplySz += joinFieldsSz;
                childRow = joinFieldsRow.finalizeRowClear(joinFieldsSz);
                reply.incAccepted();
            }
            else
            {
                childRow = nullptr;
                reply.incRejected();
            }
            reply.addRow(fetchReplyBuilder.finalizeRowClear(fetchReplySz));
        }
    public:
        CFetchLookupRequest(CFetchContext *_fetchContext, rank_t _sender, mptag_t _replyTag)
            : CLookupRequest(_fetchContext->queryActivityCtx(), _sender, _replyTag),
              fetchContext(_fetchContext), prefetchSource(fetchContext->queryPrefetchSource())
        {
            allocator = activityCtx->queryFetchInputAllocator();
            deserializer = activityCtx->queryFetchInputDeserializer();
            StringBuffer tracing;
            translator = fetchContext->queryTranslator(fetchContext->queryKey().getTracing(tracing));
            prefetcher = fetchContext->queryPrefetcher();
        }
        virtual void process(bool &abortSoon) override
        {
            Owned<IException> exception;
            try
            {
                CFetchLookupResult fetchLookupResult(*activityCtx);

                byte errorCode = 0;
                CMessageBuffer replyMsg;
                replyMsg.append(errorCode);
                unsigned startPos = replyMsg.length();
                MemoryBuffer tmpMB;
                MemoryBuffer &replyMb = activityCtx->useMessageCompression() ? tmpMB : replyMsg;

                replyMb.append(fetchContext->queryHandle()); // NB: not resent if multiple packets, see below
                unsigned rowCount = getRowCount();
                unsigned rowNum = 0;

                // JCSMORE sorting batch of requests by fpos could reduce seeking...
                while (!abortSoon)
                {
                    OwnedConstThorRow row = getRowClear(rowNum++);
                    processRow(row, fetchLookupResult);
                    bool last = rowNum == rowCount;
                    if (last || (replyMb.length() >= defaultMaxFetchLookupReplySz))
                    {
                        fetchLookupResult.serialize(replyMb);
                        if (activityCtx->useMessageCompression())
                        {
                            fastLZCompressToBuffer(replyMsg, tmpMB.length(), tmpMB.toByteArray());
                            tmpMB.clear();
                        }
                        reply(replyMsg);
                        if (last)
                            break;
                        replyMsg.setLength(startPos);
                        // NB: handle not resent, 1st packet was { errorCode, handle, fetch-row-count, fetch-row-data.. }, subsequent packets are { errorCode, fetch-row-count, fetch-row-data.. }
                        fetchLookupResult.clear();
                    }
                }
            }
            catch (IException *e)
            {
                exception.setown(e);
            }
            if (exception)
                replyError(exception);
        }
    };
    template <class ET>
    class CLookupHT : public SuperHashTableOf<ET, CLookupKey>
    {
        typedef SuperHashTableOf<ET, CLookupKey> PARENT;
    public:
        CLookupHT() : PARENT() { }
        ~CLookupHT() { PARENT::_releaseAll(); }

        inline ET *find(const CLookupKey &fp) const                                   \
        {
            return PARENT::find(&fp);
        }
        virtual void onAdd(void * et __attribute__((unused))) override { }
        virtual void onRemove(void * et __attribute__((unused))) override { }
        virtual unsigned getHashFromElement(const void *_et) const
        {
            const ET *et = static_cast<const ET *>(_et);
            return et->queryHash();
        }
        virtual unsigned getHashFromFindParam(const void *_fp) const override
        {
            const CLookupKey *fp = static_cast<const CLookupKey *>(_fp);
            return fp->queryHash();
        }
        virtual const void *getFindParam(const void *_et) const override
        {
            const ET *et = static_cast<const ET *>(_et);
            return &et->queryKey();
        }
        virtual bool matchesFindParam(const void *_et, const void *_fp, unsigned fphash __attribute__((unused))) const override
        {
            const ET *et = static_cast<const ET *>(_et);
            const CLookupKey *fp = static_cast<const CLookupKey *>(_fp);
            return et->queryKey() == *fp;
        }
    };
    template <class ET>
    class CLookupHTOwned : public CLookupHT<ET>
    {
        typedef CLookupHT<ET> PARENT;
    public:
        CLookupHTOwned() : PARENT() { }
        ~CLookupHTOwned() { PARENT::_releaseAll(); }
        virtual void onRemove(void *_et) override
        {
            ET *et = static_cast<ET *>(_et);
            et->Release();
        }
    };
    typedef CLookupHT<CKeyLookupContext> CKeyLookupContextHT;
    typedef CLookupHTOwned<CKMKeyEntry> CKMContextHT;

    class CRemoteLookupProcessor : public CSimpleInterfaceOf<IPooledThread>
    {
        CKJService &service;
        Owned<CLookupRequest> lookupRequest;
        bool abortSoon = false;

    public:
        CRemoteLookupProcessor(CKJService &_service) : service(_service)
        {
        }
    // IPooledThread impl.
        virtual void init(void *param) override
        {
            abortSoon = false;
            lookupRequest.set((CLookupRequest *)param);
        }
        virtual bool stop() override
        {
            abortSoon = true; return true;
        }
        virtual bool canReuse() const override { return true; }
        virtual void threadmain() override
        {
            Owned<CLookupRequest> request = lookupRequest.getClear();
            request->process(abortSoon);
        }
    };
    class CProcessorFactory : public CSimpleInterfaceOf<IThreadFactory>
    {
        CKJService &service;
    public:
        CProcessorFactory(CKJService &_service) : service(_service)
        {
        }
    // IThreadFactory
        virtual IPooledThread *createNew() override
        {
            return service.createProcessor();
        }
    };
    SimpleHashTableOf<CActivityContext, unsigned> activityContextsHT;
    CKeyLookupContextHT keyLookupContextsHT; // non-owning HT of CLookupContexts
    CKMContextHT cachedKMs; // NB: HT by {actId, fname, crc} links IKeyManager's within containers
    OwningSimpleHashTableOf<CKMContainer, unsigned> cachedKMsByHandle; // HT by { handle }, links IKeyManager's within containers.
    OwningSimpleHashTableOf<CFetchContext, FetchKey> fetchContextsHT;
    std::unordered_map<unsigned, CFetchContext *> fetchContextsByHandleHT;
    CICopyArrayOf<CKMContainer> cachedKMsMRU;
    CriticalSection kMCrit, lCCrit;
    Owned<IThreadPool> processorPool;

    CActivityContext *createActivityContext(CJobBase &job, activity_id id)
    {
        VStringBuffer helperName("fAc%u", (unsigned)id);
        EclHelperFactory helperFactory = (EclHelperFactory) job.queryDllEntry().getEntry(helperName.str());
        if (!helperFactory)
            throw makeOsExceptionV(GetLastError(), "Failed to load helper factory method: %s (dll handle = %p)", helperName.str(), job.queryDllEntry().getInstance());

        ICodeContext &codeCtx = job.queryJobChannel(0).querySharedMemCodeContext();
        Owned<IHThorKeyedJoinArg> helper = static_cast<IHThorKeyedJoinArg *>(helperFactory());
        return new CActivityContext(*this, id, helper.getClear(), &codeCtx);
    }
    CActivityContext *ensureActivityContext(CJobBase &job, activity_id id)
    {
        CriticalBlock b(lCCrit);
        CActivityContext *activityCtx = activityContextsHT.find(id);
        if (activityCtx)
            return LINK(activityCtx);
        activityCtx = createActivityContext(job, id);
        activityContextsHT.replace(*activityCtx); // NB: does not link/take ownership
        return activityCtx;
    }
    CKeyLookupContext *createLookupContext(CActivityContext *activityCtx, const CLookupKey &key)
    {
        return new CKeyLookupContext(*this, activityCtx, key);
    }
    CKeyLookupContext *ensureKeyLookupContext(CJobBase &job, const CLookupKey &key, bool *created=nullptr)
    {
        CriticalBlock b(lCCrit);
        CKeyLookupContext *keyLookupContext = keyLookupContextsHT.find(key);
        if (keyLookupContext)
        {
            if (created)
                *created = false;
            return LINK(keyLookupContext);
        }
        if (created)
            *created = true;
        activity_id id = key.id; // JCSMORE annoying, needed because IMPLEMENT_SUPERHASHTABLEOF_REF_FIND doesn't define const (should look at)
        Linked<CActivityContext> activityCtx = activityContextsHT.find(id);
        if (!activityCtx)
        {
            activityCtx.setown(createActivityContext(job, key.id));
            activityContextsHT.replace(*activityCtx); // NB: does not link/take ownership
        }
        keyLookupContext = createLookupContext(activityCtx, key);
        keyLookupContextsHT.replace(*keyLookupContext); // NB: does not link/take ownership
        return keyLookupContext;
    }
    void removeActivityContext(CActivityContext *activityContext)
    {
        CriticalBlock b(lCCrit);
        activityContextsHT.removeExact(activityContext);
    }
    void freeActivityContext(CActivityContext *_activityContext)
    {
        Owned<CActivityContext> activityContext = _activityContext;
        if (!activityContext->IsShared())
        {
            CriticalBlock b(lCCrit);
            activityContextsHT.removeExact(activityContext);
        }
    }
    void freeLookupContext(CKeyLookupContext *_lookupContext)
    {
        CriticalBlock b(lCCrit);
        Owned<CKeyLookupContext> keyLookupContext = _lookupContext;
        if (!keyLookupContext->IsShared())
            keyLookupContextsHT.removeExact(keyLookupContext);
    }
    CFetchContext *ensureFetchContext(CJobBase &job, FetchKey &key)
    {
        CriticalBlock b(lCCrit);
        CFetchContext *fetchContext = fetchContextsHT.find(key);
        if (fetchContext)
            return LINK(fetchContext);
        activity_id id = key.id; // JCSMORE annoying, needed because IMPLEMENT_SUPERHASHTABLEOF_REF_FIND doesn't define const (should look at)
        Linked<CActivityContext> activityCtx = activityContextsHT.find(id);
        if (!activityCtx)
        {
            activityCtx.setown(createActivityContext(job, id));
            activityContextsHT.replace(*activityCtx); // NB: does not link/take ownership
        }
        fetchContext = new CFetchContext(*this, activityCtx, key);
        fetchContextsHT.replace(*fetchContext);
        fetchContextsByHandleHT.insert({fetchContext->queryHandle(), fetchContext});
        return LINK(fetchContext);
    }
    CFetchContext *getFetchContext(unsigned handle)
    {
        CriticalBlock b(lCCrit);
        auto it = fetchContextsByHandleHT.find(handle);
        if (it == fetchContextsByHandleHT.end())
            return nullptr;
        return LINK(it->second);
    }
    bool removeFetchContext(unsigned handle)
    {
        CriticalBlock b(lCCrit);
        auto it = fetchContextsByHandleHT.find(handle);
        if (it == fetchContextsByHandleHT.end())
            return false;
        CFetchContext *fetchContext = it->second;
        it = fetchContextsByHandleHT.erase(it);
        verifyex(fetchContextsHT.removeExact(fetchContext));
        return true;
    }
    CKMContainer *getKeyManager(unsigned handle)
    {
        CriticalBlock b(kMCrit);
        Linked<CKMContainer> kmc = cachedKMsByHandle.find(handle);
        if (kmc)
        {
            verifyex(cachedKMsByHandle.removeExact(kmc));
            CKMKeyEntry *kme = cachedKMs.find(kmc->queryCtx().queryKey());
            assertex(kme);
            verifyex(kme->remove(kmc));
            if (0 == kme->count())
                verifyex(cachedKMs.removeExact(kme));
            verifyex(cachedKMsMRU.zap(*kmc));
            --numCached;
        }
        return kmc.getClear();
    }
    CKMContainer *getCachedKeyManager(const CLookupKey &key)
    {
        CriticalBlock b(kMCrit);
        CKMKeyEntry *kme = cachedKMs.find(key);
        if (!kme)
            return nullptr;
        CKMContainer *kmc = kme->pop();
        if (0 == kme->count())
            verifyex(cachedKMs.removeExact(kme));
        verifyex(cachedKMsByHandle.removeExact(kmc));
        verifyex(cachedKMsMRU.zap(*kmc));
        --numCached;
        return kmc;
    }
    CKMContainer *ensureKeyManager(CKeyLookupContext *keyLookupContext)
    {
        CKMContainer *kmc = getCachedKeyManager(keyLookupContext->queryKey());
        if (!kmc)
        {
            // NB: container links keyLookupContext, and will remove it from keyLookupContextsHT when last reference.
            // The container creates new IKeyManager and unique handle
            kmc = new CKMContainer(*this, keyLookupContext);
        }
        return kmc;
    }
    bool removeKeyManager(unsigned handle)
    {
        Owned<CKMContainer> kmc = getKeyManager(handle);
        if (!kmc)
            return false;
        return true;
    }
    unsigned getUniqId()
    {
        ++uniqueId;
        return uniqueId;
    }
    void clearAll()
    {
        if (cachedKMsMRU.ordinality())
            WARNLOG("KJService: clearing %u key manager context(s), that were not closed cleanly", cachedKMsMRU.ordinality());
        cachedKMsMRU.kill();
        cachedKMsByHandle.kill();
        cachedKMs.kill();
        activityContextsHT.kill();
        keyLookupContextsHT.kill();
        fetchContextsHT.kill();
        currentJob = nullptr;
        numCached = 0;
    }
    void setupProcessorPool()
    {
        Owned<CProcessorFactory> factory = new CProcessorFactory(*this);
        processorPool.setown(createThreadPool("KJService processor pool", factory, this, keyLookupMaxProcessThreads, 10000));
        processorPool->setStartDelayTracing(60000);
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterfaceOf<IKJService>);

    CKJService(mptag_t _mpTag) : threaded("CKJService", this), keyLookupMpTag(_mpTag)
    {
        setupProcessorPool();
    }
    ~CKJService()
    {
        stop();
    }
    void addToKeyManagerCache(CKMContainer *kmc)
    {
        CriticalBlock b(kMCrit);
        if (numCached == maxCachedKJManagers)
        {
            CKMContainer &oldest = cachedKMsMRU.item(0);
            verifyex(removeKeyManager(oldest.queryHandle())); // also removes from cachedKMsMRU
        }
        CKMKeyEntry *kme = cachedKMs.find(kmc->queryCtx().queryKey());
        if (!kme)
        {
            kme = new CKMKeyEntry(kmc->queryCtx().queryKey());
            cachedKMs.replace(*kme);
        }
        kme->push(kmc); // JCSMORE cap. to some max #
        cachedKMsByHandle.replace(*LINK(kmc));
        cachedKMsMRU.append(*kmc);
        ++numCached;
    }
    void abort()
    {
        if (aborted)
            return;
        aborted = true;
        queryNodeComm().cancel(RANK_ALL, keyLookupMpTag);
        processorPool->stopAll(true);
        processorPool->joinAll(true);
        threaded.join();
        clearAll();
    }
    void processKeyLookupRequest(CMessageBuffer &msg, CKMContainer *kmc, rank_t sender, mptag_t replyTag)
    {
        CKeyLookupContext *keyLookupContext = &kmc->queryCtx();
        Owned<CKeyLookupRequest> lookupRequest = new CKeyLookupRequest(*this, keyLookupContext, kmc, sender, replyTag);

        size32_t requestSz;
        msg.read(requestSz);
        lookupRequest->deserialize(requestSz, msg.readDirect(requestSz));
        msg.clear();
        // NB: kmc is added to cache at end of request handling
        processorPool->start(lookupRequest);
    }
    IPooledThread *createProcessor()
    {
        return new CRemoteLookupProcessor(*this);
    }
// IThreaded
    virtual void threadmain() override
    {
        while (!aborted)
        {
            rank_t sender = RANK_NULL;
            CMessageBuffer msg;
            mptag_t replyTag = TAG_NULL;
            byte errorCode = 0;
            bool replyAttempt = false;
            try
            {
                if (!queryNodeComm().recv(msg, RANK_ALL, keyLookupMpTag, &sender))
                    break;
                if (!msg.length())
                    break;
                assertex(currentJob);
                KJServiceCmds cmd;
                msg.read((byte &)cmd);
                msg.read((unsigned &)replyTag);
                switch (cmd)
                {
                    case kjs_keyopen:
                    {
                        CLookupKey key(msg);

                        bool created;
                        Owned<CKeyLookupContext> keyLookupContext = ensureKeyLookupContext(*currentJob, key, &created); // ensure entry in keyLookupContextsHT, will be removed by last CKMContainer
                        bool messageCompression;
                        msg.read(messageCompression);
                        keyLookupContext->queryActivityCtx()->setMessageCompression(messageCompression);
                        RecordTranslationMode translationMode;
                        msg.read(reinterpret_cast<std::underlying_type<RecordTranslationMode>::type &> (translationMode));
                        if (RecordTranslationMode::None != translationMode)
                        {
                            unsigned publishedFormatCrc;
                            msg.read(publishedFormatCrc);
                            Owned<IOutputMetaData> publishedFormat = createTypeInfoOutputMetaData(msg, false);
                            Owned<IOutputMetaData> projectedFormat;
                            bool projected;
                            msg.read(projected);
                            if (projected)
                                projectedFormat.setown(createTypeInfoOutputMetaData(msg, false));
                            else
                                projectedFormat.set(publishedFormat);
                            if (created) // translation for the key context will already have been setup and do not want to free existing
                                keyLookupContext->setTranslation(translationMode, publishedFormat, publishedFormatCrc, projectedFormat);
                        }
                        Owned<CKMContainer> kmc = ensureKeyManager(keyLookupContext); // owns keyLookupContext
                        processKeyLookupRequest(msg, kmc, sender, replyTag);
                        break;
                    }
                    case kjs_keyread:
                    {
                        CLookupKey key(msg);
                        unsigned handle;
                        msg.read(handle);
                        dbgassertex(handle);

                        Owned<CKMContainer> kmc = getKeyManager(handle);
                        if (!kmc) // if closed, alternative is to send just handle and send challenge response if unknown
                        {
                            Owned<CKeyLookupContext> keyLookupContext = ensureKeyLookupContext(*currentJob, key);
                            kmc.setown(ensureKeyManager(keyLookupContext));
                        }
                        processKeyLookupRequest(msg, kmc, sender, replyTag);
                        break;
                    }
                    case kjs_keyclose:
                    {
                        unsigned handle;
                        msg.read(handle);
                        bool res = removeKeyManager(handle);
                        msg.clear();
                        msg.append(errorCode);
                        msg.append(res);
                        replyAttempt = true;
                        if (!queryNodeComm().send(msg, sender, replyTag, LONGTIMEOUT))
                            throw MakeStringException(0, "kjs_close: Failed to reply to lookup request");
                        msg.clear();
                        break;
                    }
                    case kjs_fetchopen:
                    {
                        FetchKey key(msg, (unsigned)sender); // key by {actid, partNo, sender}, to keep each context (from each slave) alive.
                        Owned<CFetchContext> fetchContext = ensureFetchContext(*currentJob, key);
                        CActivityContext *activityCtx = fetchContext->queryActivityCtx();

                        /* NB: clients will send it on their first request, but might already have from others
                         * If have it already, ignore/skip it.
                         * Alternative is to not send it by default on 1st request and send challenge response.
                         */
                        byte flags;
                        msg.read(flags); // compress/encrypted;
                        StringAttr fname;
                        msg.read(fname);
                        // NB: will be ignored if it already has it
                        activityCtx->addFetchFile(flags, key.partNo, fname);

                        bool messageCompression;
                        msg.read(messageCompression);
                        activityCtx->setMessageCompression(messageCompression);

                        RecordTranslationMode translationMode;
                        msg.read(reinterpret_cast<std::underlying_type<RecordTranslationMode>::type &> (translationMode));
                        if (RecordTranslationMode::None != translationMode)
                        {
                            unsigned publishedFormatCrc;
                            msg.read(publishedFormatCrc);
                            Owned<IOutputMetaData> publishedFormat = createTypeInfoOutputMetaData(msg, false);
                            Owned<IOutputMetaData> projectedFormat;
                            bool projected;
                            msg.read(projected);
                            if (projected)
                                projectedFormat.setown(createTypeInfoOutputMetaData(msg, false));
                            else
                                projectedFormat.set(publishedFormat);

                            IHThorKeyedJoinArg *helper = activityCtx->queryHelper();
                            fetchContext->setTranslation(translationMode, publishedFormat, publishedFormatCrc, projectedFormat);
                        }

                        Owned<CFetchLookupRequest> lookupRequest = new CFetchLookupRequest(fetchContext, sender, replyTag);

                        size32_t requestSz;
                        msg.read(requestSz);
                        lookupRequest->deserialize(requestSz, msg.readDirect(requestSz));

                        msg.clear();
                        // NB: kmc is added to cache at end of request handling
                        processorPool->start(lookupRequest);
                        break;
                    }
                    case kjs_fetchread:
                    {
                        unsigned handle;
                        msg.read(handle);
                        dbgassertex(handle);

                        Owned<CFetchContext> fetchContext = getFetchContext(handle);
                        CActivityContext *activityCtx = fetchContext->queryActivityCtx();
                        Owned<CFetchLookupRequest> lookupRequest = new CFetchLookupRequest(fetchContext, sender, replyTag);

                        size32_t requestSz;
                        msg.read(requestSz);
                        lookupRequest->deserialize(requestSz, msg.readDirect(requestSz));

                        msg.clear();
                        // NB: kmc is added to cache at end of request handling
                        processorPool->start(lookupRequest);
                        break;
                    }
                    case kjs_fetchclose:
                    {
                        unsigned handle;
                        msg.read(handle);
                        bool res = removeFetchContext(handle);
                        msg.clear();
                        msg.append(errorCode);
                        msg.append(res);
                        replyAttempt = true;
                        if (!queryNodeComm().send(msg, sender, replyTag, LONGTIMEOUT))
                            throw MakeStringException(0, "kjs_fetchclose: Failed to reply to lookup request");
                        msg.clear();
                        break;
                    }
                }
            }
            catch (IException *e)
            {
                if (replyAttempt)
                    EXCLOG(e, "CKJService: failed to send reply");
                else if (TAG_NULL == replyTag)
                {
                    StringBuffer msg("CKJService: Exception without reply tag. Received from slave: ");
                    if (RANK_NULL==sender)
                        msg.append("<unknown>");
                    else
                        msg.append(sender-1);
                    EXCLOG(e, msg.str());
                    msg.clear();
                }
                else
                {
                    msg.clear();
                    errorCode = 1;
                    msg.append(errorCode);
                    serializeException(e, msg);
                }
                e->Release();
            }
            if (!replyAttempt && msg.length())
            {
                if (!queryNodeComm().send(msg, sender, replyTag, LONGTIMEOUT))
                {
                    ERRLOG("CKJService: Failed to send error response");
                    break;
                }
            }
        }
    }
// IKJService
    virtual void setCurrentJob(CJobBase &job)
    {
        /* NB: For now the service contexts are tied to activities/helpers from a particular job,
         * but since there's only 1 job running a time that is okay
         * Once there's a dynamic implementation of the helper this won't be necessary
         */
        currentJob = &job;
        maxCachedKJManagers = job.getOptUInt("keyedJoinMaxKJMs", defaultMaxCachedKJManagers);
        unsigned newKeyLookupMaxProcessThreads = job.getOptUInt("keyedJoinMaxProcessors", defaultKeyLookupMaxProcessThreads);
        if (newKeyLookupMaxProcessThreads != keyLookupMaxProcessThreads)
        {
            keyLookupMaxProcessThreads = newKeyLookupMaxProcessThreads;
            setupProcessorPool();
        }
    }
    virtual void reset() override
    {
        DBGLOG("KJService reset()");
        processorPool->stopAll(true);
        processorPool->joinAll(false);
        clearAll();
        DBGLOG("KJService reset() done");
    }
    virtual void start() override
    {
        aborted = false;
        threaded.start();
    }
    virtual void stop() override
    {
        if (aborted)
            return;
        PROGLOG("KJService stop()");
        queryNodeComm().cancel(RANK_ALL, keyLookupMpTag);
        processorPool->stopAll(true);
        processorPool->joinAll(true);
        while (!threaded.join(60000))
            PROGLOG("Receiver waiting on remote handlers to signal completion");
        if (aborted)
            return;
        aborted = true;
        clearAll();
    }
// IExceptionHandler impl.
    virtual bool fireException(IException *e) override
    {
        // exceptions should always be handled by processor
        EXCLOG(e, nullptr);
        e->Release();
        return true;
    }
};



class CJobListener : public CSimpleInterface
{
    bool &stopped;
    CriticalSection crit;
    OwningStringSuperHashTableOf<CJobSlave> jobs;
    CFifoFileCache querySoCache; // used to mirror master cache
    IArrayOf<IMPServer> mpServers;
    unsigned channelsPerSlave;

    class CThreadExceptionCatcher : implements IExceptionHandler
    {
        CJobListener &jobListener;
    public:
        CThreadExceptionCatcher(CJobListener &_jobListener) : jobListener(_jobListener)
        {
            addThreadExceptionHandler(this);
        }
        ~CThreadExceptionCatcher()
        {
            removeThreadExceptionHandler(this);
        }
        virtual bool fireException(IException *e)
        {
            mptag_t mptag;
            {
                CriticalBlock b(jobListener.crit);
                if (0 == jobListener.jobs.count())
                {
                    EXCLOG(e, "No job active exception: ");
                    return true;
                }
                IThorException *te = QUERYINTERFACE(e, IThorException);
                CJobSlave *job = NULL;
                if (te && te->queryJobId())
                    job = jobListener.jobs.find(te->queryJobId());
                if (!job)
                {
                    // JCSMORE - exception fallen through to thread exception handler, from unknown job, fire up to 1st job for now.
                    job = (CJobSlave *)jobListener.jobs.next(NULL);
                }
                mptag = job->querySlaveMpTag();
            }
            CMessageBuffer msg;
            msg.append(smt_errorMsg);
            msg.append(0); // unknown really
            serializeThorException(e, msg);

            try
            {
                if (!queryNodeComm().sendRecv(msg, 0, mptag, LONGTIMEOUT))
                    EXCLOG(e, "Failed to send exception to master");
            }
            catch (IException *e2)
            {
                StringBuffer str("Error whilst sending exception '");
                e->errorMessage(str);
                str.append("' to master");
                EXCLOG(e2, str.str());
                e2->Release();
            }
            return true;
        }
    } excptHandler;

public:
    CJobListener(bool &_stopped) : stopped(_stopped), excptHandler(*this)
    {
        stopped = true;
        channelsPerSlave = globals->getPropInt("@channelsPerSlave", 1);
        unsigned localThorPortInc = globals->getPropInt("@localThorPortInc", DEFAULT_SLAVEPORTINC);
        mpServers.append(* getMPServer());
        bool reconnect = globals->getPropBool("@MPChannelReconnect");
        for (unsigned sc=1; sc<channelsPerSlave; sc++)
        {
            unsigned port = getMachinePortBase() + (sc * localThorPortInc);
            IMPServer *mpServer = startNewMPServer(port);
            if (reconnect)
                mpServer->setOpt(mpsopt_channelreopen, "true");
            mpServers.append(*mpServer);
        }
    }
    ~CJobListener()
    {
        for (unsigned sc=1; sc<channelsPerSlave; sc++)
            mpServers.item(sc).stop();
        mpServers.kill();
        stop();
    }
    void stop()
    {
        queryNodeComm().cancel(0, masterSlaveMpTag);
    }
    virtual void slaveMain()
    {
        rank_t slaveProc = queryNodeGroup().rank()-1;
        unsigned totSlaveProcs = queryNodeClusterWidth();
        StringBuffer slaveStr;
        for (unsigned c=0; c<channelsPerSlave; c++)
        {
            unsigned o = slaveProc + (c * totSlaveProcs);
            if (c)
                slaveStr.append(",");
            slaveStr.append(o+1);
        }
        StringBuffer virtStr;
        if (channelsPerSlave>1)
            virtStr.append("virtual slaves:");
        else
            virtStr.append("slave:");
        PROGLOG("Slave log %u contains %s %s", slaveProc+1, virtStr.str(), slaveStr.str());
        traceMemUsage();

        if (channelsPerSlave>1)
        {
            class CVerifyThread : public CInterface, implements IThreaded
            {
                CThreaded threaded;
                CJobListener &jobListener;
                unsigned channel;
            public:
                CVerifyThread(CJobListener &_jobListener, unsigned _channel)
                    : jobListener(_jobListener), channel(_channel), threaded("CVerifyThread", this)
                {
                    start();
                }
                ~CVerifyThread() { join(); }
                void start() { threaded.start(); }
                void join() { threaded.join(); }
                virtual void threadmain() override
                {
                    Owned<ICommunicator> comm = jobListener.mpServers.item(channel).createCommunicator(&queryClusterGroup());
                    PROGLOG("verifying mp connection to rest of slaves (from channel=%d)", channel);
                    if (!comm->verifyAll())
                        ERRLOG("Failed to connect to rest of slaves");
                    else
                        PROGLOG("verified mp connection to rest of slaves");
                }
            };
            CIArrayOf<CInterface> verifyThreads;
            for (unsigned c=0; c<channelsPerSlave; c++)
                verifyThreads.append(*new CVerifyThread(*this, c));
        }

        StringBuffer soPath;
        globals->getProp("@query_so_dir", soPath);
        StringBuffer soPattern("*.");
#ifdef _WIN32
        soPattern.append("dll");
#else
        soPattern.append("so");
#endif
        if (globals->getPropBool("Debug/@dllsToSlaves",true))
            querySoCache.init(soPath.str(), DEFAULT_QUERYSO_LIMIT, soPattern);
        Owned<ISlaveWatchdog> watchdog;
        if (globals->getPropBool("@watchdogEnabled"))
            watchdog.setown(createProgressHandler(globals->getPropBool("@useUDPWatchdog")));

        CMessageBuffer msg;
        stopped = false;
        bool doReply;
        while (!stopped && queryNodeComm().recv(msg, 0, masterSlaveMpTag))
        {
            doReply = true;
            msgids cmd;
            try
            {
                msg.read((unsigned &)cmd);
                switch (cmd)
                {
                    case QueryInit:
                    {
                        MemoryBuffer mb;
                        decompressToBuffer(mb, msg);
                        msg.swapWith(mb);
                        mptag_t slaveMsgTag;
                        deserializeMPtag(msg, slaveMsgTag);
                        queryNodeComm().flush(slaveMsgTag);
                        StringBuffer soPath, soPathTail;
                        StringAttr wuid, graphName, remoteSoPath;
                        msg.read(wuid);
                        msg.read(graphName);
                        msg.read(remoteSoPath);
                        bool sendSo;
                        msg.read(sendSo);

                        RemoteFilename rfn;
                        SocketEndpoint masterEp = queryMyNode()->endpoint();
                        masterEp.port = 0;
                        rfn.setPath(masterEp, remoteSoPath);
                        rfn.getTail(soPathTail);
                        if (sendSo)
                        {
                            size32_t size;
                            msg.read(size);
                            globals->getProp("@query_so_dir", soPath);
                            if (soPath.length())
                                addPathSepChar(soPath);
                            soPath.append(soPathTail);
                            const byte *queryPtr = msg.readDirect(size);
                            Owned<IFile> iFile = createIFile(soPath.str());
                            try
                            {
                                iFile->setCreateFlags(S_IRWXU);
                                Owned<IFileIO> iFileIO = iFile->open(IFOwrite);
                                iFileIO->write(0, size, queryPtr);
                            }
                            catch (IException *e)
                            {
                                IException *e2 = ThorWrapException(e, "Failed to save dll: %s", soPath.str());
                                e->Release();
                                throw e2;
                            }
                            assertex(globals->getPropBool("Debug/@dllsToSlaves", true));
                            querySoCache.add(soPath.str());
                        }
                        else
                        {
                            if (!rfn.isLocal())
                            {
                                StringBuffer _remoteSoPath;
                                rfn.getRemotePath(_remoteSoPath);
                                remoteSoPath.set(_remoteSoPath);
                            }
                            if (globals->getPropBool("Debug/@dllsToSlaves", true))
                            {
                                globals->getProp("@query_so_dir", soPath);
                                if (soPath.length())
                                    addPathSepChar(soPath);
                                soPath.append(soPathTail);
                                OwnedIFile iFile = createIFile(soPath.str());
                                if (!iFile->exists())
                                {
                                    WARNLOG("Slave cached query dll missing: %s, will attempt to fetch from master", soPath.str());
                                    copyFile(soPath.str(), remoteSoPath);
                                }
                                querySoCache.add(soPath.str());
                            }
                            else
                                soPath.append(remoteSoPath);
                        }
#ifdef __linux__
                    // only relevant if dllsToSlaves=false and query_so_dir was fully qualified remote path (e.g. //<ip>/path/file
                        rfn.setRemotePath(soPath.str());
                        StringBuffer tempSo;
                        if (!rfn.isLocal())
                        {
                            WARNLOG("Cannot load shared object directly from remote path, creating temporary local copy: %s", soPath.str());
                            GetTempName(tempSo,"so",true);
                            copyFile(tempSo.str(), soPath.str());
                            soPath.clear().append(tempSo.str());
                        }
#endif
                        Owned<ILoadedDllEntry> querySo = createDllEntry(soPath.str(), false, NULL, false);

                        Owned<IPropertyTree> workUnitInfo = createPTree(msg);
                        StringBuffer user;
                        workUnitInfo->getProp("user", user);

                        PROGLOG("Started wuid=%s, user=%s, graph=%s\n", wuid.get(), user.str(), graphName.get());
                        PROGLOG("Using query: %s", soPath.str());

                        if (!globals->getPropBool("Debug/@slaveDaliClient") && workUnitInfo->getPropBool("Debug/slavedaliclient", false))
                        {
                            PROGLOG("Workunit option 'slaveDaliClient' enabled");
                            enableThorSlaveAsDaliClient();
                        }

                        Owned<IPropertyTree> deps = createPTree(msg);

                        Owned<CJobSlave> job = new CJobSlave(watchdog, workUnitInfo, graphName, querySo, slaveMsgTag);
                        job->setXGMML(deps);
                        for (unsigned sc=0; sc<channelsPerSlave; sc++)
                            job->addChannel(&mpServers.item(sc));
                        jobs.replace(*job.getLink());
                        job->startJob();

                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case QueryDone:
                    {
                        StringAttr key;
                        msg.read(key);
                        CJobSlave *job = jobs.find(key.get());
                        StringAttr wuid = job->queryWuid();
                        StringAttr graphName = job->queryGraphName();

                        PROGLOG("Finished wuid=%s, graph=%s", wuid.get(), graphName.get());

                        if (!globals->getPropBool("Debug/@slaveDaliClient") && job->getWorkUnitValueBool("slaveDaliClient", false))
                            disableThorSlaveAsDaliClient();

                        PROGLOG("QueryDone, removing %s from jobs", key.get());
                        jobs.removeExact(job);
                        PROGLOG("QueryDone, removed %s from jobs", key.get());

                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case GraphInit:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (!job)
                            throw MakeStringException(0, "Job not found: %s", jobKey.get());

                        mptag_t executeReplyTag = job->deserializeMPTag(msg);
                        size32_t len;
                        msg.read(len);
                        MemoryBuffer createInitData;
                        createInitData.append(len, msg.readDirect(len));

                        graph_id subGraphId;
                        msg.read(subGraphId);
                        unsigned graphInitDataPos = msg.getPos();

                        VStringBuffer xpath("node[@id='%" GIDPF "u']", subGraphId);
                        Owned<IPropertyTree> graphNode = job->queryGraphXGMML()->getPropTree(xpath.str());
                        job->addSubGraph(*graphNode);

                        /* JCSMORE - should improve, create 1st graph with create context/init data and clone
                         * Should perhaps do this initialization in parallel..
                         */
                        for (unsigned c=0; c<job->queryJobChannels(); c++)
                        {
                            PROGLOG("GraphInit: %s, graphId=%" GIDPF "d, slaveChannel=%d", jobKey.get(), subGraphId, c);
                            CJobChannel &jobChannel = job->queryJobChannel(c);
                            Owned<CSlaveGraph> subGraph = (CSlaveGraph *)jobChannel.getGraph(subGraphId);
                            subGraph->setExecuteReplyTag(executeReplyTag);

                            createInitData.reset(0);
                            subGraph->deserializeCreateContexts(createInitData);

                            msg.reset(graphInitDataPos);
                            subGraph->init(msg);

                            jobChannel.addDependencies(job->queryXGMML(), false);
                        }

                        for (unsigned c=0; c<job->queryJobChannels(); c++)
                        {
                            CJobChannel &jobChannel = job->queryJobChannel(c);
                            Owned<CSlaveGraph> subGraph = (CSlaveGraph *)jobChannel.getGraph(subGraphId);

                            jobChannel.startGraph(*subGraph, true, 0, NULL);
                        }
                        msg.clear();
                        msg.append(false);

                        break;
                    }
                    case GraphEnd:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            graph_id gid;
                            msg.read(gid);
                            msg.clear();
                            msg.append(false);
                            for (unsigned c=0; c<job->queryJobChannels(); c++)
                            {
                                CJobChannel &jobChannel = job->queryJobChannel(c);
                                Owned<CSlaveGraph> graph = (CSlaveGraph *)jobChannel.getGraph(gid);
                                if (graph)
                                {
                                    msg.append(jobChannel.queryMyRank()-1);
                                    graph->getDone(msg);
                                }
                                else
                                {
                                    msg.append((rank_t)0); // JCSMORE - not sure why this would ever happen
                                }
                            }
                            job->reportGraphEnd(gid);
                        }
                        else
                        {
                            msg.clear();
                            msg.append(false);
                        }
                        break;
                    }
                    case GraphAbort:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        PROGLOG("GraphAbort: %s", jobKey.get());
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            graph_id gid;
                            msg.read(gid);
                            for (unsigned c=0; c<job->queryJobChannels(); c++)
                            {
                                CJobChannel &jobChannel = job->queryJobChannel(c);
                                Owned<CGraphBase> graph = jobChannel.getGraph(gid);
                                if (graph)
                                {
                                    Owned<IThorException> e = MakeGraphException(graph, 0, "GraphAbort");
                                    graph->abort(e);
                                }
                            }
                        }
                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case Shutdown:
                    {
                        stopped = true;
                        PROGLOG("Shutdown received");
                        if (watchdog)
                            watchdog->stop();
                        mptag_t sdreplyTag;
                        deserializeMPtag(msg, sdreplyTag);
                        msg.setReplyTag(sdreplyTag);
                        msg.clear();
                        msg.append(false);
                        break;
                    }
                    case GraphGetResult:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        PROGLOG("GraphGetResult: %s", jobKey.get());
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            graph_id gid;
                            msg.read(gid);
                            activity_id ownerId;
                            msg.read(ownerId);
                            unsigned resultId;
                            msg.read(resultId);
                            mptag_t replyTag = job->deserializeMPTag(msg);
                            msg.setReplyTag(replyTag);
                            msg.clear();
                            doReply = false;
                            for (unsigned c=0; c<job->queryJobChannels(); c++)
                            {
                                CJobChannel &jobChannel = job->queryJobChannel(c);
                                Owned<IThorResult> result = jobChannel.getOwnedResult(gid, ownerId, resultId);
                                Owned<IRowStream> resultStream = result->getRowStream();
                                sendInChunks(jobChannel.queryJobComm(), 0, replyTag, resultStream, result->queryRowInterfaces());
                            }
                        }
                        break;
                    }
                    case DebugRequest:
                    {
                        StringAttr jobKey;
                        msg.read(jobKey);
                        CJobSlave *job = jobs.find(jobKey.get());
                        if (job)
                        {
                            mptag_t replyTag = job->deserializeMPTag(msg);
                            msg.setReplyTag(replyTag);
                            StringAttr rawText;
                            msg.read(rawText);
                            PROGLOG("DebugRequest: %s %s", jobKey.get(), rawText.get());
                            msg.clear();
                            job->debugRequest(msg, rawText);
                        }
                        else
                            PROGLOG("DebugRequest: %s - Job not found", jobKey.get());

                        break;
                    }
                    default:
                        throwUnexpected();
                }
            }
            catch (IException *e)
            {
                EXCLOG(e, NULL);
                if (doReply && TAG_NULL != msg.getReplyTag())
                {
                    doReply = false;
                    msg.clear();
                    msg.append(true);
                    serializeThorException(e, msg);
                    queryNodeComm().reply(msg);
                }
                e->Release();
            }
            if (doReply && msg.getReplyTag()!=TAG_NULL)
                queryNodeComm().reply(msg);
        }
    }

friend class CThreadExceptionCatcher;
};

//////////////////////////


class CStringAttr : public StringAttr, public CSimpleInterface
{
public:
    CStringAttr(const char *str) : StringAttr(str) { }
    const char *queryFindString() const { return get(); }
};
class CFileInProgressHandler : public CSimpleInterface, implements IFileInProgressHandler
{
    CriticalSection crit;
    StringSuperHashTableOf<CStringAttr> lookup;
    QueueOf<CStringAttr, false> fipList;
    OwnedIFileIO iFileIO;
    static const char *formatV;

    void write()
    {
        if (0 == fipList.ordinality())
            iFileIO->setSize(0);
        else
        {
            Owned<IFileIOStream> stream = createBufferedIOStream(iFileIO);
            stream->write(3, formatV); // 3 byte format definition, incase of change later
            ForEachItemIn(i, fipList)
            {
                writeStringToStream(*stream, fipList.item(i)->get());
                writeCharToStream(*stream, '\n');
            }
            offset_t pos = stream->tell();
            stream.clear();
            iFileIO->setSize(pos);
        }
    }
    void doDelete(const char *fip)
    {
        OwnedIFile iFile = createIFile(fip);
        try
        {
            iFile->remove();
        }
        catch (IException *e)
        {
            StringBuffer errStr("FileInProgressHandler, failed to remove: ");
            EXCLOG(e, errStr.append(fip).str());
            e->Release();
        }
    }

    void backup(const char *dir, IFile *iFile)
    {
        StringBuffer origName(iFile->queryFilename());
        StringBuffer bakName("fiplist_");
        CDateTime dt;
        dt.setNow();
        bakName.append((unsigned)dt.getSimple()).append("_").append((unsigned)GetCurrentProcessId()).append(".bak");
        iFileIO.clear(); // close old for rename
        iFile->rename(bakName.str());
        WARNLOG("Renamed to %s", bakName.str());
        OwnedIFile newIFile = createIFile(origName);
        iFileIO.setown(newIFile->open(IFOreadwrite)); // reopen
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFileInProgressHandler()
    {
        init();
    }
    ~CFileInProgressHandler()
    {
        deinit();
    }
    void deinit()
    {
        for (;;)
        {
            CStringAttr *item = fipList.dequeue();
            if (!item) break;
            doDelete(item->get());
            item->Release();
        }
        lookup.kill();
    }
    void init()
    {
        StringBuffer dir;
        globals->getProp("@thorPath", dir);
        StringBuffer path(dir);
        addPathSepChar(path);
        path.append("fiplist_");
        globals->getProp("@name", path);
        path.append("_");
        path.append(queryNodeGroup().rank(queryMyNode()));
        path.append(".lst");
        ensureDirectoryForFile(path.str());
        Owned<IFile> iFile = createIFile(path.str());
        iFileIO.setown(iFile->open(IFOreadwrite));
        if (!iFileIO)
        {
            PROGLOG("Failed to open/create backup file: %s", path.str());
            return;
        }
        MemoryBuffer mb;
        size32_t sz = read(iFileIO, 0, (size32_t)iFileIO->size(), mb);
        const char *mem = mb.toByteArray();
        if (mem)
        {
            if (sz<=3)
            {
                WARNLOG("Corrupt files-in-progress file detected: %s", path.str());
                backup(dir, iFile);
            }
            else
            {
                const char *endMem = mem+mb.length();
                mem += 3; // formatV header
                do
                {
                    const char *eol = strchr(mem, '\n');
                    if (!eol)
                    {
                        WARNLOG("Corrupt files-in-progress file detected: %s", path.str());
                        backup(dir, iFile);
                        break;
                    }
                    StringAttr fip(mem, eol-mem);
                    doDelete(fip);
                    mem = eol+1;
                }
                while (mem != endMem);
            }
        }
        write();
    }
    
// IFileInProgressHandler
    virtual void add(const char *fip)
    {
        CriticalBlock b(crit);
        CStringAttr *item = lookup.find(fip);
        assertex(!item);
        item = new CStringAttr(fip);
        fipList.enqueue(item);
        lookup.add(* item);
        write();
    }
    virtual void remove(const char *fip)
    {
        CriticalBlock b(crit);
        CStringAttr *item = lookup.find(fip);
        if (item)
        {
            lookup.removeExact(item);
            unsigned pos = fipList.find(item);
            fipList.dequeue(item);
            item->Release();
            write();
        }
    }
};
const char *CFileInProgressHandler::formatV = "01\n";


class CThorResourceSlave : public CThorResourceBase
{
    Owned<IThorFileCache> fileCache;
    Owned<IBackup> backupHandler;
    Owned<IFileInProgressHandler> fipHandler;
    Owned<IKJService> kjService;
public:
    CThorResourceSlave()
    {
        backupHandler.setown(createBackupHandler());
        fileCache.setown(createFileCache(globals->getPropInt("@fileCacheLimit", 1800)));
        fipHandler.setown(new CFileInProgressHandler());
        kjService.setown(new CKJService(kjServiceMpTag));
        kjService->start();
    }
    ~CThorResourceSlave()
    {
        kjService.clear();
        fileCache.clear();
        backupHandler.clear();
        fipHandler.clear();
    }
    virtual void beforeDispose() override
    {
        kjService->stop();
    }

// IThorResource
    virtual IThorFileCache &queryFileCache() override { return *fileCache.get(); }
    virtual IBackup &queryBackup() override { return *backupHandler.get(); }
    virtual IFileInProgressHandler &queryFileInProgressHandler() override { return *fipHandler.get(); }
    virtual IKJService &queryKeyedJoinService() override { return *kjService.get(); }
};

void slaveMain(bool &jobListenerStopped)
{
    unsigned masterMemMB = globals->getPropInt("@masterTotalMem");
    HardwareInfo hdwInfo;
    getHardwareInfo(hdwInfo);
    if (hdwInfo.totalMemory < masterMemMB)
        WARNLOG("Slave has less memory than master node");
    unsigned gmemSize = globals->getPropInt("@globalMemorySize");
    bool gmemAllowHugePages = globals->getPropBool("@heapUseHugePages", false);
    bool gmemAllowTransparentHugePages = globals->getPropBool("@heapUseTransparentHugePages", true);
    bool gmemRetainMemory = globals->getPropBool("@heapRetainMemory", false);

    if (gmemSize >= hdwInfo.totalMemory)
    {
        // should prob. error here
    }
    roxiemem::setTotalMemoryLimit(gmemAllowHugePages, gmemAllowTransparentHugePages, gmemRetainMemory, ((memsize_t)gmemSize) * 0x100000, 0, thorAllocSizes, NULL);

    CJobListener jobListener(jobListenerStopped);
    CThorResourceSlave slaveResource;
    setIThorResource(slaveResource);

#ifdef __linux__
    bool useMirrorMount = globals->getPropBool("Debug/@useMirrorMount", false);

    if (useMirrorMount && queryNodeGroup().ordinality() > 2)
    {
        unsigned slaves = queryNodeGroup().ordinality()-1;
        rank_t next = queryNodeGroup().rank()%slaves;  // note 0 = master
        const IpAddress &ip = queryNodeGroup().queryNode(next+1).endpoint();
        StringBuffer ipStr;
        ip.getIpText(ipStr);
        PROGLOG("Redirecting local mount to %s", ipStr.str());
        const char * overrideReplicateDirectory = globals->queryProp("@thorReplicateDirectory");
        StringBuffer repdir;
        if (getConfigurationDirectory(globals->queryPropTree("Directories"),"mirror","thor",globals->queryProp("@name"),repdir))
            overrideReplicateDirectory = repdir.str();
        else
            overrideReplicateDirectory = "/d$";
        setLocalMountRedirect(ip, overrideReplicateDirectory, "/mnt/mirror");
    }

#endif

    jobListener.slaveMain();
}

void abortSlave()
{
    if (clusterInitialized())
        queryNodeComm().cancel(0, masterSlaveMpTag);
}

