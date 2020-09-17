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

#include "jio.hpp"
#include "jfile.hpp"
#include "jtime.hpp"
#include "jsort.hpp"

#include "rtlkey.hpp"
#include "jhtree.hpp"
#include "rmtfile.hpp"
#include "rmtclient.hpp"

#include "thorstep.ipp"

#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "thactivityutil.ipp"
#include "thbufdef.hpp"
#include "thsortu.hpp"
#include "../hashdistrib/thhashdistribslave.ipp"

#include "thdiskbaseslave.ipp"
#include "thindexreadslave.ipp"

enum AdditionStats { AS_Seeks, AS_Scans };
class CIndexReadSlaveBase : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;
protected:
    StringAttr logicalFilename;
    IArrayOf<IPartDescriptor> partDescs;
    IHThorIndexReadBaseArg *helper;
    IHThorSourceLimitTransformExtra * limitTransformExtra;
    Owned<IEngineRowAllocator> allocator;
    Owned<IOutputRowDeserializer> deserializer;
    Owned<IOutputRowSerializer> serializer;
    rowcount_t choosenLimit = 0;
    rowcount_t remoteLimit = RCMAX;
    bool localKey = false;
    size32_t seekGEOffset = 0;
    size32_t fixedDiskRecordSize = 0;
    rowcount_t progress = 0;
    bool eoi = false;
    IArrayOf<IKeyManager> keyManagers;

    IKeyManager *currentManager = nullptr;
    unsigned currentPart = 0;
    Owned<IIndexLookup> currentInput;
    bool localMerge = false;

    Owned<IKeyManager> keyMergerManager;
    Owned<IKeyIndexSet> keyIndexSet;
    IConstPointerArrayOf<ITranslator> translators;
    bool initialized = false;

    rowcount_t keyedLimitCount = RCMAX;
    rowcount_t keyedLimit = RCMAX;
    bool keyedLimitSkips = false;
    bool rowLimitSkips = false;
    rowcount_t keyedProcessed = 0;
    rowcount_t rowLimit = RCMAX;
    bool useRemoteStreaming = false;

    template<class StatProvider>
    class CCaptureIndexStats
    {
        CRuntimeStatisticCollection &stats;
        StatProvider &statProvider;
        unsigned __int64 startSeeks = 0, startScans = 0, startWildSeeks = 0;
    public:
        inline CCaptureIndexStats(CRuntimeStatisticCollection &_stats, StatProvider &_statProvider) : stats(_stats), statProvider(_statProvider)
        {
            startSeeks = statProvider.querySeeks();
            startScans = statProvider.queryScans();
            startWildSeeks = statProvider.queryWildSeeks();
        }
        inline ~CCaptureIndexStats()
        {
            stats.mergeStatistic(StNumIndexSeeks, statProvider.querySeeks() - startSeeks);
            stats.mergeStatistic(StNumIndexScans, statProvider.queryScans() - startScans);
            stats.mergeStatistic(StNumIndexWildSeeks, statProvider.queryWildSeeks() - startWildSeeks);
        }
    };

    class TransformCallback : implements IThorIndexCallback , public CSimpleInterface
    {
    protected:
        CIndexReadSlaveBase &activity;
        IKeyManager *keyManager = nullptr;
    public:
        TransformCallback(CIndexReadSlaveBase &_activity) : activity(_activity) { };
        IMPLEMENT_IINTERFACE_O_USING(CSimpleInterface)

    //IThorIndexCallback
        virtual const byte *lookupBlob(unsigned __int64 id) override
        { 
            size32_t dummy;
            if (!keyManager)
                throw MakeActivityException(&activity, 0, "Callback attempting to read blob with no key manager - index being read remotely?");
            return (byte *) keyManager->loadBlob(id, dummy); 
        }
        void prepareManager(IKeyManager *_keyManager)
        {
            finishedRow();
            keyManager = _keyManager;
        }
        void finishedRow()
        {
            if (keyManager)
                keyManager->releaseBlobs(); 
        }
        void resetManager()
        {
            keyManager = NULL;
        }
    } callback;

    // return a ITranslator based on published format in part and expected/format
    ITranslator *getTranslators(IPartDescriptor &partDesc)
    {
        unsigned projectedFormatCrc = helper->getProjectedFormatCrc();
        IOutputMetaData *projectedFormat = helper->queryProjectedDiskRecordSize();
        IPropertyTree const &props = partDesc.queryOwner().queryProperties();
        Owned<IOutputMetaData> publishedFormat = getDaliLayoutInfo(props);
        unsigned publishedFormatCrc = (unsigned)props.getPropInt("@formatCrc", 0);
        RecordTranslationMode translationMode = getTranslationMode(*this);
        unsigned expectedFormatCrc = helper->getDiskFormatCrc();
        IOutputMetaData *expectedFormat = helper->queryDiskRecordSize();

        Owned<ITranslator> ret = ::getTranslators("rowstream", expectedFormatCrc, expectedFormat, publishedFormatCrc, publishedFormat, projectedFormatCrc, projectedFormat, translationMode);
        if (!ret)
            return nullptr;
        if (!ret->queryTranslator().canTranslate())
            throw MakeStringException(0, "Untranslatable key layout mismatch reading index %s", logicalFilename.get());
        if (ret->queryTranslator().keyedTranslated())
            throw MakeStringException(0, "Untranslatable key layout mismatch reading index %s - keyed fields do not match", logicalFilename.get());
        return ret.getClear();
    }
public:
    IIndexLookup *getNextInput(IKeyManager *&keyManager, unsigned &partNum, bool useMerger)
    {
        keyManager = nullptr;
        if (useMerger && keyMergerManager)
        {
            if (0 == partNum)
                keyManager = keyMergerManager;
        }
        else if (keyManagers.isItem(partNum))
            keyManager = &keyManagers.item(partNum);
        else if (partNum >= partDescs.ordinality())
            return nullptr;
        else
        {
            if (localMerge) // NB: keyManagers[0] will be filled below
            {
                if (partNum > 0)
                    return nullptr;
            }
            RecordTranslationMode translationMode = getTranslationMode(*this);
            unsigned expectedFormatCrc = helper->getDiskFormatCrc();
            IOutputMetaData *expectedFormat = helper->queryDiskRecordSize();
            unsigned projectedFormatCrc = helper->getProjectedFormatCrc();
            IOutputMetaData *projectedFormat = helper->queryProjectedDiskRecordSize();

            unsigned p = partNum;
            while (p<partDescs.ordinality()) // will process all parts if localMerge
            {
                IPartDescriptor &part = partDescs.item(p++);
                unsigned crc=0;
                part.getCrc(crc);

                if (useRemoteStreaming)
                {
                    Owned<ITranslator> translator = getTranslators(part);
                    IOutputMetaData *actualFormat = translator ? &translator->queryActualFormat() : expectedFormat;
                    bool tryRemoteStream = actualFormat->queryTypeInfo()->canInterpret() && actualFormat->queryTypeInfo()->canSerialize() &&
                                           projectedFormat->queryTypeInfo()->canInterpret() && projectedFormat->queryTypeInfo()->canSerialize() &&
                                           !containsKeyedSignedInt(actualFormat->queryTypeInfo());

                    /* If part can potentially be remotely streamed, 1st check if any part is local,
                     * then try to remote stream, and otherwise failover to legacy remote access
                     */
                    if (tryRemoteStream)
                    {
                        std::vector<unsigned> remoteCandidates;
                        for (unsigned copy=0; copy<part.numCopies(); copy++)
                        {
                            RemoteFilename rfn;
                            part.getFilename(copy, rfn);
                            if (!isRemoteReadCandidate(*this, rfn))
                            {
                                StringBuffer path;
                                rfn.getPath(path);
                                Owned<IFile> iFile = createIFile(path);
                                try
                                {
                                    if (iFile->exists())
                                    {
                                        remoteCandidates.clear();
                                        break;
                                    }
                                }
                                catch (IException *e)
                                {
                                    ActPrintLog(e, "getNextInput()");
                                    e->Release();
                                }
                            }
                            else
                                remoteCandidates.push_back(copy);
                        }
                        Owned<IException> remoteReadException;
                        StringBuffer remoteReadExceptionPath;
                        for (unsigned &copy : remoteCandidates) // only if no local part found above
                        {
                            RemoteFilename rfn;
                            part.getFilename(copy, rfn);
                            StringBuffer path;
                            rfn.getPath(path);

                            // Open a stream from remote file, having passed actual, expected, projected, and filters to it
                            SocketEndpoint ep(rfn.queryEndpoint());
                            setDafsEndpointPort(ep);

                            IConstArrayOf<IFieldFilter> fieldFilters;  // These refer to the expected layout
                            struct CIndexReadContext : implements IIndexReadContext
                            {
                                IConstArrayOf<IFieldFilter> &fieldFilters;
                                CIndexReadContext(IConstArrayOf<IFieldFilter> &_fieldFilters) : fieldFilters(_fieldFilters)
                                {
                                }
                                virtual void append(IKeySegmentMonitor *segment) override { throwUnexpected(); }
                                virtual void append(FFoption option, const IFieldFilter * filter) override
                                {
                                    fieldFilters.append(*filter);
                                }
                            } context(fieldFilters);
                            helper->createSegmentMonitors(&context);

                            RowFilter actualFilter;
                            Owned<const IKeyTranslator> keyedTranslator = createKeyTranslator(actualFormat->queryRecordAccessor(true), expectedFormat->queryRecordAccessor(true));
                            if (keyedTranslator && keyedTranslator->needsTranslate())
                                keyedTranslator->translate(actualFilter, fieldFilters);
                            else
                                actualFilter.appendFilters(fieldFilters);

                            StringBuffer lPath;
                            rfn.getLocalPath(lPath);
                            Owned<IIndexLookup> indexLookup = createRemoteFilteredKey(ep, lPath, crc, actualFormat, projectedFormat, actualFilter, remoteLimit);
                            if (indexLookup)
                            {
                                try
                                {
                                    indexLookup->ensureAvailable();
                                }
                                catch (IException *e)
                                {
    #ifdef _DEBUG
                                    EXCLOG(e, nullptr);
    #endif
                                    if (remoteReadException)
                                        e->Release(); // only record 1st
                                    else
                                    {
                                        remoteReadException.setown(e);
                                        remoteReadExceptionPath.set(path);
                                    }
                                    continue; // try next copy and ultimately failover to local when no more copies
                                }
                                ActPrintLog("[part=%d]: reading remote dafilesrv index '%s' (logical file = %s)", partNum, path.str(), logicalFilename.get());
                                partNum = p;
                                return indexLookup.getClear();
                            }
                        }
                        if (remoteReadException)
                        {
                            VStringBuffer msg("Remote streaming failure, failing over to direct read for: '%s'. ", remoteReadExceptionPath.str());
                            remoteReadException->errorMessage(msg);
                            Owned<IThorException> e2 = MakeActivityWarning(this, TE_RemoteReadFailure, "%s", msg.str());
                            fireException(e2);
                        }
                    }
                }

                // local key handling

                Owned<IFileIO> lazyIFileIO = queryThor().queryFileCache().lookupIFileIO(*this, logicalFilename, part);

                RemoteFilename rfn;
                part.getFilename(0, rfn);
                StringBuffer path;
                rfn.getPath(path); // NB: use for tracing only, IDelayedFile uses IPartDescriptor and any copy

                Owned<IKeyIndex> keyIndex = createKeyIndex(path, crc, *lazyIFileIO, (unsigned) -1, false, false);
                Owned<IKeyManager> klManager = createLocalKeyManager(helper->queryDiskRecordSize()->queryRecordAccessor(true), keyIndex, nullptr, helper->hasNewSegmentMonitors(), false);
                if (localMerge)
                {
                    if (!keyIndexSet)
                    {
                        keyIndexSet.setown(createKeyIndexSet());
                        OwnedRoxieString fileName = helper->getFileName();
                        Owned<const ITranslator> translator = getLayoutTranslation(fileName, part, translationMode, expectedFormatCrc, expectedFormat, projectedFormatCrc, projectedFormat);
                        translators.append(translator.getClear());
                    }
                    keyIndexSet->addIndex(keyIndex.getClear());
                    keyManagers.append(*klManager.getLink());
                    keyManager = klManager;
                }
                else
                {
                    OwnedRoxieString fileName = helper->getFileName();
                    Owned<const ITranslator> translator = getLayoutTranslation(fileName, part, translationMode, expectedFormatCrc, expectedFormat, projectedFormatCrc, projectedFormat);
                    if (translator)
                        klManager->setLayoutTranslator(&translator->queryTranslator());
                    translators.append(translator.getClear());
                    keyManagers.append(*klManager.getLink());
                    keyManager = klManager;
                    partNum = p;
                    return createIndexLookup(keyManager);
                }
            }
            keyMergerManager.setown(createKeyMerger(helper->queryDiskRecordSize()->queryRecordAccessor(true), keyIndexSet, seekGEOffset, nullptr, helper->hasNewSegmentMonitors(), false));
            const ITranslator *translator = translators.item(0);
            if (translator)
                keyMergerManager->setLayoutTranslator(&translator->queryTranslator());
            if (useMerger)
                keyManager = keyMergerManager;
            else
                keyManager = &keyManagers.item(partNum);
        }
        if (keyManager)
        {
            ++partNum;
            return createIndexLookup(keyManager);
        }
        else
            return nullptr;
    }
    void configureNextInput()
    {
        if (currentManager)
        {
            resetManager(currentManager);
            currentManager = nullptr;
        }
        IKeyManager *keyManager = nullptr;
        currentInput.setown(getNextInput(keyManager, currentPart, true));
        if (keyManager) // local
        {
            prepareManager(keyManager);
            currentManager = keyManager;
        }
    }
    virtual bool incKeyedExceedsLimit()
    {
        ++keyedProcessed;
        // NB - this is only checking if local limit exceeded (skip case previously checked)
        if (keyedProcessed > keyedLimit)
            return true;
        return false;
    }

    virtual void prepareManager(IKeyManager *manager)
    {
        if (currentManager == manager)
            return;
        resetManager(manager);
        callback.prepareManager(manager);
        helper->createSegmentMonitors(manager);
        manager->finishSegmentMonitors();
        manager->reset();
    }
    void resetManager(IKeyManager *manager)
    {
        callback.resetManager();
        if (localMerge)
            keyMergerManager->reset(); // JCSMORE - not entirely sure why necessary, if not done, resetPending is false. Should releaseSegmentMonitors() handle?
        manager->releaseSegmentMonitors();
        if (currentManager == manager)
            currentManager = nullptr;
    }
    const void *createKeyedLimitOnFailRow()
    {
        RtlDynamicRowBuilder row(allocator);
        size32_t sz = limitTransformExtra->transformOnKeyedLimitExceeded(row);
        if (sz)
            return row.finalizeRowClear(sz);
        return NULL;
    }
    const void *nextKey()
    {
        if (eoi)
            return nullptr;
        dbgassertex(currentInput);
        const void *ret = nullptr;
        while (true)
        {
            {
                CCaptureIndexStats<IIndexLookup> scoped(stats, *currentInput);
                ret = currentInput->nextKey();
                if (ret)
                    break;
            }
            configureNextInput();
            if (!currentInput)
                break;
        }
        if (nullptr == ret || incKeyedExceedsLimit())
        {
            eoi = true;
            return nullptr;
        }
        return ret;
    }
    bool checkKeyedLimit()
    {
        bool ret = false;
        if (RCMAX != keyedLimitCount)
        {
            keyedLimitCount = sendGetCount(keyedLimitCount);
            if (keyedLimitCount > keyedLimit)
            {
                ret = true;
                eoi = true;
            }
            keyedLimit = RCMAX; // don't check again [ during get() / stop() ]
            keyedLimitCount = RCMAX;
        }
        return ret;
    }
    const void *handleKeyedLimit(bool &limitHit, bool &exception)
    {
        limitHit = false;
        exception = false;
        if (checkKeyedLimit())
        {
            limitHit = true;
            if (container.queryLocalOrGrouped() || firstNode())
            {
                if (!keyedLimitSkips)
                {
                    if (0 != (TIRkeyedlimitcreates & helper->getFlags()))
                        return createKeyedLimitOnFailRow();
                    else
                        exception = true;
                }
            }
        }
        return nullptr;
    }
    void initLimits(unsigned __int64 _choosenLimit, unsigned __int64 _keyedLimit, unsigned __int64 _rowLimit, bool mayFilter)
    {
        choosenLimit = _choosenLimit;
        keyedLimit = _keyedLimit;
        rowLimit = _rowLimit;
        if (!helper->canMatchAny())
        {
            // disable
            rowLimit = RCMAX;
            keyedLimit = RCMAX;
        }
        else
        {
            if ((keyedLimit != RCMAX) && (TIRkeyedlimitskips & helper->getFlags()))
                keyedLimitSkips = true;
            if ((rowLimit != RCMAX) && (TIRlimitskips & helper->getFlags()))
                rowLimitSkips = true;
        }
        keyedLimitCount = RCMAX;

        if (!mayFilter)
        {
            if (choosenLimit)
                remoteLimit = choosenLimit;
        }

        if ((RCMAX != keyedLimit) && (keyedLimit+1 < remoteLimit))
            remoteLimit = keyedLimit+1; // 1 more to ensure triggered when received back.
        if ((RCMAX != rowLimit) && (rowLimit+1 < remoteLimit))
            remoteLimit = rowLimit+1; // 1 more to ensure triggered when received back.
    }
    void calcKeyedLimitCount()
    {
        if ((keyedLimit != RCMAX && (keyedLimitSkips || (helper->getFlags() & TIRcountkeyedlimit) != 0)))
            keyedLimitCount = getLocalCount(keyedLimit, true);
    }
public:
    CIndexReadSlaveBase(CGraphElementBase *container)
        : CSlaveActivity(container, indexReadActivityStatistics), callback(*this)
    {
        helper = (IHThorIndexReadBaseArg *)container->queryHelper();
        limitTransformExtra = nullptr;
        fixedDiskRecordSize = helper->queryDiskRecordSize()->querySerializedDiskMeta()->getFixedSize(); // 0 if variable and unused
        allocator.set(queryRowAllocator());
        deserializer.set(queryRowDeserializer());
        serializer.set(queryRowSerializer());
        helper->setCallback(&callback);
        reInit = 0 != (helper->getFlags() & (TIRvarfilename|TIRdynamicfilename));
    }
    rowcount_t getLocalCount(const rowcount_t keyedLimit, bool hard)
    {
        if (0 == partDescs.ordinality())
            return 0;
        // Note - don't use merger's count - it doesn't work
        unsigned __int64 count = 0;
        IKeyManager *_currentManager = currentManager;
        unsigned p = 0;
        while (true)
        {
            IKeyManager *keyManager = nullptr;
            Owned<IIndexLookup> indexInput = getNextInput(keyManager, p, false);
            if (!indexInput)
                break;
            if (keyManager)
                prepareManager(keyManager);
            CCaptureIndexStats<IIndexLookup> scoped(stats, *indexInput);
            if (hard) // checkCount checks hard key count only.
                count += indexInput->checkCount(keyedLimit-count); // part max, is total limit [keyedLimit] minus total so far [count]
            else
                count += indexInput->getCount();
            bool limitHit = count > keyedLimit;
            if (keyManager)
                resetManager(keyManager);
            if (limitHit)
                break;
        }
        if (_currentManager)
            prepareManager(_currentManager);
        return (rowcount_t)count;
    }
    rowcount_t sendGetCount(rowcount_t count)
    {
        if (container.queryLocalOrGrouped())
            return count;
        sendPartialCount(*this, count);
        return getFinalCount(*this);
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        data.read(logicalFilename);
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJobChannel().deserializeMPTag(data); // channel to pass back partial counts for aggregation
        if (initialized)
        {
            partDescs.kill();
            keyIndexSet.clear();
            translators.kill();
            keyManagers.kill();
            keyMergerManager.clear();
        }
        else
            initialized = true;
        
        unsigned parts;
        data.read(parts);
        if (parts)
            deserializePartFileDescriptors(data, partDescs);
        localKey = partDescs.ordinality() ? partDescs.item(0).queryOwner().queryProperties().getPropBool("@local", false) : false;
        localMerge = (localKey && partDescs.ordinality()>1) || seekGEOffset;

        if (parts)
        {
            IPartDescriptor &part0 = partDescs.item(0);
            IFileDescriptor &fileDesc = part0.queryOwner();

            if ((0 == (helper->getFlags() & TIRusesblob)) && !localMerge)
            {
                if (!inChildQuery())
                    useRemoteStreaming = true;
                else
                {
                    /*
                     * If in a CQ, it is counterproductive to use an index read stream per CQ execution if the index
                     * involved is relatively small.
                     * Because, if the index is small and direct reading (and caching) key node pages, it is likely
                     * that repeated executions will not read any (or few) new key pages (i.e. cache hit).
                     *
                     * Example: small 1-way key being remotely read by the whole cluster.
                     * If it is small it will fit (or mostly fit) in node key cache, and thus mostly read from memory vs over the network etc.
                     *
                     */

                    // # data parts excluding TLK if present
                    unsigned totalNumDataParts = fileDesc.numParts();
                    if ((totalNumDataParts>1) && !fileDesc.queryProperties().getPropBool("@local"))
                        totalNumDataParts--; // TLK

                    offset_t logicalFileSize = fileDesc.queryProperties().getPropInt64("@size"); // NB: size is compressed size
                    if (!logicalFileSize) // not sure when/if this should ever be missing, but..
                    {
                        IWARNLOG("Missing @size in meta data for index file '%s'", logicalFilename.get());
                        // estimate size based on physical size of 1st part
                        RemoteFilename rfn;
                        part0.getFilename(0, rfn);
                        StringBuffer path;
                        rfn.getPath(path);
                        Owned<IFile> iFile = createIFile(path);
                        offset_t partSize = iFile->size();
                        logicalFileSize = partSize * totalNumDataParts;
                    }

                    memsize_t keyCacheSize = queryJob().getKeyNodeCacheSize() + queryJob().getKeyLeafCacheSize();
                    memsize_t minRemoteCQIndexSizeMb = getOptInt64(THOROPT_MIN_REMOTE_CQ_INDEX_SIZE_MB);
                    if (minRemoteCQIndexSizeMb)
                    {
                        // anything larger is streamed, anything smaller is read directly
                        if (logicalFileSize > (minRemoteCQIndexSizeMb * 0x100000))
                            useRemoteStreaming = true;
                    }
                    else // no min. size to stream set, so use a heuristic
                    {
                        /*
                         * Rough heuristic.
                         *
                         * If (([average compressed part size] * [# parts handling] * [compressionMultiple] * [cacheSizeFitPercentage%]) > [keyCacheSize])
                         *     then useRemoteStreaming = true
                         *
                         * i.e. if the [cacheSizeFitPercentage] % of total size of the compressed index data this slave is handling multiplied
                         * by a rough compression multiplier [compressionMultiple] is larger than the cache size [keyCacheSize], then use streaming.
                         * If not (useRemoteStreaming=false), direct read (and use the cache).
                         *
                         * The cacheSizeFitPercentage (25%) is used, so that the index has to be significantly bigger than the cache to use streaming,
                         * because it is still worth directly reading on relatively small indexes, even if 1:4 cache hits are acheived.
                         *
                         */

                        static const unsigned compressionMultiple = 10; // v. rough approx. of compression ratio (actual compression ratio/uncompressed size unknown)
                        static const unsigned cacheSizeFitPercentage = 25; // if this much (%) of amount I'm handling fits into cache

                        offset_t avgPartSize = logicalFileSize / totalNumDataParts;

                        // NB: The # parts this slave is dealing with (partDescs.ordinality()) is equal to all data parts (totalNumDataParts) when in a CQ.
                        offset_t myIndexPartSizeTotal = avgPartSize * partDescs.ordinality() * compressionMultiple;

                        offset_t myIndexPartSizeHitShare = myIndexPartSizeTotal * cacheSizeFitPercentage / 100;
                        if (myIndexPartSizeHitShare >= keyCacheSize) // e.g. if 25% of my handled index data is larger than cache
                            useRemoteStreaming = true;
                    }
                }
            }
        }
    }
    // IThorDataLink
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        keyedProcessed = 0;
        if (!eoi)
        {
            if (0 == partDescs.ordinality())
            {
                eoi = true;
                return;
            }
            currentPart = 0;
            eoi = false;
            configureNextInput();
        }
    }
    virtual void reset() override
    {
        PARENT::reset();
        eoi = false;
        currentPart = 0;
        if (currentManager)
        {
            resetManager(currentManager);
            currentManager = nullptr;
        }
    }
    virtual void serializeStats(MemoryBuffer &mb) override
    {
        stats.setStatistic(StNumRowsProcessed, progress);
        PARENT::serializeStats(mb);
    }
};

class CIndexReadSlaveActivity : public CIndexReadSlaveBase
{
    typedef CIndexReadSlaveBase PARENT;

    IHThorIndexReadArg *helper;
    bool first = false, needTransform = false, optimizeSteppedPostFilter = false, steppingEnabled = false;
    ISteppingMeta *rawMeta;
    ISteppingMeta *projectedMeta;
    IInputSteppingMeta *inputStepping;
    IRangeCompare *stepCompare;
    IHThorSteppedSourceExtra *steppedExtra;
    CSteppingMeta steppingMeta;
    UnsignedArray seekSizes;

    const void *getNextRow()
    {
        unsigned __int64 postFiltered = 0;
        auto onScopeExitFunc = [&]()
        {
            if (postFiltered > 0)
                stats.mergeStatistic(StNumPostFiltered, postFiltered);
        };
        COnScopeExit scoped(onScopeExitFunc);
        RtlDynamicRowBuilder ret(allocator);
        for (;;)
        {
            const void *r = nextKey();
            if (!r)
                break;
            if (likely(helper->canMatch(r)))
            {
                if (needTransform)
                {
                    size32_t sz = helper->transform(ret, r);
                    if (sz)
                    {
                        callback.finishedRow();
                        return ret.finalizeRowClear(sz);
                    }
                    else
                        ++postFiltered;
                }
                else
                {
                    callback.finishedRow(); // since filter might have accessed a blob
                    size32_t sz = queryRowMetaData()->getRecordSize(r);
                    memcpy(ret.ensureCapacity(sz, NULL), r, sz);
                    return ret.finalizeRowClear(sz);
                }
            }
            else
            {
                ++postFiltered;
                callback.finishedRow(); // since filter might have accessed a blob
            }
        }
        return nullptr;
    }
    virtual void prepareManager(IKeyManager *manager) override
    {
        PARENT::prepareManager(manager);
        if (choosenLimit && !helper->transformMayFilter() && !helper->hasMatchFilter())
            manager->setChooseNLimit(choosenLimit);
    }
    const void *nextKeyGE(const void *seek, unsigned numFields)
    {
        assertex(localMerge);
        const byte *rawSeek = (const byte *)seek + seekGEOffset;
        unsigned seekSize = seekSizes.item(numFields-1);
        if (projectedMeta)
        {
            byte *temp = (byte *) alloca(seekSize);
            //GH: Is it overkill to use a builder as the target here??
            RtlStaticRowBuilder tempBuilder(temp - seekGEOffset, seekGEOffset+seekSize);
            helper->mapOutputToInput(tempBuilder, seek, numFields); // NOTE - weird interface to mapOutputToInput means that it STARTS writing at seekGEOffset...
            rawSeek = (byte *)temp;
        }
        CCaptureIndexStats<IKeyManager> scoped(stats, *currentManager);
        if (!currentManager->lookupSkip(rawSeek, seekGEOffset, seekSize))
            return NULL;
        const byte *row = currentManager->queryKeyBuffer();
#ifdef _DEBUG
        if (memcmp(row + seekGEOffset, rawSeek, seekSize) < 0)
            assertex("smart seek failure");
#endif
        if (incKeyedExceedsLimit())
        {
            eoi = true;
            return NULL;
        }
        return row;
    }
    const void *getNextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        unsigned __int64 postFiltered = 0;
        auto onScopeExitFunc = [&]()
        {
            if (postFiltered > 0)
                stats.mergeStatistic(StNumPostFiltered, postFiltered);
        };
        COnScopeExit scoped(onScopeExitFunc);
        RtlDynamicRowBuilder ret(allocator);
        size32_t seekSize = seekSizes.item(numFields-1);
        for (;;)
        {
            const void *r = nextKeyGE(seek, numFields);
            if (!r)
                break;
#ifdef _DEBUG
            if (seek && memcmp((byte *)r + seekGEOffset, seek, seekSize) < 0)
                assertex(!"smart seek failure");
#endif
            if (likely(helper->canMatch(r)))
            {
                if (needTransform)
                {
                    size32_t sz = helper->transform(ret, r);
                    if (sz)
                    {
                        callback.finishedRow();
                        return ret.finalizeRowClear(sz);
                    }
                    else
                    {
                        if (optimizeSteppedPostFilter && stepExtra.returnMismatches())
                        {
                            if (memcmp(ret.getSelf() + seekGEOffset, seek, seekSize) != 0)
                            {
                                size32_t sz = helper->unfilteredTransform(ret, r);
                                if (sz)
                                {
                                    wasCompleteMatch = false;
                                    callback.finishedRow();
                                    return ret.finalizeRowClear(sz);
                                }
                                else
                                    ++postFiltered;
                            }
                        }
                        else
                            ++postFiltered;
                    }
                }
                else
                {
                    callback.finishedRow(); // since filter might have accessed a blob
                    size32_t sz = queryRowMetaData()->getRecordSize(r);
                    memcpy(ret.ensureCapacity(sz, NULL), r, sz);
                    return ret.finalizeRowClear(sz);
                }
            }
            else
            {
                ++postFiltered;
                callback.finishedRow(); // since filter might have accessed a blob
            }
        }
        return nullptr;
    }
public:
    CIndexReadSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container)
    {
        helper = (IHThorIndexReadArg *)queryContainer().queryHelper();
        limitTransformExtra = helper;
        rawMeta = helper->queryRawSteppingMeta();
        projectedMeta = helper->queryProjectedSteppingMeta();
        steppedExtra = helper->querySteppingExtra();
        optimizeSteppedPostFilter = (helper->getFlags() & TIRunfilteredtransform) != 0;
        steppingEnabled = 0 != container.queryJob().getWorkUnitValueInt("steppingEnabled", 0);
        if (rawMeta)
        {
            //should check that no translation, also should check all keys in maxFields list can actually be keyed.
            const CFieldOffsetSize * fields = rawMeta->queryFields();
            unsigned maxFields = rawMeta->getNumFields();
            seekGEOffset = fields[0].offset;
            seekSizes.ensure(maxFields);
            seekSizes.append(fields[0].size);
            for (unsigned i=1; i < maxFields; i++)
                seekSizes.append(seekSizes.item(i-1) + fields[i].size);
        }
        appendOutputLinked(this);
    }
    virtual bool incKeyedExceedsLimit() override
    {
        if (!PARENT::incKeyedExceedsLimit())
            return false;
        helper->onKeyedLimitExceeded(); // should throw exception
        return true;
    }
// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        CIndexReadSlaveBase::init(data, slaveData);

        if (rawMeta)
        {
            bool hasPostFilter = helper->transformMayFilter() && optimizeSteppedPostFilter;
            if (projectedMeta)
                steppingMeta.init(projectedMeta, hasPostFilter);
            else
                steppingMeta.init(rawMeta, hasPostFilter);
        }
    }

// IThorDataLink
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);

        needTransform = helper->needTransform();

        // NB: initLimits sets up remoteLimit before base start() call, because if parts are remote PARENT::start() will use remoteLimit
        initLimits(helper->getChooseNLimit(), helper->getKeyedLimit(), helper->getRowLimit(), helper->transformMayFilter() || helper->hasMatchFilter());
        calcKeyedLimitCount();

        PARENT::start();

        first = true;
        if (steppedExtra)
            steppingMeta.setExtra(steppedExtra);
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }

// IRowStream
    virtual void stop() override
    {
        if (RCMAX != keyedLimit) // NB: will not be true if nextRow() has handled
        {
            keyedLimitCount = sendGetCount(keyedProcessed);
            if (keyedLimitCount > keyedLimit && !keyedLimitSkips && (container.queryLocalOrGrouped() || firstNode()))
                helper->onKeyedLimitExceeded(); // should throw exception
        }
        if (currentManager)
        {
            resetManager(currentManager);
            currentManager = nullptr;
        }
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (RCMAX != keyedLimitCount)
        {
            bool limitHit;
            bool exception;
            OwnedConstThorRow limitRow = handleKeyedLimit(limitHit, exception);
            if (exception)
                helper->onKeyedLimitExceeded(); // should throw exception
            else if (limitHit)
            {
                if (limitRow)
                    dataLinkIncrement();
                return limitRow.getClear();
            }
        }
        if (eoi) // NB: intentionally checked after keyedLimitCount above, since eoi can be set in preparation for keyedLimit handling that needs to happen here
            return nullptr;
        OwnedConstThorRow row = getNextRow();
        if (row)
        {
            ++progress;
            if (getDataLinkCount() >= rowLimit)
            {
                helper->onLimitExceeded(); // should throw exception
                eoi = true;
                return nullptr;
            }
            dataLinkIncrement();
            return row.getClear();
        }
        eoi = true;
        return nullptr;
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra) override
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        if (RCMAX != keyedLimitCount)
        {
            bool limitHit;
            bool exception;
            OwnedConstThorRow limitRow = handleKeyedLimit(limitHit, exception);
            if (exception)
                helper->onKeyedLimitExceeded(); // should throw exception
            else if (limitHit)
            {
                if (limitRow)
                    dataLinkIncrement();
                return limitRow.getClear();
            }
        }
        if (eoi) // NB: intentionally checked after keyedLimitCount above, since eoi can be set in preparation for keyedLimit handling that needs to happen here
            return nullptr;
        OwnedConstThorRow row = getNextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (row)
        {
            ++progress;
            if (getDataLinkCount() >= rowLimit)
            {
                helper->onLimitExceeded(); // should throw exception
                eoi = true;
                return nullptr;
            }
            dataLinkIncrement();
            return row.getClear();
        }
        eoi = true;
        return nullptr;
    }
    IInputSteppingMeta *querySteppingMeta()
    {
        if (rawMeta && steppingEnabled && (0 == (helper->getFlags() & (TIRlimitskips|TIRlimitcreates|TIRkeyedlimitskips|TIRkeyedlimitcreates))))
            return &steppingMeta;
        return nullptr;
    }

friend class CIndexReadHelper;
};

CActivityBase *createIndexReadSlave(CGraphElementBase *container)
{
    return new CIndexReadSlaveActivity(container);
}


/////////////////////////////////////////////////////////////

class CIndexGroupAggregateSlaveActivity : public CIndexReadSlaveBase, implements IHThorGroupAggregateCallback
{
    typedef CIndexReadSlaveBase PARENT;

    IHThorIndexGroupAggregateArg *helper;
    bool gathered, merging;
    Owned<IAggregateTable> localAggTable;
    Owned<IRowStream> aggregateStream;
    Owned<IHashDistributor> distributor;
    bool done = false;

public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    CIndexGroupAggregateSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container)
    {
        helper = (IHThorIndexGroupAggregateArg *)container.queryHelper();
        merging = false;
        appendOutputLinked(this);
    }
// IHThorGroupAggregateCallback
    virtual void processRow(const void *next)
    {
        localAggTable->addRow(next);
    }
// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        localAggTable.setown(createRowAggregator(*this, *helper, *helper));
        localAggTable->init(queryRowAllocator());
        gathered = false;
        done = false;
    }
// IRowStream
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (done)
            return NULL;
        if (!gathered)
        {
            gathered = true;
            try
            {
                IKeyManager *_currentManager = currentManager;
                unsigned p = 0;
                while (true)
                {
                    IKeyManager *keyManager = nullptr;
                    Owned<IIndexLookup> indexInput = getNextInput(keyManager, p, false);
                    if (!indexInput)
                        break;
                    if (keyManager)
                        prepareManager(keyManager);

                    CCaptureIndexStats<IIndexLookup> scoped(stats, *indexInput);
                    while (true)
                    {
                        const void *key = indexInput->nextKey();
                        if (!key)
                            break;
                        ++progress;
                        helper->processRow(key, this);
                        callback.finishedRow();
                    }
                    if (keyManager)
                        resetManager(keyManager);
                }
                if (_currentManager)
                    prepareManager(_currentManager);
                ::ActPrintLog(this, thorDetailedLogLevel, "INDEXGROUPAGGREGATE: Local aggregate table contains %d entries", localAggTable->elementCount());
                if (!container.queryLocal() && container.queryJob().querySlaves()>1)
                {
                    Owned<IRowStream> localAggStream = localAggTable->getRowStream(true);
                    BooleanOnOff tf(merging);
                    aggregateStream.setown(mergeLocalAggs(distributor, *this, *helper, *helper, localAggStream, mpTag));
                }
                else
                    aggregateStream.setown(localAggTable->getRowStream(false));
            }
            catch (IException *e)
            {
                if (!isOOMException(e))
                    throw e;
                throw checkAndCreateOOMContextException(this, e, "aggregating using hash table", localAggTable->elementCount(), helper->queryDiskRecordSize(), NULL);
            }
        }
        const void *next = aggregateStream->nextRow();
        if (next)
        {
            dataLinkIncrement();
            return next;
        }
        done = true;
        return NULL;
    }
    virtual void stop() override
    {
        if (aggregateStream)
        {
            aggregateStream->stop();
            if (distributor)
            {
                distributor->disconnect(true);
                distributor->join();
            }            
        }
        PARENT::stop();
    }
    virtual void abort() override
    {
        CIndexReadSlaveBase::abort();
        if (merging)
            queryJobChannel().queryJobComm().cancel(0, mpTag);
    }
};

CActivityBase *createIndexGroupAggregateSlave(CGraphElementBase *container) { return new CIndexGroupAggregateSlaveActivity(container); }


/////////////////////////////////////////////////////////////


class CIndexCountSlaveActivity : public CIndexReadSlaveBase
{
    typedef CIndexReadSlaveBase PARENT;

    IHThorIndexCountArg *helper;
    rowcount_t preknownTotalCount = 0;
    bool totalCountKnown = false;
    bool done = false;

    bool checkKeyedLimit()
    {
        if (!PARENT::checkKeyedLimit())
            return false;
        else if (container.queryLocalOrGrouped() || firstNode())
        {
            if (!keyedLimitSkips)
                helper->onKeyedLimitExceeded(); // should throw exception
        }
        return true;
    }
public:
    CIndexCountSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container)
    {
        helper = static_cast <IHThorIndexCountArg *> (container.queryHelper());
        appendOutputLinked(this);
    }
    virtual void prepareManager(IKeyManager *manager) override
    {
        PARENT::prepareManager(manager);
        if (choosenLimit && !helper->hasFilter())
            manager->setChooseNLimit(choosenLimit);
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);

        // NB: initLimits sets up remoteLimit before base start() call, because if parts are remote PARENT::start() will use remoteLimit
        initLimits(helper->getChooseNLimit(), helper->getKeyedLimit(), helper->getRowLimit(), helper->hasMatchFilter());
        calcKeyedLimitCount();

        PARENT::start();

        if (!helper->canMatchAny())
        {
            totalCountKnown = true;
            preknownTotalCount = 0;
        }
        done = false;
    }

// IRowStream
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (done)
            return nullptr;
        done = true;
        rowcount_t totalCount = 0;
        if (totalCountKnown)
        {
            totalCount = preknownTotalCount;
            if (!container.queryLocalOrGrouped() && !firstNode())
                return nullptr;
        }
        else
        {
            if (!checkKeyedLimit())
            {
                if (helper->hasFilter())
                {
                    IKeyManager *_currentManager = currentManager;
                    unsigned p = 0;
                    while (true)
                    {
                        IKeyManager *keyManager = nullptr;
                        Owned<IIndexLookup> indexInput = getNextInput(keyManager, p, false);
                        if (!indexInput)
                            break;
                        if (keyManager)
                            prepareManager(keyManager);

                        CCaptureIndexStats<IIndexLookup> scoped(stats, *indexInput);
                        while (true)
                        {
                            const void *key = indexInput->nextKey();
                            if (!key)
                                break;
                            if (incKeyedExceedsLimit())
                                break;
                            ++progress;
                            totalCount += helper->numValid(key);
                            if (totalCount > rowLimit)
                                break;
                            if (keyManager)
                                callback.finishedRow();
                            if ((totalCount > choosenLimit))
                                break;
                        }
                        if (keyManager)
                            resetManager(keyManager);
                        if ((totalCount > choosenLimit))
                            break;
                    }
                    if (_currentManager)
                        prepareManager(_currentManager);
                }
                else
                    totalCount = getLocalCount(choosenLimit, false);
            }
            if (!container.queryLocalOrGrouped())
            {
                sendPartialCount(*this, totalCount);
                if (!firstNode())
                    return nullptr;
                totalCount = getFinalCount(*this);
            }
            if (totalCount > rowLimit)
            {
                if (TIRlimitskips & helper->getFlags())
                    totalCount = 0;
                else
                    helper->onLimitExceeded();
            }
            if (totalCount > choosenLimit)
                totalCount = choosenLimit;
        }
        RtlDynamicRowBuilder result(allocator);
        // these should serialize using helper function?
        size32_t orsz = queryRowMetaData()->getMinRecordSize();
        size32_t sz;
        if (1 == orsz)
        {
            assertex(choosenLimit <= 255);
            byte *dst1 = (byte *)result.ensureCapacity(sizeof(byte),NULL);
            *dst1 = (byte)totalCount;
            sz = sizeof(byte);
        }
        else
        {
            assertex(sizeof(unsigned __int64) == orsz);
            unsigned __int64 *dst8 = (unsigned __int64 *)result.ensureCapacity(sizeof(unsigned __int64),NULL);
            *dst8 = (unsigned __int64)totalCount;
            sz = sizeof(unsigned __int64);
        }
        dataLinkIncrement();
        return result.finalizeRowClear(sz);     
    }
    virtual void stop() override
    {
        if (RCMAX != keyedLimit) // NB: will not be true if nextRow() has handled
        {
            keyedLimitCount = sendGetCount(keyedProcessed);
            if (keyedLimitCount > keyedLimit && !keyedLimitSkips && (container.queryLocalOrGrouped() || firstNode()))
                helper->onKeyedLimitExceeded(); // should throw exception
        }
        PARENT::stop();
    }
    virtual void abort() override
    {
        CIndexReadSlaveBase::abort();
        cancelReceiveMsg(0, mpTag);
    }
};

CActivityBase *createIndexCountSlave(CGraphElementBase *container)
{
    return new CIndexCountSlaveActivity(container);
}


class CIndexNormalizeSlaveActivity : public CIndexReadSlaveBase
{
    typedef CIndexReadSlaveBase PARENT;

    bool expanding = false;
    IHThorIndexNormalizeArg *helper;

    const void * createNextRow()
    {
        RtlDynamicRowBuilder row(allocator);
        size32_t sz = helper->transform(row);
        if (sz==0)
            return NULL;
        if (getDataLinkCount() >= rowLimit)
        {
            helper->onLimitExceeded();
            return NULL;
        }
        dataLinkIncrement();
        return row.finalizeRowClear(sz);
    }

public:
    CIndexNormalizeSlaveActivity(CGraphElementBase *_container) : CIndexReadSlaveBase(_container)
    {
        helper = (IHThorIndexNormalizeArg *)container.queryHelper();
        limitTransformExtra = helper;
        appendOutputLinked(this);
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSource = true;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);

        // NB: initLimits sets up remoteLimit before base start() call, because if parts are remote PARENT::start() will use remoteLimit
        initLimits(helper->getChooseNLimit(), helper->getKeyedLimit(), helper->getRowLimit(), helper->hasMatchFilter());
        calcKeyedLimitCount();

        PARENT::start();

        expanding = false;
    }

// IRowStream
    virtual void stop() override
    {
        if (RCMAX != keyedLimit) // NB: will not be true if nextRow() has handled
        {
            keyedLimitCount = sendGetCount(keyedProcessed);
            if (keyedLimitCount > keyedLimit)
                helper->onKeyedLimitExceeded(); // should throw exception
        }
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (RCMAX != keyedLimitCount)
        {
            bool limitHit;
            bool exception;
            OwnedConstThorRow limitRow = handleKeyedLimit(limitHit, exception);
            if (exception)
                helper->onKeyedLimitExceeded(); // should throw exception
            else if (limitHit)
            {
                if (limitRow)
                    dataLinkIncrement();
                return limitRow.getClear();
            }
        }
        if (eoi) // NB: intentionally checked after keyedLimitCount above, since eoi can be set in preparation for keyedLimit handling that needs to happen here
            return nullptr;

        rowcount_t c = getDataLinkCount();
        if ((choosenLimit && c==choosenLimit)) // NB: only slave limiter, global performed in chained choosen activity
            return nullptr;

        for (;;)
        {
            if (expanding)
            {
                for (;;)
                {
                    expanding = helper->next();
                    if (!expanding)
                        break;

                    OwnedConstThorRow row = createNextRow();
                    if (row)
                        return row.getClear();
                }
            }

            for (;;)
            {
                callback.finishedRow();
                const void *rec = nextKey();
                if (rec)
                {
                    ++progress;
                    expanding = helper->first(rec);
                    if (expanding)
                    {
                        OwnedConstThorRow row = createNextRow();
                        if (row)
                            return row.getClear();
                        break;
                    }
                }
                else
                {
                    eoi = true;
                    return nullptr;
                }
            }
        }
    }
};

CActivityBase *createIndexNormalizeSlave(CGraphElementBase *container) { return new CIndexNormalizeSlaveActivity(container); }

class CIndexAggregateSlaveActivity : public CIndexReadSlaveBase
{
    typedef CIndexReadSlaveBase PARENT;

    bool hadElement = false;
    bool done = false;
    IHThorIndexAggregateArg *helper;
    CPartialResultAggregator aggregator;

public:
    CIndexAggregateSlaveActivity(CGraphElementBase *_container) 
        : CIndexReadSlaveBase(_container), aggregator(*this)
    {
        helper = (IHThorIndexAggregateArg *)container.queryHelper();
        appendOutputLinked(this);
    }

// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSource = true;
        info.totalRowsMin = 0;
        info.totalRowsMax = 1;
        // MORE TBD
    }
    virtual bool isGrouped() const override { return false; }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        hadElement = false;
        done = false;
    }

// IRowStream
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (done)
            return nullptr;
        done = true;

        RtlDynamicRowBuilder row(allocator);
        helper->clearAggregate(row);
        for (;;)
        {
            const void *r = nextKey();
            if (!r)
                break;
            hadElement = true;
            helper->processRow(row, r);
            callback.finishedRow();
        }
        if (container.queryLocalOrGrouped())
        {
            dataLinkIncrement();
            size32_t sz = allocator->queryOutputMeta()->getRecordSize(row.getSelf());
            return row.finalizeRowClear(sz);
        }
        else
        {
            OwnedConstThorRow ret;
            if (hadElement)
            {
                size32_t sz = allocator->queryOutputMeta()->getRecordSize(row.getSelf());
                ret.setown(row.finalizeRowClear(sz));
            }
            aggregator.sendResult(ret.get());
            if (firstNode())
            {
                ret.setown(aggregator.getResult());
                if (ret)
                {
                    dataLinkIncrement();
                    return ret.getClear();
                }
            }
            return nullptr;
        }
    }  
};

CActivityBase *createIndexAggregateSlave(CGraphElementBase *container) { return new CIndexAggregateSlaveActivity(container); }

