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
#include "jfile.ipp"

#include "thbuf.hpp"
#include "slave.ipp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "thactivityutil.ipp"
#include "keybuild.hpp"
#include "thbufdef.hpp"
#include "backup.hpp"
#include "thorfile.hpp"

#define SINGLEPART_KEY_TRANSFER_SIZE 0x10000
#define FEWWARNCAP 10


class IndexWriteSlaveActivity : public ProcessSlaveActivity, public ILookAheadStopNotify, implements ICopyFileProgress, implements IBlobCreator
{
    typedef ProcessSlaveActivity PARENT;
    StringAttr logicalFilename;
    Owned<IPartDescriptor> partDesc, tlkDesc;
    IHThorIndexWriteArg *helper;
    Owned <IKeyBuilder> builder;
    Owned<IRowStream> myInputStream;
    Owned<IPropertyTree> metadata;
    Linked<IEngineRowAllocator> outRowAllocator;

    bool buildTlk, active;
    bool sizeSignalled;
    bool isLocal, singlePartKey, reportOverflow, fewcapwarned, refactor;
    bool defaultNoSeek = false;
    unsigned __int64 totalCount;

    size32_t maxDiskRecordSize, lastRowSize, firstRowSize;
    unsigned __int64 duplicateKeyCount;
    MemoryBuffer rowBuff;
    OwnedConstThorRow lastRow, firstRow;
    bool needFirstRow, enableTlkPart0, receivingTag2;

    unsigned replicateDone;
    Owned<IFile> existingTlkIFile;
    unsigned partCrc, tlkCrc;
    mptag_t mpTag2;
    Owned<IRowServer> rowServer;

    void init()
    {
        sizeSignalled = false;
        totalCount = 0;
        lastRowSize = firstRowSize = 0;
        replicateDone = 0;
        fewcapwarned = false;
        needFirstRow = true;
        receivingTag2 = false;
    }
public:
    IMPLEMENT_IINTERFACE_USING(PARENT);

    IndexWriteSlaveActivity(CGraphElementBase *_container) : ProcessSlaveActivity(_container, indexWriteActivityStatistics)
    {
        helper = static_cast <IHThorIndexWriteArg *> (queryHelper());
        init();
        maxDiskRecordSize = 0;
        active = false;
        isLocal = false;
        buildTlk = true;
        singlePartKey = false;
        refactor = false;
        enableTlkPart0 = (0 != container.queryJob().getWorkUnitValueInt("enableTlkPart0", globals->getPropBool("@enableTlkPart0", true)));
        defaultNoSeek = (0 != container.queryJob().getWorkUnitValueInt("noSeekBuildIndex", globals->getPropBool("@noSeekBuildIndex", isContainerized())));
        reInit = (0 != (TIWvarfilename & helper->getFlags()));
        duplicateKeyCount = 0;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        isLocal = 0 != (TIWlocal & helper->getFlags());

        mpTag = container.queryJobChannel().deserializeMPTag(data);
        mpTag2 = container.queryJobChannel().deserializeMPTag(data);
        data.read(active);
        if (active)
        {
            data.read(logicalFilename);
            partDesc.setown(deserializePartFileDescriptor(data));
        }

        data.read(singlePartKey);
        data.read(refactor);
        if (singlePartKey)
            buildTlk = false;
        else
        {
            data.read(buildTlk);
            if (firstNode())
            {
                if (buildTlk)
                    tlkDesc.setown(deserializePartFileDescriptor(data));
                else if (!isLocal) // existing tlk then..
                {
                    tlkDesc.setown(deserializePartFileDescriptor(data));
                    unsigned c;
                    data.read(c);
                    while (c--)
                    {
                        RemoteFilename rf;
                        rf.deserialize(data);
                        if (!existingTlkIFile)
                        {
                            Owned<IFile> iFile = createIFile(rf);
                            if (iFile->exists())
                                existingTlkIFile.set(iFile);
                        }
                    }
                    if (!existingTlkIFile)
                        throw MakeActivityException(this, TE_FileNotFound, "Top level key part does not exist, for key");
                }
            }
        }

        IOutputMetaData * diskSize = helper->queryDiskRecordSize();
        //Need to adjust the size if the last field is used in the special fileposition location.
        size32_t fileposSize = hasTrailingFileposition(diskSize->queryTypeInfo()) ? sizeof(offset_t) : 0;
        assertex(!(diskSize->getMetaFlags() & MDFneedserializedisk));
        if (diskSize->isVariableSize())
        {
            if (TIWmaxlength & helper->getFlags())
                maxDiskRecordSize = helper->getMaxKeySize();
            else
                maxDiskRecordSize = KEYBUILD_MAXLENGTH; //Current default behaviour, could be improved in the future
        }
        else
            maxDiskRecordSize = diskSize->getFixedSize() - fileposSize;
        reportOverflow = false;
    }
    void open(IPartDescriptor &partDesc, bool isTopLevel, bool isVariable, bool isTlk)
    {
        StringBuffer partFname;
        getPartFilename(partDesc, 0, partFname);
        bool compress=false;
        ActPrintLog("INDEXWRITE: created fixed output stream %s", partFname.str());
        bool needsSeek = true;
        unsigned flags = COL_PREFIX;
        if (TIWrowcompress & helper->getFlags())
            flags |= HTREE_COMPRESSED_KEY|HTREE_QUICK_COMPRESSED_KEY;
        else if (!(TIWnolzwcompress & helper->getFlags()))
            flags |= HTREE_COMPRESSED_KEY;
        if (!isLocal)
            flags |= HTREE_FULLSORT_KEY;
        if (isVariable)
            flags |= HTREE_VARSIZE;
        if (isTlk)
            flags |= HTREE_TOPLEVEL_KEY;
        buildUserMetadata(metadata);                
        buildLayoutMetadata(metadata);
        // NOTE - if you add any more flags here, be sure to update checkReservedMetadataName
        unsigned nodeSize = metadata->getPropInt("_nodeSize", NODESIZE);
        if (metadata->getPropBool("_noSeek", defaultNoSeek))
        {
            flags |= TRAILING_HEADER_ONLY;
            needsSeek = false;
        }
        if (metadata->getPropBool("_useTrailingHeader", true))
            flags |= USE_TRAILING_HEADER;
        unsigned twFlags = isUrl(partFname) ? TW_Direct : TW_RenameToPrimary;
        OwnedIFileIO iFileIO = createMultipleWrite(this, partDesc, 0, twFlags, compress, NULL, this, &abortSoon);
        Owned<IFileIOStream> out = createBufferedIOStream(iFileIO, 0x100000);
        if (!needsSeek)
            out.setown(createNoSeekIOStream(out));

        builder.setown(createKeyBuilder(out, flags, maxDiskRecordSize, nodeSize, helper->getKeyedSize(), isTopLevel ? 0 : totalCount, helper, !isTlk, isTlk));
    }
    void buildUserMetadata(Owned<IPropertyTree> & metadata)
    {
        size32_t nameLen;
        char * nameBuff;
        size32_t valueLen;
        char * valueBuff;
        unsigned idx = 0;
        while(helper->getIndexMeta(nameLen, nameBuff, valueLen, valueBuff, idx++))
        {
            StringBuffer name(nameLen, nameBuff);
            StringBuffer value(valueLen, valueBuff);
            if(*nameBuff == '_' && !checkReservedMetadataName(name))
                throw MakeActivityException(this, 0, "Invalid name %s in user metadata for index %s (names beginning with underscore are reserved)", name.str(), logicalFilename.get());
            if(!validateXMLTag(name.str()))
                throw MakeActivityException(this, 0, "Invalid name %s in user metadata for index %s (not legal XML element name)", name.str(), logicalFilename.get());
            if(!metadata) metadata.setown(createPTree("metadata"));
            metadata->setProp(name.str(), value.str());
        }
    }
    void buildLayoutMetadata(Owned<IPropertyTree> & metadata)
    {
        if(!metadata) metadata.setown(createPTree("metadata"));
        metadata->setProp("_record_ECL", helper->queryRecordECL());

        setRtlFormat(*metadata, helper->queryDiskRecordSize());
    }
    void close(IPartDescriptor &partDesc, unsigned &crc)
    {
        StringBuffer partFname;
        getPartFilename(partDesc, 0, partFname);
        Owned<IException> e;
        try
        {
            if (builder)
                builder->finish(metadata, &crc);
        }
        catch (IException *_e)
        {
            ActPrintLog(_e, "Error closing file: %s", partFname.str());
            abortSoon = true;
            e.setown(_e);
        }
        catch (CATCHALL)
        {
            abortSoon = true;
            e.setown(MakeActivityException(this, 0, "INDEXWRITE: Error closing file: %s - unknown exception", partFname.str()));
        }
        try 
        { 
            metadata.clear();
            builder.clear(); 
        }
        catch (IException *_e)
        {
            ActPrintLog(_e, "Error closing file: %s", partFname.str());
            _e->Release();
        }
        if (abortSoon)
            removeFiles(partDesc);
        if (e)
            throw LINK(e);
    }
    void removeFiles(IPartDescriptor &partDesc)
    {
        StringBuffer partFname;
        getPartFilename(partDesc, 0, partFname);
        Owned<IFile> primary = createIFile(partFname.str());
        try { primary->remove(); }
        catch (IException *e) { ActPrintLog(e, "Failed to remove file: %s", partFname.str()); e->Release(); }
        catch (CATCHALL) { ActPrintLog("Failed to remove: %s", partFname.str()); }
    }
    virtual unsigned __int64 createBlob(size32_t size, const void * ptr)
    {
        return builder->createBlob(size, (const char *) ptr);
    }
    virtual void process() override
    {
        ::ActPrintLog(this, thorDetailedLogLevel, "INDEXWRITE: Start");
        init();

        IRowStream *stream = inputStream;
        outRowAllocator.setown(getRowAllocator(helper->queryDiskRecordSize()));
        start();

        if (ensureStartFTLookAhead(0))
            setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), INDEXWRITE_SMART_BUFFER_SIZE, true, false, RCUNBOUND, this, &container.queryJob().queryIDiskUsage()), false);

        if (refactor)
        {
            assertex(isLocal);
            if (active)
            {
                unsigned targetWidth = partDesc->queryOwner().numParts()-(buildTlk?1:0);
                assertex(0 == container.queryJob().querySlaves() % targetWidth);
                unsigned partsPerNode = container.queryJob().querySlaves() / targetWidth;
                unsigned myPart = queryJobChannel().queryMyRank();

                IArrayOf<IRowStream> streams;
                streams.append(*LINK(stream));
                --partsPerNode;

 // Should this be merging 1,11,21,31 etc.
                unsigned p=0;
                unsigned fromPart = targetWidth+1 + (partsPerNode * (myPart-1));
                for (; p<partsPerNode; p++)
                {
                    streams.append(*createRowStreamFromNode(*this, fromPart++, queryJobChannel().queryJobComm(), mpTag, abortSoon));
                }
                ICompare *icompare = helper->queryCompare();
                assertex(icompare);
                Owned<IRowLinkCounter> linkCounter = new CThorRowLinkCounter;
                myInputStream.setown(createRowStreamMerger(streams.ordinality(), streams.getArray(), icompare, false, linkCounter));
                stream = myInputStream;
            }
            else // serve nodes, creating merged parts
                rowServer.setown(createRowServer(this, stream, queryJobChannel().queryJobComm(), mpTag));
        }
        processed = THORDATALINK_STARTED;

        // single part key support
        // has to serially pull all data fron nodes 2-N
        // nodes 2-N, could/should start pushing some data (as it's supposed to be small) to cut down on serial nature.
        unsigned node = queryJobChannel().queryMyRank();
        if (singlePartKey)
        {
            if (1 == node)
            {
                try
                {
                    open(*partDesc, false, helper->queryDiskRecordSize()->isVariableSize(), false);
                    for (;;)
                    {
                        OwnedConstThorRow row = inputStream->ungroupedNextRow();
                        if (!row)
                            break;
                        if (abortSoon) return;
                        processRow(row);
                    }

                    unsigned node = 2;
                    while (node <= container.queryJob().querySlaves())
                    {
                        Linked<IOutputRowDeserializer> deserializer = ::queryRowDeserializer(input);
                        CMessageBuffer mb;
                        Owned<ISerialStream> stream = createMemoryBufferSerialStream(mb);
                        CThorStreamDeserializerSource rowSource;
                        rowSource.setStream(stream);
                        bool successSR;
                        for (;;)
                        {
                            {
                                BooleanOnOff tf(receivingTag2);
                                successSR = queryJobChannel().queryJobComm().sendRecv(mb, node, mpTag2);
                            }
                            if (successSR)
                            {
                                if (rowSource.eos())
                                    break;
                                Linked<IEngineRowAllocator> allocator = ::queryRowAllocator(input);
                                do
                                {
                                    RtlDynamicRowBuilder rowBuilder(allocator);
                                    size32_t sz = deserializer->deserialize(rowBuilder, rowSource);
                                    OwnedConstThorRow fRow = rowBuilder.finalizeRowClear(sz);
                                    processRow(fRow);
                                }
                                while (!rowSource.eos());
                            }
                        }
                        node++;
                    }
                }
                catch (CATCHALL)
                {
                    close(*partDesc, partCrc);
                    throw;
                }
                duplicateKeyCount = builder->getDuplicateCount();
                close(*partDesc, partCrc);
                stop();
            }
            else
            {
                CMessageBuffer mb;
                CMemoryRowSerializer mbs(mb);
                Linked<IOutputRowSerializer> serializer = ::queryRowSerializer(input);
                for (;;)
                {
                    BooleanOnOff tf(receivingTag2);
                    if (queryJobChannel().queryJobComm().recv(mb, 1, mpTag2)) // node 1 asking for more..
                    {
                        if (abortSoon) break;
                        mb.clear();
                        do
                        {
                            OwnedConstThorRow row = inputStream->ungroupedNextRow();
                            if (!row) break;
                            serializer->serialize(mbs, (const byte *)row.get());
                        } while (mb.length() < SINGLEPART_KEY_TRANSFER_SIZE); // NB: at least one row
                        if (!queryJobChannel().queryJobComm().reply(mb))
                            throw MakeThorException(0, "Failed to send index data to node 1, from node %d", node);
                        if (0 == mb.length())
                            break;
                    }
                }
            }
        }
        else
        {
            if (!refactor || active)
            {
                try
                {
                    StringBuffer partFname;
                    getPartFilename(*partDesc, 0, partFname);
                    ::ActPrintLog(this, thorDetailedLogLevel, "INDEXWRITE: process: handling fname : %s", partFname.str());
                    open(*partDesc, false, helper->queryDiskRecordSize()->isVariableSize(), false);

                    BooleanOnOff tf(receiving);
                    if (!refactor || !active)
                        receiving = false;
                    do
                    {
                        OwnedConstThorRow row = inputStream->ungroupedNextRow();
                        if (!row)
                            break;
                        processRow(row);
                    } while (!abortSoon);
                }
                catch (CATCHALL)
                {
                    close(*partDesc, partCrc);
                    throw;
                }
                duplicateKeyCount = builder->getDuplicateCount();
                close(*partDesc, partCrc);
                stop();

                ActPrintLog("INDEXWRITE: Wrote %" RCPF "d records", processed & THORDATALINK_COUNT_MASK);

                if (buildTlk)
                {
                    ::ActPrintLog(this, thorDetailedLogLevel, "INDEXWRITE: sending rows");
                    NodeInfoArray tlkRows;

                    CMessageBuffer msg;
                    MemoryAttr dummyRow;
                    if (firstNode())
                    {
                        if (isLocal)
                        {
                            size32_t minSz = helper->queryDiskRecordSize()->getMinRecordSize();
                            if (hasTrailingFileposition(helper->queryDiskRecordSize()->queryTypeInfo()))
                                minSz -= sizeof(offset_t);
                            // dummyRow used if isLocal and a slave had no rows
                            dummyRow.allocate(minSz);
                            memset(dummyRow.mem(), 0xff, minSz);

                        }
                        if (processed & THORDATALINK_COUNT_MASK)
                        {
                            if (enableTlkPart0)
                                tlkRows.append(* new CNodeInfo(0, firstRow.get(), firstRowSize, totalCount));
                            tlkRows.append(* new CNodeInfo(1, lastRow.get(), lastRowSize, totalCount));
                        }
                        else if (isLocal)
                        {
                            // if a local key TLK (including PARTITION keys), need an entry per part
                            if (enableTlkPart0)
                                tlkRows.append(* new CNodeInfo(0, dummyRow.get(), dummyRow.length(), totalCount));
                            tlkRows.append(* new CNodeInfo(1, dummyRow.get(), dummyRow.length(), totalCount));
                        }
                    }
                    else
                    {
                        if (processed & THORDATALINK_COUNT_MASK)
                        {
                            CNodeInfo row(queryJobChannel().queryMyRank(), lastRow.get(), lastRowSize, totalCount);
                            row.serialize(msg);
                        }
                        queryJobChannel().queryJobComm().send(msg, 1, mpTag);
                    }

                    if (firstNode())
                    {
                        ::ActPrintLog(this, thorDetailedLogLevel, "INDEXWRITE: Waiting on tlk to complete");

                        // JCSMORE if refactor==true, is rowsToReceive here right??
                        unsigned rowsToReceive = (refactor ? (tlkDesc->queryOwner().numParts()-1) : container.queryJob().querySlaves()) -1; // -1 'cos got my own in array already
                        ActPrintLog("INDEXWRITE: will wait for info from %d slaves before writing TLK", rowsToReceive);

                        while (rowsToReceive--)
                        {
                            msg.clear();
                            rank_t sender;
                            receiveMsg(msg, RANK_ALL, mpTag, &sender);
                            if (abortSoon)
                                return;
                            if (msg.length())
                            {
                                CNodeInfo *ni = new CNodeInfo();
                                ni->deserialize(msg);
                                tlkRows.append(*ni);
                            }
                            else if (isLocal)
                            {
                                // if a local key TLK (including PARTITION keys), need an entry per part
                                CNodeInfo *ni = new CNodeInfo(sender, dummyRow.get(), dummyRow.length(), totalCount);
                                tlkRows.append(*ni);
                            }
                        }
                        tlkRows.sort(CNodeInfo::compare);

                        StringBuffer path;
                        getPartFilename(*tlkDesc, 0, path);
                        ::ActPrintLog(this, thorDetailedLogLevel, "INDEXWRITE: creating toplevel key file : %s", path.str());
                        try
                        {
                            open(*tlkDesc, true, helper->queryDiskRecordSize()->isVariableSize(), true);
                            if (!isLocal && tlkRows.length())
                            {
                                CNodeInfo &lastNode = tlkRows.item(tlkRows.length()-1);
                                memset(lastNode.value, 0xff, lastNode.size);
                            }
                            ForEachItemIn(idx, tlkRows)
                            {
                                CNodeInfo &info = tlkRows.item(idx);
                                builder->processKeyData((char *)info.value, info.pos, info.size);
                            }
                            close(*tlkDesc, tlkCrc);
                        }
                        catch (CATCHALL)
                        {
                            abortSoon = true;
                            close(*tlkDesc, tlkCrc);
                            removeFiles(*partDesc);
                            throw;
                        }
                    }
                }
                else if (!isLocal && firstNode())
                {
                    // if !buildTlk - then copy provided index's tlk.
                    unsigned l;
                    for (l=0; l<tlkDesc->numCopies(); l++)
                    {
                        StringBuffer path;
                        getPartFilename(*tlkDesc, l, path, true);
                        if (0 == l)
                        {
                            OwnedIFile dstIFile = createIFile(path.str());
                            copyFile(dstIFile, existingTlkIFile);
                        }
                        else
                            doReplicate(this, *tlkDesc, NULL);
                    }
                }
            }
            ::ActPrintLog(this, thorDetailedLogLevel, "INDEXWRITE: All done");
        }
    }
    virtual void endProcess() override
    {
        if (processed & THORDATALINK_STARTED)
        {
            if (!inputStopped) // probably already stopped in process()
                stop();
            processed |= THORDATALINK_STOPPED;
        }
        inputStream = NULL;
    }
    virtual void abort() override
    {
        PARENT::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
        if (receivingTag2)
            queryJobChannel().queryJobComm().cancel(RANK_ALL, mpTag2);
        if (rowServer)
            rowServer->stop();
    }
    virtual void kill() override
    {
        PARENT::kill();
        if (abortSoon)
        {
            if (partDesc)
                removeFiles(*partDesc);
            if (tlkDesc.get())
                removeFiles(*tlkDesc);
        }
    }
    virtual void processDone(MemoryBuffer &mb) override
    {
        builder.clear();
        if (refactor && !active)
            return;
        rowcount_t _processed = processed & THORDATALINK_COUNT_MASK;
        mb.append(_processed);
        mb.append(duplicateKeyCount);
        if (!singlePartKey || firstNode())
        {
            StringBuffer partFname;
            getPartFilename(*partDesc, 0, partFname);
            Owned<IFile> ifile = createIFile(partFname.str());
            offset_t sz = ifile->size();
            mb.append(sz);
            if ((offset_t)-1 != sz)
                container.queryJob().queryIDiskUsage().increase(sz);
            CDateTime createTime, modifiedTime, accessedTime;
            ifile->getTime(&createTime, &modifiedTime, &accessedTime);
            modifiedTime.serialize(mb);
            mb.append(partCrc);
            if (!singlePartKey && firstNode() && buildTlk)
            {
                mb.append(tlkCrc);
                StringBuffer path;
                getPartFilename(*tlkDesc, 0, path);
                ifile.setown(createIFile(path.str()));
                sz = ifile->size();
                mb.append(sz);
                if ((offset_t)-1 != sz)
                    container.queryJob().queryIDiskUsage().increase(sz);
                ifile->getTime(&createTime, &modifiedTime, &accessedTime);
                modifiedTime.serialize(mb);
            }
        }
    }

    inline void processRow(const void *row)
    {
        // Extract the file position and insert the sequence number and other rollups...
        unsigned __int64 fpos;
        RtlDynamicRowBuilder lastRowBuilder(outRowAllocator);
        lastRowSize = helper->transform(lastRowBuilder, row, this, fpos);
        lastRow.setown(lastRowBuilder.finalizeRowClear(lastRowSize));
        // NB: result of transform is serialized
        if (enableTlkPart0 && needFirstRow)
        {
            needFirstRow = false;
            firstRow.set(lastRow);
            firstRowSize = lastRowSize;
        }
        if (reportOverflow && totalCount == I64C(0x100000000))
        {
            Owned<IThorException> e = MakeActivityWarning(this, TE_MoxieIndarOverflow, "Moxie indar sequence number has overflowed");
            fireException(e);
            reportOverflow = false;
        }
        builder->processKeyData((const char *)lastRow.get(), fpos, lastRowSize);
        processed++;
        totalCount++;
        if (singlePartKey && !fewcapwarned && totalCount>(FEWWARNCAP*0x100000))
        {
            fewcapwarned = true;
            Owned<IThorException> e = MakeActivityWarning(this, TE_BuildIndexFewExcess, "BUILDINDEX: building single part key because marked as 'FEW' but row count in excess of %dM", FEWWARNCAP);
            fireException(e);
        }
    }
    virtual void onInputFinished(rowcount_t finalcount) override
    {
        if (!sizeSignalled)
        {
            sizeSignalled = true;
            ActPrintLog("finished input %" RCPF "d", finalcount);
        }
    }
    virtual void serializeStats(MemoryBuffer &mb) override
    {
        stats.setStatistic(StPerReplicated, replicateDone);
        PARENT::serializeStats(mb);
    }

// ICopyFileProgress
    virtual CFPmode onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize) override
    {
        replicateDone = sizeDone ? ((unsigned)(sizeDone*100/totalSize)) : 0;
        return abortSoon?CFPstop:CFPcontinue;
    }
};

CActivityBase *createIndexWriteSlave(CGraphElementBase *container)
{
    return new IndexWriteSlaveActivity(container);
}
