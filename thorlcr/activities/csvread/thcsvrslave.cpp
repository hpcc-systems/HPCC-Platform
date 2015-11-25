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

#include "jio.hpp"
#include "jio.hpp"
#include "jlzw.hpp"
#include "jtime.hpp"
#include "jsort.hpp"

#include "thormisc.hpp"
#include "thmfilemanager.hpp"
#include "thorport.hpp"
#include "thexception.hpp"
#include "thsortu.hpp"
#include "thbufdef.hpp"
#include "thactivityutil.ipp"
#include "csvsplitter.hpp"
#include "thdiskbaseslave.ipp"

class CCsvReadSlaveActivity : public CDiskReadSlaveActivityBase, public CThorDataLink
{
    IHThorCsvReadArg *helper;
    StringAttr csvQuote, csvSeparate, csvTerminate, csvEscape;
    Owned<IRowStream> out;
    rowcount_t limit;
    rowcount_t stopAfter;
    unsigned headerLines;
    OwnedMalloc<unsigned> headerLinesRemaining, localLastPart;
    Owned<IBitSet> gotHeaderLines, sentHeaderLines;
    ISuperFileDescriptor *superFDesc;
    unsigned subFiles;

    class CCsvPartHandler : public CDiskPartHandlerBase
    {
        Linked<IEngineRowAllocator> allocator;
        CCsvReadSlaveActivity &activity;
        Owned<ISerialStream> inputStream;
        OwnedIFileIO iFileIO;
        CSVSplitter csvSplitter;
        CRC32 inputCRC;
        bool readFinished;
        offset_t localOffset;
        size32_t maxRowSize;

        unsigned splitLine()
        {
            if (inputStream->eos())
                return 0;
            size32_t minRequired = 4096; // MORE - make configurable
            size32_t thisLineLength;
            loop
            {
                size32_t avail;
                const void *peek = inputStream->peek(minRequired, avail);
                thisLineLength = csvSplitter.splitLine(avail, (const byte *)peek);
                if (thisLineLength < minRequired || avail < minRequired)
                    break;
                if (minRequired == maxRowSize)
                    throw MakeActivityException(&activity, 0, "File %s contained a line of length greater than %d bytes.", activity.logicalFilename.get(), minRequired);
                if (minRequired >= maxRowSize/2)
                    minRequired = maxRowSize;
                else
                    minRequired += minRequired;
            }
            return thisLineLength;
        }
    public:
        CCsvPartHandler(CCsvReadSlaveActivity &_activity) : CDiskPartHandlerBase(_activity), activity(_activity)
        {
            readFinished = false;
            localOffset = 0;
            //Initialise information...
            ICsvParameters * csvInfo = activity.helper->queryCsvParameters();
            csvSplitter.init(activity.helper->getMaxColumns(), csvInfo, activity.csvQuote, activity.csvSeparate, activity.csvTerminate, activity.csvEscape);
            maxRowSize = activity.getOptInt(OPT_MAXCSVROWSIZE, defaultMaxCsvRowSize) * 1024 * 1024;
        }
        virtual void setPart(IPartDescriptor *partDesc, unsigned partNoSerialized)
        {
            inputCRC.reset();
            CDiskPartHandlerBase::setPart(partDesc, partNoSerialized);
        }
        virtual void open() 
        {
            allocator.set(activity.queryRowAllocator());
            localOffset = 0;
            CDiskPartHandlerBase::open();
            readFinished = false;

            {
                CriticalBlock block(statsCs);
                if (compressed)
                {
                    iFileIO.setown(createCompressedFileReader(iFile, activity.eexp));
                    if (!iFileIO)
                        throw MakeActivityException(&activity, 0, "Failed to open block compressed file '%s'", filename.get());
                    checkFileCrc = false;
                }
                else
                    iFileIO.setown(iFile->open(IFOread));
            }

            inputStream.setown(createFileSerialStream(iFileIO));
            if (activity.headerLines)
            {
                unsigned subFile = 0;
                unsigned pnum = partDesc->queryPartIndex();
                if (activity.superFDesc)
                {
                    unsigned lnum;
                    if (!activity.superFDesc->mapSubPart(pnum, subFile, lnum))
                        throwUnexpected(); // was validated earlier
                    pnum = lnum;
                }
                unsigned &headerLinesRemaining = activity.getHeaderLines(subFile);
                if (headerLinesRemaining)
                {
                    do
                    {
                        unsigned lineLength = splitLine();
                        if (0 == lineLength)
                            break;
                        inputStream->skip(lineLength);
                    }
                    while (--headerLinesRemaining);
                }
                activity.sendHeaderLines(subFile, pnum);
            }
        }
        virtual void close(CRC32 &fileCRC)
        {
            CriticalBlock block(statsCs);
            mergeStats(fileStats, iFileIO);
            iFileIO.clear();
            inputStream.clear();
            fileCRC = inputCRC;
        }
        const void *nextRow()
        {
            RtlDynamicRowBuilder row(allocator);
            loop
            {
                if (eoi || activity.abortSoon)
                    return NULL;
                unsigned lineLength = splitLine();
                if (!lineLength)
                    return NULL;
                size32_t res = activity.helper->transform(row, csvSplitter.queryLengths(), (const char * *)csvSplitter.queryData());
                inputStream->skip(lineLength);
                if (res != 0)
                {
                    localOffset += lineLength;
                    ++activity.diskProgress;
                    return row.finalizeRowClear(res);
                }
            }
        }
        offset_t getLocalOffset() { return localOffset; }
        virtual void gatherStats(CRuntimeStatisticCollection & merged)
        {
            CriticalBlock block(statsCs);
            CDiskPartHandlerBase::gatherStats(merged);
            mergeStats(merged, iFileIO);
        }
    };

    unsigned &getHeaderLines(unsigned subFile)
    {
        if (headerLinesRemaining[subFile])
        {
            if (!gotHeaderLines->test(subFile))
            {
                bool gotWanted = false;
                CMessageBuffer msgMb;
                loop
                {
                    if (!receiveMsg(msgMb, queryJobChannel().queryMyRank()-1, mpTag) || 0 == msgMb.length())
                    {
                        // all [remaining] headers read, or abort
                        // may potentially zero out previously received headers, but ok
                        unsigned hL=0;
                        for (; hL<subFiles; hL++)
                        {
                            headerLinesRemaining[hL] = 0;
                            gotHeaderLines->set(hL);
                        }
                        break;
                    }
                    else
                    {
                        unsigned which;
                        loop
                        {
                            msgMb.read(which);
                            assertex(!gotHeaderLines->testSet(which));
                            msgMb.read(headerLinesRemaining[which]);
                            if (subFile == which)
                            {
                                assertex(!gotWanted);
                                gotWanted = true;
                            }
                            if (0 == msgMb.remaining())
                                break;
                        }
                        if (gotWanted)
                            break;
                    }
                }
            }
        }
        return headerLinesRemaining[subFile];
    }
    void sendAllDone()
    {
        CMessageBuffer msgMb;
        unsigned s=queryJobChannel().queryMyRank();
        while (s<container.queryJob().querySlaves())
        {
            ++s;
            queryJobChannel().queryJobComm().send(msgMb, s, mpTag);
        }
        // mark any unmarked as sent
        unsigned which = sentHeaderLines->scanInvert(0, false);
        while (which < subFiles)
            which = sentHeaderLines->scanInvert(which+1, false);
    }
    void sendHeaderLines(unsigned subFile, unsigned part)
    {
        if (0 == headerLinesRemaining[subFile])
        {
            if (sentHeaderLines->testSet(subFile))
                return;
            unsigned which = gotHeaderLines->scan(0, false);
            if (which == subFiles) // all received
            {
                bool someLeft=false;
                unsigned hL=0;
                for (; hL<subFiles; hL++)
                {
                    if (headerLinesRemaining[hL])
                    {
                        someLeft = true;
                        break;
                    }
                }
                if (!someLeft)
                {
                    sendAllDone();
                    return;
                }
            }
        }
        else
        {
            if (localLastPart[subFile] != part) // only ready to send if last local part
                return;
            if (sentHeaderLines->testSet(subFile))
                return;
        }
        CMessageBuffer msgMb;
        msgMb.append(subFile);
        msgMb.append(headerLinesRemaining[subFile]);
        queryJobChannel().queryJobComm().send(msgMb, queryJobChannel().queryMyRank()+1, mpTag);
    }
    void sendRemainingHeaderLines()
    {
        if (!headerLines)
            return;
        unsigned which = sentHeaderLines->scanInvert(0, false);
        if (which < subFiles)
        {
            CMessageBuffer msgMb;
            bool someLeft=false;
            do
            {
                msgMb.append(which);
                unsigned &remaining = getHeaderLines(which);
                if (0 != remaining)
                    someLeft = true;
                msgMb.append(remaining);
                which = sentHeaderLines->scanInvert(which+1, false);
            }
            while (which < subFiles);
            if (someLeft)
                queryJobChannel().queryJobComm().send(msgMb, queryJobChannel().queryMyRank()+1, mpTag);
            else
                sendAllDone();
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CCsvReadSlaveActivity(CGraphElementBase *_container) : CDiskReadSlaveActivityBase(_container), CThorDataLink(this)
    {
        helper = static_cast <IHThorCsvReadArg *> (queryHelper());
        stopAfter = (rowcount_t)helper->getChooseNLimit();
        if (helper->getFlags() & TDRlimitskips)
            limit = RCMAX;
        else
            limit = (rowcount_t)helper->getRowLimit();
        headerLines = helper->queryCsvParameters()->queryHeaderLen();
        superFDesc = NULL;
        subFiles = 0;
    }

// IThorSlaveActivity
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDiskReadSlaveActivityBase::init(data, slaveData);
        if (partDescs.ordinality())
        {
            bool b;
            data.read(b);
            if (b) data.read(csvQuote);
            data.read(b);
            if (b) data.read(csvSeparate);
            data.read(b);
            if (b) data.read(csvTerminate);
            data.read(b);
            if (b) data.read(csvEscape);
        }
        if (headerLines)
        {
            mpTag = container.queryJobChannel().deserializeMPTag(data);
            data.read(subFiles);
            superFDesc = partDescs.ordinality() ? partDescs.item(0).queryOwner().querySuperFileDescriptor() : NULL;
            localLastPart.allocateN(subFiles, true);
            ForEachItemIn(p, partDescs)
            {
                IPartDescriptor &partDesc = partDescs.item(p);
                unsigned subFile = 0;
                unsigned pnum = partDesc.queryPartIndex();
                if (superFDesc)
                {
                    unsigned lnum;
                    if (!superFDesc->mapSubPart(pnum, subFile, lnum))
                        throw MakeActivityException(this, 0, "mapSubPart failed, file=%s, partnum=%d", logicalFilename.get(), pnum);
                    pnum = lnum;
                }
                if (pnum > localLastPart[subFile]) // don't think they can really be out of order
                    localLastPart[subFile] = pnum;
            }
            headerLinesRemaining.allocateN(subFiles);
            gotHeaderLines.setown(createThreadSafeBitSet());
            sentHeaderLines.setown(createThreadSafeBitSet());
        }
        partHandler.setown(new CCsvPartHandler(*this));
        appendOutputLinked(this);
    }
    virtual void kill()
    {
        out.clear();
        CDiskReadSlaveActivityBase::kill();
    }
    
// IThorDataLink
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        if (!gotMeta)
        {
            gotMeta = true;
            initMetaInfo(cachedMetaInfo);
            cachedMetaInfo.isSource = true;
            getPartsMetaInfo(cachedMetaInfo, *this, partDescs.ordinality(), partDescs.getArray(), partHandler);
            cachedMetaInfo.unknownRowsOutput = true; // at least I don't think we know
        }
        info = cachedMetaInfo;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        OwnedConstThorRow row = out->nextRow();
        if (row)
        {
            rowcount_t c = getDataLinkCount();
            if (0 == stopAfter || (c < stopAfter)) // NB: only slave limiter, global performed in chained choosen activity
            {
                if (c < limit) // NB: only slave limiter, global performed in chained limit activity
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
                helper->onLimitExceeded();
            }
        }
        sendRemainingHeaderLines();
        return NULL;
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        CDiskReadSlaveActivityBase::start();
        if (headerLines)
        {
            bool noSend = container.queryLocal() || lastNode();
            bool got = container.queryLocal() || firstNode();
            sentHeaderLines->reset();
            gotHeaderLines->reset();
            unsigned hL=0;
            for (; hL<subFiles; hL++)
            {
                headerLinesRemaining[hL] = headerLines;
                if (got)
                    gotHeaderLines->set(hL);
                if (noSend)
                    sentHeaderLines->set(hL);
            }
        }
        out.setown(createSequentialPartHandler(partHandler, partDescs, false));
        dataLinkStart();
    }
    virtual void stop()
    {
        sendRemainingHeaderLines();
        out.clear();
        dataLinkStop();
    }
    void abort()
    {
        CDiskReadSlaveActivityBase::abort();
        cancelReceiveMsg(queryJobChannel().queryMyRank()-1, mpTag);
    }
    virtual bool isGrouped() { return false; }

friend class CCsvPartHandler;
};

//---------------------------------------------------------------------------

CActivityBase *createCsvReadSlave(CGraphElementBase *container)
{
    return new CCsvReadSlaveActivity(container);
}

