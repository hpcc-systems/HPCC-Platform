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

#include "platform.h"

#include "jio.hpp"
#include "jio.hpp"
#include "jlzw.hpp"
#include "jtime.hpp"
#include "jsort.hpp"


#include "thormisc.hpp"
#include "thmfilemanager.hpp"
#include "thorport.hpp"
#include "thcrc.hpp"
#include "thexception.hpp"
#include "thsortu.hpp"
#include "thbufdef.hpp"
#include "thactivityutil.ipp"
#include "csvsplitter.hpp"
#include "thdiskbaseslave.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/csvread/thcsvrslave.cpp $ $Id: thcsvrslave.cpp 65710 2011-06-23 13:22:19Z ghalliday $");


class CCsvReadSlaveActivity : public CDiskReadSlaveActivityBase, public CThorDataLink
{
    IHThorCsvReadArg *helper;
    StringAttr csvQuote, csvSeperate, csvTerminate;
    Owned<IRowStream> out;
    rowcount_t limit;
    rowcount_t stopAfter;

    class CCsvPartHandler : public CDiskPartHandlerBase
    {
        Linked<IEngineRowAllocator> allocator;
        CCsvReadSlaveActivity &activity;
        Owned<ISerialStream> inputStream;
        CSVSplitter csvSplitter;
        CRC32 inputCRC;
        bool readFinished;
        offset_t localOffset;

        unsigned splitLine()
        {
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
                    throw MakeActivityException(&activity, 0, "File %s contained a line of length greater than %d bytes.", activity.helper->getFileName(), minRequired);
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
            //Initialise information...
            ICsvParameters * csvInfo = activity.helper->queryCsvParameters();
            csvSplitter.init(activity.helper->getMaxColumns(), csvInfo, activity.csvQuote, activity.csvSeperate, activity.csvTerminate);
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
            OwnedIFileIO iFileIO;
            if (compressed)
            {
                iFileIO.setown(createCompressedFileReader(iFile, activity.eexp));
                if (!iFileIO)
                    throw MakeActivityException(&activity, 0, "Failed to open block compressed file '%s'", filename.get());
                checkFileCrc = false;
            }
            else
                iFileIO.setown(iFile->open(IFOread));

            inputStream.setown(createFileSerialStream(iFileIO));
            unsigned pnum = partDesc->queryPartIndex();
            ISuperFileDescriptor *superFDesc = partDesc->queryOwner().querySuperFileDescriptor();
            if (superFDesc)
            {
                unsigned subfile;
                unsigned lnum;
                if (superFDesc->mapSubPart(pnum, subfile, lnum))
                    pnum = lnum;
                else
                {
                    IThorException *e = MakeActivityWarning(&activity, 0, "mapSubPart failed, file=%s, partnum=%d", activity.logicalFilename.get(), pnum);
                    EXCLOG(e, NULL);
                }
            }
            if (0==pnum)
            {
                //Skip header lines.... but only on the first part.
                unsigned lines = activity.helper->queryCsvParameters()->queryHeaderLen();
                while (lines--)
                {
                    unsigned lineLength = splitLine();
                    if (0 == lineLength)
                        break;
                    inputStream->skip(lineLength);
                }
            }
        }
        virtual void close(CRC32 &fileCRC)
        {
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
                    ++progress;
                    return row.finalizeRowClear(res);
                }
            }
        }

        offset_t getLocalOffset() { return localOffset; }


    };

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
            if (b) data.read(csvSeperate);
            data.read(b);
            if (b) data.read(csvTerminate);
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
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow row = out->nextRow();
        if (!row)
            return NULL;
        rowcount_t c = getDataLinkCount();
        if (stopAfter && (c >= stopAfter)) // NB: only slave limiter, global performed in chained choosen activity 
            return NULL;
        if (c >= limit) // NB: only slave limiter, global performed in chained limit activity
        {
            helper->onLimitExceeded();
            return NULL;
        }
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        out.setown(createSequentialPartHandler(partHandler, partDescs, false));
        dataLinkStart("CCsvReadSlaveActivity", container.queryId());
    }
    virtual void stop()
    {
        out.clear();
        dataLinkStop();
    }
    virtual bool isGrouped() { return false; }

friend class CCsvPartHandler;
};

//---------------------------------------------------------------------------

CActivityBase *createCsvReadSlave(CGraphElementBase *container)
{
    return new CCsvReadSlaveActivity(container);
}

