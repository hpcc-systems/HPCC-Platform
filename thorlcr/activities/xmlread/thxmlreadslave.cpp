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
#include <limits.h>
#include "jio.hpp"
#include "jlzw.hpp"
#include "jsort.hpp"
#include "eclhelper.hpp"
#include "slave.ipp"

#include "thexception.hpp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"
#include "thmfilemanager.hpp"
#include "thbufdef.hpp"
#include "thsortu.hpp"
#include "thorxmlread.hpp"
#include "thdiskbaseslave.ipp"

class CXmlReadSlaveActivity : public CDiskReadSlaveActivityBase, public CThorDataLink
{
    IHThorXmlReadArg *helper;
    IRowStream *out;
    rowcount_t limit;
    rowcount_t stopAfter;

    class CXmlPartHandler : public CDiskPartHandlerBase, implements IXMLSelect
    {
        CXmlReadSlaveActivity &activity;
        IXmlToRowTransformer *xmlTransformer;
        Linked<IColumnProvider> lastMatch;
        Owned<ICrcIOStream> crcStream;
        Owned<IXMLParse> xmlParser;
        CRC32 inputCRC;
        Owned<IIOStream> inputIOstream;
        offset_t localOffset;  // not sure what this is for 
        Linked<IEngineRowAllocator> allocator;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CXmlPartHandler(CXmlReadSlaveActivity &_activity, IEngineRowAllocator *_allocator) 
            : CDiskPartHandlerBase(_activity), activity(_activity), allocator(_allocator)
        {
            xmlTransformer = activity.helper->queryTransformer();
            localOffset = 0;
        }

        virtual void open() 
        {
            CDiskPartHandlerBase::open();
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
            Owned<IIOStream> stream = createIOStream(iFileIO);
            if (stream && checkFileCrc)
            {
                crcStream.setown(createCrcPipeStream(stream));
                stream.set(crcStream);
            }
            inputIOstream.setown(createBufferedIOStream(stream));
            xmlParser.setown(createXMLParse(*inputIOstream.get(), activity.helper->queryIteratorPath(), *this, (0 != (TDRxmlnoroot & activity.helper->getFlags()))?xr_noRoot:xr_none, 0 != (TDRusexmlcontents & activity.helper->getFlags())));
        }
        virtual void close(CRC32 &fileCRC)
        {
            xmlParser.clear();
            inputIOstream.clear();
            if (checkFileCrc)
                fileCRC.reset(~crcStream->queryCrc()); // MORE should prob. change stream to use CRC32
        }

        const void *nextRow()
        {
            if (eoi || activity.abortSoon)
                return false;

            try
            {
                while (xmlParser->next()) {
                    if (lastMatch)
                    {
                        RtlDynamicRowBuilder row(allocator);
                        size32_t sz = xmlTransformer->transform(row, lastMatch, this);
                        lastMatch.clear();
                        if (sz) {
                            localOffset = 0;
                            ++progress;
                            return row.finalizeRowClear(sz);
                        }
                    }
                }
            }
            catch (IXMLReadException *e)
            {
                if (XmlRead_syntax != e->errorCode())
                    throw;
                Owned<IException> _e = e;
                offset_t localFPos = makeLocalFposOffset(activity.queryContainer().queryJob().queryMyRank()-1, e->queryOffset());
                StringBuffer context;
                context.append("Logical filename = ").append(activity.logicalFilename).newline();
                context.append("Local fileposition = ");
                _WINREV8(localFPos);
                context.append("0x");
                appendDataAsHex(context, sizeof(localFPos), &localFPos);
                context.newline();
                context.append(e->queryContext());
                throw createXmlReadException(e->errorCode(), e->queryDescription(), context.str(), e->queryLine(), e->queryOffset());
            }
            catch (IOutOfMemException *e)
            {
                StringBuffer s("XMLRead actId(");
                s.append(activity.queryContainer().queryId()).append(") out of memory.").newline();
                s.append("INTERNAL ERROR ").append(e->errorCode());
                Owned<IException> e2 = MakeActivityException(&activity, e, "%s", s.str());
                e->Release();
                throw e2.getClear();
            }
            catch (IException *e)
            {
                StringBuffer s("XMLRead actId(");
                s.append(activity.queryContainer().queryId());
                s.append(") INTERNAL ERROR ").append(e->errorCode());
                Owned<IException> e2 = MakeActivityException(&activity, e, "%s", s.str());
                e->Release();
                throw e2.getClear();
            }
            eoi = true;
            return NULL;
        }

// IXMLSelect
        virtual void match(IColumnProvider &entry, offset_t startOffset, offset_t endOffset)
        {
            localOffset = startOffset;
            lastMatch.set(&entry);
        }

        offset_t getLocalOffset()
        {
            return localOffset; // NH->JCS is this what is wanted? (or should it be stream position relative?
        }
    
    };
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CXmlReadSlaveActivity(CGraphElementBase *_container) : CDiskReadSlaveActivityBase(_container), CThorDataLink(this)
    {
        out = NULL;
        helper = (IHThorXmlReadArg *)queryHelper();
        stopAfter = (rowcount_t)helper->getChooseNLimit();
        if (helper->getFlags() & TDRlimitskips)
            limit = RCMAX;
        else
            limit = (rowcount_t)helper->getRowLimit();  
    }
    ~CXmlReadSlaveActivity()
    {
        ::Release(out);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDiskReadSlaveActivityBase::init(data, slaveData);
        partHandler.setown(new CXmlPartHandler(*this,queryRowAllocator()));
        appendOutputLinked(this);
    }
    virtual void kill()
    {
        if (out)
        {
            out->Release();
            out = NULL;
        }
        CDiskReadSlaveActivityBase::kill();
    }
    
// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        CDiskReadSlaveActivityBase::start();
        out = createSequentialPartHandler(partHandler, partDescs, false);
        dataLinkStart("XMLREAD", container.queryId());
    }
    virtual void stop()
    {
        if (out)
        {
            out->Release();
            out = NULL;
        }
        dataLinkStop();
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
    
    virtual bool isGrouped() { return false; }
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
friend class CXmlPartHandler;
};

CActivityBase *createXmlReadSlave(CGraphElementBase *container)
{
    return new CXmlReadSlaveActivity(container);
}

