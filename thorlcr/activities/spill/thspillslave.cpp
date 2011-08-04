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


#include "jio.hpp"
#include "jbuff.hpp"
#include "jlzw.hpp"
#include "jtime.hpp"
#include "dadfs.hpp"
#include "thbuf.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thspillslave.ipp"

class SpillSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IThorDataLink *input;
    StringBuffer fileName;
    Owned<IPartDescriptor> partDesc;
    Owned<IExtRowWriter> out;
    bool compress;
    bool grouped;
    MemoryBuffer spillBuf;
    offset_t uncompressedBytesWritten;
    bool hadrow;
    CRC32 fileCRC;
    unsigned usageCount;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    SpillSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        compress = false;
        grouped = false;
        usageCount = 0;
    }

    ~SpillSlaveActivity()
    {
        close();
    }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        partDesc.setown(deserializePartFileDescriptor(data));
        compress = partDesc->queryOwner().isCompressed();
        data.read(usageCount);
        getPartFilename(*partDesc, 0, fileName);
        grouped = 0 != (TDXgrouped & ((IHThorSpillArg *)queryHelper())->getFlags());
        appendOutputLinked(this);
    }

    void open()
    {
        char drive       [_MAX_DRIVE];
        char dir         [_MAX_DIR];
        char fname       [_MAX_DIR];
        char ext         [_MAX_EXT];
        _splitpath(fileName.str(), drive, dir, fname, ext);

        StringBuffer directory;
        directory.append(drive).append(dir);

        Owned<IFile> cd = createIFile(directory.str());
        cd->createDirectory();

        IHThorSpillArg *helper = (IHThorSpillArg *)queryHelper();
        Owned<IRecordSize> rSz;
        if (!grouped)
            rSz.set(helper->queryDiskRecordSize());
        else
        {
            class GroupedRecordSize : public CSimpleInterface, implements IRecordSize
            {
                IRecordSize *rSz;
            public:
                IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
                GroupedRecordSize(IRecordSize *_rSz) { rSz = LINK(_rSz); }
                ~GroupedRecordSize() { ::Release(rSz); }
                virtual size32_t getRecordSize(const void *rec) { return rSz->getRecordSize(rec) + 1; }
                virtual size32_t getFixedSize() const { return rSz->getFixedSize()?(rSz->getFixedSize()+1):0; }
            };
            rSz.setown(new GroupedRecordSize(helper->queryDiskRecordSize()));
        }

        void *ekey;
        size32_t ekeylen;
        helper->getEncryptKey(ekeylen,ekey);
        Owned<ICompressor> ecomp;
        if (ekeylen!=0) {
            ecomp.setown(createAESCompressor256(ekeylen,ekey));
            memset(ekey,0,ekeylen);
            free(ekey);
            compress = true;
        }
        Owned<IFile> file = createIFile(fileName.str());
        Owned<IFileIO> iFileIO;
        bool fixedRecordSize = queryRowMetaData()->isFixedSize();
        size32_t minrecsize = queryRowMetaData()->getMinRecordSize();
        if (compress)
            iFileIO.setown(createCompressedFileWriter(file, fixedRecordSize?(minrecsize+(grouped?sizeof(byte):0)):0, false, true, ecomp));
        else
            iFileIO.setown(file->open(IFOcreate));
        if (!iFileIO)
            throw MakeActivityException(this, 0, "Failed to create temporary file: %s", fileName.str());
        if (fixedRecordSize)
            ActPrintLog("SPILL: created fixed output %s recsize=%u", (0!=ekeylen)?"[encrypted]":compress?"[compressed]":"",minrecsize);
        else
            ActPrintLog("SPILL: created variable output %s, minrecsize=%u", (0!=ekeylen)?"[encrypted]":compress?"[compressed]":"",minrecsize);
        Owned<IFileIOStream> filestrm = createBufferedIOStream(iFileIO);
        out.setown(createRowWriter(filestrm,queryRowSerializer(),queryRowAllocator(),grouped,!compress,false)); 
    }

    void close()
    {
        if (out) {
            if (compress)
                out->flush();
            else
                out->flush(&fileCRC);
            uncompressedBytesWritten = out->getPosition();
            out.clear();
        }
    }

    void readRest()
    {
        loop {
            OwnedConstThorRow row = ungroupedNextRow();
            if (!row)
                break;
        }
    }

    void processDone(MemoryBuffer &mb)
    {
        Owned<IFile> ifile = createIFile(fileName.str());
        offset_t sz = ifile->size();
        if (-1 != sz)
            container.queryJob().queryIDiskUsage().increase(sz);        
        mb.append(getDataLinkCount()).append(compress?uncompressedBytesWritten:sz).append(sz);
        unsigned crc = compress?~0:fileCRC.get();
        mb.append(crc);
        CDateTime createTime, modifiedTime, accessedTime;
        ifile->getTime(&createTime, &modifiedTime, &accessedTime);
        // round file time down to nearest sec. Nanosec accuracy is not preserved elsewhere and can lead to mismatch later.
        unsigned hour, min, sec, nanosec;
        modifiedTime.getTime(hour, min, sec, nanosec);
        modifiedTime.setTime(hour, min, sec, 0);
        modifiedTime.serialize(mb);
    }

// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        uncompressedBytesWritten = 0;
        if (!container.queryJob().queryUseCheckpoints())
            container.queryTempHandler()->registerFile(fileName.str(), container.queryOwner().queryGraphId(), usageCount, true);
        input = inputs.item(0);
        startInput(input);
        
        dataLinkStart("SPILL", container.queryId());

        open();
        hadrow = false;
    }
    virtual void stop()
    {
        readRest();
        close();
        stopInput(input);
        dataLinkStop();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (abortSoon) 
            return NULL;

        OwnedConstThorRow row = grouped?input->nextRow():input->ungroupedNextRow();
        if (row) {
            hadrow = true;
            dataLinkIncrement();
            out->putRow(row.getLink());
            return row.getClear();
        }
        if (grouped&&hadrow)
            out->putRow(NULL);
        hadrow = false;
        return NULL;
    }

    virtual bool isGrouped() { return grouped; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true; // ish
        calcMetaInfoSize(info,inputs.item(0));
    }

};

activityslaves_decl CActivityBase *createSpillSlave(CGraphElementBase *container)
{
    return new SpillSlaveActivity(container);
}

