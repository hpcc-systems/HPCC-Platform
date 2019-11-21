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

class SpillSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

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
    SpillSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        compress = false;
        grouped = false;
        usageCount = 0;
        appendOutputLinked(this);
    }

    ~SpillSlaveActivity()
    {
        close();
    }

    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        partDesc.setown(deserializePartFileDescriptor(data));
        compress = partDesc->queryOwner().isCompressed();
        data.read(usageCount);
        getPartFilename(*partDesc, 0, fileName);
        grouped = 0 != (TDXgrouped & ((IHThorSpillArg *)queryHelper())->getFlags());
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
        void *ekey;
        size32_t ekeylen;
        helper->getEncryptKey(ekeylen,ekey);
        Owned<ICompressor> ecomp;
        if (ekeylen!=0)
        {
            ecomp.setown(createAESCompressor256(ekeylen,ekey));
            memset(ekey,0,ekeylen);
            free(ekey);
            compress = true;
        }
        Owned<IFile> file = createIFile(fileName.str());
        Owned<IFileIO> iFileIO;
        bool fixedRecordSize = queryRowMetaData()->isFixedSize();
        size32_t minrecsize = queryRowMetaData()->getMinRecordSize();

        if (fixedRecordSize)
            ActPrintLog("SPILL: created fixed output %s recsize=%u", (0!=ekeylen)?"[encrypted]":compress?"[compressed]":"",minrecsize);
        else
            ActPrintLog("SPILL: created variable output %s, minrecsize=%u", (0!=ekeylen)?"[encrypted]":compress?"[compressed]":"",minrecsize);
        unsigned rwFlags = (DEFAULT_RWFLAGS & ~rw_autoflush); // flushed by close()
        if (compress)
            rwFlags |= rw_compress;
        else
            rwFlags |= rw_crc; // only if !compress
        if (grouped)
            rwFlags |= rw_grouped;
        out.setown(createRowWriter(file, this, rwFlags));
    }

    void close()
    {
        if (out)
        {
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
        for (;;) {
            OwnedConstThorRow row = ungroupedNextRow();
            if (!row)
                break;
        }
    }

    void processDone(MemoryBuffer &mb)
    {
        Owned<IFile> ifile = createIFile(fileName.str());
        offset_t sz = ifile->size();
        if ((offset_t)-1 != sz)
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
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        uncompressedBytesWritten = 0;
        if (!container.queryJob().queryUseCheckpoints())
            container.queryTempHandler()->registerFile(fileName.str(), container.queryOwner().queryGraphId(), usageCount, true);
        
        open();
        hadrow = false;
    }
    virtual void stop()
    {
        if (hasStarted())
        {
            readRest();
            close();
        }
        PARENT::stop();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (abortSoon) 
            return NULL;

        OwnedConstThorRow row = grouped?inputStream->nextRow():inputStream->ungroupedNextRow();
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

    virtual bool isGrouped() const override { return grouped; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.fastThrough = true; // ish
        calcMetaInfoSize(info, queryInput(0));
    }

};

activityslaves_decl CActivityBase *createSpillSlave(CGraphElementBase *container)
{
    return new SpillSlaveActivity(container);
}

