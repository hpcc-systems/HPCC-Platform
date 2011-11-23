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



#include "jiface.hpp"
#include "jtime.hpp"
#include "jfile.ipp"

#include "keydiff.hpp"

#include "backup.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "thbuf.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"

class CKeyDiffSlave : public ProcessSlaveActivity
{
    IHThorKeyDiffArg *helper;
    Owned<IPartDescriptor> originalIndexPart, updatedIndexPart, patchPart;
    Owned<IPartDescriptor> originalIndexTlkPart, updatedIndexTlkPart, patchTlkPart;
    Owned<IKeyDiffGenerator> diffGenerator, tlkDiffGenerator;
    bool tlk, copyTlk;
    unsigned patchCrc, tlkPatchCrc;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CKeyDiffSlave(CGraphElementBase *container) : ProcessSlaveActivity(container)
    {
        helper = NULL;
        tlk = false;
        copyTlk = globals->getPropBool("@diffCopyTlk", true); // because tlk can have meta data and diff/patch does not support
    }
    ~CKeyDiffSlave()
    {
    }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorKeyDiffArg *)queryHelper();
        bool active;
        data.read(active);
        if (!active)
        {
            abortSoon = true;
            return;
        }
        originalIndexPart.setown(deserializePartFileDescriptor(data));
        updatedIndexPart.setown(deserializePartFileDescriptor(data));
        patchPart.setown(deserializePartFileDescriptor(data));
        if (firstNode())
        {
            data.read(tlk);
            if (tlk)
            {
                originalIndexTlkPart.setown(deserializePartFileDescriptor(data));
                updatedIndexTlkPart.setown(deserializePartFileDescriptor(data));
                patchTlkPart.setown(deserializePartFileDescriptor(data));
            }
        }

        StringBuffer originalFilePart, updatedFilePart;
        locateFilePartPath(this, helper->queryOriginalName(), *originalIndexPart, originalFilePart);
        locateFilePartPath(this, helper->queryUpdatedName(), *updatedIndexPart, updatedFilePart);
        StringBuffer patchFilePath;
        getPartFilename(*patchPart, 0, patchFilePath);
        if (globals->getPropBool("@replicateAsync", true))
            cancelReplicates(this, *patchPart);
        ensureDirectoryForFile(patchFilePath.str());
        diffGenerator.setown(createKeyDiffGenerator(originalFilePart.str(), updatedFilePart.str(), patchFilePath.str(), 0, true, COMPRESS_METHOD_LZMA));

        ActPrintLog("KEYPATCH: handling original index = %s, updated index = %s, output patch = %s", originalFilePart.str(), updatedFilePart.str(), patchFilePath.str());
        if (tlk)
        {
            if (globals->getPropBool("@replicateAsync", true))
                cancelReplicates(this, *patchTlkPart);
            if (!copyTlk)
            {
                StringBuffer tmp;
                locateFilePartPath(this, tmp.clear().append(helper->queryOriginalName()).append(" [TLK]").str(), *originalIndexTlkPart, originalFilePart.clear());
                locateFilePartPath(this, tmp.clear().append(helper->queryUpdatedName()).append(" [TLK]").str(), *updatedIndexTlkPart, updatedFilePart.clear());
                getPartFilename(*patchTlkPart, 0, tmp.clear());
                tlkDiffGenerator.setown(createKeyDiffGenerator(originalFilePart.str(), updatedFilePart.str(), tmp.str(), 0, true, COMPRESS_METHOD_LZMA));
            }
        }
    }
    virtual void process()
    {
        processed = THORDATALINK_STARTED;
        if (abortSoon) return;
        try
        {
            diffGenerator->run();
            diffGenerator->logStats(); // JCSMORE - may want some into svg graph.
            patchCrc = diffGenerator->queryPatchFileCRC();
            diffGenerator.clear();
            if (tlk && !copyTlk)
            {
                tlkDiffGenerator->run();
                tlkDiffGenerator->logStats();
                tlkPatchCrc = tlkDiffGenerator->queryPatchFileCRC();
                tlkDiffGenerator.clear();
            }
            try
            {
                if (patchPart->numCopies() > 1)
                    doReplicate(this, *patchPart);
                if (tlk && copyTlk)
                {
                    StringBuffer patchFilePathTlk, updatedFilePartTlk, tmp;
                    getPartFilename(*patchTlkPart, 0, patchFilePathTlk);
                    locateFilePartPath(this, tmp.append(helper->queryUpdatedName()).append(" [TLK]").str(), *updatedIndexTlkPart, updatedFilePartTlk);
                    OwnedIFile dstIFileTlk = createIFile(patchFilePathTlk.str());
                    OwnedIFile updatedIFileTlk = createIFile(updatedFilePartTlk.str());
                    copyFile(dstIFileTlk, updatedIFileTlk);
                    if (patchTlkPart->numCopies() > 1)
                        doReplicate(this, *patchTlkPart);
                }
            }
            catch (IException *e)
            {
                ActPrintLog(e, "Failure to create backup patch files");
                throw;
            }
        }
        catch (IException *e)
        {
            ActPrintLog(e, "KEYPATH exception");
            throw;
        }
    }

    virtual void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
            processed |= THORDATALINK_STOPPED;

    }
    virtual void processDone(MemoryBuffer &mb)
    {
        if (abortSoon)
            return;
        StringBuffer tmpStr;
        Owned<IFile> ifile = createIFile(getPartFilename(*patchPart, 0, tmpStr).str());
        offset_t sz = ifile->size();
        if (-1 != sz)
            container.queryJob().queryIDiskUsage().increase(sz);
        mb.append(sz);

        CDateTime createTime, modifiedTime, accessedTime;
        ifile->getTime(&createTime, &modifiedTime, &accessedTime);
        // round file time down to nearest sec. Nanosec accuracy is not preserved elsewhere and can lead to mismatch later.
        unsigned hour, min, sec, nanosec;
        modifiedTime.getTime(hour, min, sec, nanosec);
        modifiedTime.setTime(hour, min, sec, 0);
        modifiedTime.serialize(mb);
        mb.append(patchCrc);

        if (tlk)
        {
            Owned<IFile> ifile = createIFile(getPartFilename(*patchTlkPart, 0, tmpStr.clear()).str());
            offset_t sz = ifile->size();
            if (-1 != sz)
                container.queryJob().queryIDiskUsage().increase(sz);
            mb.append(sz);

            CDateTime createTime, modifiedTime, accessedTime;
            ifile->getTime(&createTime, &modifiedTime, &accessedTime);
            // round file time down to nearest sec. Nanosec accuracy is not preserved elsewhere and can lead to mismatch later.
            unsigned hour, min, sec, nanosec;
            modifiedTime.getTime(hour, min, sec, nanosec);
            modifiedTime.setTime(hour, min, sec, 0);
            modifiedTime.serialize(mb);
            if (!copyTlk)
                mb.append(tlkPatchCrc);
        }
    }
};

activityslaves_decl CActivityBase *createKeyDiffSlave(CGraphElementBase *container)
{
    return new CKeyDiffSlave(container);
}

