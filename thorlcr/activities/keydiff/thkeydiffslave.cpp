/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
        OwnedRoxieString origName(helper->getOriginalName());
        OwnedRoxieString updatedName(helper->getUpdatedName());
        locateFilePartPath(*this, origName, *originalIndexPart, originalFilePart);
        locateFilePartPath(*this, updatedName, *updatedIndexPart, updatedFilePart);
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
                locateFilePartPath(*this, tmp.clear().append(origName).append(" [TLK]").str(), *originalIndexTlkPart, originalFilePart.clear());
                locateFilePartPath(*this, tmp.clear().append(updatedName).append(" [TLK]").str(), *updatedIndexTlkPart, updatedFilePart.clear());
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
                    doReplicate(*this, *patchPart);
                if (tlk && copyTlk)
                {
                    StringBuffer patchFilePathTlk, updatedFilePartTlk, tmp;
                    getPartFilename(*patchTlkPart, 0, patchFilePathTlk);
                    OwnedRoxieString updatedName(helper->getUpdatedName());
                    locateFilePartPath(*this, tmp.append(updatedName).append(" [TLK]").str(), *updatedIndexTlkPart, updatedFilePartTlk);
                    OwnedIFile dstIFileTlk = createIFile(patchFilePathTlk.str());
                    OwnedIFile updatedIFileTlk = createIFile(updatedFilePartTlk.str());
                    copyFile(dstIFileTlk, updatedIFileTlk);
                    if (patchTlkPart->numCopies() > 1)
                        doReplicate(*this, *patchTlkPart);
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

