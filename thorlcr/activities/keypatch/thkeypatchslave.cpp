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

class CKeyPatchSlave : public ProcessSlaveActivity
{
    IHThorKeyPatchArg *helper;
    Owned<IPartDescriptor> originalIndexPart, newIndexPart, patchPart;
    Owned<IPartDescriptor> originalIndexTlkPart, newIndexTlkPart, patchTlkPart;
    Owned<IKeyDiffApplicator> patchApplictor, tlkPatchApplicator;
    bool tlk, copyTlk;

public:
    CKeyPatchSlave(CGraphElementBase *container) : ProcessSlaveActivity(container)
    {
        helper = (IHThorKeyPatchArg *)queryHelper();
        tlk = copyTlk = false;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        bool active;
        data.read(active);
        if (!active)
        {
            abortSoon = true;
            return;
        }
        originalIndexPart.setown(deserializePartFileDescriptor(data));
        patchPart.setown(deserializePartFileDescriptor(data));
        newIndexPart.setown(deserializePartFileDescriptor(data));
        if (firstNode())
        {
            data.read(tlk);
            if (tlk)
            {
                originalIndexTlkPart.setown(deserializePartFileDescriptor(data));
                patchTlkPart.setown(deserializePartFileDescriptor(data));
                newIndexTlkPart.setown(deserializePartFileDescriptor(data));
                const char *diffFormat = patchTlkPart->queryProperties().queryProp("@diffFormat");
                if (diffFormat && 0 == stricmp("copy", diffFormat))
                    copyTlk = true;
            }
        }
    }
    virtual void process()
    {
        processed = THORDATALINK_STARTED;
        if (abortSoon) return;

        StringBuffer originalFilePart, patchFilePart;
        OwnedRoxieString originalName(helper->getOriginalName());
        OwnedRoxieString patchName(helper->getPatchName());
        locateFilePartPath(this, originalName, *originalIndexPart, originalFilePart);
        locateFilePartPath(this, patchName, *patchPart, patchFilePart);

        StringBuffer newIndexFilePath;
        getPartFilename(*newIndexPart, 0, newIndexFilePath);
        if (globals->getPropBool("@replicateAsync", true))
            cancelReplicates(this, *newIndexPart);
        ensureDirectoryForFile(newIndexFilePath.str());
        patchApplictor.setown(createKeyDiffApplicator(patchFilePart.str(), originalFilePart.str(), newIndexFilePath.str(), NULL, true, true));

        ActPrintLog("KEYPATCH: handling original index = %s, patch index = %s, new index = %s", originalFilePart.str(), patchFilePart.str(), newIndexFilePath.str());
        if (tlk)
        {
            if (globals->getPropBool("@replicateAsync", true))
                cancelReplicates(this, *newIndexTlkPart);
            if (!copyTlk)
            {
                StringBuffer tmp;
                locateFilePartPath(this, tmp.clear().append(originalName).append(" [TLK]").str(), *originalIndexTlkPart, originalFilePart.clear());
                locateFilePartPath(this, tmp.clear().append(patchName).append(" [TLK]").str(), *patchTlkPart, patchFilePart.clear());
                getPartFilename(*newIndexTlkPart, 0, tmp.clear());
                tlkPatchApplicator.setown(createKeyDiffApplicator(patchFilePart.str(), originalFilePart.str(), tmp.str(), NULL, true, true));
            }
        }
        try
        {
            patchApplictor->run();
            patchApplictor.clear();
            if (tlk && !copyTlk)
            {
                tlkPatchApplicator->run();
                tlkPatchApplicator.clear();
            }
            try
            {
                if (newIndexPart->numCopies() > 1)
                    doReplicate(this, *newIndexPart);
                if (tlk && copyTlk)
                {
                    StringBuffer newFilePathTlk, patchFilePathTlk, tmp;
                    getPartFilename(*newIndexTlkPart, 0, newFilePathTlk);
                    OwnedRoxieString patchName(helper->getPatchName());
                    locateFilePartPath(this, tmp.append(patchName).append(" [TLK]").str(), *patchTlkPart, patchFilePathTlk);
                    OwnedIFile newIFileTlk = createIFile(newFilePathTlk.str());
                    OwnedIFile patchIFileTlk = createIFile(patchFilePathTlk.str());
                    copyFile(newIFileTlk, patchIFileTlk);
                    if (newIndexTlkPart->numCopies() > 1)
                        doReplicate(this, *newIndexTlkPart);
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
        StringBuffer newIndexFilePath;
        Owned<IFile> ifile = createIFile(getPartFilename(*newIndexPart, 0, newIndexFilePath).str());
        offset_t sz = ifile->size();
        if ((offset_t)-1 != sz)
            container.queryJob().queryIDiskUsage().increase(sz);
        mb.append(sz);

        CDateTime createTime, modifiedTime, accessedTime;
        ifile->getTime(&createTime, &modifiedTime, &accessedTime);
        // round file time down to nearest sec. Nanosec accuracy is not preserved elsewhere and can lead to mismatch later.
        unsigned hour, min, sec, nanosec;
        modifiedTime.getTime(hour, min, sec, nanosec);
        modifiedTime.setTime(hour, min, sec, 0);
        modifiedTime.serialize(mb);

        if (tlk)
        {
            StringBuffer filePath;
            Owned<IFile> ifile = createIFile(getPartFilename(*newIndexTlkPart, 0, filePath).str());
            offset_t sz = ifile->size();
            if ((offset_t)-1 != sz)
                container.queryJob().queryIDiskUsage().increase(sz);
            mb.append(sz);

            CDateTime createTime, modifiedTime, accessedTime;
            ifile->getTime(&createTime, &modifiedTime, &accessedTime);
            // round file time down to nearest sec. Nanosec accuracy is not preserved elsewhere and can lead to mismatch later.
            unsigned hour, min, sec, nanosec;
            modifiedTime.getTime(hour, min, sec, nanosec);
            modifiedTime.setTime(hour, min, sec, 0);
            modifiedTime.serialize(mb);

        }
    }
};

activityslaves_decl CActivityBase *createKeyPatchSlave(CGraphElementBase *container)
{
    return new CKeyPatchSlave(container);
}

