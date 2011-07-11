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

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/keypatch/thkeypatchslave.cpp $ $Id: thkeypatchslave.cpp 64773 2011-05-20 13:47:42Z jsmith $");

class CKeyPatchSlave : public ProcessSlaveActivity
{
    IHThorKeyPatchArg *helper;
    Owned<IPartDescriptor> originalIndexPart, newIndexPart, patchPart;
    Owned<IPartDescriptor> originalIndexTlkPart, newIndexTlkPart, patchTlkPart;
    Owned<IKeyDiffApplicator> patchApplictor, tlkPatchApplicator;
    bool tlk, copyTlk;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CKeyPatchSlave(CGraphElementBase *container) : ProcessSlaveActivity(container)
    {
        helper = NULL;
        tlk = copyTlk = false;
    }
    ~CKeyPatchSlave()
    {
    }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorKeyPatchArg *)queryHelper();
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
        if (1 == container.queryJob().queryMyRank())
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

        StringBuffer originalFilePart, patchFilePart;
        locateFilePartPath(this, helper->queryOriginalName(), *originalIndexPart, originalFilePart);
        locateFilePartPath(this, helper->queryPatchName(), *patchPart, patchFilePart);

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
                locateFilePartPath(this, tmp.clear().append(helper->queryOriginalName()).append(" [TLK]").str(), *originalIndexTlkPart, originalFilePart);
                locateFilePartPath(this, tmp.clear().append(helper->queryPatchName()).append(" [TLK]").str(), *patchTlkPart, patchFilePart);
                getPartFilename(*newIndexTlkPart, 0, tmp.clear());
                tlkPatchApplicator.setown(createKeyDiffApplicator(patchFilePart.str(), originalFilePart.str(), tmp.str(), NULL, true, true));
            }
        }
    }
    virtual void process()
    {
        processed = THORDATALINK_STARTED;
        if (abortSoon) return;
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
                    locateFilePartPath(this, tmp.append(helper->queryPatchName()).append(" [TLK]").str(), *patchTlkPart, patchFilePathTlk);
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
        if (-1 != sz)
            container.queryJob().queryIDiskUsage().increase(sz);
        mb.append(sz);

        CDateTime createTime, modifiedTime, accessedTime;
        ifile->getTime(&createTime, &modifiedTime, &accessedTime);
        // round file time down to nearest sec. Nanosec accurancy is not preserved elsewhere and can lead to mismatch later.
        unsigned hour, min, sec, nanosec;
        modifiedTime.getTime(hour, min, sec, nanosec);
        modifiedTime.setTime(hour, min, sec, 0);
        modifiedTime.serialize(mb);

        if (tlk)
        {
            StringBuffer filePath;
            Owned<IFile> ifile = createIFile(getPartFilename(*newIndexTlkPart, 0, filePath).str());
            offset_t sz = ifile->size();
            if (-1 != sz)
                container.queryJob().queryIDiskUsage().increase(sz);
            mb.append(sz);

            CDateTime createTime, modifiedTime, accessedTime;
            ifile->getTime(&createTime, &modifiedTime, &accessedTime);
            // round file time down to nearest sec. Nanosec accurancy is not preserved elsewhere and can lead to mismatch later.
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

