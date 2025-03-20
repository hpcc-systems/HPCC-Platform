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

#include "jliball.hpp"

#include "platform.h"

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"

#include "fterror.hpp"
#include "dadfs.hpp"
#include "filecopy.ipp"
#include "jptree.hpp"
#include "dalienv.hpp"

#include "daft.ipp"
#include "daftmc.hpp"

CDistributedFileSystem::CDistributedFileSystem()
{
}

typedef Owned<IFileSprayer> OwnedIFileSprayer;
//-- operations on multiple files. --

void CDistributedFileSystem::copy(IDistributedFile * from, IDistributedFile * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid)
{
    if (to->queryLogicalName())
        DBGLOG("DFS: copy(%s,%s)", from->queryLogicalName(), to->queryLogicalName());
    else
        DBGLOG("DFS: copy(%s)", from->queryLogicalName());

    OwnedIFileSprayer sprayer = createFileSprayer(options, recovery, recoveryConnection, wuid);

    sprayer->setOperation(dfu_copy);
    sprayer->setProgress(progress);
    sprayer->setAbort(abort);
    sprayer->setPartFilter(filter);
    sprayer->setSource(from);
    sprayer->setTarget(to);
    sprayer->spray();
}

void CDistributedFileSystem::exportFile(IDistributedFile * from, IFileDescriptor * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid)
{
    StringBuffer temp;
    DBGLOG("DFS: export(%s,%s)", from->queryLogicalName(), to->getTraceName(temp).str());

    OwnedIFileSprayer sprayer = createFileSprayer(options, recovery, recoveryConnection, wuid);
    sprayer->setOperation(dfu_export);
    sprayer->setProgress(progress);
    sprayer->setAbort(abort);
    sprayer->setPartFilter(filter);
    sprayer->setSource(from);
    sprayer->setTarget(to);
    sprayer->spray();
}

void CDistributedFileSystem::import(IFileDescriptor * from, IDistributedFile * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid)
{
    StringBuffer temp;
    if (to->queryLogicalName())
        DBGLOG("DFS: import(%s,%s)", from->getTraceName(temp).str(), to->queryLogicalName());
    else
        DBGLOG("DFS: import(%s)", from->getTraceName(temp).str());

    OwnedIFileSprayer sprayer = createFileSprayer(options, recovery, recoveryConnection, wuid);
    sprayer->setOperation(dfu_import);
    sprayer->setProgress(progress);
    sprayer->setAbort(abort);
    sprayer->setPartFilter(filter);
    sprayer->setSource(from);
    sprayer->setTarget(to);
    sprayer->spray();
}

void CDistributedFileSystem::move(IDistributedFile * from, IDistributedFile * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid)
{
    if (to->queryLogicalName())
        DBGLOG("DFS: move(%s,%s)", from->queryLogicalName(), to->queryLogicalName());
    else
        DBGLOG("DFS: move(%s)", from->queryLogicalName());

    OwnedIFileSprayer sprayer = createFileSprayer(options, recovery, recoveryConnection, wuid);
    sprayer->setOperation(dfu_move);
    sprayer->setProgress(progress);
    sprayer->setAbort(abort);
    sprayer->setPartFilter(filter);
    sprayer->setSource(from);
    sprayer->setTarget(to);
    sprayer->spray();
    //sprayer->removeSource();
    from->detach();
}

void CDistributedFileSystem::replicate(IDistributedFile * from, IGroup *destgroup, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid)
{
    DBGLOG("DFS: replicate(%s)", from->queryLogicalName());

    FileSprayer sprayer(options, recovery, recoveryConnection, wuid);
    sprayer.setOperation(dfu_replicate_distributed);
    sprayer.setProgress(progress);
    sprayer.setAbort(abort);
    sprayer.setReplicate(true);
    sprayer.setPartFilter(filter);
    sprayer.setSource(from);
    sprayer.setTarget(destgroup);
    sprayer.spray();
}

void CDistributedFileSystem::replicate(IFileDescriptor * fd, DaftReplicateMode mode, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid)
{
    StringBuffer s;
    DBGLOG("DFS: replicate(%s, %x)", fd->getTraceName(s).str(), (unsigned)mode);

    FileSprayer sprayer(options, recovery, recoveryConnection, wuid);
    sprayer.setOperation(dfu_replicate);
    sprayer.setProgress(progress);
    sprayer.setAbort(abort);
    sprayer.setReplicate(true);
    sprayer.setPartFilter(filter);
    sprayer.setSourceTarget(fd, mode);
    sprayer.spray();
}

void CDistributedFileSystem::transfer(IFileDescriptor * from, IFileDescriptor * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid)
{
    StringBuffer s1, s2;
    DBGLOG("DFS: transfer(%s,%s)", from->getTraceName(s1).str(), to->getTraceName(s1).str());

    OwnedIFileSprayer sprayer = createFileSprayer(options, recovery, recoveryConnection, wuid);
    sprayer->setOperation(dfu_transfer);
    sprayer->setAbort(abort);
    sprayer->setProgress(progress);
    sprayer->setPartFilter(filter);
    sprayer->setSource(from);
    sprayer->setTarget(to);
    sprayer->spray();
}

//-- operations on a single file. --

offset_t CDistributedFileSystem::getSize(IDistributedFile * file, bool forceget, bool dontsetattr)
{
    //MORE: Should this be done on multiple threads??? (NH: probably)
    offset_t totalSize = forceget?-1:file->queryAttributes().getPropInt64("@size", unknownFileSize);
    if (totalSize == unknownFileSize) {
        unsigned numParts = file->numParts();
        totalSize = 0;
        for (unsigned idx=0; idx < numParts; idx++)
        {
            Owned<IDistributedFilePart> part = file->getPart(idx);
            offset_t partSize = getSize(part,forceget,dontsetattr);
            if (partSize == unknownFileSize)
            {
                totalSize = unknownFileSize;
                break;
            }
            totalSize += partSize;
        }
        if (((totalSize != unknownFileSize)||forceget) && !dontsetattr) // note forceget && !dontsetattr will reset attr if can't work out size
        {
            DistributedFilePropertyLock lock(file);
            lock.queryAttributes().setPropInt64("@size", totalSize);
        }
    }
    //LOG(MCdebugInfo(1000), "DFS: getSize(%s)=%" I64F "d", file->queryLogicalName(), totalSize);
    return totalSize;
}

bool CDistributedFileSystem::compress(IDistributedFile * file)
{
    bool ok = true;
    unsigned numParts = file->numParts();
    for (unsigned idx=0; idx < numParts; idx++)
    {
        Owned<IDistributedFilePart> part = file->getPart(idx);
        ok &= compress(part);
    }
    return ok;
}

offset_t CDistributedFileSystem::getCompressedSize(IDistributedFile * file)
{
    unsigned numParts = file->numParts();
    offset_t totalSize = 0;
    for (unsigned idx=0; idx < numParts; idx++)
    {
        Owned<IDistributedFilePart> part = file->getPart(idx);
        offset_t partSize = getCompressedSize(part);
        if (partSize == (offset_t)-1)
        {
            totalSize = (offset_t)-1;
            break;
        }
        totalSize += partSize;
    }
    return totalSize;
}

//-- operations on a file part --

IFile *CDistributedFileSystem::getIFile(IDistributedFilePart * part, unsigned copy)
{
    RemoteFilename rfn;
    return createIFile(part->getFilename(rfn,copy));
}

offset_t CDistributedFileSystem::getSize(IDistributedFilePart * part, bool forceget, bool dontsetattr)
{

    offset_t size = (forceget&&!dontsetattr)?((offset_t)-1):part->getFileSize(dontsetattr,forceget); // do in one go if possible
    if (size==-1)
    {
        size = part->getFileSize(true,forceget);
        if (((size != (offset_t)-1)||forceget) && !dontsetattr) // note forceget && !dontsetattr will reset attr if can't work out size
        {
            // TODO: Create DistributedFilePropertyLock for parts
            part->lockProperties();
            part->queryAttributes().setPropInt64("@size", size);
            part->unlockProperties();
        }
    }

    //LOG(MCdebugInfo(2000), "DFS: getSize(%s)=%" I64F "d", part->queryPartName(), size);
    return size;
}

void CDistributedFileSystem::replicate(IDistributedFilePart * part, INode *node)
{
    StringBuffer partname;
    DBGLOG("DFS: replicate part(%s)", part->getPartName(partname).str());

    FileSprayer sprayer(NULL, NULL, NULL, NULL);
    sprayer.setReplicate(true);
    sprayer.setSource(part);
    sprayer.setTarget(node);
    sprayer.spray();
}

bool CDistributedFileSystem::compress(IDistributedFilePart * part)
{
    bool ok=true;
    unsigned copies = part->numCopies();
    for (unsigned copy = 0; copy < copies; copy++)
    {
        OwnedIFile file = getIFile(part, copy);
        if (!file->setCompression(true))
        {
            OERRLOG("Failed to compress file part %s", file->queryFilename());
            ok = false;
        }
    }
    return ok;
}

offset_t CDistributedFileSystem::getCompressedSize(IDistributedFilePart * part)
{
    unsigned copies = part->numCopies();
    offset_t size = (offset_t)-1;
    for (unsigned copy = 0; copy < copies; copy++)
    {
        OwnedIFile ifile = getIFile(part, copy);
        size = ifile->compressedSize();
        if (size != -1)
        {
            break;
        }
    }
    return size;
}


//---------------------------------------------------------------------------

static CDistributedFileSystem * dfs;

IDistributedFileSystem & queryDistributedFileSystem()
{
    if (!dfs) {
        dfs = new CDistributedFileSystem();
    }
    return *dfs;
}

void cleanupDistributedFileSystem()
{
    delete dfs;
    dfs = NULL;
}

struct __cleanup_daft
{
    ~__cleanup_daft()   { cleanupDistributedFileSystem(); }
} __cleanup_daft_instance;


