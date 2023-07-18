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

#include "thorfile.hpp"

#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield.hpp"
#include "rtlds_imp.hpp"
#include "rtldynfield.hpp"

#include "eclhelper_base.hpp"
#include "thorcommon.ipp"
#include "jhtree.hpp"

#include "keybuild.hpp"
#include "roxiemem.hpp"

void setExpiryTime(IPropertyTree & properties, unsigned expireDays)
{
    properties.setPropInt("@expireDays", expireDays);
}

void setRtlFormat(IPropertyTree & properties, IOutputMetaData * meta)
{
    if (meta && meta->queryTypeInfo())
    {
        MemoryBuffer out;
        if (dumpTypeInfo(out, meta->querySerializedDiskMeta()->queryTypeInfo()))
            properties.setPropBin("_rtlType", out.length(), out.toByteArray());
    }
}


class DiskWorkUnitReadArg : public CThorDiskReadArg
{
public:
    DiskWorkUnitReadArg(const char * _filename, IHThorWorkunitReadArg * _wuRead) : filename(_filename), wuRead(_wuRead)
    {
        recordSize.set(wuRead->queryOutputMeta());
    }
    virtual IOutputMetaData * queryOutputMeta() override
    {
        return wuRead->queryOutputMeta();
    }
    virtual const char * getFileName() override
    {
        return filename;
    }
    virtual IOutputMetaData * queryDiskRecordSize() override
    {
        return (IOutputMetaData *)recordSize;
    }
    virtual IOutputMetaData * queryProjectedDiskRecordSize() override
    {
        return (IOutputMetaData *)recordSize;
    }
    virtual unsigned getDiskFormatCrc() override
    {
        return 0;
    }
    virtual unsigned getProjectedFormatCrc() override
    {
        return 0;
    }
    virtual unsigned getFlags() override
    {
        return 0;
    }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) override
    {
        unsigned size = recordSize.getRecordSize(src);
        memcpy(rowBuilder.ensureCapacity(size, NULL), src, size);
        return size;
    }

protected:
    StringAttr filename;
    Linked<IHThorWorkunitReadArg> wuRead;
    CachedOutputMetaData recordSize;
};




IHThorDiskReadArg * createWorkUnitReadArg(const char * filename, IHThorWorkunitReadArg * wuRead)
{
    return new DiskWorkUnitReadArg(filename, wuRead);
}

//-----------------------------------------------------------------------------

#define MAX_FILE_READ_FAIL_COUNT 3

IKeyIndex *openKeyFile(IDistributedFilePart & keyFile, size32_t blockedIndexIOSize)
{
    unsigned failcount = 0;
    unsigned numCopies = keyFile.numCopies();
    assertex(numCopies);
    Owned<IException> exc;
    for (unsigned copy=0; copy < numCopies && failcount < MAX_FILE_READ_FAIL_COUNT; copy++)
    {
        RemoteFilename rfn;
        try
        {
            OwnedIFile ifile = createIFile(keyFile.getFilename(rfn,copy));
            offset_t thissize = ifile->size();
            if (thissize != (offset_t)-1)
            {
                StringBuffer remotePath;
                rfn.getPath(remotePath);
                unsigned crc = 0;
                keyFile.getCrc(crc);
                Owned<IFile> iFile = createIFile(remotePath.str());
                Owned<IFileIO> iFileIO = iFile->open(IFOread);
                if (nullptr == iFileIO)
                    throw makeStringExceptionV(0, "Failed to open index file %s", remotePath.str());
                if (blockedIndexIOSize)
                    iFileIO.setown(createBlockedIO(iFileIO.getClear(), blockedIndexIOSize));
                return createKeyIndex(remotePath.str(), crc, *iFileIO, (unsigned) -1, false);
            }
        }
        catch (IException *E)
        {
            EXCLOG(E, "While opening index file");
            if (exc)
                E->Release();
            else
                exc.setown(E);
            failcount++;
        }
    }
    if (exc)
        throw exc.getClear();
    StringBuffer url;
    RemoteFilename rfn;
    keyFile.getFilename(rfn).getRemotePath(url);
    throw MakeStringException(1001, "Could not open key file at %s%s", url.str(), (numCopies > 1) ? " or any alternate location." : ".");
}


//-----------------------------------------------------------------------------

constexpr unsigned defaultTimeout = 10000;

static bool checkIndexMetaInformation(IDistributedFilePart * part, bool force)
{
    if (part->queryAttributes().hasProp("@offsetBranches"))
        return true;

    if (!force)
        return false;

    Owned<IKeyIndex> index = openKeyFile(*part);
    offset_t branchOffset = index->queryFirstBranchOffset();

    part->lockProperties(defaultTimeout);
    part->queryAttributes().setPropInt64("@offsetBranches", branchOffset);
    part->unlockProperties();
    return true;
}

bool checkIndexMetaInformation(IDistributedFile * file, bool force)
{
    IDistributedSuperFile * super = file->querySuperFile();
    if (super)
    {
        Owned<IDistributedFileIterator> subfiles = super->getSubFileIterator(true);
        bool first = false;
        ForEach(*subfiles)
        {
            IDistributedFile & cur = subfiles->query();
            if (!checkIndexMetaInformation(&cur, force))
                return false;
        }
        return true;
    }

    try
    {
        IPropertyTree & attrs = file->queryAttributes();
        if (!attrs.hasProp("@nodeSize") || !attrs.hasProp("@keyedSize"))
        {
            if (!force)
                return false;

            //Read values from the header of the first index part and save in the meta data
            IDistributedFilePart & part = file->queryPart(0);
            Owned<IKeyIndex> index = openKeyFile(part);
            size_t keySize = index->keySize();
            size_t nodeSize = index->getNodeSize();

            //FUTURE: When file information is returned from an esp service, will this be updated?
            file->lockProperties(defaultTimeout);
            file->queryAttributes().setPropInt("@nodeSize", nodeSize);
            file->queryAttributes().setPropInt("@keyedSize", keySize);
            file->unlockProperties();
        }

        if (!attrs.hasProp("@numLeafNodes"))
        {
            Owned<IDistributedFilePartIterator> parts = file->getIterator();
            ForEach(*parts)
            {
                IDistributedFilePart & cur = parts->query();
                if (!checkIndexMetaInformation(&cur, force))
                    return false;
            }
        }

        return true;
    }
    catch (IException * e)
    {
        e->Release();
        return false;
    }
}

static void gatherDerivedIndexInformation(DerivedIndexInformation & result, IDistributedFile * file)
{
    IPropertyTree & attrs = file->queryAttributes();
    const unsigned defaultNodeSize = 8192;
    unsigned nodeSize = attrs.getPropInt64("@nodeSize", defaultNodeSize);
    if (attrs.hasProp("@numLeafNodes"))
    {
        result.knownLeafCount = true;
        result.numLeafNodes = attrs.getPropInt64("@numLeafNodes");
        result.numBlobNodes = attrs.getPropInt64("@numBlobNodes");
        result.numBranchNodes = attrs.getPropInt64("@numBranchNodes");
        result.sizeDiskLeaves = result.numLeafNodes * nodeSize;
        result.sizeDiskBlobs = result.numBlobNodes * nodeSize;
        result.sizeDiskBranches = result.numBranchNodes * nodeSize;
        result.sizeMemoryBranches = attrs.getPropInt64("@branchMemorySize");
        result.sizeMemoryLeaves = attrs.getPropInt64("@leafMemorySize");
    }
    else
    {
        Owned<IDistributedFilePartIterator> parts = file->getIterator();
        ForEach(*parts)
        {
            IDistributedFilePart & curPart = parts->query();
            //Don't include the TLK in the extended information
            if (isPartTLK(&curPart))
                continue;

            IPropertyTree & partAttrs = curPart.queryAttributes();
            offset_t branchOffset = partAttrs.getPropInt64("@offsetBranches");
            offset_t compressedSize = partAttrs.getPropInt64("@size");

            if (branchOffset != 0)
            {
                offset_t sizeLeaves = branchOffset - nodeSize;                        // Assume all leaf nodes except leading header (no blobs)
                offset_t sizeBranches = (compressedSize - branchOffset) - nodeSize;  // An over-estimate - trailing header, meta, blooms
                result.numLeafNodes += sizeLeaves / nodeSize;
                result.numBranchNodes += sizeBranches / nodeSize;
                result.sizeDiskLeaves += sizeLeaves;
                result.sizeDiskBranches += sizeBranches;
            }
            else
            {
                //A single leaf node...
                result.numLeafNodes += 1;
                result.sizeDiskLeaves += nodeSize;
            }
        }
    }

    unsigned keyLen = attrs.getPropInt64("@keyedSize");
    result.sizeOriginalBranches = result.numLeafNodes * (keyLen + 8);
    offset_t compressedSize = attrs.getPropInt64("@size");
    //uncompressed size is only known if it is filled in.
    if (attrs.hasProp("@uncompressedSize"))
        result.sizeOriginalData = attrs.getPropInt64("@uncompressedSize");

    //The following will depend on the compression format - e.g. if compressed searching is implemented
    if (result.sizeMemoryBranches == 0)
        result.sizeMemoryBranches = result.sizeOriginalBranches;

    //NOTE: sizeOriginalData now includes the blob sizes that are removed before passing to the builder
    //      if the original blob size is recorded then use it, otherwise estimate it
    if (result.sizeOriginalData && (result.sizeMemoryLeaves == 0))
    {
        offset_t originalBlobSize = attrs.getPropInt64("@originalBlobSize");
        if (result.numBlobNodes == 0)
            result.sizeMemoryLeaves = result.sizeOriginalData;
        else if (originalBlobSize)
            result.sizeMemoryLeaves = result.sizeOriginalData - originalBlobSize;
        else
            result.sizeMemoryLeaves = (offset_t)((double)result.numLeafNodes * result.sizeOriginalData)/(result.numLeafNodes + result.numBlobNodes);
    }
}

bool calculateDerivedIndexInformation(DerivedIndexInformation & result, IDistributedFile * file, bool force)
{
    if (!checkIndexMetaInformation(file, force))
        return false;

    IDistributedSuperFile * super = file->querySuperFile();
    if (super)
    {
        Owned<IDistributedFileIterator> subfiles = super->getSubFileIterator(true);
        bool first = true;
        ForEach(*subfiles)
        {
            IDistributedFile & cur = subfiles->query();
            if (!first)
            {
                DerivedIndexInformation nextInfo;
                gatherDerivedIndexInformation(nextInfo, &cur);
                mergeDerivedInformation(result, nextInfo);
            }
            else
            {
                gatherDerivedIndexInformation(result, &cur);
                first = false;
            }
        }
    }
    else
        gatherDerivedIndexInformation(result, file);

    return true;
}


void mergeDerivedInformation(DerivedIndexInformation & result, const DerivedIndexInformation & other)
{
    if (!other.knownLeafCount)
        result.knownLeafCount = false;

    result.numLeafNodes += other.numLeafNodes;
    result.numBlobNodes += other.numBlobNodes;
    result.numBranchNodes += other.numBranchNodes;
    result.sizeDiskLeaves += other.sizeDiskLeaves;
    result.sizeDiskBlobs += other.sizeDiskBlobs;
    result.sizeDiskBranches += other.sizeDiskBranches;

    // These are always known/derived
    result.sizeOriginalBranches += other.sizeOriginalBranches;
    result.sizeMemoryBranches += other.sizeMemoryBranches;

    //These may or may not be known
    if (other.sizeOriginalData)
        result.sizeOriginalData += other.sizeOriginalData;
    else
        result.sizeOriginalData = 0;
    if (other.sizeMemoryLeaves)
        result.sizeMemoryLeaves += other.sizeMemoryLeaves;
    else
        result.sizeMemoryLeaves = 0;
}

//-----------------------------------------------------------------------------

void buildUserMetadata(Owned<IPropertyTree> & metadata, IHThorIndexWriteArg & helper)
{
    size32_t nameLen;
    char * nameBuff;
    size32_t valueLen;
    char * valueBuff;
    unsigned idx = 0;
    while (helper.getIndexMeta(nameLen, nameBuff, valueLen, valueBuff, idx++))
    {
        StringBuffer name(nameLen, nameBuff);
        StringBuffer value(valueLen, valueBuff);
        rtlFree(nameBuff);
        rtlFree(valueBuff);
        if(*name == '_' && !checkReservedMetadataName(name))
        {
            roxiemem::OwnedRoxieString fname(helper.getFileName());
            throw MakeStringException(0, "Invalid name %s in user metadata for index %s (names beginning with underscore are reserved)", name.str(), fname.get());
        }
        if(!validateXMLTag(name.str()))
        {
            roxiemem::OwnedRoxieString fname(helper.getFileName());
            throw MakeStringException(0, "Invalid name %s in user metadata for index %s (not legal XML element name)", name.str(), fname.get());
        }
        if(!metadata)
            metadata.setown(createPTree("metadata", ipt_fast));
        metadata->setProp(name.str(), value.str());
    }
}
