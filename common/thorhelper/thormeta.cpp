/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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
#include "jsocket.hpp"
#include "thorfile.hpp"

#include "eclhelper.hpp"
#include "eclrtl.hpp"
#include "eclrtl_imp.hpp"

#include "dautils.hpp"
#include "dadfs.hpp"
#include "dameta.hpp"

#include "thormeta.hpp"
#include "rtlcommon.hpp"
#include "thorcommon.hpp"

//Should be common with agentctx.hpp
#define WRN_SkipMissingOptIndex             5400
#define WRN_SkipMissingOptFile              5401
#define WRN_UseLayoutTranslation            5402
#define WRN_UnsupportedAlgorithm            5403
#define WRN_MismatchGroupInfo               5404
#define WRN_MismatchCompressInfo            5405
#define WRN_RemoteReadFailure               5406

//Default format - if not specified in ecl and not known to dali etc.
static constexpr const char * defaultFileFormat = "flat";

IPropertyTree * resolveLogicalFilename(const char * filename, IUserDescriptor * user, ResolveOptions options)
{
    //This may go via esp instead at some point....
    return resolveLogicalFilenameFromDali(filename, user, options);
}

//---------------------------------------------------------------------------------------------------------------------

CLogicalFile::CLogicalFile(const CStorageSystems & storage, const IPropertyTree * _metaXml, IOutputMetaData * _expectedMeta)
: metaTree(_metaXml), expectedMeta(_expectedMeta)
{
    name = metaTree->queryProp("@name");
    numParts = metaTree->getPropInt("@numParts", 1);
    fileSize = metaTree->getPropInt64("@rawSize"); // logical file size - used for file positions
    mergedMeta.set(metaTree);

    parts.reserve(numParts);
    Owned<IPropertyTreeIterator> partIter = metaTree->getElements("part");
    if (partIter->first())
    {
        offset_t baseOffset = 0;
        do
        {
            offset_t partSize = partIter->query().getPropInt("@rawSize");
            offset_t numRows = partIter->query().getPropInt("@numRows");
            parts.emplace_back(parts.size(), numRows, partSize, baseOffset);
            baseOffset += partSize;
        } while (partIter->next());
        assertex(parts.size() == numParts);

        if (baseOffset)
        {
            assertex(fileSize == 0 || fileSize == baseOffset);
            fileSize = baseOffset;
        }
    }
    else
    {
        for (unsigned part=0; part < numParts; part++)
            parts.emplace_back(part, 0, 0, 0);
    }

    Owned<IPropertyTreeIterator> planeIter = metaTree->getElements("planes");
    ForEach(*planeIter)
    {
        const char * planeName = planeIter->query().queryProp(nullptr);
        planes.append(storage.queryPlane(planeName));
    }
    if (planes.ordinality() == 0)
    {
        throwUnexpectedX("No plane associated with file");
    }

    actualCrc = metaTree->getPropInt("@metaCrc");
}


void CLogicalFile::applyHelperOptions(const IPropertyTree * helperOptions)
{
    if (!helperOptions || isEmptyPTree(helperOptions))
    {
        mergedMeta.set(metaTree);
    }
    else
    {
        //Use mergeConfiguration() instead of synchronizePTree because it merges attributes, combines single elements, appends lists
        Owned<IPropertyTree> merged = createPTreeFromIPT(metaTree);
        mergeConfiguration(*merged, *helperOptions, nullptr, true);
        mergedMeta.setown(merged.getClear());
    }
}


void CLogicalFile::noteLocation(unsigned part, unsigned superPartNum, offset_t baseOffset)
{
    auto & cur = parts[part];
    cur.superPartNum = superPartNum;
    cur.baseOffset = baseOffset;
}

const IPropertyTree * CLogicalFile::queryFileMeta() const
{
    return mergedMeta;
}

offset_t CLogicalFile::queryOffsetOfPart(unsigned part) const
{
    return queryPart(part).baseOffset;
}

offset_t CLogicalFile::getPartSize(unsigned part) const
{
    if (part < parts.size())
        return parts[part].fileSize;
    return (offset_t)-1;
}

bool CLogicalFile::isLocal(unsigned part, unsigned copy) const
{
    return queryPlane(copy)->isLocal(part);
}

bool CLogicalFile::onAttachedStorage(unsigned copy) const
{
    return queryPlane(copy)->isAttachedStorage();
}


//expand name as path, e.g. copy and translate :: into /
StringBuffer & CLogicalFile::expandLogicalAsPhysical(StringBuffer & target, unsigned copy) const
{
    const char * const separator = queryScopeSeparator(copy);
    const char * cur = name;
    for (;;)
    {
        const char * colon = strstr(cur, "::");
        if (!colon)
            break;

        //MORE: Process special characters?  Revisit commoning up these functions as HPCC-25337
        target.append(colon - cur, cur);
        target.append(separator);
        cur = colon + 2;
    }

    return target.append(cur);
}

StringBuffer & CLogicalFile::expandPath(StringBuffer & target, unsigned part, unsigned copy) const
{
    const char * separator = queryScopeSeparator(copy);
    if (isExternal())
    {
        //expandExternalPath always adds a separator character at the start
        if (endsWith(target.str(), separator))
            target.setLength(target.length()-strlen(separator));

        //skip file::
        const char * coloncolon = strstr(name, "::");
        assertex(coloncolon);
        //skip ip::
        const char * next = coloncolon+2;
        const char * s = strstr(next, "::");
        assertex(s);
        IException * e = nullptr;
        //Slightly strangely expandExternalPath() expects s to point to the leading ::
        expandExternalPath(target, target, name, s, false, &e);
        if (e)
            throw e;
    }
    else
    {
        if (!endsWith(target.str(), separator))
            target.append(separator);
        expandLogicalAsPhysical(target, copy);
    }

    //Add part number suffix
    if (includePartSuffix())
    {
        target.append("._").append(part+1).append("_of_").append(numParts);
    }

    return target;
}

StringBuffer & CLogicalFile::getURL(StringBuffer & target, unsigned part, unsigned copy) const
{
    if (planes.ordinality())
    {
        planes.item(copy)->getURL(target, part);
    }
    return expandPath(target, part, copy);
}


IOutputMetaData * CLogicalFile::queryActualMeta() const
{
    if (!actualMeta)
    {
        actualMeta.setown(getDaliLayoutInfo(*metaTree));
        if (!actualMeta)
        {
            //MORE: Old files (pre 7.0) do not have the serialized file format, some new files cannot create them
            //we should possibly have a way of distinguishing between the two
            actualMeta.set(expectedMeta);
        }
    }
    return actualMeta;
}

const char * CLogicalFile::queryFormat() const
{
    return metaTree->queryProp("@format");
}

unsigned CLogicalFile::getNumCopies() const
{
    return planes.ordinality();
}

const char * CLogicalFile::queryScopeSeparator(unsigned copy) const
{
    return planes.item(copy)->queryScopeSeparator();
}

bool CLogicalFile::includePartSuffix() const
{
    return !metaTree->getPropBool("@singlePartNoSuffix");
}

StringBuffer & CLogicalFile::getTracingFilename(StringBuffer & out, unsigned part) const
{
    return out.append(name).append(":").append(part);
}

const char * CLogicalFile::queryLogicalFilename() const
{
    return name ? name : "";
}


//---------------------------------------------------------------------------------------------------------------------

CLogicalFileSlice::CLogicalFileSlice(CLogicalFile * _file, unsigned _part, offset_t _startOffset, offset_t _length)
: file(_file), part(_part), startOffset(_startOffset), length(_length)
{
}

bool CLogicalFileSlice::isWholeFile() const
{
    if ((startOffset == 0) && file)
    {
        if ((length == unknownFileSize) || (length == file->getPartSize(part)))
            return true;
    }
    return false;
}

StringBuffer & CLogicalFileSlice::getTracingFilename(StringBuffer & out) const
{
    file->getTracingFilename(out, part);
    out.append('{').append(startOffset).append("..");
    if (length != unknownFileSize)
        out.append(startOffset + length);
    return out.append('}');
}

//---------------------------------------------------------------------------------------------------------------------

void CLogicalFileCollection::appendFile(CLogicalFile & file)
{
    files.append(file);
    totalSize += file.getFileSize();
    unsigned numParts = file.getNumParts();
    if (numParts > maxParts)
        maxParts = numParts;
}


void CLogicalFileCollection::calcLocations()
{
    //If there is only a single file, then the global part numbers and base offsets match the values within the file.
    if (files.ordinality() == 1)
        return;

    //If there are multiple parts then they need to be calculated within the interleaved files in the superfile.
    unsigned superPartNum = 0;
    offset_t baseOffset = 0;
    for (unsigned part=0; part < maxParts; part++)
    {
        ForEachItemIn(i, files)
        {
            CLogicalFile & cur = files.item(i);
            unsigned numParts = cur.getNumParts();
            if (numParts > part)
            {
                cur.noteLocation(part, superPartNum, baseOffset);
                superPartNum++;
                baseOffset += cur.getPartSize(part);
            }
        }
    }
    //We may need a function to map from super part numbers to a file/part - in which case we'll need to create a reverse
    //mapping {fileIndex,part} if it needs to be done efficiently
}

/*
Calculate which parts of which files will be included in this channel

There is an implicit ordering of the file parts - file1, part1, file2, part1, file<n> part1, file1, part2 etc.

if distribution and ordering are preservered:
    files are processed in order, multiple parts can be processed on a single node as long as global ordering is retained.

if ordering is preserved and distribution does not need to be:
    files must be processed in order, parts can be processed on any node as long as global ordering is retained.
    e.g. if number of reading nodes is > number of file parts, then part1s can be split between nodes1 and 2, part2 between nodes 3 and 4
         or if multiple files, node1 can process file1 part1, node2 file2 part1, node3 file1 part2, node4 file1 part2

if distribution is preserved, but ordering does not need to be:
    on bare metal, it might be better to have part1,part<n+1> on node1, part2,part<n+2> on node2 since first parts more likely to be local

if neither distribution or ordering are preserved
    if non local storage, could try and ensure that workload is even across the nodes.  Could split parts between nodes (but beware
    of making the read parts too small e.g. for blob read granularity)

on bare metal, if channels is a multiple of the maximum number of parts it is likely to be worth reading the same as inorder
*/

void CLogicalFileCollection::calcPartition(SliceVector & slices, unsigned numChannels, unsigned channel, bool preserveDistribution, bool preserveOrder)
{
    calcLocations();

    //MORE: Revisit and improve the following code to optimize cases detailed above, once they are likely to occur.
    //Likely to require code generator/language improvements first
    unsigned partsPerNode = (maxParts < numChannels) ? 1 : (maxParts + numChannels - 1) / numChannels;
    unsigned startPart = channel * partsPerNode;
    unsigned endPart = startPart + partsPerNode;
    if (endPart > maxParts)
        endPart = maxParts;

    for (unsigned part=startPart; part < endPart; part++)
    {
        collectPartSlices(slices, part);
    }
}


void CLogicalFileCollection::collectPartSlices(SliceVector & slices, unsigned part)
{
    unsigned numFiles = files.ordinality();
    for (unsigned from = 0; from < numFiles; from++)
    {
        CLogicalFile & cur = files.item(from);
        unsigned numParts = cur.getNumParts();
        if (part < numParts)
            slices.emplace_back(&cur, part, 0, unknownFileSize);
    }
}


void CLogicalFileCollection::reset()
{
    totalSize = 0;
    files.kill();
    maxParts = 0;
}

void CLogicalFileCollection::init(IFileCollectionContext * _context, const char * _wuid,  bool _isTemporary, bool _resolveLocally, bool _isCodeSigned, IUserDescriptor * _user, IOutputMetaData * _expectedMeta)
{
    context = _context;
    wuid.set(_wuid);
    isTemporary = _isTemporary;
    resolveLocally = _resolveLocally;
    isCodeSigned = _isCodeSigned;
    user = _user;
    expectedMeta = _expectedMeta;
}


//The following function is call each time the activity is started - more than once if in a child query
void CLogicalFileCollection::setEclFilename(const char * _filename, IPropertyTree * _helperOptions)
{
    assertex(!isTemporary);
    //Check if the same parameters have been passed, and if so avoid rebuilding the information
    if (strisame(filename, _filename))
    {
        if (areMatchingPTrees(helperOptions, _helperOptions))
            return;

        //The file list can stay the same, but the options will need to be recalculated.
        helperOptions.set(_helperOptions);
    }
    else
    {
        reset();
        filename.set(_filename);
        helperOptions.set(_helperOptions);

        processLogicalFilename();
    }

    applyHelperOptions();
}

void CLogicalFileCollection::processLogicalFilename()
{
    Owned<IPropertyTree> resolvedMeta;
    if (resolveLocally)
    {
        resolvedMeta.setown(createPTree("meta"));
        IPropertyTree * storage = resolvedMeta->addPropTree("storage");
        IPropertyTree * plane = storage->addPropTree("planes");
        plane->setProp("@prefix", ".");
        plane->setProp("@name", "local");

        IPropertyTree * file = resolvedMeta->addPropTree("file");
        file->setProp("@name", filename);
        file->setProp("@prefix", ".");
        file->setPropBool("@singlePartNoSuffix", true);
        file->addProp("planes", "local");
    }
    else
    {
        //MORE: These options could be restricted e.g., ROpartinfo/ROsizes only if a count operation, or if virtual(fileposition) used
        ResolveOptions options = ROincludeLocation|ROpartinfo|ROsizes;
        resolvedMeta.setown(resolveLogicalFilename(filename, user, options));
    }
    processResolvedMeta(resolvedMeta);
}

//Walk the information that was generated by resolving the filename and generate a set of file objects
void CLogicalFileCollection::processResolvedMeta(IPropertyTree * _resolved)
{
    resolved.set(_resolved);
    storageSystems.setFromMeta(resolved);

    bool expectedGrouped = helperOptions->getPropBool("@grouped");
    Owned<IPropertyTreeIterator> fileIter = resolved->getElements("file");
    ForEach(*fileIter)
    {
        IPropertyTree & cur = fileIter->query();
        if (cur.getPropBool("@missing"))
        {
            const char * filename = cur.queryProp("@name");
            if (!helperOptions->getPropBool("@optional", false))
            {
                StringBuffer errorMsg("");
                throw makeStringException(0, errorMsg.append(": Logical file name '").append(filename).append("' could not be resolved").str());
            }
            else
            {
                StringBuffer buff;
                buff.appendf("Input file '%s' was missing but declared optional", filename);
                context->noteException(SeverityInformation, WRN_SkipMissingOptFile, buff.str());
            }
        }
        else
        {
            CLogicalFile * file = new CLogicalFile(storageSystems, &cur, expectedMeta);
            appendFile(*file);

            bool isGrouped = file->isGrouped();
            if (isGrouped != expectedGrouped)
            {
                StringBuffer msg;
                msg.append("DFS and code generated group info for file ").append(filename).append(" differs: DFS(").append(isGrouped ? "grouped" : "ungrouped").append("), CodeGen(").append(expectedGrouped ? "ungrouped" : "grouped").append("), using DFS info");
                throw makeStringException(WRN_MismatchGroupInfo, msg.str());
            }
        }
    }
}

void CLogicalFileCollection::setTempFilename(const char * _filename, IPropertyTree * _helperOptions, const IPropertyTree * spillPlane)
{
    assertex(isTemporary);
    //Temp file parameters should never change if they are called again in a child query
    if (filename)
    {
        if (!strisame(filename, _filename) || !areMatchingPTrees(helperOptions, _helperOptions))
            throwUnexpected();
        return;
    }

    filename.set(_filename);
    helperOptions.setown(createPTreeFromIPT(_helperOptions));
    helperOptions->setProp("@name", filename);
    //Partinfo does not need to be supplied for temporary files.  Deos the number of parts?

    IPropertyTree * plane = helperOptions->addPropTreeArrayItem("planes", createPTree("planes"));
    plane->setProp("", spillPlane->queryProp("@name"));

    storageSystems.registerPlane(spillPlane);

    CLogicalFile * file = new CLogicalFile(storageSystems, helperOptions, expectedMeta);
    appendFile(*file);
    file->applyHelperOptions(nullptr);
}


void CLogicalFileCollection::applyHelperOptions()
{
    ForEachItemIn(i, files)
        files.item(i).applyHelperOptions(helperOptions);
}
