/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#include "platform.h"

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "jfile.hpp"
#include "jio.hpp"
#include "jlog.hpp"
#include "jplane.hpp"
#include "jutil.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafsaudit.hpp"
#include "dautils.hpp"
#include "dasds.hpp"
#include "dafdesc.hpp"

#include "salds.hpp"
#include "saxref.hpp"
#include "saxreflite.hpp"

// XRef Lite collects logical files touched during the audit interval, normalizes each
// file and storage plane, then checks published files in Dali for missing parts. Files
// absent from Dali are probed only on their recorded planes. Recent files are omitted,
// and a complete result generation is saved before its metadata is published.
#define LOGPFX "XREFLITE: "
static constexpr size32_t defaultXRefLiteBlockSize = 0x400000;
static constexpr offset_t maxXRefLiteFileSize = 0x100000 * 256;
static constexpr unsigned maxXRefLiteErrors = 1000;
static constexpr unsigned maxXRefLiteWarnings = 1000;
static constexpr unsigned xrefLiteSdsConnectTimeout = 1000 * 60 * 60 * 2;

struct XRefLiteCandidate
{
    std::vector<std::string> planeNames; // Storage planes named by audit records for this logical file.

    // Adds a normalized plane name unless the candidate already contains it.
    void addPlaneName(const char *name)
    {
        if (isEmptyString(name))
            return;

        StringBuffer normalized(name);
        normalized.trim();
        if (normalized.isEmpty())
            return;

        if (!findPlane(normalized.str()))
            planeNames.emplace_back(normalized.str());
    }

    bool findPlane(const char *planeName) const
    {
        for (const auto &name : planeNames)
            if (strieq(name.c_str(), planeName))
                return true;
        return false;
    }
};

struct PhysicalPart
{
    unsigned partNumber = 0; // One-based part number deduced from the physical filename.
    offset_t size = -1;      // Primary size, or replicate size when the primary part is absent.
    std::string node;        // Primary host, when the plane uses explicit hosts.
    std::string rnode;       // Mirror host, when the plane uses explicit hosts.
};

struct PhysicalMatch
{
    StringBuffer fullPhysicalPath;            // Full primary physical path containing the logical part mask.
    StringBuffer replicateFullPhysicalPath;   // Full mirror physical path, used when no primary part is found.
    std::vector<PhysicalPart> parts;  // Unique physical parts found across the primary and mirror scans.
    offset_t size = 0;                // Total size, using the replicate size for each missing primary part.
    CDateTime modified;               // Latest primary modification time, or mirror time as fallback.
    CDateTime replicateModified;      // Latest mirror modification time, retained for mirror-only fallback.
    unsigned numParts = 0;            // Total part count deduced from the physical filenames.
    bool isDirPerPart = false;        // True when any part was found in a part-number subdirectory.
    bool primaryFound = false;        // True when at least one part was found under the primary prefix.

    static void scanPhysicalDirectory(std::vector<PhysicalMatch> &matches, const char *directory, const char *fileGlob, const char *fullPhysicalPath, const char *host, bool checkDirPerPart, bool replicate);
};

class XRefLitePlane
{
public:
    XRefLitePlane(const char *_name, const IStoragePlane *_plane) : name(_name), plane(_plane) { }

    void probe(IPropertyTree &found, IPropertyTree &orphans, const char *logicalName, unsigned recentCutoffMinutes) const;
    void addPhysicalFile(IPropertyTree &found, IPropertyTree &orphans, const char *logicalName, const PhysicalMatch &match) const;

    std::string name; // Configured storage-plane name written to XRef results.

private:
    Linked<const IStoragePlane> plane; // Plane configuration used to locate primary and mirror storage.
};

static void addXRefLiteWarning(IPropertyTree &messages, const char *logicalName, const char *text)
{
    unsigned warningCount = messages.getCount("Warning");
    if (warningCount < maxXRefLiteWarnings)
    {
        IPropertyTree *warning = messages.addPropTree("Warning");
        if (!isEmptyString(logicalName))
            warning->setProp("File", logicalName);
        warning->setProp("Text", text);
    }
    else if (warningCount == maxXRefLiteWarnings)
    {
        IPropertyTree *warning = messages.addPropTree("Warning");
        warning->setProp("Text", "warning limit exceeded (1000), truncating");
    }
}

// Normalizes one audit record and rejects candidates without one consistent storage plane.
static bool parseXRefLiteCandidate(const char *line, std::string &logicalName, XRefLiteCandidate &candidate, IPropertyTree &messages)
{
    logicalName.clear();
    candidate.planeNames.clear();

    const char *payload = isEmptyString(line) ? nullptr : strstr(line, ",FileAccess,");
    if (!payload)
        return false;

    // Example: FileAccess,Dali,PUBLISH,user=alice lfn=scope::file plane=myplane
    const char *record = payload + 1; // FileAccess,Dali,PUBLISH,user=...
    const char *componentSeparator = strchr(record, ','); // ,Dali,PUBLISH,user=...
    const char *operationSeparator = componentSeparator ? strchr(componentSeparator + 1, ',') : nullptr; // ,PUBLISH,user=...
    const char *logfmtSeparator = operationSeparator ? strchr(operationSeparator + 1, ',') : nullptr; // ,user=...
    if (!logfmtSeparator)
        return false;

    DFSAuditContext auditFields = DFSAuditContext::fromLogfmt(logfmtSeparator + 1);
    const char *auditLogicalName = auditFields.queryValue("lfn", nullptr);
    CDfsLogicalFileName lfn;
    if (isEmptyString(auditLogicalName) || !lfn.setValidate(auditLogicalName) || lfn.isExternal() || lfn.isForeign() || lfn.isQuery())
    {
        addXRefLiteWarning(messages, auditLogicalName, "Ignoring invalid audit logical file name");
        return false;
    }
    logicalName.assign(lfn.get());

    const char *auditPlane = auditFields.queryValue("plane", nullptr);
    if (isEmptyString(auditPlane))
    {
        addXRefLiteWarning(messages, logicalName.c_str(), "Ignoring audit logical file without a storage plane");
        return false;
    }
    candidate.addPlaneName(auditPlane);
    return true;
}

// Collects and merges valid logical-file candidates from the requested audit interval.
static void collectXRefLiteCandidates(const CDateTime &from, const CDateTime &to, const char *auditLogFile, std::unordered_map<std::string, XRefLiteCandidate> &candidates, IPropertyTree &messages)
{
    StringAttrArray rows;
    if (isEmptyString(auditLogFile))
        queryAuditLogs(from, to, ",FileAccess", rows); // A single queryAuditLogs call is capped at 100,000 rows.
    else
    {
        OwnedIFileIOStream stream = createBufferedIOStreamFromFile(auditLogFile, IFOread);
        if (!stream)
            throw makeStringExceptionV(0, LOGPFX "Failed to open audit log file %s", auditLogFile);
        Owned<IStreamLineReader> lineReader = createLineReader(stream, false);
        StringBuffer line;
        while (!lineReader->readLine(line.clear()))
        {
            if (strstr(line.str(), ",FileAccess,"))
                rows.append(*new StringAttrItem(line.str()));
        }
    }

    ForEachItemIn(row, rows)
    {
        std::string logicalName;
        XRefLiteCandidate parsed;
        if (!parseXRefLiteCandidate(rows.item(row).text, logicalName, parsed, messages))
            continue;

        XRefLiteCandidate &candidate = candidates[logicalName];
        for (const auto &planeName : parsed.planeNames)
            candidate.addPlaneName(planeName.c_str());
    }
}

// Resolves metadata at the XRef Lite root and report files beneath its published generation.
static StringBuffer &getXRefLiteFilePath(const char *name, StringBuffer &path)
{
    getLdsPath("xref-lite", path);
    if (!streq(name, "Metadata"))
    {
        StringBuffer metadataPath(path);
        addPathSepChar(metadataPath).append("Metadata.xml");
        MemoryBuffer xml;
        readTextFileIfPresent(metadataPath.str(), maxXRefLiteFileSize, xml);
        if (xml.length())
        {
            Owned<IPropertyTree> metadata = createPTreeFromXMLString((const char *)xml.toByteArray());
            const char *generation = metadata->queryProp("@modified");
            if (!isEmptyString(generation))
                addPathSepChar(path).append(generation);
        }
    }
    addPathSepChar(path).append(name).append(".xml");
    return path;
}

// Loads and parses one XRef Lite result file when it exists.
static bool loadXRefLiteResult(const char *name, Owned<IPropertyTree> &result)
{
    result.clear();
    StringBuffer path;
    getXRefLiteFilePath(name, path);
    MemoryBuffer xml;
    readTextFileIfPresent(path.str(), maxXRefLiteFileSize, xml);
    if (!xml.length())
        return false;

    result.setown(createPTreeFromXMLString((const char *)xml.toByteArray()));
    return result.get() != nullptr;
}

// Restores the last completed scan time from the published metadata.
static void loadXRefLiteCheckpoint(CDateTime &checkpoint)
{
    Owned<IPropertyTree> metadata;
    if (!loadXRefLiteResult("Metadata", metadata))
        return;

    const char *lastScan = metadata->queryProp("@lastXRefLiteScan");
    if (!isEmptyString(lastScan))
        checkpoint.setString(lastScan, nullptr, false);
}

// Saves a complete timestamped result generation, then publishes its metadata.
static void saveXRefLiteResults(const CDateTime &checkpoint, IPropertyTree &lost, IPropertyTree &found, IPropertyTree &orphans, IPropertyTree &messages)
{
    Owned<IPropertyTree> metadata = createPTree("Metadata");
    StringBuffer checkpointText;
    checkpoint.getString(checkpointText, false);
    metadata->setProp("@lastXRefLiteScan", checkpointText.str());
    CDateTime completed;
    completed.setNow();
    StringBuffer completedText;
    completed.getString(completedText, false);
    metadata->setProp("@modified", completedText.str());
    unsigned lostCount = lost.getCount("File");
    unsigned foundCount = found.getCount("File");
    unsigned orphanCount = orphans.getCount("File");
    unsigned errorCount = messages.getCount("Error");
    metadata->setPropInt("@lost", lostCount);
    metadata->setPropInt("@found", foundCount);
    metadata->setPropInt("@orphans", orphanCount);
    metadata->setPropInt("@errors", errorCount);
    metadata->setPropInt("@count", lostCount + foundCount + orphanCount);
    if (errorCount)
        metadata->setProp("@status", VStringBuffer("Generated [%u issues, %u errors]", lostCount + foundCount + orphanCount, errorCount).str());
    else
        metadata->setProp("@status", VStringBuffer("Generated [%u issues]", lostCount + foundCount + orphanCount).str());

    StringBuffer resultDir;
    getLdsPath("xref-lite", resultDir);
    size_t rootLength = resultDir.length();
    addPathSepChar(resultDir).append(completedText);
    if (!recursiveCreateDirectory(resultDir))
        throw makeStringExceptionV(0, LOGPFX "Failed to create result directory %s", resultDir.str());

    StringBuffer planeName;
    findPlaneFromPath(resultDir.str(), planeName);
    size32_t blockSize = planeName.isEmpty() ? defaultXRefLiteBlockSize : getBlockedFileIOSize(planeName, defaultXRefLiteBlockSize);
    saveXRefBranchToPlane(resultDir.str(), "Lost", lost, blockSize);
    saveXRefBranchToPlane(resultDir.str(), "Found", found, blockSize);
    saveXRefBranchToPlane(resultDir.str(), "Orphans", orphans, blockSize);
    saveXRefBranchToPlane(resultDir.str(), "Messages", messages, blockSize);
    resultDir.setLength(rootLength);
    saveXRefBranchToPlane(resultDir.str(), "Metadata", *metadata, blockSize);
}

static bool isRecentXRefFile(const CDateTime &modified, unsigned recentCutoffMinutes)
{
    CDateTime cutoff(modified);
    cutoff.adjustTime(recentCutoffMinutes);
    CDateTime now;
    now.setNow();
    return cutoff.compare(now) >= 0;
}

struct LostPartStatus
{
    PhysicalPart missing;
    bool primaryExists = false;
    bool replicateExists = false;
};

// Checks parts concurrently while keeping each worker's result in an independent slot.
static void checkPhysicalParts(IDistributedFile &distributedFile, std::vector<LostPartStatus> &partStatus, unsigned maxAtOnce)
{
    unsigned numParts = distributedFile.numParts();
    partStatus.resize(numParts);
    class CCheckPhysicalParts : public CAsyncFor
    {
    public:
        CCheckPhysicalParts(IDistributedFile &_distributedFile, std::vector<LostPartStatus> &_partStatus)
            : CAsyncFor(SYNC_LOCATION), distributedFile(_distributedFile), partStatus(_partStatus)
        {
        }

        // Checks every physical copy of one part and records which copies exist.
        void Do(unsigned part) override
        {
            LostPartStatus &status = partStatus[part];
            status.missing.partNumber = part + 1;
            Owned<IDistributedFilePart> distributedPart = distributedFile.getPart(part);
            unsigned copies = distributedPart->numCopies();
            for (unsigned copy = 0; copy < copies; ++copy)
            {
                RemoteFilename filename;
                distributedPart->getFilename(filename, copy);
                if (copy == 0)
                {
                    StringBuffer node;
                    filename.queryEndpoint().getHostText(node);
                    status.missing.node.assign(node.str());
                }

                Owned<IFile> physicalFile = createIFile(filename);
                if (physicalFile->exists())
                {
                    if (copy == 0)
                        status.primaryExists = true;
                    else
                        status.replicateExists = true;
                }
            }
        }

    private:
        IDistributedFile &distributedFile;
        std::vector<LostPartStatus> &partStatus;
    } checker(distributedFile, partStatus);
    checker.For(numParts, maxAtOnce, true, true);
}

// Classifies a published file as healthy or adds its fully missing parts to Lost.
static bool checkIfLostFile(IPropertyTree &lost, const char *logicalName, IUserDescriptor *user, bool ignoreLazyLost, unsigned maxAtOnce)
{
    CDfsLogicalFileName lfn;
    lfn.set(logicalName);

    Owned<IDistributedFile> distributedFile = queryDistributedFileDirectory().lookup(lfn, user, AccessMode::readMeta, false, false, nullptr, defaultPrivilegedUser, xrefLiteSdsConnectTimeout);
    if (!distributedFile)
        return false; // file not present in Dali
    if (distributedFile->querySuperFile())
        return true;
    if (ignoreLazyLost && distributedFile->queryAttributes().getPropBool("@lazy", false))
        return true;

    std::vector<LostPartStatus> partStatus;
    checkPhysicalParts(*distributedFile, partStatus, maxAtOnce);
    std::vector<PhysicalPart> missingParts;
    unsigned primaryLost = 0;
    unsigned replicateLost = 0;
    for (auto &status : partStatus)
    {
        if (!status.primaryExists)
            primaryLost++;
        if (!status.replicateExists)
            replicateLost++;
        if (!status.primaryExists && !status.replicateExists)
            missingParts.emplace_back(std::move(status.missing));
    }

    if (missingParts.empty())
        return true;

    IPropertyTree *file = lost.addPropTree("File");
    file->setProp("Name", logicalName);
    StringBuffer clusterName;
    distributedFile->getClusterName(0, clusterName);
    file->setProp("Cluster", clusterName.str());
    if (const char *partMask = distributedFile->queryPartMask())
        file->setProp("Partmask", StringBuffer(partMask).toLowerCase().str());
    file->setPropInt("Numparts", distributedFile->numParts());
    file->setPropInt("Partslost", missingParts.size());
    file->setPropInt("Primarylost", primaryLost);
    file->setPropInt("Replicatedlost", replicateLost);

    CDateTime modified;
    if (distributedFile->getModificationTime(modified))
    {
        StringBuffer modifiedText;
        file->setProp("Modified", modified.getString(modifiedText).str());
    }

    for (const auto &missing : missingParts)
    {
        IPropertyTree *part = file->addPropTree("Part");
        part->setPropInt("Num", missing.partNumber);
        if (!missing.node.empty())
            part->setProp("Node", missing.node.c_str());
    }
    return true;
}

// Scans one physical directory and merges matching part files by their deduced part count.
void PhysicalMatch::scanPhysicalDirectory(std::vector<PhysicalMatch> &matches, const char *directory, const char *fileGlob, const char *fullPhysicalPath, const char *host, bool checkDirPerPart, bool replicate)
{
    Owned<IFile> scopeDirectory;
    if (isEmptyString(host))
        scopeDirectory.setown(createIFile(directory));
    else
    {
        SocketEndpoint endpoint(host);
        RemoteFilename remoteDirectory;
        remoteDirectory.setPath(endpoint, directory);
        scopeDirectory.setown(createIFile(remoteDirectory));
    }

    if (!scopeDirectory->exists())
        return;

    // Merges matching files from one directory into results grouped by deduced part count.
    auto scanDirectory = [&](IFile &directoryToScan, unsigned dirPerPartNum)
    {
        Owned<IDirectoryIterator> files = directoryToScan.directoryFiles(fileGlob, false, false);
        ForEach(*files)
        {
            StringBuffer filename;
            files->getName(filename);

            unsigned partIndex = 0;
            unsigned partCount = 0;
            unsigned filenameLength = 0;
            StringAttr deducedMask;
            if (!deduceMask(pathTail(filename.str()), false, deducedMask, partIndex, partCount, filenameLength) || !strieq(deducedMask, pathTail(fullPhysicalPath)))
                continue;

            unsigned partNumber = partIndex + 1;
            if (dirPerPartNum && dirPerPartNum != partNumber)
                continue;

            auto matchIt = std::find_if(matches.begin(), matches.end(), [partCount](const PhysicalMatch &match) { return match.numParts == partCount; });
            if (matchIt == matches.end())
            {
                matches.emplace_back();
                matchIt = matches.end() - 1;
                matchIt->numParts = partCount;
            }
            PhysicalMatch &match = *matchIt;
            StringBuffer &resultPath = replicate ? match.replicateFullPhysicalPath : match.fullPhysicalPath;
            if (resultPath.isEmpty())
                resultPath.append(fullPhysicalPath);

            auto physicalPartItr = std::find_if(match.parts.begin(), match.parts.end(), [partNumber](const PhysicalPart &part) { return part.partNumber == partNumber; });
            if (physicalPartItr == match.parts.end())
            {
                match.parts.emplace_back();
                physicalPartItr = match.parts.end() - 1;
                physicalPartItr->partNumber = partNumber;
            }
            if (!isEmptyString(host))
            {
                if (replicate)
                    physicalPartItr->rnode.assign(host);
                else
                    physicalPartItr->node.assign(host);
            }
            match.isDirPerPart |= dirPerPartNum != 0;
            offset_t fileSize = files->getFileSize();
            if (replicate)
            {
                if (physicalPartItr->size == -1)
                {
                    physicalPartItr->size = fileSize;
                    match.size += fileSize;
                }
                else if (physicalPartItr->size != fileSize)
                    IERRLOG(LOGPFX "Primary size %" I64F "d does not match replicate size %" I64F "d for part %u of %s", physicalPartItr->size, fileSize, partNumber, fullPhysicalPath);
            }
            else
            {
                match.primaryFound = true;
                physicalPartItr->size = fileSize;
                match.size += fileSize;
            }

            CDateTime fileModified;
            if (files->getModifiedTime(fileModified))
            {
                CDateTime &latest = replicate ? match.replicateModified : match.modified;
                if (latest.isNull() || fileModified.compare(latest, false) > 0)
                    latest.set(fileModified);
            }
        }
    };

    scanDirectory(*scopeDirectory, 0);
    if (!checkDirPerPart)
        return;

    Owned<IDirectoryIterator> directories = scopeDirectory->directoryFiles(nullptr, false, true);
    ForEach(*directories)
    {
        StringBuffer name;
        directories->getName(name);
        unsigned dirPerPartNum = readDigits(name.str());
        if (!directories->isDir() || !dirPerPartNum)
            continue;
        scanDirectory(directories->query(), dirPerPartNum);
    }
}

// Probes a logical file's primary and mirror prefixes across every device or host in this plane.
void XRefLitePlane::probe(IPropertyTree &found, IPropertyTree &orphans, const char *logicalName, unsigned recentCutoffMinutes) const
{
    std::vector<PhysicalMatch> matches;
    const std::vector<std::string> &hosts = plane->queryHosts();
    // Searches every configured device or host beneath one primary or mirror prefix.
    auto probePrefix = [&](const char *prefix, bool replicate)
    {
        StringBuffer directory;
        StringBuffer fileGlob;
        StringBuffer partMask;
        getLFNDirectoryUsingBaseDir(directory, logicalName, prefix);
        getPartMask(partMask, logicalName, 0);
        wildcardMask(fileGlob, partMask);
        StringBuffer resultMask(directory);
        addPathSepChar(resultMask);
        resultMask.append(partMask);

        // MORE: Consider parallelizing stripe/host scans after PhysicalMatch accumulation is made thread safe.
        if (plane->isStriped())
        {
            for (unsigned device = 1; device <= plane->numDevices(); ++device)
            {
                StringBuffer stripeDirectory;
                getLFNDirectoryUsingBaseDir(stripeDirectory, logicalName, prefix, device);
                PhysicalMatch::scanPhysicalDirectory(matches, stripeDirectory.str(), fileGlob.str(), resultMask.str(), nullptr, plane->queryDirPerPart(), replicate);
            }
        }
        else if (hosts.empty())
            PhysicalMatch::scanPhysicalDirectory(matches, directory.str(), fileGlob.str(), resultMask.str(), nullptr, plane->queryDirPerPart(), replicate);
        else
        {
            for (const auto &host : hosts)
                PhysicalMatch::scanPhysicalDirectory(matches, directory.str(), fileGlob.str(), resultMask.str(), host.c_str(), plane->queryDirPerPart(), replicate);
        }
    };

    probePrefix(plane->queryPrefix(), false);
    const char *mirrorPrefix = plane->queryMirrorPrefix();
    if (!isContainerized() && !streq(mirrorPrefix, plane->queryPrefix()))
        probePrefix(mirrorPrefix, true);
    for (auto &match : matches)
    {
        if (!match.primaryFound && !match.parts.empty())
        {
            match.fullPhysicalPath.set(match.replicateFullPhysicalPath.str());
            match.modified.set(match.replicateModified);
        }
        if (match.modified.isNull() || !isRecentXRefFile(match.modified, recentCutoffMinutes))
            addPhysicalFile(found, orphans, logicalName, match);
    }
}

// Adds a complete physical match to Found or an incomplete match to Orphans.
void XRefLitePlane::addPhysicalFile(IPropertyTree &found, IPropertyTree &orphans, const char *logicalName, const PhysicalMatch &match) const
{
    IPropertyTree &result = match.parts.size() < match.numParts ? orphans : found;
    IPropertyTree *file = result.addPropTree("File");
    file->setProp("Name", logicalName);
    file->setProp("Cluster", name.c_str());
    file->setProp("Partmask", match.fullPhysicalPath.str());
    file->setPropInt("Numparts", match.numParts ? match.numParts : static_cast<unsigned>(match.parts.size()));
    if (match.isDirPerPart)
        file->setPropBool("IsDirPerPart", true);
    if (match.parts.size() < match.numParts)
        file->setPropInt("Partsfound", match.parts.size());
    file->setPropInt64("Size", match.size);
    if (!match.modified.isNull())
    {
        StringBuffer modified;
        file->setProp("Modified", match.modified.getString(modified).str());
    }

    for (const auto &physicalPart : match.parts)
    {
        IPropertyTree *part = file->addPropTree("Part");
        part->setPropInt("Num", physicalPart.partNumber);
        if (!physicalPart.node.empty())
            part->setProp("Node", physicalPart.node.c_str());
        if (!physicalPart.rnode.empty())
            part->setProp("RNode", physicalPart.rnode.c_str());
    }
}

// MORE: Encapsulate run state in a CXRefLite owner.
// Move configurable defaults such as xrefLiteSdsConnectTimeout into that owner.
// Runs one incremental scan and publishes its results only after the generation is complete.
static void runXRefLite(bool doFullScan, unsigned daysBack, IUserDescriptor *user, bool ignoreLazyLost, unsigned maxAtOnce, unsigned recentCutoffMinutes, const char *auditLogFile)
{
    CDateTime to;
    to.setNow();

    CDateTime from;
    if (doFullScan)
        from.setString("1970-01-01T00:00:00", nullptr, false);
    else if (daysBack > 0)
    {
        from.set(to);
        from.adjustTime(-daysBack * 24 * 60);
    }
    else
    {
        loadXRefLiteCheckpoint(from);
        if (from.isNull())
            from.setString("1970-01-01T00:00:00", nullptr, false);
    }

    StringArray planeNames;
    getXRefPlanes(nullptr, planeNames);
    std::vector<XRefLitePlane> planes;
    ForEachItemIn(i, planeNames)
    {
        const char *planeName = planeNames.item(i);
        Owned<const IStoragePlane> plane = getStoragePlaneByName(planeName, true);
        planes.emplace_back(planeName, plane);
    }

    Owned<IPropertyTree> lost = createPTree("Lost");
    Owned<IPropertyTree> found = createPTree("Found");
    Owned<IPropertyTree> orphans = createPTree("Orphans");
    Owned<IPropertyTree> messages = createPTree("Messages");
    std::unordered_map<std::string, XRefLiteCandidate> candidates;
    collectXRefLiteCandidates(from, to, auditLogFile, candidates, *messages);
    unsigned errorCount = 0;

    for (const auto &candidatePair : candidates)
    {
        const char *logicalName = candidatePair.first.c_str();
        try
        {
            if (checkIfLostFile(*lost, logicalName, user, ignoreLazyLost, maxAtOnce))
                continue; // File is not lost
        }
        catch (IException *e)
        {
            StringBuffer errorText;
            e->errorMessage(errorText);
            if (errorCount++ < maxXRefLiteErrors)
            {
                IPropertyTree *error = messages->addPropTree("Error");
                error->setProp("File", logicalName);
                error->setProp("Text", errorText.str());
            }
            else if (errorCount == maxXRefLiteErrors + 1)
            {
                IPropertyTree *error = messages->addPropTree("Error");
                error->setProp("Text", "error limit exceeded (1000), truncating");
            }
            StringBuffer tmp(LOGPFX "checking candidate in Dali: ");
            EXCLOG(e, tmp.append(logicalName).str());
            e->Release();
            continue;
        }

        // Files not published in Dali are probed on their recorded planes and reported as found or orphaned.
        for (const auto &plane : planes)
        {
            if (!candidatePair.second.findPlane(plane.name.c_str()))
                continue;

            // MORE: Candidate-centric probing may enumerate the same scope directory for multiple files.
            plane.probe(*found, *orphans, logicalName, recentCutoffMinutes);
        }
    }

    saveXRefLiteResults(to, *lost, *found, *orphans, *messages);
    PROGLOG(LOGPFX "scan finished: %u candidate%s, %u lost, %u found, %u orphan%s", static_cast<unsigned>(candidates.size()), candidates.size() == 1 ? "" : "s", lost->getCount("File"), found->getCount("File"), orphans->getCount("File"), orphans->getCount("File") == 1 ? "" : "s");
}

// Creates the requested Dali user context and runs one XRef Lite scan from the command line.
void runXRefLiteCLI(const char *userName, bool doFullScan, unsigned daysBack, bool ignoreLazyLost, unsigned maxAtOnce, unsigned recentCutoffMinutes, const char *auditLogFile)
{
    Owned<IUserDescriptor> user = createUserDescriptor();
    user->set(userName, nullptr);
    runXRefLite(doFullScan, daysBack, user, ignoreLazyLost, maxAtOnce, recentCutoffMinutes, auditLogFile);
}

#ifdef _USE_CPPUNIT

#include <cppunit/extensions/HelperMacros.h>

class XRefLiteTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(XRefLiteTest);
        CPPUNIT_TEST(testStandardizedFormat);
        CPPUNIT_TEST(testAuditLogfmtParser);
        CPPUNIT_TEST(testWildcardMask);
        CPPUNIT_TEST(testIncompleteFoundParts);
    CPPUNIT_TEST_SUITE_END();

public:
    // Verifies logfmt candidate normalization, plane selection, and invalid-record rejection.
    void testStandardizedFormat()
    {
        std::string logicalName;
        XRefLiteCandidate candidate;
        Owned<IPropertyTree> messages = createPTree("Messages");

        CPPUNIT_ASSERT(parseXRefLiteCandidate("2026-09-08 10:00:00 ,FileAccess,Dali,PUBLISH,user=alice lfn=scope::standard plane=myplane note=\"value,with comma\"", logicalName, candidate, *messages));
        CPPUNIT_ASSERT_EQUAL(std::string("scope::standard"), logicalName);
        CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1), candidate.planeNames.size());
        CPPUNIT_ASSERT(candidate.findPlane("myplane"));

        CPPUNIT_ASSERT(!parseXRefLiteCandidate("2026-09-08 10:00:01 ,FileAccess,Dali,DELETED,user=alice lfn=scope::noplanes", logicalName, candidate, *messages));
        CPPUNIT_ASSERT_EQUAL(1U, messages->getCount("Warning"));
        CPPUNIT_ASSERT(streq("scope::noplanes", messages->queryProp("Warning[1]/File")));
    }

    // Verifies the platform logfmt parser handles the audit fields consumed by XRef Lite.
    void testAuditLogfmtParser()
    {
        DFSAuditContext logfmtFields = DFSAuditContext::fromLogfmt("user=alice lfn=scope::standard plane=myplane wuid=W20260908-100000 graph=\"graph,1\"");
        CPPUNIT_ASSERT(streq("scope::standard", logfmtFields.queryValue("lfn")));
        CPPUNIT_ASSERT(streq("myplane", logfmtFields.queryValue("plane")));
        CPPUNIT_ASSERT(streq("graph,1", logfmtFields.queryValue("graph")));
    }

    void testWildcardMask()
    {
        StringBuffer wildcard;
        CPPUNIT_ASSERT(streq("afile._*_of_*", wildcardMask(wildcard, "afile._$P$_of_$N$").str()));
        CPPUNIT_ASSERT(streq("*.*.gz", wildcardMask(wildcard.clear(), "$p$.$n$.gz").str()));
        CPPUNIT_ASSERT(streq("price$usd", wildcardMask(wildcard.clear(), "price$usd").str()));
        CPPUNIT_ASSERT(streq("afile_$P_of_*", wildcardMask(wildcard.clear(), "afile_$P_of_$N$").str()));
    }

    // Verifies incomplete physical matches are Orphans and complete matches are Found.
    void testIncompleteFoundParts()
    {
        XRefLitePlane plane("data", nullptr);
        PhysicalMatch match;
        match.numParts = 3;
        match.parts.emplace_back();
        match.parts.back().partNumber = 2;

        Owned<IPropertyTree> found = createPTree("Found");
        Owned<IPropertyTree> orphans = createPTree("Orphans");
        plane.addPhysicalFile(*found, *orphans, "scope::partial", match);

        CPPUNIT_ASSERT_EQUAL(0U, found->getCount("File"));
        CPPUNIT_ASSERT_EQUAL(1U, orphans->getCount("File"));
        CPPUNIT_ASSERT_EQUAL(3, orphans->getPropInt("File/Numparts"));
        CPPUNIT_ASSERT_EQUAL(1, orphans->getPropInt("File/Partsfound"));

        match.parts.resize(3);
        match.parts[0].partNumber = 1;
        match.parts[1].partNumber = 2;
        match.parts[2].partNumber = 3;
        plane.addPhysicalFile(*found, *orphans, "scope::complete", match);

        CPPUNIT_ASSERT_EQUAL(1U, found->getCount("File"));
        CPPUNIT_ASSERT_EQUAL(1U, orphans->getCount("File"));
        CPPUNIT_ASSERT_EQUAL(3, found->getPropInt("File/Numparts"));
        CPPUNIT_ASSERT(!found->hasProp("File/Partsfound"));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(XRefLiteTest);

#endif // _USE_CPPUNIT
