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


#include "dameta.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"


//More to a more central location
IPropertyTree * queryHostGroup(const char * name)
{
    if (isEmptyString(name))
        return nullptr;
    VStringBuffer xpath("storage/hostGroups[@name='%s']", name);
    IPropertyTree & global = queryGlobalConfig();
    return global.queryPropTree(xpath);
}

IPropertyTree * queryStoragePlane(const char * name)
{
    VStringBuffer xpath("storage/planes[@name='%s']", name);
    IPropertyTree & global = queryGlobalConfig();
    return global.queryPropTree(xpath);
}

//Cloned for now - export and use from elsewhere

static void copyPropIfMissing(IPropertyTree & target, const char * targetName, IPropertyTree & source, const char * sourceName)
{
    if (source.hasProp(sourceName) && !target.hasProp(targetName))
    {
        if (source.isBinary(sourceName))
        {
            MemoryBuffer value;
            source.getPropBin(sourceName, value);
            target.setPropBin(targetName, value.length(), value.toByteArray());
        }
        else
            target.setProp(targetName, source.queryProp(sourceName));
    }
}

static void copySeparatorPropIfMissing(IPropertyTree & target, const char * targetName, IPropertyTree & source, const char * sourceName)
{
    //Legacy - commas are quoted if they occur in a separator list, so need to remove the leading backslashes
    if (source.hasProp(sourceName) && !target.hasProp(targetName))
    {
        StringBuffer unquoted;
        const char * text = source.queryProp(sourceName);
        while (*text)
        {
            if ((text[0] == '\\') && (text[1] == ','))
                text++;
            unquoted.append(*text++);
        }
        target.setProp(targetName, unquoted);
    }
}

//====================================================================================================================



class LogicalFileResolver
{
public:
    LogicalFileResolver(IUserDescriptor * _user, ResolveOptions _options)
    : user(_user), options(_options)
    {
        meta.setown(createPTree("meta"));
    }

    IPropertyTree * getResultClear() { return meta.getClear(); }

    void processFilename(const char * filename);

protected:
    void ensureHostGroup(const char * name);
    void ensurePlane(const char * plane);
    IPropertyTree * processExternalFile(CDfsLogicalFileName & logicalFilename);
    void processExternalPlane(CDfsLogicalFileName & logicalFilename);
    void processFile(IDistributedFile & file);
    void processFilename(CDfsLogicalFileName & logicalFilename);
    void processMissing(const char * filename);

protected:
    Owned<IPropertyTree> meta;
    Linked<IUserDescriptor> user;
    ResolveOptions options;
};

//---------------------------------------------------------------------------------------------------------------------

void LogicalFileResolver::ensureHostGroup(const char * name)
{
    if (isEmptyString(name))
        return;

    IPropertyTree * storage = ensurePTree(meta, "storage");
    VStringBuffer xpath("hostGroups[@name='%s']", name);
    if (storage->hasProp(xpath))
        return;

    IPropertyTree * hosts = queryHostGroup(name);
    if (!hosts)
        throw makeStringExceptionV(0, "No entry found for hostGroup: '%s'", name);

    storage->addPropTreeArrayItem("hostGroups", LINK(hosts));
}

void LogicalFileResolver::ensurePlane(const char * name)
{
    VStringBuffer xpath("planes[@name='%s']", name);
    IPropertyTree * storage = ensurePTree(meta, "storage");
    if (storage->hasProp(xpath))
        return;

    IPropertyTree * plane = queryStoragePlane(name);
    if (!plane)
        throw makeStringExceptionV(0, "No entry found for plane: '%s'", name);

    storage->addPropTreeArrayItem("planes", LINK(plane));
    ensureHostGroup(plane->queryProp("@hosts"));
}


IPropertyTree * LogicalFileResolver::processExternalFile(CDfsLogicalFileName & logicalFilename)
{
    IPropertyTree * fileMeta = meta->addPropTree("file");
    fileMeta->setProp("@name", logicalFilename.get(false));
    fileMeta->setPropInt("@numParts", 1);
    fileMeta->setProp("@format", "unknown");
    fileMeta->setPropBool("@external", true);

    return fileMeta;
}

void LogicalFileResolver::processExternalPlane(CDfsLogicalFileName & logicalFilename)
{
    IPropertyTree * fileMeta = processExternalFile(logicalFilename);

    //MORE: In the future we could go and grab the meta information from disk for a file and determine the number of parts etc.
    //to provide an implicit multi part file import
    if (options & ROincludeLocation)
    {
        StringBuffer planeName;
        logicalFilename.getExternalPlane(planeName);
        IPropertyTree * plane = createPTree("planes");
        plane = fileMeta->addPropTreeArrayItem("planes", plane);
        plane->setProp("", planeName);
        ensurePlane(planeName);
    }
}

void LogicalFileResolver::processFilename(const char * filename)
{
    CDfsLogicalFileName logicalFilename;
    logicalFilename.set(filename);
    processFilename(logicalFilename);
}

void LogicalFileResolver::processFilename(CDfsLogicalFileName & logicalFilename)
{
    if (logicalFilename.isMulti())
    {
        if (!logicalFilename.isExpanded())
            logicalFilename.expand(user); //expand wild-cards

        unsigned max = logicalFilename.multiOrdinality();
        for (unsigned child=0; child < max; child++)
            processFilename(const_cast<CDfsLogicalFileName &>(logicalFilename.multiItem(child)));
        return;
    }

    if (logicalFilename.isExternal())
    {
        //MORE: There should be some scope checking for these - it seems backwards to call lookup() just to check it has access
        checkLogicalName(logicalFilename.get(), user, true, false, true, nullptr);
        if (logicalFilename.isExternalPlane())
            processExternalPlane(logicalFilename);
        else
            processExternalFile(logicalFilename);
    }
    else
    {
        Owned<IDistributedFile> f = queryDistributedFileDirectory().lookup(logicalFilename, user, true, false, false, nullptr, defaultNonPrivilegedUser);
        if (f)
            processFile(*f);
        else
            processMissing(logicalFilename.get(false));
    }
}

void LogicalFileResolver::processFile(IDistributedFile & file)
{
    IDistributedSuperFile * super = file.querySuperFile();
    if (super)
    {
        unsigned max = super->numSubFiles(false);
        for (unsigned i=0; i < max; i++)
        {
            Owned<IDistributedFile> child = super->getSubFile(i, false);
            processFile(*child);
        }
        return;
    }

    IPropertyTree & fileProperties = file.queryAttributes();
    const char * format = queryFileKind(&file);
    unsigned numParts = file.numParts();

    IPropertyTree * fileMeta = meta->addPropTree("file");
    fileMeta->setProp("@name", file.queryLogicalName());
    fileMeta->setPropInt("@numParts", numParts);
    if (fileProperties.getPropBool("@grouped", false))
        fileMeta->setPropBool("@grouped", true);
    copyPropIfMissing(*fileMeta, "@numRows", fileProperties, "@recordCount");

    offset_t totalSize = 0;
    offset_t fileSize = (options & ROsizes) ? file.getFileSize(true, false) : 0;
    if (options & ROpartinfo)
    {
        for (unsigned part=0; part < numParts; part++)
        {
            IDistributedFilePart & cur = file.queryPart(part);
            IPropertyTree & partProperties = cur.queryAttributes();

            IPropertyTree * partMeta = fileMeta->addPropTree("part");
            if (options & ROsizes)
            {
                offset_t partSize = cur.getFileSize(true, false);
                partMeta->setPropInt64("@rawSize", partSize);
                totalSize += partSize;
            }

            //I don't think this is currently stored, but it probably should be!
            copyPropIfMissing(*partMeta, "@numRows", partProperties, "@numRows");
            if (options & ROdiskinfo)
            {
                copyPropIfMissing(*partMeta, "@diskSize", partProperties, "@compressedSize");
                copyPropIfMissing(*partMeta, "@diskSize", *partMeta, "@rawSize"); // set if not set above?  Not sure about this?
            }
        }

        if (totalSize && fileSize && totalSize != fileSize)
            throw makeStringExceptionV(0, "Inconsistent file size: %" I64F "u v %" I64F "u", fileSize, totalSize);
    }

    fileMeta->setPropInt64("@rawSize", fileSize);
    if (options & ROdiskinfo)
    {
        copyPropIfMissing(*fileMeta, "@diskSize", fileProperties, "@compressedSize");
        copyPropIfMissing(*fileMeta, "@diskSize", *fileMeta, "@rawSize");
    }

    fileMeta->setProp("@format", format);

    //Various pieces of meta information
    fileMeta->setPropInt("@metaCrc", fileProperties.getPropInt("@formatCrc"));
    copyPropIfMissing(*fileMeta, "@ecl", fileProperties, "ECL");
    copyPropIfMissing(*fileMeta, "meta", fileProperties, "_rtlType"); // binary, so doesn't use an attribute

    if (options & ROtimestamps)
    {
        copyPropIfMissing(*fileMeta, "@expireDays", fileProperties, "@expireDays");
        copyPropIfMissing(*fileMeta, "@modified", fileProperties, "@modified");
    }

    if (options & ROincludeLocation)
    {
        unsigned numClusters = file.numClusters();
        for (unsigned cluster=0; cluster < numClusters; cluster++)
        {
            StringBuffer clusterName;
            file.getClusterGroupName(cluster, clusterName);
            unsigned numCopies = file.numCopies(0); // This should depend on the storage subsystem, not the part number
            //MORE: Currently files generated by hthor come back with 2 copies.  Temporarily assume single part files are not replicated
            if (numParts == 1)
                numCopies = 1;
            for (unsigned copy=0; copy < numCopies; copy++)
            {
                StringBuffer planeName(clusterName);
                if (copy != 0)
                    planeName.append("_mirror").append(copy);
                IPropertyTree *plane = fileMeta->addPropTreeArrayItem("planes", createPTree());
                plane->setProp("", planeName);
                ensurePlane(planeName);
            }
        }
    }

    bool blockcompressed = false;
    bool compressed = file.isCompressed(&blockcompressed); //try new decompression, fall back to old unless marked as block
    if (compressed)
        fileMeta->setPropBool("@compressed", true);
    if (blockcompressed)
        fileMeta->setPropBool("@blockCompressed", blockcompressed);

    size32_t dfsSize = fileProperties.getPropInt("@recordSize");
    if (dfsSize != 0)
        fileMeta->setPropInt("@recordSize", dfsSize);

    //Format options are specific to the format of the file that is being read, and can be overriden within the ECL by
    //providing arguments to the file mode in the DATASET statememt.  The values do not use attributes to allow
    //the same setting to be supplied more than once - e.g., for a list
    IPropertyTree * formatOptions = fileMeta->setPropTree("formatOptions", createPTree());
    copyPropIfMissing(*formatOptions, "quote", fileProperties, "@csvQuote");
    copySeparatorPropIfMissing(*formatOptions, "separator", fileProperties, "@csvSeparate");
    copyPropIfMissing(*formatOptions, "terminator", fileProperties, "@csvTerminate");
    copyPropIfMissing(*formatOptions, "escape", fileProperties, "@csvEscape");
    if (isEmptyPTree(formatOptions))
        fileMeta->removeTree(formatOptions);
}

void LogicalFileResolver::processMissing(const char * filename)
{
    IPropertyTree * file = meta->addPropTree("file");
    file->setProp("@name", filename);
    file->setPropBool("@missing", true);
}

//====================================================================================================================

/*
 * Resolve a logical filename, and return the information in a property tree suitable for generating to a YAML file.
 * The logical filename might be an implicit or explicti superfile, or contain references to external files etc.
 *
 * The definition of the format of the YAML file is defined in devdoc/NewFileProcessing.rst
 */

IPropertyTree * resolveLogicalFilenameFromDali(const char * filename, IUserDescriptor * user, ResolveOptions options)
{
    LogicalFileResolver resolver(user, options);
    resolver.processFilename(filename);
    return resolver.getResultClear();
}
