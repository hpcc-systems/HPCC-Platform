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
#include "hql.hpp"
#include "hqlutil.hpp"
#include "hqlmanifest.hpp"
#include "codesigner.hpp"

//-------------------------------------------------------------------------------------------------------------------
// Process manifest resources.


class ResourceManifest : public CInterface
{
public:
    ResourceManifest(const char *filename)
    {
        try
        {
            fileContents.loadFile(filename, false);
            const char *xml = fileContents.str();
            StringBuffer body;
            // Check for signature
            if (queryCodeSigner().hasSignature(fileContents))
            {
                // Note - we do not check the signature here - we are creating an archive, and typically that means we
                // are on the client machine, while the signature can only be checked on the server where the keys are installed.
                xml = queryCodeSigner().stripSignature(fileContents, body).str();
                isSigned = true;
            }
            manifest.setown(createPTreeFromXMLString(xml));
        }
        catch (IException * E)
        {
            StringBuffer msg;
            E->errorMessage(msg);
            auto code = E->errorCode();
            auto aud = E->errorAudience();
            E->Release();
            throw makeStringExceptionV(aud, code, "While loading manifest file %s: %s", filename, msg.str());
        }
        makeAbsolutePath(filename, absFilename);
        splitDirTail(absFilename, dir);
        expand();
    }

    void addToArchive(IPropertyTree *archive);
    void loadResource(const char *filepath, MemoryBuffer &content);
    bool checkResourceFilesExist();
private:
    bool loadInclude(IPropertyTree &include, const char *dir);
    void expand();
    void expandDirectory(IPropertyTree &res, IDirectoryIterator *it, const char*mask, bool recursive);
    void expandDirectory(IPropertyTree &res, const char *path, const char*mask, bool recursive);

public:
    Owned<IPropertyTree> manifest;
    StringBuffer fileContents;
    StringBuffer absFilename;
    StringBuffer dir;
    bool isSigned = false;
};

void updateResourcePaths(IPropertyTree &resource, const char *dir)
{
    StringBuffer filepath;
    makeAbsolutePath(resource.queryProp("@filename"), dir, filepath);
    resource.setProp("@originalFilename", filepath.str());

    StringBuffer respath;
    makePathUniversal(filepath.str(), respath);
    resource.setProp("@resourcePath", respath.str());
}

bool ResourceManifest::loadInclude(IPropertyTree &include, const char *dir)
{
    const char *filename = include.queryProp("@filename");
    StringBuffer includePath;
    makeAbsolutePath(filename, dir, includePath);

    VStringBuffer xpath("Include[@originalFilename='%s']", includePath.str());
    if (manifest->hasProp(xpath.str()))
        return false;

    include.setProp("@originalFilename", includePath.str());
    StringBuffer includeDir;
    splitDirTail(includePath, includeDir);

    Owned<IPropertyTree> manifestInclude = createPTreeFromXMLFile(includePath.str());
    Owned<IPropertyTreeIterator> it = manifestInclude->getElements("*");
    ForEach(*it)
    {
        IPropertyTree &item = it->query();
        if (streq(item.queryName(), "Resource"))
            updateResourcePaths(item, includeDir.str());
        else if (streq(item.queryName(), "Include"))
        {
            if (!loadInclude(item, includeDir.str()))
                continue;
        }
        manifest->addPropTree(item.queryName(), LINK(&item));
    }
    return true;
}

void ResourceManifest::expandDirectory(IPropertyTree &res, IDirectoryIterator *it, const char*mask, bool recursive)
{
    if (!it)
        return;
    ForEach(*it)
    {
        if (it->isDir())
        {
            if (recursive)
                expandDirectory(res, it->query().directoryFiles(mask, false, true), mask, recursive);
            continue;
        }
        StringBuffer reldir;
        Owned<IPropertyTree> newRes = createPTreeFromIPT(&res);
        reldir.append(splitRelativePath(it->query().queryFilename(), dir, reldir));
        VStringBuffer xpath("Resource[@filename='%s']", reldir.str());
        if (manifest->hasProp(xpath))
            continue;
        newRes->setProp("@filename", reldir.str());
        updateResourcePaths(*newRes, dir.str());
        if (manifest->hasProp(xpath.setf("resource[@resourcePath='%s']", newRes->queryProp("@resourcePath"))))
            continue;
        manifest->addPropTree("Resource", newRes.getClear());
    }
}

void ResourceManifest::expandDirectory(IPropertyTree &res, const char *path, const char*mask, bool recursive)
{
    Owned<IDirectoryIterator> it = createDirectoryIterator(path, mask);
    expandDirectory(res, it, mask, recursive);
}


void ResourceManifest::expand()
{
	manifest->setProp("@manifestDir", dir.str());
	Owned<IPropertyTreeIterator> resources = manifest->getElements("Resource[@filename]");
    ForEach(*resources)
        updateResourcePaths(resources->query(), dir.str());
    Owned<IPropertyTreeIterator> includes = manifest->getElements("Include[@filename]");
    ForEach(*includes)
        loadInclude(includes->query(), dir.str());
    resources.setown(manifest->getElements("Resource[@filename]"));
    ForEach(*resources)
    {
        IPropertyTree &res = resources->query();
        const char *name = res.queryProp("@originalFilename");
        if (containsFileWildcard(name))
        {
            StringBuffer wildpath;
            const char *tail = splitDirTail(name, wildpath);
            expandDirectory(res, wildpath, tail, res.getPropBool("@recursive"));
            manifest->removeTree(&res);
        }
    }
}

bool ResourceManifest::checkResourceFilesExist()
{
    Owned<IPropertyTreeIterator> resources = manifest->getElements("Resource[@originalFilename]");
    ForEach(*resources)
    {
        const char *filepath = resources->query().queryProp("@originalFilename");
        if (!checkFileExists(filepath))
        {
            UERRLOG("Error: RESOURCE file '%s' does not exist", filepath);
            return false;
        }
    }
    return true;
}

void ResourceManifest::loadResource(const char *filepath, MemoryBuffer &content)
{
    Owned <IFile> f = createIFile(filepath);
    Owned <IFileIO> fio = f->open(IFOread);
    read(fio, 0, (size32_t) f->size(), content);
}

void ResourceManifest::addToArchive(IPropertyTree *archive)
{
    IPropertyTree *additionalFiles = ensurePTree(archive, "AdditionalFiles");

    //xsi namespace required for proper representation after PTree::setPropBin()
    if (!additionalFiles->hasProp("@xmlns:xsi"))
        additionalFiles->setProp("@xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");

    Owned<IPropertyTreeIterator> resources = manifest->getElements("Resource[@resourcePath]");
    ForEach(*resources)
    {
        IPropertyTree &item = resources->query();
        const char *md5 = item.queryProp("@md5");
        const char *filename = item.queryProp("@filename");
        MemoryBuffer content;
        if (isSigned)
        {
            if (md5)
            {
                VStringBuffer xpath("Resource[@filename='%s'][@md5='%s']", filename, md5);
                if (!additionalFiles->hasProp(xpath.str()))
                {
                    IPropertyTree *resTree = additionalFiles->addPropTree("Resource", createPTree("Resource"));
                    resTree->setProp("@filename", filename);
                    resTree->setProp("@md5", md5);
                    loadResource(filename, content);
                    resTree->setPropBin(NULL, content.length(), content.toByteArray());
                }
            }
            else
                throw makeStringExceptionV(0, "Signed manifest %s must provide MD5 values for referenced resource %s", absFilename.str(), filename);
        }
        else
        {
            const char *respath = item.queryProp("@resourcePath");
            VStringBuffer xpath("Resource[@resourcePath='%s']", respath);
            if (!additionalFiles->hasProp(xpath.str()))
            {
                IPropertyTree *resTree = additionalFiles->addPropTree("Resource", createPTree("Resource"));
                const char *filepath = item.queryProp("@originalFilename");
                resTree->setProp("@originalFilename", filepath);
                resTree->setProp("@resourcePath", respath);
                loadResource(filepath, content);
                resTree->setPropBin(NULL, content.length(), content.toByteArray());
            }
        }
        if (md5)
        {
            StringBuffer calculated;
            md5_data(content, calculated);
            if (!strieq(calculated, md5))
                throw makeStringExceptionV(0, "MD5 mismatch on file %s in manifest %s", filename, absFilename.str());
        }
    }

    IPropertyTree *manifestWrapper = additionalFiles->addPropTree("Manifest", createPTree("Manifest", ipt_none));
    manifestWrapper->setProp("@originalFilename", absFilename.str());
    manifestWrapper->setPropBool("@isSigned", isSigned);
    if (isSigned)
        manifestWrapper->setProp(NULL, fileContents.str());
    else
    {
        StringBuffer xml;
        toXML(manifest, xml);
        manifestWrapper->setProp(NULL, xml.str());
    }
}

void addManifestResourcesToArchive(IPropertyTree *archive, const char *filename)
{
    ResourceManifest manifest(filename);
    manifest.addToArchive(archive);
}

bool isManifestFileValid(const char *filename)
{
    if (!checkFileExists(filename))
    {
        UERRLOG("Error: MANIFEST file '%s' does not exist", filename);
        return false;
    }
    ResourceManifest manifest(filename);
    return manifest.checkResourceFilesExist();
}
