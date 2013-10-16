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
#include "jliball.hpp"
#include "hql.hpp"
#include "hqlmanifest.hpp"

//-------------------------------------------------------------------------------------------------------------------
// Process manifest resources.


class ResourceManifest : public CInterface
{
public:
    ResourceManifest(const char *filename)
        : manifest(createPTreeFromXMLFile(filename))
    {
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
    StringBuffer absFilename;
    StringBuffer dir;
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
                expandDirectory(res, it->query().directoryFiles(mask, true, true), mask, recursive);
            continue;
        }
        StringBuffer reldir;
        IPropertyTree *newRes = manifest->addPropTree("Resource", createPTreeFromIPT(&res));
        newRes->setProp("@filename", splitRelativePath(it->query().queryFilename(), dir, reldir));
        updateResourcePaths(*newRes, dir.str());
    }
}

void ResourceManifest::expandDirectory(IPropertyTree &res, const char *path, const char*mask, bool recursive)
{
    Owned<IDirectoryIterator> it = createDirectoryIterator(path, mask);
    expandDirectory(res, it, mask, recursive);
}


void ResourceManifest::expand()
{
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
            ERRLOG("Error: RESOURCE file '%s' does not exist", filepath);
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

    //xsi namespace required for proper representaion after PTree::setPropBin()
    if (!additionalFiles->hasProp("@xmlns:xsi"))
        additionalFiles->setProp("@xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");

    Owned<IPropertyTreeIterator> resources = manifest->getElements("Resource[@resourcePath]");
    ForEach(*resources)
    {
        IPropertyTree &item = resources->query();
        const char *respath = item.queryProp("@resourcePath");

        VStringBuffer xpath("Resource[@resourcePath='%s']", respath);
        if (!additionalFiles->hasProp(xpath.str()))
        {
            IPropertyTree *resTree = additionalFiles->addPropTree("Resource", createPTree("Resource"));

            const char *filepath = item.queryProp("@originalFilename");
            resTree->setProp("@originalFilename", filepath);
            resTree->setProp("@resourcePath", respath);

            MemoryBuffer content;
            loadResource(filepath, content);
            resTree->setPropBin(NULL, content.length(), content.toByteArray());
        }
    }

    StringBuffer xml;
    toXML(manifest, xml);
    additionalFiles->setProp("Manifest", xml.str());
    additionalFiles->setProp("Manifest/@originalFilename", absFilename.str());
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
        ERRLOG("Error: MANIFEST file '%s' does not exist", filename);
        return false;
    }

    ResourceManifest manifest(filename);
    return manifest.checkResourceFilesExist();
}
