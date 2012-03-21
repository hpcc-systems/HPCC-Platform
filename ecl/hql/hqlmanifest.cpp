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

void ResourceManifest::expand()
{
    Owned<IPropertyTreeIterator> resources = manifest->getElements("Resource[@filename]");
    ForEach(*resources)
        updateResourcePaths(resources->query(), dir.str());
    Owned<IPropertyTreeIterator> includes = manifest->getElements("Include[@filename]");
    ForEach(*includes)
        loadInclude(includes->query(), dir.str());
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
