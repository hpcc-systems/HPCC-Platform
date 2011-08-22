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
        : manifest(createPTreeFromXMLFile(filename)), origPath(filename)
    {
        makeAbsolutePath(filename, absFilename);
        splitDirTail(absFilename, dir);
    }

    void addToArchive(IPropertyTree *archive);
    void loadResource(const char *filepath, MemoryBuffer &content);
    bool checkResourceFilesExist();
public:
    Owned<IPropertyTree> manifest;
    StringAttr origPath;
    StringBuffer absFilename;
    StringBuffer dir;
};

bool ResourceManifest::checkResourceFilesExist()
{
    Owned<IPropertyTreeIterator> resources = manifest->getElements("Resource[@filename]");
    ForEach(*resources)
    {
        const char *filename = resources->query().queryProp("@filename");
        StringBuffer fullpath;
        if (!isAbsolutePath(filename))
            fullpath.append(dir);
        fullpath.append(filename);
        if (!checkFileExists(fullpath.str()))
        {
            StringBuffer absResPath;
            makeAbsolutePath(fullpath.str(), absResPath);
            ERRLOG("Error: RESOURCE file '%s' does not exist", absResPath.str());
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

    StringBuffer xml;
    toXML(manifest, xml);
    additionalFiles->setProp("Manifest", xml.str());
    additionalFiles->setProp("Manifest/@originalFilename", origPath.sget());

    Owned<IPropertyTreeIterator> resources = manifest->getElements("Resource[@filename]");
    ForEach(*resources)
    {
        StringBuffer absResPath;
        const char *filename = resources->query().queryProp("@filename");
        VStringBuffer xpath("Resource[@originalFilename='%s']", filename);
        if (!additionalFiles->hasProp(xpath.str()))
        {
            if (!isAbsolutePath(filename))
            {
                StringBuffer relResPath(dir);
                relResPath.append(filename);
                makeAbsolutePath(relResPath.str(), absResPath);
            }
            else
                absResPath.append(filename);

            IPropertyTree *resTree = additionalFiles->addPropTree("Resource", createPTree("Resource"));
            resTree->setProp("@originalFilename", filename);

            MemoryBuffer content;
            loadResource(absResPath.str(), content);
            resTree->setPropBin(NULL, content.length(), content.toByteArray());
        }
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
        ERRLOG("Error: MANIFEST file '%s' does not exist", filename);
        return false;
    }

    ResourceManifest manifest(filename);
    return manifest.checkResourceFilesExist();
}
