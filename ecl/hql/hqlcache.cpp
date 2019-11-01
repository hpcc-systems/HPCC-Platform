/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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
#include "hqlcache.hpp"
#include "hqlcollect.hpp"
#include "hqlexpr.hpp"
#include "hqlutil.hpp"
#include "hqlerrors.hpp"
#include "junicode.hpp"
#include "hqlplugins.hpp"

//---------------------------------------------------------------------------------------------------------------------

static bool traceCache = false;
void setTraceCache(bool value)
{
    traceCache = value;
}

/*
 * Base class for implementing a cache entry for a single source file.
 */
class EclCachedDefinition : public CInterfaceOf<IEclCachedDefinition>
{
public:
    EclCachedDefinition(IEclCachedDefinitionCollection * _collection, IEclSource * _definition)
    : collection(_collection), definition(_definition) {}

    virtual bool isUpToDate(hash64_t optionHash) const override;
    virtual IEclSource * queryOriginal() const override { return definition; }

protected:
    virtual bool calcUpToDate(hash64_t optionHash) const;

protected:
    mutable bool cachedUpToDate = false;
    mutable bool upToDate = false;
    IEclCachedDefinitionCollection * collection = nullptr;
    Linked<IEclSource> definition;
};

bool EclCachedDefinition::isUpToDate(hash64_t optionHash) const
{
    //MORE: Improve thread safety if this object is shared between multiple threads.
    if (!cachedUpToDate)
    {
        cachedUpToDate = true;
        upToDate = true; // They should not occur, but initialise to ensure recursive references are treated correctly
        upToDate = calcUpToDate(optionHash);
        if (traceCache && definition)
        {
            const char * name = str(definition->queryEclId());
            IFileContents * contents = definition->queryFileContents();
            if (contents)
                name = str(contents->querySourcePath());
            if (upToDate)
                DBGLOG("Cache entry %s is up to date", name);
            else
                DBGLOG("Cache entry %s is NOT up to date", name);
        }
    }
    return upToDate;
}

bool EclCachedDefinition::calcUpToDate(hash64_t optionHash) const
{
    if (!definition)
        return false;

    IFileContents * contents = definition->queryFileContents();
    if (!contents)
        return false;

    //If the cached information is younger than the original ecl then not valid
    timestamp_type originalTs = contents->getTimeStamp();
    if ((originalTs == 0) || (getTimeStamp() < originalTs))
        return false;

    StringArray dependencies;
    queryDependencies(dependencies);
    ForEachItemIn(i, dependencies)
    {
        Owned<IEclCachedDefinition> match = collection->getDefinition(dependencies.item(i));
        if (!match || !match->isUpToDate(optionHash))
            return false;
    }
    return true;
}


//---------------------------------------------------------------------------------------------------------------------

/*
 * class for representing a cache entry defined by a block of xml
 */
class EclXmlCachedDefinition : public EclCachedDefinition
{
public:
    EclXmlCachedDefinition(IEclCachedDefinitionCollection * _collection, IEclSource * _definition, IPropertyTree * _cacheTree)
    : EclCachedDefinition(_collection, _definition), cacheTree(_cacheTree) {}

    virtual timestamp_type getTimeStamp() const override;
    virtual void queryDependencies(StringArray & values) const override;
    virtual bool hasKnownDependents() const override
    {
        return !cacheTree->getPropBool("@isMacro");
    }

protected:
    virtual bool calcUpToDate(hash64_t optionHash) const override
    {
        if (!cacheTree)
            return false;
        if (optionHash != (hash64_t)cacheTree->getPropInt64("@hash"))
            return false;
        return EclCachedDefinition::calcUpToDate(optionHash);
    }

    const char * queryName() const { return cacheTree ? cacheTree->queryProp("@name") : nullptr; }

private:
    Linked<IPropertyTree> cacheTree;
};

timestamp_type EclXmlCachedDefinition::getTimeStamp() const
{
    if (!cacheTree)
        return 0;
    return cacheTree->getPropInt64("@ts");
}

void EclXmlCachedDefinition::queryDependencies(StringArray & values) const
{
    if (!cacheTree)
        return;

    StringBuffer fullname;
    Owned<IPropertyTreeIterator> iter = cacheTree->getElements("Attr/Depend");
    ForEach(*iter)
    {
        const char * module = iter->query().queryProp("@module");
        const char * attr = iter->query().queryProp("@name");
        if (!isEmptyString(module))
        {
            fullname.clear().append(module).append(".").append(attr);
            values.append(fullname);
        }
        else
            values.append(attr);
    }
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * class for representing a cache entry defined by a single file (in xml format)
 */
class EclFileCachedDefinition : public EclXmlCachedDefinition
{
public:
    EclFileCachedDefinition(IEclCachedDefinitionCollection * _collection, IEclSource * _definition, IPropertyTree * _cacheTree, IFile * _file)
    : EclXmlCachedDefinition(_collection, _definition, _cacheTree), file(_file)
    {
    }

    virtual timestamp_type getTimeStamp() const override;

private:
    Linked<IFile> file;
};

timestamp_type EclFileCachedDefinition::getTimeStamp() const
{
    if (!file)
        return 0;
    return ::getTimeStamp(file);
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * base class for representing a cache of information extracted from the ecl definitions
 */
class EclCachedDefinitionCollection : public CInterfaceOf<IEclCachedDefinitionCollection>
{
public:
    EclCachedDefinitionCollection(IEclRepository * _repository)
    : repository(_repository){}

    virtual IEclCachedDefinition * getDefinition(const char * eclpath) override;

protected:
    virtual IEclCachedDefinition * createDefinition(const char * eclpath) = 0;

protected:
    Linked<IEclRepository> repository;
    MapStringToMyClass<IEclCachedDefinition> map;
};


IEclCachedDefinition * EclCachedDefinitionCollection::getDefinition(const char * eclpath)
{
    StringBuffer lowerPath;
    lowerPath.append(eclpath).toLowerCase();
    IEclCachedDefinition * match = map.getValue(lowerPath);
    if (match)
        return LINK(match);

    Owned<IEclCachedDefinition> cached = createDefinition(eclpath);
    map.setValue(lowerPath, cached);
    return cached.getClear();
}

//---------------------------------------------------------------------------------------------------------------------

class EclXmlCachedDefinitionCollection : public EclCachedDefinitionCollection
{
public:
    EclXmlCachedDefinitionCollection(IEclRepository * _repository, IPropertyTree * _cacheTree)
    : EclCachedDefinitionCollection(_repository), root(_cacheTree) {}

    virtual IEclCachedDefinition * createDefinition(const char * eclpath) override;

private:
    Linked<IPropertyTree> root;
};



IEclCachedDefinition * EclXmlCachedDefinitionCollection::createDefinition(const char * eclpath)
{
    StringBuffer xpath;
    xpath.append("Cache[@name='").appendLower(eclpath).append("']");
    Owned<IPropertyTree> resolved = root->getBranch(xpath);
    Owned<IEclSource> definition = repository->getSource(eclpath);
    return new EclXmlCachedDefinition(this, definition, resolved);
}

IEclCachedDefinitionCollection * createEclXmlCachedDefinitionCollection(IEclRepository * repository, IPropertyTree * cacheTree)
{
    return new EclXmlCachedDefinitionCollection(repository, cacheTree);
}

//---------------------------------------------------------------------------------------------------------------------

class EclFileCachedDefinitionCollection : public EclCachedDefinitionCollection
{
public:
    EclFileCachedDefinitionCollection(IEclRepository * _repository, const char * _cacheRootPath)
    : EclCachedDefinitionCollection(_repository), cacheRootPath(_cacheRootPath)
    {
        makeAbsolutePath(cacheRootPath, false);
        addPathSepChar(cacheRootPath);
    }

    virtual IEclCachedDefinition * createDefinition(const char * eclpath) override;

private:
    StringBuffer cacheRootPath;
};


IEclCachedDefinition * EclFileCachedDefinitionCollection::createDefinition(const char * eclpath)
{
    StringBuffer filename(cacheRootPath);
    convertSelectsToPath(filename, eclpath);
    filename.append(".cache");

    Owned<IFile> file = createIFile(filename);
    Owned<IPropertyTree> cacheTree;
    if (file->exists())
    {
        try
        {
            cacheTree.setown(createPTree(*file));
        }
        catch (IException * e)
        {
            DBGLOG(e);
            e->Release();
        }
    }

    Owned<IEclSource> definition = repository->getSource(eclpath);
    return new EclFileCachedDefinition(this, definition, cacheTree, file);
}


extern HQL_API IEclCachedDefinitionCollection * createEclFileCachedDefinitionCollection(IEclRepository * repository, const char * _cacheRootPath)
{
    return new EclFileCachedDefinitionCollection(repository, _cacheRootPath);
}


//---------------------------------------------------------------------------------------------------------------------

void convertSelectsToPath(StringBuffer & filename, const char * eclPath)
{
    for(;;)
    {
        const char * dot = strchr(eclPath, '.');
        if (!dot)
            break;
        filename.appendLower(dot-eclPath, eclPath);
        addPathSepChar(filename);
        eclPath = dot + 1;
    }
    filename.appendLower(eclPath);
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * split a full ecl path (e.g. a.b.c.d) into a module and a tail name (e.g. a.b.c, d)
 */
static const char * splitFullname(StringBuffer & module, const char * fullname)
{
    const char * dot = strrchr(fullname, '.');
    if (dot)
    {
        module.append(dot-fullname, fullname);
        return dot+1;
    }
    else
        return fullname;
}


void getFileContentText(StringBuffer & result, IFileContents * contents)
{
    unsigned len = contents->length();
    const char * text = contents->getText();
    if ((len >= 3) && (memcmp(text, UTF8_BOM, 3) == 0))
    {
        len -= 3;
        text += 3;
    }
    result.append(len, text);
}

void setDefinitionText(IPropertyTree * target, const char * prop, IFileContents * contents, bool checkDirty)
{
    StringBuffer sillyTempBuffer;
    getFileContentText(sillyTempBuffer, contents);  // We can't rely on IFileContents->getText() being null terminated..
    target->setProp(prop, sillyTempBuffer);

    ISourcePath * sourcePath = contents->querySourcePath();
    target->setProp("@sourcePath", str(sourcePath));
    if (checkDirty && contents->isDirty())
    {
        target->setPropBool("@dirty", true);
    }

    timestamp_type ts = contents->getTimeStamp();
    if (ts)
        target->setPropInt64("@ts", ts);
}

//---------------------------------------------------------------------------------------------------------------------

/*
 * Class for creating an archive directly from the cache and the original source files.
 */
class ArchiveCreator
{
public:
    ArchiveCreator(IEclCachedDefinitionCollection * _collection) : collection(_collection)
    {
        archive.setown(createAttributeArchive());
    }
    ArchiveCreator(IEclCachedDefinitionCollection * _collection, IPropertyTree * _archive) : collection(_collection), archive(_archive)
    {
    }

    void processDependency(const char * name);
    IPropertyTree * getArchive() { return archive.getClear(); }

protected:
    void createArchiveItem(const char * fullName, IEclSource * original);

protected:
    IEclCachedDefinitionCollection * collection = nullptr;
    Linked<IPropertyTree> archive;
};


void ArchiveCreator::processDependency(const char * fullName)
{
    if (queryArchiveEntry(archive, fullName))
        return;

    Owned<IEclCachedDefinition> definition = collection->getDefinition(fullName);
    IEclSource * original = definition->queryOriginal();
    if (!original)
        throwError1(HQLERR_CacheMissingOriginal, fullName);

    createArchiveItem(fullName, original);

    StringArray dependencies;
    definition->queryDependencies(dependencies);
    ForEachItemIn(i, dependencies)
        processDependency(dependencies.item(i));
}

void ArchiveCreator::createArchiveItem(const char * fullName, IEclSource * original)
{
    if (original->queryType() == ESTdefinition)
    {
        StringBuffer moduleName;
        const char * attrName = splitFullname(moduleName, fullName);

        IPropertyTree * module = queryEnsureArchiveModule(archive, moduleName, nullptr);
        assertex(!queryArchiveAttribute(module, attrName));
        IPropertyTree * attr = createArchiveAttribute(module, attrName);
        setDefinitionText(attr, "", original->queryFileContents(), false);
    }
    else
    {
        Owned<IProperties> properties = original->getProperties();
        IPropertyTree * module = queryEnsureArchiveModule(archive, fullName, nullptr);
        IFileContents * contents = original->queryFileContents();
        setDefinitionText(module, "Text", contents, false);

        StringBuffer s;
        unsigned flagsToSave = (properties->getPropInt(str(flagsAtom), 0) & PLUGIN_SAVEMASK);
        if (flagsToSave)
            module->setPropInt("@flags", flagsToSave);
        properties->getProp(str(pluginAtom), s.clear());
        if (s.length())
        {
            module->setProp("@fullname", s.str());

            StringBuffer pluginName(s.str());
            getFileNameOnly(pluginName, false);
            module->setProp("@plugin", pluginName.str());
        }
        properties->getProp(str(versionAtom), s.clear());
        if (s.length())
            module->setProp("@version", s.str());
    }
}


IPropertyTree * createArchiveFromCache(IEclCachedDefinitionCollection * collection, const char * root)
{
    ArchiveCreator creator(collection);
    creator.processDependency(root);
    return creator.getArchive();
}

extern HQL_API void updateArchiveFromCache(IPropertyTree * archive, IEclCachedDefinitionCollection * collection, const char * root)
{
    ArchiveCreator creator(collection, archive);
    creator.processDependency(root);
}

//---------------------------------------------------------------------------------------------------------------------

static void extractFile(const char * path, const char * moduleName, const char * attrName, const char * text, timestamp_type ts)
{
    StringBuffer filename;
    filename.append(path);
    if (moduleName && *moduleName)
        convertSelectsToPath(filename, moduleName);
    if (attrName)
    {
        addPathSepChar(filename);
        convertSelectsToPath(filename, attrName);
        filename.append(".ecl");
    }
    else
        filename.append(".ecllib");
    recursiveCreateDirectoryForFile(filename);

    Owned<IFile> file = createIFile(filename);
    Owned<IFileIO> io = file->open(IFOcreate);
    if (text)
        io->write(0, strlen(text), text);
    io.clear();
    if (ts)
    {
        CDateTime timeStamp;
        timeStamp.setTimeStamp(ts);
        file->setTime(&timeStamp, &timeStamp, &timeStamp);
    }
}


extern HQL_API void expandArchive(const char * path, IPropertyTree * archive, bool includePlugins)
{
    StringBuffer baseFilename;
    makeAbsolutePath(path, baseFilename, false);
    addPathSepChar(baseFilename);

    Owned<IPropertyTreeIterator> modules = archive->getElements("Module");
    ForEach(*modules)
    {
        IPropertyTree & curModule = modules->query();
        const char * moduleName = curModule.queryProp("@name");
        if (curModule.hasProp("Text"))
        {
            if (includePlugins || !curModule.hasProp("@plugin"))
                extractFile(baseFilename, moduleName, nullptr, curModule.queryProp("Text"), curModule.getPropInt64("@ts"));
        }
        else
        {
            Owned<IPropertyTreeIterator> attrs = curModule.getElements("Attribute");
            ForEach(*attrs)
            {
                IPropertyTree & curAttr = attrs->query();
                const char * attrName = curAttr.queryProp("@name");
                extractFile(baseFilename, moduleName, attrName, curAttr.queryProp(""), curAttr.getPropInt64("@ts"));
            }
        }
    }
}
