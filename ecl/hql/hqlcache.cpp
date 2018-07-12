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
    EclXmlCachedDefinition(IEclCachedDefinitionCollection * _collection, IEclSource * _definition, IPropertyTree * _root)
    : EclCachedDefinition(_collection, _definition), cacheTree(_root) {}

    virtual timestamp_type getTimeStamp() const override;
    virtual IFileContents * querySimplifiedEcl() const override;
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
        if (optionHash != cacheTree->getPropInt64("@hash"))
            return false;
        return EclCachedDefinition::calcUpToDate(optionHash);
    }


    const char * queryName() const { return cacheTree ? cacheTree->queryProp("@name") : nullptr; }

private:
    Linked<IPropertyTree> cacheTree;
    mutable Owned<IFileContents> simplified;
};

timestamp_type EclXmlCachedDefinition::getTimeStamp() const
{
    if (!cacheTree)
        return 0;
    return cacheTree->getPropInt64("@ts");
}

IFileContents * EclXmlCachedDefinition::querySimplifiedEcl() const
{
    if (!cacheTree)
        return nullptr;
    if (simplified)
        return simplified;
    const char * ecl = cacheTree->queryProp("Simplified");
    if (!ecl)
        return nullptr;
    simplified.setown(createFileContentsFromText(ecl, NULL, false, NULL, 0));
    return simplified;
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
    EclFileCachedDefinition(IEclCachedDefinitionCollection * _collection, IEclSource * _definition, IPropertyTree * _root, IFile * _file)
    : EclXmlCachedDefinition(_collection, _definition, _root), file(_file)
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
    EclXmlCachedDefinitionCollection(IEclRepository * _repository, IPropertyTree * _root)
    : EclCachedDefinitionCollection(_repository), root(_root) {}

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

IEclCachedDefinitionCollection * createEclXmlCachedDefinitionCollection(IEclRepository * repository, IPropertyTree * root)
{
    return new EclXmlCachedDefinitionCollection(repository, root);
}

//---------------------------------------------------------------------------------------------------------------------

class EclFileCachedDefinitionCollection : public EclCachedDefinitionCollection
{
public:
    EclFileCachedDefinitionCollection(IEclRepository * _repository, const char * _root)
    : EclCachedDefinitionCollection(_repository), root(_root)
    {
        makeAbsolutePath(root, false);
        addPathSepChar(root);
    }

    virtual IEclCachedDefinition * createDefinition(const char * eclpath) override;

private:
    StringBuffer root;
};


IEclCachedDefinition * EclFileCachedDefinitionCollection::createDefinition(const char * eclpath)
{
    StringBuffer filename(root);
    convertSelectsToPath(filename, eclpath);
    filename.append(".cache");

    Owned<IFile> file = createIFile(filename);
    Owned<IPropertyTree> root;
    if (file->exists())
    {
        try
        {
            root.setown(createPTree(*file));
        }
        catch (IException * e)
        {
            DBGLOG(e);
            e->Release();
        }
    }

    Owned<IEclSource> definition = repository->getSource(eclpath);
    return new EclFileCachedDefinition(this, definition, root, file);
}


extern HQL_API IEclCachedDefinitionCollection * createEclFileCachedDefinitionCollection(IEclRepository * repository, const char * root)
{
    return new EclFileCachedDefinitionCollection(repository, root);
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

static IHqlExpression * createSimplifiedDefinitionFromType(ITypeInfo * type, bool implicitConstantType=false)
{
    switch (type->getTypeCode())
    {
    case type_scope: // These may be possible - if the scope is not a forward scope or derived from another scope
        return nullptr;
    case type_pattern:
    case type_rule:
    case type_token:
        //Possible, but the default testing code doesn't work
        return nullptr;
    case type_real:
    case type_decimal:
        return createNullExpr(type);
    case type_int:
        if (implicitConstantType)
        {
            // This code is here to ensure the simplified definition is compatible with the expression parsed from
            // its ECL representation. ECL integer constants cannot specify a type - and are always parsed as int8,
            // so the constants are created as int8 for consistency.
            Owned<ITypeInfo> tempType = makeIntType(8, true);
            return createNullExpr(tempType);
        }
        return createNullExpr(type);
    }

    return nullptr;
}

static IHqlExpression * createSimplifiedBodyDefinition(IHqlExpression * expr, bool implicitConstantType=false)
{
    if (expr->isFunction())
    {
        if (!expr->isFunctionDefinition())
            return nullptr;
        OwnedHqlExpr newBody = createSimplifiedBodyDefinition(expr->queryChild(0)->queryBody());
        if (!newBody)
            return nullptr;

        IHqlExpression * params = expr->queryChild(1);
        HqlExprArray funcArgs;
        HqlExprArray dummyAttrs;
        ForEachChild(i, params)
        {
            IHqlExpression * param = params->queryChild(i)->queryBody();
            type_t tc = param->queryType()->getTypeCode();

            switch (tc)
            {
            case type_scope:
            case type_table:
            case type_row:
            case type_set:
            case type_record:
            case type_enumerated:
            case type_groupedtable:
                return nullptr;
            }
            funcArgs.append(*createParameter(param->queryId(),(unsigned)expr->querySequenceExtra(), getFullyUnqualifiedType(param->queryType()), dummyAttrs));
        }

        OwnedHqlExpr formals = createSortList(funcArgs);

        IHqlExpression * origDefaults = expr->queryChild(2);
        OwnedHqlExpr newDefaults;
        if (origDefaults && origDefaults->numChildren())
        {
            HqlExprArray newDefaultsArray;
            ForEachChild(idx, origDefaults)
            {
                IHqlExpression * defaultValue = origDefaults->queryChild(idx);
                if (defaultValue->getOperator() == no_omitted)
                    newDefaultsArray.append(*(LINK(defaultValue)));
                else
                {
                    OwnedHqlExpr newDefault = createSimplifiedBodyDefinition(defaultValue->queryBody(), true);
                    if (!newDefault)
                        return nullptr;
                    newDefaultsArray.append(*(newDefault.getClear()));
                }
            }
            newDefaults.setown(createSortList(newDefaultsArray));
        }
        return createFunctionDefinition(expr->queryId(), newBody.getClear(), formals.getClear(), newDefaults.getClear(), nullptr);
    }
    switch (expr->getOperator())
    {
    case no_typedef:
    case no_enum:
    case no_macro:
        return nullptr;
    }
    ITypeInfo * type = getFullyUnqualifiedType(expr->queryType());
    if (!type)
        return nullptr;

    return createSimplifiedDefinitionFromType(type, implicitConstantType);
}

IHqlExpression * createSimplifiedDefinition(IHqlExpression * expr)
{
    if (!expr)
        return nullptr;
    OwnedHqlExpr simple = createSimplifiedBodyDefinition(expr);
    if (simple)
        return expr->cloneAnnotation(simple);
    return nullptr;
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
    Linked<IPropertyTree> archive;
    IEclCachedDefinitionCollection * collection;
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
