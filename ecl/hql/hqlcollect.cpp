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

#include "hql.hpp"
#include "hqlcollect.hpp"

#include "eclrtl.hpp"
#include "jfile.hpp"
#include "jiter.ipp"
#include "hqlplugins.hpp"
#include "hqlexpr.hpp"
#include "hqlerrors.hpp"

#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif

inline bool isNullOrBlank(const char * s) { return !s || !*s; }

//DLLs should call CPluginCtx to manage memory
class CPluginCtx : implements IPluginContext
{
public:
    void * ctxMalloc(size_t size)               { return rtlMalloc(size); }
    void * ctxRealloc(void * _ptr, size_t size) { return rtlRealloc(_ptr, size); }
    void   ctxFree(void * _ptr)                 { rtlFree(_ptr); }
    char * ctxStrdup(char * _ptr)               { return strdup(_ptr); }
};

static CPluginCtx PluginCtx;


class CEclSourceCollection : implements IEclSourceCollection, public CInterface
{
public:
    IMPLEMENT_IINTERFACE
};


//-------------------------------------------------------------------------------------------------------------------

//MORE: Split this into a common base and a Source?
class CEclSource : public CInterfaceOf<IEclSource>
{
public:
    CEclSource(IIdAtom * _eclId, EclSourceType _type) : eclId(_eclId), type(_type) { }

//interface IEclSource
    virtual IProperties * getProperties() { return NULL; }
    virtual IIdAtom * queryEclId() const { return eclId; }
    virtual EclSourceType queryType() const { return type; }

// new virtuals implemented by the child classes
    virtual IEclSource * getSource(IIdAtom * searchName) = 0;
    virtual IEclSourceIterator * getContained() = 0;

protected:
    EclSourceType type;
    IIdAtom * eclId;
};


class CEclCollection : public CEclSource
{
public:
    CEclCollection(IIdAtom * _eclName, EclSourceType _type) : CEclSource(_eclName, _type) { expandedChildren = false; fullyDefined = false; }

//interface IEclSource
    virtual IFileContents * queryFileContents() { return NULL; }

//CEclSource virtuals
    virtual IEclSource * getSource(IIdAtom * searchName);
    virtual IEclSourceIterator * getContained();
    virtual void populateChildren() {}
    virtual void populateDefinition() {}

protected:
    void ensureChildren();
    void ensureDefinition();
    CEclSource * find(IIdAtom * searchName);

protected:
    IArrayOf<IEclSource> contents;
    CriticalSection cs;
    bool expandedChildren;
    bool fullyDefined;
};


IEclSource * CEclCollection::getSource(IIdAtom * searchName)
{
    ensureChildren();
    IEclSource * match = find(searchName);
    return LINK(match);
}

IEclSourceIterator * CEclCollection::getContained()
{
    ensureChildren();
    return new CArrayIteratorOf<IEclSource, IEclSourceIterator>(contents, 0, NULL);
}

CEclSource * CEclCollection::find(IIdAtom * name)
{
    ForEachItemIn(i, contents)
    {
        IEclSource & cur = contents.item(i);
        if (lower(cur.queryEclId()) == lower(name))
            return &static_cast<CEclSource &>(cur);
    }
    return NULL;
}


void CEclCollection::ensureChildren()
{
    CriticalBlock block(cs);
    if (expandedChildren)
        return;
    expandedChildren = true;
    populateChildren();
}

void CEclCollection::ensureDefinition()
{
    CriticalBlock block(cs);
    if (fullyDefined)
        return;
    fullyDefined = true;
    populateDefinition();
}

//-------------------------------------------------------------------------------------------------------------------

class FileSystemFile : public CEclSource
{
public:
    FileSystemFile(EclSourceType _type, IFile & _file, bool _allowPlugins);
    FileSystemFile(const char * eclName, IFileContents * _fileContents);

//interface IEclSource
    virtual IProperties * getProperties();
    virtual IFileContents * queryFileContents() { return fileContents; }

//CEclSource virtuals
    virtual IEclSource * getSource(IIdAtom * searchName) { return NULL; }
    virtual IEclSourceIterator * getContained() { return NULL; }

    bool checkValid();

public:
    Linked<IFile> file;
    Linked<IFileContents> fileContents;
    StringAttr version;
    SharedObject pluginSO;
    unsigned extraFlags = 0;
};


class FileSystemDirectory : public CEclCollection
{
public:
    FileSystemDirectory(IIdAtom * _eclName, IFile * _directory)
        : CEclCollection(_eclName, ESTcontainer), directory(_directory)
    {
    }

    void expandDirectoryTree(IDirectoryIterator * dir, bool allowPlugins);
    void addFile(IFile &file, bool allowPlugins);

    void addFile(const char * eclName, IFileContents * fileContents);
    FileSystemDirectory * addDirectory(const char * name);

protected:
    virtual void populateChildren();

public:
    Linked<IFile> directory;
};


//MORE: Create a base class for some of this code.
class FileSystemEclCollection : public CEclSourceCollection
{
public:
    FileSystemEclCollection(unsigned _trace)
        : root(NULL, NULL), trace(_trace)
    {
    }

//interface IEclSourceCollection
    virtual IEclSource * getSource(IEclSource * optParent, IIdAtom * searchName);
    virtual IEclSourceIterator * getContained(IEclSource * optParent);
    virtual void checkCacheValid();

    void processFilePath(IErrorReceiver * errs, const char * sourceSearchPath, bool allowPlugins);
    void processSingle(const char * attrName, IFileContents * contents);

public:
    FileSystemDirectory root;
    unsigned trace;
};

//-------------------------------------------------------------------------------------------------------------------

EclSourceType getEclSourceType(const char * tailname)
{
    StringBuffer temp;
#ifdef _USE_ZLIB
    removeZipExtension(temp, tailname);
#else
    temp.append(tailname);
#endif

    const char *ext = strrchr(temp.str(), '.');
    if (!ext)
        return ESTnone;
    if (stricmp(ext, ".eclmod")==0 || stricmp(ext, ".hql")==0)
        return ESTmodule;
    if (stricmp(ext, ".ecllib")==0 || stricmp(ext, ".hqllib")==0)
        return ESTlibrary;
    if (stricmp(ext, ".ecl")==0 || stricmp(ext, ".eclattr")==0)
        return ESTdefinition;
    if (stricmp(ext, SharedObjectExtension)==0)
        return ESTplugin;
    return ESTnone;
}

static IIdAtom * deriveEclName(const char * filename)
{
    if (!filename)
        return nullptr;

    StringBuffer tailname;
#ifdef _USE_ZLIB
    removeZipExtension(tailname, pathTail(filename));
#else
    tailname.append(pathTail(filename));
#endif

    const char * ext = strrchr(tailname.str(), '.');
    IIdAtom * id;
    if (ext)
        id = createIdAtom(tailname.str(), ext-tailname.str());
    else
        id = createIdAtom(tailname.str());

    if (!isCIdentifier(str(id)))
        return nullptr;
    return id;
}

//---------------------------------------------------------------------------------------

FileSystemFile::FileSystemFile(EclSourceType _type, IFile & _file, bool _allowPlugins)
: CEclSource(deriveEclName(_file.queryFilename()), _type), file(&_file)
{
    Owned<ISourcePath> path = createSourcePath(file->queryFilename());
    fileContents.setown(createFileContents(file, path, _allowPlugins, NULL));
    extraFlags = 0;
    switch (type)
    {
    case ESTplugin:
        extraFlags = PLUGIN_DLL_MODULE|PLUGIN_IMPLICIT_MODULE;
        break;
    case ESTlibrary:
        extraFlags = PLUGIN_IMPLICIT_MODULE;
        break;
    }
}

FileSystemFile::FileSystemFile(const char * eclName, IFileContents * _fileContents)
: CEclSource(createIdAtom(eclName), ESTdefinition), file(nullptr), fileContents(_fileContents)
{
}

bool FileSystemFile::checkValid()
{
    if (!eclId)
        return false;

    if (type == ESTplugin)
    {
        const char * filename = file->queryFilename();
        try
        {
            if (pluginSO.load(filename, false, false))     // don't clash getECLPluginDefinition symbol
            {
                HINSTANCE h = pluginSO.getInstanceHandle();
                EclPluginSetCtx pSetCtx = (EclPluginSetCtx) GetSharedProcedure(h,"setPluginContext");
                if (pSetCtx)
                    pSetCtx(&PluginCtx);

                EclPluginDefinition p= (EclPluginDefinition) GetSharedProcedure(h,"getECLPluginDefinition");
                if (p)
                {
                    ECLPluginDefinitionBlock pb;
                    pb.size = sizeof(pb);
                    if (p(&pb) && (pb.magicVersion == PLUGIN_VERSION) && pb.ECL)
                    {
                        //Name in the plugin overrides the name of the plugin, and the filename where errors are reported.
                        eclId = createIdAtom(pb.moduleName);
                        version.set(pb.version);

                        Owned<ISourcePath> pluginPath = createSourcePath(pb.moduleName);
                        fileContents.setown(createFileContentsFromText(pb.ECL, pluginPath, true, NULL, ::getTimeStamp(file)));

                        //if (traceMask & PLUGIN_DLL_MODULE)
                        DBGLOG("Loading plugin %s[%s] version = %s", filename, pb.moduleName, version.get());
                        //Note: Don't unload the plugin dll.
                        //Otherwise if the plugin is used in a constant folding context it will keep being loaded and unloaded.
                    }
                    else
                    {
                        DBGLOG("Plugin %s exports getECLPluginDefinition but does not export ECL - not loading", filename);
                        return false;
                    }
                }
                else
                {
                    WARNLOG("getECLPluginDefinition not found in %s, unloading", filename);
                    return false;
                }
            }
        }
        catch (IException * e)
        {
            EXCLOG(e, "Trying to load dll");
            ::Release(e);
            return false;
        }
        catch (...)
        {
            return false;
        }
    }

    return true;
}

IProperties * FileSystemFile::getProperties()
{
    Owned<IProperties> properties;
    //MORE: This should set individual properties rather than the "flags", or use flags defined in hqlexpr.hpp
    switch (type)
    {
    case ESTplugin:
        {
            properties.setown(createProperties());
            properties->setProp(str(flagsAtom), extraFlags);
            properties->setProp(str(versionAtom), version.get());
            properties->setProp(str(pluginAtom), file->queryFilename());
            break;
        }
    case ESTmodule:
    case ESTlibrary:
        {
            if (extraFlags)
            {
                properties.setown(createProperties());
                properties->setProp(str(flagsAtom), extraFlags);
            }
            break;
        }
    }
    return properties.getClear();
}

//---------------------------------------------------------------------------------------

#define SOURCEFILE_PLUGIN         0x20000000

void FileSystemDirectory::addFile(IFile &file, bool allowPlugins)
{
    const char * filename = file.queryFilename();
    const char * tail = pathTail(filename);
    if (tail && tail[0]!='.')
    {
        Owned<CEclSource> newSource;
        if (file.isFile() == foundYes)
        {
            EclSourceType type = getEclSourceType(tail);
            if (type && ((type != ESTplugin) || allowPlugins))
            {
                Owned<FileSystemFile> newFile = new FileSystemFile(type, file, allowPlugins);
                if (allowPlugins)
                    newFile->extraFlags |= SOURCEFILE_PLUGIN;
                if (newFile->checkValid())
                    newSource.setown(newFile.getClear());
            }
        }
        else if (file.isDirectory() == foundYes)
        {
            newSource.setown(new FileSystemDirectory(deriveEclName(tail), &file));
        }

        if (newSource && newSource->queryEclId())
        {
            if (!find(newSource->queryEclId()))
                contents.append(*newSource.getClear());
            else
            {
                WARNLOG("Duplicate module found at %s", filename);
            }
        }
    }
    expandedChildren = true;
}

void FileSystemDirectory::addFile(const char * eclName, IFileContents * fileContents)
{
    contents.append(*new FileSystemFile(eclName, fileContents));
}

FileSystemDirectory * FileSystemDirectory::addDirectory(const char * name)
{
    FileSystemDirectory * dir = new FileSystemDirectory(createIdAtom(name), nullptr);
    contents.append(*dir);
    return dir;
}


void FileSystemDirectory::expandDirectoryTree(IDirectoryIterator * dir, bool allowPlugins)
{
    ForEach (*dir)
    {
        IFile &file = dir->query();
        addFile(file, allowPlugins);
    }
    expandedChildren = true;
}

void FileSystemDirectory::populateChildren()
{
    if (directory)
    {
        Owned<IDirectoryIterator> childIter = directory->directoryFiles(NULL, false, true);
        expandDirectoryTree(childIter, false);
    }
}


//---------------------------------------------------------------------------------------

IEclSource * FileSystemEclCollection::getSource(IEclSource * optParent, IIdAtom * searchName)
{
    if (!optParent)
        return root.getSource(searchName);
    CEclSource * parent = static_cast<CEclSource *>(optParent);
    return parent->getSource(searchName);
}

IEclSourceIterator * FileSystemEclCollection::getContained(IEclSource * optParent)
{
    if (!optParent)
        return root.getContained();
    CEclSource * parent = static_cast<CEclSource *>(optParent);
    return parent->getContained();
}

void FileSystemEclCollection::processFilePath(IErrorReceiver * errs, const char * sourceSearchPath, bool allowPlugins)
{
    if (!sourceSearchPath)
        return;

    const char * cursor = sourceSearchPath;
    for (;*cursor;)
    {
        StringBuffer searchPattern;
        while (*cursor && *cursor != ENVSEPCHAR)
            searchPattern.append(*cursor++);
        if(*cursor)
            cursor++;

        if(!searchPattern.length())
            continue;

        StringBuffer dirPath, dirTail, absolutePath;
        splitFilename(searchPattern.str(), &dirPath, &dirPath, &dirTail, &dirTail);
        makeAbsolutePath(dirPath.str(), absolutePath);
        if (!containsFileWildcard(dirTail))
        {
            addPathSepChar(absolutePath).append(dirTail);
            Owned<IFile> file = createIFile(absolutePath);
            if (file->isDirectory() == foundYes)
            {
                Owned<IDirectoryIterator> dir = file->directoryFiles(NULL, false, true);
                root.expandDirectoryTree(dir, allowPlugins);
            }
            else if (file->isFile() == foundYes)
            {
                root.addFile(*file, allowPlugins);
            }
        }
        else
        {
            Owned<IDirectoryIterator> dir = createDirectoryIterator(absolutePath, dirTail);
            root.expandDirectoryTree(dir, allowPlugins);
        }
    }
}

void FileSystemEclCollection::processSingle(const char * attrName, IFileContents * contents)
{
    FileSystemDirectory * directory = &root;
    for (;;)
    {
        const char * dot = strchr(attrName, '.');
        if (!dot)
            break;

        StringAttr name(attrName, dot-attrName);
        directory = directory->addDirectory(name);
        attrName = dot + 1;
    }
    directory->addFile(attrName, contents);
}


void FileSystemEclCollection::checkCacheValid()
{
}

extern HQL_API IEclSourceCollection * createFileSystemEclCollection(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace)
{
    Owned<FileSystemEclCollection> collection = new FileSystemEclCollection(trace);
    collection->processFilePath(errs, path, (flags & ESFallowplugins) != 0);
    return collection.getClear();
}


static IEclSourceCollection * createSingleDefinitionEclCollectionNew(const char * attrName, IFileContents * contents)
{
    Owned<FileSystemEclCollection> collection = new FileSystemEclCollection(0);
    collection->processSingle(attrName, contents);
    return collection.getClear();
}



//-------------------------------------------------------------------------------------------------------------------

static const char * queryText(IPropertyTree * tree)
{
    if (!tree)
        return NULL;

    //A mess because of legacy formats.
    const char* text = tree->queryProp("Text");
    if (!text)
    {
        text = tree->queryProp("text");
        if (!text)
            text = tree->queryProp("");
    }
    return text;
}

class CXmlEclElement : public CEclCollection
{
public:
    CXmlEclElement(IIdAtom * _name, EclSourceType _type, IPropertyTree * _elemTree, CXmlEclElement * _container);

//interface IEclSource
    virtual IProperties * getProperties();
    virtual IFileContents * queryFileContents();

    virtual CXmlEclElement * createElement(IIdAtom * _name, EclSourceType _type, IPropertyTree * _elemTree)
    {
        return new CXmlEclElement(_name, _type, _elemTree, this);
    }

    virtual void populateChildren();
    virtual void setTree(IPropertyTree * _elemTree) { elemTree.set(_elemTree); }

    CXmlEclElement * find(IIdAtom * searchName) { return static_cast<CXmlEclElement *>(CEclCollection::find(searchName)); }
    CXmlEclElement * select(IIdAtom * eclName, EclSourceType _type, IPropertyTree * _tree);
    void setFlags(unsigned _flags) { extraFlags = _flags; }

protected:
    void expandAttribute(const char * modname, IPropertyTree * tree);
    void expandChildren(IPropertyTree * xml);
    void expandModule(const char * modname, IPropertyTree * tree);
    void getFullName(StringBuffer & target);

public:
    Linked<IPropertyTree> elemTree;
    Linked<IFileContents> fileContents;
    CXmlEclElement * container;
    unsigned extraFlags;
};



CXmlEclElement::CXmlEclElement(IIdAtom * _name, EclSourceType _type, IPropertyTree * _elemTree, CXmlEclElement * _container)
: CEclCollection(_name, _type), elemTree(_elemTree), container(_container)
{
    extraFlags = 0;
    switch (type)
    {
    case ESTplugin:
        extraFlags = PLUGIN_DLL_MODULE|PLUGIN_IMPLICIT_MODULE;
        break;
    case ESTlibrary:
        extraFlags = PLUGIN_IMPLICIT_MODULE;
        break;
    }
}

void CXmlEclElement::populateChildren()
{
    if (elemTree)
        expandChildren(elemTree);
}

void CXmlEclElement::expandChildren(IPropertyTree * xml)
{
    Owned<IPropertyTreeIterator> modit = xml->getElements("Module");
    ForEach(*modit)
    {
        IPropertyTree & cur = modit->query();
        const char* modname = cur.queryProp("@name");
        expandModule(modname, &cur);
    }

    Owned<IPropertyTreeIterator> attrit = xml->getElements("Attribute");
    ForEach(*attrit)
    {
        IPropertyTree & cur = attrit->query();
        const char* modname = cur.queryProp("@name");
        expandAttribute(modname, &cur);
    }
}

void CXmlEclElement::expandModule(const char * modname, IPropertyTree * tree)
{
    if (modname && *modname)
    {
        const char * dot = strchr(modname, '.');
        if (dot)
        {
            IIdAtom * name = createIdAtom(modname, dot-modname);
            select(name, ESTcontainer, NULL)->expandModule(dot+1, tree);
            return;
        }

        IIdAtom * thisName = createIdAtom(modname);
        EclSourceType thisType = ESTcontainer;
        const char * text = queryText(tree);

        //This is all primarily here for backward compatibility with existing archives (especially in regression suite)
        int iflags = tree->getPropInt("@flags", 0);
        //backward compatibility of old archive files
        if (stricmp(modname, "default")==0)
            iflags &= ~(PLUGIN_IMPLICIT_MODULE);

        if (text)
        {
            if (iflags & PLUGIN_DLL_MODULE)
                thisType = ESTplugin;
            else if (iflags & PLUGIN_IMPLICIT_MODULE)
                thisType = ESTlibrary;
            else
                thisType = ESTmodule;
            iflags = 0;
        }

        CXmlEclElement * match = select(thisName, thisType, tree);
        if (iflags)
            match->setFlags(iflags);
    }
    else
        expandChildren(tree);
}


void CXmlEclElement::expandAttribute(const char * modname, IPropertyTree * tree)
{
    IIdAtom * thisName = createIdAtom(modname);
    CXmlEclElement * match = select(thisName, ESTdefinition, tree);

    unsigned iflags = tree->getPropInt("@flags", 0);
    if (iflags)
        match->setFlags(iflags);
}


void CXmlEclElement::getFullName(StringBuffer & target)
{
    if (container)
    {
        unsigned prev = target.length();
        container->getFullName(target);
        if (target.length() != prev)
            target.append('.');
    }
    target.append(str(eclId));
}

IFileContents * CXmlEclElement::queryFileContents()
{
    ensureDefinition();
    if (!fileContents)
    {
        const char* text = queryText(elemTree);
        if (text && *text)
        {
            const char * path = elemTree->queryProp("@sourcePath");
            Owned<ISourcePath> sourcePath;
            if (path)
            {
                sourcePath.setown(createSourcePath(path));
            }
            else
            {
                StringBuffer defaultName;
                getFullName(defaultName);
                sourcePath.setown(createSourcePath(defaultName));
            }
            timestamp_type ts = elemTree->getPropInt64("@ts");
            fileContents.setown(createFileContentsFromText(text, sourcePath, false, NULL, ts));
        }
    }
    return fileContents;
}

IProperties * CXmlEclElement::getProperties()
{
    ensureDefinition();
    Owned<IProperties> properties;
    //MORE: This should set individual properties rather than the "flags", or use flags defined in hqlexpr.hpp
    switch (type)
    {
    case ESTdefinition:
        {
            unsigned flags = extraFlags;
            if (elemTree->getPropBool("@dirty", false))
                flags |= ob_sandbox;
            if (flags)
            {
                properties.setown(createProperties());
                properties->setProp(str(flagsAtom), flags);
            }
            break;
        }
    case ESTplugin:
        {
            properties.setown(createProperties());
            properties->setProp(str(flagsAtom), extraFlags);
            properties->setProp(str(versionAtom), elemTree->queryProp("@version"));
            properties->setProp(str(pluginAtom), elemTree->queryProp("@fullname"));
            break;
        }
    default:
        {
            if (extraFlags)
            {
                properties.setown(createProperties());
                properties->setProp(str(flagsAtom), extraFlags);
            }
            break;
        }
    }
    return properties.getClear();
}


CXmlEclElement * CXmlEclElement::select(IIdAtom * _name, EclSourceType _type, IPropertyTree * _tree)
{
    //Critial section??
    CXmlEclElement * match = find(_name);
    if (!match)
    {
        match = createElement(_name, _type, _tree);
        contents.append(*match);
    }
    else if (!match->elemTree)
    {
        assertex(match->queryType() == _type);
        match->setTree(_tree);
    }
    else if (_tree)
    {
        //Some old archives seeem to contain duplicate definitions.  Clean then up then delete this code
        OWARNLOG("Source Seems to have duplicate definition of %s", str(_name));
        //throwUnexpected();
    }

    return match;
}


//-------------------------------------------------------------------------------------------------------------------

class PropertyTreeEclCollection : public CEclSourceCollection
{
public:
    PropertyTreeEclCollection(IPropertyTree * _archive)
        : archive(_archive)
    {
    }

//interface IEclSourceCollection
    virtual IEclSource * getSource(IEclSource * optParent, IIdAtom * searchName);
    virtual IEclSourceIterator * getContained(IEclSource * optParent);

public:
    Linked<IPropertyTree> archive;
    Linked<CXmlEclElement> root;
};


//---------------------------------------------------------------------------------------

IEclSource * PropertyTreeEclCollection::getSource(IEclSource * optParent, IIdAtom * searchName)
{
    if (!optParent)
        return root->getSource(searchName);
    CEclSource * parent = static_cast<CEclSource *>(optParent);
    return parent->getSource(searchName);
}

IEclSourceIterator * PropertyTreeEclCollection::getContained(IEclSource * optParent)
{
    if (!optParent)
        return root->getContained();
    CEclSource * parent = static_cast<CEclSource *>(optParent);
    return parent->getContained();
}

//---------------------------------------------------------------------------------------

class ArchiveEclCollection : public PropertyTreeEclCollection
{
public:
    ArchiveEclCollection(IPropertyTree * _archive)
        : PropertyTreeEclCollection(_archive)
    {
        root.setown(new CXmlEclElement(NULL, ESTcontainer, archive, NULL));
    }

    virtual void checkCacheValid() {}
};


extern HQL_API IEclSourceCollection * createArchiveEclCollection(IPropertyTree * tree)
{
    Owned<ArchiveEclCollection> collection = new ArchiveEclCollection(tree);
    return collection.getClear();
}


//---------------------------------------------------------------------------------------

IEclSourceCollection * createSingleDefinitionEclCollection(const char * moduleName, const char * attrName, IFileContents * contents)
{
    //Create an archive with a single module/
    Owned<IPropertyTree> archive = createPTree("Archive");
    IPropertyTree * module = archive->addPropTree("Module", createPTree("Module"));
    module->setProp("@name", moduleName);
    IPropertyTree * attr = module->addPropTree("Attribute", createPTree("Attribute"));
    attr->setProp("@name", attrName);
    const char * filename = str(contents->querySourcePath());
    if (filename)
        attr->setProp("@sourcePath", filename);
    timestamp_type ts = contents->getTimeStamp();
    if (ts)
        attr->setPropInt64("@ts", ts);

    StringBuffer temp;
    temp.append(contents->length(), contents->getText());
    attr->setProp("", temp.str());
    return createArchiveEclCollection(archive);
}

IEclSourceCollection * createSingleDefinitionEclCollection(const char * attrName, IFileContents * contents)
{
    //Use the directory and file based collection - so that file information (including timestamps) is preserved.
    return createSingleDefinitionEclCollectionNew(attrName, contents);
#if 0
    const char * dot = strrchr(attrName, '.');
    if (dot)
    {
        StringAttr module(attrName, dot-attrName);
        return createSingleDefinitionEclCollection(module, dot+1, contents);
    }
    return createSingleDefinitionEclCollection("", attrName, contents);
#endif
}

//---------------------------------------------------------------------------------------

static void setKeyAttribute(IPropertyTree * elem)
{
    StringBuffer nameKey;
    const char* name = elem->queryProp("@name");
    if (name)
    {
        nameKey.append(name).toLowerCase();
        elem->setProp("@key", nameKey);
    }
}

class RemoteXmlEclCollection;
class CRemoteXmlEclElement : public CXmlEclElement
{
public:
    CRemoteXmlEclElement(IIdAtom * _name, EclSourceType _type, IPropertyTree * _elemTree, CXmlEclElement * _container, RemoteXmlEclCollection * _collection)
        : CXmlEclElement(_name, _type, _elemTree, _container), collection(_collection)
    {
        updateKey();
        if (queryText(elemTree))
            fullyDefined = true;
    }

//interface IEclSource
    virtual CXmlEclElement * createElement(IIdAtom * _name, EclSourceType _type, IPropertyTree * _elemTree)
    {
        return new CRemoteXmlEclElement(_name, _type, _elemTree, this, collection);
    }

    virtual void setTree(IPropertyTree * _elemTree)
    {
        CXmlEclElement::setTree(_elemTree);
        updateKey();
    }

    virtual void populateChildren();
    virtual void populateDefinition();

    inline const char * queryName() { return elemTree ? elemTree->queryProp("@name") : NULL; }

protected:
    inline void updateKey()
    {
        if (elemTree && !elemTree->queryProp("@key"))
            setKeyAttribute(elemTree);
    }

protected:
    RemoteXmlEclCollection * collection;
};


//---------------------------------------------------------------------------------------

class RemoteXmlEclCollection : public PropertyTreeEclCollection
{
public:
    RemoteXmlEclCollection(IEclUser * _user, IXmlEclRepository &_repository, const char * _snapshot, bool _useSandbox)
        : PropertyTreeEclCollection(NULL), repository(&_repository), user(_user)
    {
        root.setown(new CRemoteXmlEclElement(NULL, ESTcontainer, archive, NULL, this));
        if (_snapshot && *_snapshot)
            snapshot.set(_snapshot);
        useSandbox = _useSandbox;
        cachestamp = 0;
        archive.setown(createPTree("Archive"));
        preloadText = true;
    }

//interface IEclSourceCollection
    virtual void checkCacheValid();

    IPropertyTree * fetchAttribute(const char * moduleName, const char * attrName);
    IPropertyTree * fetchModule(const char * moduleName);

protected:
    void logException(IException *e);
    IPropertyTree * lookupModule(const char * moduleName);
    IPropertyTree * updateAttribute(IPropertyTree * module, IPropertyTree * attribute);
    IPropertyTree * updateModule(IPropertyTree & module);

private:
    IPropertyTree * getAttributes(const char *module, const char *attr, int version, unsigned char infoLevel);
    IPropertyTree * getModules(timestamp_type from);

    static IPropertyTree * lookup(IPropertyTree * parent, const char * name);
    static IPropertyTree * update(IPropertyTree * parent, IPropertyTree * child, bool needToDelete);

public:
    Linked<IXmlEclRepository> repository;
    Linked<IEclUser> user;
    StringAttr snapshot;
    StringBuffer lastError;
    timestamp_type cachestamp;
    bool useSandbox;
    bool preloadText;
};

//==============================================================================================================

void CRemoteXmlEclElement::populateChildren()
{
    if ((type == ESTcontainer) && elemTree)
    {
        if (container)
        {
            IPropertyTree * resolved = collection->fetchModule(queryName());
            if (resolved)
                setTree(resolved);
        }
        else
        {
            IPropertyTree * resolved = collection->fetchModule("");
            if (resolved)
            {
                //MORE: This updates the archive entry.
                //I think that means modification to remote non module attributes may not get updated
                //Not a show stopping issue, since not currently supported.
                //But needs a clean solution.
                //e.g., clone, merge, replace.
                mergePTree(elemTree, resolved);
            }
        }
    }
    CXmlEclElement::populateChildren();
}

void CRemoteXmlEclElement::populateDefinition()
{
    if ((type == ESTdefinition) && elemTree)
    {
        CRemoteXmlEclElement * module = static_cast<CRemoteXmlEclElement *>(container);
        const char * moduleName = module->queryName();
        IPropertyTree * resolved = collection->fetchAttribute(moduleName, queryName());
        if (resolved)
            setTree(resolved);
    }
    CXmlEclElement::populateDefinition();
}



//==============================================================================================================

void RemoteXmlEclCollection::logException(IException *e)
{
    if (e)
    {
        e->errorMessage(lastError.clear());
    }
    else
    {
        lastError.clear().append("Unknown exception");
    }
    DBGLOG("Log Exception: %s", lastError.str());
}

void RemoteXmlEclCollection::checkCacheValid()
{
    //Check cache valid ensures that all the module instances are created.
    DBGLOG("check cache");
    Owned<IPropertyTree> repository = getModules(cachestamp);
    if(!repository)
    {
        DBGLOG("getModules returned null");
        //process error
        return;
    }

    bool somethingChanged = false;
    timestamp_type newest = cachestamp;
    Owned<IPropertyTreeIterator> it = repository->getElements("./Module");
    for (it->first(); it->isValid(); it->next())
    {
        IPropertyTree & cur = it->query();
        timestamp_type timestamp = (timestamp_type)cur.getPropInt64("@timestamp");
        if ((cachestamp == 0) || (timestamp > cachestamp))
        {
            updateModule(cur);
            //merge/invalidate
            if (timestamp > newest)
                newest = timestamp;
            somethingChanged = true;
        }
    }

    if (somethingChanged)
        root.setown(new CRemoteXmlEclElement(NULL, ESTcontainer, archive, NULL, this));

    fetchModule("");
}

IPropertyTree * RemoteXmlEclCollection::lookup(IPropertyTree * parent, const char * name)
{
    if (!parent)
        return NULL;
    StringBuffer xpath;
    xpath.append("*[@key='").appendLower(strlen(name), name).append("']");
    return parent->queryPropTree(xpath);
}


IPropertyTree * RemoteXmlEclCollection::update(IPropertyTree * parent, IPropertyTree * child, bool needToDelete)
{
    if (!parent)
        return NULL;

    const char* name = child->queryProp("@name");
    IPropertyTree * match = lookup(parent, name);
    if (match)
        parent->removeTree(match);

    if (!needToDelete)
    {
        IPropertyTree * clone = parent->addPropTree(child->queryName(), LINK(child));
        return clone;
    }
    return NULL;
}



IPropertyTree * RemoteXmlEclCollection::lookupModule(const char * moduleName)
{
    return lookup(archive, moduleName);
}

IPropertyTree * RemoteXmlEclCollection::updateModule(IPropertyTree & module)
{
    int iflags = module.getPropInt("@flags", 0);
    bool needToDelete = (iflags & ZOMBIE_MODULE) != 0;

    return update(archive, &module, needToDelete);
}

IPropertyTree * RemoteXmlEclCollection::fetchModule(const char * moduleName)
{
    //Also adds the retrieved values to the "archive" cache.
    DBGLOG("load module %s",moduleName);

    Owned<IPropertyTree> repository = getAttributes(moduleName, NULL, 0, preloadText ? 1 : 0);
    IPropertyTree * module = repository ? repository->queryPropTree("Module") : NULL;
    if (!module)
    {
        if (*moduleName)
            DBGLOG("getAttributes %s.* returned null", moduleName);
        return NULL;
    }
    return updateModule(*module);
}


IPropertyTree * RemoteXmlEclCollection::updateAttribute(IPropertyTree * module, IPropertyTree * attribute)
{
    return update(module, attribute, false);
}

IPropertyTree * RemoteXmlEclCollection::fetchAttribute(const char * moduleName, const char * attrName)
{
    DBGLOG("load attribute %s.%s",moduleName, attrName);
    Owned<IPropertyTree> repository = getAttributes(moduleName, attrName, 0, 2);
    IPropertyTree * moduleTree = repository ? repository->queryPropTree("Module") : NULL;
    IPropertyTree * attributeTree = moduleTree ? moduleTree->queryPropTree("Attribute") : NULL;
    if (!attributeTree)
    {
        DBGLOG("getAttributes %s.%s returned null",moduleName, attrName);
        return NULL;
    }
    IPropertyTree * module = lookupModule(moduleName);
    return updateAttribute(module, attributeTree);
}

IPropertyTree* RemoteXmlEclCollection::getModules(timestamp_type from)
{
    StringBuffer modNames;
    IPropertyTree* repositoryTree = 0;
    try
    {
        repository->getModules(modNames, user, from);
        repositoryTree = createPTreeFromXMLString(modNames.str(), ipt_caseInsensitive);
    }
    catch(IException *e)
    {
        logException(e);
        e->Release();
    }
    catch (...)
    {
        Owned<IException> e = makeUnexpectedException();
        logException(e);
    }

    return repositoryTree;
}

IPropertyTree* RemoteXmlEclCollection::getAttributes(const char *module, const char *attr, int version, unsigned char infoLevel)
{
    IPropertyTree* repositoryTree = 0;
    StringBuffer xml;
    try
    {
        repository->getAttributes(xml, user, module, attr, version, infoLevel, snapshot.get(), useSandbox);
        if (xml.length())
            repositoryTree = createPTreeFromXMLString(xml, ipt_caseInsensitive);
    }
    catch(IException *e)
    {
        logException(e);
        if (xml.length())
            DBGLOG("Xml: %s", xml.str());
        e->Release();
    }
    catch (...)
    {
        Owned<IException> e = makeUnexpectedException();
        logException(e);
    }

    return repositoryTree;
}

//---------------------------------------------------------------------------------------------------------------------

extern HQL_API IEclSourceCollection * createRemoteXmlEclCollection(IEclUser * user, IXmlEclRepository & repository, const char * snapshot, bool useSandbox)
{
    return new RemoteXmlEclCollection(user, repository, snapshot, useSandbox);
}

//---------------------------------------------------------------------------------------------------------------------

IPropertyTree * cloneModuleNoAttributes(IPropertyTree * target, IPropertyTree * source)
{
    IPropertyTree * cloned = target->addPropTree(source->queryName(), LINK(source));
    while (cloned->removeProp("Attribute"))
    {
    }
    return cloned;
}

//This is not implemented for efficiency, it is purely for testing this interface
class ArchiveXmlEclRepository : implements IXmlEclRepository, public CInterface
{
public:
    ArchiveXmlEclRepository(IPropertyTree * _archive) : archive(_archive)
    {
    }
    IMPLEMENT_IINTERFACE;

    IPropertyTree * findModule(const char * searchName)
    {
        Owned<IPropertyTreeIterator> iter = archive->getElements("./Module");
        ForEach(*iter)
        {
            const char * name = iter->query().queryProp("@name");
            if (name && (stricmp(name, searchName) == 0))
                return &iter->query();
        }
        return NULL;
    }

    virtual int getModules(StringBuffer & xml, IEclUser * , timestamp_type )
    {
        Owned<IPropertyTree> result = createPTree("Repository");
        Owned<IPropertyTreeIterator> iter = archive->getElements("./Module");
        ForEach(*iter)
        {
            cloneModuleNoAttributes(result, &iter->query());
        }
        toXML(result, xml);
        return 1;
    }

    virtual int getAttributes(StringBuffer & xml, IEclUser * user, const char * modname, const char * attribute, int version, unsigned char infoLevel, const char * snapshot, bool sandbox4snapshot)
    {
        IPropertyTree * module = findModule(modname);
        if (!module)
            return 0;

        Owned<IPropertyTree> result = createPTree("Repository");
        IPropertyTree * clonedModule = cloneModuleNoAttributes(result, module);
        Owned<IPropertyTreeIterator> it = module->getElements("./Attribute");
        for (it->first(); it->isValid(); it->next())
        {
            IPropertyTree & cur = it->query();
            const char * name = cur.queryProp("@name");
            if (!attribute || (stricmp(name, attribute) == 0))
            {
                IPropertyTree * cloned = clonedModule->addPropTree(cur.queryName(), LINK(&cur));
                if (infoLevel == 0)
                    cloned->removeProp("Text");
            }
        }
        toXML(result, xml);
        return 1;
    }

protected:
    Linked<IPropertyTree> archive;
};


extern HQL_API IXmlEclRepository * createArchiveXmlEclRepository(IPropertyTree * archive)
{
    return new ArchiveXmlEclRepository(archive);
}


//---------------------------------------------------------------------------------------------------------------------


class ReplayXmlEclRepository : implements IXmlEclRepository, public CInterface
{
public:
    ReplayXmlEclRepository(IPropertyTree * _xmlTree) : xmlTree(_xmlTree)
    {
        seq = 0;
    }
    IMPLEMENT_IINTERFACE;

    virtual int getModules(StringBuffer & xml, IEclUser * user, timestamp_type timestamp)
    {
        StringBuffer xpath;
        xpath.append("Timestamp[@seq=\"").append(++seq).append("\"]");
        IPropertyTree * archiveVersion = xmlTree->queryPropTree(xpath);
        if (!archiveVersion)
        {
            activeArchive.clear();
            return 0;
        }
        activeArchive.setown(new ArchiveXmlEclRepository(archiveVersion));
        return activeArchive->getModules(xml, user, timestamp);
    }

    virtual int getAttributes(StringBuffer & xml, IEclUser * user, const char * modname, const char * attribute, int version, unsigned char infoLevel, const char * snapshot, bool sandbox4snapshot)
    {
        if (!activeArchive)
            return 0;
        return activeArchive->getAttributes(xml, user, modname, attribute, version, infoLevel, snapshot, sandbox4snapshot);
    }

protected:
    Linked<IPropertyTree> xmlTree;
    Owned<ArchiveXmlEclRepository> activeArchive;
    unsigned seq;
};


extern HQL_API IXmlEclRepository * createReplayXmlEclRepository(IPropertyTree * xml)
{
    return new ReplayXmlEclRepository(xml);
}
