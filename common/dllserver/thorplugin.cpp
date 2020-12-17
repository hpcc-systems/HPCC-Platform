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

#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jsocket.hpp"
#include "jprop.hpp"
#include "jdebug.hpp"
#include "jregexp.hpp"
#include "jlzw.hpp"
#include "eclrtl.hpp"
#include "build-config.h"
#if defined(__APPLE__)
#include <mach-o/getsect.h>
#include <sys/mman.h>
#include <sys/stat.h>
#elif !defined(_WIN32)
#include <sys/mman.h>
#include <sys/stat.h>
#include <elf.h>
#endif

#include "thorplugin.hpp"

void * SimplePluginCtx::ctxMalloc(size_t size)
{ 
    return rtlMalloc(size); 
}

void * SimplePluginCtx::ctxRealloc(void * _ptr, size_t size)
{ 
    return rtlRealloc(_ptr, size);
}
void SimplePluginCtx::ctxFree(void * _ptr)
{ 
    rtlFree(_ptr);
}

char * SimplePluginCtx::ctxStrdup(char * _ptr)
{ 
    return strdup(_ptr); 
}

int SimplePluginCtx::ctxGetPropInt(const char *propName, int defaultValue) const
{
    return defaultValue;
}

const char * SimplePluginCtx::ctxQueryProp(const char *propName) const
{
    return NULL;
}

//-------------------------------------------------------------------------------------------------------------------

static bool getResourceFromMappedFile(const char * filename, const byte * start_addr, size32_t & lenData, const void * & data, const char * type, unsigned id)
{
#if defined(_WIN32)
    throwUnexpected();
#elif defined(__APPLE__)
    VStringBuffer sectname("%s_%u", type, id);
    // The first bytes are the Mach-O header
    const struct mach_header_64 *mh = (const struct mach_header_64 *) start_addr;
    if (mh->magic != MH_MAGIC_64)
    {
        DBGLOG("Failed to extract resource %s: Does not appear to be a Mach-O 64-bit binary", filename);
        return false;
    }

    unsigned long len = 0;
    data = getsectiondata(mh, "__TEXT", sectname.str(), &len);
    lenData = (size32_t)len;
    return true;
#elif defined (__64BIT__)
    // The first bytes are the ELF header
    const Elf64_Ehdr * hdr = (const Elf64_Ehdr *) start_addr;
    if (memcmp(hdr->e_ident, ELFMAG, SELFMAG) != 0)
    {
        DBGLOG("Failed to extract resource %s: Does not appear to be a ELF binary", filename);
        return false;
    }
    if (hdr->e_ident[EI_CLASS] != ELFCLASS64)
    {
        DBGLOG("Failed to extract resource %s: Does not appear to be a ELF 64-bit binary", filename);
        return false;
    }

    //Check that there is a symbol table for the sections.
    if (hdr->e_shstrndx == SHN_UNDEF)
    {
        DBGLOG("Failed to extract resource %s: Does not include a section symbol table", filename);
        return false;
    }

    //Now walk the sections comparing the section names
    Elf64_Half numSections = hdr->e_shnum;
    const Elf64_Shdr * sectionHeaders = reinterpret_cast<const Elf64_Shdr *>(start_addr + hdr->e_shoff);
    const Elf64_Shdr & symbolTableSection = sectionHeaders[hdr->e_shstrndx];
    const char * symbolTable = (const char *)start_addr + symbolTableSection.sh_offset;
    VStringBuffer sectname("%s_%u", type, id);
    for (unsigned iSect= 0; iSect < numSections; iSect++)
    {
        const Elf64_Shdr & section = sectionHeaders[iSect];
        const char * sectionName = symbolTable + section.sh_name;
        if (streq(sectionName, sectname))
        {
            lenData = (size32_t)section.sh_size;
            data = start_addr + section.sh_offset;
            return true;
        }
    }

    return false;
#else
    // The first bytes are the ELF header
    const Elf32_Ehdr * hdr = (const Elf32_Ehdr *) start_addr;
    if (memcmp(hdr->e_ident, ELFMAG, SELFMAG) != 0)
    {
        DBGLOG("Failed to extract resource %s: Does not appear to be a ELF binary", filename);
        return false;
    }
    if (hdr->e_ident[EI_CLASS] != ELFCLASS32)
    {
        DBGLOG("Failed to extract resource %s: Does not appear to be a ELF 32-bit binary", filename);
        return false;
    }

    //Check that there is a symbol table for the sections.
    if (hdr->e_shstrndx == SHN_UNDEF)
    {
        DBGLOG("Failed to extract resource %s: Does not include a section symbol table", filename);
        return false;
    }

    //Now walk the sections comparing the section names
    Elf32_Half numSections = hdr->e_shnum;
    const Elf32_Shdr * sectionHeaders = reinterpret_cast<const Elf32_Shdr *>(start_addr + hdr->e_shoff);
    const Elf32_Shdr & symbolTableSection = sectionHeaders[hdr->e_shstrndx];
    const char * symbolTable = (const char *)start_addr + symbolTableSection.sh_offset;
    VStringBuffer sectname("%s_%u", type, id);
    for (unsigned iSect= 0; iSect < numSections; iSect++)
    {
        const Elf32_Shdr & section = sectionHeaders[iSect];
        const char * sectionName = symbolTable + section.sh_name;
        if (streq(sectionName, sectname))
        {
            lenData = (size32_t)section.sh_size;
            data = start_addr + section.sh_offset;
            return true;
        }
    }

    return false;
#endif
}

static bool getResourceFromMappedFile(const char * filename, const byte * start_addr, MemoryBuffer & result, const char * type, unsigned id)
{
    size32_t len = 0;
    const void * data = nullptr;
    bool ok = getResourceFromMappedFile(filename, start_addr, len, data, type, id);
    if (ok)
        result.append(len, data);
    return ok;
}

extern bool getResourceFromFile(const char *filename, MemoryBuffer &data, const char * type, unsigned id)
{
#ifdef _WIN32
    HINSTANCE dllHandle = LoadLibraryEx(filename, NULL, LOAD_LIBRARY_AS_DATAFILE|LOAD_LIBRARY_AS_IMAGE_RESOURCE);
    if (dllHandle == NULL)
        dllHandle = LoadLibraryEx(filename, NULL, LOAD_LIBRARY_AS_DATAFILE); // the LOAD_LIBRARY_AS_IMAGE_RESOURCE flag is not supported on all versions of Windows
    if (dllHandle == NULL)
    {
        DBGLOG("Failed to load library %s: %d", filename, GetLastError());
        return false;
    }
    HRSRC hrsrc = FindResource(dllHandle, MAKEINTRESOURCE(id), type);
    if (!hrsrc)
        return false;
    size32_t len = SizeofResource(dllHandle, hrsrc);
    const void *rdata = (const void *) LoadResource(dllHandle, hrsrc);
    data.append(len, rdata);
    FreeLibrary(dllHandle);
    return true;
#else
    struct stat stat_buf;
    VStringBuffer sectname("%s_%u", type, id);
    int fd = open(filename, O_RDONLY);
    if (fd == -1)
    {
        DBGLOG("Failed to load library %s: %d", filename, errno);
        return false;
    }

    bool ok = false;
    if (fstat(fd, &stat_buf) != -1)
    {
        __uint64 size = stat_buf.st_size;
        const byte *start_addr = (const byte *) mmap(0, size, PROT_READ, MAP_FILE | MAP_PRIVATE, fd, 0);
        if (start_addr == MAP_FAILED)
        {
            DBGLOG("Failed to load library %s: %d", filename, errno);
        }
        else
        {
            ok = getResourceFromMappedFile(filename, start_addr, data, type, id);
            munmap((void *)start_addr, size);
        }
    }
    else
        DBGLOG("Failed to load library %s: %d", filename, errno);

    close(fd);
    return ok;
#endif
}

//-------------------------------------------------------------------------------------------------------------------

class ManifestFileList : public MappingBase
{
    StringArray filenames;
    StringAttr type;
    StringAttr dir;
    void recursiveRemoveDirectory(const char *fullPath)
    {
        if (rmdir(fullPath) == 0 && !streq(fullPath, dir))
        {
            StringBuffer head;
            splitFilename(fullPath, &head, &head, NULL, NULL);
            if (head.length() > 1)
            {
                head.setLength(head.length()-1);
                recursiveRemoveDirectory(head);
            }
        }
    }
    void removeFileAndEmptyParents(const char *fullFileName)
    {
        remove(fullFileName);
        StringBuffer path;
        splitFilename(fullFileName, &path, &path, NULL, NULL);
        if (path.length() > 1)
        {
            path.setLength(path.length()-1);
            recursiveRemoveDirectory(path.str());
        }
    }

public:
    ManifestFileList(const char *_type, const char *_dir) : type(_type), dir(_dir) {}
    ~ManifestFileList()
    {
        ForEachItemIn(idx, filenames)
        {
            removeFileAndEmptyParents(filenames.item(idx));
        }
        rmdir(dir);  // If the specified temporary directory is now empty, remove it.
    }
    void append(const char *filename)
    {
        assertex(strncmp(filename, dir, strlen(dir))==0);
        filenames.append(filename);
    }
    inline const StringArray &queryFileNames() { return filenames; }
    virtual const void * getKey() const { return type; }
};

class HelperDll : implements ILoadedDllEntry, public CInterface
{
    SharedObject so;
    Linked<const IFileIO> dllFile;
    Owned<IMemoryMappedFile> mappedDll;
    mutable std::atomic<IPropertyTree *> manifest {nullptr};
    mutable CriticalSection manifestLock;
    mutable StringMapOf<ManifestFileList> manifestFiles;

protected:
    StringAttr name;
    bool logLoad;
public:
    IMPLEMENT_IINTERFACE;
    HelperDll(const char *_name, const IFileIO *dllFile);
    ~HelperDll();

//interface ILoadedDllEntry
    virtual HINSTANCE getInstance() const;
    virtual void * getEntry(const char * name) const;
    virtual bool IsShared();
    virtual const char * queryVersion() const;
    virtual const char * queryName() const;
    virtual const byte * getResource(unsigned id) const;
    virtual bool getResource(size32_t & len, const void * & data, const char * type, unsigned id, bool trace) const;
    virtual IPropertyTree &queryManifest() const override;
    virtual const StringArray &queryManifestFiles(const char *type, const char *wuid) const override;
    bool load(bool isGlobal, bool raiseOnError);
    bool loadCurrentExecutable();
    bool loadResources();

    virtual void logLoaded();
    virtual bool checkVersion(const char *expected);
};

class PluginDll : public HelperDll
{
    ECLPluginDefinitionBlockEx pb;

public:
    PluginDll(const char *_name, const IFileIO *_dllFile) : HelperDll(_name, _dllFile) {}

    bool init(IPluginContextEx * pluginCtx);

    virtual bool checkVersion(const char *expected) override;
    virtual void logLoaded() override;
};

HelperDll::HelperDll(const char *_name, const IFileIO *_dllFile)
: name(_name), dllFile(_dllFile), manifestFiles(false)
{
    logLoad = false;
}

bool HelperDll::load(bool isGlobal, bool raiseOnError)
{
    if (!so.load(name, isGlobal, raiseOnError))
        return false;
    return true;
}

bool HelperDll::loadResources()
{
#ifdef _WIN32
    return so.loadResources(name);
#else
    Owned<IFile> file = createIFile(name);
    mappedDll.setown(file->openMemoryMapped());
    return mappedDll != nullptr;
#endif
}

bool HelperDll::loadCurrentExecutable()
{
    if (!so.loadCurrentExecutable())
        return false;
    return true;
}

HelperDll::~HelperDll()
{
    if (logLoad)
        DBGLOG("Unloading dll %s", name.get());
    ::Release(manifest.load(std::memory_order_relaxed));
}

HINSTANCE HelperDll::getInstance() const
{
    if (!so.loaded())
        throw MakeStringException(0, "Dll %s only loaded for resources", name.str());
    return so.getInstanceHandle();
}

void * HelperDll::getEntry(const char * entry) const
{
    if (!so.loaded())
        throw MakeStringException(0, "Dll %s only loaded for resources", name.str());
    return so.getEntry(entry);
}

bool HelperDll::IsShared()
{
    return CInterface::IsShared();
}

const char * HelperDll::queryVersion() const
{
    return "";
}

void HelperDll::logLoaded()
{
    logLoad = true;
    DBGLOG("Loaded DLL %s", name.get());
}

bool HelperDll::checkVersion(const char *expected)
{
    return true;
}

const char * HelperDll::queryName() const
{
    return name.get();
}

const byte * HelperDll::getResource(unsigned id) const
{
    if (so.loaded())
    {
#ifdef _WIN32
        HINSTANCE dllHandle = so.getInstanceHandle();
        HRSRC hrsrc = FindResource(dllHandle, MAKEINTRESOURCE(id), "BIGSTRING");
        if (hrsrc)
            return (const byte *)LoadResource(dllHandle, hrsrc);
        return NULL;
#else
        StringBuffer resourceName;
        resourceName.appendf("BIGSTRING_%d_txt_start", id);
        return (const byte *)getEntry(resourceName.str());
#endif
    }
    else
    {
        size32_t len;
        const void * data;
        if (getResource(len, data, "BIGSTRING", id, false))
            return (const byte *)data;
        return nullptr;
    }
}

const byte resourceHeaderVersion=1;
const size32_t resourceHeaderLength = sizeof(byte) + sizeof(byte) + sizeof(bool) + sizeof(size32_t);


bool HelperDll::getResource(size32_t & len, const void * & data, const char * type, unsigned id, bool trace) const
{
    if (so.loaded())
    {
#ifdef _WIN32
        HINSTANCE dllHandle = so.getInstanceHandle();
        HRSRC hrsrc = FindResource(dllHandle, MAKEINTRESOURCE(id), type);
        if (!hrsrc)
            return false;
        len = SizeofResource(dllHandle, hrsrc);
        data = (const byte *)LoadResource(dllHandle, hrsrc);
        return true;
#else
        StringBuffer symName;
        symName.append(type).append("_").append(id).append("_txt_start");
        data = (const void *)getEntry(symName.str());
        if (!data)
        {
            if (trace)
                printf("Failed to locate symbol %s\n", symName.str());
            return false;
        }
        byte bom;
        byte version;
        bool compressed;

        MemoryBuffer mb;
        mb.setBuffer(resourceHeaderLength, const_cast<void *>(data));
        mb.read(bom);
        if (bom != 0x80)
            return false;
        mb.read(version);
        if (version > resourceHeaderVersion)
            return false;
        mb.read(compressed).read(len);
        len += resourceHeaderLength;
        return true;
#endif
    }
    else
    {
#ifdef _WIN32
        return false;
#endif
        if (!mappedDll)
            return false;
        return getResourceFromMappedFile(name, mappedDll->base(), len, data, type, id);
    }
}

IPropertyTree &HelperDll::queryManifest() const
{
    return *querySingleton(manifest, manifestLock, [this]{ return getEmbeddedManifestPTree(this); });
}

const StringArray &HelperDll::queryManifestFiles(const char *type, const char *wuid) const
{
    CriticalBlock b(manifestLock);
    Linked<ManifestFileList> list = manifestFiles.find(type);
    if (!list)
    {
        // The temporary path we unpack to is based on so file's current location and workunit
        // MORE - this is good for deployed cases, may not be so good for standalone executables.
        StringBuffer tempDir;
        splitFilename(name, &tempDir, &tempDir, &tempDir, nullptr);
        list.setown(new ManifestFileList(type, tempDir));
        tempDir.append(".tmp").append(PATHSEPCHAR).append(wuid);
        VStringBuffer xpath("Resource[@type='%s']", type);
        Owned<IPropertyTreeIterator> resourceFiles = queryManifest().getElements(xpath.str());
        ForEach(*resourceFiles)
        {
            IPropertyTree &resourceFile = resourceFiles->query();
            unsigned id = resourceFile.getPropInt("@id", 0);
            size32_t len = 0;
            const void *data = nullptr;
            if (!getResource(len, data, type, id, false))
                throwUnexpected();
            MemoryBuffer decompressed;
            if (resourceFile.getPropBool("@compressed"))
            {
                // MORE - would be better to try to spot files that are not worth recompressing (like jar files)?
                decompressResource(len, data, decompressed);
                data = decompressed.toByteArray();
                len = decompressed.length();
            }
            else
            {
                // Data is preceded by the resource header
                // MORE - does this depend on whether @header is set? is that what @header means?
                data = ((const byte *) data) + resourceHeaderLength;
                len -= resourceHeaderLength;
            }
            StringBuffer extractName(tempDir);
            extractName.append(PATHSEPCHAR);
            if (resourceFile.hasProp("@filename"))
                resourceFile.getProp("@filename", extractName);
            else
                extractName.append(id).append('.').append(type);
            recursiveCreateDirectoryForFile(extractName);
            OwnedIFile f = createIFile(extractName);
            OwnedIFileIO o = f->open(IFOcreaterw);
            assertex(o.get() != nullptr);
            o->write(0, len, data);
            list->append(extractName);
        }
        manifestFiles.replaceOwn(*list.getLink());
    }
    return list->queryFileNames();
}


//-------------------------------------------------------------------------------------------------------------------

bool PluginDll::init(IPluginContextEx * pluginCtx)
{
    HINSTANCE h = getInstance();
    assertex(h != (HINSTANCE) -1);
    EclPluginSetCtxEx pSetCtxEx = (EclPluginSetCtxEx) GetSharedProcedure(h,"setPluginContextEx");
    if (pSetCtxEx)
        pSetCtxEx(pluginCtx);
    else
    {
        // Older plugins may only support setPluginContext - fall back to that
        EclPluginSetCtx pSetCtx = (EclPluginSetCtx) GetSharedProcedure(h,"setPluginContext");
        if (pSetCtx)
            pSetCtx(pluginCtx);
    }

    EclPluginDefinition p= (EclPluginDefinition) GetSharedProcedure(h,"getECLPluginDefinition");
    if (!p)
        return false;

    pb.size = sizeof(ECLPluginDefinitionBlockEx);
    if (!p(&pb))
    {
        pb.compatibleVersions = NULL;
        pb.size = sizeof(ECLPluginDefinitionBlock);
        if (!p(&pb))
            return false;
    }
    return true;
}


bool PluginDll::checkVersion(const char *expected)
{
    assertex(expected);
    if (stricmp(pb.version, expected) == 0)
        return true;

    if (pb.compatibleVersions)
    {
        const char **finger = pb.compatibleVersions;
        while (*finger)
        {
            if (stricmp(*finger, expected) == 0)
                return true;
            finger++;
        }
    }
    return false;
}


void PluginDll::logLoaded()
{
    logLoad = true;
    DBGLOG("Loaded DLL %s [%s]", name.get(), pb.version);
}

extern DLLSERVER_API ILoadedDllEntry * createDllEntry(const char *path, bool isGlobal, const IFileIO *dllFile, bool resourcesOnly)
{
    Owned<HelperDll> result = new HelperDll(path, dllFile);
    bool ok;
    if (!resourcesOnly)
        ok = result->load(isGlobal, true);
    else
        ok = result->loadResources();
    if (!ok)
        throw MakeStringException(0, "Failed to create ILoadedDllEntry for dll %s", path);
    return result.getClear();
}

extern DLLSERVER_API ILoadedDllEntry * createExeDllEntry(const char *path)
{
    Owned<HelperDll> result = new HelperDll(path, NULL);
    if (!result->loadCurrentExecutable())
        throw MakeStringException(0, "Failed to create ILoadedDllEntry for current executable");
    return result.getClear();
}

extern DLLSERVER_API bool decompressResource(size32_t len, const void *data, MemoryBuffer &result)
{
    bool hasVersion = len && (*(const byte *)data == 0x80);
    MemoryBuffer src;
    src.setBuffer(len, const_cast<void *>(data), false);
    byte version = 1;
    if (hasVersion)
    {
        src.skip(1);
        src.read(version);
    }

    switch (version)
    {
    case 1:
        decompressToBuffer(result, src);
        break;
    default:
        throwUnexpected();
    }

    return true;
}

extern DLLSERVER_API bool decompressResource(size32_t len, const void *data, StringBuffer &result)
{
    MemoryBuffer tgt;
    if (len)
        decompressResource(len, data, tgt);
    tgt.append((char)0);
    unsigned expandedLen = tgt.length();
    result.setBuffer(expandedLen, reinterpret_cast<char *>(tgt.detach()), expandedLen-1);
    return true;
}

extern DLLSERVER_API void appendResource(MemoryBuffer & mb, size32_t len, const void *data, bool compress)
{
    mb.append((byte)0x80).append(resourceHeaderVersion);
    if (compress)
        compressToBuffer(mb, len, data);
    else
        appendToBuffer(mb, len, data);
}

extern DLLSERVER_API void compressResource(MemoryBuffer & compressed, size32_t len, const void *data)
{
    appendResource(compressed, len, data, true);
}

extern DLLSERVER_API bool getEmbeddedWorkUnitXML(ILoadedDllEntry *dll, StringBuffer &xml)
{
    size32_t len = 0;
    const void * data = NULL;
    if (!dll->getResource(len, data, "WORKUNIT", 1000))
        return false;
    return decompressResource(len, data, xml);
}

extern DLLSERVER_API bool getEmbeddedManifestXML(const ILoadedDllEntry *dll, StringBuffer &xml)
{
    size32_t len = 0;
    const void * data = NULL;
    if (!dll->getResource(len, data, "MANIFEST", 1000))
        return false;
    return decompressResource(len, data, xml);
}

extern DLLSERVER_API bool getEmbeddedArchiveXML(ILoadedDllEntry *dll, StringBuffer &xml)
{
    size32_t len = 0;
    const void * data = NULL;
    if (!dll->getResource(len, data, "ARCHIVE", 1000))
        return false;
    return decompressResource(len, data, xml);
}

extern DLLSERVER_API IPropertyTree *getEmbeddedManifestPTree(const ILoadedDllEntry *dll)
{
    StringBuffer xml;
    return getEmbeddedManifestXML(dll, xml) ? createPTreeFromXMLString(xml.str()) : createPTree();
}

extern DLLSERVER_API bool checkEmbeddedWorkUnitXML(ILoadedDllEntry *dll)
{
    size32_t len = 0;
    const void * data = NULL;
    return dll->getResource(len, data, "WORKUNIT", 1000, false);
}

extern DLLSERVER_API bool getResourceXMLFromFile(const char *filename, const char *type, unsigned id, StringBuffer &xml)
{
    MemoryBuffer data;
    if (!getResourceFromFile(filename, data, type, id))
        return false;
    return decompressResource(data.length(), data.toByteArray(), xml);
}

extern DLLSERVER_API bool getWorkunitXMLFromFile(const char *filename, StringBuffer &xml)
{
    return getResourceXMLFromFile(filename, "WORKUNIT", 1000, xml);
}

extern DLLSERVER_API bool getArchiveXMLFromFile(const char *filename, StringBuffer &xml)
{
    return getResourceXMLFromFile(filename, "ARCHIVE", 1000, xml);
}

extern DLLSERVER_API bool getManifestXMLFromFile(const char *filename, StringBuffer &xml)
{
    return getResourceXMLFromFile(filename, "MANIFEST", 1000, xml);
}

//-------------------------------------------------------------------------------------------------------------------

extern DLLSERVER_API void getAdditionalPluginsPath(StringBuffer &pluginsPath, const char *_base)
{
    // We only add the additional plugins if the plugins path already includes the default plugins location
    StringBuffer base(_base);
    removeTrailingPathSepChar(base);
    removeTrailingPathSepChar(pluginsPath);
    StringBuffer defaultLocation(base);
    defaultLocation.append(PATHSEPSTR "plugins");
    StringArray paths;
    paths.appendList(pluginsPath, ENVSEPSTR);
    if (paths.contains(defaultLocation))
    {
        const char *additional = queryEnvironmentConf().queryProp("additionalPlugins");
        if (additional)
        {
            StringArray additionalPaths;
            additionalPaths.appendList(additional, ENVSEPSTR);
            ForEachItemIn(idx, additionalPaths)
            {
                const char *additionalPath = additionalPaths.item(idx);
                pluginsPath.append(ENVSEPCHAR);
                if (!isAbsolutePath(additionalPath))
                    pluginsPath.append(base).append(PATHSEPSTR "versioned" PATHSEPSTR);
                pluginsPath.append(additionalPath);
            }
        }
    }
}

bool SafePluginMap::addPlugin(const char *path, const char *dllname)
{
    if (!endsWithIgnoreCase(path, SharedObjectExtension))
    {
        if (trace)
            DBGLOG("Ecl plugin %s ignored", path);
        return false;
    }

    try
    {
        CriticalBlock b(crit);
        ILoadedDllEntry *dll = map.getValue(dllname);
        if (!dll)
        {
            Owned<PluginDll> n = new PluginDll(path, NULL);
            // Note - we used to load plugins with global=true, but that caused issues when loading
            // Python3 and Python2 plugins at the same time as the export similar symbols
            // Loading with global=false should not cause any adverse issues
            if (!n->load(false, false) || !n->init(pluginCtx))
                throw MakeStringException(0, "Failed to load plugin %s", path);
            if (trace)
                n->logLoaded();
            map.setValue(dllname, n);  // note: setValue links arg
            return true;
        }
        return false;
    }
    catch (IException * e) // MORE - not sure why we don't throw exceptions back here...
    {
        EXCLOG(e, "Loading plugin");
        e->Release();
        return false;
    }
}


ILoadedDllEntry * SafePluginMap::getPluginDll(const char *id, const char *version, bool checkVersion)
{
    CriticalBlock b(crit);
    Linked<PluginDll> ret = static_cast<PluginDll *>(map.getValue(id));
    if (ret && checkVersion)
    {
        if (!ret->checkVersion(version))
            return NULL;
    }
    return ret.getLink();
}

void SafePluginMap::loadFromList(const char * pluginsList)
{
    const char *pluginDir = pluginsList;
    for (;*pluginDir;)
    {
        StringBuffer thisPlugin;
        while (*pluginDir && *pluginDir != ENVSEPCHAR)
            thisPlugin.append(*pluginDir++);
        if(*pluginDir)
            pluginDir++;

        if(!thisPlugin.length())
            continue;

        Owned<IFile> file = createIFile(thisPlugin.str());
        if (file->isDirectory() == fileBool::foundYes)
            loadFromDirectory(thisPlugin);
        else
        {
            StringBuffer tail;
            splitFilename(thisPlugin, NULL, NULL, &tail, &tail);
            addPlugin(thisPlugin, tail.str());
        }
    }
}

void SafePluginMap::loadFromDirectory(const char * pluginDirectory)
{
    const char * mask = "*" SharedObjectExtension;
    
    Owned<IFile> pluginDir = createIFile(pluginDirectory);
    Owned<IDirectoryIterator> pluginFiles = pluginDir->directoryFiles(mask,false,false);
    ForEach(*pluginFiles)
    {
        const char *thisPlugin = pluginFiles->query().queryFilename();
        StringBuffer tail;
        splitFilename(thisPlugin, NULL, NULL, &tail, &tail);
        addPlugin(thisPlugin, tail.str());
    }
}
