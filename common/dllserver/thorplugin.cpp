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
#include "jlzw.hpp"
#include "eclrtl.hpp"
#ifdef _USE_BINUTILS
#define PACKAGE "hpcc-system"
#define PACKAGE_VERSION "1.0"
#include "bfd.h"
#elif defined(__APPLE__)
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

class HelperDll : public CInterface, implements ILoadedDllEntry
{
    SharedObject so;
    StringAttr name;
    Linked<const IFileIO> dllFile;
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

    bool load(bool isGlobal, bool raiseOnError);
    bool loadCurrentExecutable();
    virtual void logLoaded();
    virtual bool checkVersion(const char *expected);
};

class PluginDll : public HelperDll
{
    ECLPluginDefinitionBlockEx pb;

public:
    PluginDll(const char *_name, const IFileIO *_dllFile) : HelperDll(_name, _dllFile) {}

    bool init(IPluginContextEx * pluginCtx);

    virtual bool checkVersion(const char *expected);
    virtual void logLoaded();
};

HelperDll::HelperDll(const char *_name, const IFileIO *_dllFile) 
: name(_name), dllFile(_dllFile)
{
    logLoad = false;
}

bool HelperDll::load(bool isGlobal, bool raiseOnError)
{
    if (!so.load(name, isGlobal, raiseOnError))
        return false;
    return true;
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
}

HINSTANCE HelperDll::getInstance() const
{
    return so.getInstanceHandle();
}

void * HelperDll::getEntry(const char * name) const
{
    return so.getEntry(name);
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
#ifdef _WIN32
    HINSTANCE dllHandle = so.getInstanceHandle();
    HRSRC hrsrc = FindResource(dllHandle, MAKEINTRESOURCE(id), "BIGSTRING");
    if (hrsrc)
        return (const byte *) LoadResource(dllHandle, hrsrc);
    return NULL;
#else
    StringBuffer resourceName;
    resourceName.appendf("BIGSTRING_%d_txt_start", id);
    return (const byte *) getEntry(resourceName.str());
#endif
}

const byte resourceHeaderVersion=1;
const size32_t resourceHeaderLength = sizeof(byte) + sizeof(byte) + sizeof(bool) + sizeof(size32_t);


bool HelperDll::getResource(size32_t & len, const void * & data, const char * type, unsigned id, bool trace) const
{
#ifdef _WIN32
    HINSTANCE dllHandle = so.getInstanceHandle();
    HRSRC hrsrc = FindResource(dllHandle, MAKEINTRESOURCE(id), type);
    if (!hrsrc)
        return false;
    len = SizeofResource(dllHandle, hrsrc);
    data = (const byte *) LoadResource(dllHandle, hrsrc);
    return true;
#else
    StringBuffer symName;
    symName.append(type).append("_").append(id).append("_txt_start");
    data = (const void *) getEntry(symName.str());
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
    if (bom!=0x80)
        return false;
    mb.read(version);
    if (version>resourceHeaderVersion)
        return false;
    mb.read(compressed).read(len);
    len+=resourceHeaderLength;
    return true;
#endif
}

#ifdef _USE_BINUTILS
struct SecScanParam
{
    MemoryBuffer &result;
    const char *sectionName;
    SecScanParam(MemoryBuffer &_result, const char *_sectionName) 
        : result(_result), sectionName(_sectionName)
    {
    }
};

static void secscan (bfd *file, sec_ptr sec, void *userParam)
{
    SecScanParam *param = (SecScanParam *) userParam;
    if (strcmp(param->sectionName, bfd_section_name (file, sec))==0)
    {
        bfd_size_type size = bfd_section_size (file, sec);
        void *data = (void *) param->result.reserve(size);
        bfd_get_section_contents(file, sec, data, 0, size);
    }
}
#endif

static bool getResourceFromMappedFile(const char * filename, const byte * start_addr, MemoryBuffer &data, const char * type, unsigned id)
{
#if defined(_WIN32) || defined (_USE_BINUTILS)
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
    unsigned char *data2 = getsectiondata(mh, "__TEXT", sectname.str(), &len);
    data.append(len, data2);
    return true;
#else
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
            data.append(section.sh_size, start_addr + section.sh_offset);
            return true;
        }
    }

    DBGLOG("Failed to extract resource %s: Does not include a matching entry", filename);
    return false;
#endif
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
#elif defined (_USE_BINUTILS)
    bfd_init ();
    bfd *file = bfd_openr(filename, NULL);
    if (file)
    {
        StringBuffer sectionName;
        sectionName.append(type).append("_").append(id).append(".data");
        SecScanParam param(data, sectionName.str());
        if (bfd_check_format (file, bfd_object))
            bfd_map_over_sections (file, secscan, &param);
        bfd_close (file);
   }
   return data.length() != 0;
#else
    struct stat stat_buf;
    VStringBuffer sectname("%s_%u", type, id);
    int fd = open(filename, O_RDONLY);
    if (fd == -1 || fstat(fd, &stat_buf) == -1)
    {
        DBGLOG("Failed to load library %s: %d", filename, errno);
        return false;
    }

    bool ok = false;
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
    close(fd);
    return ok;
#endif
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
    HelperDll::logLoaded();
    DBGLOG("Current reported version is %s", pb.version);
    if (pb.compatibleVersions)
    {
        const char **finger = pb.compatibleVersions;
        while (*finger)
        {
            DBGLOG("Compatible version %s", *finger);
            finger++;
        }
    }
}

extern DLLSERVER_API ILoadedDllEntry * createDllEntry(const char *path, bool isGlobal, const IFileIO *dllFile)
{
    Owned<HelperDll> result = new HelperDll(path, dllFile);
    if (!result->load(isGlobal, true))
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

extern DLLSERVER_API bool getEmbeddedManifestXML(ILoadedDllEntry *dll, StringBuffer &xml)
{
    size32_t len = 0;
    const void * data = NULL;
    if (!dll->getResource(len, data, "MANIFEST", 1000))
        return false;
    return decompressResource(len, data, xml);
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

extern DLLSERVER_API bool getManifestXMLFromFile(const char *filename, StringBuffer &xml)
{
    return getResourceXMLFromFile(filename, "MANIFEST", 1000, xml);
}


//-------------------------------------------------------------------------------------------------------------------


//-------------------------------------------------------------------------------------------------------------------

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
            if (!n->load(true, false) || !n->init(pluginCtx))
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
        if (file->isDirectory() == foundYes)
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
