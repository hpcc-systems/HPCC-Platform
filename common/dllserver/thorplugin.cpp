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
#include "jevent.hpp"

#include "eclrtl.hpp"
#include <ctype.h>
#if defined(__APPLE__)
#include <sys/mman.h>
#include <sys/stat.h>
#elif !defined(_WIN32)
#include <sys/mman.h>
#include <sys/stat.h>
#endif

#include "thorplugin.hpp"

static constexpr CompressionMethod defaultResourceCompression = COMPRESS_METHOD_ZSTD3;

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

static constexpr uint32_t mach64SegmentCommand = 0x19;

struct Elf64Header
{
    byte ident[16];
    uint16_t type;
    uint16_t machine;
    uint32_t version;
    uint64_t entry;
    uint64_t programHeaderOffset;
    uint64_t sectionHeaderOffset;
    uint32_t flags;
    uint16_t headerSize;
    uint16_t programHeaderEntrySize;
    uint16_t programHeaderCount;
    uint16_t sectionHeaderEntrySize;
    uint16_t sectionHeaderCount;
    uint16_t sectionNameIndex;
};

struct Elf64SectionHeader
{
    uint32_t name;
    uint32_t type;
    uint64_t flags;
    uint64_t address;
    uint64_t offset;
    uint64_t size;
    uint32_t link;
    uint32_t info;
    uint64_t addressAlign;
    uint64_t entrySize;
};

struct Elf32Header
{
    byte ident[16];
    uint16_t type;
    uint16_t machine;
    uint32_t version;
    uint32_t entry;
    uint32_t programHeaderOffset;
    uint32_t sectionHeaderOffset;
    uint32_t flags;
    uint16_t headerSize;
    uint16_t programHeaderEntrySize;
    uint16_t programHeaderCount;
    uint16_t sectionHeaderEntrySize;
    uint16_t sectionHeaderCount;
    uint16_t sectionNameIndex;
};

struct Elf32SectionHeader
{
    uint32_t name;
    uint32_t type;
    uint32_t flags;
    uint32_t address;
    uint32_t offset;
    uint32_t size;
    uint32_t link;
    uint32_t info;
    uint32_t addressAlign;
    uint32_t entrySize;
};

struct Mach64Header
{
    uint32_t magic;
    uint32_t cpuType;
    uint32_t cpuSubtype;
    uint32_t fileType;
    uint32_t commandCount;
    uint32_t commandSize;
    uint32_t flags;
    uint32_t reserved;
};

struct MachLoadCommand
{
    uint32_t command;
    uint32_t size;
};

struct Mach64SegmentCommand
{
    uint32_t command;
    uint32_t size;
    char segmentName[16];
    uint64_t virtualAddress;
    uint64_t virtualSize;
    uint64_t fileOffset;
    uint64_t fileSize;
    uint32_t maximumProtection;
    uint32_t initialProtection;
    uint32_t sectionCount;
    uint32_t flags;
};

struct Mach64Section
{
    char sectionName[16];
    char segmentName[16];
    uint64_t address;
    uint64_t size;
    uint32_t offset;
    uint32_t align;
    uint32_t relocationOffset;
    uint32_t relocationCount;
    uint32_t flags;
    uint32_t reserved1;
    uint32_t reserved2;
    uint32_t reserved3;
};

static bool rangeIsValid(size_t offset, size_t length, size_t bufferLength)
{
    return offset <= bufferLength && length <= bufferLength - offset;
}

template <class STRUCT>
static bool readStruct(const byte *startAddress, size_t bufferLength, size_t offset, STRUCT &value)
{
    if (!rangeIsValid(offset, sizeof(value), bufferLength))
        return false;
    memcpy(&value, startAddress + offset, sizeof(value));
    return true;
}

static bool calculateArraySize(size_t elementSize, size_t count, size_t &totalSize)
{
    if (count && elementSize > SIZE_MAX / count)
        return false;
    totalSize = elementSize * count;
    return true;
}

static bool fixedStringMatches(const char *fixed, size_t fixedLength, const char *value)
{
    size_t valueLength = strlen(value);
    return valueLength <= fixedLength && memcmp(fixed, value, valueLength) == 0 && (valueLength == fixedLength || fixed[valueLength] == 0);
}

static bool getElf64Resource(const byte *startAddress, size_t bufferLength, size32_t &length, const void *&data, const char *sectionName)
{
    Elf64Header header;
    if (!readStruct(startAddress, bufferLength, 0, header))
        return false;
    if (memcmp(header.ident, "\x7f" "ELF", 4) != 0 || header.ident[4] != 2 || header.ident[5] != 1)
        return false;
    if (header.sectionHeaderEntrySize < sizeof(Elf64SectionHeader) || header.sectionNameIndex >= header.sectionHeaderCount)
        return false;
    size_t sectionTableSize;
    if (!calculateArraySize(header.sectionHeaderEntrySize, header.sectionHeaderCount, sectionTableSize) || !rangeIsValid(header.sectionHeaderOffset, sectionTableSize, bufferLength))
        return false;

    Elf64SectionHeader nameSection;
    if (!readStruct(startAddress, bufferLength, header.sectionHeaderOffset + static_cast<size_t>(header.sectionNameIndex) * header.sectionHeaderEntrySize, nameSection))
        return false;
    if (!rangeIsValid(nameSection.offset, nameSection.size, bufferLength))
        return false;
    const char *names = reinterpret_cast<const char *>(startAddress + nameSection.offset);
    for (unsigned index = 0; index < header.sectionHeaderCount; index++)
    {
        Elf64SectionHeader section;
        if (!readStruct(startAddress, bufferLength, header.sectionHeaderOffset + static_cast<size_t>(index) * header.sectionHeaderEntrySize, section))
            return false;
        if (section.name >= nameSection.size || !memchr(names + section.name, 0, nameSection.size - section.name))
            continue;
        if (streq(names + section.name, sectionName) && rangeIsValid(section.offset, section.size, bufferLength) && section.size <= UINT_MAX)
        {
            length = static_cast<size32_t>(section.size);
            data = startAddress + section.offset;
            return true;
        }
    }
    return false;
}

static bool getElf32Resource(const byte *startAddress, size_t bufferLength, size32_t &length, const void *&data, const char *sectionName)
{
    Elf32Header header;
    if (!readStruct(startAddress, bufferLength, 0, header))
        return false;
    if (memcmp(header.ident, "\x7f" "ELF", 4) != 0 || header.ident[4] != 1 || header.ident[5] != 1)
        return false;
    if (header.sectionHeaderEntrySize < sizeof(Elf32SectionHeader) || header.sectionNameIndex >= header.sectionHeaderCount)
        return false;
    size_t sectionTableSize;
    if (!calculateArraySize(header.sectionHeaderEntrySize, header.sectionHeaderCount, sectionTableSize) || !rangeIsValid(header.sectionHeaderOffset, sectionTableSize, bufferLength))
        return false;

    Elf32SectionHeader nameSection;
    if (!readStruct(startAddress, bufferLength, header.sectionHeaderOffset + static_cast<size_t>(header.sectionNameIndex) * header.sectionHeaderEntrySize, nameSection) || !rangeIsValid(nameSection.offset, nameSection.size, bufferLength))
        return false;
    const char *names = reinterpret_cast<const char *>(startAddress + nameSection.offset);
    for (unsigned index = 0; index < header.sectionHeaderCount; index++)
    {
        Elf32SectionHeader section;
        if (!readStruct(startAddress, bufferLength, header.sectionHeaderOffset + static_cast<size_t>(index) * header.sectionHeaderEntrySize, section))
            return false;
        if (section.name >= nameSection.size || !memchr(names + section.name, 0, nameSection.size - section.name))
            continue;
        if (streq(names + section.name, sectionName) && rangeIsValid(section.offset, section.size, bufferLength))
        {
            length = section.size;
            data = startAddress + section.offset;
            return true;
        }
    }
    return false;
}

static bool getMach64Resource(const byte *startAddress, size_t bufferLength, size32_t &length, const void *&data, const char *sectionName)
{
    Mach64Header header;
    if (!readStruct(startAddress, bufferLength, 0, header))
        return false;
    if (memcmp(startAddress, "\xcf\xfa\xed\xfe", 4) != 0 || !rangeIsValid(sizeof(header), header.commandSize, bufferLength))
        return false;

    size_t commandOffset = sizeof(header);
    size_t commandEnd = commandOffset + header.commandSize;
    for (unsigned index = 0; index < header.commandCount; index++)
    {
        MachLoadCommand command;
        if (!rangeIsValid(commandOffset, sizeof(command), commandEnd) || !readStruct(startAddress, bufferLength, commandOffset, command))
            return false;
        if (command.size < sizeof(command) || !rangeIsValid(commandOffset, command.size, commandEnd))
            return false;
        if (command.command == mach64SegmentCommand && command.size >= sizeof(Mach64SegmentCommand))
        {
            Mach64SegmentCommand segment;
            if (!readStruct(startAddress, bufferLength, commandOffset, segment))
                return false;
            size_t sectionsSize;
            if (!calculateArraySize(sizeof(Mach64Section), segment.sectionCount, sectionsSize) || sectionsSize > command.size - sizeof(segment))
                return false;
            for (unsigned sectionIndex = 0; sectionIndex < segment.sectionCount; sectionIndex++)
            {
                Mach64Section section;
                if (!readStruct(startAddress, bufferLength, commandOffset + sizeof(segment) + static_cast<size_t>(sectionIndex) * sizeof(section), section))
                    return false;
                if (fixedStringMatches(section.segmentName, sizeof(section.segmentName), "__TEXT") && fixedStringMatches(section.sectionName, sizeof(section.sectionName), sectionName) && rangeIsValid(section.offset, section.size, bufferLength) && section.size <= UINT_MAX)
                {
                    length = static_cast<size32_t>(section.size);
                    data = startAddress + section.offset;
                    return true;
                }
            }
        }
        commandOffset += command.size;
    }
    return false;
}

static bool getResourceFromMappedFile(const char *filename, const byte *startAddress, size_t bufferLength, size32_t &length, const void *&data, const char *type, unsigned id)
{
    VStringBuffer sectionName("%s_%u", type, id);
    if (bufferLength >= 16)
    {
        if (memcmp(startAddress, "\x7f" "ELF", 4) == 0)
        {
            if (startAddress[4] == 2)
                return getElf64Resource(startAddress, bufferLength, length, data, sectionName.str());
            if (startAddress[4] == 1)
                return getElf32Resource(startAddress, bufferLength, length, data, sectionName.str());
        }
        if (memcmp(startAddress, "\xcf\xfa\xed\xfe", 4) == 0)
            return getMach64Resource(startAddress, bufferLength, length, data, sectionName.str());
    }
    DBGLOG("Failed to extract resource %s: unrecognized or unsupported executable format", filename);
    return false;
}

static bool getResourceFromMappedFile(const char *filename, const byte *startAddress, size_t bufferLength, MemoryBuffer &result, const char *type, unsigned id)
{
    size32_t len = 0;
    const void * data = nullptr;
    bool ok = getResourceFromMappedFile(filename, startAddress, bufferLength, len, data, type, id);
    if (ok)
        result.append(len, data);
    return ok;
}

extern DLLSERVER_API bool getResourceFromBuffer(const void *buffer, size32_t bufferLength, MemoryBuffer &data, const char *type, unsigned id)
{
    return getResourceFromMappedFile("memory buffer", static_cast<const byte *>(buffer), bufferLength, data, type, id);
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
            ok = getResourceFromMappedFile(filename, start_addr, size, data, type, id);
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
    mutable CriticalSection manifestLock{SYNC_LOCATION};
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
    virtual const StringArray &queryManifestFiles(const char *type, const char *wuid, const char *tempRoot) const override;
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
: dllFile(_dllFile), manifestFiles(false), name(_name)
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
        return getResourceFromMappedFile(name, mappedDll->base(), mappedDll->length(), len, data, type, id);
    }
}

IPropertyTree &HelperDll::queryManifest() const
{
    return *querySingleton(manifest, manifestLock, [this]{ return getEmbeddedManifestPTree(this); });
}

const StringArray &HelperDll::queryManifestFiles(const char *type, const char *wuid, const char *tempRoot) const
{
    CriticalBlock b(manifestLock);
    Linked<ManifestFileList> list = manifestFiles.find(type);
    if (!list)
    {
        // The temporary path we unpack to is based on so file's current location and workunit
        // MORE - this is good for deployed cases, may not be so good for standalone executables.
        // Doesn't really work for cloud either - it needs to be in ephemeral there
        StringBuffer tempDir;
        if (!isEmptyString(tempRoot))
        {
            tempDir.append(tempRoot);
            addPathSepChar(tempDir);
            splitFilename(name, nullptr, nullptr, &tempDir, nullptr);
        }
        else
        {
            splitFilename(name, &tempDir, &tempDir, &tempDir, nullptr);
        }
        list.setown(new ManifestFileList(type, tempDir));
        tempDir.append(".tmp").append(PATHSEPCHAR).append(wuid);
        VStringBuffer xpath("Resource[@type='%s']", type);
        Owned<IPropertyTreeIterator> resourceFiles = queryManifest().getElements(xpath.str());
        ForEach(*resourceFiles)
        {
            unsigned start = msTick();
            IPropertyTree &resourceFile = resourceFiles->query();
            if (resourceFile.hasProp("@jfrogUser"))
            {
                StringBuffer localpath(tempDir.append(PATHSEPCHAR).append(resourceFile.queryProp("@filename")));
                getResourceFromJfrog(localpath, resourceFile);
                list->append(localpath);
            }
            else
            {
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
                OwnedIFileIO o = f->open(IFOcreate);
                assertex(o.get() != nullptr);
                o->write(0, len, data);
                o->close();

                list->append(extractName);
                if (doTrace(traceJava) && streq(type, "jar"))
                    DBGLOG("Extracted jar resource %u size %u to %s in %u ms", id, len, extractName.str(), msTick() - start);
            }
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
    ProTraceTaskScopeTracker tracker(EventTask::Reading);

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

static const char *skipArchiveResourceWhitespace(MemoryBuffer &archive, const char *&archiveEnd)
{
    if (!archive.length())
    {
        archiveEnd = nullptr;
        return nullptr;
    }
    const char *archiveText = static_cast<const char *>(archive.toByteArray());
    archiveEnd = archiveText + archive.length();
    while (archiveText != archiveEnd && isspace(static_cast<unsigned char>(*archiveText)))
        archiveText++;
    return archiveText;
}

static IPropertyTree *createArchiveResourcePTree(MemoryBuffer &archive)
{
    const char *archiveEnd = nullptr;
    const char *archiveText = skipArchiveResourceWhitespace(archive, archiveEnd);
    try
    {
        if (archiveText != archiveEnd && *archiveText == '<')
        {
            size32_t textOffset = archiveText - static_cast<const char *>(archive.toByteArray());
            archive.append((char)0);
            return createPTreeFromXMLString(static_cast<const char *>(archive.toByteArray()) + textOffset, ipt_caseInsensitive|ipt_lowmem);
        }
        return createPTree(archive, ipt_caseInsensitive|ipt_lowmem);
    }
    catch (IException *e)
    {
        e->Release();
        return nullptr;
    }
}

static IPropertyTree *decompressArchiveResourcePTree(size32_t len, const void *data)
{
    MemoryBuffer archive;
    if (!decompressResource(len, data, archive))
        return nullptr;
    return createArchiveResourcePTree(archive);
}

static bool decompressArchiveResourceXML(size32_t len, const void *data, StringBuffer &xml)
{
    MemoryBuffer archive;
    if (!decompressResource(len, data, archive))
        return false;

    xml.clear();
    // Archive resources may be stored as XML text or serialized binary PTree data. Prefer the
    // text path when the decompressed bytes look like XML; otherwise assume binary and parse it.
    const char *archiveEnd = nullptr;
    const char *archiveText = skipArchiveResourceWhitespace(archive, archiveEnd);
    if (archiveText != archiveEnd && *archiveText == '<')
        xml.append(archive.length(), static_cast<const char *>(archive.toByteArray()));
    else
    {
        Owned<IPropertyTree> archiveTree = createArchiveResourcePTree(archive);
        if (!archiveTree)
        {
            xml.clear();
            return false;
        }
        toXML(archiveTree, xml);
    }
    return true;
}

extern DLLSERVER_API void appendResource(MemoryBuffer & mb, size32_t len, const void *data, bool compress)
{
    mb.append((byte)0x80).append(resourceHeaderVersion);
    compressToBuffer(mb, len, data, compress ? defaultResourceCompression : COMPRESS_METHOD_NONE);
}

extern DLLSERVER_API void compressResource(MemoryBuffer & compressed, size32_t len, const void *data)
{
    appendResource(compressed, len, data, true);
}

extern DLLSERVER_API bool getEmbeddedWorkUnitXML(ILoadedDllEntry *dll, StringBuffer &xml)
{
    size32_t len = 0;
    const void * data = NULL;
    if (!dll->getResource(len, data, "WORKUNIT", 1000, false))
        return false;
    return decompressResource(len, data, xml);
}

extern DLLSERVER_API bool getEmbeddedWorkUnitBinary(ILoadedDllEntry *dll, MemoryBuffer &result)
{
    size32_t len = 0;
    const void * data = NULL;
    if (!dll->getResource(len, data, "BINWORKUNIT", 1000, false))
        return false;
    return decompressResource(len, data, result);
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
    return decompressArchiveResourceXML(len, data, xml);
}

extern DLLSERVER_API IPropertyTree *getEmbeddedArchivePTree(ILoadedDllEntry *dll)
{
    size32_t len = 0;
    const void * data = NULL;
    if (!dll->getResource(len, data, "ARCHIVE", 1000))
        return createPTree();

    Owned<IPropertyTree> archiveTree = decompressArchiveResourcePTree(len, data);
    if (!archiveTree)
        return createPTree();
    return archiveTree.getClear();
}

extern DLLSERVER_API IPropertyTree *getEmbeddedManifestPTree(const ILoadedDllEntry *dll)
{
    StringBuffer xml;
    return getEmbeddedManifestXML(dll, xml) ? createPTreeFromXMLString(xml.str()) : createPTree();
}

extern DLLSERVER_API bool containsEmbeddedWorkUnit(ILoadedDllEntry *dll)
{
    size32_t len = 0;
    const void * data = NULL;
    return dll->getResource(len, data, "BINWORKUNIT", 1000, false) ||
           dll->getResource(len, data, "WORKUNIT", 1000, false);
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
    MemoryBuffer data;
    if (!getResourceFromFile(filename, data, "ARCHIVE", 1000))
        return false;
    return decompressArchiveResourceXML(data.length(), data.toByteArray(), xml);
}

extern DLLSERVER_API bool getManifestXMLFromFile(const char *filename, StringBuffer &xml)
{
    return getResourceXMLFromFile(filename, "MANIFEST", 1000, xml);
}

extern DLLSERVER_API bool getWorkunitBinaryFromFile(const char *filename, MemoryBuffer &result)
{
    MemoryBuffer data;
    if (!getResourceFromFile(filename, data, "BINWORKUNIT", 1000))
        return false;
    return decompressResource(data.length(), data.toByteArray(), result);
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
#ifdef _CONTAINERIZED
    //MORE: No place to provide additional plugins...
#else
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
#endif
}

bool SafePluginMap::addPlugin(const char *path, const char *dllname)
{
    StringBuffer fullpath;
    if (!endsWithIgnoreCase(path, SharedObjectExtension))
    {
        //Check to see if the plugin name was the name of the plugin without the shared object extension
        //If the tail of the path does not contain an extension then check if the corresponding plugin exists.
        bool ok = false;
        StringBuffer tail;
        splitFilename(path, &fullpath, &fullpath, &tail, &tail);

        if (!strchr(tail, '.'))
        {
            fullpath.appendf("%s%s%s", SharedObjectPrefix, tail.str(), SharedObjectExtension);
            Owned<IFile> cur = createIFile(fullpath);
            if (cur->isFile() == fileBool::foundYes)
            {
                path = fullpath;
                ok = true;
            }
       }

        if (!ok)
        {
            if (trace)
                DBGLOG("Ecl plugin %s ignored", path);
            return false;
        }
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

bool SafePluginMap::loadNamed(const char * pluginDirectories, const char * plugin)
{
    const char *pluginDir = pluginDirectories;
    for (;*pluginDir;)
    {
        StringBuffer thisFile;
        while (*pluginDir && *pluginDir != ENVSEPCHAR)
            thisFile.append(*pluginDir++);
        if(*pluginDir)
            pluginDir++;

        if(!thisFile.length())
            continue;
        Owned<IFile> dir = createIFile(thisFile);
        if (dir->isDirectory() == fileBool::foundYes)
        {
            Owned<IFile> file = createIFile(addPathSepChar(thisFile).append(plugin));
            if (file->exists())
            {
                if (addPlugin(thisFile, plugin))
                    return true;
            }
        }
        else if (dir->isFile() == fileBool::foundYes)
        {
            StringBuffer tail;
            splitFilename(thisFile, NULL, NULL, &tail, &tail);
            if (streq(tail, plugin) && addPlugin(thisFile, plugin))
                return true;
        }
    }
    return false;
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
