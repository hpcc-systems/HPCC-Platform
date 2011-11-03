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

#include "jlib.hpp"
#include "hqlres.hpp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "hqlcerrors.hpp"
#include "thorplugin.hpp"
#ifndef _WIN32
#include "bfd.h"
#endif

#define BIGSTRING_BASE 101
#define MANIFEST_BASE 1000

class ResourceItem : public CInterface
{
public:
    ResourceItem(const char * _type, unsigned _id, size32_t _len, const void * _ptr) 
        : data(_len, _ptr), type(_type), id(_id) {}

public:
    MemoryAttr data;
    StringAttr type;
    unsigned id;
};


ResourceManager::ResourceManager()
{
    nextmfid = MANIFEST_BASE + 1;
    nextbsid = BIGSTRING_BASE;
    totalbytes = 0;
    finalized=false;
}

unsigned ResourceManager::count()
{
    return resources.ordinality();
}

unsigned ResourceManager::addString(unsigned len, const char *data)
{
    unsigned id = nextbsid++;
    resources.append(*new ResourceItem("BIGSTRING", id, len, data));
    return id;
}

void ResourceManager::addNamed(const char * type, unsigned len, const void * data, IPropertyTree *manifestEntry, unsigned id, bool addToManifest, bool compressed)
{
    if (id==(unsigned)-1)
        id = nextmfid++;
    if (addToManifest)
    {
        if (finalized)
            throwError1(HQLERR_ResourceAddAfterFinalManifest, type);
        Owned<IPropertyTree> entry=createPTree("Resource");
        entry->setProp("@type", type);
        entry->setPropInt("@id", id);
        if (compressed)
            entry->setPropBool("@compressed", true);
        if (manifestEntry)
            mergePTree(entry, manifestEntry);
        ensureManifestInfo()->addPropTree("Resource", entry.getClear());
    }
    resources.append(*new ResourceItem(type, id, len, data));
}

bool ResourceManager::addCompress(const char * type, unsigned len, const void * data, IPropertyTree *manifestEntry, unsigned id, bool addToManifest)
{
    bool isCompressed=false;
    if (len>=32) //lzw assert if too small
    {
        isCompressed = true;
        MemoryBuffer compressed;
        compressResource(compressed, len, data);
        addNamed(type, compressed.length(), compressed.toByteArray(), manifestEntry, id, addToManifest, isCompressed);
    }
    else
        addNamed(type, len, data, manifestEntry, id, addToManifest, isCompressed);
    return isCompressed;
}

static void loadResource(const char *filepath, MemoryBuffer &content)
{
    Owned <IFile> f = createIFile(filepath);
    Owned <IFileIO> fio = f->open(IFOread);
    read(fio, 0, (size32_t) f->size(), content);
}

bool ResourceManager::getDuplicateResourceId(const char *srctype, const char *filename, int &id)
{
    VStringBuffer xpath("Resource[@filename='%s']", filename);
    Owned<IPropertyTreeIterator> iter = manifest->getElements(xpath.str());
    ForEach (*iter)
    {
        IPropertyTree &item = iter->query();
        if (item.hasProp("@id"))
        {
            const char *type = item.queryProp("@type");
            if (type && strieq(type, srctype))
            {
                id=item.getPropInt("@id");
                return true;
            }
        }
    }
    return false;
}

void ResourceManager::addManifest(IPropertyTree *manifestSrc, const char *dir)
{
    if (finalized)
        throwError1(HQLERR_ResourceAddAfterFinalManifest, "MANIFEST");

    ensureManifestInfo();
    Owned<IAttributeIterator> aiter = manifestSrc->getAttributes();
    ForEach (*aiter)
        manifest->setProp(aiter->queryName(), aiter->queryValue());
    Owned<IPropertyTreeIterator> iter = manifestSrc->getElements("*");
    ForEach(*iter)
    {
        IPropertyTree &item = iter->query();
        if (streq(item.queryName(), "Resource") && item.hasProp("@filename"))
        {
            if (dir && !item.hasProp("@rootPath"))
                item.setProp("@rootPath", dir);
            if (!item.hasProp("@type"))
                item.setProp("@type", "UNKNOWN");
            const char *filename = item.queryProp("@filename");
            int id;
            if (getDuplicateResourceId(item.queryProp("@type"), filename, id))
            {
                item.setPropInt("@id", id);
                manifest->addPropTree("Resource", LINK(&item));
            }
            else
            {
                StringBuffer fullpath;
                if (!isAbsolutePath(filename) && item.hasProp("@rootPath"))
                    fullpath.append(item.queryProp("@rootPath"));
                fullpath.append(filename);

                MemoryBuffer content;
                loadResource(fullpath.str(), content);
                addCompress(item.queryProp("@type"), content.length(), content.toByteArray(), &item);
            }
        }
        else
            manifest->addPropTree(item.queryName(), LINK(&item));
    }
}

void ResourceManager::addManifest(const char *filename)
{
    Owned<IPropertyTree> manifestSrc = createPTreeFromXMLFile(filename);

    if (!strieq(manifestSrc->queryName(), "manifest"))
        throwError1(HQLERR_IncorrectResourceContentType, "MANIFEST");

    StringBuffer dir;
    splitDirTail(filename, dir);

    addManifest(manifestSrc, dir.str());
}

void ResourceManager::addManifestFromArchive(IPropertyTree *archive)
{
    if (!archive)
        return;
    if (finalized)
        throwError1(HQLERR_ResourceAddAfterFinalManifest, "MANIFEST");
    ensureManifestInfo();
    Owned<IPropertyTreeIterator> manifests = archive->getElements("AdditionalFiles/Manifest");
    ForEach(*manifests)
    {
        const char *xml = manifests->query().queryProp(NULL);
        Owned<IPropertyTree> manifestSrc = createPTreeFromXMLString(xml);
        Owned<IAttributeIterator> aiter = manifestSrc->getAttributes();
        ForEach (*aiter)
            manifest->setProp(aiter->queryName(), aiter->queryValue());
        const char *manifestFilename = manifestSrc->queryProp("@originalFilename");
        Owned<IPropertyTreeIterator> iter = manifestSrc->getElements("*");
        ForEach(*iter)
        {
            IPropertyTree &item = iter->query();
            if (streq(item.queryName(), "Resource")&& item.hasProp("@filename"))
            {
                if (manifestFilename && *manifestFilename && !item.hasProp("@rootPath"))
                    item.setProp("@rootPath", manifestFilename);
                if (!item.hasProp("@type"))
                    item.setProp("@type", "UNKNOWN");
                const char *filename = item.queryProp("@filename");
                int id;
                if (getDuplicateResourceId(item.queryProp("@type"), filename, id))
                {
                    item.setPropInt("@id", (int)id);
                    manifest->addPropTree("Resource", LINK(&item));
                }
                else
                {
                    VStringBuffer xpath("AdditionalFiles/Resource[@originalFilename=\"%s\"]", filename);
                    MemoryBuffer content;
                    archive->getPropBin(xpath.str(), content);
                    addCompress(item.queryProp("@type"), content.length(), content.toByteArray(), &item);
                }
            }
            else
                manifest->addPropTree(item.queryName(), LINK(&item));
        }
    }
}

void ResourceManager::addResourceIncludes(IPropertyTree *includes)
{
    if (!includes)
        return;
    if (finalized)
        throwError1(HQLERR_ResourceAddAfterFinalManifest, "MANIFEST");
    ensureManifestInfo();
    Owned<IPropertyTree> directIncludeManifest = createPTree("Manifest");
    Owned<IPropertyTreeIterator> it = includes->getElements("*");
    ForEach(*it)
    {
        IPropertyTree &item = it->query();
        StringBuffer path(item.queryProp("@scope"));
        if (path.length())
            path.replace('.', '/').append('/');
        if (strieq(item.queryName(), "manifest"))
            addManifest(path.append(item.queryProp("@filename")).str());
        else
        {
            if (path.length())
                item.setProp("@rootPath", path.str());
            item.setProp("@type", item.queryName());
            directIncludeManifest->addPropTree("Resource", LINK(&item));
        }
    }
    if (directIncludeManifest->hasChildren())
        addManifest(directIncludeManifest, NULL);
}

void ResourceManager::addWebServiceInfo(IPropertyTree *wsinfo)
{
    //convert legacy web service info to the new resource format
    if (wsinfo)
    {
        if (wsinfo->hasProp("SOAP"))
            ensureManifestInfo()->addProp("WS-PARAMS", wsinfo->queryProp("SOAP"));
        if (wsinfo->hasProp("HELP"))
        {
            const char *content = wsinfo->queryProp("HELP");
            addCompress("HELP", strlen(content)+1, content);
        }
        if (wsinfo->hasProp("INFO"))
        {
            const char *content = wsinfo->queryProp("INFO");
            addCompress("INFO", strlen(content)+1, content);
        }
        if (wsinfo->hasProp("OTX"))
        {
            const char *content = wsinfo->queryProp("OTX");
            addCompress("HYPER-LINK", strlen(content)+1, content);
        }
        if (wsinfo->hasProp("HTML"))
        {
            const char *content = wsinfo->queryProp("HTML");
            Owned<IPropertyTree> manifestEntry = createPTree("Resource");
            manifestEntry->setProp("@name", "Custom Form");
            addCompress("XSLT", strlen(content)+1, content, manifestEntry);
            IPropertyTree *view = ensurePTree(ensureManifestInfo(), "Views/XSLT/FORM");
            view->setProp("@resource", "Custom Form");
        }
        if (wsinfo->hasProp("HTMLD"))
        {
            const char *content = wsinfo->queryProp("HTMLD");
            Owned<IPropertyTree> manifestEntry = createPTree("Resource");
            manifestEntry->setProp("@name", "Custom HTML");
            addCompress("HTML", strlen(content)+1, content, manifestEntry);
            IPropertyTree *view = ensurePTree(ensureManifestInfo(), "Views/HTML/FORM");
            view->setProp("@resource", "Custom HTML");
        }
        if (wsinfo->hasProp("RESULT"))
        {
            const char *content = wsinfo->queryProp("RESULT");
            Owned<IPropertyTree> manifestEntry = createPTree("Resource");
            manifestEntry->setProp("@name", "Results");
            addCompress("XSLT", strlen(content)+1, content, manifestEntry);
            IPropertyTree *view = ensurePTree(ensureManifestInfo(), "Views/XSLT/RESULTS");
            view->setProp("@resource", "Results");
        }
        if (wsinfo->hasProp("ERROR"))
        {
            const char *content = wsinfo->queryProp("ERROR");
            Owned<IPropertyTree> manifestEntry = createPTree("Resource");
            manifestEntry->setProp("@name", "Error");
            addCompress("XSLT", strlen(content)+1, content, manifestEntry);
            IPropertyTree *view = ensurePTree(ensureManifestInfo(), "Views/XSLT/ERROR");
            view->setProp("@resource", "Error");
        }
    }
}

void ResourceManager::finalize()
{
    if (!finalized)
    {
        if (manifest)
        {
            StringBuffer content;
            toXML(manifest, content);
            addCompress("MANIFEST", content.length()+1, content.str(), NULL, MANIFEST_BASE, false);
        }
        finalized=true;
    }
}

void ResourceManager::putbytes(int h, const void *b, unsigned len)
{
    int written = _write(h, b, len);
    assertex(written == len);
    totalbytes += len;
}

void ResourceManager::flushAsText(const char *filename)
{
    finalize();

    StringBuffer name;
    int len = strlen(filename);
    name.append(filename,0,len-4).append(".txt");

    FILE* f = fopen(name.str(), "wb");
    if (f==NULL)
    {
        PrintLog("Create resource text file %s failed", name.str());
        return; // error is ignorable.
    }

    ForEachItemIn(idx, resources)
    {
        ResourceItem&s = (ResourceItem&)resources.item(idx);
        fwrite(s.data.get(),1,s.data.length(),f);
    }

    fclose(f);
}

void ResourceManager::flush(const char *filename, bool flushText, bool target64bit)
{
    finalize();

    // Use "resources" for strings that are a bit large to generate in the c++ (some compilers had limits at 64k) 
    // or that we want to access without having to run the dll/so
    // In linux there is no .res concept but we can achieve the same effect by generating an object file with a specially-named section 
    // bintils tools can be used to extract the data externally (internally we just have a named symbol for it)
#ifdef _WIN32
    int h = _open(filename, _O_WRONLY|_O_CREAT|_O_TRUNC|_O_BINARY|_O_SEQUENTIAL, _S_IREAD | _S_IWRITE | _S_IEXEC);
    
    //assertex(h != HFILE_ERROR);
    if (h == HFILE_ERROR) // error can not be ignored!
        throwError1(HQLERR_ResourceCreateFailed, filename);

    totalbytes = 0;
    putbytes(h, "\x00\x00\x00\x00\x20\x00\x00\x00\xff\xff\x00\x00\xff\xff\x00\x00"
                "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 0x20);

    MemoryBuffer temp;
    ForEachItemIn(idx, resources)
    {
        ResourceItem&s = static_cast<ResourceItem&>(resources.item(idx));
        __int32 len = s.data.length();
        unsigned lenType = strlen(s.type);
        unsigned sizeType = (lenType+1)*2;
        unsigned sizeTypeName = (sizeType + 4);
        unsigned packedSizeTypeName = ((sizeTypeName + 2) & ~3);
        __int32 lenHeader = 4 + 4 + packedSizeTypeName + 4 + 2 + 2 + 4 + 4;
        unsigned short id = s.id;
        temp.clear();
        temp.append(sizeof(len), &len);
        temp.append(sizeof(lenHeader), &lenHeader);
        for (unsigned i=0; i < lenType; i++)
            temp.append((byte)s.type[i]).append((byte)0);
        temp.append((byte)0).append((byte)0);
        temp.append((byte)0xff).append((byte)0xff);
        temp.append(sizeof(id), &id);
        if (temp.length() & 2)
            temp.append((byte)0).append((byte)0);
        temp.append(4, "\x00\x00\x00\x00"); // version
        temp.append(12, "\x30\x10\x09\x04\x00\x00\x00\x00\x00\x00\x00\x00");    // 0x1030 memory 0x0409 language
        assertex(lenHeader == temp.length());

        putbytes(h, temp.bufferBase(), lenHeader);
        putbytes(h, s.data.get(), len);
        if (totalbytes & 3)
            putbytes(h, "\x00\x00\x00",4-(totalbytes & 3));
    }
    _close(h);
#else
    asymbol **syms = NULL;
    bfd *file = NULL;
    StringArray names;  // need to make sure that the strings we use in symbol table have appropriate lifetime 
    try 
    {
        bfd_init ();
        bfd_set_default_target(target64bit ? "x86_64-unknown-linux-gnu" : "x86_32-unknown-linux-gnu");
        const bfd_arch_info_type *temp_arch_info = bfd_scan_arch ("i386");
        file = bfd_openw(filename, target64bit ? "elf64-x86-64" : NULL);//MORE: Test on 64 bit to see if we can always pass NULL
        verifyex(file);
        verifyex(bfd_set_arch_mach(file, temp_arch_info->arch, temp_arch_info->mach));
        verifyex(bfd_set_start_address(file, 0));
        verifyex(bfd_set_format(file, bfd_object));
        syms = new asymbol *[resources.length()*2+1];
        ForEachItemIn(idx, resources)
        {
            ResourceItem&s = (ResourceItem&)resources.item(idx);
            unsigned len = s.data.length();
            unsigned id = s.id;
            StringBuffer baseName;
            baseName.append(s.type).append("_").append(id);
            StringBuffer str;
            str.clear().append(baseName).append(".data");
            names.append(str);
            sec_ptr osection = bfd_make_section_anyway_with_flags (file, names.tos(), SEC_HAS_CONTENTS|SEC_ALLOC|SEC_LOAD|SEC_DATA|SEC_READONLY);
            verifyex(osection);
            verifyex(bfd_set_section_size(file, osection, len));
            verifyex(bfd_set_section_vma(file, osection, 0));
            bfd_set_reloc (file, osection, NULL, 0);
            osection->lma=0;
            osection->entsize=0;
            syms[idx*2] = bfd_make_empty_symbol(file);
            syms[idx*2]->flags = BSF_GLOBAL;
            syms[idx*2]->section = osection;
            names.append(str.clear().append(baseName).append("_txt_start"));
            syms[idx*2]->name = names.tos();
            syms[idx*2]->value = 0;
            syms[idx*2+1] = bfd_make_empty_symbol(file);
            syms[idx*2+1]->flags = BSF_GLOBAL;
            syms[idx*2+1]->section = bfd_abs_section_ptr;
            names.append(str.clear().append(baseName).append("_txt_size"));
            syms[idx*2+1]->name = names.tos();
            syms[idx*2+1]->value = len;
        }
        syms[resources.length()*2] = NULL;
        bfd_set_symtab (file, syms, resources.length()*2);
        // experience suggests symtab need to be in place before setting contents
        ForEachItemIn(idx2, resources)
        {
            ResourceItem &s = (ResourceItem&)resources.item(idx2);
            verifyex(bfd_set_section_contents(file, syms[idx2*2]->section, s.data.get(), 0, s.data.length()));
        }
        verifyex(bfd_close(file));
        delete [] syms;
    }
    catch (IException *E)
    {
        E->Release();
        //translate the assert exceptions into something else...
        StringBuffer msg;
        msg.appendf("%s: %s", filename, bfd_errmsg(bfd_get_error()));
        delete syms;
        if (file)
            bfd_close_all_done(file); // allow bfd to clean up memory
        throwError1(HQLERR_ResourceCreateFailed, msg.str());
    }
#endif
    if (flushText)
        flushAsText(filename);
}


bool ResourceManager::queryWriteText(StringBuffer & resTextName, const char * filename)
{
    int len = strlen(filename);
    resTextName.append(filename,0,len-4).append(".txt");
    return true;
}


#if 0
int test()
{
    ResourceManager r;
    r.add("Hello there!2");
    r.add("Hello again");
    r.flush("c:\\t2.res");
    return 6;
}

static int dummy = test();
#endif
