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
#ifndef _WIN32
#include "bfd.h"
#endif

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/hqlcpp/hqlres.cpp $ $Id: hqlres.cpp 66009 2011-07-06 12:28:32Z ghalliday $");

#define RESOURCE_BASE 101

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
    nextid = 0;
    totalbytes = 0;
}

unsigned ResourceManager::count()
{
    return resources.ordinality();
}

unsigned ResourceManager::addString(unsigned len, const char *data)
{
    unsigned id = RESOURCE_BASE + nextid++;
    resources.append(*new ResourceItem("BIGSTRING", id, len, data));
    return id;
}

void ResourceManager::addNamed(const char * type, unsigned id, unsigned len, const void * data)
{
    resources.append(*new ResourceItem(type, id, len, data));
}

void ResourceManager::putbytes(int h, const void *b, unsigned len)
{
    int written = _write(h, b, len);
    assertex(written == len);
    totalbytes += len;
}

void ResourceManager::flushAsText(const char *filename)
{
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
