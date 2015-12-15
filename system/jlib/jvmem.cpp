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

#include "platform.h"
#include "jlib.hpp"
#include "jiface.hpp"
#include "jexcept.hpp"
#include "jbuff.hpp"

#ifndef WIN32
#include <sys/mman.h>
#endif

#include "jvmem.hpp"

class CVMSectionAllocator: public CInterface, implements IVMAllocator 
{
protected: friend class CVMAllocator;
    byte *base;
    size32_t maxsize;
    offset_t ofs;
    struct cFreeItem
    {
        cFreeItem *next;
        size32_t ofs;
        size32_t size;
    } freelist;

public:
    IMPLEMENT_IINTERFACE;


    CVMSectionAllocator()
    {
        base = NULL;
        freelist.next = NULL;
        freelist.size = 0;
    }

    ~CVMSectionAllocator()
    {
#ifdef WIN32
        if (base)
            UnmapViewOfFile(base);
#else
        if (base)
            munmap(base, maxsize);
#endif
        while (freelist.next) {
            cFreeItem *p = freelist.next;
            freelist.next = p->next;
            delete p;
        }
    }


    bool init(HANDLE hfile,offset_t _ofs, size32_t size
#ifdef WIN32
        ,HANDLE hmap
#endif
        )
    {
        ofs = _ofs;
        maxsize = VMPAGEROUND(size);
        assertex(maxsize==size);
        freelist.ofs = 0;
        freelist.size = maxsize;
        freelist.next = NULL;
#ifdef WIN32
        LARGE_INTEGER li;
        li.QuadPart = _ofs; 
        base = (byte *) MapViewOfFile(hmap, FILE_MAP_READ|FILE_MAP_WRITE, li.HighPart, li.LowPart, size);
#else
        offset_t savedPos = lseek(hfile,0,SEEK_CUR);
        offset_t length = lseek(hfile,0,SEEK_END);
        lseek(hfile,savedPos,SEEK_SET);
        if (length<ofs+maxsize) {
            if (0 != ftruncate(hfile,ofs+maxsize))
                throw makeErrnoException(errno, "CVMSectionAllocator truncate");
        }
        base = (byte *)mmap(NULL, maxsize, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_NORESERVE, hfile, _ofs);

        if (base == (byte *)MAP_FAILED)
            base = NULL;
//      memset(base,0xdd,maxsize);
#endif
        return base!=NULL;
    }

    void *alloc(size32_t sz)
    {
        sz = VMPAGEROUND(sz);
        cFreeItem *p = &freelist;
        cFreeItem *pp = NULL;
        do {
            if (sz<=p->size) {
                void *ret = base+p->ofs;
                if (sz==p->size) {
                    if (pp) {
                        pp->next = p->next;
                        delete p;
                    }
                    else if (p->next) { // p==freelist
                        p = freelist.next;
                        freelist = *p;
                        delete p;
                    }
                    else {
                        freelist.ofs = 0;
                        freelist.size = 0;
                    }
                }
                else {
                    p->size-=sz;
                    p->ofs+=sz;
                }
                return ret;
            }
            pp = p;
            p = p->next;
        } while (p);
        return NULL;
    }

    bool dealloc(void *ptr,size32_t sz)
    {
        sz = VMPAGEROUND(sz); // was allocated by free
        if ((memsize_t)ptr<(memsize_t)base)
            return false;
        memsize_t mo = (memsize_t)ptr-(memsize_t)base;
        if (mo>=maxsize)
            return false;
        size32_t o = (size32_t)mo;
        size32_t e = o+sz;
        assertex(e<=maxsize);   // can't straddle blocks
        cFreeItem *p = &freelist;
        cFreeItem *pp = NULL;
        loop {
            cFreeItem *n=p->next;
            if (e<p->ofs) {
                n = new cFreeItem;
                *n = *p;
                p->next = n;
                p->ofs = o;
                p->size = sz;
                break;
            }
            if (e==p->ofs) {
                p->ofs -= sz;
                p->size += sz;
                // coalesce prev
                if (pp&&(pp->ofs+pp->size==p->ofs)) {
                    pp->size += p->size;
                    pp->next = n;
                    delete p;
                    p=pp;
                }
                // fallthrough to coalesce next
            }
            else if (o==p->ofs+p->size) {
                p->size += sz;
                // fallthrough to coalesce next
            }
            else if (n) {
                pp = p;
                p = n;
                n = p->next;
                continue; // loop from the middle
            }
            else {
                // add to end
                n = new cFreeItem;
                p->next = n;
                n->ofs = o;
                n->size = sz;
                n->next = NULL;
                break;
            }
            // coalesce next
            if (n&&(n->ofs==p->ofs+p->size)) {
                p->size += n->size;
                p->next = n->next;
                delete n;
            }
            break;
        }
        return true;
    }

    offset_t allocated() const
    {
        const cFreeItem *p = &freelist;
        offset_t ret=maxsize;
        do {
            ret -= p->size;
            p = p->next;
        } while (p);
        return ret;
    }
};

#define SECTIONSIZE (64*0x100000)        // 64MB -- assumes multiple of page size

class CVMAllocator: public CInterface, implements IVMAllocator 
{
    StringAttr filename;
    UnsignedArray freesections;
    IArrayOf<CVMSectionAllocator> sections;
    HANDLE hfile;
    offset_t maxsize;
    offset_t totalallocated;
    mutable CriticalSection sect;


#ifdef WIN32
    HANDLE hmap;
#endif

public:
    IMPLEMENT_IINTERFACE;

    CVMAllocator(const char *_filename,offset_t size)
        : filename(_filename)
    {
        maxsize = size;
        totalallocated = 0;
        offset_t normsize = size+SECTIONSIZE-1;
        unsigned nsections=(unsigned)(normsize/SECTIONSIZE);
#ifdef WIN32
        hfile = CreateFile(filename, GENERIC_READ|GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_FLAG_RANDOM_ACCESS, 0);
        if (hfile == INVALID_HANDLE_VALUE) 
            throw makeOsExceptionV(GetLastError(), "CVMAllocator(CreateFile) %s", filename);
        LARGE_INTEGER li;
        li.QuadPart = nsections*SECTIONSIZE; 
//      li.LowPart = SetFilePointer(hfile, li.LowPart, &li.HighPart, FILE_BEGIN);
//      SetEndOfFile(hfile);
        hmap = CreateFileMapping(hfile, NULL, PAGE_READWRITE, li.HighPart, li.LowPart, NULL);
#else
        hfile = _lopen(filename.get(), O_RDWR | O_CREAT | O_TRUNC, 0);
        if (hfile == -1)
            throw makeOsExceptionV(GetLastError(), "CMmapLargeMemoryAllocator(CreateFile) %s", filename.get());
        unlink(filename);   // delete now (linux won't really delete until handle released)
#endif
        for (unsigned i=nsections;i;)
            freesections.append(--i);
    }

    ~CVMAllocator()
    {
        sections.kill();        
#ifdef WIN32
        CloseHandle(hmap);
        CloseHandle(hfile);
        DeleteFile(filename);
#else
        close(hfile);
#endif
    }

    void *alloc(size32_t sz)
    {
        CriticalBlock block(sect);
        offset_t nta = totalallocated + sz;
        if (nta>maxsize)
            return NULL;
        void *ret;
        if (sz>SECTIONSIZE) { // too big, panic and resort to OS!
            ret = malloc(sz);
            if (ret) 
                totalallocated = nta;
            return ret;
        }
        ForEachItemInRev(i,sections) {
            ret = sections.item(i).alloc(sz);
            if (ret) {
                totalallocated = nta;
                return ret;
            }
        }
        if (freesections.ordinality()==0)
            return NULL;
        unsigned s = freesections.popGet();
        CVMSectionAllocator *section = new CVMSectionAllocator();
        if (!section)
            return NULL;
        if (!section->init(hfile,SECTIONSIZE*(offset_t)s,SECTIONSIZE
#ifdef WIN32
            ,hmap
#endif
            )) {
            section->Release();
            return NULL;
        }
        ret = section->alloc(sz); 
        if (!ret) { // hmm - error?
            section->Release();
            return NULL;
        }
        totalallocated = nta;
        sections.append(*section);
        return ret;
    }
    bool dealloc(void *ptr,size32_t sz)
    {
        CriticalBlock block(sect);
        if (!ptr)
            return true;
        if (sz>SECTIONSIZE) { // was too big, resorted to OS
            free(ptr);
            totalallocated -= sz;
            return true;
        }
        ForEachItemIn(i,sections) {
            CVMSectionAllocator &section = sections.item(i);
            if (section.dealloc(ptr,sz)) {
                if (section.maxsize==section.freelist.size) { // empty
                    freesections.append((unsigned)(section.ofs/SECTIONSIZE));
                    sections.remove(i);
                }
                totalallocated -= sz;
                return true;
            }
        }
        return false;
    }

    offset_t allocated() const
    {
        CriticalBlock block(sect);
        return totalallocated;
    }

};

IVMAllocator *createVMAllocator(const char *filename,offset_t size)
{
    return new CVMAllocator(filename,size);
}


//CVMLargeMemoryAllocator: 
    
void CVMLargeMemoryAllocator::allocchunkmem()
{
    chunk.base = (byte *)vm->alloc(chunk.max);
}

void CVMLargeMemoryAllocator::disposechunkmem()
{
    vm->dealloc(chunk.base,chunk.max);
}


CVMLargeMemoryAllocator::CVMLargeMemoryAllocator(
                          IVMAllocator *_vm,    
                          size32_t _totalmax,
                          size32_t _chunkmin,
                          bool _throwexception)
   : CLargeMemoryAllocator(_totalmax,_chunkmin,_throwexception), vm(_vm)
{
}




#ifdef TESTVMEM
void testVM()
{
    Owned<IVMAllocator> vm = createVMAllocator(
#ifdef WIN32
        "d:\\vmtemp.$$$",
#else
        "/d$/vmtemp.$$$",
#endif
        0x40000000*2U);
    unsigned n = SECTIONSIZE/VMPAGESIZE;
    void **ptrs = new void *[n];
    unsigned *sizes = new unsigned[n];
    size32_t allocated=0;
    unsigned i;
    for (i=0;i<n;i++) {
        allocated += (i+1)*VMPAGESIZE;
        ptrs[i] = vm->alloc((i+1)*VMPAGESIZE);
        if (!ptrs[i])
            break;
    }
    printf("allocated %u in %d blocks\n",allocated,n);
    printf("sleeping...\n");
    Sleep(1000*60);
    printf("slept\n");
    n = i;
    for (i=n;i;i--)
        ptrs[i-1] = memset(ptrs[i-1],i%17,i*VMPAGESIZE);
    printf("written %u\n",allocated);
    printf("sleeping...\n");
    Sleep(1000*60);
    printf("slept\n");
    for (i=0;i<n;i++) {
        if (i==n-1)
            printf("last\n");
        if (!vm->dealloc(ptrs[i],(i+1)*VMPAGESIZE))
            printf("not deallocated %d\n",i);
    }
    printf("deallocated %u now allocated =%" I64F "d\n",allocated,vm->allocated());
    delete [] sizes;
    delete [] ptrs;
}
#endif
