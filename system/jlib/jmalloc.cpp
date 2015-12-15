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
#include "jlog.hpp"
#include "jbuff.hpp"

#ifndef WIN32
#include <sys/mman.h>
#endif

#include "jmalloc.hpp"


#ifdef _DEBUG
#define ASSERTEX(e) assertex(e)
#else
#define ASSERTEX(e)
#endif

static PointerArray osblkptrs;

// Super (large block) manager

#define  NoCheck        0
#define  InitCheck      1
#define  PtrCheck       2

/*
* Checking mode of the heap manager
*
*     NoCheck          - No checking!
*     InitCheck        - Initialise allocated and freed blocks of memory
*     PtrCheck        - Check validity of each pointer that is freed.  This
*                        should catch invalid frees, multiple frees and
*                        blocks which have been changed past the end of the
*                        allocated size.

*/

//#define HeapChecking         NoCheck
#define HeapChecking         InitCheck
//#define HeapChecking         PtrCheck


// Maximum number of bytes that will be non-OS allocated
#define  MaxSuperAllocSize     0xD000


#define InitialiseMemory     0  // initialises malloc & free'd blocks

#ifndef _DEBUG
#undef  HeapChecking
#undef  InitialiseMemory
#define HeapChecking    NoCheck
#endif

#if HeapChecking == NoCheck
#undef  InitialiseMemory
#endif


interface IWalkMem
{
    virtual void inuse(const void *a,size32_t sz)=0;
    virtual void osblock(const void *a,size32_t sz)=0;
};


/*
* The alignment that each memory block should have.
* MUST be a power of 2
*/

#define  Alignment          8
#define  OSCHUNKSIZE        0x100000U   // 1MB
#define  OSPAGESIZE         (0x1000U)
#define  OSPAGEMASK         (OSPAGESIZE-1)
#define  OSPAGEROUND(s)     (((s)+OSPAGEMASK)&~OSPAGEMASK)

#define MAXALLOCATESIZE       0x80000000U   // 2GB   (uses top bits for flag so don't increase!)
#define MAXUSERALLOCATESIZE   (MAXALLOCATESIZE-OSCHUNKSIZE-OSPAGESIZE)

#define ISSUBALLOCATED(sz) (sz>MAXALLOCATESIZE)

/* Get the aligned size of the information */
#define  DoAlign(size, align)    ((size + align - 1) & (~(align - 1)))
#define  Align(Size)             (DoAlign(Size, Alignment))

/*
* The minimum sensible size for a free block.
* MUST be less than 256 - Alignment.
* It must be at least Alignment(4 + CheckHeadSize + CheckTailSize
*/
#define  _MinFreeSize         32 // Should be Align(32)

#if HeapChecking >= PtrCheck
#define _MINFREESIZE   (_MinFreeSize + 4)
#undef  MinFreeSize
#define MinFreeSize    (_MINFREESIZE)
#else
#define MinFreeSize    _MinFreeSize
#endif


/******************** End of configurable constants ******************/

/*
* Check the constants are OK.
*/
#if MaxSuperAllocSize > OSCHUNKSIZE
#error "MaxSuperAllocSize is too large"
#endif

#if MinFreeSize >= (256 - Alignment)
#error "MinFreeSize too large" MinFreeSize Alignment
#endif


/**************** Various constants and type definitions ***************/

/* Following are macros so they can be used in #defines */
#define  BestFit        0
#define  FirstFit       1
#define  LargestFit     2


#if HeapChecking < PtrCheck
#define  CheckHeadSize  0
#define  CheckTailSize  0
#else
#define  CheckHeadSize   sizeof(byte)
#define  CheckTailSize   sizeof(byte)
#endif


/*
* Tags which tag where each block came from - unusual values so we don't
* normally hit on them by accident.
*/
#define  FreeBlockTag   0x34
#define  SuperAllocTag  0xa3
#define  OSAllocTag     0xe7
#define  BlockBoundTag  0x17

#define  HeadCheckVal   0x76
#define  CheckFillVal   0x4f

#define  FreeFillChar   0x66    // 'f'
#define  MallocFillChar 0x4d    // 'M'
#define  SubFillChar    0x53    // 'S'

/*
* The types used to hold information about the heap....
*/

typedef struct FreeListTag * FreeListPtr;

typedef struct FreeListTag
{
    FreeListPtr     Prev;
    FreeListPtr     Next;
    size32_t          Size;
    byte *        BlockPtr;
} FreeListEnt;

/*
* The 'structures' used at the start and end of the allocated/free
* blocks of memory.     (Not actually used - here for information).
*/
typedef struct BlockHeadTag
{
    byte            SizeChk;
    byte            Tag;
    size32_t          Size;
} BlockHeadRec;

typedef struct BlockTailTag
{
    byte            WriteChk;
    byte            Tag;
}  BlockTailRec;

/*****************************************************************
****************Set up several derived constants ****************
*****************************************************************/

#define HeadTagOff       (sizeof(size32_t) + sizeof(byte))
#define TailTagOff       (CheckTailSize)

#define HeadChkOff       (HeadTagOff + CheckHeadSize)
#define TailChkOff       0

/* Number of extra bytes that occur at the start and end of the record */
#define HeadExtraBytes   (sizeof(size32_t) + sizeof(byte) + sizeof(unsigned short) + CheckHeadSize)
#define TailExtraBytes   (CheckTailSize + sizeof(byte))

/* Total number of extra bytes for each allocated block. */
#define BlockExtraSize   (Align(HeadExtraBytes + TailExtraBytes))

#define OSBlockExtra     Align(BlockExtraSize + sizeof(FreeListPtr)) // this looks excessive to me
#define OSPtrExtra       Align(sizeof(FreeListPtr))



/*
****************** Helpful macros to get at fields *****************
*/
#define BlockSize(ptr)  (*((size32_t *)((byte *)ptr - sizeof(size32_t))))
#define UserLinkCount(ptr)   (*((unsigned short *)((byte *)ptr - HeadTagOff-sizeof(unsigned short))))
#define OwnerOffset(size) ((size) - sizeof(FreeListPtr))
#define ListOwner(ptr, size) *((FreeListPtr *)((byte *)ptr + OwnerOffset(size)))

#define  e_corrupt_owner            1
#define  e_mangled_free_list        2
#define  e_bad_block_ptr            3
#define  e_size_inconsitancy        4
#define  e_tag_mismatch             5
#define  e_free_block_tag_corrupt   6
#define  e_unknown_os_block         7
#define  e_free_block_twice         8
#define  e_corrupt_unknown_block    9
#define  e_illegal_size_field       10
#define  e_alloc_too_big            11
#define  e_out_free_space           12
#define  e_inconsist_block_tag      13
#define  e_end_heap_overwritten     14
#define  e_corrupt_sub_block        15
#define  e_out_of_memory            28   // coincides with ENOSPC



class jlib_thrown_decl CAllocatorOutOfMemException: public CInterface, implements IOutOfMemException
{
    int errcode;
    memsize_t wanted;
    memsize_t got;
public:
    IMPLEMENT_IINTERFACE;
    CAllocatorOutOfMemException(int _errcode,memsize_t _wanted,memsize_t _got)
    {
        errcode = _errcode;
        wanted = _wanted;
        got = _got;
    };

    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        str.append("JMalloc: Out of Memory (").append((offset_t)wanted);
        if (got)
            str.append(',').append((offset_t)got);
        return str.append(')');
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }
};


static void HeapError(int errnum, const void * BlockPtr=NULL)  // probably fatal
{
    StringBuffer tmp;
    tmp.appendf("JMalloc Heap error %d (%p)",errnum, BlockPtr);
    ERRLOG("%s",tmp.str());
    PrintStackReport();
    throw MakeStringException(errnum,"%s",tmp.str());
}


//--------------------------------------------------------------------------------
// Sub heap manager
//--------------------------------------------------------------------------------



#define SBOVERHEAD sizeof(short)
#define MINSUBBLOCKSIZE sizeof(void *)
#define SBPAGESIZE 4050   // default block size

class CSubAllocatorBlock;

struct CSubAllocatorBlockLinkHead
{
    CSubAllocatorBlock *next;   // next/prev must match CSubAllocatorBlockLink
    CSubAllocatorBlock *prev;
};

struct CSubAllocatorBlockData: public CSubAllocatorBlockLinkHead
{
    void *freelist;                 // points to free chain (note data i.e. past startadj)
    unsigned short recsize;         // doesn't includes startadj
    unsigned short usedcount;
    // { short startadj; byte[recsize] data; } startadj+&startadj = block begin
};




#define SBHEADERSIZE (sizeof(CSubAllocatorBlockData))
#define FIRSTSUBELEM(blk) ((byte *)(blk))+SBHEADERSIZE;
#define NEXTSUBELEMFREE(fl) (*(void **)(fl))

class CSubAllocatorBlock : public CSubAllocatorBlockData
{
public:
    static inline CSubAllocatorBlock *base(const void * p)
    {
        byte *b = (byte *)p-sizeof(short);
        b += *(short *)b;
        return  (CSubAllocatorBlock *)b;

    }

    bool subfree(void *p) // returns true if entire block gone
    {
#if HeapChecking >= PtrCheck
        assertex(usedcount);
#endif
#if InitialiseMemory
        memset((byte *)p+sizeof(freelist), FreeFillChar, recsize-sizeof(freelist));
#endif
        NEXTSUBELEMFREE(p) = freelist;
        freelist = p;
        return --usedcount==0;
    }


    inline bool full()
    {
        return freelist==NULL;
    }

    inline void *suballoc()
    {
        void *ret = freelist;
        if (ret) {
            freelist = NEXTSUBELEMFREE(freelist);
            usedcount++;
#if InitialiseMemory
            memset(ret, SubFillChar, recsize);
#endif
        }
        return ret;
    }


    void init(size32_t _recsize,unsigned num)   // recs
    {
        assertex(SBHEADERSIZE==sizeof(CSubAllocatorBlock));
        recsize = (unsigned short)_recsize;     // don't include startadj
        usedcount = 0;
        byte *p = FIRSTSUBELEM(this);
        freelist = p+sizeof(short);
        loop {
            *(short *)p = (short)((byte *)this-p);
            p += SBOVERHEAD;
            if (--num) { // not last
                NEXTSUBELEMFREE(p) = p+recsize+SBOVERHEAD; // forward chain
                p += recsize;
            }
            else {
                NEXTSUBELEMFREE(p) = NULL;
                break;
            }
        }
    }

    inline void unlink()
    {
        next->prev = prev;
        prev->next = next;
    }

    bool isFree(const void *a)
    {
        const void *p = freelist;
        while (p) {
            if (p==a)
                return true;
            p = NEXTSUBELEMFREE(p);
        }
        return false;
    }

    void walk(IWalkMem &walker)
    {
        byte *p = FIRSTSUBELEM(this);
        unsigned n = usedcount;
        while (n) {
            p += SBOVERHEAD;
            if (!isFree(p)) {
                n--;
                walker.inuse(p,recsize);
            }
            p += recsize;
        }
    }

};


struct CSubAllocatorBlockLink: public CSubAllocatorBlockLinkHead // for head of chain
{

    inline CSubAllocatorBlockLink()
    {
        next = (CSubAllocatorBlock *)this;
        prev = (CSubAllocatorBlock *)this;
    }

    inline bool isempty() { return next==(CSubAllocatorBlock *)this; }

    inline void prepend(CSubAllocatorBlock &link)
    {
        link.next = next;
        link.prev = (CSubAllocatorBlock *)this;
        link.next->prev = &link;
        next = &link;
    }

    inline void append(CSubAllocatorBlock &link)
    {
        link.next = (CSubAllocatorBlock *)this;
        link.prev = (CSubAllocatorBlock *)prev;
        link.prev->next = &link;
        prev = &link;
    }

};

class CSuperAllocator;

class CSubAllocator: public CInterface
{

    CSubAllocatorBlockLink *blks;
    NonReentrantSpinLock sublock;

    unsigned nblks;
    CSuperAllocator &superallocator;
    size32_t maxrecsize;    // even


    inline size32_t sbroundsz(size32_t recsize)
    {
        return (recsize<MINSUBBLOCKSIZE)?MINSUBBLOCKSIZE:((recsize+1)&0xfffffffe);
    }


    inline CSubAllocatorBlock *item(size32_t recsize, unsigned &i)
    {
        i = recsize/2-1;
        ASSERTEX(i<nblks);
        return blks[i].isempty()?NULL:blks[i].next;
    }



public:

    CSubAllocator(CSuperAllocator &_superallocator,size32_t _maxrecsize)
        : superallocator(_superallocator)
    {
        size32_t ms = (SBPAGESIZE-SBHEADERSIZE)/16-SBOVERHEAD; // fit at least 16 per page
        if (_maxrecsize>ms)
            _maxrecsize = ms;
        maxrecsize = sbroundsz(_maxrecsize);
        nblks = maxrecsize/2;
        blks = new CSubAllocatorBlockLink[nblks];
    }

    ~CSubAllocator();

    inline size32_t maxRecSize() const { return maxrecsize; }

    inline size32_t roundupSize(size32_t sz)
    {
        if (sz>maxrecsize)
            return 0;
        return sbroundsz(sz);
    }
    void *allocMem(size32_t sz, size32_t &usablesize) // returns NULL when htab full
    {
        if (sz>maxrecsize)
            return NULL;
        size32_t recsize = sbroundsz(sz);
        {
            NonReentrantSpinBlock block(sublock);
            unsigned i;
            CSubAllocatorBlock *b = item(recsize,i);
            if (b) {
                void *ret = b->suballoc();
                if (ret) {
                    if (b->full()) {
                        b->unlink();
                        blks[i].append(*b); // move to end
                    }
                    usablesize = recsize;
                    return ret;
                }
            }
        }
        unsigned n;
        CSubAllocatorBlock *b = (CSubAllocatorBlock *)getMem(recsize,n);
        b->init(recsize,n);
        NonReentrantSpinBlock block(sublock);
        blks[recsize/2-1].prepend(*b);
        usablesize = recsize;
        return b->suballoc();  // should work
    }


    void freeMem(void * p);

    void *reallocMem(void *p, size32_t sz, size32_t &usablesize)
    {
        ASSERTEX(p);
        ASSERTEX(sz);
        CSubAllocatorBlock *b = CSubAllocatorBlock::base(p); // don't need lock here
        size32_t oldsz = b->recsize;
        size32_t newsz = sbroundsz(sz);
        if (oldsz==newsz) {
            usablesize = newsz;
            return p;
        }
        void *ret = allocMem(sz,usablesize);
        if (!ret)
            return ret; // sligtly odd - indicates could not reallocate
        memcpy(ret,p,(oldsz<sz)?oldsz:sz);
        freeMem(p);
        return ret;
    }

    void *getMem(size32_t recsz,unsigned &n);

    size32_t usableSize(const void *p)
    {
        CSubAllocatorBlock *b = CSubAllocatorBlock::base(p); // don't need lock here
        return b->recsize;
    }

    void checkPtrValid(const void *p)
    {
        CSubAllocatorBlock *b = CSubAllocatorBlock::base(p);
        if ((memsize_t)p>=SBHEADERSIZE+(memsize_t)b) {
            NonReentrantSpinBlock block(sublock);
            unsigned i;
            CSubAllocatorBlock * f = item(b->recsize,i);
            while (f) {
                if (f==b)
                    return;
                f = f->next;
            }
        }
        HeapError(e_corrupt_sub_block,p);
    }

    void walk(IWalkMem &walker)
    {
        NonReentrantSpinBlock block(sublock);
        for (unsigned i=0;i<nblks;i++) {
            CSubAllocatorBlock *head = (CSubAllocatorBlock *)&blks[i];
            CSubAllocatorBlock *p = head->next;
            while (p!=head) {
                CSubAllocatorBlock *n = p->next;
                p->walk(walker);
                p = n;
            }
        }
    }

    bool isBlock(const void *blk)
    {
        NonReentrantSpinBlock block(sublock);
        for (unsigned i=0;i<nblks;i++) {
            CSubAllocatorBlock *head = (CSubAllocatorBlock *)&blks[i];
            CSubAllocatorBlock *p = head->next;
            while (p!=head) {
                if (p==blk)
                    return true;
                p = p->next;
            }
        }
        return false;
    }
};


/*
***************************************************************************
*                                                                         *
*  Implementation of the superheap manager:                             *
*                                                                         *
*  Designed with the following design goals:                              *
*                                                                         *
*  1. Good operation within a paged memory environment                    *
*  2. Quick location of free memory                                       *
*  3. Low fragmentation.                                                  *
*                                                                         *
*  This is achieved as follows:                                           *
*                                                                         *
*  The housekeeping information is kept separate from the allocated       *
*  memory.  This means that searching for a free block doesn't swap in    *
*  each of the allocated pages - just the small amount of house keeping   *
*  information.                                                           *
*                                                                         *
*  The routine is based on the Boundary Tag Method (described in          *
*  Fundamentals of Data Structures by Horowitz and Sahni).                *
*                                                                         *
*  The housekeeping information consists of a circular doubly linked list *
*  holding the following information about free blocks:                   *
*                                                                         *
*      LeftPtr  -> Next Block Lower in memory.                            *
*      RightPtr -> Next Block following this one in memory.               *
*      Size     -> Number of bytes (including extra bytes we might need). *
*      BlockPtr -> Pointer to the allocated block.                        *
*                                                                         *
*  Unused nodes on this free list are also linked on another linked list. *
*                                                                         *
*                                                                         *
*  The blocks of memory have the following format:                        *
*                                                                         *
*              Allocated:                            Free:                *
*                                                                         *
*  +-------------------------------+  +-------------------------------+   *
*  |       | ! | 1 |  S  I  Z  E   |  |   |   | ! | 0 |  S  I  Z  E   |   *
*  +-------------------------------+  +-------------------------------+   *
*  |                               |  | x   x   x   x   x   x   x   x |   *
*  |                               |  | x   x   x   x   x   x   x   x |   *
*  |                               |  | x   x   x   x +---------------+   *
*  |                     $   $   $ |  | x   x   x   x | ^HouseKeeping |   *
*  +-------------------------------+  +-------------------------------+   *
*  | ? | 1 |   |   |   |   |   |   |  |   | 0 |   |   |   |   |   |   |   *
*  +-------------------------------+  +-------------------------------+   *
*                                                                         *
*  The block of memory allocated when malloc() is called, is actually 8   *
*  bytes larger than required size.  6 bytes of house keeping information *
*  are tagged on the front of the block, and 2 on the end.                *
*
*  The 0 and 1 tags (together with the pointer to the housekeeping        *
*  information are used as in a quick method of checking whether a        *
*  block that is being free()'d should be concatenated with blocks to the *
*  left and right, and if so where those blocks are.                      *
*                                                                         *
*  The !, ? and the $ characters mark bytes that are used in the checking *
*  version of the heap system.                                            *
*                                                                         *
*  ? is a byte containing the difference between the number of bytes      *
*  requested, and the number of bytes in the block.  This byte is then    *
*  exclusive OR'd with a mask so the common error of storing a zero byte  *
*  past the end of memory is detected.                                    *
*                                                                         *
*  i.e. *TailCheck = (SizeBlock - SizeNeeded) ^ CheckMask.                *
*                                                                         *
*  The (SizeBlock - SizeNeeded) bytes at the end of the block are filled  *
*  with a special value - which can be checked that they're not modified. *
*                                                                         *
*  The ! is similar, except it contains the last byte of the size, so     *
*  we can check that the front of the block hasn't been overwritten.      *
*                                                                         *
*                                                                         *
*  The normal system of allocation is to ask the operating system for     *
*  large chunks of memory, and then use our routines to sub-divide the    *
*  memory up.                                                             *
*                                                                         *
*  Any requests for large chunks of memory are passed straight through to *
*  the OS, and the allocated block is tagged with the number 2, to        *
*  distinguish it from blocks that have been sub-allocated from large     *
*  blocks.                                                                *
*                                                                         *
*                                                                         *
*  The block pointers in the free list point just after the size field -  *
*  at the first byte of the memory that would be used when allocated.     *
*  Free->Size includes the size of the extra housekeeping information     *
*                                                                         *
*  It would be possible to                                                *
*      i. Only use one tag (at the end of the block), to reduce the       *
*         housekeeping information by one byte at the expense of extra    *
*         processing.                                                     *
*     ii. Could have different sizes, since most blocks are less than     *
*         64K or even 256 bytes.  Probably introduces unnecessary         *
*         complexity in the block processing routines.                    *
*                                                                         *
***************************************************************************
*/


class CSuperAllocator: public CInterface, implements IAllocator
{
    SpinLock lock;
    CFixedSizeAllocator FreeListAllocator;
    FreeListEnt HeadFreeList;
    FreeListEnt HeadOsBlockList;
    memsize_t OsTotal;
    memsize_t OsMax;
    memsize_t OsMin;

    FreeListPtr LastFreeEnt;

    Owned<CSubAllocator> suballocator;

    bool avoidReallocFragmentation;         // avoids reallocMem leaving gap


    inline FreeListPtr newFreeListEnt()
    {
        return (FreeListPtr)FreeListAllocator.alloc();
    }

    inline void InsertNode(FreeListEnt &head, FreeListPtr NewNode)
    {
        NewNode->Prev = &head;
        NewNode->Next = head.Next;
        head.Next = NewNode;
        NewNode->Next->Prev = NewNode;

        // Always HeadFreeList?? NH
    }

    inline void InsertFreeNode(FreeListPtr NewNode)
    {
        InsertNode(HeadFreeList,NewNode);
        LastFreeEnt = NewNode->Prev;   // Point at the next block to look at
    }


    inline void UnLinkNode(FreeListPtr CurNode)
    {
        /*
        * Remove the node from the free list - and add it to the list of unused
        * free blocks.
        */
        CurNode->Prev->Next = CurNode->Next;
        CurNode->Next->Prev = CurNode->Prev;
        FreeListAllocator.dealloc(CurNode);
    }


#if InitialiseMemory
    inline void InitFreeBlock(void * ptr)
    {
        memset(ptr, FreeFillChar, BlockSize(ptr) - BlockExtraSize);
    }

#else
#define InitFreeBlock(ptr)
#endif

    void checkPtrValid(const void * ptr)
    {
        /*
        * Perform several checks on a pointer that is being freed.
        *
        *   o That the tags at either end of the block are valid.
        *   o That the size is sensible
        *   o That the check bytes are OK.
        *   o That the padding bytes on the end of the block haven't been
        *     overwritten.
        */
#if HeapChecking >= PtrCheck

        /*
        * Check that the tags at the head and tail of the block hold valid numbers.
        */

        if (!ptr)
            return;

        size32_t bs = BlockSize(ptr);

        if (ISSUBALLOCATED(bs)) {
            suballocator->checkPtrValid(ptr);
            return;
        }

        byte HeadTagVal = *((byte *)ptr - HeadTagOff);
        if ((HeadTagVal != SuperAllocTag) && (HeadTagVal != OSAllocTag)) {
            if (HeadTagVal == FreeBlockTag)
                HeapError(e_free_block_twice,ptr);
            else
                HeapError(e_corrupt_unknown_block,ptr);
            return;
        }

        size32_t SourceSize = bs - BlockExtraSize;
        byte *EndPtr = (byte *)ptr + SourceSize;

        if ((SourceSize == 0) || ((SourceSize & (Alignment - 1)) != 0)) {
            HeapError(e_illegal_size_field);
            return;
        }

        byte TailTagVal = *(EndPtr + TailTagOff);
        if ((TailTagVal != SuperAllocTag) && (TailTagVal != OSAllocTag)) {
            if (TailTagVal == FreeBlockTag)
                HeapError(e_free_block_twice,ptr);
            else
                HeapError(e_corrupt_unknown_block,ptr);
            return;
        }

        if (HeadTagVal != TailTagVal) {
            HeapError(e_inconsist_block_tag);
            return;
        }

        if (*(EndPtr + TailChkOff) != CheckFillVal)
            HeapError(e_end_heap_overwritten,ptr);
#endif
    }


#if HeapChecking < PtrCheck
#define SetCheckInfo(StartAllocPtr, ActualSize, AllocSize)
#else

    void SetCheckInfo(byte * StartAllocPtr, size32_t ActualSize, size32_t AllocSize)
    {
        /*
        * Add checking info into the block.
        *
        * Fill the extra bytes that were allocated, but not requested
        * with special characters, so we can tell if they were overwritten.
        */
        byte *     EndPtr = (byte *)StartAllocPtr + AllocSize;

        *((byte *)StartAllocPtr - HeadChkOff) = HeadCheckVal;
        *(EndPtr + TailChkOff) = CheckFillVal;

    }


#endif

    memsize_t freeListMemRemaining(bool trace)
    {
        memsize_t sz = 0;
        unsigned num = 0;
        size32_t max = 0;
        size32_t min = (size32_t)-1;
        FreeListPtr  CurNode = LastFreeEnt;
        do {
            sz += CurNode->Size;
            if (CurNode->Size > max) max = CurNode->Size;
            if (CurNode->Size < min) min = CurNode->Size;
            num++;
            CurNode = CurNode->Next;
        }
        while (CurNode != LastFreeEnt);
        if (trace)
        {
            size32_t avg = (size32_t) (sz / num);
            StringBuffer report("FreeMem");
            report.newline();
            report.append("total free     : ").append((unsigned __int64)sz).newline();
            report.append("free ptr count : ").append(num).newline();
            report.append("largest        : ").append(max).newline();
            report.append("smallest       : ").append(min).newline();
            report.append("average        : ").append(avg).newline();
            PROGLOG("%s", report.str());
        }
        return sz;
    }
    void * OsAllocMem(size32_t sz)
    {
        ASSERTEX(sz==OSPAGEROUND(sz));
        if (OsTotal+sz>OsMax)
        {
            PrintStackReport();
            DBGLOG("Free list mem = %" I64F "d", (unsigned __int64)freeListMemRemaining(true));
            throw new CAllocatorOutOfMemException(e_out_of_memory,sz,OsTotal);
        }

#ifdef _WIN32
        void * ret = VirtualAlloc(NULL, sz, MEM_RESERVE|MEM_COMMIT, PAGE_READWRITE);
        if (ret == NULL) {
#else
        void * ret =  mmap(NULL,sz,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_NORESERVE|MAP_ANONYMOUS,-1,0);
        if (ret == (void *)MAP_FAILED) {
#endif
            PrintStackReport();
            DBGLOG("Free list mem = %" I64F "d", (unsigned __int64)freeListMemRemaining(true));
            throw new CAllocatorOutOfMemException(e_out_of_memory,sz,OsTotal);
            return NULL;
        }
        OsTotal += sz;
        return ret;
    }

    void OsFreeMem(void *ptr, size32_t sz)
    {
        if (!ptr)
            return;
        ASSERTEX(OsTotal>=sz);
        ASSERTEX(sz==OSPAGEROUND(sz));
        OsTotal -= sz;
#ifdef _WIN32
        if (!VirtualFree(ptr,0,MEM_RELEASE))
            HeapError(GetLastError(),ptr);
#else
        if (munmap(ptr,sz)<0)
            HeapError(errno ,ptr);
#endif

    }


    void * AllocOSChunk(size32_t  SizeRequired)
    {
        /*
        * Allocate a block of memory directly from the operating system.
        * Set up the housekeeping information that we need.
        */
        size32_t UserSize = Align(SizeRequired) + BlockExtraSize;
        size32_t TotalSize = OSPAGEROUND(UserSize + OSBlockExtra);
        byte *BlockPtr = (byte *) OsAllocMem((size32_t)TotalSize);

        byte * StartAllocPtr = BlockPtr + BlockExtraSize;

        /*
        * Set up the housekeeping information - the size, and tag it as a block
        * directly allocated from the OS.
        */
        BlockSize(StartAllocPtr) = UserSize;
        *(StartAllocPtr - HeadTagOff) = OSAllocTag;
        *(StartAllocPtr + Align(SizeRequired) + TailTagOff) = OSAllocTag;

        FreeListPtr CurNode = newFreeListEnt();
        CurNode->BlockPtr = BlockPtr;
        CurNode->Size = TotalSize;
        InsertNode(HeadOsBlockList,CurNode);
        ListOwner(BlockPtr, TotalSize) = CurNode;

        SetCheckInfo(StartAllocPtr, SizeRequired, Align(SizeRequired));
        return StartAllocPtr;
    }



    void * AllocBlock(FreeListPtr CurBlock, size32_t SizeRequired, size32_t TotalSize)
    {
        /*
        * Allocate the required amount of memory from the block, and
        * either allocate the whole block, or allocate a chunk of the block.
        */
        byte *       StartAllocPtr;

        if (CurBlock->Size - TotalSize < MinFreeSize) {
            /*
            * There isn't a sensible amount of memory left in the remainder of
            * the block, so allocate
            * the whole block.
            */

            /*
            * Unlink the block from the free block list, and add it to the
            * unused free node list.
            */
            LastFreeEnt = CurBlock->Prev;      /* Point at the next block to look at */
            StartAllocPtr = CurBlock->BlockPtr;
            TotalSize = CurBlock->Size;
            UnLinkNode(CurBlock);
        }
        else {
            /*
            * Only use part of the block - use the top half, so that a subsequent
            * call to realloc() is more likely to be able to expand the block.
            */
            StartAllocPtr = CurBlock->BlockPtr;

            CurBlock->BlockPtr = StartAllocPtr + TotalSize;
            CurBlock->Size -= TotalSize;
            BlockSize(CurBlock->BlockPtr) = CurBlock->Size;

            /*
            * The housekeeping information, and the
            * pointer to the housekeeping information remains in the same place, so
            * they don't need updating.  We do need to set the HeadBlockTag.
            */
            *(CurBlock->BlockPtr - HeadTagOff) = FreeBlockTag;
        }

        /*
        * Then set up the housekeeping information.
        */
        BlockSize(StartAllocPtr) = TotalSize;
        *(StartAllocPtr - HeadTagOff) = SuperAllocTag;
        *(StartAllocPtr + TotalSize - BlockExtraSize + TailTagOff) = SuperAllocTag;

        SetCheckInfo(StartAllocPtr, SizeRequired, TotalSize - BlockExtraSize);
        return (void *) StartAllocPtr;
    }



    FreeListPtr AllocMoreMemory()
    {
        /*
        * Get a new chunk of memory from the operating system, and add it onto
        * the free list.
        * return a pointer to the free list information.
        */

        byte * StartOSBlock = (byte *) OsAllocMem((size32_t)OSCHUNKSIZE);
        FreeListPtr OsNode = newFreeListEnt();
        /*
        * We get BlockExtraSize less bytes than were allocated, in order to store
        * the housekeeping information required at the end of the last block.
        */
        OsNode->BlockPtr = StartOSBlock;
        OsNode->Size = OSCHUNKSIZE;
        InsertNode(HeadOsBlockList,OsNode);
        ListOwner(StartOSBlock, OSCHUNKSIZE) = OsNode;

        FreeListPtr CurNode = newFreeListEnt();
        CurNode->BlockPtr = (StartOSBlock + BlockExtraSize);
        CurNode->Size = OSCHUNKSIZE - OSBlockExtra;
        BlockSize(CurNode->BlockPtr) = CurNode->Size;

        /*
        * Set up the special house keeping information at the start, and end
        * of the memory block.  This means we don't merge the current block with
        * anything before or after it.
        */
        *(StartOSBlock + TailTagOff) = BlockBoundTag;
        *(StartOSBlock + (OSCHUNKSIZE - OSPtrExtra) - HeadTagOff) = BlockBoundTag;

        /*
        * Tag the bounds of the new free block.
        */
        *(CurNode->BlockPtr - HeadTagOff) = FreeBlockTag;
        *(StartOSBlock + (OSCHUNKSIZE - OSBlockExtra) + TailTagOff) = FreeBlockTag;

        /*
        * Add a pointer to the free list entry at the end of the block.
        */
        ListOwner(StartOSBlock, (OSCHUNKSIZE - OSBlockExtra)) = CurNode;
        InsertFreeNode(CurNode);

        return CurNode;
    }

    void * AllocChunk(size32_t   SizeRequired)
    {
        // Allocate a chunk of memory - and return a pointer to the base.
        // Include the size of the housekeeping information...
        size32_t TotalSize = Align(SizeRequired) + BlockExtraSize;
        if (TotalSize >= MaxSuperAllocSize)
            return AllocOSChunk(SizeRequired);

        FreeListPtr  CurNode = LastFreeEnt;
        do {
            if (CurNode->Size >= TotalSize)
                return AllocBlock(CurNode, SizeRequired, TotalSize);
            CurNode = CurNode->Next;
        }
        while (CurNode != LastFreeEnt);

        CurNode = AllocMoreMemory();
        return AllocBlock(CurNode, SizeRequired, TotalSize);
    }


    void FreeOSBlock(void * ptr, size32_t size)
    {
        byte *BlockPtr = (byte *)ptr - BlockExtraSize;
        size = OSPAGEROUND(size+OSBlockExtra);

        FreeListPtr  CurNode = ListOwner(BlockPtr, size);
        ASSERTEX(CurNode->Size==size);
        UnLinkNode(CurNode);
        OsFreeMem(BlockPtr,size);
    }

    void FreeOSChunk(FreeListPtr fp)
    {
        if (OsTotal<=OsMin)
            return;
        byte *ptr = fp->BlockPtr;
        UnLinkNode(fp);
        byte *BlockPtr = (byte *)ptr - BlockExtraSize;
        FreeListPtr  CurNode = ListOwner(BlockPtr, OSCHUNKSIZE);
        ASSERTEX(CurNode->Size==OSCHUNKSIZE);
        UnLinkNode(CurNode);
        OsFreeMem(BlockPtr,OSCHUNKSIZE);
    }


    void FreeSubBlock(void * ptr, const size32_t CurBlockSize)
    {
        FreeListPtr      PrevFreeEnt;
        FreeListPtr      NextFreeEnt;
        byte *           NextBlockBase;
        size32_t         NextSize;

#if HeapChecking >= PtrCheck
        /*** Make sure a second free gets recorded as such ***/
        *((byte *)ptr - HeadTagOff) = FreeBlockTag;
        *((byte *)ptr + CurBlockSize - BlockExtraSize + TailTagOff) = FreeBlockTag;
#endif

        /*
        * Check for a possible merge on the left and right hand sides:
        */
        byte PrevTag = *((byte *)ptr - BlockExtraSize + TailTagOff);
        byte NextTag = *((byte *)ptr + CurBlockSize - HeadTagOff);

        /*
        * Merge with previous and subsequent blocks...
        */
        if (PrevTag == FreeBlockTag)
        {
            /*
            * Get the free list entry of the previous block.
            */
            byte *TempPtr = (byte *)ptr - BlockExtraSize - sizeof(FreeListPtr);
            PrevFreeEnt = *((FreeListPtr *)TempPtr);
        }

        if (NextTag == FreeBlockTag)
        {
            /* The free list entry of the next block */
            NextBlockBase = (byte *)ptr + CurBlockSize;
            NextSize = BlockSize(NextBlockBase);

            NextFreeEnt = ListOwner(NextBlockBase, NextSize - BlockExtraSize);
        }

        /*
        * Initialise the data from the free'd block.  We don't clear the whole
        * new entry (which would be best), but this is most efficient.
        */
        InitFreeBlock(ptr);

        if ((PrevTag == FreeBlockTag) && (NextTag == FreeBlockTag))
        {
            /*
            * Merge the new block into both the previous and next free list entries.
            *       1. Increase the size of the previous block.
            *       2. A a pointer at the end of the block to point at the relevant
            *          free list entry.
            *       3. Set the size recorded in the block.
            *
            * The tags at the start and end of the block are already set up correctly.
            */
            PrevFreeEnt->Size += CurBlockSize + NextFreeEnt->Size;
            ListOwner(NextBlockBase, NextSize - BlockExtraSize) = PrevFreeEnt;
            BlockSize(PrevFreeEnt->BlockPtr) = PrevFreeEnt->Size;

            /* Now unlink the second block from the free list... */
            UnLinkNode(NextFreeEnt);

            LastFreeEnt = PrevFreeEnt->Prev;   /* Point at the next block to look at */
            if (PrevFreeEnt->Size==OSCHUNKSIZE-OSBlockExtra)
                FreeOSChunk(PrevFreeEnt);
        }
        else if (PrevTag == FreeBlockTag)
        {
            /*
            * Merge with the block in front of it.
            *   1. Increase size of free block
            *   2. Point at the (new) beginning of the free block.
            *   3. Put a 'free' tag on the end.
            *   4. Change the size recorded in the block.
            */
            PrevFreeEnt->Size += CurBlockSize;
            ListOwner(ptr, CurBlockSize - BlockExtraSize) = PrevFreeEnt;

            /* Change the last tag to indicate the block is free */
            *((byte *)ptr + CurBlockSize - BlockExtraSize + TailTagOff) = FreeBlockTag;
            BlockSize(PrevFreeEnt->BlockPtr) = PrevFreeEnt->Size;

            LastFreeEnt = PrevFreeEnt->Prev;   /* Point at the next block to look at */
            if (PrevFreeEnt->Size==OSCHUNKSIZE-OSBlockExtra)
                FreeOSChunk(PrevFreeEnt);
        }
        else if (NextTag == FreeBlockTag)
        {
            /*
            * Merge with next block:
            *   1. Increase size of free block
            *   2. Point at the (new) beginning of the free block.
            *   3. Put a 'free' tag on the start.
            *   4. Change the size recorded in the block.
            *
            * (The owning free block is already set up)
            */
            NextFreeEnt->Size += CurBlockSize;
            NextFreeEnt->BlockPtr = (byte *) ptr;
            *((byte *)ptr - HeadTagOff) = FreeBlockTag;
            BlockSize(ptr) = NextFreeEnt->Size;

            LastFreeEnt = NextFreeEnt->Prev;   /* Point at the next block to look at */
            if (NextFreeEnt->Size==OSCHUNKSIZE-OSBlockExtra)
                FreeOSChunk(NextFreeEnt);

        }
        else
        {
            /*
            * The free()'d block doesn't merge with any other block => create
            * a new item on the free list.
            *
            * First, allocate a new free node to point at the new memory area.
            */
            FreeListPtr CurFreeEnt = newFreeListEnt();

            /*
            * Set up
            *    1. the fields in the free-list-entry,
            *    2. The tags at either end of the new free block.
            *    3. The pointer to the owning free-node.
            */
            CurFreeEnt->BlockPtr = (byte *)ptr;
            CurFreeEnt->Size = CurBlockSize;

            *((byte *)ptr - HeadTagOff) = FreeBlockTag;
            *((byte *)ptr + CurBlockSize - BlockExtraSize + TailTagOff) = FreeBlockTag;

            ListOwner(ptr, CurBlockSize - BlockExtraSize) = CurFreeEnt;

            InsertFreeNode(CurFreeEnt);
        }
    }


    void * ExpandOSBlock(void * ptr, size32_t NewSize)
    {
        /*
        * Expand a block that was allocated directly from the operating system.
        */

        const size32_t OldBlockSize = OSPAGEROUND(BlockSize(ptr) + OSBlockExtra);
        size32_t NewUserSize = Align(NewSize) + BlockExtraSize;
        size32_t NewTotalSize = OSPAGEROUND(NewUserSize + OSBlockExtra);
        if (NewTotalSize!=OldBlockSize)
            return NULL;

        byte * StartOSBlock = (byte *)ptr - BlockExtraSize;
        FreeListPtr BlockInfo = ListOwner(StartOSBlock, OldBlockSize);
        ASSERTEX(BlockInfo->Size==NewTotalSize);
        /*
        * We could expand the block....
        * NOTE- this means the value of ptr is still valid.
        *
        *     1. Change the block size.
        *     2. Update the trailing tags.
        *     3. Update the pointer to the owning-block
        */
        BlockSize(ptr) = NewUserSize;
        *((byte *)ptr + NewUserSize - HeadTagOff) = BlockBoundTag;
        *((byte *)ptr + (NewUserSize - BlockExtraSize) + TailTagOff) = OSAllocTag;

        ListOwner(StartOSBlock, NewTotalSize) = BlockInfo;
        SetCheckInfo((byte *)ptr, NewSize, NewUserSize - BlockExtraSize);
        return ptr;
    }



    void * ExpandSubBlock(void * ptr, size32_t NewSize)
    {
        const size32_t OldBlockSize = BlockSize(ptr);
        size32_t       TotalNewSize = Align(NewSize) + BlockExtraSize;
        size32_t       SizeDiff;
        size32_t       NextSize;
        FreeListPtr  NextNode;
        FreeListPtr  CurFreeEnt;
        byte *     StartNewBlock;

        byte * NextBlock = (byte *)ptr + OldBlockSize;

        if (TotalNewSize < OldBlockSize)
        {
            if (avoidReallocFragmentation)
                return NULL;

            SizeDiff = OldBlockSize - TotalNewSize;

            /*
            * The block of memory is being contracted - we can always do this - unless
            * we run our of free nodes.
            */
            if (*(NextBlock - HeadTagOff) == FreeBlockTag)
            {
             /*
             * Easy... just cut down the size of this block, and increase the
             * size of the next free block.
             */
                NextSize = BlockSize(NextBlock);
                NextNode = ListOwner(NextBlock, NextSize - BlockExtraSize);

             /*
             *    1. Change the size in the free list.
             *    2. Change the pointer to the block.
             *    3. Add the leading free list tag.
             *    4. Set the size in the block.
             */
                NextNode->Size += SizeDiff;

                NextBlock -= SizeDiff;
                NextNode->BlockPtr = NextBlock;
                *(NextBlock - HeadTagOff) = FreeBlockTag;
                BlockSize(NextBlock) = NextNode->Size;
            }
            else
            {
             /*
             * May need to create a new free entry - only do this if the change in
             * size is sufficient to warrant it.
             */
                if (SizeDiff >= MinFreeSize)
             {
                 CurFreeEnt = newFreeListEnt();

                 /*
                 * Set up
                 *    1. the fields in the free-list-entry,
                 *    2. The tags at either end of the new free block.
                 *    3. The size held within the block.
                 *    4. The pointer to the owning free-node.
                 */
                 StartNewBlock = (byte *)ptr + TotalNewSize;
                 CurFreeEnt->BlockPtr = StartNewBlock;
                 CurFreeEnt->Size = SizeDiff;

                 *(StartNewBlock - HeadTagOff) = FreeBlockTag;
                 *(StartNewBlock + SizeDiff - BlockExtraSize + TailTagOff) = FreeBlockTag;
                 BlockSize(StartNewBlock) = SizeDiff;

                 ListOwner(StartNewBlock, SizeDiff - BlockExtraSize) = CurFreeEnt;

                 InsertFreeNode(CurFreeEnt);
             }
                else
                    TotalNewSize = OldBlockSize;
            }
        }
        else if (TotalNewSize != OldBlockSize)
        {
            SizeDiff = TotalNewSize - OldBlockSize;

            /*
            * We can't expand this block if the next section of memory isn't free
            * or if the next free section of memory isn't big enough to give us
            * the extra space we need.
            */
            if (*(NextBlock - HeadTagOff) != FreeBlockTag)
                return NULL;

            NextSize = BlockSize(NextBlock);
            if (NextSize < SizeDiff)
                return NULL;

            if (NextSize - SizeDiff >= MinFreeSize)
            {
             /*
             * The next free block is large enough to just have a chunk taken out
             * of it.
             */
                CurFreeEnt = ListOwner(NextBlock, NextSize - BlockExtraSize);

             /*
             * First update the information in the free space list
             *       1. Size
             *       2. Base address.
             */
                CurFreeEnt->Size -= SizeDiff;
                CurFreeEnt->BlockPtr += SizeDiff;

             /*
             * Now update the free block's information
             *     1. Size
             *     2. Lower Tag.
             */
                BlockSize(CurFreeEnt->BlockPtr) = CurFreeEnt->Size;
                *(CurFreeEnt->BlockPtr - HeadTagOff) = FreeBlockTag;
            }
            else
            {
                TotalNewSize = NextSize + OldBlockSize;

             /*
             * Eat the whole of the next free space chunk...
             */
                CurFreeEnt = ListOwner(NextBlock, NextSize - BlockExtraSize);

             /*
             * Remove the entry from the freespace list...
             */
                LastFreeEnt = CurFreeEnt->Prev;
                UnLinkNode(CurFreeEnt);
            }
        }

        /*
        * Change the information stored in the allocated block:
        *     1. The block size.
        *     2. The trailing tag.
        */
        BlockSize(ptr) = TotalNewSize;
        *((byte *)ptr + TotalNewSize - BlockExtraSize + TailTagOff) = SuperAllocTag;
        SetCheckInfo((byte *)ptr, NewSize, TotalNewSize - BlockExtraSize);

        return ptr;
    }


public:

    IMPLEMENT_IINTERFACE;

    CSuperAllocator(memsize_t maxtotal,unsigned _maxsubrecsize, memsize_t mintotal,bool _avoidReallocFragmentation)
        : FreeListAllocator(sizeof(FreeListEnt),0x10000)
    {
        ASSERTEX ((BlockExtraSize == 8) || (HeapChecking >= PtrCheck));
        HeadFreeList.BlockPtr = NULL;
        HeadFreeList.Next = &HeadFreeList;
        HeadFreeList.Prev = &HeadFreeList;
        HeadFreeList.Size = 0;
        LastFreeEnt    = &HeadFreeList;
        HeadOsBlockList.BlockPtr = NULL;
        HeadOsBlockList.Next = &HeadOsBlockList;
        HeadOsBlockList.Prev = &HeadOsBlockList;
        HeadOsBlockList.Size = 0;
        OsTotal = 0;
        OsMax = maxtotal ? maxtotal : ((memsize_t)-1); // unbound if not set
        OsMin = mintotal;
        if (_maxsubrecsize)
            suballocator.setown(new CSubAllocator(*this,_maxsubrecsize));
        avoidReallocFragmentation = _avoidReallocFragmentation;
    }

    ~CSuperAllocator()
    {
        suballocator.clear();
        while (HeadOsBlockList.Next!=&HeadOsBlockList) {
            OsFreeMem(HeadOsBlockList.Next->BlockPtr,HeadOsBlockList.Next->Size);
            UnLinkNode(HeadOsBlockList.Next);
        }
    }

    size32_t roundupSize(size32_t sz)
    {
        size32_t ret;
        if (suballocator) {
            ret = suballocator->roundupSize(sz);
            if (ret)
                return ret;
        }
        return Align(sz);
    }

    void *allocMem2(size32_t size,size32_t &usablesize)
    {
        if (size == 0)
            return NULL;
        if (size>MAXUSERALLOCATESIZE) // too big!
            HeapError(e_alloc_too_big);
        void *ptr;
        if (suballocator) {
            ptr = suballocator->allocMem(size,usablesize);
            if (ptr) {
                ASSERTEX(roundupSize(size)<=usablesize);
                return ptr;
            }
        }
        SpinBlock block(lock);
        ptr = AllocChunk(size);
#if InitialiseMemory
        memset(ptr, MallocFillChar, size);
#endif
        usablesize =  BlockSize(ptr)-BlockExtraSize;
        ASSERTEX(roundupSize(size)<=usablesize);
        return ptr;
    }

    void *allocMem(size32_t size)
    {
        size32_t usablesize;
        return allocMem2(size,usablesize);
    }

    void freeMem(void *ptr)
    {
        if (!ptr)
            return;


#if HeapChecking >= PtrCheck
        checkPtrValid(ptr);
#endif

        size32_t  CurBlockSize   = BlockSize(ptr);
        if (!ISSUBALLOCATED(CurBlockSize)) {
            SpinBlock block(lock);
            if (*((byte *)ptr - HeadTagOff) == OSAllocTag)
                FreeOSBlock(ptr,CurBlockSize);
            else {
#if InitialiseMemory
                memset(ptr, FreeFillChar, CurBlockSize-BlockExtraSize);
#endif
                FreeSubBlock(ptr,CurBlockSize);
            }
        }
        else if (suballocator)
            suballocator->freeMem(ptr);
        else
            HeapError(e_inconsist_block_tag,ptr);
    }

    void * reallocMem2 (void * ptr, size32_t NewSize,size32_t &usablesize)
    {
        if (ptr == NULL)
            return allocMem2(NewSize,usablesize);

        if (NewSize == 0) {
            freeMem(ptr);
            usablesize = 0;
            return NULL;
        }

        if (NewSize>MAXUSERALLOCATESIZE) // too big!
            HeapError(e_alloc_too_big);

#if HeapChecking >= PtrCheck
        checkPtrValid(ptr);
#endif

        /*
        * Check for an allocation request that exceeds the amount we can
        * allocate in one go.
        */

        size32_t  CurBlockSize = BlockSize(ptr);
        void * NewBlock=NULL;
        if (!ISSUBALLOCATED(CurBlockSize)) {
            if (suballocator) // want to use suballocator if can
                NewBlock = suballocator->allocMem(NewSize,usablesize);
            if (!NewBlock) {
                {
                    SpinBlock block(lock);
                    if (*((byte *)ptr - HeadTagOff) == OSAllocTag)
                        NewBlock = ExpandOSBlock(ptr, NewSize);
                    else
                        NewBlock = ExpandSubBlock(ptr, NewSize);
                    if (NewBlock) {
                        usablesize =  BlockSize(ptr)-BlockExtraSize;
                        return NewBlock;
                    }
                }
                NewBlock = allocMem2(NewSize,usablesize);
            }
            memcpy(NewBlock,ptr,(NewSize<CurBlockSize-BlockExtraSize)?NewSize:(CurBlockSize-BlockExtraSize));
            freeMem(ptr);
        }
        else if (suballocator) {
            NewBlock = suballocator->reallocMem(ptr,NewSize,usablesize); // returns NULL if can't do
            if (NewBlock) {
                ASSERTEX(roundupSize(NewSize)<=usablesize);
                return NewBlock;
            }
            NewBlock = allocMem2(NewSize,usablesize);
            size32_t oldsize = suballocator->usableSize(ptr);
            memcpy(NewBlock,ptr,(NewSize<oldsize)?NewSize:oldsize);
            freeMem(ptr);
        }
        else
            HeapError(e_inconsist_block_tag,ptr);

        ASSERTEX(roundupSize(NewSize)<=usablesize);



        return NewBlock;
    }

    void * reallocMem (void * ptr, size32_t NewSize)
    {
        size32_t usablesize;
        return reallocMem2(ptr,NewSize,usablesize);
    }

    size32_t usableSize(const void * ptr)
    {
        if (!ptr)
            return 0;
        // SpinBlock block(lock); // lock shouldn't be needed
        size32_t  CurBlockSize   = BlockSize(ptr);
        if (!ISSUBALLOCATED(CurBlockSize))
            return CurBlockSize-BlockExtraSize;
        if (suballocator)
            return suballocator->usableSize(ptr);
        HeapError(e_inconsist_block_tag,ptr);
        return 0;
    }


    memsize_t totalAllocated()
    {
        SpinBlock block(lock);
        return OsTotal;
    }

    memsize_t totalMax()
    {
        SpinBlock block(lock);
        return OsMax;
    }

    memsize_t totalRemaining()
    {
        SpinBlock block(lock);
        if (OsTotal<OsMax) // JCS, don't see how OsTotal could be > OsMax, but code elsewhere implied it could
            return OsMax-OsTotal;
        return 0;
    }

    void walkBlock(IWalkMem &walker,const byte * BlockPtr, size32_t OSBlockSize)
    {
        if (!OSBlockSize)
            return;
        byte BlockHeadTag = *(BlockPtr - HeadTagOff);
        do {
            size32_t CurBlockSize = BlockSize(BlockPtr);
            byte HeadTag = *(BlockPtr - HeadTagOff);
            if (HeadTag == SuperAllocTag) {
                if (!suballocator.get()||!suballocator->isBlock(BlockPtr))
                    walker.inuse(BlockPtr,CurBlockSize);
            }
            else if (HeadTag == OSAllocTag) {
                walker.inuse(BlockPtr,CurBlockSize);
            }
            BlockPtr += CurBlockSize;
            OSBlockSize -= CurBlockSize;
        } while ((BlockHeadTag != OSAllocTag) && (*(BlockPtr - HeadTagOff) != BlockBoundTag));
    }

    void walk(IWalkMem &walker)
    {
        if (suballocator.get())
            suballocator->walk(walker);
        FreeListEnt  *CurOsBlock = HeadOsBlockList.Next;
        while (CurOsBlock!=&HeadOsBlockList) {
            walker.osblock(CurOsBlock->BlockPtr,CurOsBlock->Size);
            walkBlock(walker,(const byte *)CurOsBlock->BlockPtr+BlockExtraSize,CurOsBlock->Size);
            CurOsBlock = CurOsBlock->Next;
        }
    }

    void logMemLeaks(bool logdata)
    {
        struct cwalker: public IWalkMem
        {
            memsize_t tot;
            unsigned num;
            memsize_t ostot;
            unsigned osnum;
            bool logdata;
            cwalker(bool _logdata)
            {
                tot = 0;
                num = 0;
                ostot = 0;
                osnum = 0;
                logdata = _logdata;
            }
            void inuse(const void *mem,size32_t sz)
            {
                num++;
                if (logdata) {
                    StringBuffer line;
                    for (unsigned i=0;(i<sz)&&(i<16);i++) {
                        if (i)
                            line.append(", ");
                        line.appendf("%02x",(unsigned)(*((byte *)mem+i)));
                    }
                    PROGLOG("LEAK(%d,%u,%p) : %s",num,sz,mem,line.str());
                }
                tot += sz;
            }
            void osblock(const void *mem,size32_t sz)
            {
                osnum++;
                if (logdata)
                    PROGLOG("OSBLOCK(%d,%u,%p)",osnum,sz,mem);
                ostot += sz;
            }
        } walker(logdata);
        walk(walker);
        if (walker.num)
            PROGLOG("JMALLOC LEAKCHECKING: %d leaks, total memory %" I64F "d",walker.num,(__int64)walker.tot);
        PROGLOG("JMALLOC OSBLOCKS: %d, total memory %" I64F "d",walker.osnum,(__int64)walker.ostot);
    }

};

CSubAllocator::~CSubAllocator()
{
    for (unsigned i=0;i<nblks;i++) {
        CSubAllocatorBlock *head = (CSubAllocatorBlock *)&blks[i];
        CSubAllocatorBlock *p = head->next;
        while (p!=head) {
            CSubAllocatorBlock *n = p->next;
            superallocator.freeMem(p);
            p = n;
        }
    }
    delete [] blks;
}

void CSubAllocator::freeMem(void * p)
{
    NonReentrantSpinBlock block(sublock);
    ASSERTEX(p);
    CSubAllocatorBlock *b = CSubAllocatorBlock::base(p);
    bool wasfull = b->full();
    if (b->subfree(p)) { // empty so free block from chain
        b->unlink();
        superallocator.freeMem(b);
    }
    else if (wasfull)
    {
        unsigned i;
        CSubAllocatorBlock * f = item(b->recsize,i);
        if (!f)
        {
#ifdef _DEBUG
            HeapError(e_corrupt_sub_block,p);
#endif
        }
        b->unlink();
        blks[i].prepend(*b);
    }
}

void *CSubAllocator::getMem(size32_t recsz,unsigned &n)
{
    ASSERTEX(recsz<=maxrecsize);
    n = (SBPAGESIZE-SBHEADERSIZE)/(recsz+SBOVERHEAD);
    ASSERTEX(n>=15);
    size32_t wanted = SBHEADERSIZE+(recsz+SBOVERHEAD)*n;
    ASSERTEX(wanted<=SBPAGESIZE);
    void *ret = superallocator.allocMem(wanted);
    n = (superallocator.usableSize(ret)-SBHEADERSIZE)/(recsz+SBOVERHEAD);
    return ret;
}


IAllocator *createMemoryAllocator(memsize_t maxtotal, unsigned suballoc_htabsize, memsize_t mintotal,bool avoidReallocFragmentation)
{
    return new CSuperAllocator(maxtotal,suballoc_htabsize,mintotal,avoidReallocFragmentation);

}


class CGuardedSuperAllocator: public CInterface, implements IAllocator
{
    CSuperAllocator allocator;

    size32_t guardsize;
    CriticalSection crit;
    __int64 numallocs;

    unsigned guarderror(const void *ptr,size32_t sz,unsigned n)
    {
        const byte * p = (const byte *)ptr;
        p += sizeof(size32_t)+sz;
        size32_t szn = *(const size32_t *)p;
        ERRLOG("CGuardedSuperAllocator guard error(%d) %p len %u %d",n,ptr,sz,(int)szn);
        PrintStackReport();
#ifdef _WIN32
        DebugBreak();
#endif
        throw MakeStringException(-1,"CGuardedSuperAllocator guard error(%d) %p len %u",n,ptr,sz);
    }

    void *fillguard(void *_p,size32_t sz )
    {
        // format sz,data,-sz,e0,e1,...
        byte * p = (byte *)_p;
        *(size32_t *)p = sz;
        p += sizeof(size32_t);
        void * ret = p;
        p += sz;
        *(size32_t *)p = 0-sz;
        p += sizeof(size32_t);
        unsigned rest = guardsize-sizeof(size32_t)*2;
        byte b = 0xe0;
        while (rest--)
            *(p++) = b++;
        return ret;
    }

    void checkguard(const void *_p,size32_t &sz)
    {
        const byte * p = (const byte *)_p;
        sz = *(const size32_t *)p;
        p += sizeof(size32_t);
        p += sz;
        size32_t szn = *(const size32_t *)p;
        if (szn!=0-sz)
            guarderror(_p,sz,1);
        p += sizeof(size32_t);
        unsigned rest = guardsize-sizeof(size32_t)*2;
        byte b = 0xe0;
        while (rest--)
            if (*(p++) != b++)
                guarderror(_p,sz,2);
    }



public:
    IMPLEMENT_IINTERFACE;

    CGuardedSuperAllocator(memsize_t maxtotal, unsigned suballoc_htabsize, memsize_t mintotal,bool avoidReallocFragmentation, size32_t _guardsize)
        : allocator(maxtotal,suballoc_htabsize,mintotal,avoidReallocFragmentation)
    {
        guardsize = (_guardsize<sizeof(size32_t)*2)?(sizeof(size32_t)*2):_guardsize;
        numallocs = 0;
    }

    virtual void * allocMem(size32_t sz)
    {
        if (!sz)
            return NULL;
        void *ret = allocator.allocMem(sz+guardsize);
        ret = fillguard(ret,sz);
        CriticalBlock block(crit);
        numallocs++;
        return ret;

    }
    virtual void   freeMem(void *_ptr)
    {
        if (_ptr) {
            size32_t sz;
            void *ptr = (byte *)_ptr-sizeof(size32_t);
            checkguard(ptr,sz);
            memset(ptr,0xff,sz+guardsize);
            allocator.freeMem(ptr);
            CriticalBlock block(crit);
            numallocs--;
        }
    }
    virtual void * reallocMem(void *_prev,size32_t sz)
    {
        size32_t oldsz;
        checkguard((byte *)_prev-sizeof(size32_t),oldsz);
        if (oldsz==sz)
            return _prev;
        void *ret = allocMem(sz);
        memcpy(ret,_prev,oldsz<sz?oldsz:sz);
        freeMem(_prev);
        return ret;
    }

    virtual size32_t usableSize(const void *ptr)
    {
        if (!ptr)
            return 0;
        size32_t ret;
        checkguard((byte *)ptr-sizeof(size32_t),ret);
        return ret;
    }
    virtual memsize_t totalAllocated()
    {
        return allocator.totalAllocated();
    }
    virtual memsize_t totalMax()
    {
        return allocator.totalMax();
    }
    virtual memsize_t totalRemaining()
    {
        return allocator.totalRemaining();
    }
    virtual void checkPtrValid(const void * ptr)
    {
        size32_t sz;
        checkguard((byte *)ptr-sizeof(size32_t),sz);
    }
    virtual void logMemLeaks(bool logdata)
    {
        //allocator.logMemLeaks(logdata);
    }
    virtual void * allocMem2(size32_t sz, size32_t &usablesz)
    {
        usablesz = sz;
        return allocMem(sz);
    }

    virtual void * reallocMem2(void *prev,size32_t sz, size32_t &usablesz)
    {
        usablesz = sz;
        return reallocMem(prev,sz);
    }

    virtual size32_t roundupSize(size32_t sz)
    {
         return sz;
    }
};

IAllocator *createGuardedMemoryAllocator(memsize_t maxtotal, unsigned suballoc_htabsize, memsize_t mintotal,bool avoidReallocFragmentation, size32_t guardsize)
{
    return new CGuardedSuperAllocator(maxtotal,suballoc_htabsize,mintotal,avoidReallocFragmentation,guardsize);

}


void fillPtr(IAllocator *a,void *p,size32_t sz)
{
    size32_t us = a->usableSize(p);
    assertex(us>=sz);
    memset(p,sz^0xc7,us);
}

void testFillPtr(IAllocator *a,void *p,size32_t sz)
{
    size32_t us = a->usableSize(p);
    byte *b = (byte *)p;
    assertex(us>=sz);
    assertex(us<sz+OSPAGESIZE);
    byte t = sz^0xc7;
    while (us--)
        assertex(b[us]==t);
}

void testAllocator()
{
    printf("HeadTagOff=%x\n",(unsigned) HeadTagOff);
    printf("TailTagOff=%x\n",(unsigned) TailTagOff);
    printf("HeadChkOff=%x\n",(unsigned) HeadChkOff);
    printf("TailChkOff=%x\n",(unsigned) TailChkOff);
    printf("HeadExtraBytes=%x\n",(unsigned) HeadExtraBytes);
    printf("TailExtraBytes=%x\n",(unsigned) TailExtraBytes);
    printf("BlockExtraSize=%x\n",(unsigned) BlockExtraSize);
    printf("OSBlockExtra=%x\n",(unsigned) OSBlockExtra);
    printf("OSPtrExtra=%x\n",(unsigned) OSPtrExtra);
    printf("SBHEADERSIZE=%x\n",(unsigned) SBHEADERSIZE);
    printf("MINSUBBLOCKSIZE=%x\n",(unsigned) MINSUBBLOCKSIZE);

    Owned<IAllocator> a;
    a.setown(createMemoryAllocator(0x100000*100));

    byte *ptrs[8192];
    unsigned i;
    unsigned j;
    size32_t us;
    for (i=0;i<8192;i++) {
        ptrs[i] = (byte *)a->allocMem(i);
        us = a->usableSize(ptrs[i]);
        assertex(us>=i);
        memset(ptrs[i],(byte)i,us);
    }
    for (i=0;i<8192;i++) {
        a->checkPtrValid(ptrs[i]);
        us = a->usableSize(ptrs[i]);
        assertex(us>=i);
        for (j=0;j<us;j++)
            assertex(ptrs[i][j]==(byte)i);
    }
    for (i=0;i<8192;i++) {
        for (j=0;j<i;j++)
            assertex(ptrs[i][j]==(byte)i);
        memset(ptrs[i],255-(byte)i,i);
        a->freeMem(ptrs[i]);
    }
    //assertex(a->totalAllocated()==0);
    memsize_t fibs[100];
    seedRandom(1234);
    unsigned f = 2;
    fibs[0] = 1;
    fibs[1] = 1;
    for (f=2;f<100;f++) {
        unsigned nf = fibs[f-1]+fibs[f-2];
        if (nf>0x100000*10)
            break;
        fibs[f] = nf;
    }
    size32_t sizes[8192];
    memset(sizes,0,sizeof(sizes));
    size32_t total = 0;
    size32_t max = 0x100000*50;
    for (unsigned iter=0;iter<10000000;iter++) {
        unsigned r = getRandom();
        i = r%8192;
        r /= 8192;
        size32_t sz = sizes[i];
        if (sz) {
            if (r%8==0) {
                r/=8;
                size32_t nsz = fibs[r%f];
                r/=f;
                if (r%16==0)
                    nsz = getRandom()%nsz+1;
                if (total+nsz-sz>max)
                    continue;
                testFillPtr(a,ptrs[i],sz);
                ptrs[i] = (byte *)a->reallocMem(ptrs[i],nsz);
                fillPtr(a,ptrs[i],nsz);
                a->checkPtrValid(ptrs[i]);
                sizes[i] = nsz;
                total += nsz-sz;
            }
            else {
                testFillPtr(a,ptrs[i],sz);
                a->freeMem(ptrs[i]);
                sizes[i] = 0;
                assertex(total>=sz);
                total -= sz;
            }
        }
        else {
            sz = fibs[r%f];
            r/=f;
            if (r%16==0)
                sz = getRandom()%sz+1;
            if (total+sz>max)
                continue;
            ptrs[i] = (byte *)a->allocMem(sz);
            fillPtr(a,ptrs[i],sz);
            a->checkPtrValid(ptrs[i]);
            sizes[i] = sz;
            total += sz;
        }
        if (iter%10000==0)
            printf("iter = %d, total=%d, allocated=%d\n",iter,total, (unsigned) a->totalAllocated());
    }
    for (i=0;i<8192;i++)
        if (sizes[i])
            a->freeMem(ptrs[i]);
    //assertex(a->totalAllocated()==0);

    a.clear();
}
