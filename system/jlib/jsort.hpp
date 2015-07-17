/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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



#ifndef JSORT_HPP
#define JSORT_HPP

#include "jiface.hpp"
#include "jio.hpp"

#ifndef ICOMPARE_DEFINED
#define ICOMPARE_DEFINED
struct ICompare
{
    virtual int docompare(const void *,const void *) const =0;
protected:
    virtual ~ICompare() {}
};
#endif

// useful binary insertion function used by array functions.

typedef int (*sortCompareFunction)(const void * left, const void * right);

extern jlib_decl void * binary_add(const void *newitem, const void *base,
             size32_t nmemb, 
             size32_t width,
             sortCompareFunction compare,
             bool * ItemAdded);

extern jlib_decl void * binary_vec_find(const void *search, const void * *base,
                                        size32_t nmemb, 
                                        sortCompareFunction compare,
                                        bool * isNew);

extern jlib_decl void * binary_vec_find(const void *search, const void * *base,
                                        size32_t nmemb, 
                                        ICompare & compare,
                                        bool * isNew);

extern jlib_decl void * binary_vec_insert(const void *newitem, const void * *base,
                                          size32_t nmemb, 
                                          sortCompareFunction compare);

extern jlib_decl void * binary_vec_insert(const void *newitem, const void * *base,
                                          size32_t nmemb, 
                                          ICompare const & compare);

extern jlib_decl void * binary_vec_insert_stable(const void *newitem, const void * *base,
                                          size32_t nmemb, 
                                          sortCompareFunction compare);

extern jlib_decl void * binary_vec_insert_stable(const void *newitem, const void * *base,
                                          size32_t nmemb, 
                                          ICompare const & compare);

extern jlib_decl void qsortvec(void **a, size32_t n, size32_t es);
extern jlib_decl void qsortvec(void **a, size32_t n, sortCompareFunction compare);
extern jlib_decl void qsortvec(void **a, size32_t n, const ICompare & compare);
extern jlib_decl void qsortvec(void **a, size32_t n, const ICompare & compare1, const ICompare & compare2);

// Call with n rows of data in rows, index an (uninitialized) array of size n. The function will fill index with a stably sorted index into rows.
extern jlib_decl void qsortvecstableinplace(void ** rows, size32_t n, const ICompare & compare, void ** temp);
extern jlib_decl void msortvecstableinplace(void ** rows, size32_t n, const ICompare & compare, void ** temp);
extern jlib_decl void parmsortvecstableinplace(void ** rows, size32_t n, const ICompare & compare, void ** temp, unsigned ncpus=0);


extern jlib_decl void parqsortvec(void **a, size32_t n, const ICompare & compare, unsigned ncpus=0); // runs in parallel on multi-core
extern jlib_decl void parqsortvecstableinplace(void ** rows, size32_t n, const ICompare & compare, void ** temp, unsigned ncpus=0); // runs in parallel on multi-core


// we define the heap property that no element c should be smaller than its parent (unsigned)(c-1)/2
// heap stores indexes into the data in rows, so compare->docompare is called with arguments rows[heap[i]]
// these functions are stable

// assuming that all elements >p form a heap, this function sifts p down to its correct position, and so includes it in the heap; it returns true if no change is made
extern jlib_decl bool heap_push_down(unsigned p, unsigned num, unsigned * heap, const void ** rows, ICompare * compare);
// assuming that all elements <c form a heap, this function pushes c up to its correct position; it returns true if no change is made
extern jlib_decl bool heap_push_up(unsigned c, unsigned * heap, const void ** rows, ICompare * compare);



inline void parsortvecstableinplace(void ** rows, size32_t n, const ICompare & compare, void ** stableTablePtr, unsigned maxCores=0)
{
#ifdef _USE_TBB
    parmsortvecstableinplace(rows, n, compare, stableTablePtr, maxCores);
#else
    parqsortvecstableinplace(rows, n, compare, stableTablePtr, maxCores);
#endif
}



extern jlib_decl IRowStream *createRowStreamMerger(unsigned numstreams,IRowProvider &provider,ICompare *icmp, bool partdedup=false);
extern jlib_decl IRowStream *createRowStreamMerger(unsigned numstreams,IRowStream **instreams,ICompare *icmp, bool partdedup, IRowLinkCounter *linkcounter);


class ISortedRowProvider
{
public:
    virtual void * getNextSorted() = 0;
};

#define MIN(a, b)  ((a) < (b) ? a : b)

typedef void (*sortSwapFunction)(void * left, void * right);

// see jsorta.hpp for array sorting routines


#endif
