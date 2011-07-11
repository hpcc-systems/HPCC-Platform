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



#ifndef JSORT_HPP
#define JSORT_HPP

#include "jexpdef.hpp"
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
extern jlib_decl void qsortvecstable(void ** const rows, size32_t n, const ICompare & compare, void *** index);


extern jlib_decl void parqsortvec(void **a, size32_t n, const ICompare & compare, unsigned ncpus=0); // runs in parallel on multi-core
extern jlib_decl void parqsortvec(void **a, size32_t n, const ICompare & compare1, const ICompare & compare2,unsigned ncpus=0);
extern jlib_decl void parqsortvecstable(void ** const rows, size32_t n, const ICompare & compare, void *** index, unsigned ncpus=0); // runs in parallel on multi-core


// we define the heap property that no element c should be smaller than its parent (unsigned)(c-1)/2
// heap stores indexes into the data in rows, so compare->docompare is called with arguments rows[heap[i]]
// these functions are stable

// assuming that all elements >p form a heap, this function sifts p down to its correct position, and so includes it in the heap; it returns true if no change is made
extern jlib_decl bool heap_push_down(unsigned p, unsigned num, unsigned * heap, const void ** rows, ICompare * compare);
// assuming that all elements <c form a heap, this function pushes c up to its correct position; it returns true if no change is made
extern jlib_decl bool heap_push_up(unsigned c, unsigned * heap, const void ** rows, ICompare * compare);







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
