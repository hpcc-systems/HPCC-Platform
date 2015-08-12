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



#ifndef JSORTARR_HPP
#define JSORTARR_HPP


#include "jiface.hpp"
#include "jsort.hpp"



/* the following somewhat inelegantly implements:

template <class C> 
void qsortarray(C *a, size32_t n, sortCompareFunction compare, sortSwapFunction doswap)

template <class C> 
void qsortarray(C *a, size32_t n, sortCompareFunction compare)

template <class C> 
void qsortarray(C *a, size32_t n, const ICompare &compare, sortSwapFunction doswap)

template <class C> 
void qsortarray(C *a, size32_t n, const ICompare &compare)

template <class C> 
void qsortarray(C *a, size32_t n)


*/


#define CMP(a,b)         (compare((a),(b)))
#define MED3(a,b,c)      ((C *)med3ca(a,b,c,compare))
#define RECURSE(a,b)     qsortarray(a, b, compare, doswap)
static inline void *med3ca(void *a, void *b, void *c, sortCompareFunction compare)
{ return CMP(a, b) < 0 ? (CMP(b, c) < 0 ? b : (CMP(a, c) < 0 ? c : a )) : (CMP(b, c) > 0 ? b : (CMP(a, c) < 0 ? a : c )); }
#define VECTOR C*
#define SWAP(a,b)        doswap(a,b)
template <class C> 
void qsortarray(C *a, size32_t n, sortCompareFunction compare, sortSwapFunction doswap)
#include "jsort2.inc"
#undef SWAP
#define SWAP(a,b)        { C t = *(a); *(a) = *(b); *(b) = t; }
#undef RECURSE
#define RECURSE(a,b)     qsortarray(a, b, compare)
template <class C> 
void qsortarray(C *a, size32_t n, sortCompareFunction compare)
#include "jsort2.inc"
#undef SWAP
#define SWAP(a,b)        doswap(a,b)
#undef CMP
#define CMP(a,b)         (compare.docompare((a),(b)))
#undef RECURSE
#define RECURSE(a,b)     qsortarray(a, b, compare, doswap)
#undef MED3
static inline void *med3cac(void *a, void *b, void *c, const ICompare &compare)
{ return CMP(a, b) < 0 ? (CMP(b, c) < 0 ? b : (CMP(a, c) < 0 ? c : a )) : (CMP(b, c) > 0 ? b : (CMP(a, c) < 0 ? a : c )); }
#define MED3(a,b,c)      ((C*)med3cac(a,b,c,compare))
template <class C> 
void qsortarray(C *a, size32_t n, const ICompare &compare, sortSwapFunction doswap)
#include "jsort2.inc"
#undef SWAP
#define SWAP(a,b)        { C t = *(a); *(a) = *(b); *(b) = t; }
#undef RECURSE
#define RECURSE(a,b)     qsortarray(a, b, compare)
template <class C> 
void qsortarray(C *a, size32_t n, const ICompare &compare)
#include "jsort2.inc"
#undef CMP
#define CMP(a,b)         ((*(a)<*(b))?-1:((*(a)>*(b))?1:0))
#undef RECURSE
#define RECURSE(a,b)     qsortarray(a, b)
#undef MED3
template <class C> 
static inline C *med3cas(C *a, C *b, C *c)
{ return *a<*b ? (*b<*c ? b : (*a<*c ? c : a )) : (*b>*c ? b : (*a<*c ? a : c )); }
#define MED3(a,b,c)      med3cas<C>(a,b,c)
template <class C> 
void qsortarray(C *a, size32_t n)
#include "jsort2.inc"
#undef CMP
#undef MED3
#undef RECURSE
#undef SWAP
#undef VECTOR
#endif

