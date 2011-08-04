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



#ifndef JSORTARR_HPP
#define JSORTARR_HPP


#include "jexpdef.hpp"
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

