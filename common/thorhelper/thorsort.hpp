/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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



#ifndef THORSORT_HPP
#define THORSORT_HPP

#include "thorhelper.hpp"
#include "jsort.hpp"

//#define DEFAULT_MERGE_SORT

extern THORHELPER_API void msortvecstableinplace(void ** rows, size_t n, const ICompare & compare, void ** temp);
extern THORHELPER_API void parmsortvecstableinplace(void ** rows, size_t n, const ICompare & compare, void ** temp);

inline void parsortvecstableinplace(void ** rows, size_t n, const ICompare & compare, void ** stableTablePtr, unsigned maxCores=0)
{
#ifdef DEFAULT_MERGE_SORT
    parmsortvecstableinplace(rows, n, compare, stableTablePtr);
#else
    parqsortvecstableinplace(rows, n, compare, stableTablePtr, maxCores);
#endif
}

extern THORHELPER_API void tbbqsortvec(void **a, size_t n, const ICompare & compare);
extern THORHELPER_API void tbbqsortstable(void ** rows, size_t n, const ICompare & compare, void ** temp);

#endif
