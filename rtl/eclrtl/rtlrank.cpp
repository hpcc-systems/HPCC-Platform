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
#include <math.h>
#include <stdio.h>
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jsort.hpp"
#include "jlib.hpp"
#include "eclrtl.hpp"

//=============================================================================
// Miscellaneous string functions...

void rtlCreateOrder(void * tgt, const void * src, unsigned num, unsigned width, const void * compare)
{
    //create an array of pointers to the objects...
    void * * vector = (void * *)malloc(num * sizeof(void *));
    unsigned idx;
    char * finger = (char *)src;
    for (idx = 0; idx < num; idx++)
    {
        vector[idx] = finger;
        finger += width;
    }

    //sort the elements.
    qsortvec(vector, num, (sortCompareFunction)compare);

    //now calculate the indices...
    unsigned * indices = (unsigned *)tgt;
    for (idx = 0; idx < num; idx++)
        indices[idx] = ((char *)vector[idx] - (char *)src) / width + 1;         // NB: 1 based...
    free(vector);
}

unsigned rtlRankFromOrder(unsigned index, unsigned num, const void * order) 
{
    const unsigned * indices = (const unsigned *)order;
    unsigned idx;
    for (idx =0; idx < num; idx++)
        if (indices[idx] == index)
            return idx+1;
    return 0;
}

unsigned rtlRankedFromOrder(unsigned index, unsigned num, const void * order) 
{
    const unsigned * indices = (const unsigned *)order;
    if ((index < 1) || (index > num))
        return 0;
    return indices[index-1];
}

