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
#include <string.h>
#include <stdio.h>
#include <stdlib.h>


#include "jhutil.hpp"

#if 0
// CSwapper impl.

void CSwapper::append(void *fldAddr, int size)
{
    assert((long)fldAddr>=(long)ptr && (long)fldAddr<(long)ptr+ptrsize);
    assert(size>=1 && size<=8);

    long offset = (long)fldAddr-(long)ptr;
    if (size > 1)
    {
        int j;
        for (j = 0; j < size/2; j++)
        {
            pairs.append(offset+j);
            pairs.append(offset+size-j-1);
        }
    }
}

void CSwapper::swap(void *ptr, int size)
{
    char *targ = (char *) ptr;

    for (int j = 0; j < size/2; j++)
    {
        char t = targ[j];
        targ[j] = targ[size-j-1];
        targ[size-j-1] = t;
    }
}

void CSwapper::swap()
{
    char *targ = (char *) ptr;
    aindex_t swaplen = pairs.length();
    for (aindex_t i = 0; i < swaplen; )
    {
        int o1 = pairs.item(i++);
        int o2 = pairs.item(i++);
        char t = targ[o1];
        targ[o1] = targ[o2];
        targ[o2] = t;
    }
}

#endif
