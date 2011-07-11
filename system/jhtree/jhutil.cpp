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
