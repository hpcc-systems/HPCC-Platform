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
#include <math.h>
#include <stdio.h>
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jsort.hpp"
#include "jlib.hpp"
#include "eclrtl.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/rtl/eclrtl/rtlrank.cpp $ $Id: rtlrank.cpp 62376 2011-02-04 21:59:58Z sort $");

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

