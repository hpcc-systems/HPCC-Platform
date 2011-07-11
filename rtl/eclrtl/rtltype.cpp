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
#include "jlib.hpp"
#include "rtltype.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/rtl/eclrtl/rtltype.cpp $ $Id: rtltype.cpp 62376 2011-02-04 21:59:58Z sort $");

//=============================================================================
// Pascal string helper functions...

typedef unsigned char pascalLengthField;
unsigned rtlGetPascalLength(unsigned len, const void * data)
{
    return sizeof(pascalLengthField) + *(pascalLengthField *)data;
}

void rtlPascalToString(unsigned & tgtLen, char * & tgt, unsigned, const void * src)
{
    unsigned len = *(pascalLengthField *)src;
    char * buff = (char *)malloc(len);
    memcpy(buff, (const char *)src+sizeof(pascalLengthField), len);

    tgtLen = len;
    tgt = buff;
}

void rtlStringToPascal(unsigned & tgtLen, void * & tgt, unsigned srcLen, const char * src)
{
    if (srcLen > (pascalLengthField)-1)
        srcLen = (pascalLengthField)-1;

    unsigned size = srcLen + sizeof(pascalLengthField);
    char * buff = (char *)malloc(size);
    *(pascalLengthField *)buff = (pascalLengthField)srcLen;
    memcpy(buff + sizeof(pascalLengthField), src, srcLen);

    tgtLen = size;
    tgt = buff;
}

//=============================================================================


void rtlPadTruncString(unsigned & tgtLen, char * & tgt, unsigned newLen, unsigned len, const char * src)
{
    if (len > newLen)
        len = newLen;
    char * buff = (char *)malloc(len);
    memcpy(buff, src, len);
    if (len < newLen)
        memset(buff+len, ' ', newLen - len);

    tgtLen = newLen;
    tgt = buff;
}

//=============================================================================

__int64 rtlBcdToInteger(unsigned len, char * src)
{
    unsigned char * cur = (unsigned char *)src;
    __int64 value = 0;
    while (len--)
    {
        unsigned next = *cur++;
        value = value * 100 + (next >> 4) * 10 + (next & 0xf);
    }
    return value;
}

void rtlIntegerToBcd(unsigned & tgtLen, char * & tgt, unsigned digits, __int64 value)
{
}

void rtlIntegerToBcdFixed(char * tgt, unsigned digits, __int64 value)
{
}

//=============================================================================

