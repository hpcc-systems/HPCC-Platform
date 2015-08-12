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
#include "jlib.hpp"
#include "rtltype.hpp"

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

