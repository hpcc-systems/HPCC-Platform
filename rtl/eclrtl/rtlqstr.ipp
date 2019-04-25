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

#ifndef rtlqstr_ipp
#define rtlqstr_ipp

void copyQStrRange(unsigned tlen, char * tgt, const char * src, unsigned from, unsigned to);
//The following function works if matching a substring of a qstring, currently the default function does not
int rtlSafeCompareQStrQStr(size32_t llen, const void * left, size32_t rlen, const void * right);
bool incrementQString(byte *buf, size32_t size);
bool decrementQString(byte *buf, size32_t size);

//Utility functions for getting and setting an individual character within a qstring.
byte getQChar(const byte * buffer, size32_t index);
void setQChar(byte * buffer, size32_t index, byte value);

#endif
