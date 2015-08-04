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

unicode unicodeStrip(unicode original, unicode search) := BEGINC++
    size32_t start;
    for (start = 0; start < lenOriginal; start++)
    {
        unsigned i=0;
        for (;i<lenSearch;i++)
        {
            if (original[start] == search[i])
                break;
        }
        if (i == lenSearch)
            break;
    }

    size32_t end;
    for (end = lenOriginal; end != start; end--)
    {
        unsigned i=0;
        for (;i<lenSearch;i++)
        {
            if (original[end-1] == search[i])
                break;
        }
        if (i == lenSearch)
            break;
    }

    size32_t len = end-start;
    UChar * value = (UChar *)rtlMalloc(len);
    memcpy(value, original+start, len*sizeof(UChar));

    __lenResult = len;
    __result = value;
ENDC++;

s:=U'this is a test';
unicodeStrip(s, U'heti')
