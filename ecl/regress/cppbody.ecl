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



//Slightly weird - no parameters, so c++ is expanded inline instead of generating a function.
real8 sqrt2 :=
BEGINC++
sqrt(2.0)
ENDC++
;

output(sqrt2);


integer4 add(integer4 x, integer4 y) :=
BEGINC++
return x + y;
EnDc++;         // check that comments also work on the end of line


output(add(10,20));


string reverseString(string valUE) :=
BEGINC++
    size32_t len = lenValue;
    char * out = (char *)rtlMalloc(len);
    for (unsigned i= 0; i < len; i++)
        out[i] = value[len-1-i];
    __lenResult = len;
    __result = out;
ENDC++              // obscure comment test
;

output(reverseString('Gavin'));


r :=
record
    string10 name;
    unsigned4 id;
end;

ds := dataset('ds', r, thor);

ds unknownPerson() :=
BEGINC++
memcpy(x, "Unknown   ");
*(unsigned *)((byte *)x+10)= 0;
ENDC++;

//output(unknownPerson());

ds clearPerson :=
BEGINC++
memcpy(x, "          ");
*(unsigned *)((byte *)x+10)= 0;
ENDC++;

//output(clearPerson);



string26 alphabet(boolean lowercase) :=
BEGINC++
    char base = lowercase ? 'a' : 'A';
    for (unsigned i=0;i<26;i++)
        __result[i] = base+i;
ENDC++;

output(alphabet(true));


boolean nocaseInList(string search, set of string values) :=
BEGINC++
#include <string.h>
#body
    if (isAllValues)
        return true;
    const byte * cur = (const byte *)values;
    const byte * end = cur + lenValues;
    while (cur != end)
    {
        unsigned len = *(unsigned *)cur;
        cur += sizeof(unsigned);
        if (lenSearch == len && memicmp(search, cur, len) == 0)
            return true;
        cur += len;
    }
    return false;
endc++;

output(noCaseInList('gavin', ALL));
output(noCaseInList('gavin', ['Gavin','Jason','Emma']));
output(noCaseInList('gavin', ['Jason','Emma']));
