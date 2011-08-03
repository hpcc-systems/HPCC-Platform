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
