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

UNSIGNED4 LengthOfTheFirstStringInSet(set of string s) := BEGINC++
//unsigned int LengthOfTheFirstStringInSet(bool isAllS, unsigned lenS, void *s)
// The length of the first string is either lenS or *((unsigned int *) s depending on on whether // a single string set or multiple string set is passed in. This is an inconsistency that // needs to be corrected, because the code in the BEGINC++ structure cannot distinguish // those two cases. In my opinion the length of the first string in the set // should be stored at *((unsigned int *) s in both cases.
   return *(unsigned *)s;
 ENDC++;

string5 TextOfTheFirstString10InSet(set of string10 s) := BEGINC++
//unsigned int LengthOfTheFirstStringInSet(char * __result, bool isAllS, unsigned lenS, void *s)
// The length of the first string is either lenS or *((unsigned int *) s depending on on whether // a single string set or multiple string set is passed in. This is an inconsistency that // needs to be corrected, because the code in the BEGINC++ structure cannot distinguish // those two cases. In my opinion the length of the first string in the set // should be stored at *((unsigned int *) s in both cases.
    memcpy(__result, (char *)s+lenS-10, 5);
 ENDC++;

UNSIGNED4 LengthOfTheFirstStringInSet2(set of string s) :=
    LengthOfTheFirstStringInSet(set(dataset(s, { string x}), x));

LengthOfTheFirstStringInSet(['DAVID']);
LengthOfTheFirstStringInSet(['DAVID','James']);

LengthOfTheFirstStringInSet(['DAVID', 'BAYLISS']);

LengthOfTheFirstStringInSet2(['DAVID']);
LengthOfTheFirstStringInSet2(['DAVID','James']);

LengthOfTheFirstStringInSet2(['DAVID', 'BAYLISS']);

'!'+TextOfTheFirstString10InSet(['DAVID'])+'!';
'!'+TextOfTheFirstString10InSet(['DAVID','James'])+'!';
'!'+TextOfTheFirstString10InSet(['DAVID', 'BAYLISS'])+'!';
