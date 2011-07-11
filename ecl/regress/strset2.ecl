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
