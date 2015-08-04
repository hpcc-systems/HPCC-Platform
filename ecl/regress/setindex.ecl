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

#option ('globalFold', false);
person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, string40 per_last_name}, thor);
integer one := 1 : stored('one');
integer three := 1 : stored('three');

f(set of string a) := if ( a[1]='', 'yes','no');
g(set of string a) := if ( a[3]='', 'yes','no');
f2(set of string a) := if ( a[one]='', 'yes','no');
g2(set of string a) := if ( a[three]='', 'yes','no');
isHello(set of string a, unsigned4 idx) := if(a[idx]='Hello','yes','no');
isHello2(set of string a, unsigned4 idx) := if('Hello'=a[idx],'yes','no');

f([]);
g(['a']);
g(['a','b','c']);
isHello([],count(person));
isHello([],4);
isHello(['a','b','c'],count(person));
if (['a','b','c'][count(person)]='Hello','yes','no');
isHello2([],count(person));
isHello2([],4);
isHello2(['a','b','c'],count(person));
f2([]);
//f2(['a','ab','abc']);
g2(['a']);
g2(['a','b','c']);
