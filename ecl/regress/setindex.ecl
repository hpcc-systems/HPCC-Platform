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
