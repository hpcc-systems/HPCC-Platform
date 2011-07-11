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
a := trim(' abc def ', left);
b := trim(' abc def ', right);
c := trim(' abc def ', all);
d := trim(' abc def ', left, right);
e := trim(' abc def ');


a;
b;
c;
d;
e;

#IF (a+'!' = 'abc def !')
'Correct';
#ELSE
'Incorrect';
#END
#IF (b+'!' = ' abc def!')
'Correct';
#ELSE
'Incorrect';
#END
#IF (c+'!' = 'abcdef!')
'Correct';
#ELSE
'Incorrect';
#END
#IF (d+'!'='abc def!')
'Correct';
#ELSE
'Incorrect';
#END
#IF (e+'!' = ' abc def!')
'Correct';
#ELSE
'Incorrect';
#END

a3 := trim((varstring)' abc def ', left);
b3 := trim((varstring)' abc def ', right);
c3 := trim((varstring)' abc def ', all);
d3 := trim((varstring)' abc def ', left, right);
e3 := trim((varstring)' abc def ');


a3;
b3;
c3;
d3;
e3;

#IF (a3+'!' = 'abc def !')
'Correct';
#ELSE
'Incorrect';
#END
#IF (b3+'!' = ' abc def!')
'Correct';
#ELSE
'Incorrect';
#END
#IF (c3+'!' = 'abcdef!')
'Correct';
#ELSE
'Incorrect';
#END
#IF (d3+'!'='abc def!')
'Correct';
#ELSE
'Incorrect';
#END
#IF (e3+'!' = ' abc def!')
'Correct';
#ELSE
'Incorrect';
#END

person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, string40 per_last_name}, thor);
a2 := trim(person.per_first_name, left);
b2 := trim(person.per_first_name, right);
c2 := trim(person.per_first_name, all);
d2 := trim(person.per_first_name, left, right);
e2 := trim(person.per_first_name);


count(person(a <> '' AND b <> '' AND c <> '' AND d <> '' AND e <> ''));
count(person(a2 <> '' AND b2 <> '' AND c2 <> '' AND d2 <> '' AND e2 <> ''));
