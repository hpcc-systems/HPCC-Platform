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


pperson := DATASET('person', RECORD
string30    surname;
string30    forename;
integer4    age;
boolean     alive;
END, THOR);

InternalCppService := SERVICE
    memcpy(integer4 target, integer4 src, integer4 len);
    searchTableStringN(integer4 num, string entries, string search) : library='eclrtl', entrypoint='__searchTableStringN__';
    END;

Three := 3;
Four := 4;

count(pperson(pperson.age=20));

Three * Four
+ map(pperson.age=1=>1,pperson.age=2=>1,pperson.age=3=>3,5)
+ if(pperson.surname IN ['ab','cd','de'],3,4)
+ if(pperson.age IN [1,2,3,4,7,8,9],1,2)
+ if(pperson.age = 10, 99, -99) + (3 % 6);

pperson.forename + pperson.forename;

(3 + 4);

