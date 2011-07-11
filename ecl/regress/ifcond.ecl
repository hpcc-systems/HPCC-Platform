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

#option ('foldAssign', false);
#option ('globalFold', false);
TestRecord :=   RECORD
string20            surname;
ebcdic string1      x;
ebcdic string1      x2;
string1     x3;
unsigned1           flags;
string20            forename;
integer4            age := 0;
integer4            dob := 0;
                END;


TestData := DATASET('if.d00',testRecord,FLAT);

TestRecord t(TestRecord r) := TRANSFORM
   SELF.AGE := IF(r.flags=10 AND r.surname IN ['abcdefghijklmnopqrst','abcdefghijklmnopqrsz'], 10,20);
   SELF.dob := IF(r.flags=10 , 10,20);
   SELF.x := IF(r.flags=10 AND r.x IN ['a','b'], 'a', 'b');
   SELF.x2 := IF(r.flags=10 AND r.x2 NOT IN ['a','b'], 'a', 'b');
   SELF.x3 := MAP(x'00' = x'00' => 'A', ' ');
   SELF := R;
   END;

OUTPUT(project(TestDAta,t(LEFT)));


