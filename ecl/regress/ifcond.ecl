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


