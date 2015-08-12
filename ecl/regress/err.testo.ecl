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

