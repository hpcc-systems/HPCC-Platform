/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

namesTable2 := dataset([
        {'Hawthorn','Gavin',1},
        {'Hawthorn','Mia',2},
        {'Smithe','Pru',3},
        {'X','Z'}], namesRecord);

namesRecord t(namesRecord l) := TRANSFORM
    SELF.age := l.age + 1;
    SELF := l;
END;

null1 := NOFOLD(namesTable2)(age > 100);
o1 := output(DENORMALIZE(namesTable, null1, LEFT.surname = RIGHT.surname, t(LEFT)));

null2 := namesTable2(false);
o2 := output(DENORMALIZE(namesTable, null2, LEFT.surname = RIGHT.surname, t(LEFT)));


SEQUENTIAL(o1, o2);
