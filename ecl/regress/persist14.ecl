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

import lib_FileServices;

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

o1 := output(namesTable1(age>10),,'~testing::out1');
o2 := output(namesTable1(age<=10),,'~testing::out2');

a1 := parallel(o1, o2);
a2 := SEQUENTIAL(
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile('~testing::super','~testing::out1'),
    FileServices.AddSuperFile('~testing::super','~testing::out2'),
    FileServices.FinishSuperFileTransaction()
    );

a3 := SEQUENTIAL(
    FileServices.StartSuperFileTransaction(),
    FileServices.RemoveSuperFile('~testing::super','~testing::out2'),
    FileServices.FinishSuperFileTransaction(),
    FileServices.StartSuperFileTransaction(),
    FileServices.AddSuperFile('~testing::super','~testing::out2'),
    FileServices.FinishSuperFileTransaction()
    );

a5 := SEQUENTIAL(
    FileServices.StartSuperFileTransaction(),
    FileServices.RemoveSuperFile('~testing::super','~testing::out2'),
    FileServices.FinishSuperFileTransaction()
    );

super := dataset('~testing::super', namesRecord, thor);
per := super(age <> 0) : persist('per');

a4 := output(per);

unsigned action := 4;
case (action,
        1=>a1,
        2=>a2,
        3=>a3,
        5=>a5,
        a4);
