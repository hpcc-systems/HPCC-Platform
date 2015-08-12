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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
            END;

personRecord :=
            RECORD(namesRecord)
integer2        age := 25;
            END;

personRecordEx :=
            RECORD(personRecord), locale('Somewhere')
                record
unsigned8       filepos{virtual(fileposition)};
                end;
            END;

personTable := dataset('x',personRecord,FLAT);
personTableEx1 := dataset('x1',personRecordEx,FLAT);
personTableEx2 := dataset('x2',personRecordEx,FLAT);


namesRecord t1(personRecord l) := TRANSFORM
            SELF := l;
        END;


output(project(personTableEx1, t1(LEFT)),,'out1.d00',overwrite);

personRecord t2(namesRecord l) := TRANSFORM
            SELF := l;
            SELF := [];
        END;


output(project(personTableEx2, t2(LEFT)),,'out2.d00',overwrite);


output(project(personTable, t2(LEFT)),,'out3.d00',overwrite);
