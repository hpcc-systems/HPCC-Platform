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

#option ('childQueries', true);

personRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesRecord :=
            RECORD
unsigned        id;
personRecord;
            END;

namesTable := dataset('x',namesRecord,FLAT);


addressRecord :=
            RECORD
unsigned        id;
dataset(personRecord) people;
string40        street;
string40        town;
string2         st;
            END;


personRecord tc0(namesRecord l) :=
    TRANSFORM
        SELF := l;
    END;

addressRecord t0(namesRecord l) :=
        TRANSFORM
SELF.people := project(dataset([{l.id,l.surname,l.forename,l.age}], namesRecord), tc0(LEFT));
SELF := [];
        END;

p0 := project(namesTable, t0(LEFT));
output(p0);


personRecord tc1(namesRecord l) :=
    TRANSFORM
        SELF := l;
    END;

addressRecord t1(namesRecord l) :=
        TRANSFORM
SELF.people := project(dataset(l), tc1(LEFT));
SELF := [];
        END;

p1 := project(namesTable, t1(LEFT));
output(p1);

addressRecord t2(namesRecord l) :=
        TRANSFORM
SELF.people := dataset(row(l,personRecord));
SELF := [];
        END;

p2 := project(namesTable, t2(LEFT));
//output(p2);

//MORE: This should really work, but it doesn't because we distinguish too strongly between datasets and datarows,
//which begins to fall apart when child queries and multiple datasets in scope come into play.
//p3 := table(namesTable, { dataset(personRecord) people := dataset(row(namesTable,personRecord)); });
//output(p3);

personRecord tc4(namesRecord l) :=
    TRANSFORM
        SELF := l;
    END;

addressRecord t4(namesRecord l) :=
        TRANSFORM
SELF.people := project(dataset([l], namesRecord), tc4(LEFT));
SELF := [];
        END;

p4 := project(namesTable, t4(LEFT));
output(p4);


addressRecord t5(namesRecord l) :=
        TRANSFORM
SELF.people := (row(l,personRecord)+row(l,personRecord))[1..1];
SELF := [];
        END;

p5 := project(namesTable, t5(LEFT));
output(p5);

//ugly, but allow assignment of a datarow to a dataset.  Remain undocumented for the moment.
addressRecord t6(namesRecord l) :=
        TRANSFORM
SELF.people := row(l,personRecord);         // same as dataset(row(l,personRecord))
SELF := [];
        END;

p6 := project(namesTable, t6(LEFT));
output(p6);


//ugly, but allow assignment of a datarow to a dataset.  Remain undocumented for the moment.
personRecord t7(namesRecord l) :=
        TRANSFORM
            SELF := l;
        END;

addressRecord t8(namesRecord l) :=
        TRANSFORM
SELF.people := row(l,t7(LEFT));         // same as dataset(row(l,personRecord))
SELF := [];
        END;

p8 := project(namesTable, t8(LEFT));
output(p8);

