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
integer2        age := 25;
unsigned8       filepos{virtual(fileposition)};
            END;

namesIdRecord :=
            RECORD
namesRecord;
unsigned        id;
            END;

namesTable := dataset('x',namesRecord,FLAT);


projected := project(namesTable, transform(namesIdRecord, self := left; self := []));
output(projected);


//Not sure why you would want to do this... but we may as well be consistent.
namesIdRecord doClear :=
        transform
            self := [];
        end;

projected2 := project(namesTable, doClear);
output(projected2);


//functions of other transforms

namesIdRecord assignId(namesRecord l, unsigned value) :=
        TRANSFORM
            SELF.id := value;
            SELF := l;
        END;


assignId1(namesRecord l) := assignId(l, 1);
assignId2(namesRecord l) := assignId(l, 2);


//more check for mismatched types.

//more ad projects!


output(project(namesTable, assignId1(LEFT)));
output(project(namesTable, assignId2(LEFT)));

