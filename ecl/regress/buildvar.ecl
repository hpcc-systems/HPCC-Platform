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

mainRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
        END;

pstring := type
    export integer physicallength(string x) := transfer(x[1], unsigned1)+1;
    export string load(string x) := x[2..transfer(x[1], unsigned1)+1];
    export string store(string x) := transfer(length(x), string1)+x;
end;

varRecord :=
    RECORD
integer8            sequence;
pstring         forename;
pstring         surname;
    END;

d := DATASET('keyed.d00', mainRecord, THOR);

varRecord t(mainRecord l) :=
    TRANSFORM
        SELF.forename := TRIM(l.forename);
        SELF.surname := TRIM(l.surname);
        SELF := l;
    END;

x := project(d, t(left));

output(x,,'var.d00');

