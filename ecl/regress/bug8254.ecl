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

idRecord := { unsigned id; };

id2Record := { unsigned id; unsigned id2; };

namesRecord :=
            RECORD
string20        surname;
string10        forename;
dataset(idRecord) ids;
            END;

names2Record :=
            RECORD
string20        surname;
string10        forename;
dataset(id2Record) ids;
            END;

namesTable := dataset('x',namesRecord,FLAT);

id2Record t(idRecord l) := transform
    SELF.id := l.id - 1;
    SELF.id2 := l.id + 1;
END;

p := PROJECT(namesTable, TRANSFORM(names2Record, SELF.ids := PROJECT(LEFT.ids, t(LEFT)), SELF := LEFT));

//This filter is merged into the disk read, but can't be (easily) moved over the project

f := p(ids[1].id = 10);

output(f);
