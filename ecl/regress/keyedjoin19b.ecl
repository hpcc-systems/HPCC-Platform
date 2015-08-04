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

#option('targetClusterType', 'roxie');

mainRecord :=
        RECORD
qstring14           surname;
integer8            sequence;
string20            forename;
string20            alias;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('~keyed.d00',mainRecord,THOR);

sequenceKey := INDEX(mainTable, { mainTable }, 'sequence.idx');

r := { string20 id; };

peopleRecord := RECORD
string20 id2;
r       idx;
unsigned4 extra;
string15   xyz;
            END;


peopleDataset1 := DATASET('ds', peopleRecord, thor);


mainRecord doJoin(peopleRecord l, sequenceKey r) := TRANSFORM
    SELF := r;
    END;



//output(join(peopleDataset1, sequenceKey, keyed(left.id.id = right.surname), atmost(100)));
//output(join(peopleDataset1, sequenceKey, keyed(left.id.id = right.surname[1..length(trim(left.id.id))]), atmost(100)));
//output(join(peopleDataset1, sequenceKey, keyed((qstring)left.id = left.id and left.id = right.surname[1..length(trim(left.id))])));
//output(join(peopleDataset1, sequenceKey, keyed((qstring)left.idx.id = left.idx.id and left.idx.id = right.surname[1..length(trim(left.idx.id))])));
//output(join(peopleDataset1, sequenceKey, keyed(right.surname[1..length(trim(left.idx.id))] in [left.idx.id, left.id2])));
//can't be done because requires condition to be duplicated.
output(join(peopleDataset1, sequenceKey, keyed(right.surname[1..length(trim(left.idx.id))] in [left.idx.id, left.id2]), atmost(100)));


