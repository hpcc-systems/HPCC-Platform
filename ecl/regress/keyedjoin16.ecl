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

__set_debug_option__('targetClusterType', 'hthor');

unsigned sv1 := 0 : stored('sv1');
unsigned sv2 := 0 : stored('sv2');
unsigned sv3 := 0 : stored('sv3');

mainRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
string20            alias;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('~keyed.d00',mainRecord,THOR);

sequenceKey := INDEX(mainTable, { sequence }, { mainTable }, 'sequence.idx');

peopleRecord := RECORD
integer8        id;
            END;


peopleDataset1 := DATASET([3000,3500,30], peopleRecord);
peopleDataset2 := DATASET([{3000},{3500},{30}], peopleRecord);


mainRecord doJoin(peopleRecord l, sequenceKey r) := TRANSFORM
    SELF := r;
    END;



output(join(peopleDataset1, sequenceKey, left.id=right.sequence and left.id != sv3,doJoin(left,right), limit(sv1, fail('Help')),  keep(sv3)));
output(join(peopleDataset1, sequenceKey, left.id=right.sequence and left.id != sv3,doJoin(left,right), atmost(sv2), keep(sv3)));

