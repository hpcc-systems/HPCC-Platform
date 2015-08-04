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

#option ('globalFold', false);
#option ('optimizeGraph', false);
#option ('targetClusterType', 'roxie');
#option ('spillMultiCondition', true);

mainRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
string20            alias;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('~keyed.d00',mainRecord,THOR);

sequenceRecord := RECORD
        mainTable.sequence;
        mainTable.surname;
        mainTable.alias;
        mainTable.filepos;
    END;

nameKey := INDEX(mainTable, { surname, forename, filepos }, 'name.idx');


peopleRecord := RECORD
integer8        id;
string20        name;
            END;

peopleDataset := DATASET([], peopleRecord) : stored('people');

peopleDataset0 := if(0=0, peopleDataset,peopleDataset);
peopleDataset1 := if(1=1, peopleDataset,peopleDataset);
peopleDataset2 := if(2=2, peopleDataset,peopleDataset);

joinedRecord :=
        RECORD
integer8            id;
string20            forename;
string20            surname;
        END;

joinedRecord doJoin(peopleRecord l, nameKey r) := TRANSFORM
    SELF := l;
    SELF := r;
    END;

outFile0 := join(peopleDataset0, nameKey, left.name[1..19]+'!' = right.surname,doJoin(left,right));
outFile1 := join(peopleDataset1, nameKey, left.name = right.surname,doJoin(left,right));
outFile2 := join(peopleDataset2, nameKey, left.name = right.forename,doJoin(left,right));

outFile := if(count(outFile0) > 10, outFile1, outFile2);
output(outFile);
