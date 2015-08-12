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

gavLib := service
    set of integer4 getZipCodes(unsigned4 location) : eclrtl,pure,library='eclrtl',entrypoint='rtlGetZipCodes',oldSetFormat;
end;

mainRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
string20            alias;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('~keyed.d00',mainRecord,THOR);

nameKey := INDEX(mainTable, { surname, forename, filepos }, 'name.idx');
sequenceKey := INDEX(mainTable, { sequence, unsigned8 zfilepos := filepos }, 'sequence.idx');


peopleRecord := RECORD
integer8        id;
string20        addr;
            END;

peopleDataset := DATASET([{3000,'London'},{3500,'Miami'},{30,'Houndslow'}], peopleRecord);


joinedRecord :=
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
string20            addr;
unsigned8           filepos;
        END;

joinedRecord doJoin(peopleRecord l, mainRecord r) := TRANSFORM
    SELF := r;
    SELF := l;
    END;

FilledRecs := join(peopleDataset, mainTable, left.id=right.sequence AND left.addr <> right.alias,doJoin(left,right), left outer, KEYED(sequenceKey));
output(FilledRecs);

joinedRecord doJoin2(peopleRecord l, sequenceKey r) := TRANSFORM
    SELF := r;
    SELF := l;
    SELF := [];
    END;

FilledRecs2 := join(peopleDataset, sequenceKey, left.id=right.sequence, doJoin2(left,right), left outer);
output(FilledRecs2);

