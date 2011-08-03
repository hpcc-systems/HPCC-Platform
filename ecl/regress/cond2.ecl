/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#option ('targetClusterType', 'roxie');
#option ('minimizeSpillSize', true);

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

peopleDataset0 := peopleDataset;
peopleDataset1 := peopleDataset;
peopleDataset2 := peopleDataset;

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
