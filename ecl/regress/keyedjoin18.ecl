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
sequenceKey := INDEX(mainTable, sequenceRecord, 'sequence.idx');


peopleRecord := RECORD
integer8        id;
string20        addr;
            END;

//peopleDataset := DATASET([{3000,'London'}], peopleRecord);
peopleDataset := DATASET([{3000,'London'},{3500,'Miami'},{30,'Houndslow'}], peopleRecord);


output(join(peopleDataset, sequenceKey, left.id=right.sequence));                   // should have an implicit limit
output(join(peopleDataset, sequenceKey, left.id=right.sequence, atmost(100)));      // no implicit limit
output(join(peopleDataset, sequenceKey, left.id=right.sequence, limit(100)));       // no implicit limit
output(join(peopleDataset, sequenceKey, left.id=right.sequence, keep(100)));        // no implicit limit
output(join(peopleDataset, sequenceKey, keyed(left.id=right.sequence) and right.surname != '', keep(100)));     // implicit limit - since post filter
