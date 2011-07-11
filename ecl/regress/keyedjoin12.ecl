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

//Illustate bug in RecordSelectIterator() found while implementing record inheritance

__set_debug_option__('targetClusterType', 'hthor');

baseRecord := record
integer3        value;
              end;

derivedRecord := record(baseRecord), maxlength(100)
string              extra;
                end;
mainRecord := 
        RECORD
integer8            sequence;
string20            forename;
string20            surname;
string20            alias;
integer             a;
derivedRecord       blah;
unsigned8           filepos{virtual(fileposition)};
        END;

mainTable := dataset('~keyed.d00',mainRecord,THOR);

nameKey := INDEX(mainTable, { surname, forename, filepos }, 'name.idx');
sequenceKey := INDEX(mainTable, { sequence }, { forename, ifblock(self.forename <> '') surname; a; end; blah; filepos }, 'sequence.idx');

build(sequenceKey);

peopleRecord := RECORD
integer8        id;
string20        addr;
            END;

//peopleDataset := DATASET([{3000,'London'}], peopleRecord);
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
    SELF := l;
    SELF := r;
    END;

FilledRecs2 := join(peopleDataset, sequenceKey, left.id=right.sequence,transform(right), limit(100), left outer);
output(FilledRecs2);

