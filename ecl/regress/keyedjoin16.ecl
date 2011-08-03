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

