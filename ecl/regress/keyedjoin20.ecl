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
#option ('allowActivityForKeyedJoin', true);

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

output(join(peopleDataset1, sequenceKey(KEYED(surname <> '')), keyed(left.idx.id = right.surname), KEYED));
