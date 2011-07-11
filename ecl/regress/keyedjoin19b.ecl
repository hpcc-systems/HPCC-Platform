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


