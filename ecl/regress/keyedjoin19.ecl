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
//Tests several things:
//1. That keyed conditions only involving LEFT.x.y are correctly mapped back to the no-serialized LEFT
//2. That different variations of range checking can be keyed
//See bug 18731 for original report

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





output(join(peopleDataset1, sequenceKey, keyed(left.idx.id = right.surname), atmost(100)));
output(join(peopleDataset1, sequenceKey, keyed(left.idx.id = right.surname[1..length(trim(left.idx.id))]), atmost(100)));
output(join(peopleDataset1, sequenceKey, keyed((qstring)left.id2 = left.id2 and left.id2 = right.surname[1..length(trim(left.id2))])));
output(join(peopleDataset1, sequenceKey, keyed((qstring)left.idx.id = left.idx.id and left.idx.id = right.surname[1..length(trim(left.idx.id))])));
output(join(peopleDataset1, sequenceKey, keyed(right.surname[1..length(trim(left.idx.id))] in [left.idx.id, left.id2])));
output(join(peopleDataset1, sequenceKey, keyed(right.surname[1..length(trim(left.idx.id))] = left.idx.id or
                                               right.surname[1..length(trim(left.idx.id))] = left.id2)));
output(join(peopleDataset1, sequenceKey, keyed(right.surname[1..length(trim(left.idx.id))] in [left.idx.id, left.id2]), atmost(100)));


