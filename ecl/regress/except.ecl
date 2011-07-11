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

//Normalize a denormalised dataset...

householdRecord := RECORD
unsigned4 house_id;
string20  address1;
string20  zip;
unsigned4 score;
string10 s_SequenceKey ;
unsigned2 scoreRank;
unsigned2 elementsMatched;
data10    m_l90key;
    END;


householdDataset := DATASET('household',householdRecord,FLAT);
filtered := householdDataset(zip != '');

a1 := sort(householdDataset, WHOLE RECORD);
output(a1);

a2 := sort(filtered, EXCEPT address1, filtered.zip, householdDataset.scoreRank, LOCAL);
output(a2);

b1 := dedup(householdDataset, WHOLE RECORD, all);
output(b1);

b2 := dedup(filtered, WHOLE RECORD, EXCEPT address1, EXCEPT filtered.zip, EXCEPT householdDataset.scoreRank, all);
output(b2);


g2 := group(filtered, WHOLE RECORD, EXCEPT address1, EXCEPT filtered.zip, EXCEPT householdDataset.scoreRank, all);
output(sort(g2, zip));


householdRecord tr(householdrecord l, householdrecord r) := transform
    self.zip := r.zip;
    self := l;
    end;

r2 := rollup(filtered, tr(left, right), WHOLE RECORD, EXCEPT address1, EXCEPT filtered.zip, EXCEPT householdDataset.scoreRank, local);
output(r2);

t2 := table(filtered, {count(group)}, WHOLE RECORD, EXCEPT address1, EXCEPT filtered.zip, EXCEPT householdDataset.scoreRank);
output(t2);

r3 := rollup(filtered, tr(left, right), score * scoreRank);
output(r3);
