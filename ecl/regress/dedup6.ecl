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

x := sort(householdDataset, address1,zip,-score,scoreRank);

y:= rollup(x, left.address1 = right.address1 and left.zip = right.zip, TRANSFORM(LEFT));

count(x);
count(y);
output(y);
