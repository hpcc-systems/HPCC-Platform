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

//Check a single child records can be treated as a blob

childRecord := RECORD
unsigned4 person_id;
string20  per_surname;
string20  per_forename;
unsigned8 holepos;
    END;

parentRecord :=
                RECORD
unsigned8           id;
string20            address1;
string20            address2;
string20            address3;
unsigned2           numPeople;
DATASET(childRecord, COUNT(SELF.numPeople))   children;
string10            postcode;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);
childDataset := DATASET('test',childRecord,FLAT);


//calculate a result in thor and return the result to ecl.
//count(childDataset);


//calculate sums separately and then return the result
//should really do this in thor, but can't at the moment.
count(childDataset(person_id=1)) + count(childDataset(person_id=2));

/*
//result needed to calc and return...
if (count(childDataset(person_id=3)) > 0,
   count(childDataset(person_id=4)),
   count(childDataset(person_id=5)));
*/

z := table(childDataset, { c := count(group), s:= sum(group,holepos); });
z1 := z[1];
zs := z.s;

//Examples where the dataset is not avaiable, and need to go and calculate

//z1.c;
//evaluate(z1, zs);
