/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
