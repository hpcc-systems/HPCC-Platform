/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//nothor
//noRoxie
#option ('optimizeDiskSource',true)
#option ('optimizeThorCounts',false)
#option ('optimizeChildSource',true)


namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

indirectRecord :=
            RECORD
unsigned4            id;
dataset(namesRecord) extra;
            END;
            

houseRecord :=
            RECORD
string100       addr;
dataset(indirectRecord) occupants;
            END;


d := dataset([
            {
                'Great Chishill', 
                [{1, [
                    {'Halliday','Gavin',35},
                    {'Halliday','Abigail',2},
                    {'Smith','John',57}
                ]}]
            },
            {
                'Birdcage walk',
                [{2, [
                      {'Smith','Gavin',12}
                ]}]
            }
        ], houseRecord);

output(d,,'houses3',overwrite);

houseTable2 := dataset('houses3', houseRecord, thor);

output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra, surname, all)); }));

output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra(age != 0), surname, all)); }));

p := table(houseTable2.occupants.extra(age != 0), { surname });

output(table(houseTable2, { addr, numFamilies := count(dedup(p, surname, all)); }));


//Filter at multiple levels
output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra(houseTable2.occupants.id != 2, age != 0), surname, all)); }));

//Filter at multiple levels where first level cannot get merged into the outer filter

p2 := table(houseTable2.occupants.extra, { surname, unsigned4 seq := random() % 100, age });

output(table(houseTable2, { addr, numFamilies := count(dedup(p2(seq != 101, age != 57), surname, all)); }));
