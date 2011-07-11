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

output(d,,'houses2',overwrite);

houseTable2 := dataset('houses2', houseRecord, thor);

output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra, surname, all)); }));

output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra(age != 0), surname, all)); }));

p := table(houseTable2.occupants.extra(age != 0), { surname });

output(table(houseTable2, { addr, numFamilies := count(dedup(p, surname, all)); }));


//Filter at multiple levels
output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra(houseTable2.occupants.id != 2, age != 0), surname, all)); }));

//Filter at multiple levels where first level cannot get merged into the outer filter

p2 := table(houseTable2.occupants.extra, { surname, unsigned4 seq := random() % 100, age });

output(table(houseTable2, { addr, numFamilies := count(dedup(p2(seq != 101, age != 57), surname, all)); }));
