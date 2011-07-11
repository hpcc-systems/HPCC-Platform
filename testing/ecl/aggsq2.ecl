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

houseRecord :=
            RECORD
string100       addr;
dataset(namesRecord) occupants;
            END;

d := dataset([
            {
                'Great Chishill', 
                [
                    {'Halliday','Gavin',35},
                    {'Halliday','Abigail',2},
                    {'Smith','John',57}
                ]
            },
            {
                'Birdcage walk',
                [
                      {'Smith','Gavin',12}
                ]
            }
        ], houseRecord);

output(d,,'houses2',overwrite);

houseTable := dataset('houses2', houseRecord, thor);

//--- End of common ---

//Test disknormalize + disknormalize non-grouped aggregate
output(sort(dedup(houseTable.occupants, surname, all, local),surname));

output(count(dedup(houseTable.occupants, surname, all)));

output(count(dedup(houseTable.occupants(age != 0), surname, all)));

p := table(houseTable.occupants(age != 0), { surname });

output(count(dedup(p, surname, all)));

