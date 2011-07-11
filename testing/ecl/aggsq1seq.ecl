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

//noThor
//noRoxie

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

output(d,,'houses1s',overwrite);

houseTable := dataset('houses1s', houseRecord, thor);

//--- End of common ---

// Test compound child activities

p := table(houseTable.occupants(age != 0), { surname });

sequential(
    output(table(houseTable, { addr, numFamilies := count(dedup(occupants, surname, all)); })),

    output(table(houseTable, { addr, numFamilies := count(dedup(occupants(age != 0), surname, all)); })),

    output(table(houseTable, { addr, numFamilies := count(dedup(p, surname, all)); }))
);

