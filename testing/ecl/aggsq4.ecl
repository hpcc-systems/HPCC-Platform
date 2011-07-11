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
#option ('optimizeChildSource',true)

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        aage := 25;
            END;

indirectRecord :=
            RECORD
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
                [{[
                    {'Halliday','Gavin',35},
                    {'Halliday','Abigail',2},
                    {'Smith','John',57}
                ]}]
            },
            {
                'Birdcage walk',
                [{[
                      {'Smith','Gavin',12}
                ]}]
            }
        ], houseRecord);

output(d,,'houses2',overwrite);

houseTable2 := dataset('houses2', houseRecord, thor);

output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra, surname, all)); }));

output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra(aage != 0), surname, all)); }));

p := table(houseTable2.occupants.extra(aage != 0), { surname });

output(table(houseTable2, { addr, numFamilies := count(dedup(p, surname, all)); }));

