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

output(d,,'houses4s',overwrite);

houseTable2 := dataset('houses4s', houseRecord, thor);

output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra, surname, all)); }));

output(table(houseTable2, { addr, numFamilies := count(dedup(occupants.extra(aage != 0), surname, all)); }));

p := table(houseTable2.occupants.extra(aage != 0), { surname });

output(table(houseTable2, { addr, numFamilies := count(dedup(p, surname, all)); }));

