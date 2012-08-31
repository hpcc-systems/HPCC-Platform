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

output(d,,'houses2s',overwrite);

houseTable := dataset('houses2s', houseRecord, thor);

//--- End of common ---

p := table(houseTable.occupants(age != 0), { surname });


//Test disknormalize + disknormalize non-grouped aggregate
sequential(
    output(sort(dedup(houseTable.occupants, surname, all, local),surname)),

    output(count(dedup(houseTable.occupants, surname, all))),

    output(count(dedup(houseTable.occupants(age != 0), surname, all))),

    output(count(dedup(p, surname, all)))
);

