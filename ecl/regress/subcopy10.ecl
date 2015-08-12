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

//Check a single person records can be treated as a blob

personRecord := RECORD
unsigned4 person_id;
string20  per_surname;
string20  per_forename;
    END;

householdRecord :=
                RECORD
unsigned8           id;
string20            addressText;
DATASET(personRecord)   people;
string10            postcode;
                END;

householdDataset := DATASET([
        {1,'10 Slapdash Lane',[{1,'Hawthorn','Gavin'},{2,'Hawthorn','Mia'}],'SG8'},
        {2,'Buck House',[{3,'Windsor','Elizabeth'},{4,'Corgi','Rolph'}],'WC1'},
        {3,'An empty location',[],'WC1'}
        ],householdRecord);

forenameRecord := RECORD
string20  per_forename;
    END;

forenamerecord extractForename(personRecord l) :=
            TRANSFORM
                SELF := l;
            END;


//This syntax is awful, but it works...
slimmedChildren := project(householdDataset.people((person_id & 1) <> 0), extractForename(LEFT));

slimHousehold1 := table(householdDataset, { id, dataset(forenameRecord) newPeople := slimmedChildren; });
output(slimHousehold1);

slimHousehold1b := table(householdDataset, { id, newPeople := slimmedChildren; });
output(slimHousehold1b);

slimHouseholdRecord :=
                RECORD
unsigned8           id;
DATASET(forenameRecord)   people;
                END;


slimHouseholdRecord slimHousehold(householdRecord l) :=
            TRANSFORM
                SELF.people := project(l.people((person_id & 1) <> 0), extractForename(LEFT));
                SELF := l;
            END;

//Much nicer using
slimHousehold2 := project(householdDataset, slimHousehold(LEFT));
output(slimHousehold2);
