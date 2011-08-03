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
