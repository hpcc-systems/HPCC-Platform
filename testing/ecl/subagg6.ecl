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

householdRecord := RECORD
  unsigned8     id;
  string20      addressText;
  DATASET(personRecord) people;
  string10      postcode;
END;

householdDataset := DATASET([
        {1,'99 Maltings Road',[{1,'Halliday','Gavin'},{2,'Halliday','Liz'}],'SG8'},
        {2,'Buck House',[{3,'Windsor','Elizabeth'},{4,'Corgi','Rolph'}],'WC1'},
        {3,'An empty location',[],'WC1'}
        ],householdRecord);

personAggRecord(personRecord ds) := 
            RECORD
               f1 := COUNT(GROUP);
               f2 := SUM(GROUP, ds.person_id);
               f3 := MAX(GROUP, ds.per_surname);
               f4 := min(group, ds.per_forename);
            END;

personSummary := table(householdDataset.people, personAggRecord(householdDataset.people))[1];

rolleduphousehold := table(householdDataset, { id, addressText, postCode, 
                                               unsigned8 countGroup := personSummary.f1, 
                                               unsigned8 sumGroup := personSummary.f2,
                                               string20 maxSurname := personSummary.f3,
                                               string20 minForename := personSummary.f4  } );

output(rolleduphousehold,NAMED('Summary'));

firstPerson := sort(householdDataset.people, per_surname, per_forename)[1];

firstHousehold := table(householdDataset, { id, addressText, postCode, firstPerson.per_surname, firstPerson.per_forename });
output(firstHousehold,NAMED('First'));
