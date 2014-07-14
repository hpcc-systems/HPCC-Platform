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
