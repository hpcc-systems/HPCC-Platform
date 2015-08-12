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

childPersonRecord := RECORD
  string forename;
        unsigned1 age;
  END;

personRecord := RECORD
  string20 forename;
        string20 surname;
        unsigned2 numchildren;
        DATASET (childPersonRecord, COUNT (self.numChildren)) children;
        END;

//                            [{'Abigail',2}], {'Nathan', 2}},

personDataset := DATASET ([{'Gavin', 'Hawthorn', 2,
                            [{'Abigail',2}]},
                                                                                                         {'John', 'Simmons', 3,
                            [{'Alison',16}, {'Andrew', 10}, {'Jennifer', 18}]}],
                                                                                                        personRecord);

DATASET procKids := TRANSFORM
  RETURN DATASET (personDataset.children, childPersonRecord);
END;

kids := iterate (personDataset, procKids);
OUTPUT (kids);
//OUTPUT (personDataset.children, {personDataset.forename, forename, age});
/*
r := RECORD
  STRING name := People.Firstname;
        People.Firstname;
        num := COUNT (Property);
END;

T1 := TABLE (People, r);
OUTPUT (T1[1]);
OUTPUT (T1[2]);
OUTPUT (SUM (T1, num));
*/
/*
a := OUTPUT (People (FirstName = 'VLADIMIR'), NAMED ('Vlads'));
x := DATASET (WORKUNIT ('Vlads'), RECORDOF (People));
b := OUTPUT (x);

SEQUENTIAL (a,b);
OUTPUT (COUNT (x));
*/
