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
        
personDataset := DATASET ([{'Gavin', 'Halliday', 2,
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
