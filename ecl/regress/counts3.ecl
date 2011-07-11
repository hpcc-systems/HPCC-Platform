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

personRecord := RECORD
string10 personid;
string1 sex;
string33 forename;
string33 surname;
string4 yob;
unsigned4 salary;
string20 security;
string1 ownsRentsBoard;
unsigned2 numPublic;
unsigned1 numNotes;
unsigned2 numTrade;
unsigned4 person_sumtradeid;
unsigned4 public_c;
unsigned4 trade_c;
    END;

suffix := 'zz' : stored('suffix');

personDataset1 := DATASET('person',personRecord,FLAT);
personDataset2 := DATASET('person'+'x',personRecord,FLAT);
personDataset3 := DATASET('person'+NOFOLD('x'),personRecord,FLAT);
personDataset4 := DATASET('person'+suffix,personRecord,FLAT);


count(personDataset1);
count(personDataset2);
count(personDataset3);
count(personDataset4);
