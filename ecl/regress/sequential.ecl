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
unsigned4 personid;
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
personDataset := DATASET('person',personRecord,FLAT);


o1 := output(personDataset,,'out1.d00');
o2 := output(personDataset,,'out2.d00');
o3 := output(personDataset,,'out3.d00');
o4 := output(personDataset,,'out4.d00');

o1;
o2;
sequential(o1, o2);

sequential(parallel(o2,o3),parallel(o1,o4));



