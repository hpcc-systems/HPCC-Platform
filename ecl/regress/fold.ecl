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

#option ('foldAssign', false);
#option ('globalFold', false);

export mydataset := DATASET('household', RECORD
            unsigned integer4 person_id,
            unsigned integer4 household_id,
            unsigned integer8 holepos
            END, THOR);

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);


NOT NOT true;
output(mydataset((real)person_id > 10.1));

// remove CHOOSEN/ENTH/SAMPLE
OUTPUT(CHOOSEN(namesTable, ALL));
OUTPUT(ENTH(namesTable, 1));
OUTPUT(SAMPLE(namesTable, 100, 1));

// fold to COUNT(namesTable) * 10
//SUM(namesTable, 10);             // remove as it generates incorrect hole code

// fold to constant
MAX(namesTable, 10);
MIN(namesTable, 10);
EVALUATE(namesTable[1], 10);
AVE(namesTable, 10);


count(namesTable(age in [1,2] and not age in [1,2]));
count(namesTable(age in [1,2] or not age in [1,2]));

[1,2,3][3];
