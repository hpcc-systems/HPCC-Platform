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

//BUG: 13645 Check maxlength is inherited correctly
//It should really extend it if child only contains fixed length fields, but that isn't implemented yet.

namesRecord := 
            RECORD,maxlength(12345)
string          surname;
string          forename;
            END;

personRecord := 
            RECORD(namesRecord)
integer2        age := 25;
            END;

personRecordEx :=
            RECORD(personRecord), locale('Somewhere')
unsigned8       filepos{virtual(fileposition)};
            END;

personRecord2 := 
            RECORD(namesRecord),maxlength(1000)
integer2        age := 25;
            END;

output(dataset('x',personRecord,FLAT));
output(dataset('x1',personRecordEx,FLAT));
output(dataset('x',personRecord2,FLAT));
