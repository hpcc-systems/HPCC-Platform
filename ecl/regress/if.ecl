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

TestRecord :=   RECORD
string20            surname;
unsigned1           flags;
                    IFBLOCK(SELF.flags & 1 <> 0)
string20                forename;
                    END;
                    IFBLOCK(SELF.flags & 2 <> 0)
integer4                age := 0;
string8                 dob := '00001900';
                    END;
                END;


TestData := DATASET('if.d00',testRecord,FLAT);


OUTPUT(CHOOSEN(TestData,100),{surname,forename,age,dob},'out.d00');

