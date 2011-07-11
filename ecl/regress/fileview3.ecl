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

namesRecord := 
            RECORD
string      surname;
string      forename;
integer2    age := 25;
            ifblock(self.age > 30)
string          occupation;
            end;
            END;

namesTable := dataset([
        {'Halliday','Gavin',34,'Tea boy'},
        {'Halliday','Liz',34, 'Doctor'},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);

output(namesTable,,'out.d00',overwrite);

nameRecordEx := RECORD
namesRecord;
unsigned8 filepos{virtual(fileposition)};
                END;

ds := dataset('out.d00', nameRecordEx, thor);
output(ds);



crazyNameRecordEx := RECORD
namesRecord;
unsigned8 filepos{virtual(fileposition)};
            ifblock(SELF.filepos=9999999999)
string20        extra;
            end;
                END;

//This doesn't work, but I don't really care at the moment.
//output(dataset('out.d00', crazyNameRecordEx, thor));

