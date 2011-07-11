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
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesTable2 := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);

output(namesTable2,,'out1.d00',CSV,overwrite);

output(namesTable2,,'out2.d00',CSV(MAXLENGTH(9999),SEPARATOR(';'),TERMINATOR(['\r\n','\r','\n','\032']),QUOTE(['\'','"'])), overwrite);

output(namesTable2,,'out3.d00',CSV(heading('surname forename age')), overwrite);

output(namesTable2,,'out4.d00',CSV(heading('surname forename age', 'end')), overwrite);

output(namesTable2,,'out5.d00',CSV(heading('surname forename age', single)), overwrite);

output(namesTable2,,'out6.d00',CSV(heading('surname forename age', 'end', single)), overwrite);
