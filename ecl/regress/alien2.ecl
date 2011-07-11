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

export extractXStringLength(data x, unsigned len) := transfer(((data4)(x[1..len])), unsigned4);

//A pretty weird type example - a length prefixed string, where the number of bytes used for the length is configurable...
export xstring(unsigned len) := type
    export integer physicallength(data x) := extractXStringLength(x, len)+len;
    export string load(data x) := (string)x[(len+1)..extractXStringLength(x, len)+len];
    export data store(string x) := transfer(length(x), data4)[1..len]+(data)x;
end;

pstring := xstring(1);
ppstring := xstring(2);
pppstring := xstring(3);
nameString := string20;

namesRecord := 
            RECORD
pstring         surname;
nameString      forename;
pppString       addr;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesTable2 := dataset([
        {'Halliday','Gavin','Slapdash Lane'},
        {'Halliday','Liz','Slapdash Lane'},
        {'Salter','Abi','Ashley Road'},
        {'X','Z','Mars'}], namesRecord);

output(namesTable2,,'x');
