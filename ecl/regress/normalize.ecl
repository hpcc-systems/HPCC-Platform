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
unsigned1       numRows;
string20        name;
string20        addr1 := '';
string20        addr2 := '';
string20        addr3 := '';
string20        addr4 := '';
            END;

namesTable := dataset([
        {1,'Gavin','10 Slapdash Lane'},
        {2,'Liz','10 Slapdash Lane','3 The cottages'},
        {0,'Mr Nobody'},
        {4,'Mr Everywhere','Here','There','Near','Far'}
        ], namesRecord);

outRecord := 
            RECORD
unsigned1       numRows;
string20        name;
string20        addr;
            END;


outRecord normalizeAddresses(namesRecord l, integer c) := 
                TRANSFORM
                    SELF := l;
                    SELF.addr := CHOOSE(c, l.addr1, l.addr2, l.addr3, l.addr4);
                END;

normalizedNames := normalize(namesTable, LEFT.numRows, normalizeAddresses(LEFT, COUNTER));

outRecord rollupPeople(outRecord l, outRecord r) := 
                TRANSFORM
                    SELF := l;
                END;

singleNames := rollup(normalizedNames, name, rollupPeople(LEFT, RIGHT));

namesRecord denormalizeAddresses(outRecord l, outRecord r, integer c) := 
                TRANSFORM
                    SELF.numRows := c;
                    SELF.name := l.name;
                    //SELF.addr[c] == r.addr;
                    SELF.addr1 := IF(c = 1, r.addr, self.addr1);
                    SELF.addr2 := IF(c = 2, r.addr, self.addr2);
                    SELF.addr3 := IF(c = 3, r.addr, self.addr3);
                    SELF.addr4 := IF(c = 4, r.addr, self.addr4);
                END;


denormalizedNames := denormalize(singleNames, normalizedNames, LEFT.name = RIGHT.name, denormalizeAddresses(LEFT, RIGHT, COUNTER));

output(denormalizedNames,,'out.d00');
//output(normalizedNames,,'out.d00');
