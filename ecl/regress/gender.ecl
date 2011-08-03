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
string9         cid;
string20        firstn;
string20        middle;
            END;

genderRecord :=
            RECORD
string20        name;
string30        addr := 'Unknown';
            END;

namesTable := dataset([
        {'Smithe','Pru','10'},
        {'Hawthorn','Gavin','31'},
        {'Hawthorn','Mia','30'},
        {'Smith','Jo','x'},
        {'Smith','Matthew','y'},
        {'X','Z','Z'}], namesRecord);

genderTable := dataset([
        {'Hawthorn','10 Slapdash Lane'},
        {'Smith','Leicester'},
        {'Smith','China'},
        {'X','12 The burrows'},
        {'X','14 The crescent'},
        {'Z','The end of the world'}
        ], genderRecord);


Stub := namesTable;

LayoutGenderTable_Join_Out := record
 string9 cid;
end;

LayoutGenderTable_Join_Out JoinTrans(Stub L, GenderTable R) := TRANSFORM
SELF := L;
end;

LayoutGenderTable_Join_Out JoinTrans2(LayoutGenderTable_Join_Out L, LayoutGenderTable_Join_Out R) := TRANSFORM
SELF := L;
end;

GenderTable_1a := JOIN(Stub, GenderTable, left.firstn = right.name, JoinTrans(left,right));

GenderTable_1b := JOIN(Stub, GenderTable, left.middle = right.name, JoinTrans(left,right));

DOut := JOIN(GenderTable_1a, GenderTable_1b, left.CID = right.cid, JoinTrans2(left,right));

output(Dout)
