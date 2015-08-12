/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
