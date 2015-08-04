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
unsigned1       numRows;
string20        name;
string20        addr1 := '';
string20        addr2 := '';
string20        addr3 := '';
string20        addr4 := '';
            END;

namesTable := dataset([
        {1,'Gavin','10 Slapdash Lane'},
        {2,'Mia','10 Slapdash Lane','3 The cottages'},
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
