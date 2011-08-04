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

import lib_fileservices;

string do1(data s) := transfer(s, string);
string do2(data s) := transfer(transfer(s, string1000),string);
string do3(data s) := function
    len := transfer(s, integer4);
    return transfer(s, string10000)[5..len+4];
end;
string do(data s) := do3(s);


MAC_ScanFile(IP, infile, scansize) := MACRO
    ds := DATASET(FileServices.ExternalLogicalFileName(IP, infile),{DATA1 S}, THOR )[1..scansize];
    output(ds);
    Rec := RECORD,MAXLENGTH(64* 1024)
        UNSIGNED2 C;
        DATA S;
    END;
    Rec XF1(ds L,INTEGER C) := TRANSFORM
        SELF.C := C;
        SELF.S := L.s;
    END;
    ds2  := PROJECT(ds,XF1(LEFT,COUNTER));
    Rec XF2(Rec L,Rec R) := TRANSFORM
        SELF.S := L.S[1 .. R.C-1] + R.S[1];
        SELF := L;
    END;
    Rolled := ROLLUP(ds2,TRUE,XF2(LEFT,RIGHT));
    transferred := project(Rolled, transform({string s}, self.s := do(left.S)));
    OUTPUT(do(Rolled[1].S));
    OUTPUT(transferred[1].S);
ENDMACRO;

MAC_ScanFile('10.173.9.4', 'C:\\training\\import\\NamePhones', 100)

