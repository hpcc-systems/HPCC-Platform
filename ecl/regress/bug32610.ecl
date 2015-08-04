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

