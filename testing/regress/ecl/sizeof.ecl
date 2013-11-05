/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

BOOLEAN b   := TRUE;

output('------ SIZEOF BOOLEAN');
output(SIZEOF(b));

INTEGER  i  := 0;
INTEGER1 i1 := 0;
INTEGER2 i2 := 0;
INTEGER3 i3 := 0;
INTEGER4 i4 := 0;
INTEGER5 i5 := 0;
INTEGER6 i6 := 0;
INTEGER7 i7 := 0;
INTEGER8 i8 := 0;

output('------ SIZEOF INTEGERn');
output(SIZEOF(i));
output(SIZEOF(i1));
output(SIZEOF(i2));
output(SIZEOF(i3));
output(SIZEOF(i4));
output(SIZEOF(i5));
output(SIZEOF(i6));
output(SIZEOF(i7));
output(SIZEOF(i8));

UNSIGNED INTEGER   ui   := 0;
UNSIGNED INTEGER1  ui1  := 0;
UNSIGNED INTEGER2  ui2  := 0;
UNSIGNED INTEGER3  ui3  := 0;
UNSIGNED INTEGER4  ui4  := 0;
UNSIGNED INTEGER5  ui5  := 0;
UNSIGNED INTEGER6  ui6  := 0;
UNSIGNED INTEGER7  ui7  := 0;
UNSIGNED INTEGER8  ui8  := 0;

output('------ SIZEOF UNSIGNED INTEGERn');
output(SIZEOF(ui));
output(SIZEOF(ui1));
output(SIZEOF(ui2));
output(SIZEOF(ui3));
output(SIZEOF(ui4));
output(SIZEOF(ui5));
output(SIZEOF(ui6));
output(SIZEOF(ui7));
output(SIZEOF(ui8));

REAL  r  := 0.0;
REAL4 r4     := 0.0;
REAL8 r8     := 0.0;

output('------ SIZEOF REALn');
output(SIZEOF(r));
output(SIZEOF(r4));
output(SIZEOF(r8));

DECIMAL1   d1       := 0;
DECIMAL2_2 d2_2 := 0;
DECIMAL3_2 d3_2 := 0;
DECIMAL4_2 d4_2 := 0;
DECIMAL5_2 d5_2 := 0;
DECIMAL6_2 d6_2 := 0;
DECIMAL7_2 d7_2 := 0;
DECIMAL8_2 d8_2 := 0;
DECIMAL9_2 d9_2 := 0;

output('------ SIZEOF DECIMAL 1..9');

output(SIZEOF(d1));
output(SIZEOF(d2_2));
output(SIZEOF(d3_2));
output(SIZEOF(d4_2));
output(SIZEOF(d5_2));
output(SIZEOF(d6_2));
output(SIZEOF(d7_2));
output(SIZEOF(d8_2));
output(SIZEOF(d9_2));

output('------ SIZEOF QSTRING Variable length, 1, & 120');

QSTRING1    qs1 := 'S';
QSTRING120  qs120   := 'Seisint';

output(SIZEOF(qs1));
output(SIZEOF(qs120));

output('------ SIZEOF & LENGTH of STRING concatenations');

output(SIZEOF('abc' + '123'));
output(LENGTH('abc' + '123'));
output(SIZEOF(''));
output(LENGTH(''));

output('------ SIZEOF DATASET');

SomeRecord  := RECORD
    STRING1 Value1;
    STRING1 Value2;
END;

SomeFile    := DATASET([   {'C','G'},
                     {'C','C'},
                           {'A','X'},
                     {'B','G'},
                     {'A','B'}],SomeRecord);

SIZEOF(SomeFile);
SIZEOF(SomeRecord);
SIZEOF(SomeFile.Value1);
