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

#option ('globalFold', false);
BOOLEAN b1  := IF(0>10,TRUE,FALSE); // = FALSE
output('------ IF(0>10,TRUE,FALSE)     = FALSE ?');
output (b1);

BOOLEAN b2  := IF(0<10,TRUE,FALSE); // = TRUE
output('------ IF(0<10,TRUE,FALSE)     = TRUE ?');
output (b2);

BOOLEAN b3  := IF(0=10,TRUE,FALSE); // = FALSE
output('------ IF(0=10,TRUE,FALSE)     = FALSE ?');
output (b3);

BOOLEAN b4  := IF(0!=10,TRUE,FALSE);// = TRUE
output('------ IF(0!=10,TRUE,FALSE)    = TRUE ?');
output (b4);

INTEGER  i  := 255;
INTEGER1 i1 := 255;
INTEGER2 i2 := 255;
INTEGER3 i3 := 255;
INTEGER4 i4 := 255;
INTEGER5 i5 := 255;
INTEGER6 i6 := 255;
INTEGER7 i7 := 255;
INTEGER8 i8 := 255;

UNSIGNED INTEGER   ui   := 255;
UNSIGNED INTEGER1  ui1  := 255;
UNSIGNED INTEGER2  ui2  := 255;
UNSIGNED INTEGER3  ui3  := 255;
UNSIGNED INTEGER4  ui4  := 255;
UNSIGNED INTEGER5  ui5  := 255;
UNSIGNED INTEGER6  ui6  := 255;
UNSIGNED INTEGER7  ui7  := 255;
UNSIGNED INTEGER8  ui8  := 255;

output('------ INTEGER[n] - 8 @ 255');

output(i1);
output(i2);
output(i3);
output(i4);
output(i5);
output(i6);
output(i7);
output(i8);

output('------ UNSIGNED INTEGER [n] - 8 @ 255');

output(ui);
output(ui1);
output(ui2);
output(ui3);
output(ui4);
output(ui5);
output(ui6);
output(ui7);
output(ui8);

output('------ COMPARE INTEGER to INTEGER[n] = TRUE ?');

output(IF(i=i1,FALSE,TRUE));           // = TRUE triggers signed bit
output(IF(i=i2,TRUE,FALSE));           // = TRUE
output(IF(i=i3,TRUE,FALSE));           // = TRUE
output(IF(i=i4,TRUE,FALSE));           // = TRUE
output(IF(i=i5,TRUE,FALSE));           // = TRUE
output(IF(i=i6,TRUE,FALSE));           // = TRUE
output(IF(i=i7,TRUE,FALSE));           // = TRUE

output('------ COMPARE UNSIGNED to UNSIGNED[n] = TRUE ?');

output(IF(ui=ui,TRUE,FALSE));          // = TRUE
output(IF(ui=ui1,TRUE,FALSE));     // = TRUE
output(IF(ui=ui2,TRUE,FALSE));     // = TRUE
output(IF(ui=ui3,TRUE,FALSE));     // = TRUE
output(IF(ui=ui4,TRUE,FALSE));     // = TRUE
output(IF(ui=ui5,TRUE,FALSE));     // = TRUE
output(IF(ui=ui6,TRUE,FALSE));     // = TRUE
output(IF(ui=ui7,TRUE,FALSE));     // = TRUE
output(IF(ui=ui8,TRUE,FALSE));     // = TRUE

REAL  r     := 255.0;
REAL4 r4        := 255.0;
REAL4 r4big     := 255.1234567890;
REAL8 r8        := 255.1234567890;


output('------ REAL, REAL4 & REAL8 255 or 255.123');

output(r);                          // = 255
output(r4);                         // = 255
output(r4big);                      // = 255.123
output(r8);                         // = 255.123
output(ISVALID(r));                 // = TRUE
output(ISVALID(r4));                // = TRUE
output(ISVALID(r4big));             // = TRUE
output(ISVALID(r8));                // = TRUE

output('------ COMPARE REAL4 & REAL8   = TRUE ?');

output(IF(r=r4,TRUE,FALSE));           // = TRUE
output(IF(r=r8,FALSE,TRUE));           // = TRUE

output(IF(r=i,TRUE,FALSE));        // = TRUE
output(IF(r4=i,TRUE,FALSE));           // = TRUE
output(IF(r8=i,FALSE,TRUE));           // = TRUE

DECIMAL4_1 d4_1   := 255.1;
DECIMAL5_2 d5_2   := 255.12;

output('------ DECIMAL                 = 255.1 & 255.12 ?');

output(d4_1);                           // = 255.1
output(d5_2);                           // = 255.12
output(ISVALID(d4_1));              // = TRUE
output(ISVALID(d5_2));              // = TRUE
output(ISVALID(transfer(x'ffffff',decimal3)));  // = FALSE;

output('------ QSTRING,STRING & LENGTH = 8 ?');

QSTRING qs120   := 'MixedCase';
output(qs120);                      // = MIXEDCASE
STRING s        := qs120;
output(s);                          // = MIXEDCASE
output(LENGTH(s));                  // = 8;

SET OF INTEGER1 soI1 := [1,2,3,4,5,6];
SET OF STRING1  soS1 := ['1','2','3','4','5','6'];

output('------ INTEGER SETs;           = TRUE, TRUE & FALSE ?');

output(IF(1 IN soI1,TRUE,FALSE));   // = TRUE
output(IF(6 IN soI1,TRUE,FALSE));   // = TRUE
output(IF(0 IN soI1,TRUE,FALSE));   // = FALSE

output('------ STRING SETs;            = TRUE, TRUE & FALSE ?');

output(IF('1' IN soS1,TRUE,FALSE)); // = TRUE
output(IF('6' IN soS1,TRUE,FALSE)); // = TRUE
output(IF('0' IN soS1,TRUE,FALSE)); // = FALSE

output('------ TYPEOF;                 = MIXEDCASE & TRUE ?');

TYPEOF(s) tos := s;
output(s);                          // = MIXEDCASE
output(IF(tos=s,TRUE,FALSE));       // = TRUE

output('------ BOOLEAN to INTEGER      = 1, 0 ?');

output((INTEGER)TRUE);              // = 1
output((INTEGER)FALSE);             // = 0

output('------ INTEGER to BOOLEAN      = FALSE, TRUE, TRUE ?');

output((BOOLEAN)0);                 // = FALSE
output((BOOLEAN)1);                 // = TRUE
output((BOOLEAN)2);                 // = TRUE

output('------ INTEGER to DECIMAL      = 0.0, 255 ?');

output((DECIMAL4_1)0);              // = 0.0
output((DECIMAL5_2)255);            // = 255

output('------ INTEGER to REAL         = 0, 1, 100 ?');

output((REAL)0);                    // = 0
output((REAL)1);                    // = 1
output((REAL)100);                  // = 100

output('------ INTEGER to STRING1      = 0, 1, 2 ?');

output((STRING1)0);                 // = '0'
output((STRING1)1);                 // = '1'
output((STRING1)2);                 // = '2'

output('------ DECIMAL to INTEGER      = 255, 255 ?');

output((INTEGER)d4_1);              // = 255
output((INTEGER)d5_2);              // = 255

output('------ DECIMAL to REAL         = 255.1, 255.12 ?');

output((REAL)d4_1);                 // = 255.1
output((REAL)d5_2);                 // = 255.12

output('------ DECIMAL to STRING1      = 255.1, 255.12 ?');

output((STRING)d4_1);               // = 255.1
output((STRING)d5_2);               // = 255.12

output('------ REAL to DECIMAL         = 0.0, 255.25, 0.0 ?');

output((DECIMAL4_1)0.0);            // = 0.0
output((DECIMAL5_2)255.25);         // = 255.25
output((DECIMAL5_2)999.999);        // = 0.0  loss of precision

output('------ REAL to INTEGER         = 0, 255, 999 ?');

output((INTEGER)0.0);               // = 0
output((INTEGER)255.25);            // = 255
output((INTEGER)999.999);           // = 999

output('------ REAL to STRING8         = 255.1230, 0.000000 ?');

output((STRING8)255.123);           // = 255.1230
output((STRING8)0.0);               // = 0.000000

output('------ STRING to DECIMAL       = 123.456, -23.456 ?');

output((DECIMAL8_3)'123.456');      // = 123.456
output((DECIMAL8_3)'-23.456');      // = -23.456

output('------ STRING to REAL          = 123.456, -23.456 ?');

output((REAL)'123.456');            // = 123.456
output((REAL)'-23.456');            // = -23.456

output('------ STRING  to INTEGER      = 0, 1, 100, 0 ?');

output((INTEGER)'0');               // = 0
output((INTEGER)'1');               // = 1
output((INTEGER)'100');             // = 100
output((INTEGER)'ABC');             // = 0

output('------ STRING  to QSTRING      = MIXED CASE, INC. BOCA RATON, FL. 33487 ?');

output((QSTRING)'Mixed Case, Inc.');   // = MIXED CASE, INC.
output((QSTRING)'Boca Raton, Fl.'); // = BOCA RATON, FL.
output((QSTRING)'33487');           // = 33487
