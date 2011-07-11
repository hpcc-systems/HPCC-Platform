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

output(ABS(1));
output(ABS(-1));

Rad2Deg := 57.295779513082;     // number of degrees in a angle
Deg2Rad := 0.0174532925199;     // number of radians in a degree
Angle45 := 45 * Deg2Rad;            // translate 45 degrees into radians

Cosine45    := COS(Angle45);            // get cosine of 45 degree angle
output(Cosine45);

CosineH45   := COSH(Angle45);           // get hperbolic cosine of 45 degree angle
output(CosineH45);

Sine45  := SIN(Angle45);            // get sine of 45 degee angle
output(Sine45);

SineH45 := SINH(Angle45);           // get hperbolic sine of 45 degree angle
output(SineH45);

ArcSine := ASIN(Sine45);            // get inverse of sine of 45 degree angle
output(ArcSine);

ArcCosine   := ACOS(Cosine45);      // get inverse of cosine of 45 degree angle
output(ArcCosine);

Tangent45   := TAN(Angle45);            // get tagent of 45 degree angle
output(Tangent45);

TangentH45  := TANH(Angle45);           // get tagent of 45 degree angle
output(TangentH45);
    
ArcTangent  := ATAN(Tangent45);     // get inverse of tangent of 45 degree angle
output(ArcTangent);

PI      := 3.14159;
output(EXP(PI));                    // get natural exponential value of PI
output(LN(PI));                 // get natural logarithm value of PI
output(LOG(PI));                    // get base-10 logarithm of PI

output(POWER(2.0,3.0));             // = 8
output(POWER(3.0,2.0));             // = 9

output(RANK(1,[20,30,10,40]));      // = 2
output(RANK(1,[20,30,10,40],DESCEND));  // = 3
output(RANKED(1,[20,30,10,40]));        // = 3
output(RANKED(1,[20,30,10,40],DESCEND));    // = 4

output(ROUND(3.14159));             // = 3
output(ROUND(3.5));             // = 4
output(ROUND(-1.3));                // = -1
output(ROUND(-1.8));                // = -2

output(ROUNDUP(3.14159));           // = 4
output(ROUNDUP(-3.9));              // = -4

output(SQRT(16.0));             // = 4
output(SQRT(81.0));             // = 9

output((INTEGER)(TRANSFER(65,STRING1)));    // = 0 because 'A' is not a numeric number

string20 trim20 := 'ABC';           // = 'ABC'
output(TRIM(trim20));

string15 trim15 := '   ABC';
output(TRIM(trim15));               // = '   ABC'
output(TRIM(trim15,ALL));           // = 'ABC'

output(TRUNCATE(3.75));             // = 3
output(TRUNCATE(1.25));             // = 1
output(TRUNCATE(10.1));             // = 10

output(CASE(1, 1 => 9, 2 => 8, 3 => 7, 4 => 6, 5)); // = 9
output(CASE(2, 1 => 9, 2 => 8, 3 => 7, 4 => 6, 5)); // = 8
output(CASE(3, 1 => 9, 2 => 8, 3 => 7, 4 => 6, 5)); // = 7
output(CASE(0, 1 => 9, 2 => 8, 3 => 7, 4 => 6, 5)); // = 5

output(CHOOSE(3,9,8,7,6,5));            // = 7
output(CHOOSE(3,1,2,3,4,5));            // = 3
output(CHOOSE(9,1,2,3,4,5));            // = 5  the default value
output(CHOOSE(3,15,14,13,12));      // = 13

output(REJECTED(1=1,2=2,3=3,4=4,5=5));  // = 0
output(REJECTED(1=1,2=2,3=0,4=4,5=5));  // = 3
output(REJECTED(1=1,2=2,3=0,4=0,5=0));  // = 3
output(REJECTED(1=0,2=0,3=0,4=0,5=0));  // = 1

output(WHICH(1=0,2=0,3=0,4=0,5=5));     // = 5
output(WHICH(1=0,2=2,3=0,4=4,5=0));     // = 2
output(WHICH(1=1,2=0,3=0,4=0,5=0));     // = 1
output(WHICH(1=0,2=0,3=0,4=0,5=0));     // = 0

// WARNING
// WARNING  If you change the values, or the number of records in this in-line
// WARNING  dataset you may break some or all of the subsequent test cases!
// WARNING

SomeRecord  := RECORD
            STRING1 Value1;
            STRING2 Value2;
            INTEGER4    Value3;
            INTEGER4    Value4;
           END;

SomeFile    := DATASET([{'C','G',25,53},
                  {'C','C',71,15},
                {'A','X',66,22},
                {'B','G',102,97},
                {'A','B',20,57}],SomeRecord);

output(AVE(SomeFile,SomeRecord.Value3));    // = 65.8
output(AVE(SomeFile,SomeRecord.Value4));    // = 48.8

output(MAX(SomeFile,SomeRecord.Value3));    // = 102
output(MIN(SomeFile,SomeRecord.Value3));    // = 20
output(MAX(SomeFile,SomeRecord.Value4));    // = 97
output(MIN(SomeFile,SomeRecord.Value4));    // = 15

output(SUM(SomeFile,SomeRecord.Value3));    // = 284
output(SUM(SomeFile,SomeRecord.Value4));    // = 244

output(COUNT(SomeFile));                    // = 5
output(COUNT(SomeFile(SomeRecord.Value3 = 66)));    // = 1
output(COUNT(SomeFile(SomeRecord.Value1 = 'C')));   // = 2

MapResult1  := MAP(EXISTS(SomeFile(SomeRecord.Value1 = 'C')) => 1
                ,NOT EXISTS(SomeFile(SomeRecord.Value2 = 'X')) => 2
              ,3);

output(mapResult1);