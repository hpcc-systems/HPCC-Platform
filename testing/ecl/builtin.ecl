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

SixPlaces(real8 x) := TRIM(REALFORMAT(x, 20, 6), LEFT);

output('------ ABS(1) = 1 ?');
output(ABS(1));                 // = 1

output('------ ABS(-1) = 1 ?');
output(ABS(-1));                    // = 1

Rad2Deg := 57.295779513082;     // number of degrees in a angle
Deg2Rad := 0.0174532925199;     // number of radians in a degree
Angle45 := 45 * Deg2Rad;            // translate 45 degrees into radians

Cosine45    := COS(Angle45);            // get cosine of 45 degree angle
output('------ COS(Angle45) = 0.707107 ?');
output(SixPlaces(Cosine45));                    // = 0.707107

CosineH45   := COSH(Angle45);           // get hperbolic cosine of 45 degree angle
output('------ COSH(Angle45) = 1.324609 ?');
output(SixPlaces(CosineH45));               // = 1.324609

Sine45  := SIN(Angle45);            // get sine of 45 degee angle
output('------ SIN(Angle45) = 0.707107 ?');
output(SixPlaces(Sine45));                  // = 0.707107

SineH45 := SINH(Angle45);           // get hperbolic sine of 45 degree angle
output('------ SINH(Angle45) = 0.868671 ?');
output(SixPlaces(SineH45));                 // = 0.868671

ArcSine := ASIN(Sine45);            // get inverse of sine of 45 degree angle
output('------ ASIN(Sine45) = 0.785398 ?');
output(SixPlaces(ArcSine));                 // = 0.785398

ArcCosine   := ACOS(Cosine45);      // get inverse of cosine of 45 degree angle
output('------ ACOS(Cosine45) = 0.785398 ?');
output(SixPlaces(ArcCosine));               // = 0.785398

Tangent45   := TAN(Angle45);            // get tagent of 45 degree angle
output('------ TAN(Angle45) = 1 ?');
output(SixPlaces(Tangent45));               // = 1

TangentH45  := TANH(Angle45);           // get tagent of 45 degree angle
output('------ TANH(Angle45) = 0.655794 ?');
output(SixPlaces(TangentH45));              // = 0.655794
    
ArcTangent  := ATAN(Tangent45);     // get inverse of tangent of 45 degree angle
output('------ ATAN(Tangent45) = 0.785398 ?');
output(SixPlaces(ArcTangent));              // = 0.785398

PI      := 3.14159;
output('------ EXP(3.14159)= 23.1406 ?');
output(SixPlaces(EXP(PI)));                 // get natural exponential value of PI = 23.1406

output('------ LN(3.14159)= 1.14473 ?');
output(SixPlaces(LN(PI)));                  // get natural logarithm value of PI = 1.14473

output('------ LOG(3.14159) = 0.49715 ?');
output(SixPlaces(LOG(PI)));                 // get base-10 logarithm of PI = 0.49715

output('------ POWER(2.0,3.0) = 8 ?');
output(POWER(2.0,3.0));             // = 8

output('------ POWER(3.0,2.0) = 9 ?');
output(POWER(3.0,2.0));             // = 9

output('------ RANK(1,[20,30,10,40]) = 2 ?');
output(RANK(1,[20,30,10,40]));      // = 2

output('------ RANK(1,[20,30,10,40],DESCEND) = 3 ?');
output(RANK(1,[20,30,10,40],DESCEND));  // = 3

output('------ RANKED(1,[20,30,10,40]) = 3 ?');
output(RANKED(1,[20,30,10,40]));        // = 3

output('------ RANKED(1,[20,30,10,40],DESCEND) = 4 ?');
output(RANKED(1,[20,30,10,40],DESCEND));    // = 4

output('------ ROUND(3.14159) = 3 ?');
output(ROUND(3.14159));             // = 3

output('------ ROUND(3.5) = 4 ?');
output(ROUND(3.5));             // = 4

output('------ ROUND(-1.3) = -1 ?');
output(ROUND(-1.3));                // = -1

output('------ ROUND(-1.8) = -2 ?');
output(ROUND(-1.8));                // = -2

output('------ ROUNDUP(3.14159) = 4 ?');
output(ROUNDUP(3.14159));           // = 4

output('------ ROUNDUP(-3.9) = -4 ?');
output(ROUNDUP(-3.9));              // = -4

output('------ SQRT(16.0) = 4 ?');
output(SQRT(16.0));             // = 4

output('------ SQRT(81.0) = 9 ?');
output(SQRT(81.0));             // = 9

output('------ TRANSFER(65,STRING1) = 0 ?');
output((INTEGER)(TRANSFER(65,STRING1)));    // = 0 because 'A' is not a numeric number

output('------ TRIM STRING COMMANDS');

string20 trim20 := 'ABC';
string15 trim15 := '   ABC';

output(TRIM(trim15));               // = '   ABC'
output(TRIM(trim15,ALL));           // = 'ABC'
output(TRIM(trim20));               // = 'ABC'

output('------ TRUNCATE(3.75) = 3 ?');
output(TRUNCATE(3.75));             // = 3

output('------ TRUNCATE(1.25) = 1 ?');
output(TRUNCATE(1.25));             // = 1

output('------ TRUNCATE(10.1) = 10 ?');
output(TRUNCATE(10.1));             // = 10

output('------ CASE COMMANDS = 9,8,7,5 ?');
output(CASE(1, 1 => 9, 2 => 8, 3 => 7, 4 => 6, 5)); // = 9
output(CASE(2, 1 => 9, 2 => 8, 3 => 7, 4 => 6, 5)); // = 8
output(CASE(3, 1 => 9, 2 => 8, 3 => 7, 4 => 6, 5)); // = 7
output(CASE(0, 1 => 9, 2 => 8, 3 => 7, 4 => 6, 5)); // = 5

output('------ CHOOSE COMMANDS = 7,3,5,13 ?');
output(CHOOSE(3,9,8,7,6,5));            // = 7
output(CHOOSE(3,1,2,3,4,5));            // = 3
output(CHOOSE(9,1,2,3,4,5));            // = 5  the default value
output(CHOOSE(3,15,14,13,12));      // = 13

output('------ REJECTED COMMANDS = 0,3,3,1 ?');
output(REJECTED(1=1,2=2,3=3,4=4,5=5));  // = 0
output(REJECTED(1=1,2=2,3=0,4=4,5=5));  // = 3
output(REJECTED(1=1,2=2,3=0,4=0,5=0));  // = 3
output(REJECTED(1=0,2=0,3=0,4=0,5=0));  // = 1

output('------ REJECTED COMMANDS = 5,2,1,0 ?');
output(WHICH(1=0,2=0,3=0,4=0,5=5));     // = 5
output(WHICH(1=0,2=2,3=0,4=4,5=0));     // = 2
output(WHICH(1=1,2=0,3=0,4=0,5=0));     // = 1
output(WHICH(1=0,2=0,3=0,4=0,5=0));     // = 0

// WARNING
// WARNING  If you change the values, or the number of records in this in-line
// WARNING  dataset you may break some or all of the subsequent test cases!
// WARNING

SomeRecord  := RECORD
   STRING1  Value1;
    STRING2 Value2;
    INTEGER4    Value3;
    INTEGER4    Value4;
END;

SomeFile    := DATASET([{'C','G',25,53},
                        {'C','C',71,15},
                         {'A','X',66,22},
                         {'B','G',102,97},
                         {'A','B',20,57}],SomeRecord);

output('------ AVE COMMANDS = 65.8,48.8 ?');
output(AVE(SomeFile,Value3));   // = 65.8
output(AVE(SomeFile,Value4));   // = 48.8

output('------ MIN COMMANDS = 20,15 ?');
output(MIN(SomeFile,Value3));   // = 20
output(MIN(SomeFile,Value4));   // = 15

output('------ MAX COMMANDS = 102,97 ?');
output(MAX(SomeFile,Value3));   // = 102
output(MAX(SomeFile,Value4));   // = 97

output('------ SUM COMMANDS = 284,244 ?');
output(SUM(SomeFile,Value3));   // = 284
output(SUM(SomeFile,Value4));   // = 244

output('------ COUNT COMMANDS = 5,1,2 ?');
output(COUNT(SomeFile));                    // = 5
output(COUNT(SomeFile(Value3 = 66)));   // = 1
output(COUNT(SomeFile(Value1 = 'C')));  // = 2

MapResult1  := MAP(EXISTS(SomeFile(Value1 = 'C')) => 1
                ,EXISTS(SomeFile(Value2 = 'X')) => 2
              ,3);

output(INTFORMAT(SomeFile[1].Value3,8,1));   // 25 = 00000025
output(INTFORMAT(SomeFile[2].Value3,8,1));   // 71 = 00000071
output(INTFORMAT(SomeFile[3].Value3,3,1));   // 66 = 066
output(INTFORMAT(SomeFile[4].Value3,3,1));   // 102= 102
output(INTFORMAT(SomeFile[3].Value3,1,1));   // 66 = * (field overflow)
output(INTFORMAT(SomeFile[4].Value3,1,1));   // 102= * (field overflow)
output(SomeFile[6].Value3);   // Out of range should equal 0
output(SomeFile[0].Value3);   // Out of range should equal 0
output(SomeFile[6].Value1);   // Out of range should equal ' '
output(SomeFile[0].Value1);   // Out of range should equal ' '
output(SomeFile[50000000000].Value1);   // Out of range should equal ' ', preferably quickly...
output(SomeFile[0x100000001].Value1);   // Out of range should equal ' ', preferably quickly...

