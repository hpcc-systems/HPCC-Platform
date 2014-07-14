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

//
// DIVISION
//
OUTPUT('Executed:   0/0 = 0?');
OUTPUT(0/0);
OUTPUT('Executed:   1/0 = 0?');
OUTPUT(1/0);
OUTPUT('Executed:   1 / 1 = 1?');
OUTPUT(1/1);
OUTPUT('Executed:   1 / 2 = 0.5?');
OUTPUT(1/2);
OUTPUT('Executed:   2 / 1 = 2?');
OUTPUT(2/1);
OUTPUT('Executed:   2 / 2 = 1?');
OUTPUT(2/2);
OUTPUT('Executed:   2 / 3 = 0.666667?');
OUTPUT(round(2/3,6));
OUTPUT('Executed:   3 / 2 = 1.5?');
OUTPUT(3/2);
OUTPUT('Executed:   3.5 / 0 = 0?');
OUTPUT(3.5/0);
OUTPUT('Executed:   3.5 / 1.5 = 2.33333?');
OUTPUT(round(3.5/1.5,6));
//
// INTEGER DIVISION
//
OUTPUT('Executed:   1 DIV 0 = 0?');
OUTPUT(1 DIV 0);
OUTPUT('Executed:   1 DIV 1 = 1?');
OUTPUT(1 DIV 1);
OUTPUT('Executed:   2 DIV 1 = 2?');
OUTPUT(2 DIV 1);
OUTPUT('Executed:   2 DIV 2 = 2?');
OUTPUT(2 DIV 2);
OUTPUT('Executed:   3 DIV 2 = 1?');
OUTPUT(3 DIV 2);
OUTPUT('Executed:   (INTEGER)3.5 DIV (INTEGER)1.5 = 3?');
OUTPUT((INTEGER)3.5 DIV (INTEGER)1.5); 
//
// MODULUS DIVISION
//
//OUTPUT('Executed:   1 % 0 = 0?');
//OUTPUT(1%0);
OUTPUT('Executed:   0 % 1 = 0?');
OUTPUT(0%1);
OUTPUT('Executed:   1 % 1 = 0?');
OUTPUT(1%1);
OUTPUT('Executed:   7 % 2 = 1?');
OUTPUT(7%2);
OUTPUT('Executed:   8 % 3 = 2?');
OUTPUT(8%3);
OUTPUT('Executed:  -8 % 3 = -2?');
OUTPUT(-8%3);
OUTPUT('Executed:   3 % 8 = 3?');
OUTPUT(3%8);
//
// MULTIPLICATION
//
OUTPUT('Executed:   0 * 0 = 0?');
OUTPUT(0*0);
OUTPUT('Executed:   0 * 1 = 0?');
OUTPUT(0*1);
OUTPUT('Executed:   1 * 0 = 0?');
OUTPUT(1*0);
OUTPUT('Executed:   1 * 1 = 0?');
OUTPUT(1*1);
OUTPUT('Executed:  -1 * -1 = 1?');
OUTPUT(-1*-1);
OUTPUT('Executed:   0 * -1 = 0?');
OUTPUT(0*-1);
OUTPUT('Executed:  -1 * 0 = 0?');
OUTPUT(-1*0);
OUTPUT('Executed:  -.5 * .5 = -.025?');
OUTPUT(-.5*.5);
OUTPUT('Executed:  -1.5 * 1.5 = -2.25?');
OUTPUT(-1.5*1.5);
OUTPUT('Executed:  -1.5 * 1.5 / 2.25 = -1?');
OUTPUT(-1.5*1.5/2.25);
//
// BITWISE
//
OUTPUT('Executed:   1 & 1 = 1?');
OUTPUT(1 & 1);
OUTPUT('Executed:   1 & 0 = 0?');
OUTPUT(1 & 0);
OUTPUT('Executed:   1 | 1 = 1?');
OUTPUT(1 | 1);
OUTPUT('Executed:   1 | 0 = 0?');
OUTPUT(1 | 0);
OUTPUT('Executed:   1 ^ 1 = 0?');
OUTPUT(1 ^ 1);
OUTPUT('Executed:   1 ^ 0 = 1?');
OUTPUT(1 ^ 0);
//
// COMPARISON
//
OUTPUT('Executed:   1 = 1 = TRUE?');
OUTPUT(1 = 1);
OUTPUT('Executed:   1 = 0 = FALSE?');
OUTPUT(1 = 0);
OUTPUT('Executed:   1 <> 1 = FALSE?');
OUTPUT(1 <> 1);
OUTPUT('Executed:   1 <> 0 = TRUE?');
OUTPUT(1 <> 0);
OUTPUT('Executed:   1 != 1 = FALSE?');
OUTPUT(1 != 1);
OUTPUT('Executed:   1 != 0 = TRUE?');
OUTPUT(1 != 0);
OUTPUT('Executed:   1 < 0 = FALSE?');
OUTPUT(1 < 0);
OUTPUT('Executed:   0 < 1 = TRUE?');
OUTPUT(0 < 1);
OUTPUT('Executed:   -1 < 0 = TRUE?');
OUTPUT(-1 < 0);
OUTPUT('Executed:   -1 < -0 = TRUE?');  // trick question: -0
OUTPUT(-1 < -0);
OUTPUT('Executed:   1 > 0 = TRUE?');
OUTPUT(1 > 0);
OUTPUT('Executed:   0 > 1 = FALSE?');
OUTPUT(0 > 1);
OUTPUT('Executed:   -1 > 0 = FALSE?');
OUTPUT(-1 > 0);
OUTPUT('Executed:   -1 > -0 = FALSE?');  // trick question: -0
OUTPUT(-1 > -0);
OUTPUT('Executed:   1 <=> 0 =  1?');
OUTPUT(1 <=> 0);
OUTPUT('Executed:   1 <=> 1 =  0?'); 
OUTPUT(1 <=> 1);
OUTPUT('Executed:   0 <=> 1 = -1?');
OUTPUT(0 <=> 1);
//
// LOGICAL
//
OUTPUT('Executed:   TRUE AND TRUE = TRUE?');
OUTPUT(TRUE AND TRUE);
OUTPUT('Executed:   TRUE AND FALSE = FALSE?');
OUTPUT(TRUE AND FALSE);
OUTPUT('Executed:   FALSE AND FALSE = FALSE?');
OUTPUT(FALSE AND FALSE);
OUTPUT('Executed:   TRUE OR TRUE = TRUE?');
OUTPUT(TRUE OR TRUE);
OUTPUT('Executed:   TRUE OR FALSE = TRUE?');
OUTPUT(TRUE OR FALSE);
OUTPUT('Executed:   FALSE OR FALSE = FALSE?');
OUTPUT(FALSE OR FALSE);
OUTPUT('Executed:   NOT TRUE = FALSE?');
OUTPUT(NOT TRUE);
OUTPUT('Executed:   NOT FALSE = TRUE?');
OUTPUT(NOT FALSE);
//
// BETWEEN
//
OUTPUT('Executed:   15 BETWEEN 10 AND 20 = TRUE?');
OUTPUT(15 BETWEEN 10 AND 20);
OUTPUT('Executed:   15 BETWEEN  0 AND 10 = FALSE?');
OUTPUT(15 BETWEEN  0 AND 10);
OUTPUT('Executed:   15 NOT BETWEEN 10 AND 20 = FALSE?');
OUTPUT(15 NOT BETWEEN 10 AND 20);
OUTPUT('Executed:   15 NOT BETWEEN  0 AND 10 = TRUE?');
OUTPUT(15 NOT BETWEEN  0 AND 10);

