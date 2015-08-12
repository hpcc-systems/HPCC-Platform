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

inRecord :=
            RECORD
unsigned        box;
string          text{maxlength(10)};
            END;

inTable := dataset([
        {1, 'Shoe'},
        {1, 'Feather'},
        {1, 'Cymbal'},
        {1, 'Train'},
        {2, 'Envelope'},
        {2, 'Pen'},
        {2, 'Jumper'},
        {3, 'Dinosoaur'},
        {3, 'Dish'}
        ], inRecord);

outRecord1 := RECORD
                STRING          contents{maxlength(200)};
             END;

outRecord2 := RECORD(outRecord1)
    unsigned        id;
END;

//MORE: This could be optimized to just extend the field in place
transform(outRecord1) t1(inRecord l, outRecord1 r) := BEGINC++
    return 0;   //t1()
ENDC++;

transform(outRecord1) merge1(outRecord1 r1, outRecord1 r2) := BEGINC++
    return 0;   //merge...()
ENDC++;

transform(outRecord2) t2(inRecord l, outRecord2 r) := BEGINC++
    return 0;   //t2()
ENDC++;


o1 := output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT), LOCAL));
o2 := output(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box, LOCAL));
o3 := output(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box, FEW, LOCAL));
o4 := output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT), merge1(ROWS(RIGHT)[1], ROWS(RIGHT)[2])));

SEQUENTIAL(o1, o2, o3, o4);
