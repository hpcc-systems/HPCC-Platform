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

outRecord2 := RECORD
    unsigned        id;
    IFBLOCK(SELF.id <> 0)
        outRecord1;
    END;
    '\n';
END;

//MORE: This could be optimized to just extend the field in place
outRecord1 t1(inRecord l, outRecord1 r) := TRANSFORM
    SELF.contents := r.contents + l.text + ' ';
END;

outRecord2 t2(inRecord l, outRecord2 r) := TRANSFORM
    SELF.contents := r.contents + l.text + ' ';
    SELF.id := l.box;
END;

o1 := output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT), LOCAL));
o2 := output(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box, LOCAL));
o3 := output(sort(nofold(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box, FEW, LOCAL)),id));

SEQUENTIAL(output('o1\n'),o1,output('\no2\n'),o2,output('\no3\n'),o3);
