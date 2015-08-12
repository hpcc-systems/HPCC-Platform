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
                STRING          contents2{maxlength(200)};
             END;

outRecord2 := RECORD(outRecord1)
    unsigned        id;
    '\n';
END;

outRecord3 := RECORD
    unsigned        id;
    outRecord1;
    '\n';
END;

outRecord4 := RECORD
    unsigned        id;
    '\n';
    unsigned4       magiclen;
    '\n';
    unsigned        maxlen;
    '\n';
END;

//MORE: This could be optimized to just extend the field in place
outRecord1 t1(inRecord l, outRecord1 r) := TRANSFORM
    SELF.contents := r.contents + l.text + ' ';
    SELF.contents2 := r.contents2 + l.text[1..2] + ' ';
END;

outRecord2 t2(inRecord l, outRecord2 r) := TRANSFORM
    SELF.contents := r.contents + l.text + ' ';
    SELF.contents2 := r.contents2 + l.text[1..2] + ' ';
    SELF.id := l.box;
END;

outRecord3 t3(inRecord l, outRecord3 r) := TRANSFORM
    SELF.contents := r.contents + l.text + ' ';
    SELF.contents2 := l.text[1..2] + ' ' + r.contents2;
    SELF.id := l.box;
END;

outRecord4 t4(inRecord l, outRecord4 r) := TRANSFORM
    SELF.magiclen := 10 + r.magiclen + LENGTH(TRIM(l.text));
    SELF.maxlen := MAX(8, r.maxlen, LENGTH(TRIM(l.text)));
    SELF.id := l.box;
END;

o1 := output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT)));
o2 := output(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box));
o3 := output(sort(nofold(AGGREGATE(inTable, outRecord2, t2(LEFT, RIGHT), LEFT.box, FEW)),id));
o4 := output(sort(nofold(AGGREGATE(inTable, outRecord3, t3(LEFT, RIGHT), LEFT.box, FEW)),id));
o5 := output(sort(nofold(AGGREGATE(inTable, outRecord4, t4(LEFT, RIGHT), LEFT.box, FEW)),magiclen));

SEQUENTIAL(output('o1\n'),o1,output('\no2\n'),o2,output('\no3\n'),o3,output('\no4\n'),o4,output('\no5\n'),o5);
