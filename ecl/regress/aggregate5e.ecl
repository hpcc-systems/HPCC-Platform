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

outRecord4 := RECORD
    unsigned        id;
    '\n';
    unsigned4       magiclen;
    '\n';
    unsigned        maxlen;
    '\n';
END;


outRecord4 t4(inRecord l, outRecord4 r) := TRANSFORM
    SELF.magiclen := 10 * r.magiclen * LENGTH(TRIM(l.text));
    SELF.maxlen := SUM(8, r.maxlen, LENGTH(TRIM(l.text)));
    SELF.id := l.box;
END;

o1 := output(sort(nofold(AGGREGATE(inTable, outRecord4, t4(LEFT, RIGHT), LEFT.box, FEW)),magiclen));

SEQUENTIAL(output('o1\n'),o1);
