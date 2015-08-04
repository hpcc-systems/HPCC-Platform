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
    unsigned        id;
                STRING          contents{maxlength(200)};
                STRING          contents2{maxlength(200)};
             END;

outRecord1 t1(inRecord l, outRecord1 r) := TRANSFORM
    SELF.id := l.box;
    SELF.contents := r.contents + ',' + l.text;
    SELF.contents2 := r.contents2 + ',' + l.text;
END;

output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT), LEFT.box, MERGE);
