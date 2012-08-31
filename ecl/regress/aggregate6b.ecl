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

itemRecord := RECORD
string          text{maxlength(10)};
            END;

outRecord := RECORD
unsigned        box;
dataset(itemRecord) items{embedded};
             END;

outRecord t1(inRecord l, outRecord r) := TRANSFORM
    SELF.box := l.box;
    SELF.items := r.items + row(TRANSFORM(itemRecord, SELF.text := l.text));
END;

groupedByBox := group(inTable, box);
output(AGGREGATE(groupedByBox, outRecord, t1(LEFT, RIGHT)));
