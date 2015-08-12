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
unsigned        mask;
            END;

inTable := dataset([
        {1, 0x11},
        {1, 0x33},
        {1, 0xDD},
        {1, 0x11},
        {2, 0x0F},
        {2, 0xF0},
        {2, 0x137F},
        {3, 0xf731},
        {3, 0x11FF}
        ], inRecord);

outRecord1 := RECORD
    unsigned        box;
    unsigned        maskor;
    unsigned        maskand := 0xffffffffff;
             END;

outRecord1 t1(inRecord l, outRecord1 r) := TRANSFORM
    SELF.box := l.box;
    SELF.maskor := r.maskor | l.mask;
    SELF.maskand := IF(r.box <> 0, r.maskand, 0xffffffff) & l.mask;
END;

output(AGGREGATE(inTable, outRecord1, t1(LEFT, RIGHT), LEFT.box));
