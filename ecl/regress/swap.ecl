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

#option ('optimizeGraph', false);
#option ('foldAssign', false);
#option ('globalFold', false);
personRecord := RECORD
LITTLE_ENDIAN unsigned8 l8;
LITTLE_ENDIAN unsigned4 l4;
LITTLE_ENDIAN unsigned2 l2;
LITTLE_ENDIAN unsigned1 l1;
BIG_ENDIAN    unsigned8 b8;
BIG_ENDIAN    unsigned4 b4;
BIG_ENDIAN    unsigned2 b2;
BIG_ENDIAN    unsigned1 b1;
string10      s10;
varstring10   v10;
    END;

personDataset := DATASET('person',personRecord,FLAT);

personRecord t1(personRecord l) := TRANSFORM
        SELF.l8 := l.b8;    // swap - direct?
        SELF.l4 := l.b2;    // swap, but need temporary
        SELF.l2 := l.l2;    // no need to swap
        SELF.l1 := l.b1;    // no need to swap
        SELF.b8 := l.l4 + l.l2; // swap after calc.
        SELF.b4 := l.b4;    // shouldn't need to swap...
        SELF.b2 := l.b8 * 2;    // swap twice...
        SELF.b1 := l.l1;    // no need to swap...
        SELF := l;
    END;

personRecord t2(personRecord l) := TRANSFORM
        SELF.l8 := transfer(l.b8, LITTLE_ENDIAN unsigned8); // copy no translate.
        SELF.l4 := transfer(l.b4 * l.b2, LITTLE_ENDIAN unsigned4);  // should swap 2times(!)
        SELF.l2 := (BIG_ENDIAN unsigned8)(l.b4 * l.b2); // should swap 3 times(!)
        SELF.b8 := l.b8 * l.b4; // need to swap on operation...
        SELF.b4 := l.b2;        //could try and be clever... (and shift result)
        SELF.b2 := l.l1;        //yuk...
        SELF.s10 := (string10)l.b8;
        SELF.v10 := (varstring10)l.b8;
        SELF := l;
    END;


personRecord t3(personRecord l) := TRANSFORM
        SELF.b8 := (big_endian unsigned4)(big_endian unsigned2)1;
        SELF.b4 := 1;
        SELF.b2 := (big_endian unsigned2)1;
        SELF.l8 := (big_endian unsigned4)(big_endian unsigned2)(big_endian unsigned4)(big_endian unsigned2)(1+1);
        SELF.s10 := (string)l.b8 + (string)l.l8;
        SELF.v10 := (varstring)l.b8 + '!';
        SELF := l;
    END;



o1 := project(personDataset, t1(LEFT));
o2 := project(o1, t2(LEFT));
o3 := project(o2, t3(LEFT));
output(o3,,'out1.d00');
