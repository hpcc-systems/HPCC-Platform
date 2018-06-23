/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//Test serialization of various record/dataset types, including default values

s := service
   string dumpRecordType(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='dumpRecordType',fold;
   string dumpRecordTypeNF(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='dumpRecordType';
   data serializeRecordType(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='serializeRecordType',fold;
   data serializeRecordTypeNF(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='serializeRecordType';
end;

//Simplest case (and only one likely to need!)
r1 := RECORD
    UNSIGNED id;
    IFBLOCK(SELF.id = 4)
        UNSIGNED extra;
    END;
END;

//More complicated use of and or and equalities
r2 := RECORD
    UNSIGNED id;
    IFBLOCK((SELF.id > 4 AND SELF.id < 10) OR (SELF.id >= 200 AND SELF.id <= 250))
        UNSIGNED extra;
    END;
END;

//Example that simplifies to >= 200 and < 1000
r3 := RECORD
    UNSIGNED id;
    IFBLOCK((SELF.id > 4 AND SELF.id < 1000) AND (SELF.id >= 200 AND SELF.id <= 2500))
        UNSIGNED extra;
    END;
END;

//Check set syntax works
r4 := RECORD
    UNSIGNED id;
    IFBLOCK(SELF.id IN [1,2,3,99,1000])
        UNSIGNED extra;
    END;
END;

//This example does not currently work since the expression folds to TRUE
r5 := RECORD
    UNSIGNED id;
    IFBLOCK(SELF.id IN ALL)
        UNSIGNED extra;
    END;
END;

//Test with strings
r6 := RECORD
    STRING2 id;
    IFBLOCK(SELF.id IN ['a','b','c','xx'] OR (SELF.id >= 'f' AND SELF.id < 'k'))
        UNSIGNED extra;
    END;
END;

//Range testing - variable length strings
r7 := RECORD
    STRING id;
    IFBLOCK(SELF.id IN ['\'', 'a','b ','bxx'] OR (SELF.id >= 'faa' AND SELF.id <= 'kaa') OR (SELF.id > 'xxa' AND SELF.id < 'xyz'))
        UNSIGNED extra;
    END;
END;

//Range testing - some values out of range
r8 := RECORD
    STRING2 id;
    IFBLOCK(SELF.id IN ['a','b ','bxx'] OR (SELF.id >= 'faa' AND SELF.id <= 'kaa') OR (SELF.id > 'xxa' AND SELF.id < 'xyz') OR (SELF.id >= 'zfa' AND SELF.id <= 'zfx'))
        UNSIGNED extra;
    END;
END;

//Range testing - some values out of range
r9 := RECORD
    STRING2 id;
    IFBLOCK(SELF.id between 'faa' AND 'kaa')
        UNSIGNED extra;
    END;
END;

//Range testing - some values out of range
r10 := RECORD
    STRING2 id;
    IFBLOCK(SELF.id = NOFOLD('a') OR SELF.id = NOFOLD('b') OR SELF.id = NOFOLD('bxx'))
        UNSIGNED extra;
    END;
END;

r11 := RECORD
    STRING2 id;
    IFBLOCK((SELF.id >= NOFOLD('axx') AND SELF.id <= NOFOLD('bxx')) OR (SELF.id > NOFOLD('dxx') AND SELF.id <= NOFOLD('exx')))
        UNSIGNED extra;
    END;
END;

//This cannot create a simple ifblock since it would require a post filter check.  Could possibly truncate and adjust the test conditions....
r12 := RECORD
    integer id;
    IFBLOCK((SELF.id >= 12.23 AND SELF.id <= 14.67) OR (SELF.id > 98.74 AND SELF.id <= 123.4))
        UNSIGNED extra;
    END;
END;

r13 := RECORD
    STRING2 id;
    IFBLOCK(SELF.id IN [NOFOLD('a'),NOFOLD('b '),NOFOLD('bxx')])
        UNSIGNED extra;
    END;
END;

//Same as r1 - check that it works with reversed operands
r14 := RECORD
    UNSIGNED id;
    IFBLOCK(4 = SELF.id)
        UNSIGNED extra;
    END;
END;

//Same as r2 - check reversed operands
r15 := RECORD
    UNSIGNED id;
    IFBLOCK((4 < SELF.id AND 10 > SELF.id) OR (200 <= SELF.id AND 250 >= SELF.id))
        UNSIGNED extra;
    END;
END;

TEST(REC) := MACRO
s.dumpRecordType(_empty_(REC)[1]);
s.dumpRecordTypeNF(_empty_(REC)[1]);
s.dumpRecordType(_empty_(REC)[1]) = s.dumpRecordTypeNF(_empty_(REC)[1]);
s.serializeRecordType(_empty_(REC)[1]) = s.serializeRecordTypeNF(_empty_(REC)[1]);
ENDMACRO;

TEST(r1);
TEST(r2);
TEST(r3);
TEST(r4);
//TEST(r5);         //This example does not currently work since the expression folds to TRUE
TEST(r6);
TEST(r7);
TEST(r8);
TEST(r9);
TEST(r10);
TEST(r11);
//TEST(r12);        // currently too complex
TEST(r13);
TEST(r14);
TEST(r15);
