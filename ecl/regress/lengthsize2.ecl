/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

testRecord := RECORD
    UTF8 name;
    UTF8 name1{LENGTHSIZE(1)};
    UTF8 name2{LENGTHSIZE(2)};
    UTF8 name4{LENGTHSIZE(4)};
END;

//Create UTF8s with different length sizes
ds1 := NOFOLD(DATASET(100, TRANSFORM(testRecord,
            SELF.name  := (UTF8)counter,
            SELF.name1 := (UTF8)counter + 'x1',
            SELF.name2 := (UTF8)counter + 'x2',
            SELF.name4 := (UTF8)counter + 'x4'
        )));

// Assign so the sizes are known to be good - v1.
p1 := NOFOLD(PROJECT(ds1, TRANSFORM(testRecord,
            SELF.name  := LEFT.name + 'x0',
            SELF := LEFT // Assignment from the same length
        )));


// Assign so the sizes are known to be good - v12
p2 := NOFOLD(PROJECT(ds1, TRANSFORM(testRecord,
            SELF.name1 := TRIM(LEFT.name1, ALL), // can only reduce the length
            SELF.name2 := LEFT.name1 + 'x';      // assignment from a smaller UTF8
            SELF := LEFT
        )));


build(p2, { STRING10 search := (STRING10)p2.name }, { p2 }, 'myindex');
