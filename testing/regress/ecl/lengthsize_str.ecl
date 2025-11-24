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

//Test the ability to configure the number of bytes used for a string's length
#onwarning(4523, ignore);

import $.setup;
import Std.File;

prefix := setup.Files(false, false).QueryFilePrefix;

testRecord := RECORD
    STRING name;
    STRING name1{LENGTHSIZE(1)};
    STRING name2{LENGTHSIZE(2)};
    STRING name4{LENGTHSIZE(4)};
    UNSIGNED1 sentinel;
END;

//Create strings with different length sizes
ds1 := NOFOLD(DATASET(100, TRANSFORM(testRecord,
            SELF.name  := (STRING)counter,
            SELF.name1 := (STRING)counter + 'x1',
            SELF.name2 := (STRING)counter + 'x2',
            SELF.name4 := (STRING)counter + 'x4',
            SELF.sentinel := 42;
        )));

// Assign so the sizes are known to be good - v1.
p1 := NOFOLD(PROJECT(ds1, TRANSFORM(testRecord,
            SELF.name  := LEFT.name + 'x0',
            SELF := LEFT // Assignment from the same length
        )));


// Assign so the sizes are known to be good - v12
p2 := NOFOLD(PROJECT(ds1, TRANSFORM(testRecord,
            SELF.name1 := TRIM(LEFT.name1, ALL), // can only reduce the length
            SELF.name2 := LEFT.name1 + 'x';      // assignment from a smaller string
            SELF := LEFT
        )));


i2 := INDEX(p2, { STRING10 search := p2.name }, { p2 }, prefix + 'lengthsize::strindex');
ds2 := DATASET(prefix + 'lengthsize::strfile', testRecord, THOR);

build(i2,OVERWRITE);
output(p2,, prefix + 'lengthsize::strfile',overwrite);
output(count(nofold(ds2)(sentinel = 42)));
output(count(nofold(i2)(sentinel = 42)));


// Now check that strings that are too long are truncated
singleRecord := RECORD
    STRING name1{LENGTHSIZE(1)};
END;

singleRecord t(STRING x) := TRANSFORM
    SELF.name1 := x;
END;

STRING300 longstring1 := 'xabc' + (STRING255)' ' + 'yz';
STRING longstring2 := 'xabc' + (STRING255)' ' + 'yz';
STRING longstring3 := 'xabc' + (STRING255)' ' + 'yz' : stored('longstring3');
STRING longstring4 := 'xabc' + (STRING253)' ' : stored('longstring4');
STRING longstring5 := longstring4 + 'yz';
STRING longstring6 := TRIM(longstring4) + 'yz';
STRING50 longstring7 := 'x' : stored('longstring7');
STRING longstring8 := TRIM(longstring7) + 'yz';


ds5 := DATASET([t(longstring1),
                t(longstring2),
                t(longstring3),
                t(longstring4),
                t(longstring5),
                t(longstring6),
                t(longstring7),
                t(longstring8)]);

output(nofold(ds5), { TRIM(name1), LENGTH(name1) }, all);

pxx1 := PROJECT(ds5, TRANSFORM(singleRecord,
            SELF.name1 := TRIM((STRING50)LEFT.name1) + 'xx';
        ));

output(nofold(pxx1), { TRIM(name1), LENGTH(name1) }, all);

File.DeleteLogicalFile(prefix + 'lengthsize::strindex');
File.DeleteLogicalFile(prefix + 'lengthsize::strfile');
