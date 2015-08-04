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


datafilename := '~jaketest::d2.d00';
idxfilename := '~jaketest::d2.idx';

baserec := RECORD
    STRING6 name;
    INTEGER6 blah;
    STRING9 value;
END;

_baseset := DATASET([{'fruit', 123, 'apple'}, {'car', 246, 'ford'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'book', 987, 'cch22'}], baserec);

baseset := DATASET([{'fruit', 123, 'apple'}, {'fruit', 246, 'ford'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'os', 987, 'os'}], baserec);

sort0 := SORT(baseset, name);
genbase := OUTPUT(sort0, , datafilename, OVERWRITE);

fpbaserec := RECORD
    baserec;
    UNSIGNED8 filepos{virtual(fileposition)};
END;

fpbaseset := DATASET(datafilename, fpbaserec, FLAT);

indexRec := RECORD
fpbaseset.name;
fpbaseset.filepos;
END;
idx := INDEX(fpbaseset, indexRec, idxfilename);

b1 := BUILDINDEX(idx, OVERWRITE);


// SEQUENTIAL(genbase, b1, OUTPUT(COUNT(idx)));
x := global(COUNT(idx));

baserec ff(baserec L) := TRANSFORM
    SELF.blah := x;
    SELF := L;
 END;
nx := PROJECT(baseset, ff(LEFT));

// OUTPUT(nx);
// SEQUENTIAL(genbase, b1);
SEQUENTIAL(genbase, b1, OUTPUT(nx));
// SEQUENTIAL(OUTPUT(nx));
