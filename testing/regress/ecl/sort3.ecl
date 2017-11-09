/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

import $.setup;
prefix := setup.Files(false, false).IndexPrefix;

r1 := { unsigned id };
MyRec := RECORD
    UNSIGNED2 uv;
    STRING10   sv;
END;
MyRec2 := RECORD(myRec)
    DATASET(r1) child;
END;

SomeFile := DATASET([{0x001,'GAVIN'},
                     {0x301,'JAMES'},
                     {0x301,'ABSALOM'},
                     {0x301,'BARNEY'},
                     {0x301,'CHARLIE'},
                     {0x301,'JETHROW'},
                     {0x001,'CHARLIE'},
                     {0x301,'DANIEL'},
                     {0x201,'Jim'}
                    ],MyRec);

p := PROJECT(SomeFile, TRANSFORM(myRec2, SELF := LEFT; SELF := []));
p2 := PROJECT(SomeFile, TRANSFORM(myRec, SELF.uv := LEFT.uv - 0x200; SELF := LEFT));
s2 := SORT(p2, uv);

sequential(
    output(SORT(SomeFile, uv)), // needs to be stable
    output(SORT(SomeFile, (unsigned1)uv)), // needs to be stable
    output(SORT(SomeFile, (unsigned1)uv,sv)), // needs to be stable
    output(SORT(SomeFile, (unsigned1)uv,sv,uv)), // can be unstable
    output(SORT(SomeFile, trim(sv),uv)), // can be unstable
    output(SORT(SomeFile, (string20)sv,(unsigned4)uv)), // can be unstable
    buildindex(NOFOLD(SomeFile), { uv }, { SomeFile }, prefix + 'dummyIndex1',overwrite);
    buildindex(NOFOLD(p), { uv }, { p }, prefix + 'dummyIndex2',overwrite);
    output(SORT(s2, (integer2)uv));
    output('done')
);    
