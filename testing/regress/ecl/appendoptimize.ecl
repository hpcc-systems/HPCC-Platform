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

r := { unsigned id; };


mkId(unsigned num) := TRANSFORM(r, SELF.id := num);

ds(unsigned num, unsigned from) := NOFOLD(DATASET(num, mkId(from + (COUNTER-1))));

ds11 := ds(1,1);
ds22 := ds(2,2);

boolean true1 := true : stored('true1');
boolean false1 := false : stored('false1');

SEQUENTIAL(
    output(IF(true1, ds11 & ROW(mkId(12)), ds11));
    output(IF(true1, ds11, ds11 & ROW(mkId(12))));
    output(IF(true1, ds11 & ds22, ds11 & ROW(mkId(12))));
    output(IF(false1, ds11 & ROW(mkId(12)), ds11));
    output(IF(false1, ds11, ds11 & ROW(mkId(12))));
    output(IF(false1, ds11 & ds22, ds11 & ROW(mkId(12))))
);
