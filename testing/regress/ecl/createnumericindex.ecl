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

//Create a pathologically compressible index - especially with the new inplace compression
#onwarning (4523, ignore);

import std.system.thorlib;

r := { string10 key => unsigned value };
numRows := 2000000;
scale := 1;

ds := DATASET(numRows, TRANSFORM(r, SELF.key := INTFORMAT(COUNTER, 10, 1); SELF.value := COUNTER * scale), DISTRIBUTED);

engine := thorlib.platform();
filename := '~regress::index::numeric::' + engine;

numberIndex := INDEX(r, filename, compressed('inplace'));


compareFiles(ds1, ds2) := FUNCTIONMACRO
    c := COMBINE(ds1, ds2, transform({ boolean same, RECORDOF(LEFT) l, RECORDOF(RIGHT) r,  }, SELF.same := LEFT = RIGHT; SELF.l := LEFT; SELF.r := RIGHT ), LOCAL);
    RETURN output(choosen(c(not same), 10));
ENDMACRO;

s := PROJECT(ds, recordof(numberIndex));


ordered(
BUILD(numberIndex, ds, overwrite, LOCAL);
compareFiles(s, numberIndex);
);
