/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#onwarning(4523, ignore);

import Std.File AS FileServices;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

num := 10;
pnum := 3;

payloadRec := RECORD
 UNSIGNED8 p1;
END;
rec := RECORD
 UNSIGNED4 id;
 DATASET(payloadRec) pl;
END;

d := DATASET(num, TRANSFORM(rec, SELF.id := COUNTER; SELF.pl := DATASET(pnum, TRANSFORM(payloadRec, SELF.p1 := -COUNTER))));

i := INDEX(d, {id}, {d}, prefix + 'idx');

payloadRec2 := RECORD
 STRING p1;
END;
rec2 := RECORD
 UNSIGNED4 id;
 DATASET(payloadRec2) pl;
END;

d2 := DATASET([], rec2);
i2 := INDEX(d2, {id}, {d2}, prefix + 'idx');

SEQUENTIAL(
 OUTPUT(d, , prefix + 'ds', OVERWRITE);
 BUILDINDEX(i, OVERWRITE);
 OUTPUT(i2);
);
