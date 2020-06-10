/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
###############################################################################*/

#onwarning(5402, ignore);

import Std;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;


rec1 := RECORD
 UNSIGNED4 id;
 STRING s;
END;

rec2 := RECORD
 STRING s;
 UNSIGNED4 id;
END;

ds1 := DATASET(2, TRANSFORM(rec1, SELF.id := COUNTER; SELF.s := (STRING)SELF.id));
ds2 := DATASET(3, TRANSFORM(rec2, SELF.id := COUNTER; SELF.s := (STRING)SELF.id));

{unsigned c} doTrans({string lfn} l) := TRANSFORM
 ds := NOFOLD(DATASET(l.lfn, rec2, FLAT));
 SELF.c := COUNT(ds);
END;

fname1 := prefix + 'file1';
fname2 := prefix + 'file2';
p := PROJECT(DATASET([{fname1}, {fname2}], {string lfn}), doTrans(LEFT));


SEQUENTIAL(
 PARALLEL(
  OUTPUT(ds1, , fname1, OVERWRITE);
  OUTPUT(ds2, , fname2, OVERWRITE);
 );
 OUTPUT(p);
);
