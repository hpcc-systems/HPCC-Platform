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

//class=file

import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

rec := RECORD
 unsigned4 id;
 string s;
END;

// want all counts to be same, because output order of firstn, will
// vary dependent on the configuration

duplicates := 10;
num := duplicates * 1000;

dsname := prefix + 'gafile';
indexname := prefix + 'gaindex';

inds := DATASET(num, TRANSFORM(rec, SELF.id := COUNTER % duplicates; SELF.s := (string)COUNTER));
ds := DATASET(dsname, rec, FLAT);
i := INDEX(ds, {id}, {ds}, indexname);

t1 := TABLE(ds, {id, c := COUNT(GROUP)}, id, FEW);
t2 := TABLE(i, {id, c := COUNT(GROUP)}, id, FEW);

SEQUENTIAL(
 PARALLEL(
  OUTPUT(inds, , dsname, OVERWRITE);
  BUILD(i, OVERWRITE);
 );
 PARALLEL(
  OUTPUT(CHOOSEN(TABLE(t1, {c}), 10));
  OUTPUT(CHOOSEN(TABLE(t2, {c}), 10));
 );
);
