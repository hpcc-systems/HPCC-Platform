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

import $.setup;
prefix := setup.Files(false, false).FilePrefix;

//noRoxie
//nolocal

import Std;

#onwarning(10125, ignore); // ignore UPDATE 'up to date' messages, so that output is consistent across engines

rec := RECORD
  unsigned4 id;
  string100 str;
END;

unsigned num := 1000000;
ds := DATASET(num, TRANSFORM(rec, SELF.id := COUNTER; SELF.str := (string)HASH(COUNTER);), DISTRIBUTED);

p1 := PROJECT(ds, TRANSFORM(rec, SELF.id := LEFT.id*2; SELF := LEFT));
p2 := PROJECT(ds, TRANSFORM(rec, SELF.id := LEFT.id*3; SELF := LEFT));

gc1 := 0 : STORED('gc1');
ifp2 := IF(gc1=1, p2);

fname1 := prefix + 'splitout1';
fname2 := prefix + 'splitout2';
SEQUENTIAL(
 PARALLEL(
  OUTPUT(p1, , fname1, OVERWRITE, UPDATE);
  OUTPUT(p2, , fname2, OVERWRITE, UPDATE);
 );
 Std.File.DeleteLogicalFile(fname1);
 PARALLEL(
  OUTPUT(p1, , fname1, OVERWRITE, UPDATE);
  OUTPUT(p2, , fname2, OVERWRITE, UPDATE);
 );
 Std.File.DeleteLogicalFile(fname1);
 Std.File.DeleteLogicalFile(fname2);
);
