/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

//version optRemoteRead=false
//version optRemoteRead=true

#onwarning(4523, ignore);

import $.setup;
import ^ as root;
prefix := setup.Files(false, false).QueryFilePrefix;
optRemoteRead := #IFDEFINED(root.optRemoteRead, false);
#option('forceRemoteRead', optRemoteRead);


crec := RECORD
 unsigned ckey;
 string info := '';
END;

rec := RECORD
  unsigned key;
  unsigned key2;
  unsigned v;
  DATASET(crec) kkeys;
  string info := '';
END;


ds := DATASET([
 {1,2,1,[{991}]},
 {1,2,6,[{992}]},
 {1,2,7,[{993}]},
 {1,2,8,[{994}]},
 {1,2,9,[{995}]},
 {4,2,2,[{996}]},
 {4,2,3,[{997}]},
 {4,2,4,[{998}]},
 {4,2,5,[{999}]}
], rec);

i := INDEX(ds, {key, key2}, {ds}, prefix + 'index');

// NB: filter designed so result is not 1st keyed match.
fi := i(KEYED(key=1) AND v >= 6 AND v <= 8);

SEQUENTIAL(
 BUILD(i, FEW, OVERWRITE);

 OUTPUT(fi);
 OUTPUT(COUNT(fi));

// Hit limits
// indexread with LIMIT
 OUTPUT(LIMIT(fi, 2, ONFAIL(TRANSFORM(RECORDOF(fi), SELF.info := '**ROW LIMIT HIT IN INDEXREAD**'; SELF := []))));
 OUTPUT(LIMIT(fi, 4, KEYED, ONFAIL(TRANSFORM(RECORDOF(fi), SELF.info := '**KEYED LIMIT HIT IN INDEXREAD**'; SELF := []))));

// indexcount with limits. NB: if exceeded, count will be 1
 OUTPUT(COUNT(LIMIT(fi, 2, ONFAIL(TRANSFORM(RECORDOF(fi), SELF.info := '**ROW LIMIT HIT IN INDEXCOUNT**'; SELF := [])))));
 OUTPUT(COUNT(LIMIT(fi, 4, KEYED, ONFAIL(TRANSFORM(RECORDOF(fi), SELF.info := '**KEYED LIMIT HIT IN INDEXCOUNT**'; SELF := [])))));

// indexnormalize
 //OUTPUT(LIMIT(fi.kkeys(ckey>=992), 2, ONFAIL(TRANSFORM(RECORDOF(crec), SELF.info := '**ROW LIMIT HIT IN INDEXNORMALIZE**'; SELF := []))));


// Under limits
// indexread with LIMIT
 OUTPUT(LIMIT(fi, 3, ONFAIL(TRANSFORM(RECORDOF(fi), SELF.info := '**ROW LIMIT HIT IN INDEXREAD**'; SELF := []))));
 OUTPUT(LIMIT(fi, 5, KEYED, ONFAIL(TRANSFORM(RECORDOF(fi), SELF.info := '**KEYED LIMIT HIT IN INDEXREAD**'; SELF := []))));

// indexcount with limits. NB: if exceeded, count will be 1
 OUTPUT(COUNT(LIMIT(fi, 3, ONFAIL(TRANSFORM(RECORDOF(fi), SELF.info := '**ROW LIMIT HIT IN INDEXCOUNT**'; SELF := [])))));
 OUTPUT(COUNT(LIMIT(fi, 5, KEYED, ONFAIL(TRANSFORM(RECORDOF(fi), SELF.info := '**KEYED LIMIT HIT IN INDEXCOUNT**'; SELF := [])))));
 OUTPUT(EXISTS(fi));

// indexread with choosen
 OUTPUT(CHOOSEN(fi, 1));

// indexnormalize
 OUTPUT(LIMIT(fi.kkeys(ckey>=992), 3, ONFAIL(TRANSFORM(RECORDOF(crec), SELF.info := '**ROW LIMIT HIT IN INDEXNORMALIZE**'; SELF := []))));
 OUTPUT(CHOOSEN(fi.kkeys(ckey>=994), 1));
);
