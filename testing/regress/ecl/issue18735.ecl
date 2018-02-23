/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

rec1 := RECORD
  unsigned4 id;
END;

rec2 := RECORD
  unsigned c;
  dataset(rec1) child;
END;

numRecs := 10000;
mkchild(unsigned num, unsigned base) := DATASET(num, TRANSFORM(rec1, SELF.id := COUNTER + base));
ds := DATASET(numRecs, TRANSFORM(rec2, SELF.c := COUNTER, SELF.child := mkchild(COUNTER % 3, COUNTER % 7)));

s := SORT(ds, c);
gs := GROUP(s, c) : independent;
ngs := gs.child;
t := TABLE(ngs, { cnt := COUNT(GROUP) });
t2 := TABLE(ngs, { sm := SUM(GROUP, id) });

SEQUENTIAL(
    OUTPUT(TABLE(t, { cnt, num := COUNT(GROUP); }, cnt));
    OUTPUT(TABLE(t2, { sm, num := COUNT(GROUP); }, sm ));
    OUTPUT('"Done');
);
