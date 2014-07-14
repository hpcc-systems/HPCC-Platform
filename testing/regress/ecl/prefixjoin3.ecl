/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#option ('smallSortThreshold', 0);  // Stop all the records ending up on node 1

// limited prefix match join tests

rec := record
  unsigned      id;
  unsigned      sect;
  STRING2       s1;
  boolean       b1;
  STRING2       s2;
  unsigned      i1;
END;

outR := RECORD
    rec l;
    rec r;
END;

ds := DATASET([
    //Match less than 3 on s1[1]
    { 1, 1, 'AA', false, 'AA', 10 },
    { 2, 1, 'AZ', true, 'QA', 12 },
    //Match less than 3 on s1[2]
    { 3, 1, 'BA', false, 'AA', 10 },
    { 4, 1, 'BA', true, 'QA', 12 },
    { 5, 1, 'BA', true, 'QA', 12 },
    //Match less than 3 on s1, b1
    { 6, 1, 'BB', true, 'QA', 12 },
    { 7, 1, 'BB', true, 'QA', 12 },
    { 8, 1, 'BB', true, 'QA', 12 },
    //Match less than 3 on s1, b1, s2[1]
    { 9, 1, 'BB', false, 'AA', 12 },
    { 10, 1, 'BB', false, 'AB', 12 },
    { 11, 1, 'BB', false, 'AC', 12 },
    //Match less than 3 on s1, b1, s2
    { 12, 1, 'BB', false, 'BA', 1 },
    { 13, 1, 'BB', false, 'BA', 2 },
    { 14, 1, 'BB', false, 'BA', 3 },
    //Match less than 3 on s1, b1, s2, i1
    { 15, 1, 'BB', false, 'BB', 1 },
    { 16, 1, 'BB', false, 'BB', 1 },
    { 17, 1, 'BB', false, 'BB', 1 },
    //
    { 18, 1, 'BB', false, 'BB', 2 },
    { 19, 1, 'BB', false, 'BB', 2 },
    //
    { 20, 1, 'BB', false, 'BB', 3 }
    ], rec);


//Create duplicate sections
dds := NOFOLD(DISTRIBUTE(ds, RANDOM()));
sds := NOFOLD(NORMALIZE(dds, 2, TRANSFORM(rec, SELF.sect := COUNTER, SELF := LEFT)));

JT := JOIN(sds, sds, left.sect = right.sect,
            TRANSFORM(outR, SELF.l := LEFT; SELF.r := RIGHT), ATMOST({ left.s1[1..*]=right.s1[1..*], left.b1=right.b1, left.s2[1..*]=right.s2[1..*], left.i1=right.i1 },3));

OUTPUT(SORT(JT, l.sect, l.id, r.id), { l.sect, leftid := l.id, rightid := r.id });
