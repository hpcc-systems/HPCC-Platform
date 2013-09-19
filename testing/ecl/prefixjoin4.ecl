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
  STRING6       s;
END;

outR := RECORD
    rec l;
    rec r;
END;

ds := DATASET([
    //Match less than 3 on s1[1]
    { 1, 1, 'AAFAA0' },
    { 2, 1, 'AZTQA2' },
    //Match less than 3 on s1[2]
    { 3, 1, 'BAFAA0' },
    { 4, 1, 'BATQA2' },
    { 5, 1, 'BATQA2' },
    //Match less than 3 on s1, b1
    { 6, 1, 'BBTQA2' },
    { 7, 1, 'BBTQA2' },
    { 8, 1, 'BBTQA2' },
    //Match less than 3 on s1, b1, s2[1]
    { 9, 1, 'BBFAA2' },
    { 10, 1, 'BBFAB2' },
    { 11, 1, 'BBFAC2' },
    //Match less than 3 on s1, b1, s2
    { 12, 1, 'BBFBA1' },
    { 13, 1, 'BBFBA2' },
    { 14, 1, 'BBFBA3' },
    //Match less than 3 on s1, b1, s2, i1
    { 15, 1, 'BBFBB1' },
    { 16, 1, 'BBFBB1' },
    { 17, 1, 'BBFBB1' },
    //
    { 18, 1, 'BBFBB2' },
    { 19, 1, 'BBFBB2' },
    //
    { 20, 1, 'BBFBB3' }
    ], rec);


//Create duplicate sections
dds := NOFOLD(DISTRIBUTE(ds, id));
sds := NOFOLD(NORMALIZE(dds, 7, TRANSFORM(rec, SELF.sect := COUNTER, SELF := LEFT)));

JT := JOIN(sds, sds, left.sect = right.sect AND left.s[1..*] = right.s[1..*],
            TRANSFORM(outR, SELF.l := LEFT; SELF.r := RIGHT), ATMOST(left.sect = right.sect AND left.s[1..*] = right.s[1..*],3));

OUTPUT(SORT(JT, l.sect, l.id, r.id), { l.sect, leftid := l.id, rightid := r.id });
