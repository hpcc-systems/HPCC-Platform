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
    { 3, 1, 'BAAAAA' },
    { 4, 1, 'BAAAAB' },
    { 5, 1, 'BAAAAC' },
    { 6, 1, 'BAAAAD' },
    { 7, 1, 'BAAAAE' },
    { 8, 1, 'BAAAAF' },
    { 9, 1, 'BAAAAG' },
    { 10, 1, 'BAAAAH' },
    { 11, 1, 'BAAAAI' },
    { 12, 1, 'BAAAAJ' },
    { 13, 1, 'BAAAAK' },
    { 14, 1, 'BAAAAL' },
    { 15, 1, 'BAAAAM' },
    { 16, 1, 'BAAAAN' },
    { 17, 1, 'BAAAAO' },
    { 18, 1, 'BAAAAP' },
    { 19, 1, 'BAAAAQ' },
    { 20, 1, 'BAAAAR' },
    { 21, 1, 'BAAAAS' },
    { 22, 1, 'BAAAAT' }
    ], rec);


//Create duplicate sections
sds := NOFOLD(DISTRIBUTE(ds, id));

JT := JOIN(sds, sds, left.sect = right.sect AND left.s[1..*] = right.s[1..*],
            TRANSFORM(outR, SELF.l := LEFT; SELF.r := RIGHT), ATMOST(left.sect = right.sect AND left.s[1..*] = right.s[1..*],20));

OUTPUT(SORT(JT, l.sect, l.id, r.id), { l.sect, leftid := l.id, rightid := r.id });
