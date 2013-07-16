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


// limited prefix match join tests

recL := record
    unsigned id;
    STRING10 name;
  unsigned      id2;
  unsigned  val;
END;

recR := record
  unsigned      id;
  STRING        name {MAXLENGTH(12)};
  unsigned      id2;
  unsigned2     val;
END;

outR := RECORD
    recL l;
    recR r;
END;

dsL := DATASET([
    { 1, 'ABAAA', 3, 0 },
    { 1, 'ABAAA', 3, 3 },
    { 1, 'ACAAB', 3, 0 },
    { 1, 'ABAAA', 4, 0 },
    { 1, 'FAAAA', 1, 0 },
    { 2, 'ABAAB', 1, 0 }
    ], recL);

dsR := DATASET([
    { 1, 'ABAAA', 1, 1 },   // atmost ok if including id2
    { 1, 'ABAAA', 2, 2 },
    { 1, 'ABAAA', 3, 3 },
    { 1, 'ABAAA', 4, 4 },
    { 1, 'ACAAA', 1, 5 },   //atmost ok if including str[name[1..4]
    { 1, 'ACAAB', 1, 6 },
    { 1, 'ACAAC', 1, 7 },
    { 1, 'ACAAD', 1, 8 },
    { 1, 'DAAAA', 1, 9 },
    { 1, 'EAAAA', 1, 9 },
    { 1, 'FAAAA', 1, 9 },
    { 2, 'ABAAA', 1, 1 },   // atmost ok if including id2
    { 2, 'ABAAA', 2, 2 },
    { 2, 'ABAAA', 3, 3 },
    { 2, 'ABAAA', 4, 4 },
    { 2, 'ACAAA', 1, 5 },   //atmost ok if including str[name[1..4]
    { 2, 'ACAAB', 1, 6 },
    { 2, 'ACAAC', 1, 7 },
    { 2, 'ACAAD', 1, 8 },
    { 2, 'DAAAA', 1, 9 },
    { 2, 'EAAAA', 1, 9 },
    { 2, 'FAAAA', 1, 9 }
    ], recR);


sdsL := NOFOLD(dsL);
sdsR := NOFOLD(dsR);

JT := JOIN(sdsL, sdsR, left.id = right.id AND left.name[1..*]=right.name[1..*] and left.val<=right.val,
            TRANSFORM(outR, SELF.l := LEFT; SELF.r := RIGHT), ATMOST(left.name[1..*]=right.name[1..*],3));

OUTPUT(JT);
