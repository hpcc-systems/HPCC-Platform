/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

person := dataset('person', { unsigned8 person_id, string2 per_st, string10 per_first_name, string10 per_last_name }, thor);
prevTable := person;
inputRecord :=
  record
    person.per_last_name;
    person.per_st;
  end;
inputTable := prevTable;

seedRecord :=
  record
    string25 per_last_name;
    string15 per_first_name;
    string42 per_street;
    string20 per_full_city;
    string4 per_abr_city;
    string2 per_st;
    string5 per_zip;
    string4 per_ssn;
    string9 per_cid;
  end;

seedTable := dataset('ecl_test::56recs_seed_file', seedRecord, flat);

//  Calc included states.
stateTable := table(inputTable, {per_st}, per_st);

//  Sequence Seed Table
seedSeqRecord :=
  record
    integer8 seqNum;
    seedRecord;
  end;

seedSeqRecord toSeq(seedTable L) :=
  transform
    self.seqNum := 0;
    self.per_st := L.per_st;
  end;

tmpA := join(seedTable, stateTable, left.per_st = right.per_st,
toSeq(left));

seedSeqRecord createSeqNum(seedSeqRecord L, seedSeqRecord R) :=
  transform
    self.seqNum := L.seqNum + 1;
    self := R;
  end;

seedSeqTable := iterate(tmpA, createSeqNum(left, right));

inputSeqRecord :=
  record
    integer8 seqNum;
    inputRecord;
  end;

inputPartialTable := choosen(table(inputTable, inputRecord), 56);

inputSeqRecord toSeqInput(inputRecord L) :=
  transform
    self.seqNum := 0;
    self := L;
  end;

tmpB := project(inputPartialTable, toSeqInput(left));
inputSeqRecord createSeqInput(inputSeqRecord L, inputSeqRecord R) :=
  transform
    self.seqNum := l.seqNum + 1;
    self := r;
  end;

inputPartialSeqTable := iterate(tmpB, createSeqInput(left, right));

inputSeqRecord seededTransform(inputPartialSeqTable L, seedSeqTable R)
:=
  transform
    self := r;
    self := l;
  end;

inputPartialSeedJoin := join(inputPartialSeqTable, seedSeqTable,
left.seqNum = right.seqNum, seededTransform(left, right));

output_seed := inputPartialSeedJoin + inputTable;

output(output_seed);
