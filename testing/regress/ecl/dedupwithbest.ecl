/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#option ('testHashDedupSpillTimes',10);
MyRec := RECORD
    INTEGER3 Id;
    STRING10 Field1;
    STRING50 Field2;
END;

ds := DATASET([{'001','KC','G'},
               {'002','KC','Z'},
               {'003','KC','Z'},
               {'004','KC','C'},
               {'005','KA','X'},
               {'006','KB','A'},
               {'007','KB','G'},
               {'008','KA','B'},
               {'009','KC','Z'},
               {'010','KB','B'},
               {'011','KB','B'}],MyRec);

sorted_ds := SORT(ds, Field1);

Dedup1 := DEDUP(sorted_ds, Field1, BEST(Field2));
Dedup2 := DEDUP(sorted_ds, Field1, BEST(-Field2));
Dedup3 := SORT(DEDUP(ds, Field1, HASH, BEST(Field2)), Field1);

OUTPUT(dedup1, NAMED('BEST_Field2'));
OUTPUT(dedup2, NAMED('BEST_Field2_Reverse'));
OUTPUT(dedup3, NAMED('BEST_Field2_HASH'));

// Grouped dedup
gr1 := GROUP(sorted_ds, Field1);
gr1_sorted := SORT(gr1, Field2);
Dedupgr1 := DEDUP(gr1_sorted, field2, BEST(Id));
Dedupgr2 := DEDUP(gr1_sorted, field2, BEST(-Id));
OUTPUT(Dedupgr1, NAMED('GroupDedup'));
OUTPUT(Dedupgr2, NAMED('GroupDedup_Reverse'));

//Grouped HASH dedup
Dedupgr3 := SORT(DEDUP(gr1_sorted, field2, BEST(Id), HASH), Field2, Id);
Dedupgr4 := SORT(DEDUP(gr1_sorted, field2, BEST(-Id), HASH), Field2, Id);
OUTPUT(Dedupgr3, NAMED('GroupDedupHash'));
OUTPUT(Dedupgr4, NAMED('GroupDedupHash_Reverse'));

//Larger test
numRecords := 1000000;
// Generate DS set so that
// 1) Id fields values are from 1..1000000
// 2) Field1 fields are from K0..K49999
// 3) Field2 fields are from 0..19
createIds(unsigned n) := NOFOLD(DATASET(n, TRANSFORM(MyRec, SELF.id := COUNTER , SELF.Field1:='K' + (STRING) (COUNTER % 50000), SELF.Field2:=(STRING)((COUNTER-1) DIV 50000) ), DISTRIBUTED));

generatedDS := createIds(numRecords);

// The expected result willl be (if dedup is working)
// 1) Id field has value 1..50000
// 2) field1 fields should have value 'K0' to 'K49999'
// 3) field2 fields should be '0' (because this is the 'best' field)
// 4) field2 should be the same as field1 prefixed with 'K', other than for Id field 50000 where field1 should be K0
// 5) There should be 50K rows
dedupds := DEDUP(generatedDS, Field1, HASH, BEST(field2));
OUTPUT( COUNT(dedupds), NAMED('DEDUP_COUNT') );

d := ENTH(SORT(dedupds,Id),20);
OUTPUT( d, NAMED('DEDUP_SAMPLE') ) ;

