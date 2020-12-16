/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#onwarning (4523, ignore);

import ^ as root;
optRemoteRead := #IFDEFINED(root.optRemoteRead, false);
#option('forceRemoteRead', optRemoteRead);

IMPORT STD;
import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;


addrRec := RECORD
 string addr;
END;

rec := RECORD
 string15 fname;
 string15 lname;
 unsigned age;
 DATASET(addrRec) addrs;
END;

thefname := prefix + 'indexlimits_file';
thefname_index := prefix + 'indexlimits_index';
inds := DATASET([{ 'Aaron', 'Jones', 100, [{'somewhere'}]}, {'Adam', 'Smith', 90, [{'somewhere'}]}, {'Bob', 'Brown', 80, [{'somewhere'}]}, {'Brian', 'Brown', 70, [{'somewhere'}]}, {'Charles', 'Dance', 60, [{'somewhere'}]}, {'Charles', 'Gould', 50, [{'somewhere'}]},  {'David', 'Brokenshire', 40, [{'somewhere'}]}, {'Edward', 'Green', 30, [{'somewhere'}]}, {'Egbert', 'Sillyname', 20, [{'somewhere'}]}, {'Freddy', 'Peters', 10, [{'somewhere'}]} ], rec, DISTRIBUTED);
i := INDEX(inds, { fname }, { inds }, thefname_index);

// NB: Index read row limit tests.
indexReadHitLocalLimit := LIMIT(i(lname = 'Brown'), 1, SKIP); // could hit limit locally. NB: no getRowLimit() generated.
indexReadHitGlobalLimit := LIMIT(i(age < 100), 8, SKIP); // on multinode cluster, will pick up limit globally.
indexReadHitOnFail := LIMIT(i(lname = 'Brown'), 1, ONFAIL(TRANSFORM(RECORDOF(i), SELF.fname := 'LIMIT EXCEEDED'; SELF.lname := 'LIMIT EXCEEDED'; SELF.age := 0; SELF.addrs := [])));
indexReadNoHitRowLimit := LIMIT(i(age < 30), 2, SKIP); // Skip should not be triggered.

// NB: Index keyed limit tests
indexReadHitKeyedLimit := LIMIT(i(fname = 'Charles'), 1, SKIP, KEYED); // could hit limit locally.
indexReadHitKeyedOnFail := LIMIT(i(fname = 'Charles'), 1, KEYED, ONFAIL(TRANSFORM(RECORDOF(i), SELF.fname := 'KEYED LIMIT EXCEEDED'; SELF.lname := 'KEYED LIMIT EXCEEDED'; SELF.age := 0; SELF.addrs := [])));
indexReadComboHitRowLimit := LIMIT(LIMIT(i(fname = 'Charles'), 2, SKIP, KEYED)(age >= 50), 1, SKIP); // keyed limit should not be triggered, but row limit should
indexReadNoHitKeyedLimit := LIMIT(i(fname = 'Charles'), 2, SKIP, KEYED); // Skip should not be triggered

// NB: Index read row limit tests on normalized index
normIndexReadHitLocalLimit := LIMIT(i(lname = 'Brown').addrs, 1, SKIP); // could hit limit locally. NB: no getRowLimit() generated.
normIndexReadHitGlobalLimit := LIMIT(i(age < 100).addrs, 8, SKIP); // on multinode cluster, will pick up limit globally.
normIndexReadHitOnFail := LIMIT(i(lname = 'Brown').addrs, 1, ONFAIL(TRANSFORM(RECORDOF(i.addrs), SELF.addr := 'LIMIT EXCEEDED')));
normIndexReadNoHitRowLimit := LIMIT(i(age < 30).addrs, 2, SKIP); // Skip should not be triggered.

// NB: Index keyed limit tests on normalized index
normIndexReadHitKeyedLimit := LIMIT(i(fname = 'Charles').addrs, 1, SKIP, KEYED); // could hit limit locally.
normIndexReadHitKeyedOnFail := LIMIT(i(fname = 'Charles').addrs, 1, KEYED, ONFAIL(TRANSFORM(RECORDOF(i.addrs), SELF.addr := 'KEYED LIMIT EXCEEDED')));
normIndexReadNoHitKeyedLimit := LIMIT(i(fname = 'Charles').addrs, 2, SKIP, KEYED); // Skip should not be triggered

SEQUENTIAL(
 OUTPUT(inds, , thefname, OVERWRITE);
 BUILDINDEX(i, SORTED, OVERWRITE);

 PARALLEL(
  /* NB: in Thor these generate a separate LIMIT activity after the index read
   * but getRowLimit() is not generated, which would be a useful optimization
   */
  OUTPUT(indexReadHitLocalLimit, NAMED('indexReadHitLocalLimit'));
  OUTPUT(indexReadHitGlobalLimit, NAMED('indexReadHitGlobalLimit'));
  OUTPUT(indexReadHitOnFail, NAMED('indexReadHitOnFail'));
  OUTPUT(indexReadNoHitRowLimit, NAMED('indexReadNoHitRowLimit'));
  OUTPUT(CHOOSEN(indexReadNoHitRowLimit, 1), NAMED('choosen_indexReadNoHitRowLimit')); // NB: choosen follows limit
 
  // NB: These COUNT's will generate separate aggregate activities, because they follow a generated LIMIT activity.
  OUTPUT(COUNT(indexReadHitLocalLimit), NAMED('count_indexReadHitLocalLimit'));
  OUTPUT(COUNT(indexReadHitGlobalLimit), NAMED('count_indexReadHitGlobalLimit'));
  OUTPUT(COUNT(indexReadNoHitRowLimit), NAMED('count_indexReadNoHitRowLimit'));
  OUTPUT(COUNT(CHOOSEN(indexReadNoHitRowLimit, 1)), NAMED('count_choosen_indexReadNoHitRowLimit'));
 
  OUTPUT(indexReadHitKeyedLimit, NAMED('indexReadHitKeyedLimit'));
  OUTPUT(indexReadHitKeyedOnFail, NAMED('indexReadHitKeyedOnFail'));

// Not correct in Thor/HThor because no LIMIT activity generated
//  OUTPUT(indexReadComboHitRowLimit, NAMED('indexReadComboHitRowLimit'));

  OUTPUT(indexReadNoHitKeyedLimit, NAMED('indexReadNoHitKeyedLimit'));
  OUTPUT(CHOOSEN(indexReadNoHitKeyedLimit, 1), NAMED('choosen_indexReadNoHitKeyedLimit'));
 
  OUTPUT(COUNT(indexReadHitKeyedLimit), NAMED('count_indexReadHitKeyedLimit'));

  OUTPUT(COUNT(indexReadComboHitRowLimit), NAMED('count_indexReadComboHitRowLimit'));

  OUTPUT(COUNT(indexReadNoHitKeyedLimit), NAMED('count_indexReadNoHitKeyedLimit'));
  OUTPUT(COUNT(CHOOSEN(indexReadNoHitKeyedLimit, 1)), NAMED('count_choosen_indexReadNoHitKeyedLimit'));

// normalize tests
  OUTPUT(normIndexReadHitLocalLimit, NAMED('normIndexReadHitLocalLimit'));
  OUTPUT(normIndexReadHitGlobalLimit, NAMED('normIndexReadHitGlobalLimit'));
  OUTPUT(normIndexReadHitOnFail, NAMED('normIndexReadHitOnFail'));
  OUTPUT(normIndexReadNoHitRowLimit, NAMED('normIndexReadNoHitRowLimit'));
  OUTPUT(CHOOSEN(normIndexReadNoHitRowLimit, 1), NAMED('choosen_normIndexReadNoHitRowLimit')); // NB: choosen follows limit
 
  OUTPUT(normIndexReadHitKeyedLimit, NAMED('normIndexReadHitKeyedLimit'));
  OUTPUT(normIndexReadHitKeyedOnFail, NAMED('normIndexReadHitKeyedOnFail'));
  OUTPUT(normIndexReadNoHitKeyedLimit, NAMED('normIndexReadNoHitKeyedLimit'));
  OUTPUT(CHOOSEN(normIndexReadNoHitKeyedLimit, 1), NAMED('choosen_normIndexReadNoHitKeyedLimit'));
 );
);
