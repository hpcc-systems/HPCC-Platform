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

//class=file
//class=index
//version forceRemoteKeyedLookup=false,forceRemoteKeyedFetch=false
//version forceRemoteKeyedLookup=true,forceRemoteKeyedFetch=true

import ^ as root;
forceRemoteKeyedLookup := #IFDEFINED(root.forceRemoteKeyedLookup, false);
forceRemoteKeyedFetch := #IFDEFINED(root.forceRemoteKeyedLookup, false);

#option('forceRemoteKeyedLookup', forceRemoteKeyedLookup);
#option('forceRemoteKeyedFetch', forceRemoteKeyedFetch);

#onwarning(4522, ignore);

rhsRec := RECORD
 unsigned4 key;
 string10 f1;
 string10 f2;
END;

rhsRecFP := RECORD(rhsRec)
 unsigned8 filePos{virtual(fileposition)};
END;


rhs := DATASET([{1, 'a', 'b'}, {1, 'a2', 'b2'}, {1, 'a3', 'b3'}, {1, 'a4', 'b4'}, {2, 'a2', 'd'}, {3, 'e', 'f'}], rhsRec);

lhsRec := RECORD
 unsigned4 someid;
 unsigned4 lhsKey;
END;

lhs := DISTRIBUTE(DATASET([{0, 1}, {0, 1}, {0,2}, {1, 1}, {1, 3}], lhsRec), someid);

rhsDs := DATASET('~REGRESS::'+WORKUNIT+'::rhsDs', rhsRecFP, FLAT);

i := INDEX(rhsDs, {key}, {f1, filePos}, '~REGRESS::'+WORKUNIT+'::rhsDs.idx');

rhsRec doHKJoinTrans(lhsRec l, RECORDOF(i) r) := TRANSFORM
 SELF.key := IF(l.lhsKey=1 AND r.f1='a', SKIP, l.lhsKey);
 SELF.f2 := 'blank';
 SELF := r;
END;
rhsRec doFKJoinTrans(lhsRec l, rhsRecFP r) := TRANSFORM
 SELF.key := IF(l.lhsKey=1 AND r.f1='a', SKIP, l.lhsKey);
 SELF := r;
END;


// a test with a expression on a lhs field - tests helper->leftCanMatch() handling
j1hk := JOIN(lhs, i, LEFT.lhsKey>1 AND LEFT.lhsKey=RIGHT.key);
j1fk := JOIN(lhs, rhsDs, LEFT.lhsKey>1 AND LEFT.lhsKey=RIGHT.key, TRANSFORM({lhsRec, rhsRec}, SELF := LEFT; SELF := RIGHT), KEYED(i));


// All the tests below with KEEP(2) have a filter with some kind to remove one of the '1' set matches.

// a test with a expression on a rhs index field - tests helper->indexReadMatch() handling
j2hk := JOIN(lhs, i, LEFT.lhsKey=RIGHT.key AND RIGHT.f1 != 'a2', KEEP(2));
j2fk := JOIN(lhs, rhsDs, LEFT.lhsKey=RIGHT.key AND RIGHT.f1 != 'a2', TRANSFORM({lhsRec, rhsRec}, SELF := LEFT; SELF := RIGHT), KEYED(i), KEEP(2));

// a test with a expression on a rhs fetch field - tests helper->fetchMatch() handling
j3fk := JOIN(lhs, rhsDs, LEFT.lhsKey=RIGHT.key AND RIGHT.f2 != 'b2', TRANSFORM({lhsRec, rhsRec}, SELF := LEFT; SELF := RIGHT), KEYED(i), KEEP(2));

// a test with a transform that skips
j4hk := JOIN(lhs, i, LEFT.lhsKey=RIGHT.key, doHKJoinTrans(LEFT, RIGHT), KEEP(2));
j4fk := JOIN(lhs, rhsDs, LEFT.lhsKey=RIGHT.key, doFKJoinTrans(LEFT, RIGHT), KEYED(i), KEEP(2));


// test lhs group preserved and group counts
j5 := TABLE(JOIN(GROUP(lhs, someid), i, LEFT.lhsKey=RIGHT.key, KEEP(2)), { lhsKey, COUNT(GROUP) }, FEW);

// test helper->getRowLimit, generated inside KJ by enclosing within LIMIT()
j6 := LIMIT(JOIN(lhs, i, LEFT.lhsKey=RIGHT.key, doHKJoinTrans(LEFT, RIGHT), KEEP(3)), 2, onFail(TRANSFORM(rhsRec, SELF.f1 := 'limit hit'; SELF := [])));


childFunc(unsigned v) := FUNCTION
 j := JOIN(lhs, i, v>20 AND v<80 AND LEFT.someid=RIGHT.key);
 RETURN IF(COUNT(j)>0, j[1].key, 0);
END;

parentDs := DATASET(100, TRANSFORM({unsigned4 id1; unsigned4 id2}, SELF.id1 := COUNTER; SELF.id2 := 0));
j7 := PROJECT(parentDs, TRANSFORM(RECORDOF(parentDs), SELF.id2 := childFunc(LEFT.id1); SELF := LEFT));
j7sumid2 := SUM(j7, id2);


SEQUENTIAL(
 OUTPUT(rhs, , '~REGRESS::'+WORKUNIT+'::rhsDs', OVERWRITE);
 BUILD(i, OVERWRITE);
 PARALLEL(
  OUTPUT(j1hk);
  OUTPUT(j1fk);
  OUTPUT(j2hk);
  OUTPUT(j2fk);
  OUTPUT(j3fk);
  OUTPUT(j4fk);

  OUTPUT(j5);
  OUTPUT(j6);

  OUTPUT(j7sumid2);
 );
);
