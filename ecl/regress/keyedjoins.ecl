/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#option ('globalFold', false);
#option ('targetClusterType', 'roxie');

rec := record
    unsigned4 value;
  END;

recplus := { rec, unsigned8 _fpos{virtual(fileposition)}};

recpair := record
    string20 name;
    unsigned4 leftvalue;
    unsigned4 rightvalue;
  END;

rec makeSequence(rec L, integer n) := TRANSFORM
    self.value := n;
  END;

rec makeDuplicate(rec L) := TRANSFORM
    self := L;
  END;

seed := dataset([{0}], rec);
seed100 := normalize(seed, 5, makeSequence(LEFT, COUNTER));
seed500 := normalize(seed100, LEFT.value, makeDuplicate(LEFT));
evens := seed500(value%2=0);

prepareFiles := PARALLEL(
  output(seed100,,'~rkc::file100', overwrite),
  output(seed500,,'~rkc::file500', overwrite),
  output(evens,,'~rkc::evens', overwrite));

file100 := dataset('~rkc::file100', recplus, FLAT);
file500 := dataset('~rkc::file500', recplus, FLAT);
fileevens := dataset('~rkc::evens', recplus, FLAT);

key100 := INDEX(file100, {value, _fpos}, '~rkc::key100');
key500 := INDEX(file500, {value, _fpos}, '~rkc::key500');
keyevens := INDEX(fileevens, {value, _fpos}, '~rkc::keyevens');

prepareKeys := PARALLEL(
  buildindex(key100, overwrite),
  buildindex(key500, overwrite),
  buildindex(keyevens, overwrite));

recpair makePair(rec L, keyevens R, string name) := TRANSFORM
    self.name := name;
    self.leftvalue := L.value;
    self.rightvalue := R.value;
  END;

recpair makePairSkip(rec L, keyevens R, string name) := TRANSFORM
    self.name := name;
    self.leftvalue := IF (L.value > 3, SKIP, L.value);
    self.rightvalue := R.value;
  END;

recpair makePairFK(rec L, recplus R, string name) := TRANSFORM
    self.name := name;
    self.leftvalue := L.value;
    self.rightvalue := R.value;
  END;

recpair makePairFKSkip(rec L, recplus R, string name) := TRANSFORM
    self.name := name;
    self.leftvalue := IF (L.value > 3, SKIP, L.value);
    self.rightvalue := R.value;
  END;

recpair makePairUK(rec L, rec R, string name) := TRANSFORM
    self.name := name;
    self.leftvalue := L.value;
    self.rightvalue := R.value;
  END;

recpair makePairUKSkip(rec L, rec R, string name) := TRANSFORM
    self.name := name;
    self.leftvalue := IF (L.value > 3, SKIP, L.value);
    self.rightvalue := R.value;
  END;

// Half keyed joins, no unkeyed filter....
halfkeyedjoins(result, input, name, filters='TRUE') := MACRO
result := SEQUENTIAL(
  output(dataset([{'Half keyed: ' + name}], {string80 __________________})),
  output(JOIN(input, keyEvens, left.value = right.value AND filters, makePair(left, right, 'inner'))),
  output(JOIN(input, keyEvens, left.value = right.value AND filters, makePair(left, right, 'left only'), LEFT ONLY)),
  output(JOIN(input, keyEvens, left.value = right.value AND filters, makePair(left, right, 'left outer'), LEFT OUTER)),
  output(JOIN(input, keyEvens, left.value = right.value AND filters, makePairSkip(left, right, 'skip'))),
  output(JOIN(input, keyEvens, left.value = right.value AND filters, makePairSkip(left, right, 'skip, left only'), LEFT ONLY)),
  output(JOIN(input, keyEvens, left.value = right.value AND filters, makePairSkip(left, right, 'skip, left outer'), LEFT OUTER)),
  output(JOIN(input, keyEvens, left.value = right.value AND filters, makePair(left, right, 'keep(2)'), KEEP(2))),
  output(JOIN(input, keyEvens, left.value = right.value AND filters, makePair(left, right, 'atmost(3))'), ATMOST(3))),
  output(choosen(JOIN(input, keyEvens, left.value = right.value AND filters, makePair(left, right, 'left outer'), LEFT OUTER),1))
)
ENDMACRO;

// Full keyed joins, no unkeyed filter....
fullkeyedjoins(result, input, name, filters='TRUE') := MACRO
result := SEQUENTIAL(
  output(dataset([{'Full keyed: ' + name}], {string80 __________________})),
  output(JOIN(input, fileEvens, left.value = right.value AND filters, makePairFK(left, right, 'inner'), KEYED(keyEvens))),
  output(JOIN(input, fileEvens, left.value = right.value AND filters, makePairFK(left, right, 'left only'), KEYED(keyEvens), LEFT ONLY)),
  output(JOIN(input, fileEvens, left.value = right.value AND filters, makePairFK(left, right, 'left outer'), KEYED(keyEvens), LEFT OUTER)),
  output(JOIN(input, fileEvens, left.value = right.value AND filters, makePairFKSkip(left, right, 'skip'), KEYED(keyEvens))),
  output(JOIN(input, fileEvens, left.value = right.value AND filters, makePairFKSkip(left, right, 'skip, left only'), KEYED(keyEvens), LEFT ONLY)),
  output(JOIN(input, fileEvens, left.value = right.value AND filters, makePairFKSkip(left, right, 'skip, left outer'), KEYED(keyEvens), LEFT OUTER)),
  output(JOIN(input, fileEvens, left.value = right.value AND filters, makePairFK(left, right, 'keep(2)'), KEYED(keyEvens), KEEP(2))),
  output(JOIN(input, fileEvens, left.value = right.value AND filters, makePairFK(left, right, 'atmost(3)'), KEYED(keyEvens), ATMOST(3))),
  output(choosen(JOIN(input, fileEvens, left.value = right.value AND filters, makePairFK(left, right, 'choosen'), KEYED(keyEvens), LEFT OUTER), 1))
)
ENDMACRO;

// unkeyed joins, no additional filter....
unkeyedjoins(result, input, name, filters='TRUE') := MACRO
result := SEQUENTIAL(
  output(dataset([{'Unkeyed: ' + name}], {string80 __________________})),
  output(JOIN(input, evens, left.value = right.value AND filters, makePairUK(left, right, 'inner'))),
  output(JOIN(input, evens, left.value = right.value AND filters, makePairUK(left, right, 'only'), LEFT ONLY)),
  output(JOIN(input, evens, left.value = right.value AND filters, makePairUK(left, right, 'outer'), LEFT OUTER)),
  output(JOIN(input, evens, left.value = right.value AND filters, makePairUKSkip(left, right, 'skip'))),
  output(JOIN(input, evens, left.value = right.value AND filters, makePairUKSkip(left, right, 'skip, left only'), LEFT ONLY)),
  output(JOIN(input, evens, left.value = right.value AND filters, makePairUKSkip(left, right, 'skip, left outer'), LEFT OUTER)),
  output(JOIN(input, evens, left.value = right.value AND filters, makePairUK(left, right, 'keep(2)'), KEEP(2))),
  output(JOIN(input, evens, left.value = right.value AND filters, makePairUK(left, right, 'atmost(3)'), ATMOST(3))),
  output(choosen(JOIN(input, evens, left.value = right.value AND filters, makePairUK(left, right, 'choosen'), LEFT OUTER), 1))
)
ENDMACRO;


halfkeyedjoins(halfkeyed, seed100, 'simple');
fullkeyedjoins(fullkeyed, seed100, 'simple');
unkeyedjoins(unkeyed, seed100, 'simple');

halfkeyedjoins(halfkeyedgrouped, group(seed100, value), 'grouped');
fullkeyedjoins(fullkeyedgrouped, group(seed100, value), 'grouped');
unkeyedjoins(unkeyedgrouped, group(seed100, value), 'grouped');

doPrepareFiles := false ;
doTestKeyedJoins := true;
doTestUnkeyedJoins := true;
doTestGroupedJoins := true;

SEQUENTIAL(
#if (doPrepareFiles)
  prepareFiles,
  prepareKeys,
#end
#if (doTestKeyedJoins)
  halfkeyed,
  fullkeyed,
#end
#if (doTestUnkeyedJoins)
  unkeyed,
#end
#if (doTestGroupedJoins)
#if (doTestKeyedJoins)
  halfkeyedgrouped,
  fullkeyedgrouped,
#end
#if (doTestUnkeyedJoins)
  unkeyedgrouped,
#end
#end
  output(dataset([{'Done'}], {string20 ________________}))
);




