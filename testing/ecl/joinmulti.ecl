unsigned numgroups := 10000;
unsigned largeGroupSize := 500;
unsigned largeGroupFreq := 30;
unsigned smallGroupSizeMax := 20;
unsigned fakeWorkUs := 0; // up to pretent match takes time for parallel timing tests

rec := record
 unsigned key;
 unsigned keyn;
end;

seed := dataset([{0, 0}], rec);

rec gen1(rec L, unsigned4 c) := transform
 SELF.key := c;
 SELF.keyn := IF(0=(c%largeGroupFreq), largeGroupSize, 1+((c-1)%smallGroupSizeMax)); // # keys in group, either largeGroupSize or 1-smallGroupSizeMax
 SELF := L;
END;

seed2 := DISTRIBUTE(NORMALIZE(seed, numGroups, gen1(LEFT, COUNTER)),key);

rec gen2(rec L, unsigned4 c) := transform
 SELF := L;
END;

bigstream := NORMALIZE(seed2, LEFT.keyn, gen2(LEFT, counter));

unsigned delay(unsigned d, unsigned us) := BEGINC++ if (us) usleep(us); return d; ENDC++;

// RIGHT.keyn>=delay(RIGHT.keyn) is fake test on field to add some work to match()
baseJoin := JOIN(bigstream, bigstream, LEFT.key=RIGHT.key AND RIGHT.keyn>=delay(RIGHT.keyn, fakeWorkUs));
parallelJoin := JOIN(bigstream, bigstream, LEFT.key=RIGHT.key AND RIGHT.keyn>=delay(RIGHT.keyn, fakeWorkUs), HINT(parallel_match));
parallelUnorderedJoin := JOIN(bigstream, bigstream, LEFT.key=RIGHT.key AND RIGHT.keyn>=delay(RIGHT.keyn, fakeWorkUs), HINT(parallel_match), HINT(unsorted_output));


countGroups(dataset(rec) ds) := FUNCTION
 t := TABLE(ds, { key, unsigned groupSize := COUNT(GROUP) }, key, FEW);
 s := SORT(t, key);
 return s;
END;

baseJoinGroupCounts := countGroups(baseJoin);
parallelJoinGroupCounts := countGroups(parallelJoin);
parallelUnorderedJoinGroupCounts := countGroups(parallelUnorderedJoin);

// although ordering can differ, group sizes from all varieties should match
cmpj1 := JOIN(baseJoinGroupCounts, parallelJoinGroupCounts, LEFT.key=RIGHT.key AND LEFT.groupSize=RIGHT.groupSize, FULL ONLY);
cmpj2 := JOIN(baseJoinGroupCounts, parallelUnorderedJoinGroupCounts, LEFT.key=RIGHT.key AND LEFT.groupSize=RIGHT.groupSize, FULL ONLY);

SEQUENTIAL(
OUTPUT(COUNT(baseJoin)),
OUTPUT(COUNT(parallelJoin)),
OUTPUT(COUNT(parallelUnorderedJoin)),
PARALLEL(OUTPUT(cmpj1), OUTPUT(cmpj2))
)


