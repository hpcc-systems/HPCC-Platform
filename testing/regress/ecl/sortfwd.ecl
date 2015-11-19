//class=sort
//version algo='quicksort'
//version algo='parquicksort'
//version algo='mergesort'
//version algo='parmergesort'
//version algo='heapsort'
//version algo='tbbstableqsort',nohthor

import ^ as root;
algo := #IFDEFINED(root.algo, 'quicksort');

numRows := 100000;

ds := DATASET(numRows, TRANSFORM({unsigned id}, SELF.id := COUNTER));
s1 := sort(ds, id, local, stable(algo));
s2 := SORTED(NOFOLD(s1), id, local, assert);
output(COUNT(NOFOLD(s2)) = numRows);
