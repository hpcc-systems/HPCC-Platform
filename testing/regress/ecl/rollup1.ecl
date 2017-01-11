d := dataset([{1},{1},{2},{3},{4},{8},{9}], { unsigned r; });

d t(d L, d r) := transform
 SELF.r := IF (L.r=3,SKIP,L.r + 1);
END;

rollup(d, t(LEFT, RIGHT), LEFT.r = RIGHT.r, LOCAL);
rollup(d, t(LEFT, RIGHT), LEFT.r >= RIGHT.r-1, LOCAL);

// use global sort to spread across nodes, then perform global rollup to string
distd := SORT(d, r);
p := PROJECT(distd, TRANSFORM({ d, string res:=''; }, SELF := LEFT));
p t2(p l, p r) := transform
 SELF.res := IF(L.res='',(string)l.r+(string)r.r,(string)l.res + (string)r.r);
 SELF.r := IF(R.r=4, SKIP, L.r);
 SELF := l;
END;
rollup(p, true, t2(LEFT, RIGHT));
