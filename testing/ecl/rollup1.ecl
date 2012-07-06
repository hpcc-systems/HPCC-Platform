d := dataset([{1},{1},{2},{3},{4},{8},{9}], { unsigned r; });

d t(d L, d r) := transform
SELF.r := IF (L.r=3,SKIP,L.r + 1);
END;

rollup(d, t(LEFT, RIGHT), LEFT.r = RIGHT.r);
rollup(d, t(LEFT, RIGHT), LEFT.r >= RIGHT.r-1, LOCAL);
