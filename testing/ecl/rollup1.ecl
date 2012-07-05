d := dataset([{1},{1},{2},{3},{4}], { unsigned r; });

d t(d L, d r) := transform
SELF.r := L.r + 1;
END;

rollup(d, t(LEFT, RIGHT), LEFT.r = RIGHT.r, LOCAL);
